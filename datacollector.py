import os
import re
import random
import secrets
import threading
import bcrypt
import jwt as pyjwt
import psycopg2
import psycopg2.errors
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Header, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import Optional, List
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded


load_dotenv()

app = FastAPI(docs_url="/docs")

# ── CORS: debe agregarse ÚLTIMO para ejecutarse PRIMERO (orden inverso en Starlette) ──
# Maneja los preflight OPTIONS antes que cualquier otro middleware.
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "X-Admin-Key", "Authorization"],
    allow_credentials=False,
    max_age=600,  # cachear preflight 10 minutos
)

# ── Security headers: se agrega antes para ejecutarse DESPUÉS de CORS ────────
# Así no interfiere con las respuestas preflight OPTIONS.
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        # No agregar headers en preflight OPTIONS — CORS los maneja
        if request.method != "OPTIONS":
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response

app.add_middleware(SecurityHeadersMiddleware)

# ── TrustedHost: valida el header Host de cada request ───────────────────────
# Estaba importado pero nunca se llegaba a activar con add_middleware(), así
# que no hacía nada. Se agrega ÚLTIMO (ver comentario de CORS arriba: en
# Starlette el último middleware agregado es el que se ejecuta PRIMERO), para
# que una request con un Host inválido se rechace antes de llegar a CORS o a
# cualquier lógica de negocio.
#
# Por defecto queda en "*" (sin restricción, igual que el comportamiento
# actual) para no romper nada si no se configura nada. Para endurecerlo,
# setear ALLOWED_HOSTS en Render con el/los dominio(s) reales separados por
# coma (p. ej. "matuclub-api.onrender.com"). Con "*" en la lista (el default),
# TrustedHostMiddleware no valida nada — es un no-op explícito, no una
# restricción real, hasta que se configure.
ALLOWED_HOSTS = [h.strip() for h in os.getenv("ALLOWED_HOSTS", "*").split(",") if h.strip()] or ["*"]
app.add_middleware(TrustedHostMiddleware, allowed_hosts=ALLOWED_HOSTS)

# ── Rate limiter: leer IP real detrás del proxy de Render ────────────────────
def get_real_ip(request: Request) -> str:
    """
    IP real del cliente detrás del proxy de Render.

    El PRIMER valor de X-Forwarded-For lo pone el cliente original y es
    100% falsificable: cualquiera puede mandar el header que quiera en su
    request. Confiar en ese primer valor (como se hacía antes) permite
    evadir el rate limiting y el bloqueo por intentos fallidos mandando un
    X-Forwarded-For distinto en cada request.

    El ÚLTIMO valor de la cadena, en cambio, es el que agrega el proxy de
    Render al reenviar la request a esta app — el cliente no lo puede
    pisar. Como delante de esta app hay un único salto de proxy confiable
    (Render), ese último valor es la IP real. Si en el futuro se agrega
    otro proxy/CDN delante de Render, este criterio habría que revisarlo.
    """
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        parts = [p.strip() for p in forwarded_for.split(",") if p.strip()]
        if parts:
            return parts[-1]
    return request.client.host if request.client else "unknown"

limiter = Limiter(key_func=get_real_ip)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Devuelve errores HTTP conocidos con su código y mensaje — sin stack trace."""
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """
    Captura cualquier error inesperado (DB caída, bug, etc.) y devuelve
    un mensaje genérico. Nunca expone detalles internos al cliente.
    """
    import logging
    logging.error(f"Unhandled error on {request.method} {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Error interno del servidor. Intentá de nuevo en unos segundos."}
    )

CLUBS = {
    "1": "#282Y2LR8R",
    "2": "#2Y9GY220C",
    "3": "#2VG0RQ299",
    "4": "#2LLQ8VR2Q"
}

def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"), connect_timeout=10)


# ── OPTIMIZACIÓN CU-hrs: cache en memoria de "tabla ya existe" ─────────────
# Casi todos los endpoints empiezan llamando a uno o más ensure_X_table(cursor)
# para garantizar que su(s) tabla(s)/columna(s) existan (CREATE TABLE IF NOT
# EXISTS / ALTER TABLE ADD COLUMN IF NOT EXISTS). Eso está bien la primera
# vez, pero en producción la tabla YA existe casi siempre — y aun así, cada
# ensure_X_table() vuelve a mandar esas sentencias a Postgres en CADA
# request, cada una como un round-trip de red aparte. Con decenas de
# endpoints haciendo esto en cada llamada, es trabajo puro desperdiciado que
# mantiene más tiempo del necesario despierto el compute de Neon.
#
# _SCHEMA_READY memoiza, EN MEMORIA y por proceso, qué tablas ya se
# confirmaron: cada ensure_X_table() de más abajo chequea esto primero y,
# si ya está marcada, no toca la base para nada. La primera vez que se
# necesita de verdad, corre su DDL y lo comitea de inmediato con
# cursor.connection.commit() — SIEMPRE es lo primero que se ejecuta en su
# conexión (así están escritos todos los endpoints), así que ese commit
# nunca confirma ni afecta ningún otro trabajo de la request.
#
# Por qué el commit inmediato en vez de dejar que lo comitee el
# conn.commit() de más abajo en cada endpoint (como pasaba antes): sin esto,
# si una request fallara justo después del ensure pero antes de su propio
# commit, Postgres revertiría también la creación de tabla al cerrarse la
# conexión sin confirmar — antes no importaba porque la siguiente request
# volvía a intentar el mismo DDL idempotente sin más costo que ese; pero con
# la memoria en proceso, ese reintento automático ya no pasa, así que hace
# falta comitear acá mismo para garantizar que, la primera vez que se marca
# algo como "listo", ya haya quedado escrito de verdad en la base.
#
# Nada de esto cambia el comportamiento hacia afuera: cada ensure_X_table()
# sigue garantizando que su tabla exista antes de continuar, exactamente
# igual que antes — solo que, salvo la primera vez por proceso, no vuelve a
# tocar la base para confirmarlo.
_schema_lock = threading.Lock()
_SCHEMA_READY = set()


# ── AUTH: config ──────────────────────────────────────────────────────────
# JWT_SECRET debe fijarse como variable de entorno en Render para que las
# sesiones sobrevivan a reinicios/deploys. Si no está seteada, generamos una
# de emergencia para que el servicio no rompa, pero avisamos por log: todas
# las sesiones existentes quedarán invalidadas en cada reinicio del server.
JWT_SECRET = os.getenv("JWT_SECRET") or secrets.token_hex(32)
if not os.getenv("JWT_SECRET"):
    import logging
    logging.warning(
        "JWT_SECRET no está seteada como variable de entorno: usando una clave "
        "temporal generada en memoria. Las sesiones de usuario se invalidarán "
        "en cada reinicio del servidor. Configurá JWT_SECRET en Render."
    )
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_DAYS = 30
MAX_LOGIN_ATTEMPTS = 8
LOGIN_LOCKOUT_MINUTES = 15

USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,20}$")
# Tope superior agregado (antes era ".{8,}", sin límite de longitud alguno).
# El mínimo de 8 y los requisitos de mayúscula/minúscula/dígito no cambian.
#
# El valor 72 no es arbitrario: es el límite real y duro de bcrypt (la
# librería usada más abajo para hashear). Se comprobó al testear el fix que
# bcrypt.hashpw()/checkpw() de la versión instalada (bcrypt>=4.0) levantan
# ValueError si la contraseña supera 72 BYTES — no la truncan en silencio.
# Es decir que antes de este fix, cualquier contraseña de más de 72 bytes ya
# rompía el registro y el login con un 500 ("no se pudo completar..."), no
# solo dejaba pasar payloads grandes sin sentido. Por eso, además del tope
# por caracteres acá y en los modelos (Field(max_length=...)), se agrega más
# abajo una verificación explícita en bytes (UTF-8) antes de llamar a bcrypt,
# para cubrir contraseñas con caracteres no-ASCII (tildes, emojis, etc.) que
# podrían tener ≤72 caracteres pero más de 72 bytes.
PASSWORD_MAX_LENGTH = 72
PASSWORD_RE = re.compile(r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d).{8,72}$")
TAG_RE = re.compile(r"^#[0-9A-Z]{3,15}$")


def reject_if_password_too_many_bytes(password: str):
    """
    Chequeo defensivo adicional al de caracteres: bcrypt trabaja en bytes,
    no en caracteres. Un password con acentos/emojis podría tener <=72
    caracteres (pasa PASSWORD_RE) y aun así pesar más de 72 bytes en UTF-8,
    lo que haría explotar bcrypt.hashpw/checkpw con un ValueError sin este
    chequeo.
    """
    if len((password or "").encode("utf-8")) > PASSWORD_MAX_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Entre 8 y {PASSWORD_MAX_LENGTH} caracteres, con mayúsculas, minúsculas y números."
        )


def normalize_tag(tag: str) -> str:
    tag = (tag or "").strip().upper()
    if not tag.startswith("#"):
        tag = "#" + tag
    return tag


def canonical_tag(tag: str) -> str:
    """
    Forma canónica de una tag SOLO para comparar tags entre tablas (users vs
    players). Las tags de Brawl Stars no usan la letra O: el juego la trata
    como el número 0. TAG_RE sí deja registrarse con la letra O, así que un
    usuario puede tener guardado "#2YCORRPV" mientras que players (que viene
    de la API oficial) tiene "#2YC0RRPV". Comparando las formas canónicas, los
    dos matchean. No se usa para guardar nada, solo para comparar.
    """
    return normalize_tag(tag).replace("O", "0")


def create_access_token(user_id: int, username: str, tag: str) -> str:
    from datetime import timedelta
    payload = {
        "sub": str(user_id),
        "username": username,
        "tag": tag,
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS),
        "iat": datetime.now(timezone.utc),
    }
    return pyjwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def get_current_user(authorization: Optional[str] = Header(None)):
    """
    Dependencia de FastAPI: valida el JWT del header Authorization: Bearer <token>.

    El JWT no tiene revocación propia y dura hasta 30 días (JWT_EXPIRE_DAYS):
    si solo confiáramos en su firma/expiración, una cuenta baneada (o borrada)
    DESPUÉS de emitido el token seguiría pasando esta validación sin problema
    hasta que el token expire solo, y cualquier endpoint que dependa de esta
    función sin re-chequear el baneo a mano quedaría abierto para esa cuenta.
    Por eso acá se vuelve a consultar el estado real contra la base en cada
    request — el baneo (o el borrado de la cuenta) tiene efecto inmediato en
    TODOS los endpoints protegidos, no solo en los que lo verifican aparte.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="No autenticado")
    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = pyjwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Tu sesión expiró, iniciá sesión de nuevo")
    except pyjwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Sesión inválida")

    try:
        user_id = int(payload.get("sub"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=401, detail="Sesión inválida")

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_users_table(cursor)
        cursor.execute("SELECT banned, banned_reason FROM users WHERE id = %s", (user_id,))
        row = cursor.fetchone()
        if row is None:
            # La cuenta ya no existe (borrada por un admin) — el token quedó huérfano.
            raise HTTPException(status_code=401, detail="Sesión inválida")
        banned, banned_reason = row
        if banned:
            detail = "Esta cuenta fue suspendida."
            if banned_reason:
                detail = f"Esta cuenta fue suspendida: {banned_reason}"
            raise HTTPException(status_code=403, detail=detail)
    finally:
        cursor.close()
        conn.close()

    return payload


# ── AUTH: tabla y endpoints ──────────────────────────────────────────────────
# Reemplaza el login/registro anterior (que vivía en storage temporal del
# navegador). Las contraseñas se hashean con bcrypt (nunca se guardan ni se
# transmiten en texto plano más que en el body HTTPS del request original) y
# cada cuenta queda ligada a una tag de Brawl Stars única. El login devuelve
# un JWT que el front-end guarda y reenvía en el header Authorization.

def ensure_users_table(cursor):
    if "users" in _SCHEMA_READY:
        return
    with _schema_lock:
        if "users" in _SCHEMA_READY:
            return
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id               SERIAL PRIMARY KEY,
                username         TEXT NOT NULL,
                username_lower   TEXT NOT NULL UNIQUE,
                player_tag       TEXT NOT NULL UNIQUE,
                password_hash    TEXT NOT NULL,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_login_at    TIMESTAMPTZ,
                failed_attempts  INTEGER NOT NULL DEFAULT 0,
                locked_until     TIMESTAMPTZ
            )
        """)
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS banned BOOLEAN NOT NULL DEFAULT FALSE")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS banned_reason TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS banned_at TIMESTAMPTZ")
        cursor.connection.commit()
        _SCHEMA_READY.add("users")


def ensure_login_attempts_table(cursor):
    """
    Contador de intentos fallidos de login por (usuario, IP).

    Antes, el bloqueo de MAX_LOGIN_ATTEMPTS vivía únicamente en
    users.failed_attempts/locked_until, es decir, por CUENTA completa sin
    importar de dónde vinieran los intentos. Eso significa que cualquiera
    que supiera (o adivinara) un username podía mandar contraseñas
    incorrectas y dejar esa cuenta bloqueada 15 minutos, indefinidamente,
    sin necesitar la contraseña ni estar autenticado — un DoS dirigido
    contra cualquier usuario conocido.

    Esta tabla nueva bloquea por la COMBINACIÓN (usuario, IP): un atacante
    desde su propia IP sigue quedando bloqueado tras varios intentos
    fallidos contra una cuenta (se mantiene la protección contra fuerza
    bruta), pero eso ya no le impide al dueño real de la cuenta loguearse
    con su contraseña correcta desde su propia conexión.

    Las columnas users.failed_attempts/locked_until se siguen actualizando
    en login() exactamente igual que antes — el panel de admin las lee en
    GET /admin/users y eso no cambia — pero dejan de ser lo que decide si
    se bloquea o no un intento de login; esa decisión ahora la toma esta
    tabla.
    """
    if "login_attempts" in _SCHEMA_READY:
        return
    with _schema_lock:
        if "login_attempts" in _SCHEMA_READY:
            return
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS login_attempts (
                username_lower   TEXT NOT NULL,
                ip               TEXT NOT NULL,
                failed_attempts  INTEGER NOT NULL DEFAULT 0,
                locked_until     TIMESTAMPTZ,
                PRIMARY KEY (username_lower, ip)
            )
        """)
        cursor.connection.commit()
        _SCHEMA_READY.add("login_attempts")


class RegisterBody(BaseModel):
    username: str
    tag: str
    # max_length acá corta un payload gigante ANTES de correrle el regex o
    # el hash de bcrypt (que igual trunca a 72 bytes internamente). No
    # afecta a ninguna contraseña real: nadie usa 128+ caracteres.
    password: str = Field(..., max_length=PASSWORD_MAX_LENGTH)


class LoginBody(BaseModel):
    username: str
    password: str = Field(..., max_length=PASSWORD_MAX_LENGTH)


@app.post("/auth/register")
@limiter.limit("5/minute")
def register(request: Request, body: RegisterBody):
    username = (body.username or "").strip()
    if not USERNAME_RE.match(username):
        raise HTTPException(status_code=400, detail="Usá entre 3 y 20 caracteres: letras, números o guion bajo.")

    tag = normalize_tag(body.tag)
    if not TAG_RE.match(tag):
        raise HTTPException(status_code=400, detail="Tag de Brawl Stars inválida.")

    if not PASSWORD_RE.match(body.password or ""):
        raise HTTPException(
            status_code=400,
            detail=f"Entre 8 y {PASSWORD_MAX_LENGTH} caracteres, con mayúsculas, minúsculas y números."
        )
    reject_if_password_too_many_bytes(body.password)

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_users_table(cursor)

        cursor.execute("SELECT 1 FROM users WHERE username_lower = %s", (username.lower(),))
        if cursor.fetchone():
            raise HTTPException(status_code=409, detail="Ese nombre de usuario ya está en uso.")
        cursor.execute("SELECT 1 FROM users WHERE player_tag = %s", (tag,))
        if cursor.fetchone():
            raise HTTPException(status_code=409, detail="Esa tag de Brawl Stars ya está registrada por otro usuario.")

        pw_hash = bcrypt.hashpw(body.password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        try:
            cursor.execute("""
                INSERT INTO users (username, username_lower, player_tag, password_hash)
                VALUES (%s, %s, %s, %s) RETURNING id
            """, (username, username.lower(), tag, pw_hash))
            user_id = cursor.fetchone()[0]
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            # Carrera entre dos requests simultáneos: alguien ganó la unicidad primero.
            raise HTTPException(status_code=409, detail="Ese usuario o esa tag ya están registrados.")

        token = create_access_token(user_id, username, tag)
        return {"token": token, "username": username, "tag": tag}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo completar el registro.")
    finally:
        cursor.close()
        conn.close()


@app.post("/auth/login")
@limiter.limit("10/minute")
def login(request: Request, body: LoginBody):
    username = (body.username or "").strip().lower()
    ip = get_real_ip(request)
    generic_error = HTTPException(status_code=401, detail="Usuario o contraseña incorrectos.")
    if not username or not body.password:
        raise generic_error
    if len(body.password.encode("utf-8")) > PASSWORD_MAX_LENGTH:
        # Un candidato de más de 72 bytes nunca puede ser la contraseña
        # correcta (ver PASSWORD_MAX_LENGTH / reject_if_password_too_many_bytes):
        # ninguna cuenta puede tener una así hasheada desde que también se
        # capó en /auth/register. Cortamos acá mismo para no pasarle a
        # bcrypt.checkpw un candidato que lo hace explotar con ValueError.
        raise generic_error

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_users_table(cursor)
        ensure_login_attempts_table(cursor)
        cursor.execute("""
            SELECT id, username, player_tag, password_hash, failed_attempts, locked_until, banned, banned_reason
            FROM users WHERE username_lower = %s
        """, (username,))
        row = cursor.fetchone()
        if not row:
            raise generic_error
        user_id, real_username, tag, pw_hash, failed_attempts, locked_until, banned, banned_reason = row

        if banned:
            raise HTTPException(
                status_code=403,
                detail="Esta cuenta fue suspendida" + (f": {banned_reason}" if banned_reason else ".")
            )

        # Gate real de bloqueo: por (usuario, IP), no por la cuenta entera
        # (ver ensure_login_attempts_table). users.locked_until de arriba
        # YA NO se usa para decidir si se bloquea esta request.
        cursor.execute(
            "SELECT failed_attempts, locked_until FROM login_attempts WHERE username_lower = %s AND ip = %s",
            (username, ip)
        )
        la_row = cursor.fetchone()
        ip_failed_attempts, ip_locked_until = la_row if la_row else (0, None)

        if ip_locked_until and ip_locked_until > datetime.now(timezone.utc):
            raise HTTPException(
                status_code=429,
                detail="Cuenta bloqueada temporalmente por demasiados intentos fallidos. Probá de nuevo en unos minutos."
            )

        if not bcrypt.checkpw(body.password.encode("utf-8"), pw_hash.encode("utf-8")):
            from datetime import timedelta
            failed_attempts += 1
            ip_failed_attempts += 1
            reached_limit = failed_attempts >= MAX_LOGIN_ATTEMPTS
            ip_reached_limit = ip_failed_attempts >= MAX_LOGIN_ATTEMPTS
            new_lock = (datetime.now(timezone.utc) + timedelta(minutes=LOGIN_LOCKOUT_MINUTES)) if reached_limit else None
            ip_new_lock = (datetime.now(timezone.utc) + timedelta(minutes=LOGIN_LOCKOUT_MINUTES)) if ip_reached_limit else None

            # Contador agregado por cuenta: se mantiene igual que antes,
            # solo para que lo siga mostrando GET /admin/users. Ya no
            # bloquea nada por sí solo.
            cursor.execute("""
                UPDATE users SET failed_attempts = %s, locked_until = %s WHERE id = %s
            """, (0 if reached_limit else failed_attempts, new_lock, user_id))
            # Contador real que sí bloquea, acotado a esta IP puntual.
            cursor.execute("""
                INSERT INTO login_attempts (username_lower, ip, failed_attempts, locked_until)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (username_lower, ip) DO UPDATE SET
                    failed_attempts = EXCLUDED.failed_attempts,
                    locked_until    = EXCLUDED.locked_until
            """, (username, ip, 0 if ip_reached_limit else ip_failed_attempts, ip_new_lock))
            conn.commit()
            raise generic_error

        cursor.execute("""
            UPDATE users SET failed_attempts = 0, locked_until = NULL, last_login_at = NOW() WHERE id = %s
        """, (user_id,))
        cursor.execute(
            "DELETE FROM login_attempts WHERE username_lower = %s AND ip = %s",
            (username, ip)
        )
        conn.commit()

        token = create_access_token(user_id, real_username, tag)
        return {"token": token, "username": real_username, "tag": tag}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo iniciar sesión.")
    finally:
        cursor.close()
        conn.close()


@app.get("/auth/me")
@limiter.limit("30/minute")
def me(request: Request, user=Depends(get_current_user)):
    return {"username": user["username"], "tag": user["tag"]}


# ── ADMIN: GESTIÓN DE CUENTAS ────────────────────────────────────────────────
class BanBody(BaseModel):
    reason: Optional[str] = None


@app.get("/admin/users")
@limiter.limit("20/minute")
def adminListUsers(request: Request, x_admin_key: Optional[str] = Header(None)):
    check_admin(request, x_admin_key)
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_users_table(cursor)
        cursor.execute("""
            SELECT id, username, player_tag, created_at, last_login_at,
                   banned, banned_reason, banned_at, failed_attempts, locked_until
            FROM users
            ORDER BY created_at DESC
        """)
        rows = cursor.fetchall()
        return [{
            "id": uid,
            "username": username,
            "tag": tag,
            "created_at": created_at.isoformat() if created_at else None,
            "last_login_at": last_login.isoformat() if last_login else None,
            "banned": banned,
            "banned_reason": banned_reason,
            "banned_at": banned_at.isoformat() if banned_at else None,
            "failed_attempts": failed_attempts,
            "locked_until": locked_until.isoformat() if locked_until else None,
        } for (uid, username, tag, created_at, last_login, banned, banned_reason,
               banned_at, failed_attempts, locked_until) in rows]
    finally:
        cursor.close()
        conn.close()


@app.patch("/admin/users/{user_id}/ban")
@limiter.limit("15/minute")
def adminBanUser(request: Request, user_id: int, body: BanBody, x_admin_key: Optional[str] = Header(None)):
    check_admin(request, x_admin_key)
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_users_table(cursor)
        cursor.execute("""
            UPDATE users SET banned = TRUE, banned_reason = %s, banned_at = NOW()
            WHERE id = %s RETURNING id
        """, ((body.reason or '').strip() or None, user_id))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Cuenta no encontrada")
        conn.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo suspender la cuenta")
    finally:
        cursor.close()
        conn.close()


@app.patch("/admin/users/{user_id}/unban")
@limiter.limit("15/minute")
def adminUnbanUser(request: Request, user_id: int, x_admin_key: Optional[str] = Header(None)):
    check_admin(request, x_admin_key)
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_users_table(cursor)
        cursor.execute("""
            UPDATE users SET banned = FALSE, banned_reason = NULL, banned_at = NULL
            WHERE id = %s RETURNING id
        """, (user_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Cuenta no encontrada")
        conn.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo reactivar la cuenta")
    finally:
        cursor.close()
        conn.close()


@app.delete("/admin/users/{user_id}")
@limiter.limit("10/minute")
def adminDeleteUser(request: Request, user_id: int, x_admin_key: Optional[str] = Header(None)):
    check_admin(request, x_admin_key)
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_users_table(cursor)
        cursor.execute("DELETE FROM users WHERE id = %s RETURNING id", (user_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Cuenta no encontrada")
        conn.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo eliminar la cuenta")
    finally:
        cursor.close()
        conn.close()


# ── EXISTING ENDPOINTS (unchanged) ───────────────────────────────────────────

@app.get("/player/{player_tag}")
@limiter.limit("30/minute")
def ver_datos(request: Request, player_tag: str):
    player_tag = player_tag.strip().upper()
    if not player_tag.startswith("#"):
        player_tag = "#" + player_tag
    # Tags de Brawl Stars: # seguido de 3-15 caracteres alfanuméricos (0-9, A-Z, sin I,O,U)
    if not re.match(r"^#[0-9A-Z]{3,15}$", player_tag):
        raise HTTPException(status_code=400, detail="Tag de jugador inválido")

    conn = get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT name, highest_trophies, wins3v3, winsSolo,
                   total_prestige, highestWinstreak, maxWsBrawler, club_tag, club_name, icon_url
            FROM players WHERE tag = %s
        """, (player_tag,))

        result = cursor.fetchone()
        if not result:
            return {"error": "Jugador no encontrado"}

        (name, highest_trophies, wins3v3, winsSolo,
         total_prestige, highest_ws, ws_brawler, club_tag, club_name, icon_url) = result

        # Muestreo uniforme de hasta 60 puntos sobre toda la historia del jugador.
        # Paso 1: numerar filas y asignar bucket con NTILE en una CTE.
        # Paso 2: en otra CTE tomar ROW_NUMBER dentro de cada bucket.
        # Esto evita anidar window functions, que PostgreSQL no permite.
        # Siempre se incluye la última fila (rn = total) para llegar al presente.
        cursor.execute("""
            WITH numbered AS (
                SELECT timestamp, trophies, wins3v3, winsSolo, total_prestige,
                       ROW_NUMBER() OVER (ORDER BY timestamp ASC) AS rn,
                       COUNT(*)     OVER ()                        AS total,
                       NTILE(60)    OVER (ORDER BY timestamp ASC) AS bucket
                FROM player_stats_history
                WHERE player_tag = %s
            ),
            bucketed AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY timestamp ASC) AS rn_in_bucket
                FROM numbered
            )
            SELECT timestamp, trophies, wins3v3, winsSolo, total_prestige
            FROM bucketed
            WHERE rn_in_bucket = 1  -- primer punto de cada bucket
               OR rn = total         -- siempre incluir el último snapshot
            ORDER BY timestamp ASC
            LIMIT 60
        """, (player_tag,))
        history = cursor.fetchall()

        cursor.execute("""
            SELECT brawler_name, power_level, gadgets, star_powers, hipercharge, trophies
            FROM player_brawlers
            WHERE player_tag = %s
            ORDER BY trophies DESC
            LIMIT 12
        """, (player_tag,))
        brawlers = cursor.fetchall()

        return {
            "name": name,
            "highest_trophies": highest_trophies,
            "wins3v3": wins3v3,
            "winsSolo": winsSolo,
            "total_prestige": total_prestige,
            "best_winstreak": {
                "value": highest_ws,
                "brawler": ws_brawler
            },
            "club_tag": club_tag,
            "club_name": club_name,
            "icon_url": icon_url,
            "history": [list(h) for h in history],
            "top_brawlers": [list(b) for b in brawlers]
        }
    finally:
        cursor.close()
        conn.close()


def _club_filter(club: Optional[str]):
    """Devuelve (WHERE clause, params) para filtrar por club si corresponde."""
    if club and club in CLUBS:
        return "WHERE club_tag = %s", (CLUBS[club],)
    return "", ()


@app.get("/top/prestige")
@limiter.limit("20/minute")
def topPrestige(request: Request, club: Optional[str] = None):
    where, params = _club_filter(club)
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT tag, name, total_prestige
            FROM players {where} ORDER BY total_prestige DESC LIMIT 50
        """, params)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/trophies")
@limiter.limit("20/minute")
def topTrophies(request: Request, club: Optional[str] = None):
    where, params = _club_filter(club)
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT tag, name, highest_trophies
            FROM players {where} ORDER BY highest_trophies DESC LIMIT 50
        """, params)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/wins3v3")
@limiter.limit("20/minute")
def topWins3v3(request: Request, club: Optional[str] = None):
    where, params = _club_filter(club)
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT tag, name, wins3v3
            FROM players {where} ORDER BY wins3v3 DESC LIMIT 50
        """, params)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/winssolo")
@limiter.limit("20/minute")
def topWinsSolo(request: Request, club: Optional[str] = None):
    where, params = _club_filter(club)
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT tag, name, winsSolo
            FROM players {where} ORDER BY winsSolo DESC LIMIT 50
        """, params)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v}
                for i, (t, n, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/winstreak")
@limiter.limit("20/minute")
def topWinstreak(request: Request, club: Optional[str] = None):
    where, params = _club_filter(club)
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT tag, name, highestWinstreak, maxWsBrawler
            FROM players {where} ORDER BY highestWinstreak DESC LIMIT 50
        """, params)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "value": v, "brawler": b}
                for i, (t, n, v, b) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/brawler-trophies")
@limiter.limit("20/minute")
def topBrawlerTrophies(request: Request, club: Optional[str] = None):
    where_p = "WHERE p.club_tag = %s AND" if (club and club in CLUBS) else "WHERE"
    params_pre = (CLUBS[club],) if (club and club in CLUBS) else ()
    conn = get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT p.tag, p.name, pb.brawler_name, pb.trophies
            FROM player_brawlers pb
            JOIN players p ON pb.player_tag = p.tag
            {where_p} (pb.player_tag, pb.trophies) IN (
                SELECT player_tag, MAX(trophies) FROM player_brawlers GROUP BY player_tag
            )
            ORDER BY pb.trophies DESC LIMIT 50
        """, params_pre)
        rows = cursor.fetchall()
        return [{"rank": i+1, "tag": t, "name": n, "brawler": b, "value": v}
                for i, (t, n, b, v) in enumerate(rows)]
    finally:
        cursor.close(); conn.close()


@app.get("/top/brawler/{brawler_name}")
@limiter.limit("20/minute")
def topBrawler(request: Request, brawler_name: str, club: str = None):
    brawler_name = brawler_name.strip().upper()

    conn = get_conn()
    cursor = conn.cursor()

    try:
        if club and club in CLUBS:
            club_tag = CLUBS[club]
            cursor.execute("""
                SELECT p.tag, p.name, pb.trophies
                FROM player_brawlers pb
                JOIN players p ON pb.player_tag = p.tag
                WHERE pb.brawler_name = %s AND p.club_tag = %s
                ORDER BY pb.trophies DESC
                LIMIT 12
            """, (brawler_name, club_tag))
        else:
            cursor.execute("""
                SELECT p.tag, p.name, pb.trophies
                FROM player_brawlers pb
                JOIN players p ON pb.player_tag = p.tag
                WHERE pb.brawler_name = %s
                ORDER BY pb.trophies DESC
                LIMIT 12
            """, (brawler_name,))

        rows = cursor.fetchall()

        if not rows:
            return {"error": "No hay datos"}

        return [{"rank": i+1, "tag": tag, "name": n, "trophies": t}
                for i, (tag, n, t) in enumerate(rows)]
    finally:
        cursor.close()
        conn.close()


@app.get("/club/{club_num}/members")
@limiter.limit("20/minute")
def clubMembers(request: Request, club_num: str):
    if club_num not in CLUBS:
        return {"error": "Club no encontrado"}

    club_tag = CLUBS[club_num]
    conn = get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT tag, name, highest_trophies, icon_url
            FROM players
            WHERE club_tag = %s
            ORDER BY highest_trophies DESC
        """, (club_tag,))
        rows = cursor.fetchall()

        return [{"rank": i+1, "tag": tag, "name": n, "trophies": t, "icon_url": ico}
                for i, (tag, n, t, ico) in enumerate(rows)]
    finally:
        cursor.close()
        conn.close()


# ── EVENTS: HELPERS ───────────────────────────────────────────────────────────

VALID_METRICS = ("trophies", "wins3v3", "winsSolo", "prestige", "brawler_trophies")

# Maps metric → (players column for snapshot, players column for live value)
# For brawler_trophies we query player_brawlers instead, handled separately.
METRIC_PLAYER_COL = {
    "trophies":       "highest_trophies",
    "wins3v3":        "wins3v3",
    "winsSolo":       "winsSolo",
    "prestige":       "total_prestige",
}

def ensure_events_tables(cursor):
    if "events" in _SCHEMA_READY:
        return
    with _schema_lock:
        if "events" in _SCHEMA_READY:
            return
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,
                reward TEXT NOT NULL,
                metric TEXT NOT NULL,
                brawler_name TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                ends_at TIMESTAMPTZ NOT NULL,
                closed_at TIMESTAMPTZ,
                is_active BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS event_snapshots (
                id SERIAL PRIMARY KEY,
                event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
                player_tag TEXT NOT NULL,
                player_name TEXT NOT NULL,
                icon_url TEXT,
                value_start INTEGER NOT NULL,
                value_end INTEGER,
                UNIQUE(event_id, player_tag)
            )
        """)
        cursor.execute("""
            DO $$
            DECLARE c TEXT;
            BEGIN
                SELECT conname INTO c FROM pg_constraint
                WHERE conrelid = 'events'::regclass AND contype = 'c' AND conname LIKE '%metric%';
                IF c IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE events DROP CONSTRAINT ' || quote_ident(c);
                END IF;
            END $$
        """)
        cursor.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS brawler_name TEXT")
        cursor.execute("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='event_snapshots' AND column_name='trophies_start'
                ) THEN
                    ALTER TABLE event_snapshots RENAME COLUMN trophies_start TO value_start;
                    ALTER TABLE event_snapshots RENAME COLUMN trophies_end   TO value_end;
                END IF;
            END $$
        """)
        cursor.connection.commit()
        _SCHEMA_READY.add("events")


ADMIN_MAX_ATTEMPTS = 5
ADMIN_LOCKOUT_MINUTES = 15


def ensure_admin_auth_table(cursor):
    if "admin_auth" in _SCHEMA_READY:
        return
    with _schema_lock:
        if "admin_auth" in _SCHEMA_READY:
            return
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admin_auth_attempts (
                ip              TEXT PRIMARY KEY,
                failed_attempts INTEGER NOT NULL DEFAULT 0,
                locked_until    TIMESTAMPTZ
            )
        """)
        cursor.connection.commit()
        _SCHEMA_READY.add("admin_auth")


def check_admin(request: Request, x_admin_key: Optional[str]):
    """
    Valida la clave de administrador.

    Dos cosas que antes fallaban:
    1) La comparación se hacía con `!=`, que en Python no es de tiempo
       constante — en teoría filtra por timing cuánto del prefijo de la
       clave ingresada coincide con la real. Ahora se usa
       secrets.compare_digest, pensado justo para esto.
    2) No había ningún límite de intentos específico para la clave de
       admin. El propio frontend usa un endpoint cualquiera protegido por
       esta función como "oráculo" (403 = clave mal, cualquier otra cosa =
       clave bien) para validarla en el panel de admin, y antes de este
       fix el rate limiting general se podía evadir falsificando
       X-Forwarded-For (ver get_real_ip) — la combinación permitía probar
       la clave a fuerza bruta sin límite real. Ahora, además de la IP
       real (ya no falsificable), se lleva un conteo de intentos fallidos
       por IP y se bloquea temporalmente tras varios seguidos, igual que
       el lockout de /auth/login.
    """
    admin_key = os.getenv("ADMIN_KEY", "")
    ip = get_real_ip(request)

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_admin_auth_table(cursor)
        cursor.execute(
            "SELECT failed_attempts, locked_until FROM admin_auth_attempts WHERE ip = %s",
            (ip,)
        )
        row = cursor.fetchone()
        failed_attempts, locked_until = row if row else (0, None)

        if locked_until and locked_until > datetime.now(timezone.utc):
            raise HTTPException(
                status_code=429,
                detail="Demasiados intentos fallidos. Probá de nuevo más tarde."
            )

        is_valid = bool(admin_key) and secrets.compare_digest(x_admin_key or "", admin_key)

        if not is_valid:
            from datetime import timedelta
            failed_attempts += 1
            reached_limit = failed_attempts >= ADMIN_MAX_ATTEMPTS
            new_lock = (
                datetime.now(timezone.utc) + timedelta(minutes=ADMIN_LOCKOUT_MINUTES)
                if reached_limit else None
            )
            cursor.execute("""
                INSERT INTO admin_auth_attempts (ip, failed_attempts, locked_until)
                VALUES (%s, %s, %s)
                ON CONFLICT (ip) DO UPDATE SET
                    failed_attempts = EXCLUDED.failed_attempts,
                    locked_until    = EXCLUDED.locked_until
            """, (ip, 0 if reached_limit else failed_attempts, new_lock))
            conn.commit()
            raise HTTPException(status_code=403, detail="No autorizado")

        if failed_attempts or locked_until:
            cursor.execute("""
                UPDATE admin_auth_attempts SET failed_attempts = 0, locked_until = NULL
                WHERE ip = %s
            """, (ip,))
            conn.commit()
    finally:
        cursor.close()
        conn.close()


def auto_close_expired(cursor):
    """
    Cierra eventos expirados y freezea sus valores finales.
    Sin el freeze, compute_results usa valores live del jugador
    y el resultado del torneo cambia después de terminar.
    """
    # Primero obtener los eventos que hay que cerrar
    cursor.execute("""
        SELECT id, metric, brawler_name FROM events
        WHERE is_active = TRUE AND ends_at <= NOW()
    """)
    to_close = cursor.fetchall()

    for (event_id, metric, brawler_name) in to_close:
        # Freeze solo los snapshots que aún no tienen value_end
        freeze_values(cursor, event_id, metric, brawler_name)

    if to_close:
        cursor.execute("""
            UPDATE events
            SET is_active = FALSE, closed_at = NOW()
            WHERE is_active = TRUE AND ends_at <= NOW()
        """)


def snapshot_values(cursor, event_id: int, metric: str, brawler_name: Optional[str]):
    """Insert starting-value snapshot for all players for the given metric."""
    if metric == "brawler_trophies":
        bn = (brawler_name or "").strip().upper()
        cursor.execute("""
            INSERT INTO event_snapshots (event_id, player_tag, player_name, icon_url, value_start)
            SELECT %s, p.tag, p.name, p.icon_url, COALESCE(pb.trophies, 0)
            FROM players p
            LEFT JOIN player_brawlers pb
              ON pb.player_tag = p.tag AND UPPER(pb.brawler_name) = %s
        """, (event_id, bn))
    else:
        col = METRIC_PLAYER_COL[metric]
        cursor.execute(f"""
            INSERT INTO event_snapshots (event_id, player_tag, player_name, icon_url, value_start)
            SELECT %s, tag, name, icon_url, {col}
            FROM players
        """, (event_id,))


def freeze_values(cursor, event_id: int, metric: str, brawler_name: Optional[str]):
    """
    Freeze final values into value_end for all participants of an event.
    Solo actualiza filas con value_end IS NULL para no sobreescribir
    snapshots ya freezeados correctamente por un cierre manual previo.
    """
    if metric == "brawler_trophies":
        bn = (brawler_name or "").strip().upper()
        cursor.execute("""
            UPDATE event_snapshots es
            SET value_end = COALESCE(pb.trophies, 0)
            FROM players p
            LEFT JOIN player_brawlers pb
              ON pb.player_tag = p.tag AND UPPER(pb.brawler_name) = %s
            WHERE es.player_tag = p.tag AND es.event_id = %s
              AND es.value_end IS NULL
        """, (bn, event_id))
    else:
        col = METRIC_PLAYER_COL[metric]
        cursor.execute(f"""
            UPDATE event_snapshots es
            SET value_end = p.{col}
            FROM players p
            WHERE es.player_tag = p.tag AND es.event_id = %s
              AND es.value_end IS NULL
        """, (event_id,))


def compute_results(cursor, event_id: int, metric: str, brawler_name: Optional[str]):
    """Return ranked leaderboard for an event, using live values when still active."""
    if metric == "brawler_trophies":
        bn = (brawler_name or "").strip().upper()
        cursor.execute("""
            SELECT
                es.player_tag,
                es.player_name,
                es.icon_url,
                es.value_start,
                COALESCE(es.value_end, COALESCE(pb.trophies, 0)) AS value_now,
                COALESCE(es.value_end, COALESCE(pb.trophies, 0)) - es.value_start AS delta
            FROM event_snapshots es
            LEFT JOIN players p ON es.player_tag = p.tag
            LEFT JOIN player_brawlers pb
              ON pb.player_tag = es.player_tag AND UPPER(pb.brawler_name) = %s
            WHERE es.event_id = %s
            ORDER BY delta DESC
        """, (bn, event_id))
    else:
        col = METRIC_PLAYER_COL.get(metric, "highest_trophies")
        cursor.execute(f"""
            SELECT
                es.player_tag,
                es.player_name,
                es.icon_url,
                es.value_start,
                COALESCE(es.value_end, p.{col}) AS value_now,
                COALESCE(es.value_end, p.{col}) - es.value_start AS delta
            FROM event_snapshots es
            LEFT JOIN players p ON es.player_tag = p.tag
            WHERE es.event_id = %s
            ORDER BY delta DESC
        """, (event_id,))

    rows = cursor.fetchall()
    return [
        {
            "rank": i + 1,
            "tag": tag,
            "name": name,
            "icon_url": ico,
            "value_start": vs,
            "value_now": vn,
            "delta": delta
        }
        for i, (tag, name, ico, vs, vn, delta) in enumerate(rows)
    ]


# ── EVENTS: PUBLIC ENDPOINTS ─────────────────────────────────────────────────

@app.get("/events")
@limiter.limit("10/minute")
def getEvents(request: Request):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_events_tables(cursor)
        ensure_raffle_tables(cursor)
        auto_close_expired(cursor)
        conn.commit()

        cursor.execute("""
            SELECT id, title, description, reward, metric, brawler_name,
                   started_at, ends_at, closed_at, is_active
            FROM events
            ORDER BY started_at DESC
        """)
        rows = cursor.fetchall()

        result = []
        for (eid, title, desc, reward, metric, brawler_name,
             started_at, ends_at, closed_at, is_active) in rows:
            participants = compute_results(cursor, eid, metric, brawler_name)
            cursor.execute("""
                SELECT position, tickets FROM event_ticket_rules
                WHERE event_id = %s ORDER BY position
            """, (eid,))
            ticket_rewards = [{"position": p, "tickets": t} for (p, t) in cursor.fetchall()]
            result.append({
                "id": eid,
                "title": title,
                "description": desc,
                "reward": reward,
                "metric": metric,
                "brawler_name": brawler_name,
                "started_at": started_at.isoformat() if started_at else None,
                "ends_at": ends_at.isoformat() if ends_at else None,
                "closed_at": closed_at.isoformat() if closed_at else None,
                "is_active": is_active,
                "participants": participants,
                "ticket_rewards": ticket_rewards
            })
        return result
    finally:
        cursor.close()
        conn.close()


# ── EVENTS: ADMIN ENDPOINTS ──────────────────────────────────────────────────

class TicketReward(BaseModel):
    position: int
    tickets: int


class CreateEventBody(BaseModel):
    # Solo se agrega tope MÁXIMO (max_length); no se agrega min_length ni
    # se cambia nada del resto de las validaciones que ya hace el handler
    # (metric, brawler_name, duration_hours, ticket_rewards siguen igual).
    title: str = Field(..., max_length=200)
    description: Optional[str] = Field(None, max_length=2000)
    reward: str = Field(..., max_length=300)
    metric: str = "trophies"
    brawler_name: Optional[str] = None   # required when metric == brawler_trophies
    duration_hours: float
    ticket_rewards: Optional[List[TicketReward]] = None  # tickets de sorteo por posición final


@app.post("/events")
@limiter.limit("5/minute")
def createEvent(request: Request, body: CreateEventBody, x_admin_key: Optional[str] = Header(None)):
    check_admin(request, x_admin_key)

    if body.metric not in VALID_METRICS:
        raise HTTPException(
            status_code=400,
            detail=f"metric invalida. Valores válidos: {', '.join(VALID_METRICS)}"
        )
    if body.metric == "brawler_trophies" and not body.brawler_name:
        raise HTTPException(status_code=400, detail="brawler_name es requerido para la metric brawler_trophies")
    if body.duration_hours <= 0:
        raise HTTPException(status_code=400, detail="duration_hours debe ser mayor a 0")

    clean_rewards = []
    if body.ticket_rewards:
        seen_positions = set()
        for tr in body.ticket_rewards:
            if tr.position < 1 or tr.tickets < 0:
                raise HTTPException(status_code=400, detail="Posición y tickets de premio deben ser válidos (posición ≥ 1, tickets ≥ 0).")
            if tr.position in seen_positions:
                raise HTTPException(status_code=400, detail=f"Posición {tr.position} repetida en los premios de tickets.")
            seen_positions.add(tr.position)
            clean_rewards.append((tr.position, tr.tickets))

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_events_tables(cursor)
        ensure_raffle_tables(cursor)

        # Close any currently active event before creating a new one
        cursor.execute("""
            UPDATE events SET is_active = FALSE, closed_at = NOW()
            WHERE is_active = TRUE
        """)

        cursor.execute("""
            INSERT INTO events (title, description, reward, metric, brawler_name, ends_at)
            VALUES (%s, %s, %s, %s, %s, NOW() + INTERVAL '1 hour' * %s)
            RETURNING id
        """, (body.title, body.description, body.reward, body.metric,
              body.brawler_name, body.duration_hours))
        event_id = cursor.fetchone()[0]

        snapshot_values(cursor, event_id, body.metric, body.brawler_name)

        for position, tickets in clean_rewards:
            cursor.execute("""
                INSERT INTO event_ticket_rules (event_id, position, tickets)
                VALUES (%s, %s, %s)
                ON CONFLICT (event_id, position) DO UPDATE SET tickets = EXCLUDED.tickets
            """, (event_id, position, tickets))

        conn.commit()
        return {"ok": True, "event_id": event_id}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error al crear el evento")
    finally:
        cursor.close()
        conn.close()


@app.patch("/events/{event_id}/close")
@limiter.limit("5/minute")
def closeEvent(request: Request, event_id: int, x_admin_key: Optional[str] = Header(None)):
    check_admin(request, x_admin_key)

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_events_tables(cursor)

        cursor.execute("SELECT is_active, metric, brawler_name FROM events WHERE id = %s", (event_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Evento no encontrado")
        if not row[0]:
            raise HTTPException(status_code=400, detail="El evento ya está cerrado")

        metric, brawler_name = row[1], row[2]
        freeze_values(cursor, event_id, metric, brawler_name)

        cursor.execute("""
            UPDATE events SET is_active = FALSE, closed_at = NOW()
            WHERE id = %s
        """, (event_id,))

        conn.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error al cerrar el evento")
    finally:
        cursor.close()
        conn.close()


@app.delete("/events/{event_id}")
@limiter.limit("5/minute")
def deleteEvent(request: Request, event_id: int, x_admin_key: Optional[str] = Header(None)):
    check_admin(request, x_admin_key)

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_events_tables(cursor)
        ensure_raffle_tables(cursor)

        cursor.execute("SELECT 1 FROM events WHERE id = %s", (event_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Evento no encontrado")

        # event_snapshots y event_ticket_rules se borran solos vía ON DELETE CASCADE
        cursor.execute("DELETE FROM events WHERE id = %s", (event_id,))
        conn.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error al eliminar el evento")
    finally:
        cursor.close()
        conn.close()



# Cada mes se sortea un Brawl Pass Plus entre todos los miembros del club. La
# chance de ganar es proporcional a la cantidad de Tickets acumulados en el
# mes en curso. Los tickets NO se guardan como contador que hay que resetear:
# se calculan siempre en vivo a partir de datos que ya existen en la base
# (player_of_day, player_stats_history, resultados de torneos cerrados),
# filtrados por el mes actual (huso horario UY). Esto tiene una ventaja
# grande: el reset mensual es automático y gratis, ya que en cuanto cambia
# el mes la ventana de la consulta vuelve a empezar de cero para todos.
#
# Fuentes de tickets:
#   +10  por cada vez que el jugador fue "jugador del día" durante el mes
#   +1   por cada 100 trofeos netos ganados en el mes (no baja si perdés)
#   +N   según la posición final en cada torneo cerrado durante el mes,
#        donde N lo define el admin al crear el torneo (event_ticket_rules)
#
# El sorteo en sí (elegir un ganador al azar, ponderado por tickets) es lo
# único que SÍ se persiste, porque debe quedar fijo para siempre una vez
# ejecutado. Lo dispara el propio datacollector.py en cada corrida (cada 30
# minutos) llamando a POST /raffle/draw con la clave de admin: el endpoint
# es idempotente por mes, así que no hay riesgo de sortear dos veces.

# Meses excluidos EXPLÍCITAMENTE del sorteo: nunca deben generar una fila
# en raffle_draws, ni siquiera una "vacía" o marcada como excluida — el
# objetivo es que no quede ningún registro de que ese sorteo existió.
# Agosto 2026 fue el mes de testing del sistema (antes de que Sorteos
# estuviera realmente en producción), así que se excluye a mano acá.
# Si en el futuro hace falta excluir otro mes por el mismo motivo, se
# agrega otra fecha a este set.
EXCLUDED_RAFFLE_MONTHS = {datetime(2026, 8, 1).date()}


def ensure_raffle_tables(cursor):
    if "raffle" in _SCHEMA_READY:
        return
    with _schema_lock:
        if "raffle" in _SCHEMA_READY:
            return
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS event_ticket_rules (
                event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
                position INTEGER NOT NULL,
                tickets  INTEGER NOT NULL,
                PRIMARY KEY (event_id, position)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raffle_config (
                id         INTEGER PRIMARY KEY DEFAULT 1,
                prize      TEXT NOT NULL DEFAULT 'Brawl Pass Plus',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT raffle_config_single_row CHECK (id = 1)
            )
        """)
        cursor.execute("""
            INSERT INTO raffle_config (id, prize) VALUES (1, 'Brawl Pass Plus')
            ON CONFLICT (id) DO NOTHING
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raffle_draws (
                id                  SERIAL PRIMARY KEY,
                cycle_month         DATE NOT NULL UNIQUE,
                prize               TEXT NOT NULL,
                winner_tag          TEXT,
                winner_name         TEXT,
                winner_tickets      INTEGER,
                total_tickets       INTEGER NOT NULL DEFAULT 0,
                total_participants  INTEGER NOT NULL DEFAULT 0,
                drawn_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        # Migraciones idempotentes (columnas agregadas después de la primera versión
        # de esta tabla) — permiten resortear un mes sin perder de vista a quién ya
        # se le dio la oportunidad, para nunca repetir ganador dentro del mismo mes.
        cursor.execute("ALTER TABLE raffle_draws ADD COLUMN IF NOT EXISTS excluded_tags TEXT[] NOT NULL DEFAULT '{}'")
        cursor.execute("ALTER TABLE raffle_draws ADD COLUMN IF NOT EXISTS redraw_count INTEGER NOT NULL DEFAULT 0")

        # Tickets diarios reclamados a mano desde la web (2 por día, ver
        # /raffle/claim-daily). El UNIQUE (player_tag, claim_date) es la
        # verdadera barrera anti-doble-reclamo: es una restricción de base de
        # datos, no una validación de aplicación, así que ni una condición de
        # carrera ni un reintento manual pueden burlarla.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_ticket_claims (
                id          SERIAL PRIMARY KEY,
                player_tag  TEXT NOT NULL,
                claim_date  DATE NOT NULL,
                tickets     INTEGER NOT NULL DEFAULT 2,
                claimed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (player_tag, claim_date)
            )
        """)
        cursor.connection.commit()
        _SCHEMA_READY.add("raffle")


def ensure_predictions_table(cursor):
    """
    Predicción diaria de jugador del día. predictor_tag es quien predice,
    target_day es el día que se está prediciendo (siempre "mañana" al
    momento de crearse — nunca se puede predecir un día ya arrancado, ver
    POST /predictions), y predicted_tag es a quién eligió.

    Se resuelve (resolved_at, correct, winner_tag) una vez que target_day
    ya pasó, comparando contra player_of_day. ticket_claimed evita el
    doble cobro de los +2 tickets de una predicción acertada — igual que
    daily_ticket_claims, la restricción real es la UNIQUE de abajo, no una
    validación de aplicación.
    """
    if "predictions" in _SCHEMA_READY:
        return
    with _schema_lock:
        if "predictions" in _SCHEMA_READY:
            return
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_predictions (
                id             SERIAL PRIMARY KEY,
                predictor_tag  TEXT NOT NULL,
                target_day     DATE NOT NULL,
                predicted_tag  TEXT NOT NULL,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                resolved_at    TIMESTAMPTZ,
                correct        BOOLEAN,
                winner_tag     TEXT,
                ticket_claimed BOOLEAN NOT NULL DEFAULT FALSE,
                UNIQUE (predictor_tag, target_day)
            )
        """)
        cursor.connection.commit()
        _SCHEMA_READY.add("predictions")


def uy_today():
    """Fecha de hoy en horario de Uruguay (UTC-3)."""
    from datetime import timedelta
    return (datetime.now(timezone.utc) - timedelta(hours=3)).date()


def uy_next_midnight_utc():
    """Próxima medianoche en horario UY (00:00 UY), expresada en UTC."""
    from datetime import timedelta
    tomorrow_uy = uy_today() + timedelta(days=1)
    return datetime(tomorrow_uy.year, tomorrow_uy.month, tomorrow_uy.day, 3, 0, 0, tzinfo=timezone.utc)


def uy_month_bounds(year: int, month: int):
    """(inicio, fin) del mes en horario UY, expresados en UTC. fin es exclusivo."""
    start = datetime(year, month, 1, 3, 0, 0, tzinfo=timezone.utc)  # 00:00 UY = 03:00 UTC
    if month == 12:
        end = datetime(year + 1, 1, 1, 3, 0, 0, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, 3, 0, 0, tzinfo=timezone.utc)
    return start, end


PREDICTION_CORRECT_TICKETS = 2


def resolve_predictions(cursor, today):
    """
    Resuelve toda predicción cuyo target_day ya haya terminado (< today) y
    todavía no esté resuelta: compara predicted_tag contra el ganador real
    en player_of_day para ese día. Si ese día no tiene fila en player_of_day
    (nadie tuvo actividad — caso raro pero posible), se resuelve igual como
    incorrecta, con winner_tag NULL, para que la predicción no quede
    pendiente para siempre.

    Es idempotente y liviana (solo toca filas con resolved_at IS NULL), así
    que se puede llamar tanto desde el cron del collector como, a modo de
    red de seguridad, desde la propia API antes de responder — el mismo
    patrón que ya se usa para jugador del día (_compute_and_save_player_of_day
    como fallback de lo que hace el collector cada 30 min).
    """
    cursor.execute("""
        UPDATE player_predictions pp
        SET resolved_at = NOW(),
            winner_tag  = pod.player_tag,
            correct     = (pp.predicted_tag = pod.player_tag)
        FROM player_of_day pod
        WHERE pp.target_day = pod.day
          AND pp.resolved_at IS NULL
          AND pp.target_day < %s
    """, (today,))

    cursor.execute("""
        UPDATE player_predictions pp
        SET resolved_at = NOW(),
            winner_tag  = NULL,
            correct     = FALSE
        WHERE pp.resolved_at IS NULL
          AND pp.target_day < %s
          AND NOT EXISTS (SELECT 1 FROM player_of_day pod WHERE pod.day = pp.target_day)
    """, (today,))


def compute_raffle_tickets(cursor, month_start: datetime, month_end: datetime):
    """
    Devuelve la lista de TODOS los jugadores del club con su desglose de
    tickets para la ventana [month_start, month_end), ordenada de mayor a
    menor. Cada fuente se recalcula en vivo (ver comentario de sección).
    """
    # 1) +10 por cada día en que el jugador fue "jugador del día" este mes
    cursor.execute("""
        SELECT player_tag, COUNT(*) * 10
        FROM player_of_day
        WHERE day >= %s AND day < %s
        GROUP BY player_tag
    """, (month_start.date(), month_end.date()))
    potd_map = dict(cursor.fetchall())

    # 2) +1 por cada 100 trofeos netos ganados este mes (mismo patrón que el
    #    cálculo de "jugador del día", pero con ventana mensual en vez de diaria).
    #    Esta consulta tenía el mismo bug de raíz que ya se corrigió en jugador
    #    del día, y acá era todavía peor: no existía ni siquiera el fallback a
    #    "último valor antes de que arrancara la ventana" (prev_last). Si un
    #    jugador tuvo un solo cambio de trofeos en TODO el mes (algo común,
    #    dado que el historial solo guarda una fila cuando el valor cambia),
    #    month_first == month_last (misma fila) y el cálculo daba "gained = 0"
    #    aunque el jugador viniera de mucho más abajo desde el mes anterior.
    #    Se agrega prev_last (último snapshot ANTES de que arrancara el mes) y
    #    se usa como base preferida, igual que en jugador del día.
    cursor.execute("""
        WITH ranked AS (
            SELECT player_tag, trophies, timestamp,
                   ROW_NUMBER() OVER (PARTITION BY player_tag ORDER BY timestamp ASC)  AS rn_asc,
                   ROW_NUMBER() OVER (PARTITION BY player_tag ORDER BY timestamp DESC) AS rn_desc
            FROM player_stats_history
            WHERE timestamp >= %s AND timestamp < %s
        ),
        month_first AS (SELECT player_tag, trophies FROM ranked WHERE rn_asc = 1),
        month_last  AS (SELECT player_tag, trophies FROM ranked WHERE rn_desc = 1),
        prev_last AS (
            SELECT DISTINCT ON (player_tag) player_tag, trophies
            FROM player_stats_history
            WHERE timestamp < %s
            ORDER BY player_tag, timestamp DESC
        )
        SELECT
            ml.player_tag,
            GREATEST(0, COALESCE(ml.trophies, pv.trophies) - COALESCE(pv.trophies, mf.trophies, 0)) AS gained
        FROM month_last ml
        LEFT JOIN month_first mf USING (player_tag)
        LEFT JOIN prev_last  pv USING (player_tag)
    """, (month_start, month_end, month_start))
    trophies_map = {tag: gained // 100 for tag, gained in cursor.fetchall()}

    # 3) tickets según posición final en torneos cerrados durante el mes
    cursor.execute("""
        SELECT id, metric, brawler_name FROM events
        WHERE is_active = FALSE AND closed_at >= %s AND closed_at < %s
    """, (month_start, month_end))
    events_map = {}
    for (eid, metric, brawler_name) in cursor.fetchall():
        cursor.execute("SELECT position, tickets FROM event_ticket_rules WHERE event_id = %s", (eid,))
        rules = dict(cursor.fetchall())
        if not rules:
            continue
        for r in compute_results(cursor, eid, metric, brawler_name):
            reward = rules.get(r["rank"])
            if reward:
                events_map[r["tag"]] = events_map.get(r["tag"], 0) + reward

    # 4) Tickets diarios reclamados a mano en la web (2 por día reclamado este mes)
    cursor.execute("""
        SELECT player_tag, SUM(tickets)
        FROM daily_ticket_claims
        WHERE claim_date >= %s AND claim_date < %s
        GROUP BY player_tag
    """, (month_start.date(), month_end.date()))
    daily_map = dict(cursor.fetchall())

    # 5) +2 tickets por cada predicción de jugador del día acertada y
    #    reclamada este mes (ver POST /predictions/claim). Solo cuentan las
    #    ya reclamadas por el usuario — igual que el resto de las fuentes,
    #    que reflejan acciones ya confirmadas, no aciertos pendientes de
    #    reclamar.
    cursor.execute("""
        SELECT predictor_tag, COUNT(*) * %s
        FROM player_predictions
        WHERE ticket_claimed = TRUE
          AND target_day >= %s AND target_day < %s
        GROUP BY predictor_tag
    """, (PREDICTION_CORRECT_TICKETS, month_start.date(), month_end.date()))
    prediction_map = dict(cursor.fetchall())

    # 6) combinar con la lista completa de jugadores actuales del club.
    # Las cuentas baneadas quedan totalmente afuera del sorteo (ranking Y
    # elegibilidad) mientras dure el baneo.
    #
    # Nota sobre cuentas con tag ajena a los 4 clubes: el resultado de esta
    # función se arma iterando `players` (más abajo), tabla que
    # datacollector.py llena únicamente con los miembros reales de los 4
    # clubes. Una cuenta cuya tag no pertenezca a ninguno jamás va a tener
    # fila en `players`, así que ni aparece en el ranking ni puede ser
    # elegida en /raffle/draw (que sortea sobre este mismo resultado) —
    # aunque hubiera acumulado tickets en otras tablas (daily_ticket_claims,
    # player_predictions), esos valores simplemente nunca se leen para un
    # tag que no está en este loop. No hace falta un chequeo aparte acá.
    #
    # Registro en la página: solo puede GANAR un jugador cuya tag esté en la
    # tabla `users` (y no baneado). Acá solo se marca cada fila con
    # "registered"; el filtro que realmente impide ganar está en
    # /raffle/draw y /admin/raffle/redraw. El ranking sigue listando a todos
    # los jugadores del club (con sus tickets) — así el frontend puede
    # mostrar quién todavía no se registró.
    # Las tags se comparan en forma canónica (O -> 0, ver canonical_tag) para
    # que un usuario registrado con la letra O no quede afuera por error —
    # y, por el mismo motivo, para que un baneo tampoco se pueda esquivar así.
    cursor.execute("SELECT player_tag, banned FROM users")
    banned_tags, registered_tags = set(), set()
    for (user_tag, is_banned) in cursor.fetchall():
        (banned_tags if is_banned else registered_tags).add(canonical_tag(user_tag))
    registered_tags -= banned_tags

    cursor.execute("SELECT tag, name, icon_url, club_name FROM players")
    result = []
    for (tag, name, icon_url, club_name) in cursor.fetchall():
        ctag = canonical_tag(tag)
        if ctag in banned_tags:
            continue
        potd = potd_map.get(tag, 0)
        troph = trophies_map.get(tag, 0)
        ev = events_map.get(tag, 0)
        daily = daily_map.get(tag, 0)
        pred = prediction_map.get(tag, 0)
        result.append({
            "tag": tag,
            "name": name,
            "icon_url": icon_url,
            "club_name": club_name,
            "registered": ctag in registered_tags,
            "tickets_player_of_day": potd,
            "tickets_trophies": troph,
            "tickets_events": ev,
            "tickets_daily_claim": daily,
            "tickets_predictions": pred,
            "tickets_total": potd + troph + ev + daily + pred,
        })

    result.sort(key=lambda r: (-r["tickets_total"], r["name"] or ""))
    for i, r in enumerate(result):
        r["rank"] = i + 1
    return result


@app.get("/raffle")
@limiter.limit("20/minute")
def getRaffle(request: Request):
    conn = get_conn()
    cursor = conn.cursor()
    from datetime import timedelta
    try:
        ensure_raffle_tables(cursor)

        now_uy = datetime.now(timezone.utc) - timedelta(hours=3)
        month_start, month_end = uy_month_bounds(now_uy.year, now_uy.month)
        ranking = compute_raffle_tickets(cursor, month_start, month_end)

        cursor.execute("SELECT prize, updated_at FROM raffle_config WHERE id = 1")
        row = cursor.fetchone()
        prize, prize_updated_at = (row[0], row[1]) if row else ("Brawl Pass Plus", None)

        cursor.execute("""
            SELECT cycle_month, prize, winner_tag, winner_name, winner_tickets,
                   total_tickets, total_participants, drawn_at
            FROM raffle_draws ORDER BY cycle_month DESC LIMIT 6
        """)
        last_draws = [{
            "cycle_month": cm.isoformat(),
            "prize": pz,
            "winner_tag": wt,
            "winner_name": wn,
            "winner_tickets": wtk,
            "total_tickets": tt,
            "total_participants": tp,
            "drawn_at": da.isoformat() if da else None,
        } for (cm, pz, wt, wn, wtk, tt, tp, da) in cursor.fetchall()]

        return {
            "cycle_month": month_start.date().isoformat(),
            "draw_at": month_end.isoformat(),
            "prize": prize,
            "ranking": ranking,
            "last_draws": last_draws,
        }
    finally:
        cursor.close()
        conn.close()


# ── TICKETS DIARIOS ───────────────────────────────────────────────────────
# 2 Tickets por día, reclamables una vez por día calendario UY, exclusivos
# para cuentas registradas y vinculadas a un jugador actual del club. La
# barrera real contra el doble reclamo es la restricción UNIQUE de la base
# de datos (player_tag, claim_date) en daily_ticket_claims — no una
# comprobación de aplicación — así que ni una doble request simultánea, ni
# un reintento manual, ni manipular el estado del navegador puede burlarla:
# Postgres rechaza el segundo INSERT del mismo día pase lo que pase.
DAILY_CLAIM_TICKETS = 2


@app.get("/raffle/claim-daily")
@limiter.limit("30/minute")
def getClaimDailyStatus(request: Request, user=Depends(get_current_user)):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_raffle_tables(cursor)

        today = uy_today()
        cursor.execute("""
            SELECT 1 FROM daily_ticket_claims WHERE player_tag = %s AND claim_date = %s
        """, (user["tag"], today))
        claimed = cursor.fetchone() is not None

        # No bloquea nada acá (eso lo sigue haciendo POST /raffle/claim-daily
        # contra la base, fresco, en cada intento) — es solo para que el
        # frontend pueda mostrar el botón deshabilitado en vez de dejar que
        # el usuario reciba un 403 recién al tocarlo.
        cursor.execute("SELECT 1 FROM players WHERE tag = %s", (user["tag"],))
        eligible = cursor.fetchone() is not None

        return {
            "claimed_today": claimed,
            "eligible": eligible,
            "tickets": DAILY_CLAIM_TICKETS,
            "next_claim_at": uy_next_midnight_utc().isoformat(),
        }
    finally:
        cursor.close()
        conn.close()


@app.post("/raffle/claim-daily")
@limiter.limit("10/minute")
def claimDaily(request: Request, user=Depends(get_current_user)):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_raffle_tables(cursor)
        ensure_users_table(cursor)

        # El estado de baneo y de pertenencia al club se verifica siempre
        # fresco contra la base — nunca confiamos en el contenido del JWT
        # para esto, porque el token pudo emitirse antes de un baneo.
        cursor.execute("SELECT banned FROM users WHERE player_tag = %s", (user["tag"],))
        row = cursor.fetchone()
        if row and row[0]:
            raise HTTPException(status_code=403, detail="Tu cuenta está suspendida.")

        cursor.execute("SELECT 1 FROM players WHERE tag = %s", (user["tag"],))
        if not cursor.fetchone():
            raise HTTPException(
                status_code=403,
                detail="Tu tag no corresponde a un jugador activo del club en este momento."
            )

        today = uy_today()
        try:
            cursor.execute("""
                INSERT INTO daily_ticket_claims (player_tag, claim_date, tickets)
                VALUES (%s, %s, %s)
            """, (user["tag"], today, DAILY_CLAIM_TICKETS))
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=409, detail="Ya reclamaste tus Tickets de hoy.")

        return {
            "ok": True,
            "tickets": DAILY_CLAIM_TICKETS,
            "next_claim_at": uy_next_midnight_utc().isoformat(),
        }
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo reclamar el Ticket diario.")
    finally:
        cursor.close()
        conn.close()


class RaffleConfigBody(BaseModel):
    prize: str = Field(..., max_length=300)


# ── PREDICCIÓN DE JUGADOR DEL DÍA ────────────────────────────────────────────
# Cada día se puede predecir quién va a ganar jugador del día de MAÑANA.
# target_day siempre se calcula server-side como "hoy + 1" — nunca lo manda
# el cliente — así que no hay forma de tocar una predicción una vez que el
# día que se estaba prediciendo ya arrancó: la próxima llamada a POST
# /predictions solo puede afectar al nuevo "mañana". Acertar paga +2 tickets
# para el sorteo, reclamables al día siguiente una vez resuelto (ver
# resolve_predictions).

class PredictionBody(BaseModel):
    tag: str


@app.get("/predictions/today")
@limiter.limit("30/minute")
def getPredictionsToday(request: Request, user=Depends(get_current_user)):
    """
    Devuelve todo lo que necesita el widget de predicciones:
      - tomorrow: la predicción editable para mañana (o vacía si no eligió)
      - today: tracking en vivo de la predicción hecha ayer para HOY
        (líder actual, puntos del jugador predicho, puntos propios)
      - yesterday_result: resultado ya resuelto de la predicción de ayer
        (a quién predijo, quién ganó realmente, si acertó, si ya reclamó)
      - pending_claims: total de aciertos sin reclamar (por si se acumuló
        más de uno)
    """
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_predictions_table(cursor)
        ensure_player_of_day_table(cursor)

        from datetime import timedelta
        today = uy_today()
        tomorrow = today + timedelta(days=1)
        yesterday = today - timedelta(days=1)
        predictor_tag = user["tag"]

        # Red de seguridad: resuelve cualquier predicción vencida por si el
        # collector todavía no corrió desde la medianoche.
        resolve_predictions(cursor, today)
        conn.commit()

        # ── Predicción para mañana (editable hasta las 00:00 UY) ────────
        cursor.execute("""
            SELECT pp.predicted_tag, p.name, p.icon_url
            FROM player_predictions pp
            LEFT JOIN players p ON p.tag = pp.predicted_tag
            WHERE pp.predictor_tag = %s AND pp.target_day = %s
        """, (predictor_tag, tomorrow))
        row = cursor.fetchone()
        tomorrow_block = {
            "target_day": tomorrow.isoformat(),
            "predicted_tag": row[0] if row else None,
            "predicted_name": row[1] if row else None,
            "predicted_icon_url": row[2] if row else None,
            "lock_at": uy_next_midnight_utc().isoformat(),
        }

        # ── Tracking en vivo de la predicción de HOY (hecha ayer) ───────
        cursor.execute("""
            SELECT pp.predicted_tag, p.name, p.icon_url
            FROM player_predictions pp
            LEFT JOIN players p ON p.tag = pp.predicted_tag
            WHERE pp.predictor_tag = %s AND pp.target_day = %s
        """, (predictor_tag, today))
        row = cursor.fetchone()
        today_block = None
        if row:
            predicted_tag, predicted_name, predicted_icon = row
            live_ranking = get_today_live_ranking(cursor, today)
            by_tag = {r["player_tag"]: r for r in live_ranking}
            leader = live_ranking[0] if live_ranking else None
            today_block = {
                "target_day": today.isoformat(),
                "predicted_tag": predicted_tag,
                "predicted_name": predicted_name,
                "predicted_icon_url": predicted_icon,
                "predicted_points": by_tag.get(predicted_tag, {}).get("points", 0),
                "leader_tag": leader["player_tag"] if leader else None,
                "leader_name": leader["player_name"] if leader else None,
                "leader_icon_url": leader["icon_url"] if leader else None,
                "leader_points": leader["points"] if leader else 0,
                "own_points": by_tag.get(predictor_tag, {}).get("points", 0),
            }

        # ── Resultado de la predicción de AYER (ya resuelta) ────────────
        cursor.execute("""
            SELECT pp.predicted_tag, pred_p.name, pred_p.icon_url,
                   pp.winner_tag, win_p.name, win_p.icon_url,
                   pp.correct, pp.ticket_claimed
            FROM player_predictions pp
            LEFT JOIN players pred_p ON pred_p.tag = pp.predicted_tag
            LEFT JOIN players win_p  ON win_p.tag  = pp.winner_tag
            WHERE pp.predictor_tag = %s AND pp.target_day = %s AND pp.resolved_at IS NOT NULL
        """, (predictor_tag, yesterday))
        row = cursor.fetchone()
        yesterday_block = None
        if row:
            (p_tag, p_name, p_icon, w_tag, w_name, w_icon, correct, claimed) = row
            yesterday_block = {
                "target_day": yesterday.isoformat(),
                "predicted_tag": p_tag,
                "predicted_name": p_name,
                "predicted_icon_url": p_icon,
                "winner_tag": w_tag,
                "winner_name": w_name,
                "winner_icon_url": w_icon,
                "correct": bool(correct),
                "ticket_claimed": bool(claimed),
            }

        cursor.execute("""
            SELECT COUNT(*) FROM player_predictions
            WHERE predictor_tag = %s AND correct = TRUE AND ticket_claimed = FALSE
        """, (predictor_tag,))
        pending_claims = cursor.fetchone()[0]

        # Mismo propósito que en GET /raffle/claim-daily: no bloquea nada acá
        # (POST /predictions y POST /predictions/claim vuelven a chequear
        # esto fresco contra la base), solo le permite al frontend deshabilitar
        # el widget de predicciones para cuentas no-miembro sin esperar a un 403.
        cursor.execute("SELECT 1 FROM players WHERE tag = %s", (predictor_tag,))
        eligible = cursor.fetchone() is not None

        return {
            "eligible": eligible,
            "tomorrow": tomorrow_block,
            "today": today_block,
            "yesterday_result": yesterday_block,
            "pending_claims": pending_claims,
        }
    finally:
        cursor.close()
        conn.close()


@app.post("/predictions")
@limiter.limit("10/minute")
def submitPrediction(request: Request, body: PredictionBody, user=Depends(get_current_user)):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_predictions_table(cursor)
        ensure_users_table(cursor)

        cursor.execute("SELECT banned FROM users WHERE player_tag = %s", (user["tag"],))
        row = cursor.fetchone()
        if row and row[0]:
            raise HTTPException(status_code=403, detail="Tu cuenta está suspendida.")

        # Gap: acá se validaba que el jugador PREDICHO (predicted_tag, más
        # abajo) fuera miembro de un club, pero nunca que quien predice
        # (user["tag"]) también lo sea. Una cuenta creada con una tag ajena
        # a los 4 clubes podía predecir sin problema — solo no podía elegir
        # a otro no-miembro como resultado. Mismo criterio y mismo mensaje
        # que ya usa POST /raffle/claim-daily para este caso.
        cursor.execute("SELECT 1 FROM players WHERE tag = %s", (user["tag"],))
        if not cursor.fetchone():
            raise HTTPException(
                status_code=403,
                detail="Tu tag no corresponde a un jugador activo del club en este momento."
            )

        predicted_tag = normalize_tag(body.tag)
        if not TAG_RE.match(predicted_tag):
            raise HTTPException(status_code=400, detail="Tag inválida.")

        cursor.execute("SELECT 1 FROM players WHERE tag = %s", (predicted_tag,))
        if not cursor.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Ese jugador no está activo en ninguno de los 4 clubes en este momento."
            )

        from datetime import timedelta
        tomorrow = uy_today() + timedelta(days=1)

        cursor.execute("""
            INSERT INTO player_predictions (predictor_tag, target_day, predicted_tag, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (predictor_tag, target_day) DO UPDATE SET
                predicted_tag = EXCLUDED.predicted_tag,
                updated_at    = NOW()
        """, (user["tag"], tomorrow, predicted_tag))
        conn.commit()

        return {"ok": True, "target_day": tomorrow.isoformat(), "predicted_tag": predicted_tag}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo guardar tu predicción.")
    finally:
        cursor.close()
        conn.close()


@app.post("/predictions/claim")
@limiter.limit("10/minute")
def claimPredictionTickets(request: Request, user=Depends(get_current_user)):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_predictions_table(cursor)
        ensure_users_table(cursor)

        cursor.execute("SELECT banned FROM users WHERE player_tag = %s", (user["tag"],))
        row = cursor.fetchone()
        if row and row[0]:
            raise HTTPException(status_code=403, detail="Tu cuenta está suspendida.")

        # Defensa en profundidad: con el fix de POST /predictions una cuenta
        # no-miembro ya no puede generar predicciones nuevas, pero esto cubre
        # además al que predijo siendo miembro y para cuando llega a reclamar
        # ya dejó el club (su fila en `players` se borra a las 24hs de irse,
        # ver datacollector.py). Mismo criterio que el resto de las acciones.
        cursor.execute("SELECT 1 FROM players WHERE tag = %s", (user["tag"],))
        if not cursor.fetchone():
            raise HTTPException(
                status_code=403,
                detail="Tu tag no corresponde a un jugador activo del club en este momento."
            )

        today = uy_today()
        resolve_predictions(cursor, today)

        # Reclama TODOS los aciertos pendientes de una sola vez (no solo el
        # de ayer) para que no se pierdan tickets si el usuario se salteó
        # algún día sin entrar al sitio. La restricción real contra el
        # doble cobro es el propio WHERE ticket_claimed = FALSE bajo el
        # bloqueo de fila de Postgres — no una validación de aplicación.
        cursor.execute("""
            UPDATE player_predictions
            SET ticket_claimed = TRUE
            WHERE predictor_tag = %s AND correct = TRUE AND ticket_claimed = FALSE
            RETURNING id
        """, (user["tag"],))
        claimed_ids = cursor.fetchall()
        conn.commit()

        count = len(claimed_ids)
        if count == 0:
            raise HTTPException(
                status_code=409,
                detail="No tenés predicciones acertadas pendientes de reclamar."
            )

        return {
            "ok": True,
            "predictions_claimed": count,
            "tickets": count * PREDICTION_CORRECT_TICKETS,
        }
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudieron reclamar los tickets.")
    finally:
        cursor.close()
        conn.close()


@app.patch("/raffle/config")
@limiter.limit("5/minute")
def updateRaffleConfig(request: Request, body: RaffleConfigBody, x_admin_key: Optional[str] = Header(None)):
    check_admin(request, x_admin_key)
    prize = (body.prize or "").strip()
    if not prize:
        raise HTTPException(status_code=400, detail="El premio no puede estar vacío.")
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_raffle_tables(cursor)
        cursor.execute("UPDATE raffle_config SET prize = %s, updated_at = NOW() WHERE id = 1", (prize,))
        conn.commit()
        return {"ok": True}
    finally:
        cursor.close()
        conn.close()


def pick_weighted_winner(eligible):
    """Elige un ganador al azar, ponderado por tickets. eligible ya debe venir
    filtrado (tickets_total > 0, registrado en la página, excluidos los que
    correspondan)."""
    if not eligible:
        return None
    rng = random.SystemRandom()
    weights = [r["tickets_total"] for r in eligible]
    return rng.choices(eligible, weights=weights, k=1)[0]


@app.post("/raffle/draw")
@limiter.limit("5/minute")
def drawRaffle(request: Request, x_admin_key: Optional[str] = Header(None)):
    """
    Sortea el ganador del ÚLTIMO mes ya finalizado (el anterior al actual),
    si todavía no fue sorteado. Es seguro llamarlo repetidamente: si ese mes
    ya tiene un sorteo registrado, no hace nada. Lo llama el datacollector
    en cada corrida automática; también sirve como botón de emergencia para
    el admin si por algún motivo el sorteo automático no se disparó.
    """
    check_admin(request, x_admin_key)
    from datetime import date as date_cls, timedelta

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_raffle_tables(cursor)

        now_uy = datetime.now(timezone.utc) - timedelta(hours=3)
        if now_uy.month == 1:
            prev_year, prev_month = now_uy.year - 1, 12
        else:
            prev_year, prev_month = now_uy.year, now_uy.month - 1
        cycle_date = date_cls(prev_year, prev_month, 1)

        if cycle_date in EXCLUDED_RAFFLE_MONTHS:
            # No se toca la base de datos para este mes: ni se lee ni se
            # escribe nada en raffle_draws, para que no quede ningún rastro
            # de que este ciclo existió.
            return {"ok": True, "skipped": True, "reason": "Mes excluido del sorteo."}

        cursor.execute("SELECT 1 FROM raffle_draws WHERE cycle_month = %s", (cycle_date,))
        if cursor.fetchone():
            return {"ok": True, "skipped": True, "reason": "Ese mes ya fue sorteado."}

        month_start, month_end = uy_month_bounds(prev_year, prev_month)
        ranking = compute_raffle_tickets(cursor, month_start, month_end)
        # Solo participan del sorteo los jugadores registrados en la página.
        eligible = [r for r in ranking if r["tickets_total"] > 0 and r["registered"]]

        cursor.execute("SELECT prize FROM raffle_config WHERE id = 1")
        row = cursor.fetchone()
        prize = row[0] if row else "Brawl Pass Plus"

        winner = pick_weighted_winner(eligible)

        cursor.execute("""
            INSERT INTO raffle_draws
                (cycle_month, prize, winner_tag, winner_name, winner_tickets,
                 total_tickets, total_participants)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cycle_month) DO NOTHING
            RETURNING id
        """, (
            cycle_date, prize,
            winner["tag"] if winner else None,
            winner["name"] if winner else None,
            winner["tickets_total"] if winner else None,
            sum(r["tickets_total"] for r in eligible),
            len(eligible),
        ))
        inserted = cursor.fetchone()
        conn.commit()

        if not inserted:
            # Otra request ganó la carrera y sorteó este mes justo antes que nosotros.
            return {"ok": True, "skipped": True, "reason": "Ese mes ya fue sorteado."}

        return {"ok": True, "skipped": False, "cycle_month": cycle_date.isoformat(), "winner": winner}
    finally:
        cursor.close()
        conn.close()


class RedrawBody(BaseModel):
    cycle_month: str  # "YYYY-MM-01"


@app.post("/admin/raffle/redraw")
@limiter.limit("10/minute")
def redrawRaffle(request: Request, body: RedrawBody, x_admin_key: Optional[str] = Header(None)):
    """
    Vuelve a sortear un mes YA sorteado, excluyendo a todos los ganadores
    anteriores de ese mes (si esta es la segunda vez que se re-sortea porque
    el primer re-sorteo tampoco reclamó el premio, ese también queda afuera).
    Útil cuando el ganador no reclama el Brawl Pass Plus en un tiempo
    razonable. Usa los mismos tickets ya calculados para ese mes — el pool
    de participantes no cambia, solo se vuelve a tirar el dado excluyendo a
    quienes ya tuvieron su oportunidad.
    """
    check_admin(request, x_admin_key)
    from datetime import date as date_cls

    try:
        cycle_date = date_cls.fromisoformat(body.cycle_month)
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de mes inválido, usá YYYY-MM-01")

    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_raffle_tables(cursor)

        cursor.execute("""
            SELECT winner_tag, excluded_tags, prize, redraw_count
            FROM raffle_draws WHERE cycle_month = %s
        """, (cycle_date,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Ese mes no tiene un sorteo registrado todavía.")
        prev_winner, excluded_tags, prize, redraw_count = row

        new_excluded = set(excluded_tags or [])
        if prev_winner:
            new_excluded.add(prev_winner)

        month_start, month_end = uy_month_bounds(cycle_date.year, cycle_date.month)
        ranking = compute_raffle_tickets(cursor, month_start, month_end)
        eligible = [
            r for r in ranking
            if r["tickets_total"] > 0
            and r["registered"]
            and r["tag"] not in new_excluded
        ]

        winner = pick_weighted_winner(eligible)

        cursor.execute("""
            UPDATE raffle_draws SET
                winner_tag = %s, winner_name = %s, winner_tickets = %s,
                total_tickets = %s, total_participants = %s,
                excluded_tags = %s, redraw_count = redraw_count + 1, drawn_at = NOW()
            WHERE cycle_month = %s
        """, (
            winner["tag"] if winner else None,
            winner["name"] if winner else None,
            winner["tickets_total"] if winner else None,
            sum(r["tickets_total"] for r in eligible),
            len(eligible),
            list(new_excluded),
            cycle_date,
        ))
        conn.commit()

        return {
            "ok": True,
            "cycle_month": cycle_date.isoformat(),
            "winner": winner,
            "excluded_tags": list(new_excluded),
            "redraw_count": redraw_count + 1,
        }
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="No se pudo re-sortear ese mes.")
    finally:
        cursor.close()
        conn.close()


# ── ANÁLISIS: RANKING DE CLUBES (Uruguay / Mundo) ────────────────────────────
# Guarda una foto del ranking OFICIAL de clubes de Supercell (top 200 de
# Uruguay y top 200 del Mundo). Se pisa por completo en cada corrida del
# datacollector (no nos interesa el historial, solo la posición actual).
def ensure_club_rankings_table(cursor):
    if "club_rankings" in _SCHEMA_READY:
        return
    with _schema_lock:
        if "club_rankings" in _SCHEMA_READY:
            return
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS club_rankings (
                region       TEXT NOT NULL,
                rank         INTEGER NOT NULL,
                club_tag     TEXT NOT NULL,
                club_name    TEXT NOT NULL,
                trophies     INTEGER NOT NULL,
                member_count INTEGER,
                fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (region, club_tag)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_club_rankings_region_rank
            ON club_rankings(region, rank)
        """)
        cursor.connection.commit()
        _SCHEMA_READY.add("club_rankings")


@app.get("/club-rankings")
@limiter.limit("20/minute")
def getClubRankings(request: Request):
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_club_rankings_table(cursor)

        result = {}
        for region in ("UY", "global"):
            cursor.execute("""
                SELECT rank, club_tag, club_name, trophies, member_count
                FROM club_rankings
                WHERE region = %s
                ORDER BY rank ASC
            """, (region,))
            result[region] = [
                {"rank": r, "tag": tag, "name": name, "trophies": tr, "member_count": mc}
                for (r, tag, name, tr, mc) in cursor.fetchall()
            ]

        cursor.execute("SELECT MAX(fetched_at) FROM club_rankings")
        row = cursor.fetchone()

        return {
            "uy": result["UY"],
            "global": result["global"],
            "last_updated": row[0].isoformat() if row and row[0] else None,
        }
    finally:
        cursor.close()
        conn.close()


# ── STATUS ────────────────────────────────────────────────────────────────────

@app.get("/status")
@limiter.limit("10/minute")
def getStatus(request: Request):
    """Devuelve el timestamp del último dato guardado en la DB."""
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT MAX(timestamp) FROM player_stats_history
        """)
        row = cursor.fetchone()
        last_updated = row[0].isoformat() if row and row[0] else None
        return {"last_updated": last_updated}
    finally:
        cursor.close()
        conn.close()


# ── JUGADOR DEL DÍA ───────────────────────────────────────────────────────────
# Sistema de puntos:
#   1 trofeo ganado      = 1 punto
#   1 victoria (3v3/solo) = 4 puntos
#   1 prestige subido    = 80 puntos
#
# Se compara el primer snapshot del día vs el último snapshot del día anterior
# (o el último snapshot disponible antes del día) para calcular el delta.
# Se guarda el resultado en la tabla player_of_day para historial de 7 días.

def ensure_player_of_day_table(cursor):
    if "player_of_day" in _SCHEMA_READY:
        return
    with _schema_lock:
        if "player_of_day" in _SCHEMA_READY:
            return
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_of_day (
                day         DATE PRIMARY KEY,
                player_tag  TEXT NOT NULL,
                player_name TEXT NOT NULL,
                icon_url    TEXT,
                club_name   TEXT,
                points      INTEGER NOT NULL,
                delta_trophies  INTEGER NOT NULL DEFAULT 0,
                delta_wins3v3   INTEGER NOT NULL DEFAULT 0,
                delta_winsSolo  INTEGER NOT NULL DEFAULT 0,
                delta_prestige  INTEGER NOT NULL DEFAULT 0,
                computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cursor.connection.commit()
        _SCHEMA_READY.add("player_of_day")


def get_today_live_ranking(cursor, today):
    """
    Ranking en vivo del día `today` (deltas contra el último valor antes de
    las 00:00 UY), el mismo cálculo que usa /player-of-day. Factorizado acá
    para que el widget de predicciones pueda mostrar "puntos de hoy" del
    jugador predicho y del propio usuario sin duplicar por tercera vez esta
    consulta (ya delicada — ver el fix del bug de COALESCE más arriba).
    """
    from datetime import timedelta
    day_start = datetime(today.year, today.month, today.day,
                         3, 0, 0, tzinfo=timezone.utc)
    day_end   = day_start + timedelta(hours=24)

    cursor.execute("""
        WITH ranked AS (
            SELECT player_tag, trophies, wins3v3, winssolo, total_prestige, timestamp,
                   ROW_NUMBER() OVER (PARTITION BY player_tag ORDER BY timestamp ASC)  AS rn_asc,
                   ROW_NUMBER() OVER (PARTITION BY player_tag ORDER BY timestamp DESC) AS rn_desc
            FROM player_stats_history
            WHERE timestamp >= %s AND timestamp < %s
        ),
        day_first AS (SELECT player_tag, trophies, wins3v3, winssolo, total_prestige FROM ranked WHERE rn_asc  = 1),
        day_last  AS (SELECT player_tag, trophies, wins3v3, winssolo, total_prestige FROM ranked WHERE rn_desc = 1),
        prev_last AS (
            SELECT DISTINCT ON (player_tag) player_tag, trophies, wins3v3, winssolo, total_prestige
            FROM player_stats_history WHERE timestamp < %s
            ORDER BY player_tag, timestamp DESC
        )
        SELECT
            dl.player_tag,
            p.name, p.icon_url, p.club_name,
            -- BASE del día = último valor ANTES de las 00:00 UY (pv), no
            -- el primer cambio registrado DENTRO del día (df). El
            -- historial solo guarda una fila cuando el valor cambia, así
            -- que "df" puede ser la única fila de hoy y coincidir con
            -- "dl" -> delta 0 aunque el jugador sí progresó. pv siempre
            -- representa el valor real a las 00:00 UY.
            GREATEST(0, COALESCE(dl.trophies,       pv.trophies)       - COALESCE(pv.trophies,       df.trophies,       0)) AS dt,
            GREATEST(0, COALESCE(dl.wins3v3,        pv.wins3v3)        - COALESCE(pv.wins3v3,        df.wins3v3,        0)) AS dw3,
            GREATEST(0, COALESCE(dl.winssolo,       pv.winssolo)       - COALESCE(pv.winssolo,       df.winssolo,       0)) AS dws,
            GREATEST(0, COALESCE(dl.total_prestige, pv.total_prestige) - COALESCE(pv.total_prestige, df.total_prestige, 0)) AS dp
        FROM day_last dl
        LEFT JOIN day_first df USING (player_tag)
        LEFT JOIN prev_last  pv USING (player_tag)
        JOIN players p ON p.tag = dl.player_tag
        ORDER BY (
            GREATEST(0, COALESCE(dl.trophies,       pv.trophies)       - COALESCE(pv.trophies,       df.trophies,       0)) * 1 +
            GREATEST(0, COALESCE(dl.wins3v3,        pv.wins3v3)        - COALESCE(pv.wins3v3,        df.wins3v3,        0)) * 4 +
            GREATEST(0, COALESCE(dl.winssolo,       pv.winssolo)       - COALESCE(pv.winssolo,       df.winssolo,       0)) * 4 +
            GREATEST(0, COALESCE(dl.total_prestige, pv.total_prestige) - COALESCE(pv.total_prestige, df.total_prestige, 0)) * 80
        ) DESC, dl.player_tag ASC
    """, (day_start, day_end, day_start))

    today_rows = cursor.fetchall()
    today_ranking = []
    for i, (tag, name, icon_url, club_name, dt, dw3, dws, dp) in enumerate(today_rows):
        points = dt * 1 + (dw3 + dws) * 4 + dp * 80
        today_ranking.append({
            "rank":           i + 1,
            "player_tag":     tag,
            "player_name":    name,
            "icon_url":       icon_url,
            "club_name":      club_name,
            "points":         points,
            "delta_trophies": dt,
            "delta_wins3v3":  dw3,
            "delta_winsSolo": dws,
            "delta_prestige": dp,
        })
    return today_ranking


@app.get("/player-of-day/winners")
@limiter.limit("20/minute")
def getPlayerOfDayWinners(request: Request):
    """
    Top 20 jugadores que más veces fueron jugador del día,
    ordenados por cantidad de victorias descendente.
    """
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_player_of_day_table(cursor)
        cursor.execute("""
            SELECT
                p.tag,
                pod.player_name,
                p.icon_url,
                p.club_name,
                COUNT(*) AS wins
            FROM player_of_day pod
            JOIN players p ON p.tag = pod.player_tag
            GROUP BY p.tag, pod.player_name, p.icon_url, p.club_name
            ORDER BY wins DESC
            LIMIT 20
        """)
        rows = cursor.fetchall()
        return [
            {
                "rank":        i + 1,
                "player_tag":  tag,
                "player_name": name,
                "icon_url":    icon_url,
                "club_name":   club_name,
                "wins":        wins,
            }
            for i, (tag, name, icon_url, club_name, wins) in enumerate(rows)
        ]
    finally:
        cursor.close()
        conn.close()


@app.get("/player-of-day")
@limiter.limit("20/minute")
def getPlayerOfDay(request: Request):
    """
    Devuelve:
      - today_ranking: TODOS los jugadores con actividad hoy y sus deltas
        (calculado en vivo, sin límite — así cualquier jugador puede ver
        sus puntos reales aunque no esté entre los primeros puestos)
      - history: ganadores de los últimos 7 días (sin incluir hoy)
      - last_updated: timestamp del último cómputo del collector
    """
    conn = get_conn()
    cursor = conn.cursor()
    try:
        ensure_player_of_day_table(cursor)

        from datetime import date, timedelta
        today = (datetime.now(timezone.utc) - timedelta(hours=3)).date()

        # Fallback: calcular ganador de hoy si el collector aún no corrió
        cursor.execute("SELECT 1 FROM player_of_day WHERE day = %s", (today,))
        if not cursor.fetchone():
            _compute_and_save_player_of_day(cursor, today)
            conn.commit()

        # ── Ranking de hoy en vivo (top 20 con deltas) ──────────────────
        today_ranking = get_today_live_ranking(cursor, today)

        # ── Historial (últimos 7 días, sin hoy) + computed_at de hoy ─────
        # Antes eran 2 SELECT separados: uno para el historial (day < hoy)
        # y otro aparte solo para leer el computed_at de HOY (necesario
        # para el fallback de last_updated más abajo). Se combinan en uno
        # solo ampliando el rango a [hoy-7, hoy] (8 días posibles, de ahí
        # el LIMIT 8) y separando la fila de "hoy" en Python — el
        # resultado es exactamente el mismo, un round-trip menos.
        cursor.execute("""
            SELECT day, player_tag, player_name, icon_url, club_name,
                   points, delta_trophies, delta_wins3v3, delta_winsSolo,
                   delta_prestige, computed_at
            FROM player_of_day
            WHERE day >= %s AND day <= %s
            ORDER BY day DESC
            LIMIT 8
        """, (today - timedelta(days=7), today))

        history = []
        last_updated = None
        today_computed_at = None
        for row in cursor.fetchall():
            (day, tag, name, icon_url, club_name,
             points, dt, dw3, dws, dp, computed_at) = row
            if day == today:
                today_computed_at = computed_at
                continue
            if last_updated is None and computed_at:
                last_updated = computed_at.isoformat()
            history.append({
                "day":            day.isoformat(),
                "player_tag":     tag,
                "player_name":    name,
                "icon_url":       icon_url,
                "club_name":      club_name,
                "points":         points,
                "delta_trophies": dt,
                "delta_wins3v3":  dw3,
                "delta_winsSolo": dws,
                "delta_prestige": dp,
            })

        # last_updated: usar computed_at del registro de hoy si existe
        if today_computed_at:
            last_updated = today_computed_at.isoformat()

        return {
            "last_updated":   last_updated,
            "today_ranking":  today_ranking,
            "history":        history,
        }

    finally:
        cursor.close()
        conn.close()


def _compute_and_save_player_of_day(cursor, day):
    """
    Calcula el jugador del día para `day` comparando snapshots
    del historial y guarda el resultado en player_of_day.
    """
    from datetime import date, timedelta

    day_start = datetime(day.year, day.month, day.day,
                         3, 0, 0, tzinfo=timezone.utc)   # 00:00 UY = 03:00 UTC
    day_end   = day_start + timedelta(hours=24)
    prev_end  = day_start   # = inicio del día actual = fin del día anterior

    # Para cada jugador: valor al INICIO del día (o último antes) y al FINAL del día
    cursor.execute("""
        WITH ranked AS (
            SELECT
                player_tag,
                trophies,
                wins3v3,
                winsSolo,
                total_prestige,
                timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY player_tag
                    ORDER BY timestamp ASC
                ) AS rn_asc,
                ROW_NUMBER() OVER (
                    PARTITION BY player_tag
                    ORDER BY timestamp DESC
                ) AS rn_desc
            FROM player_stats_history
            WHERE timestamp >= %s AND timestamp < %s
        ),
        day_first AS (
            SELECT player_tag, trophies, wins3v3, winsSolo, total_prestige
            FROM ranked WHERE rn_asc = 1
        ),
        day_last AS (
            SELECT player_tag, trophies, wins3v3, winsSolo, total_prestige
            FROM ranked WHERE rn_desc = 1
        ),
        -- Último snapshot ANTES del inicio del día (referencia base)
        prev_last AS (
            SELECT DISTINCT ON (player_tag)
                player_tag, trophies, wins3v3, winsSolo, total_prestige
            FROM player_stats_history
            WHERE timestamp < %s
            ORDER BY player_tag, timestamp DESC
        )
        SELECT
            dl.player_tag,
            -- BASE del día = último valor ANTES de las 00:00 UY (pf), no el
            -- primer cambio registrado DENTRO del día (df). El historial solo
            -- guarda una fila cuando el valor cambia, así que "df" puede ser
            -- la única fila de hoy y coincidir con "dl" -> delta 0 aunque el
            -- jugador sí progresó. pf siempre representa el valor real a las
            -- 00:00 UY, se haya registrado una fila hoy o no.
            COALESCE(dl.trophies, pf.trophies)    - COALESCE(pf.trophies, df.trophies, 0)    AS dt,
            COALESCE(dl.wins3v3, pf.wins3v3)      - COALESCE(pf.wins3v3,  df.wins3v3,  0)    AS dw3,
            COALESCE(dl.winsSolo, pf.winsSolo)    - COALESCE(pf.winsSolo, df.winsSolo, 0)    AS dws,
            COALESCE(dl.total_prestige, pf.total_prestige) - COALESCE(pf.total_prestige, df.total_prestige, 0) AS dp
        FROM day_last dl
        LEFT JOIN day_first df USING (player_tag)
        LEFT JOIN prev_last  pf USING (player_tag)
        ORDER BY dl.player_tag
    """, (day_start, day_end, day_start))

    deltas = cursor.fetchall()
    if not deltas:
        return  # Sin datos para este día, no guardamos nada

    # Calcular puntos y elegir ganador
    best = None
    best_points = -1
    for (tag, dt, dw3, dws, dp) in deltas:
        dt  = max(0, dt  or 0)
        dw3 = max(0, dw3 or 0)
        dws = max(0, dws or 0)
        dp  = max(0, dp  or 0)
        points = dt * 1 + (dw3 + dws) * 4 + dp * 80
        if points > best_points:
            best_points = points
            best = (tag, dt, dw3, dws, dp, points)

    if not best or best_points == 0:
        return  # Nadie progresó ese día

    tag, dt, dw3, dws, dp, points = best

    # Traer info del jugador
    cursor.execute("""
        SELECT name, icon_url, club_name FROM players WHERE tag = %s
    """, (tag,))
    row = cursor.fetchone()
    if not row:
        return
    name, icon_url, club_name = row

    cursor.execute("""
        INSERT INTO player_of_day
            (day, player_tag, player_name, icon_url, club_name,
             points, delta_trophies, delta_wins3v3, delta_winsSolo, delta_prestige,
             computed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (day) DO UPDATE SET
            player_tag     = EXCLUDED.player_tag,
            player_name    = EXCLUDED.player_name,
            icon_url       = EXCLUDED.icon_url,
            club_name      = EXCLUDED.club_name,
            points         = EXCLUDED.points,
            delta_trophies = EXCLUDED.delta_trophies,
            delta_wins3v3  = EXCLUDED.delta_wins3v3,
            delta_winsSolo = EXCLUDED.delta_winsSolo,
            delta_prestige = EXCLUDED.delta_prestige,
            computed_at    = NOW()
    """, (day, tag, name, icon_url, club_name, points, dt, dw3, dws, dp))
