import asyncio
import psycopg2
import psycopg2.extras
import brawlstats
import httpx
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
UY_TZ = timezone(timedelta(hours=-3))

CLUB_TAGS = ['282Y2LR8R', '2Y9GY220C', '2VG0RQ299', '2LLQ8VR2Q']

# La lógica del sorteo mensual (Sorteos / Brawl Pass Plus) vive en api_main.py,
# expuesta como POST /raffle/draw — así hay una sola fuente de verdad para el
# cálculo de tickets y la selección del ganador. Este script solo "toca el
# timbre" en cada corrida; el endpoint decide si corresponde sortear (es
# idempotente por mes, así que llamarlo de más no tiene efecto).
RAFFLE_API_BASE = os.getenv("RAFFLE_API_BASE", "https://matuclub-api.onrender.com").rstrip("/")
ADMIN_KEY = os.getenv("ADMIN_KEY", "")
# Solo llamamos al endpoint durante los primeros N días del mes (hora UY).
# Evita despertar el servidor de Render (cold start de ~30-90 s) en las ~48
# corridas diarias que no tienen nada que sortear, y deja una ventana de
# reintentos por si el día 1 falla. Poné 31 para llamarlo siempre.
RAFFLE_TRIGGER_DAYS = int(os.getenv("RAFFLE_TRIGGER_DAYS", "31"))

def get_conn():
    url = os.getenv("DATABASE_URL", "")
    return psycopg2.connect(url, connect_timeout=15)

# ── AUTO API KEY ──────────────────────────────────────────────
async def refresh_brawl_key():
    ip = httpx.get("https://api.ipify.org", timeout=10).text.strip()
    print(f"IP: {ip}")

    session = httpx.Client(follow_redirects=True, timeout=15)
    base_headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://developer.brawlstars.com",
        "Referer": "https://developer.brawlstars.com/"
    }

    login = session.post(
        "https://developer.brawlstars.com/api/login",
        json={"email": os.getenv("BS_EMAIL"), "password": os.getenv("BS_PASSWORD")},
        headers=base_headers
    )
    data = login.json()
    token = data.get("auth", {}).get("token")
    temp_token = data.get("temporaryAPIToken")

    # El endpoint de lista solo acepta POST
    keys = None
    headers = {**base_headers, "Authorization": f"Bearer {token}"}
    r = session.post("https://developer.brawlstars.com/api/apikey/list", headers=headers)
    if r.status_code == 200:
        try:
            keys = r.json()["keys"]
        except Exception:
            pass

    if keys is None:
        print("No se pudo obtener lista de keys")
        return None

    # Buscar key existente para esta IP
    for key in keys:
        if ip in key.get("cidrRanges", []):
            print("Key existente reutilizada")
            return key["key"]

    # Borrar la más vieja si llegamos al límite
    if len(keys) >= 10:
        oldest = sorted(keys, key=lambda k: k.get("validTime", 0))[0]
        session.post(
            "https://developer.brawlstars.com/api/apikey/revoke",
            json={"id": oldest["id"]},
            headers=headers
        )
        print(f"Key vieja borrada: {oldest['id']}")

    # Crear nueva key
    create = session.post(
        "https://developer.brawlstars.com/api/apikey/create",
        json={
            "name": "auto-key",
            "description": "Auto generated",
            "cidrRanges": [ip],
            "scopes": ["brawlstars"]
        },
        headers=headers
    )
    try:
        new_key = create.json()["key"]["key"]
        print("Nueva key creada")
        return new_key
    except Exception:
        print(f"No se pudo crear key: {create.text[:200]}")
        return None

# ── DEPARTURE TRACKING ────────────────────────────────────────

def ensure_departures_table(cursor):
    """Crea la tabla player_departures si no existe."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_departures (
            player_tag TEXT PRIMARY KEY,
            left_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

def normalize_tag(tag: str) -> str:
    tag = (tag or "").strip().upper()
    return tag.replace("##", "#") if tag.startswith("#") else "#" + tag


def handle_departures(cursor, current_club_tags: set, fetched_club_tags: set):
    """
    Compara los jugadores en la DB con los que están actualmente en los clubs.
    - Jugadores que salieron:  registrar su hora de salida (si no está ya registrada).
    - Jugadores que volvieron: eliminar su registro de salida.
    - Jugadores fuera por más de 24h: borrar TODOS sus datos en cascada.

    current_club_tags: tags de JUGADORES (sin o con '#', se normaliza) que
        aparecen ahora mismo en los clubes que se pudieron leer esta corrida.
    fetched_club_tags: tags de CLUB (sin o con '#', se normaliza) que se
        pudieron leer con éxito esta corrida.

    IMPORTANTE sobre fetched_club_tags: si el fetch de un club falla esta
    corrida (timeout, error de la API de Brawl Stars, etc.), sus miembros no
    aparecen en current_club_tags — pero eso NO significa que se hayan ido,
    solo que no pudimos leer los datos. Antes, esto se trataba igual que una
    salida real: iniciaba la cuenta de 24h para el club ENTERO, y si la falla
    se repetía sostenidamente, terminaba borrando en cascada todo el roster
    y su historial por una falla transitoria de la API. Ahora solo se
    considera "posible salida" a un jugador cuyo club SÍ se pudo leer con
    éxito esta corrida — si no se pudo leer, se lo deja como estaba (ni se
    inicia ni se cancela su conteo) hasta que haya una lectura confiable.

    Devuelve la cantidad de jugadores eliminados.
    """
    ensure_departures_table(cursor)

    # Tags de jugadores que ya teníamos en la DB, junto con el club en el
    # que estaban registrados la última vez (para poder filtrar por
    # fetched_club_tags más abajo).
    cursor.execute("SELECT tag, club_tag FROM players")
    db_rows = cursor.fetchall()
    db_club_of = {normalize_tag(tag): normalize_tag(club_tag) for tag, club_tag in db_rows}
    db_tags = set(db_club_of.keys())

    # Tags actuales en clubs convertidos a formato con '#'
    api_tags_with_hash = {normalize_tag(t) for t in current_club_tags}
    fetched_clubs_norm = {normalize_tag(t) for t in fetched_club_tags}
    print(f"Total DB: {len(db_tags)}")
    print(f"Total API: {len(api_tags_with_hash)}")

    # ── Jugadores que VOLVIERON a un club (estaban en departures pero ahora aparecen)
    if api_tags_with_hash:
        cursor.execute("""
            DELETE FROM player_departures
            WHERE player_tag = ANY(%s)
        """, (list(api_tags_with_hash),))
        returned = cursor.rowcount
        if returned:
            print(f"↩  {returned} jugador(es) volvieron a un club — conteo de salida cancelado")

    # ── Jugadores que SALIERON (están en DB, no aparecen en los clubes leídos
    #    esta corrida, Y su club SÍ se pudo leer con éxito esta corrida — ver
    #    docstring). Los que quedan afuera del filtro no se tocan.
    candidates = db_tags - api_tags_with_hash
    left_tags = {tag for tag in candidates if db_club_of.get(tag) in fetched_clubs_norm}
    skipped = len(candidates) - len(left_tags)
    if skipped:
        print(f"⏸  {skipped} jugador(es) no aparecen en los clubes leídos, pero su club "
              f"falló esta corrida — no se los marca como salida todavía")

    if left_tags:
        psycopg2.extras.execute_values(cursor, """
            INSERT INTO player_departures (player_tag)
            VALUES %s
            ON CONFLICT (player_tag) DO NOTHING
        """, [(tag,) for tag in left_tags])
        new_departures = cursor.rowcount
        if new_departures:
            print(f"🚪 {new_departures} jugador(es) nuevos sin club — conteo de 24h iniciado")

    # ── Eliminar jugadores que llevan más de 24h fuera
    cursor.execute("""
        SELECT player_tag FROM player_departures
        WHERE left_at <= NOW() - INTERVAL '24 hours'
    """)
    expired_tags = [row[0] for row in cursor.fetchall()]

    if expired_tags:
        print(f"🗑  {len(expired_tags)} jugador(es) llevan +24h fuera — eliminando todos sus datos...")
        for tag in expired_tags:
            try:
                # Borrar en orden correcto respetando foreign keys
                cursor.execute("DELETE FROM event_snapshots WHERE player_tag = %s", (tag,))
                cursor.execute("DELETE FROM player_stats_history WHERE player_tag = %s", (tag,))
                cursor.execute("DELETE FROM player_brawlers WHERE player_tag = %s", (tag,))
                cursor.execute("DELETE FROM players WHERE tag = %s", (tag,))
                cursor.execute("DELETE FROM player_departures WHERE player_tag = %s", (tag,))
                print(f"   ✓ Eliminado: {tag}")
            except Exception as e:
                print(f"   ERROR eliminando {tag}: {e}")
                raise

    return len(expired_tags)

# ── DATA COLLECTION ───────────────────────────────────────────
async def add_data_to_database():
    key = await refresh_brawl_key()
    if not key:
        print("ERROR: No se pudo obtener API key")
        return

    client = brawlstats.Client(key, is_async=True)
    conn = get_conn()
    cursor = conn.cursor()

    try:
        # Fetchear los 4 clubs en paralelo
        clubs = await asyncio.gather(
            *[client.get_club(tag) for tag in CLUB_TAGS],
            return_exceptions=True
        )

        members_dict = {}
        fetched_club_tags = set()  # clubes que SÍ se pudieron leer esta corrida
        for club_tag, club in zip(CLUB_TAGS, clubs):
            if isinstance(club, Exception):
                print(f"Error fetching club: {club}")
                continue
            fetched_club_tags.add(normalize_tag(club_tag))
            for m in club.members:
                members_dict[m.tag] = m

        unique_tags = list(members_dict.keys())
        print(f"Jugadores únicos en clubs: {len(unique_tags)}")

        failed_clubs = len(CLUB_TAGS) - len(fetched_club_tags)
        if failed_clubs:
            print(f"⚠ {failed_clubs} club(es) no se pudieron leer esta corrida — "
                  f"sus miembros quedan exentos del conteo de salida por esta vez")

        # ── Gestión de salidas / eliminaciones ANTES de guardar datos nuevos
        deleted = handle_departures(cursor, set(unique_tags), fetched_club_tags)
        conn.commit()
        if deleted:
            print(f"✓ {deleted} jugador(es) eliminados por superar las 24h fuera del club")

        # Fetchear jugadores con semáforo de 10 paralelos
        semaphore = asyncio.Semaphore(10)

        async def fetch_player(tag):
            async with semaphore:
                try:
                    return await asyncio.wait_for(client.get_profile(tag), timeout=8)
                except asyncio.TimeoutError:
                    print(f"Timeout: {tag}")
                    return None
                except Exception as e:
                    print(f"Error {tag}: {e}")
                    return None

        players = await asyncio.gather(
            *[fetch_player(tag) for tag in unique_tags],
            return_exceptions=True
        )
        print(f"Fetch terminado: {sum(1 for p in players if p is not None)} exitosos")

        # Últimos snapshots conocidos, en batch (evita 1 SELECT por jugador
        # adentro del for de abajo — antes eran hasta ~len(unique_tags)
        # round-trips solo para esto, uno por jugador, en cada corrida).
        # Es 100% equivalente a la consulta que hacía cada iteración: el
        # resultado para un player_tag no depende de los históricos que se
        # inserten en ESTA misma corrida para OTROS jugadores (cada fila de
        # player_stats_history es independiente por player_tag, y ningún
        # tag se repite dentro de unique_tags), así que traer todo de una
        # sola vez ACÁ, antes de que el loop escriba nada, da exactamente
        # el mismo "último valor conocido" que cada SELECT individual
        # hubiera dado en su turno.
        last_history_by_tag = {}
        if unique_tags:
            cursor.execute("""
                SELECT DISTINCT ON (player_tag)
                    player_tag, trophies, wins3v3, winsSolo, total_prestige
                FROM player_stats_history
                WHERE player_tag = ANY(%s)
                ORDER BY player_tag, timestamp DESC
            """, (unique_tags,))
            last_history_by_tag = {row[0]: tuple(row[1:]) for row in cursor.fetchall()}

        saved = 0
        for player in players:
            if player is None or isinstance(player, Exception):
                continue

            try:
                # Calcular winstreak máximo
                maxWs, maxWsbrawler = 0, ""
                for b in player.brawlers:
                    if b.max_win_streak > maxWs:
                        maxWs = b.max_win_streak
                        maxWsbrawler = b.name

                # 1. Insertar jugador primero (tabla padre)
                club_name = getattr(player.club, 'name', None)
                icon_id   = getattr(player.icon, 'id', None)
                # Construir URL directamente — Brawlify usa el icon_id de la API oficial
                icon_url  = f"https://cdn.brawlify.com/profile-icons/regular/{icon_id}.png" if icon_id else None

                cursor.execute("""
                    INSERT INTO players
                        (tag, name, highest_trophies, wins3v3, winsSolo,
                         total_prestige, highestWinstreak, maxWsBrawler, club_tag, club_name, icon_id, icon_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tag) DO UPDATE SET
                        name             = EXCLUDED.name,
                        highest_trophies = EXCLUDED.highest_trophies,
                        wins3v3          = EXCLUDED.wins3v3,
                        winsSolo         = EXCLUDED.winsSolo,
                        total_prestige   = EXCLUDED.total_prestige,
                        highestWinstreak = EXCLUDED.highestWinstreak,
                        maxWsBrawler     = EXCLUDED.maxWsBrawler,
                        club_tag         = EXCLUDED.club_tag,
                        club_name        = EXCLUDED.club_name,
                        icon_id          = EXCLUDED.icon_id,
                        icon_url         = EXCLUDED.icon_url
                """, (player.tag, player.name, player.highest_trophies,
                      player.team_victories,
                      player.solo_victories + player.duo_victories,
                      player.totalPrestigeLevel, maxWs, maxWsbrawler,
                      player.club.tag, club_name, icon_id, icon_url))

                # 2. Insertar brawlers en batch (ejecuteMany es más rápido)
                brawler_data = [
                    (player.tag, b.name, b.power, len(b.gadgets),
                     len(b.star_powers), len(b.hyper_charges), b.trophies)
                    for b in player.brawlers
                ]
                psycopg2.extras.execute_values(cursor, """
                    INSERT INTO player_brawlers
                        (player_tag, brawler_name, power_level, gadgets, star_powers, hipercharge, trophies)
                    VALUES %s
                    ON CONFLICT (player_tag, brawler_name) DO UPDATE SET
                        power_level = EXCLUDED.power_level,
                        gadgets     = EXCLUDED.gadgets,
                        star_powers = EXCLUDED.star_powers,
                        hipercharge = EXCLUDED.hipercharge,
                        trophies    = EXCLUDED.trophies
                """, brawler_data)

                # 3. Historial solo si algo cambió
                # IMPORTANTE: total_prestige tiene que estar en esta comparación.
                # Si se omite (como antes), un jugador que sube de prestigio sin
                # cambiar de trofeos/victorias en esa misma corrida no genera
                # fila nueva -> ese avance se pierde para siempre del historial
                # y jugador del día nunca lo puede contar (80 pts por prestigio,
                # el valor más alto de la fórmula).
                last = last_history_by_tag.get(player.tag)
                current = (player.trophies, player.team_victories,
                           player.solo_victories + player.duo_victories,
                           player.totalPrestigeLevel)

                if last is None or tuple(last) != current:
                    cursor.execute("""
                        INSERT INTO player_stats_history
                            (player_tag, trophies, wins3v3, winsSolo, total_prestige, club_tag)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (player.tag, player.trophies, player.team_victories,
                          player.solo_victories + player.duo_victories,
                          player.totalPrestigeLevel, player.club.tag))

                # Confirmamos ACÁ, jugador por jugador, en vez de un solo
                # commit() al final del for. Antes, todos los INSERT/UPDATE
                # de esta corrida compartían una única transacción sin
                # confirmar: si un jugador a mitad de la lista tiraba una
                # excepción, el rollback() de abajo descartaba TAMBIÉN el
                # trabajo ya hecho para todos los jugadores procesados ANTES
                # en esa misma corrida — aunque el contador "saved" ya los
                # hubiera contado como guardados exitosamente. Confirmando
                # por jugador, una falla puntual solo pierde a ese jugador.
                conn.commit()
                saved += 1

            except Exception as e:
                conn.rollback()
                print(f"ERROR {player.tag}: {e}")

        print(f"✓ Datos guardados: {saved}/{len(unique_tags)} jugadores")

        # Actualizar jugador del día con los datos frescos
        compute_and_update_player_of_day(cursor)
        conn.commit()

        # Resolver predicciones de días anteriores (no rompe la corrida si
        # falla — el fallback en la API la vuelve a intentar en la próxima
        # visita al sitio).
        try:
            now_uy = datetime.now(timezone.utc) - timedelta(hours=3)
            resolve_predictions(cursor, now_uy.date())
            conn.commit()
        except Exception as e:
            print(f"⚠ No se pudieron resolver predicciones: {e}")
            conn.rollback()

        # Guardar la foto del ranking oficial de clubes (Uruguay y Mundo)
        # que usa la pestaña de Análisis. No rompe la corrida si falla.
        try:
            ensure_club_rankings_table(cursor)
            await fetch_and_store_club_rankings(cursor, client)
            conn.commit()
        except Exception as e:
            print(f"⚠ No se pudo actualizar el ranking de clubes: {e}")
            conn.rollback()

        # Disparar el sorteo mensual de Brawl Pass Plus si corresponde (no
        # rompe la corrida si falla: es un "nice to have", no collection core).
        await trigger_raffle_draw()

    finally:
        cursor.close()
        conn.close()
        await client.close()


# ── ANÁLISIS: RANKING OFICIAL DE CLUBES (Uruguay / Mundo) ──────────────────
def ensure_club_rankings_table(cursor):
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


async def fetch_and_store_club_rankings(cursor, client):
    """
    Guarda el top 200 oficial de clubes de Supercell para Uruguay y para el
    Mundo. Se pisa por completo en cada corrida (DELETE + INSERT) porque a
    la pestaña de Análisis solo le interesa la foto más reciente, no un
    historial.
    """
    for region in ("UY", "global"):
        try:
            ranking = await client.get_rankings(ranking="clubs", region=region, limit=200)
            rows = [
                (region, c.rank, c.tag, c.name, c.trophies, getattr(c, "member_count", None))
                for c in ranking
            ]
        except Exception as e:
            print(f"⚠ No se pudo obtener el ranking de clubes de '{region}': {e}")
            continue

        if not rows:
            continue

        cursor.execute("DELETE FROM club_rankings WHERE region = %s", (region,))
        psycopg2.extras.execute_values(cursor, """
            INSERT INTO club_rankings (region, rank, club_tag, club_name, trophies, member_count)
            VALUES %s
        """, rows)
        print(f"✓ Ranking de clubes '{region}' actualizado ({len(rows)} clubes)")


# ── SORTEOS (Brawl Pass Plus) ─────────────────────────────────
def _describe_exc(e: Exception) -> str:
    """str(e) viene VACÍO en los timeouts de httpx; siempre mostramos el tipo."""
    return f"{type(e).__name__}: {e}" if str(e) else f"{type(e).__name__} (sin mensaje)"


async def trigger_raffle_draw():
    """
    Le pide a la API que sortee el mes anterior si todavía no lo hizo. El
    endpoint es idempotente por mes, así que llamarlo de más no tiene efecto.

    La API corre en Render (free tier), que se duerme tras ~15 min sin tráfico
    y tarda 30-90 s en despertar. Por eso:
      1. Primero la "despertamos" con un GET (cualquier respuesta HTTP sirve).
      2. Luego hacemos el POST con timeouts generosos y reintentos.
    """
    if not ADMIN_KEY:
        print("⚠ ADMIN_KEY no configurada: no se puede disparar el sorteo mensual.")
        return

    today_uy = (datetime.now(timezone.utc) - timedelta(hours=3)).date()
    if today_uy.day > RAFFLE_TRIGGER_DAYS:
        return  # Fuera de la ventana de sorteo: no molestamos a la API.

    # connect/read/write/pool por separado: el read es el que sufre con el cold start.
    timeout = httpx.Timeout(connect=30, read=90, write=30, pool=30)
    max_attempts = 4

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as http:
        # 1) Despertar el servidor. Un 404/405 también significa "ya está despierto".
        try:
            await http.get(f"{RAFFLE_API_BASE}/")
        except Exception as e:
            print(f"  (warm-up de la API falló: {_describe_exc(e)}; sigo igual)")

        # 2) POST con reintentos.
        for attempt in range(1, max_attempts + 1):
            try:
                r = await http.post(
                    f"{RAFFLE_API_BASE}/raffle/draw",
                    headers={"X-Admin-Key": ADMIN_KEY},
                )
            except (httpx.TimeoutException, httpx.TransportError) as e:
                # Timeout / red / DNS / conexión cortada: vale la pena reintentar.
                print(f"⚠ Sorteo: intento {attempt}/{max_attempts} falló: {_describe_exc(e)}")
            except Exception as e:
                print(f"⚠ No se pudo disparar el sorteo mensual: {_describe_exc(e)}")
                return
            else:
                if r.status_code >= 500:
                    # 502/503/504 son típicos mientras Render termina de arrancar.
                    print(f"⚠ Sorteo: intento {attempt}/{max_attempts} -> HTTP {r.status_code}: {r.text[:200]}")
                elif r.status_code != 200:
                    # 401/403/404/422...: reintentar no arregla nada.
                    print(f"⚠ /raffle/draw devolvió {r.status_code}: {r.text[:200]}")
                    return
                else:
                    try:
                        data = r.json()
                    except ValueError:
                        print(f"⚠ /raffle/draw devolvió una respuesta no-JSON: {r.text[:200]}")
                        return
                    if data.get("skipped"):
                        print(f"🎟️  Sorteo: {data.get('reason', 'sin novedades')}")
                    else:
                        winner = data.get("winner")
                        if winner:
                            print(f"🎉 Sorteo del mes {data.get('cycle_month')}: ganó {winner['name']} ({winner['tag']}) con {winner['tickets_total']} tickets")
                        else:
                            print(f"🎟️  Sorteo del mes {data.get('cycle_month')} ejecutado sin participantes elegibles.")
                    return

            if attempt < max_attempts:
                await asyncio.sleep(10 * attempt)  # 10 s, 20 s, 30 s

    # Se agotaron los intentos. ::warning:: aparece destacado en el resumen de GitHub Actions.
    print("::warning::No se pudo disparar el sorteo mensual tras varios intentos "
          "(se reintenta en la próxima corrida)")


# ── JUGADOR DEL DÍA ───────────────────────────────────────────
def compute_and_update_player_of_day(cursor):
    """
    Calcula (o actualiza) el jugador del día para HOY en horario UY (UTC-3).
    Se llama al final de cada run del collector.

    Lógica:
    - Día UY: 00:00 UY (03:00 UTC) → 00:00 UY del día siguiente
    - Compara, para cada jugador, su snapshot MÁS ANTIGUO del día vs el MÁS RECIENTE.
    - Si no tiene snapshot previo en el día, usa el último snapshot del día anterior como base.
    - Gana quien acumuló más puntos (trofeos×1 + victorias×4 + prestige×80).
    - Usa ON CONFLICT DO UPDATE para sobreescribir el resultado anterior del día.
    - A partir de las 00:00 UY el día siguiente, este día ya no se toca más.
    """
    from datetime import date, timedelta

    now_uy = datetime.now(timezone.utc) - timedelta(hours=3)
    today  = now_uy.date()

    day_start = datetime(today.year, today.month, today.day,
                         3, 0, 0, tzinfo=timezone.utc)   # 00:00 UY = 03:00 UTC
    day_end   = day_start + timedelta(hours=24)

    print(f"Calculando jugador del día para {today} (UY)…")

    cursor.execute("""
        WITH ranked AS (
            SELECT
                player_tag,
                trophies,
                wins3v3,
                winssolo,
                total_prestige,
                timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY player_tag ORDER BY timestamp ASC
                ) AS rn_asc,
                ROW_NUMBER() OVER (
                    PARTITION BY player_tag ORDER BY timestamp DESC
                ) AS rn_desc
            FROM player_stats_history
            WHERE timestamp >= %s AND timestamp < %s
        ),
        day_first AS (
            SELECT player_tag, trophies, wins3v3, winssolo, total_prestige
            FROM ranked WHERE rn_asc = 1
        ),
        day_last AS (
            SELECT player_tag, trophies, wins3v3, winssolo, total_prestige
            FROM ranked WHERE rn_desc = 1
        ),
        prev_last AS (
            SELECT DISTINCT ON (player_tag)
                player_tag, trophies, wins3v3, winssolo, total_prestige
            FROM player_stats_history
            WHERE timestamp < %s
            ORDER BY player_tag, timestamp DESC
        )
        SELECT
            dl.player_tag,
            -- BASE del día = último valor ANTES de las 00:00 UY (pv), no el
            -- primer cambio registrado DENTRO del día (df). Como el historial
            -- solo guarda una fila cuando el valor cambia respecto a la fila
            -- anterior, "df" puede ser la única fila del día y coincidir
            -- exactamente con "dl" (mismo cambio) -> eso daba delta 0 aunque
            -- el jugador sí progresó. pv sí representa el valor real a las
            -- 00:00 UY, se haya registrado una fila hoy o no.
            COALESCE(dl.trophies,       pv.trophies)       - COALESCE(pv.trophies,       df.trophies,       0) AS dt,
            COALESCE(dl.wins3v3,        pv.wins3v3)        - COALESCE(pv.wins3v3,        df.wins3v3,        0) AS dw3,
            COALESCE(dl.winssolo,       pv.winssolo)       - COALESCE(pv.winssolo,       df.winssolo,       0) AS dws,
            COALESCE(dl.total_prestige, pv.total_prestige) - COALESCE(pv.total_prestige, df.total_prestige, 0) AS dp
        FROM day_last dl
        LEFT JOIN day_first df USING (player_tag)
        LEFT JOIN prev_last  pv USING (player_tag)
        ORDER BY dl.player_tag
    """, (day_start, day_end, day_start))
    # ORDER BY determinístico: sin esto, Postgres puede devolver las filas en
    # cualquier orden y, si dos jugadores empatan en puntos, el ganador
    # elegido por el "if points > best_points" de abajo podía variar entre
    # corridas (no reproducible). Con el orden fijo, el empate siempre lo
    # gana la tag alfabéticamente menor — igual que en la query en vivo de
    # api_main.py, que ahora usa el mismo criterio de desempate.

    deltas = cursor.fetchall()
    if not deltas:
        print("  Sin snapshots del día todavía, saltando.")
        return

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
        print("  Nadie ha progresado todavía hoy.")
        return

    tag, dt, dw3, dws, dp, points = best

    cursor.execute("""
        SELECT name, icon_url, club_name FROM players WHERE tag = %s
    """, (tag,))
    row = cursor.fetchone()
    if not row:
        print(f"  Jugador {tag} no encontrado en players.")
        return
    name, icon_url, club_name = row

    # Crear tabla si no existe (idempotente)
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

    # DO UPDATE: sobreescribe el resultado anterior del mismo día
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
    """, (today, tag, name, icon_url, club_name, points, dt, dw3, dws, dp))

    print(f"  ⭐ Jugador del día actualizado: {name} ({tag}) — {points} pts")


def resolve_predictions(cursor, today):
    """
    Resuelve toda predicción cuyo target_day ya haya terminado (< today) y
    todavía no esté resuelta: compara predicted_tag contra el ganador real
    en player_of_day para ese día. Si ese día no tiene fila en player_of_day
    (nadie tuvo actividad — caso raro pero posible), se resuelve igual como
    incorrecta, con winner_tag NULL, para que la predicción no quede
    pendiente para siempre.

    Duplica la lógica de la función homónima en api_main.py (misma idea que
    ya existe para jugador del día: el collector la corre cada 30 min, y la
    API la vuelve a correr como red de seguridad antes de responder).
    """
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
    resolved_matched = cursor.rowcount

    cursor.execute("""
        UPDATE player_predictions pp
        SET resolved_at = NOW(),
            winner_tag  = NULL,
            correct     = FALSE
        WHERE pp.resolved_at IS NULL
          AND pp.target_day < %s
          AND NOT EXISTS (SELECT 1 FROM player_of_day pod WHERE pod.day = pp.target_day)
    """, (today,))
    resolved_empty = cursor.rowcount

    total = resolved_matched + resolved_empty
    if total:
        print(f"  🔮 {total} predicción(es) resuelta(s) para días anteriores")


if __name__ == "__main__":
    asyncio.run(add_data_to_database())
