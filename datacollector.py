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

def handle_departures(cursor, current_club_tags: set):
    """
    Compara los jugadores en la DB con los que están actualmente en los clubs.
    - Jugadores que salieron:  registrar su hora de salida (si no está ya registrada).
    - Jugadores que volvieron: eliminar su registro de salida.
    - Jugadores fuera por más de 24h: borrar TODOS sus datos en cascada.

    current_club_tags: set de tags SIN '#' (como los devuelve la API de BS)
    Devuelve la cantidad de jugadores eliminados.
    """
    ensure_departures_table(cursor)

    def normalize(tag: str):
        return tag.strip().upper().replace("##", "#") if tag.startswith("#") else "#" + tag.strip().upper()
    # Tags de jugadores que ya teníamos en la DB (con '#')
    cursor.execute("SELECT tag FROM players")
    db_tags = {normalize(row[0]) for row in cursor.fetchall()}

    # Tags actuales en clubs convertidos a formato con '#'
    api_tags_with_hash = {normalize(t) for t in current_club_tags}
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

    # ── Jugadores que SALIERON (están en DB pero no en ningún club actual)
    left_tags = db_tags - api_tags_with_hash
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
        for club in clubs:
            if isinstance(club, Exception):
                print(f"Error fetching club: {club}")
                continue
            for m in club.members:
                members_dict[m.tag] = m

        unique_tags = list(members_dict.keys())
        print(f"Jugadores únicos en clubs: {len(unique_tags)}")

        # ── Gestión de salidas / eliminaciones ANTES de guardar datos nuevos
        deleted = handle_departures(cursor, set(unique_tags))
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
                cursor.execute("""
                    SELECT trophies, wins3v3, winsSolo
                    FROM player_stats_history
                    WHERE player_tag = %s
                    ORDER BY timestamp DESC LIMIT 1
                """, (player.tag,))
                last = cursor.fetchone()
                current = (player.trophies, player.team_victories,
                           player.solo_victories + player.duo_victories)

                if last is None or tuple(last) != current:
                    cursor.execute("""
                        INSERT INTO player_stats_history
                            (player_tag, trophies, wins3v3, winsSolo, total_prestige, club_tag)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (player.tag, player.trophies, player.team_victories,
                          player.solo_victories + player.duo_victories,
                          player.totalPrestigeLevel, player.club.tag))

                saved += 1

            except Exception as e:
                conn.rollback()
                print(f"ERROR {player.tag}: {e}")

        conn.commit()
        print(f"✓ Datos guardados: {saved}/{len(unique_tags)} jugadores")

        # Actualizar jugador del día con los datos frescos
        compute_and_update_player_of_day(cursor)
        conn.commit()

    finally:
        cursor.close()
        conn.close()
        await client.close()


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
                "winsSolo",
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
            SELECT player_tag, trophies, wins3v3, "winsSolo", total_prestige
            FROM ranked WHERE rn_asc = 1
        ),
        day_last AS (
            SELECT player_tag, trophies, wins3v3, "winsSolo", total_prestige
            FROM ranked WHERE rn_desc = 1
        ),
        prev_last AS (
            SELECT DISTINCT ON (player_tag)
                player_tag, trophies, wins3v3, "winsSolo", total_prestige
            FROM player_stats_history
            WHERE timestamp < %s
            ORDER BY player_tag, timestamp DESC
        )
        SELECT
            dl.player_tag,
            COALESCE(dl.trophies,       pv.trophies)       - COALESCE(df.trophies,       pv.trophies,       0) AS dt,
            COALESCE(dl.wins3v3,        pv.wins3v3)        - COALESCE(df.wins3v3,        pv.wins3v3,        0) AS dw3,
            COALESCE(dl."winsSolo",     pv."winsSolo")     - COALESCE(df."winsSolo",     pv."winsSolo",     0) AS dws,
            COALESCE(dl.total_prestige, pv.total_prestige) - COALESCE(df.total_prestige, pv.total_prestige, 0) AS dp
        FROM day_last dl
        LEFT JOIN day_first df USING (player_tag)
        LEFT JOIN prev_last  pv USING (player_tag)
    """, (day_start, day_end, day_start))

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


if __name__ == "__main__":
    asyncio.run(add_data_to_database())
