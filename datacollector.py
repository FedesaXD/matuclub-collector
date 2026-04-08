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
        # Migración: asegurar columnas de ranked
        cursor.execute("""
            ALTER TABLE players
                ADD COLUMN IF NOT EXISTS current_ranked_points INTEGER NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS highest_ranked_points  INTEGER NOT NULL DEFAULT 0
        """)
        conn.commit()

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

                # Ranked points — leídos del raw_data para evitar KeyError del Box
                raw = player.raw_data if hasattr(player, 'raw_data') else {}
                current_ranked_pts = raw.get('currentRankedPoints') or 0
                highest_ranked_pts = raw.get('highestRankedPoints') or 0

                cursor.execute("""
                    INSERT INTO players
                        (tag, name, highest_trophies, wins3v3, winsSolo,
                         total_prestige, highestWinstreak, maxWsBrawler, club_tag, club_name, icon_id, icon_url,
                         current_ranked_points, highest_ranked_points)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tag) DO UPDATE SET
                        name                  = EXCLUDED.name,
                        highest_trophies      = EXCLUDED.highest_trophies,
                        wins3v3               = EXCLUDED.wins3v3,
                        winsSolo              = EXCLUDED.winsSolo,
                        total_prestige        = EXCLUDED.total_prestige,
                        highestWinstreak      = EXCLUDED.highestWinstreak,
                        maxWsBrawler          = EXCLUDED.maxWsBrawler,
                        club_tag              = EXCLUDED.club_tag,
                        club_name             = EXCLUDED.club_name,
                        icon_id               = EXCLUDED.icon_id,
                        icon_url              = EXCLUDED.icon_url,
                        current_ranked_points = EXCLUDED.current_ranked_points,
                        highest_ranked_points = EXCLUDED.highest_ranked_points
                """, (player.tag, player.name, player.highest_trophies,
                      player.team_victories,
                      player.solo_victories + player.duo_victories,
                      player.totalPrestigeLevel, maxWs, maxWsbrawler,
                      player.club.tag, club_name, icon_id, icon_url,
                      current_ranked_pts, highest_ranked_pts))

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

    finally:
        cursor.close()
        conn.close()
        await client.close()


if __name__ == "__main__":
    asyncio.run(add_data_to_database())
