import asyncio
import psycopg2
import brawlstats
import httpx
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
UY_TZ = timezone(timedelta(hours=-3))

MATU1_TAG = '282Y2LR8R'
MATU2_TAG = '2Y9GY220C'
MATU3_TAG = '2VG0RQ299'
MATU4_TAG = '2LLQ8VR2Q'

def get_conn():
    url = os.getenv("DATABASE_URL", "")
    url = url.replace("?sslmode=require", "").replace("&sslmode=require", "")
    return psycopg2.connect(url, sslmode="require", connect_timeout=15)

# ── AUTO API KEY ──────────────────────────────────────────────
async def refresh_brawl_key():
    ip = httpx.get("https://api.ipify.org").text.strip()
    print("IP:", ip)

    session = httpx.Client(follow_redirects=True)
    base_headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://developer.brawlstars.com",
        "Referer": "https://developer.brawlstars.com/"
    }

    login = session.post(
        "https://developer.brawlstars.com/api/login",
        json={
            "email": os.getenv("BS_EMAIL"),
            "password": os.getenv("BS_PASSWORD")
        },
        headers=base_headers
    )

    data = login.json()
    print("LOGIN RESPONSE:", data)

    token = data.get("auth", {}).get("token")
    temp_token = data.get("temporaryAPIToken")

    headers = {**base_headers, "Authorization": f"Bearer {token}"}

    response = session.get(
        "https://developer.brawlstars.com/api/apikey/list",
        headers=headers
    )

    print("LIST STATUS:", response.status_code)

    try:
        keys = response.json()["keys"]
    except Exception:
        print("Probando temporaryAPIToken...")
        headers["Authorization"] = f"Bearer {temp_token}"
        response = session.post(
            "https://developer.brawlstars.com/api/apikey/list",
            headers=headers
        )
        try:
            keys = response.json()["keys"]
        except Exception:
            print("No se pudo obtener keys.")
            return None

    for key in keys:
        if ip in key.get("cidrRanges", []):
            print("Key existente encontrada")
            return key["key"]

    if len(keys) >= 10:
        print("Borrando key vieja...")
        session.post(
            "https://developer.brawlstars.com/api/apikey/revoke",
            json={"id": keys[0]["id"]},
            headers=headers
        )

    print("Creando nueva key...")
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

    print("CREATE STATUS:", create.status_code)

    try:
        new_key = create.json()["key"]["key"]
        print("Nueva key creada")
        return new_key
    except Exception:
        print("No se pudo crear la key")
        return None

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
        matu1 = await client.get_club(MATU1_TAG)
        matu2 = await client.get_club(MATU2_TAG)
        matu3 = await client.get_club(MATU3_TAG)
        matu4 = await client.get_club(MATU4_TAG)

        members_dict = {}
        for m in (matu1.members + matu2.members + matu3.members + matu4.members):
            members_dict[m.tag] = m
        unique_tags = list(members_dict.keys())

        semaphore = asyncio.Semaphore(5)

        async def fetch_player(tag):
            async with semaphore:
                try:
                    return await asyncio.wait_for(client.get_profile(tag), timeout=10)
                except asyncio.TimeoutError:
                    print(f"Timeout en {tag}")
                    return None
                except Exception as e:
                    print(f"Error en {tag}: {e}")
                    return None

        tasks = [fetch_player(tag) for tag in unique_tags]
        players = await asyncio.gather(*tasks, return_exceptions=True)
        print("Todos los fetch terminaron")

        for player in players:
            if player is None or isinstance(player, Exception):
                print("ERROR: No se pudo fetchear un player")
                continue

            try:
                maxWs = 0
                maxWsbrawler = ""

                for b in player.brawlers:
                    # INSERT OR REPLACE en PostgreSQL
                    cursor.execute("""
                        INSERT INTO player_brawlers
                            (player_tag, brawler_name, power_level, gadgets, star_powers, hipercharge, trophies)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (player_tag, brawler_name)
                        DO UPDATE SET
                            power_level = EXCLUDED.power_level,
                            gadgets     = EXCLUDED.gadgets,
                            star_powers = EXCLUDED.star_powers,
                            hipercharge = EXCLUDED.hipercharge,
                            trophies    = EXCLUDED.trophies
                    """, (player.tag, b.name, b.power, len(b.gadgets),
                          len(b.star_powers), len(b.hyper_charges), b.trophies))

                    if b.max_win_streak > maxWs:
                        maxWs = b.max_win_streak
                        maxWsbrawler = b.name

                cursor.execute("""
                    INSERT INTO players
                        (tag, name, highest_trophies, wins3v3, winsSolo,
                         total_prestige, highestWinstreak, maxWsBrawler, club_tag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tag)
                    DO UPDATE SET
                        name             = EXCLUDED.name,
                        highest_trophies = EXCLUDED.highest_trophies,
                        wins3v3          = EXCLUDED.wins3v3,
                        winsSolo         = EXCLUDED.winsSolo,
                        total_prestige   = EXCLUDED.total_prestige,
                        highestWinstreak = EXCLUDED.highestWinstreak,
                        maxWsBrawler     = EXCLUDED.maxWsBrawler,
                        club_tag         = EXCLUDED.club_tag
                """, (player.tag, player.name, player.highest_trophies,
                      player.team_victories,
                      player.solo_victories + player.duo_victories,
                      player.totalPrestigeLevel, maxWs, maxWsbrawler,
                      player.club.tag))

                # Solo guardar en historial si algo cambió
                cursor.execute("""
                    SELECT trophies, wins3v3, winsSolo
                    FROM player_stats_history
                    WHERE player_tag = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (player.tag,))
                last = cursor.fetchone()

                current = (
                    player.trophies,
                    player.team_victories,
                    player.solo_victories + player.duo_victories
                )

                if last is None or tuple(last) != current:
                    cursor.execute("""
                        INSERT INTO player_stats_history
                            (player_tag, trophies, wins3v3, winsSolo, total_prestige, club_tag)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (player.tag, player.trophies, player.team_victories,
                          player.solo_victories + player.duo_victories,
                          player.totalPrestigeLevel, player.club.tag))

            except Exception as e:
                print("ERROR PROCESANDO PLAYER:", e)

        conn.commit()
        print("Datos guardados en Supabase correctamente")

    finally:
        cursor.close()
        conn.close()
        await client.close()


if __name__ == "__main__":
    asyncio.run(add_data_to_database())
