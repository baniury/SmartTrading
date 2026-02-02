import os
import requests
import psycopg2
from psycopg2.extras import execute_values

API_KEY = os.getenv("TWELVE_API_KEY")
DB_URL = os.getenv("DATABASE_URL")

if not API_KEY:
    raise RuntimeError("Brak TWELVE_API_KEY w env (GitHub Secrets).")
if not DB_URL:
    raise RuntimeError("Brak DATABASE_URL w env (GitHub Secrets).")

TZ = "Europe/Warsaw"
SYMBOLS = ["EUR/USD", "USD/JPY", "XAU/USD", "DAX"]  # jeśli któryś symbol będzie error, poprawimy
INTERVAL = "15min"
TIMEFRAME = "M15"
OUTPUTSIZE = 3  # bierzemy kilka ostatnich świec, żeby UPSERT poprawiał ewentualne korekty

URL = "https://api.twelvedata.com/time_series"

def fetch_batch(symbols, interval, outputsize):
    params = {
        "symbol": ",".join(symbols),
        "interval": interval,
        "outputsize": outputsize,
        "timezone": TZ,
        "apikey": API_KEY,
    }
    r = requests.get(URL, params=params, timeout=25)
    r.raise_for_status()
    return r.json()

def parse_rows(raw, symbols):
    rows = []
    errors = {}

    for sym in symbols:
        payload = raw.get(sym)
        if not payload or payload.get("status") != "ok" or "values" not in payload:
            errors[sym] = payload if payload else {"status": "error", "message": "No payload for symbol"}
            continue

        for c in payload["values"]:
            # Twelve Data daje stringi -> zamienimy na float
            rows.append((
                sym,
                TIMEFRAME,
                c["datetime"],          # timestamptz: Postgres łyknie format "YYYY-MM-DD HH:MM:SS"
                float(c["open"]),
                float(c["high"]),
                float(c["low"]),
                float(c["close"]),
                float(c["volume"]) if c.get("volume") not in (None, "") else None,
                "twelvedata",
            ))
    return rows, errors

def upsert_candles(conn, rows):
    sql = """
    INSERT INTO candles (instrument, timeframe, ts, o, h, l, c, volume, source)
    VALUES %s
    ON CONFLICT (instrument, timeframe, ts)
    DO UPDATE SET
      o = EXCLUDED.o,
      h = EXCLUDED.h,
      l = EXCLUDED.l,
      c = EXCLUDED.c,
      volume = EXCLUDED.volume,
      source = EXCLUDED.source;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
    conn.commit()

def main():
    raw = fetch_batch(SYMBOLS, INTERVAL, OUTPUTSIZE)
    rows, errors = parse_rows(raw, SYMBOLS)

    if errors:
        print("API ERRORS (symbole do poprawy, jeśli trzeba):")
        for k, v in errors.items():
            print("-", k, v)

    if not rows:
        raise RuntimeError("Brak świec do zapisu (wszystkie symbole zwróciły błąd).")

    conn = psycopg2.connect(DB_URL)
    upsert_candles(conn, rows)
    conn.close()

    print(f"Zapisano/zaaktualizowano {len(rows)} świec do candles ({TIMEFRAME}).")

if __name__ == "__main__":
    main()
