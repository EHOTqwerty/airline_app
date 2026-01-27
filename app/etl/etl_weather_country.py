from datetime import date, datetime, timedelta
from sqlalchemy import text
from app.db.connection import get_engine
from app.api.open_meteo_client import fetch_hourly

def split_days(start_date: str, end_date: str):
    d1 = datetime.strptime(start_date, "%Y-%m-%d").date()
    d2 = datetime.strptime(end_date, "%Y-%m-%d").date()
    days = []
    cur = d1
    while cur <= d2:
        days.append(cur)
        cur += timedelta(days=1)

    today = date.today()
    hist_cutoff = today - timedelta(days=2)
    historical = [d for d in days if d <= hist_cutoff]
    forecast = [d for d in days if d > hist_cutoff]
    return historical, forecast

def run(country_code: str, start_date: str, end_date: str) -> str:
    engine = get_engine()

    # airports with coords
    with engine.connect() as conn:
        airports = conn.execute(text("""
            SELECT iata_code, latitude, longitude
            FROM airports
            WHERE country_code=:cc AND is_active=1
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        """), {"cc": country_code}).fetchall()

    if not airports:
        return f"EMPTY: no airports with coords for {country_code}"

    hist_days, fc_days = split_days(start_date, end_date)

    inserted = 0
    with engine.begin() as conn:
        for (iata, lat, lon) in airports:
            if hist_days:
                hs = hist_days[0].strftime("%Y-%m-%d")
                he = hist_days[-1].strftime("%Y-%m-%d")
                data = fetch_hourly(lat, lon, hs, he, mode="historical")
                inserted += _save_hourly(conn, iata, data, source="historical")

            if fc_days:
                fs = fc_days[0].strftime("%Y-%m-%d")
                fe = fc_days[-1].strftime("%Y-%m-%d")
                data = fetch_hourly(lat, lon, fs, fe, mode="forecast")
                inserted += _save_hourly(conn, iata, data, source="forecast")

    return f"OK: saved {inserted} hourly weather rows for {country_code} ({start_date}..{end_date})"

def _save_hourly(conn, iata: str, data: dict, source: str) -> int:
    h = data.get("hourly", {})
    times = h.get("time", [])
    t2m = h.get("temperature_2m", [])
    wind = h.get("wind_speed_10m", [])
    prec = h.get("precipitation", [])
    vis = h.get("visibility", [])

    n = 0
    for i in range(len(times)):
        dt_utc = times[i].replace("T", " ")
        conn.execute(text("""
            INSERT INTO weather_hourly
              (iata_code, dt_utc, source, temperature_c, windspeed_ms, precipitation_mm, visibility_m)
            VALUES
              (:iata, :dt, :src, :t, :w, :p, :v)
            ON DUPLICATE KEY UPDATE
              temperature_c=VALUES(temperature_c),
              windspeed_ms=VALUES(windspeed_ms),
              precipitation_mm=VALUES(precipitation_mm),
              visibility_m=VALUES(visibility_m)
        """), {
            "iata": iata,
            "dt": dt_utc,
            "src": source,
            "t": t2m[i] if i < len(t2m) else None,
            "w": wind[i] if i < len(wind) else None,
            "p": prec[i] if i < len(prec) else None,
            "v": vis[i] if i < len(vis) else None,
        })
        n += 1
    return n

if __name__ == "__main__":
    print(run("PL", "2026-02-01", "2026-02-07"))