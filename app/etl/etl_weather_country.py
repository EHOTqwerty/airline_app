from datetime import date, datetime, timedelta
from sqlalchemy import text
from app.db.connection import get_engine
from app.api.open_meteo_client import fetch_hourly, generate_synthetic_hourly_weather

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


def save_weather_hourly(conn, iata_code: str, rows, source: str):
    conn.execute(
        text("""
            INSERT INTO weather_hourly
              (iata_code, dt_utc, temperature_c, windspeed_ms, precipitation_mm, visibility_m, source)
            VALUES
              (:iata, :dt, :t, :w, :p, :v, :src)
            ON DUPLICATE KEY UPDATE
              temperature_c=VALUES(temperature_c),
              windspeed_ms=VALUES(windspeed_ms),
              precipitation_mm=VALUES(precipitation_mm),
              visibility_m=VALUES(visibility_m),
              source=VALUES(source)
        """),
        [
            {
                "iata": iata_code,
                "dt": r.dt_utc,
                "t": r.temperature_c,
                "w": r.windspeed_ms,
                "p": r.precipitation_mm,
                "v": r.visibility_m,
                "src": source,
            }
            for r in rows
        ],
    )

def etl_weather_for_airport(iata: str, lat: float, lon: float, start: date, end: date) -> str:
    engine = get_engine()
    with engine.begin() as conn:
        try:
            api_rows = fetch_hourly(lat, lon, start.isoformat(), end.isoformat(), mode="historical")
            if not api_rows or not api_rows.get("hourly") or len(api_rows["hourly"].get("time", [])) < 1:
                raise ValueError("Open-Meteo returned empty hourly data")

            # Konwersja do HourlyWeatherRow
            rows = []
            h = api_rows["hourly"]
            for i, t in enumerate(h["time"]):
                rows.append(
                    type('Row', (), dict(
                        dt_utc=datetime.strptime(t, "%Y-%m-%dT%H:%M"),
                        temperature_c=h["temperature_2m"][i] if i < len(h["temperature_2m"]) else None,
                        windspeed_ms=h["wind_speed_10m"][i] if i < len(h["wind_speed_10m"]) else None,
                        precipitation_mm=h["precipitation"][i] if i < len(h["precipitation"]) else None,
                        visibility_m=h["visibility"][i] if i < len(h["visibility"]) else None,
                    ))()
                )
            save_weather_hourly(conn, iata, rows, source="api")
            return f"OK: Open-Meteo saved {len(rows)} hourly rows for {iata}"

        except Exception as e:
            msg = f"FALLBACK weather for {iata}: {type(e).__name__}: {e}"
            print(msg)

            synth_rows = generate_synthetic_hourly_weather(iata, start, end)
            save_weather_hourly(conn, iata, synth_rows, source="synthetic")
            return f"{msg}\nOK: saved synthetic {len(synth_rows)} hourly rows for {iata}"

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


# Main ETL entrypoint for country-level weather ETL
from app.config.top_airports import TOP_AIRPORTS
from app.config.eu_countries import EU_COUNTRIES

def run(country_code: str, start_date: str, end_date: str) -> str:
    """
    ETL weather data for all top airports in a given country and date range.
    """
    if country_code not in TOP_AIRPORTS:
        return f"Unknown country code: {country_code}"
    airports = TOP_AIRPORTS[country_code]
    # For demo, use fixed coordinates for each airport (should be improved)
    # In real use, fetch lat/lon from DB or config
    from app.db.connection import get_engine
    engine = get_engine()
    results = []
    with engine.connect() as conn:
        for iata in airports:
            # Try to get lat/lon from DB
            row = conn.execute(
                text("SELECT latitude, longitude FROM airports WHERE iata_code=:iata"),
                {"iata": iata}
            ).fetchone()
            if not row:
                results.append(f"{iata}: coordinates not found in DB")
                continue
            lat, lon = row
            try:
                msg = etl_weather_for_airport(iata, lat, lon, datetime.strptime(start_date, "%Y-%m-%d").date(), datetime.strptime(end_date, "%Y-%m-%d").date())
                results.append(f"{iata}: {msg}")
            except Exception as e:
                results.append(f"{iata}: ERROR {e}")
    return "\n".join(results)

if __name__ == "__main__":
    print(run("PL", "2026-02-01", "2026-02-07"))