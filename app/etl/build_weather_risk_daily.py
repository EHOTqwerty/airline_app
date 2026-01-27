from sqlalchemy import text
from app.db.connection import get_engine

def risk_level(score: float) -> str:
    if score >= 2.0:
        return "HIGH"
    if score >= 1.0:
        return "MEDIUM"
    return "LOW"

def run(country_code: str) -> str:
    engine = get_engine()

    # Prosty scoring:
    # - wiatr > 12 m/s
    # - opad > 1.0 mm/h
    # - widoczność < 3000 m
    # score = suma 0..3
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT
              a.iata_code,
              DATE(w.dt_utc) AS day,
              w.source,
              AVG(CASE WHEN w.windspeed_ms IS NOT NULL AND w.windspeed_ms > 12 THEN 1 ELSE 0 END) AS wind_r,
              AVG(CASE WHEN w.precipitation_mm IS NOT NULL AND w.precipitation_mm > 1.0 THEN 1 ELSE 0 END) AS prec_r,
              AVG(CASE WHEN w.visibility_m IS NOT NULL AND w.visibility_m < 3000 THEN 1 ELSE 0 END) AS vis_r
            FROM airports a
            JOIN weather_hourly w ON w.iata_code = a.iata_code
            WHERE a.country_code=:cc
            GROUP BY a.iata_code, DATE(w.dt_utc), w.source
        """), {"cc": country_code}).fetchall()

        up = 0
        for iata, day, source, wind_r, prec_r, vis_r in rows:
            score = float(wind_r or 0) + float(prec_r or 0) + float(vis_r or 0)
            lvl = risk_level(score)
            conn.execute(text("""
                INSERT INTO weather_risk_daily (iata_code, day, source, risk_score, risk_level)
                VALUES (:iata, :day, :src, :score, :lvl)
                ON DUPLICATE KEY UPDATE risk_score=VALUES(risk_score), risk_level=VALUES(risk_level)
            """), {"iata": iata, "day": day, "src": source, "score": score, "lvl": lvl})
            up += 1

    return f"OK: upserted {up} risk_daily rows for {country_code}"

if __name__ == "__main__":
    print(run("PL"))
