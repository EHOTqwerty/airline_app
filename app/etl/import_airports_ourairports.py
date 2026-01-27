from pathlib import Path
import csv
from sqlalchemy import text
from app.db.connection import get_engine
from app.config.eu_codes import EU_COUNTRY_CODES

DATA_PATH = Path("airports.csv")

def run():
    if not DATA_PATH.exists():
        raise RuntimeError("Brak pliku airports.csv")

    engine = get_engine()
    inserted = 0

    with DATA_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        with engine.begin() as conn:
            for row in reader:
                iata = (row.get("iata_code") or "").strip().upper()
                country = (row.get("iso_country") or "").strip().upper()
                if not iata or len(iata) != 3:
                    continue
                if country not in EU_COUNTRY_CODES:
                    continue

                name = (row.get("name") or "").strip()[:200]
                lat = row.get("latitude_deg")
                lon = row.get("longitude_deg")
                lat = float(lat) if lat else None
                lon = float(lon) if lon else None

                conn.execute(text("""
                    INSERT INTO airports (iata_code, name, country_code, latitude, longitude, is_active, source)
                    VALUES (:iata, :name, :cc, :lat, :lon, 1, 'ourairports')
                    ON DUPLICATE KEY UPDATE
                      name=VALUES(name),
                      country_code=VALUES(country_code),
                      latitude=VALUES(latitude),
                      longitude=VALUES(longitude),
                      is_active=1
                """), {"iata": iata, "name": name, "cc": country, "lat": lat, "lon": lon})
                inserted += 1

    return f"OK: imported/updated {inserted} airport rows"

if __name__ == "__main__":
    print(run())