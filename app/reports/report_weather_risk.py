from pathlib import Path
import pandas as pd
from sqlalchemy import text
from app.db.connection import get_engine

REPORT_DIR = Path("reports")

def main(country_code: str):
    REPORT_DIR.mkdir(exist_ok=True)
    q = text("""
      SELECT a.iata_code, r.day, r.source, r.risk_score, r.risk_level
      FROM weather_risk_daily r
      JOIN airports a ON a.iata_code=r.iata_code
      WHERE a.country_code=:cc
      ORDER BY r.day DESC, r.risk_score DESC
      LIMIT 500
    """)
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"cc": country_code})
    csv_path = REPORT_DIR / f"weather_risk_{country_code}.csv"
    df.to_csv(csv_path, index=False)
    html_path = REPORT_DIR / f"weather_risk_{country_code}.html"
    html_path.write_text(df.to_html(index=False), encoding="utf-8")
    return str(csv_path), "", str(html_path)
