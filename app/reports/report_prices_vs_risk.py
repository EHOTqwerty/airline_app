from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import text
from app.db.connection import get_engine

REPORT_DIR = Path("reports")

def main(country_code: str):
    REPORT_DIR.mkdir(exist_ok=True)

    q = text("""
    SELECT
        COALESCE(r.risk_level,'LOW') AS risk_level,
        o.source,
        COUNT(*) AS offers_cnt,
        ROUND(AVG(o.price_total),2) AS avg_price
    FROM amadeus_flight_offers o
    JOIN amadeus_offer_requests req ON req.request_id = o.request_id
    JOIN airports a ON a.iata_code = req.origin_iata
    LEFT JOIN weather_risk_daily r
      ON r.iata_code = req.origin_iata
     AND r.day = req.depart_date
     AND r.source = 'forecast'
    WHERE a.country_code = :cc
    GROUP BY COALESCE(r.risk_level,'LOW'), o.source
    ORDER BY FIELD(COALESCE(r.risk_level,'LOW'),'LOW','MEDIUM','HIGH'), o.source
    """)

    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"cc": country_code})

    csv_path = REPORT_DIR / f"prices_vs_risk_{country_code}.csv"
    df.to_csv(csv_path, index=False)

    png_path = ""
    if not df.empty:
        pivot = df.pivot(index="risk_level", columns="source", values="avg_price").fillna(0)
        ax = pivot.plot(kind="bar")
        ax.set_title(f"Ceny ofert vs ryzyko (forecast) - {country_code}")
        ax.set_xlabel("Risk level")
        ax.set_ylabel("Avg price")
        plt.tight_layout()
        png = REPORT_DIR / f"prices_vs_risk_{country_code}.png"
        plt.savefig(png, dpi=160)
        plt.close()
        png_path = str(png)

    html_path = REPORT_DIR / f"prices_vs_risk_{country_code}.html"
    html = []
    html.append(f"<h2>Ceny ofert vs ryzyko pogody (prognoza) - {country_code}</h2>")
    if png_path:
        html.append(f"<img src='{Path(png_path).name}' style='max-width:1000px;'>")
    html.append(df.to_html(index=False))
    html_path.write_text("\n".join(html), encoding="utf-8")

    return str(csv_path), png_path, str(html_path)
