import random
from sqlalchemy import text
from app.db.connection import get_engine

def run(country_code: str) -> str:
    engine = get_engine()
    updated = 0

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT f.flight_id, DATE(f.sched_dep) AS day, f.dep_iata,
                   COALESCE(r.risk_level,'LOW') AS risk_level
            FROM flights f
            JOIN airports a ON a.iata_code = f.dep_iata
            LEFT JOIN weather_risk_daily r
              ON r.iata_code = f.dep_iata
             AND r.day = DATE(f.sched_dep)
             AND r.source = 'forecast'
            WHERE a.country_code = :cc
              AND f.status = 'scheduled'
        """), {"cc": country_code}).fetchall()

        for flight_id, day, dep_iata, risk in rows:
            # probabilistyki (proste i czytelne w pracy)
            if risk == "HIGH":
                p_cancel = 0.08
                p_delay = 0.45
                delay_min = random.randint(30, 180)
            elif risk == "MEDIUM":
                p_cancel = 0.03
                p_delay = 0.25
                delay_min = random.randint(15, 90)
            else:
                p_cancel = 0.01
                p_delay = 0.12
                delay_min = random.randint(5, 40)

            rnd = random.random()
            if rnd < p_cancel:
                conn.execute(text("""
                    UPDATE flights SET status='cancelled', delay_min=0 WHERE flight_id=:id
                """), {"id": flight_id})
            elif rnd < p_cancel + p_delay:
                conn.execute(text("""
                    UPDATE flights SET status='delayed', delay_min=:d WHERE flight_id=:id
                """), {"id": flight_id, "d": delay_min})
            updated += 1

    return f"OK: processed {updated} scheduled flights for impact"

if __name__ == "__main__":
    print(run("PL"))
