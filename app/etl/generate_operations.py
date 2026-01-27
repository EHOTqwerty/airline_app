import random
from datetime import datetime, timedelta
from sqlalchemy import text
from app.db.connection import get_engine
from app.config.top_airports import TOP_AIRPORTS

FIRST_NAMES = ["Jan","Adam","Piotr","Anna","Maria","Katarzyna","Julia","Paweł","Tomasz","Michał"]
LAST_NAMES = ["Kowalski","Nowak","Wiśniewski","Wójcik","Kowalczyk","Kamiński","Lewandowski","Zieliński","Szymański"]

def run(country_code: str, start_date: str, end_date: str, flights_per_day: int = 10):
    engine = get_engine()

    origins = TOP_AIRPORTS.get(country_code, [])[:3]
    if len(origins) < 1:
        return f"EMPTY: no TOP airports for {country_code}"

    # weź kilka destów z innych krajów
    all_keys = list(TOP_AIRPORTS.keys())
    random.shuffle(all_keys)
    dests = []
    for cc in all_keys:
        if cc != country_code and TOP_AIRPORTS.get(cc):
            dests.extend(TOP_AIRPORTS[cc][:2])
        if len(dests) >= 6:
            break

    d1 = datetime.strptime(start_date, "%Y-%m-%d")
    d2 = datetime.strptime(end_date, "%Y-%m-%d")
    days = (d2.date() - d1.date()).days + 1

    created_flights = 0
    created_tickets = 0

    with engine.begin() as conn:
        for di in range(days):
            day = (d1 + timedelta(days=di)).date()
            for _ in range(flights_per_day):
                dep = random.choice(origins)
                arr = random.choice(dests)
                dep_time = datetime.combine(day, datetime.min.time()) + timedelta(hours=random.randint(6, 20), minutes=random.choice([0,15,30,45]))
                duration = timedelta(minutes=random.randint(80, 180))
                arr_time = dep_time + duration
                seats = random.choice([160,180,200])

                res = conn.execute(text("""
                    INSERT INTO flights (dep_iata, arr_iata, sched_dep, sched_arr, status, delay_min, seats)
                    VALUES (:dep, :arr, :sd, :sa, 'scheduled', 0, :seats)
                """), {"dep": dep, "arr": arr, "sd": dep_time, "sa": arr_time, "seats": seats})
                flight_id = res.lastrowid
                created_flights += 1

                # pasażerowie i bilety
                booking_id = conn.execute(text("INSERT INTO bookings () VALUES ()")).lastrowid

                load = random.uniform(0.55, 0.92)
                pax = int(seats * load)

                for _p in range(pax):
                    fn = random.choice(FIRST_NAMES)
                    ln = random.choice(LAST_NAMES)
                    pid = conn.execute(text("""
                        INSERT INTO passengers (first_name, last_name, nationality)
                        VALUES (:fn, :ln, :nat)
                    """), {"fn": fn, "ln": ln, "nat": country_code}).lastrowid

                    price = round(random.uniform(50, 220), 2)
                    cabin = "BUSINESS" if random.random() < 0.08 else "ECONOMY"

                    conn.execute(text("""
                        INSERT INTO tickets (booking_id, passenger_id, flight_id, price_eur, cabin)
                        VALUES (:b, :p, :f, :price, :cabin)
                    """), {"b": booking_id, "p": pid, "f": flight_id, "price": price, "cabin": cabin})
                    created_tickets += 1

    return f"OK: flights={created_flights}, tickets={created_tickets}"

if __name__ == "__main__":
    print(run("PL","2026-02-01","2026-02-03", flights_per_day=5))
