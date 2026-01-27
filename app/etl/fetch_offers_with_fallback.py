from datetime import datetime, date
from sqlalchemy import text
from app.db.connection import get_engine
from app.api.amadeus_offers_client import search_offers
from app.etl.synthetic_offers import generate_synthetic_offers

def parse_offer(offer: dict):
    price = offer.get("price", {}).get("total")
    currency = offer.get("price", {}).get("currency", "EUR")
    itineraries = offer.get("itineraries", [])
    segs = itineraries[0].get("segments", []) if itineraries else []
    stops = max(0, len(segs) - 1)
    duration_min = 120
    carrier = segs[0].get("carrierCode") if segs else None
    if price is None:
        return None
    return {
        "price_total": float(price),
        "currency": currency,
        "stops": int(stops),
        "duration_min": int(duration_min),
        "carrier_code": carrier
    }

def run(origin: str, dest: str, depart_date: str, adults: int = 1, fallback_n: int = 10) -> str:
    # walidacja daty (żeby nie generować “syntetyków” na złych wejściach)
    d = datetime.strptime(depart_date, "%Y-%m-%d").date()
    if d < date.today():
        return f"INVALID_INPUT: depart_date={depart_date} is in the past"

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO amadeus_offer_requests (origin_iata, dest_iata, depart_date, adults, status, offers_cnt)
            VALUES (:o, :d, :dt, :a, 'fallback', 0)
            ON DUPLICATE KEY UPDATE request_id=request_id
        """), {"o": origin, "d": dest, "dt": depart_date, "a": adults})

        request_id = conn.execute(text("""
            SELECT request_id FROM amadeus_offer_requests
            WHERE origin_iata=:o AND dest_iata=:d AND depart_date=:dt AND adults=:a
        """), {"o": origin, "d": dest, "dt": depart_date, "a": adults}).scalar()

    offers_out = []
    status = "ok"
    error_msg = None
    source = "amadeus"

    try:
        data = search_offers(origin, dest, depart_date, adults=adults, max_results=20)
        parsed = [parse_offer(x) for x in data]
        offers_out = [x for x in parsed if x is not None]

        if len(offers_out) == 0:
            status = "fallback"
            source = "synthetic"
            error_msg = "0 offers from Amadeus"
            offers_out = generate_synthetic_offers(fallback_n)

    except Exception as e:
        msg = str(e)
        if "INVALID DATE" in msg or "in the past" in msg:
            status = "invalid_input"
            source = "synthetic"
            error_msg = "INVALID DATE (past)"
            offers_out = []  # tu nie generujemy
        else:
            status = "fallback"
            source = "synthetic"
            error_msg = "429 Too Many Requests" if ("429" in msg or "Too Many Requests" in msg) else msg[:350]
            offers_out = generate_synthetic_offers(fallback_n)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM amadeus_flight_offers WHERE request_id=:rid"), {"rid": request_id})

        for o in offers_out:
            conn.execute(text("""
                INSERT INTO amadeus_flight_offers
                  (request_id, source, price_total, currency, stops, duration_min, carrier_code)
                VALUES (:rid, :src, :price, :cur, :stops, :dur, :car)
            """), {
                "rid": request_id,
                "src": source,
                "price": o["price_total"],
                "cur": o.get("currency", "EUR"),
                "stops": o["stops"],
                "dur": o["duration_min"],
                "car": o.get("carrier_code")
            })

        conn.execute(text("""
            UPDATE amadeus_offer_requests
            SET status=:st, offers_cnt=:cnt, error_msg=:err
            WHERE request_id=:rid
        """), {"st": status, "cnt": len(offers_out), "err": error_msg, "rid": request_id})

    if status == "invalid_input":
        return f"INVALID_INPUT: Amadeus rejected request ({error_msg}) {origin}-{dest} {depart_date}"
    if source == "synthetic":
        return f"FALLBACK: saved {len(offers_out)} synthetic offers for {origin}-{dest} {depart_date} (reason={error_msg})"
    return f"OK: saved {len(offers_out)} amadeus offers for {origin}-{dest} {depart_date}"
