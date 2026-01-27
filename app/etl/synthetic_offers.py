import random

def generate_synthetic_offers(n: int = 10):
    offers = []
    for _ in range(n):
        stops = random.choices([0,1,2], weights=[0.6,0.3,0.1])[0]
        duration_min = random.randint(70, 260) + stops * random.randint(40, 120)
        base = random.uniform(60, 230) + stops * random.uniform(15, 90)
        base += (duration_min / 60.0) * random.uniform(4, 16)
        price_total = round(base, 2)
        carrier = random.choice(["LO","LH","AF","KL","IB","AZ","SK","FR","W6"])
        offers.append({
            "price_total": price_total,
            "currency": "EUR",
            "stops": stops,
            "duration_min": duration_min,
            "carrier_code": carrier
        })
    return offers
