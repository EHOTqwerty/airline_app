import requests
from app.config.settings import AMADEUS_BASE_URL, AMADEUS_API_KEY, AMADEUS_API_SECRET

def get_token() -> str:
    if not AMADEUS_API_KEY or not AMADEUS_API_SECRET:
        raise RuntimeError("Missing AMADEUS_API_KEY / AMADEUS_API_SECRET")

    r = requests.post(
        f"{AMADEUS_BASE_URL}/v1/security/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": AMADEUS_API_KEY,
            "client_secret": AMADEUS_API_SECRET
        },
        timeout=20
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Amadeus token HTTP {r.status_code}: {r.text[:1200]}")
    return r.json()["access_token"]

def search_offers(origin: str, dest: str, depart_date: str, adults: int = 1, max_results: int = 20):
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": dest,
        "departureDate": depart_date,
        "adults": adults,
        "max": max_results
    }
    r = requests.get(
        f"{AMADEUS_BASE_URL}/v2/shopping/flight-offers",
        headers=headers,
        params=params,
        timeout=30
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Amadeus HTTP {r.status_code}: {r.text[:1200]}")
    return r.json().get("data", [])
