import requests

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY = "temperature_2m,wind_speed_10m,precipitation,visibility"

def fetch_hourly(lat: float, lon: float, start_date: str, end_date: str, mode: str):
    """
    mode: 'historical' uses ARCHIVE_URL, 'forecast' uses FORECAST_URL
    """
    base = ARCHIVE_URL if mode == "historical" else FORECAST_URL
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": HOURLY,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "UTC"
    }
    r = requests.get(base, params=params, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Open-Meteo {mode} HTTP {r.status_code}: {r.text[:800]}")
    return r.json()