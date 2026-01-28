from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, date, timedelta
import hashlib
import random

@dataclass
class HourlyWeatherRow:
    dt_utc: datetime
    temperature_c: float
    windspeed_ms: float
    precipitation_mm: float
    visibility_m: float

def _stable_seed(*parts: str) -> int:
    s = "|".join(parts).encode("utf-8")
    h = hashlib.sha256(s).hexdigest()
    return int(h[:8], 16)

def generate_synthetic_hourly_weather(
    iata_code: str,
    start_date: date,
    end_date: date,
) -> list[HourlyWeatherRow]:
    """
    Generuje realistycznie wyglądające dane godzinowe dla zakresu [start_date, end_date].
    Deterministyczne: dla tego samego iata+zakres wyniki będą takie same (ważne do testów).
    """
    rng = random.Random(_stable_seed(iata_code, start_date.isoformat(), end_date.isoformat()))

    # Proste "klimatyczne" widełki dla Europy (można później ulepszyć).
    base_temp = rng.uniform(2.0, 18.0)         # średnia dzienna
    temp_amp = rng.uniform(4.0, 10.0)          # amplituda dobowa
    base_wind = rng.uniform(1.5, 7.0)
    rain_chance = rng.uniform(0.05, 0.35)

    rows: list[HourlyWeatherRow] = []

    cur = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    while cur <= end_dt:
        hour = cur.hour

        # Prosty cykl dobowy temperatury: chłodniej nocą, cieplej po południu
        # 14:00 ~ maksimum, 04:00 ~ minimum
        phase = (hour - 14) / 24.0 * 2 * 3.14159
        temp = base_temp + (-temp_amp) * (0.5 * (1.0 + (1.0 * (1.0 if phase > 0 else -1.0))))  # prosta asymetria
        # delikatny szum
        temp += rng.uniform(-1.2, 1.2)

        wind = max(0.0, base_wind + rng.uniform(-2.0, 4.0))

        # Opady: losowo, z “paczkami” deszczu
        if rng.random() < rain_chance:
            precip = max(0.0, rng.uniform(0.1, 6.0))
        else:
            precip = 0.0

        # Widzialność: spada przy opadach i przy większym wietrze
        visibility = 20000.0
        visibility -= precip * rng.uniform(800, 2500)
        visibility -= max(0.0, wind - 8.0) * rng.uniform(300, 1200)
        visibility = float(min(20000.0, max(800.0, visibility)))

        rows.append(
            HourlyWeatherRow(
                dt_utc=cur,
                temperature_c=float(temp),
                windspeed_ms=float(wind),
                precipitation_mm=float(precip),
                visibility_m=float(visibility),
            )
        )
        cur += timedelta(hours=1)

    return rows
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