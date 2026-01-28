USE airline_final;

-- Airports (baseline UE)
CREATE TABLE IF NOT EXISTS airports (
  iata_code CHAR(3) PRIMARY KEY,
  name VARCHAR(200) NULL,
  country_code CHAR(2) NOT NULL,
  latitude DOUBLE NULL,
  longitude DOUBLE NULL,
  is_active TINYINT NOT NULL DEFAULT 1,
  source VARCHAR(50) NOT NULL DEFAULT 'ourairports'
);

CREATE INDEX idx_airports_country ON airports(country_code);

-- Weather hourly (history + forecast in one table)
CREATE TABLE IF NOT EXISTS weather_hourly (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  iata_code CHAR(3) NOT NULL,
  dt_utc DATETIME NOT NULL,
  source ENUM('historical','forecast') NOT NULL,
  temperature_c DOUBLE NULL,
  windspeed_ms DOUBLE NULL,
  precipitation_mm DOUBLE NULL,
  visibility_m DOUBLE NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_weather (iata_code, dt_utc, source),
  FOREIGN KEY (iata_code) REFERENCES airports(iata_code)
);

CREATE INDEX idx_weather_iata_day ON weather_hourly(iata_code, dt_utc);
CREATE INDEX idx_weather_source ON weather_hourly(source);

-- Weather risk daily (also history + forecast)
CREATE TABLE IF NOT EXISTS weather_risk_daily (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  iata_code CHAR(3) NOT NULL,
  day DATE NOT NULL,
  source ENUM('historical','forecast') NOT NULL,
  risk_score DOUBLE NOT NULL,
  risk_level ENUM('LOW','MEDIUM','HIGH') NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_risk (iata_code, day, source),
  FOREIGN KEY (iata_code) REFERENCES airports(iata_code)
);

CREATE INDEX idx_risk_country_day ON weather_risk_daily(day, source);

-- Operations (synthetic)
CREATE TABLE IF NOT EXISTS flights (
  flight_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  dep_iata CHAR(3) NOT NULL,
  arr_iata CHAR(3) NOT NULL,
  sched_dep DATETIME NOT NULL,
  sched_arr DATETIME NOT NULL,
  status ENUM('scheduled','delayed','cancelled','completed') NOT NULL DEFAULT 'scheduled',
  delay_min INT NOT NULL DEFAULT 0,
  seats INT NOT NULL DEFAULT 180,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (dep_iata) REFERENCES airports(iata_code),
  FOREIGN KEY (arr_iata) REFERENCES airports(iata_code)
);

CREATE INDEX idx_flights_dep_day ON flights(dep_iata, sched_dep);

CREATE TABLE IF NOT EXISTS passengers (
  passenger_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(60) NOT NULL,
  last_name VARCHAR(60) NOT NULL,
  nationality CHAR(2) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bookings (
  booking_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tickets (
  ticket_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  booking_id BIGINT NOT NULL,
  passenger_id BIGINT NOT NULL,
  flight_id BIGINT NOT NULL,
  price_eur DECIMAL(10,2) NOT NULL,
  cabin ENUM('ECONOMY','BUSINESS') NOT NULL DEFAULT 'ECONOMY',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (booking_id) REFERENCES bookings(booking_id),
  FOREIGN KEY (passenger_id) REFERENCES passengers(passenger_id),
  FOREIGN KEY (flight_id) REFERENCES flights(flight_id)
);

CREATE INDEX idx_tickets_flight ON tickets(flight_id);

-- Amadeus offers + fallback
CREATE TABLE IF NOT EXISTS amadeus_offer_requests (
  request_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  origin_iata CHAR(3) NOT NULL,
  dest_iata CHAR(3) NOT NULL,
  depart_date DATE NOT NULL,
  adults INT NOT NULL DEFAULT 1,
  status ENUM('ok','fallback','invalid_input') NOT NULL,
  offers_cnt INT NOT NULL DEFAULT 0,
  error_msg VARCHAR(400) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_req (origin_iata, dest_iata, depart_date, adults)
);

CREATE TABLE IF NOT EXISTS amadeus_flight_offers (
  offer_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  request_id BIGINT NOT NULL,
  source ENUM('amadeus','synthetic') NOT NULL,
  price_total DECIMAL(10,2) NOT NULL,
  currency CHAR(3) NOT NULL DEFAULT 'EUR',
  stops INT NOT NULL,
  duration_min INT NOT NULL,
  carrier_code VARCHAR(3) NULL,
  fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (request_id) REFERENCES amadeus_offer_requests(request_id)
);

CREATE INDEX idx_offers_req ON amadeus_flight_offers(request_id);

ALTER TABLE amadeus_offer_requests
  ADD INDEX idx_aor_origin (origin_iata),
  ADD INDEX idx_aor_destination (dest_iata);

ALTER TABLE amadeus_offer_requests
  ADD CONSTRAINT fk_aor_origin_airport
  FOREIGN KEY (origin_iata)
  REFERENCES airports(iata_code)
  ON UPDATE CASCADE
  ON DELETE RESTRICT;

ALTER TABLE amadeus_offer_requests
  ADD CONSTRAINT fk_aor_destination_airport
  FOREIGN KEY (dest_iata)
  REFERENCES airports(iata_code)
  ON UPDATE CASCADE
  ON DELETE RESTRICT;