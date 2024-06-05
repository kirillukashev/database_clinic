CREATE SCHEMA IF NOT EXISTS medical_center

CREATE TABLE IF NOT EXISTS Clients
(
  client_id SERIAL PRIMARY KEY,
  name VARCHAR(100),
  birth_date DATE,
  gender VARCHAR(1),
  phone_number VARCHAR(20),
  email VARCHAR(100)
)

CREATE TABLE IF NOT EXISTS Services
(
  service_id SERIAL PRIMARY KEY,
  name VARCHAR(100) UNIQUE,
  price INT
)

CREATE TABLE IF NOT EXISTS Doctors
(
  doctor_id SERIAL PRIMARY KEY,
  name VARCHAR(100),
  birth_date DATE,
  gender VARCHAR(5),
  phone_number VARCHAR(20),
  email VARCHAR(100),
  price_for_hour INT
)

CREATE TABLE IF NOT EXISTS Competence
(
  competence_id SERIAL PRIMARY KEY,
  service_id INT,
  doctor_id INT,
  FOREIGN KEY (service_id) REFERENCES Services(service_id),
  FOREIGN KEY (doctor_id) REFERENCES Doctors(doctor_id)
)


CREATE TABLE IF NOT EXISTS Schedule
(
  window_id SERIAL PRIMARY KEY,
  competence_id INT,
  start_timestamp TIMESTAMP,
  end_timestamp TIMESTAMP,
  FOREIGN KEY (competence_id) REFERENCES Competence(competence_id)
)

CREATE TABLE IF NOT EXISTS Appointments
(
  id SERIAL PRIMARY KEY,
  window_id INT,
  client_id INT,
  FOREIGN KEY (client_id) REFERENCES Clients (client_id),
  FOREIGN KEY (window_id) REFERENCES Schedule(window_id)
)
