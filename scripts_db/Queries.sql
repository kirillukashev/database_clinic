-- Подсчёт доходов за промежуток времени
SELECT
  SUM(s.price) AS total_income
FROM
  Appointments a
  JOIN Schedule sch ON a.window_id = sch.window_id
  JOIN Competence c ON sch.competence_id = c.competence_id
  JOIN Services s ON c.service_id = s.service_id
WHERE
    sch.start_timestamp BETWEEN '2024-01-01' AND '2024-12-31';

-- Выборка расходов на персонал за указанный период времени
SELECT
  d.name AS doctor_name,
  CAST(SUM(EXTRACT(EPOCH FROM (s.end_timestamp - s.start_timestamp)) / 3600 * d.price_for_hour) AS INT) AS total_expenses
FROM
  Doctors AS d
  JOIN Competence AS c ON d.doctor_id = c.doctor_id
  JOIN Schedule AS s ON c.competence_id = s.competence_id
WHERE
    s.start_timestamp BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY
    d.name
ORDER BY d.name;

-- Список услуг и средней стоимости каждой услуги
SELECT
  s.name AS service_name,
  s.price AS average_price
FROM
  Services s
  JOIN
  Competence c ON s.service_id = c.service_id
GROUP BY
    s.service_id;

--  Информация по клиентам и врачу для него
SELECT
  Clients.name AS client_name,
  Doctors.name AS doctor_name,
  Services.name AS service_name,
  SUM(Services.price) AS total_spent
FROM
  Clients
  JOIN Appointments ON Clients.client_id = Appointments.client_id
  JOIN Schedule ON Appointments.window_id = Schedule.window_id
  JOIN Competence ON Schedule.competence_id = Competence.competence_id
  JOIN Doctors ON Competence.doctor_id = Doctors.doctor_id
  JOIN Services ON Competence.service_id = Services.service_id
WHERE
    Doctors.doctor_id = 5
GROUP BY
    Clients.name, Doctors.name, Services.name;

-- Отсортированный список услуг по количеству поинтов на каждую услугу
SELECT
  Services.name AS service_name,
  COUNT(Appointments.id) AS appointments_count
FROM
  Services
  LEFT JOIN Competence ON Services.service_id = Competence.service_id
  LEFT JOIN Schedule ON Competence.competence_id = Schedule.competence_id
  LEFT JOIN Appointments ON Schedule.window_id = Appointments.window_id
GROUP BY
    Services.service_id
ORDER BY
    appointments_count DESC;

-- Клиенты с количеством посещение больше среднего
SELECT
  Clients.name AS client_name,
  COUNT(Appointments.id) AS appointments_count
FROM
  Clients
  LEFT JOIN Appointments ON Clients.client_id = Appointments.client_id
GROUP BY
    Clients.client_id
HAVING
    COUNT(Appointments.id) > (
      SELECT AVG(appointments_count)
      FROM (
        SELECT
          COUNT(id) AS appointments_count
        FROM
          Appointments
        GROUP BY client_id
      ) AS subquery
    );

-- Врач с самой высокой средней стоимостью услуг
SELECT
  doctor_id,
  name,
  ROUND(average_service_price) AS average_service_price
FROM
  (
    SELECT
      d.doctor_id,
      d.name,
      AVG(s.price) AS average_service_price
    FROM
      Doctors d
      JOIN Competence c ON d.doctor_id = c.doctor_id
      JOIN Services s ON c.service_id = s.service_id
    GROUP BY
      d.doctor_id, d.name
  ) AS doctor_service_prices
WHERE
  average_service_price = (
    SELECT MAX(avg_price)
    FROM (
      SELECT
        AVG(s.price) AS avg_price
      FROM
        Doctors d
        JOIN Competence c ON d.doctor_id = c.doctor_id
        JOIN Services s ON c.service_id = s.service_id
      GROUP BY
        d.doctor_id
    ) AS max_avg_price
  );

-- Средний возраст клиентов, посещающих каждую услугу
SELECT
  Services.name AS service_name,
  ROUND(AVG(EXTRACT(YEAR FROM AGE(Clients.birth_date)))) AS average_age
FROM
  Services
  JOIN Competence ON Services.service_id = Competence.service_id
  JOIN Schedule ON Competence.competence_id = Schedule.competence_id
  JOIN Appointments ON Schedule.window_id = Appointments.window_id
  JOIN Clients ON Appointments.client_id = Clients.client_id
GROUP BY
    Services.service_id;

-- Рейтинг клиентов по тратам
SELECT
  client_id,
  name,
  total_spent,
  RANK() OVER (ORDER BY total_spent DESC) AS spending_rank
FROM
  (
    SELECT
      Clients.client_id,
      Clients.name,
      SUM(Services.price) AS total_spent
    FROM
      Clients
      JOIN Appointments ON Clients.client_id = Appointments.client_id
      JOIN Schedule ON Appointments.window_id = Schedule.window_id
      JOIN Competence ON Schedule.competence_id = Competence.competence_id
      JOIN Services ON Competence.service_id = Services.service_id
    GROUP BY
      Clients.client_id, Clients.name
  ) AS client_spending;

-- Клиент и количество поинтов для него
SELECT
  c.name AS client_name,
  COUNT(a.id) AS appointments_count
FROM
  Clients c
  LEFT JOIN
  Appointments a ON c.client_id = a.client_id
GROUP BY
    c.client_id
ORDER BY
    appointments_count DESC;