import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
from random import randint
from random import choices

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="1234",
    host="127.0.0.1"
)

cur = conn.cursor()


def intervals_do_not_intersect(start1, end1, start2, end2):
    return end1 < start2 or start1 > end2


def fit_in_sched(start, end, sched):
    for interval in sched:
        if not intervals_do_not_intersect(start, end, *interval):
            return False
    return True


def get_competences(doc_id):
    cur.execute(f"""
    SELECT c.competence_id
    FROM medical_center.Competence c
    JOIN medical_center.Doctors d ON c.doctor_id = d.doctor_id
    WHERE d.doctor_id = {doc_id};
  """)
    rows = cur.fetchall()
    return [i[0] for i in rows]


def get_intervals(start, end, len):
    prev_end = start
    ret = []
    while prev_end != end:
        ret.append((prev_end, prev_end + len))
        prev_end += len

    return ret


def get_doc_len():
    cur.execute("SELECT COUNT(*) FROM medical_center.Doctors")
    return cur.fetchone()[0]


def get_serv_len():
    cur.execute("SELECT COUNT(*) FROM medical_center.Services")
    return cur.fetchone()[0]


def get_clients_len():
    cur.execute("SELECT COUNT(*) FROM medical_center.Clients")
    return cur.fetchone()[0]


def get_schedule_len():
    cur.execute("SELECT COUNT(*) FROM medical_center.Schedule")
    return cur.fetchone()[0]


def get_interval(id):
    cur.execute(
        f"SELECT (start_timestamp, end_timestamp) FROM medical_center.Schedule WHERE window_id = {id}")
    return cur.fetchone()[0]


clients_len = get_clients_len()
schedule_len = get_schedule_len()

busy_intervals = [[] for i in range(clients_len)]

for sched_id in range(1, schedule_len + 1):
    inter = get_interval(sched_id)
    client = randint(0, clients_len)

    while client != 0 and inter in busy_intervals[client - 1]:
        client = randint(0, clients_len)

    if client == 0:
        continue

    busy_intervals[client - 1].append(inter)

    query = f"""INSERT INTO medical_center.Appointments
              (window_id, client_id)
              VALUES ({sched_id}, {client})"""

    cur.execute(query)


conn.commit()
conn.close()
