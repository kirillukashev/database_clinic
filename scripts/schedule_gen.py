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


first_day = datetime(2024, 4, 1, 0, 0)

day_start = timedelta(hours=8)
day_end = timedelta(hours=18)
interval_len = timedelta(minutes=30)
doc_len = get_doc_len()


for day in range(60):
    if (first_day + timedelta(days=day)).weekday() in [5, 6]:
        continue

    intervals = get_intervals(first_day + timedelta(days=day) + day_start,
                              first_day + timedelta(days=day) + day_end, interval_len)

    for doc in range(doc_len):
        comps = get_competences(doc + 1)
        comps_per_day = randint(0, 3)
        chosen_comps = choices(comps, k=comps_per_day)

        chosen_changes = sorted(
            choices(range(len(intervals)), k=comps_per_day))

        cur_change = 0
        cur_comp = -1

        if len(chosen_comps) == 0:
            continue

        for i, inter in enumerate(intervals):
            if chosen_changes[cur_change] == i:
                cur_change += 1
                cur_comp += 1

            if cur_change == len(chosen_changes):
                break

            query = f"""
                            INSERT INTO medical_center.Schedule
                            (competence_id, start_timestamp, end_timestamp)
                            VALUES
                            ({chosen_comps[cur_comp]}, '{inter[0]}', '{inter[1]}')"""
            cur.execute(query)

conn.commit()
conn.close()
