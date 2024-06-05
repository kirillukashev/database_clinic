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

def get_doc_len():
    cur.execute("SELECT COUNT(*) FROM medical_center.Doctors")
    return cur.fetchone()[0]

def get_serv_len():
    cur.execute("SELECT COUNT(*) FROM medical_center.Services")
    return cur.fetchone()[0]

used_d = [False] * get_doc_len()
used_s = [False] * get_serv_len()

while not all(used_d) and not all(used_s):
  d, s = randint(1, len(used_d)), randint(1, len(used_s))
  used_d[d - 1] = True
  used_s[s - 1] = True

  query = f"""
    INSERT INTO medical_center.Competence
    (service_id, doctor_id)
    VALUES ({s}, {d})
"""
  cur.execute(query)

conn.commit()
conn.close()