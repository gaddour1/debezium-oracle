import cx_Oracle
import time
import random
import threading
import streamlit as st

# --- Paramètres Oracle ---
dsn = cx_Oracle.makedsn("localhost", 1521, service_name="XE")
conn = cx_Oracle.connect("C##DBZUSER", "dbz", dsn)

# --- Variables globales pour métriques ---
total_dml = 0
total_tx = 0
response_times = []

# --- Fonction pour exécuter DML en boucle ---
def worker(thread_id):
    global total_dml, total_tx, response_times
    cur = conn.cursor()
    while True:
        start = time.time()
        # Choisir aléatoirement INSERT, UPDATE, DELETE, SELECT
        op = random.choice(["INSERT","UPDATE","DELETE","SELECT"])
        if op == "INSERT":
            cur.execute("INSERT INTO ACCOUNT (RECID, XMLRECORD) VALUES ((SELECT NVL(MAX(RECID),0)+1 FROM ACCOUNT), '<row><c1>{}</c1></row>')".format(random.randint(1,10000)))
        elif op == "UPDATE":
            cur.execute("UPDATE ACCOUNT SET XMLRECORD = '<row><c1>updated</c1></row>' WHERE ROWNUM=1")
        elif op == "DELETE":
            cur.execute("DELETE FROM ACCOUNT WHERE ROWNUM=1")
        elif op == "SELECT":
            cur.execute("SELECT * FROM ACCOUNT WHERE ROWNUM=1")
        conn.commit()
        end = time.time()
        response_times.append((end-start)*1000)
        total_dml += 1
        total_tx += 1
        time.sleep(0.1)

# --- Lancer plusieurs threads ---
for i in range(4):  # 4 utilisateurs simultanés
    threading.Thread(target=worker, args=(i,), daemon=True).start()

# --- Dashboard Streamlit ---
st.title("Oracle Table Performance Monitor")

while True:
    st.metric("Transactions/sec", total_tx)
    st.metric("DML/sec", total_dml)
    st.metric("Response Time (ms)", round(sum(response_times[-20:])/20, 2) if response_times else 0)
    time.sleep(1)

