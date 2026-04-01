from faker import Faker
from lxml import etree
import random
import cx_Oracle

# Connexion Oracle
dsn = cx_Oracle.makedsn("localhost", 1521, service_name="XE")  # modifie host/service
conn = cx_Oracle.connect(user="C##DBZUSER", password="dbz", dsn=dsn)
cur = conn.cursor()

# Vérifier si le table ACCOUNT existe dans le schéma OT
cur.execute("""
    SELECT COUNT(*) 
    FROM all_tables 
    WHERE table_name = 'ACCOUNT' 
      AND owner = 'OT'
""")
exists = cur.fetchone()[0]

if exists == 0:
    print("Table ACCOUNT n'existe pas dans OT, création...")
    cur.execute("""
        CREATE TABLE OT.ACCOUNT (
            RECID NUMBER PRIMARY KEY,
            XMLRECORD CLOB
        )
    """)
    print("Table ACCOUNT créée avec succès !")
else:
    print("Table ACCOUNT existe déjà.")

# Faker pour générer des données
fake = Faker('fr_FR')

def generate_t24_record(recid):
    nsmap = {'xml': 'http://www.w3.org/XML/1998/namespace'}
    row = etree.Element("row", id=str(recid), nsmap=nsmap)
    row.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")

    c1 = etree.SubElement(row, "c1")
    c1.text = f"C{100000 + recid}"

    c2 = etree.SubElement(row, "c2")
    c2.text = fake.first_name()

    c3 = etree.SubElement(row, "c3")
    c3.text = fake.last_name()

    c5 = etree.SubElement(row, "c5")
    c5.text = fake.street_address()

    c7 = etree.SubElement(row, "c7")
    c7.text = fake.city()

    c8 = etree.SubElement(row, "c8")
    c8.text = fake.postcode()

    c31 = etree.SubElement(row, "c31")
    c31.text = fake.date_between(start_date='-20y', end_date='today').strftime('%Y%m%d')

    c48 = etree.SubElement(row, "c48")
    c48.text = f"TN{recid:07d}"

    c101 = etree.SubElement(row, "c101")
    c101.text = random.choice(["ACTIVE", "INACTIVE"])

    c137 = etree.SubElement(row, "c137")
    c137.text = random.choice(["YES", "NO"])

    return etree.tostring(row, encoding='unicode', pretty_print=False)

# Génération et insertion
N = 1000
for recid in range(1, N + 1):
    xml_record = generate_t24_record(recid)
    cur.execute(
        "INSERT INTO OT.ACCOUNT (RECID, XMLRECORD) VALUES (:1, :2)",
        (recid, xml_record)
    )
    if recid % 100 == 0:
        print(f"{recid} lignes insérées...")

conn.commit()
cur.close()
conn.close()
print(f"{N} lignes insérées dans OT.ACCOUNT avec succès !")