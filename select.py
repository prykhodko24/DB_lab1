import psycopg2
import csv
import os

result = 'marks.csv'
QUERY = '''
    SELECT 
        zno_year,
        REGNAME,
        max(engBall100), 
        max(engBall12)
    FROM zno 
    WHERE engTestStatus = 'Зараховано' 
    GROUP BY zno_year, REGNAME
'''
COLUMNS = ['Year', 'Region', 'ZNO Grade', 'DPA Grade']


def select(conn):
    cur = conn.cursor()

    cur.execute(QUERY)
    res = cur.fetchall()

    with open(os.path.join('data', result), 'w', newline='') as csvf:
        csv_writer = csv.writer(csvf, dialect='excel')
        csv_writer.writerow(COLUMNS)
        csv_writer.writerows(res)

    cur.close()

conn = psycopg2.connect("dbname=postgres user=postgres password=password")
select(conn)
conn.close()
