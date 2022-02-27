from pyhive import hive

host = 'sandbox-hdp.hortonworks.com'
port = '10000'
user = 'hive'
password = 'hive'
db = 'default'

query = '''SELECT hotel_continent, hotel_country, hotel_market, COUNT(1) AS searches
 FROM train WHERE is_booking=0
 GROUP BY hotel_continent, hotel_country, hotel_market ORDER BY searches DESC LIMIT 3
'''

conn = hive.Connection(host=host, port=port, username=user, password=password,
                       database=db, auth='CUSTOM')
cursor = conn.cursor()
cursor.execute(query)
result = cursor.fetchall()
cursor.close()
conn.close()

print('Top 3 most popular not booked hotels:')
print(result)
