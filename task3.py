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
print('Hotel 1: ' + str(result[0][0]) +', '+ str(result[0][1]) +',
 '+ str(result[0][2]) + ' with searches ' + str(result[0][3]))
print('Hotel 2: ' + str(result[1][0]) +', '+ str(result[1][1]) +',
 '+ str(result[1][2]) + ' with searches ' + str(result[1][3]))
print('Hotel 3: ' + str(result[2][0]) +', '+ str(result[2][1]) +',
 '+ str(result[2][2]) + ' with searches ' + str(result[2][3]))
