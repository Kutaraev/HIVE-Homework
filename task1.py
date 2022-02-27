from pyhive import hive

host = 'sandbox-hdp.hortonworks.com'
port = '10000'
user = 'hive'
password = 'hive'
db = 'default'

query = '''SELECT hotel_country, COUNT(is_booking) AS booking_times
 FROM train GROUP BY hotel_country ORDER BY booking_times DESC LIMIT 3
'''

conn = hive.Connection(host=host, port=port, username=user, password=password,
                       database=db, auth='CUSTOM')
cursor = conn.cursor()
cursor.execute(query)
result = cursor.fetchall()
cursor.close()
conn.close()

print('Top-3 popular countries with successful booking:')
print('First place: country ' + str(result[0][0]) + ' with booking ' + str(result[0][1]))
print('Second place: country ' + str(result[1][0]) + ' with booking ' + str(result[1][1]))
print('Third place: country ' + str(result[2][0]) + ' with booking ' + str(result[2][1]))
