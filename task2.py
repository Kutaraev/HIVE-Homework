from pyhive import hive

host = 'sandbox-hdp.hortonworks.com'
port = '10000'
user = 'hive'
password = 'hive'
db = 'default'

query = '''SELECT DATEDIFF(srch_co, srch_ci) AS longest_period
 FROM train WHERE srch_children_cnt > 0 ORDER by longest_period DESC LIMIT 1
'''

conn = hive.Connection(host=host, port=port, username=user, password=password,
                       database=db, auth='CUSTOM')
cursor = conn.cursor()
cursor.execute(query)
result = cursor.fetchall()
cursor.close()
conn.close()

print('Longest period of stay of couples with children: ' + str(result[0][0]))
