import pandas as pd
from hdfs import InsecureClient
from pyhive import hive


host = 'sandbox-hdp.hortonworks.com'
port = '10000'
user = 'hive'
password = 'hive'
db = 'default'

# running DDL commands
def hql(command):
    conn = hive.Connection(host=host, port=port, username=user, password=password,
                           database=db, auth='CUSTOM')
    cursor = conn.cursor()
    cursor.execute(command)
    cursor.close()
    conn.close()

# creating local csv-file with limit of rows
def local_csv_limit(name, limit):
    client_hdfs = InsecureClient('http://sandbox-hdp.hortonworks.com:50070')
    hdfs_path = '/user/root/data/' + name
    local_path = '/home/hive/' + name
    with client_hdfs.read(hdfs_path, encoding='utf-8') as reader:
        df = pd.read_csv(reader, skiprows=1, nrows=limit)
    df.to_csv(local_path)
    
table = """CREATE TABLE IF NOT EXISTS default.train (
 id int,
 date_time string,
 site_name int,
 posa_continent int,
 user_location_country int,
 user_location_region int,
 user_location_city int,
 orig_destination_distance double,
 user_id int,
 is_mobile tinyint,
 is_package int,
 channel int,
 srch_ci string,
 srch_co string,
 srch_adults_cnt int,
 srch_children_cnt int,
 srch_rm_cnt int,
 srch_destination_id int,
 srch_destination_type_id int,
 is_booking tinyint,
 cnt bigint,
 hotel_continent int,
 hotel_country int,
 hotel_market int,
 hotel_cluster int )
 COMMENT 'Train Table'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 STORED AS TEXTFILE
"""

load_data = "LOAD DATA LOCAL INPATH 'train.csv' INTO TABLE default.train"

print('***Creating table***')
hql(table)
print('***table created***')
print('***Creating limited local csv***')
local_csv_limit('train.csv', 300)
print('***Local csv created***')
print('***Loading data into table***')
hql(load_data)
print('***Data loaded!***')
