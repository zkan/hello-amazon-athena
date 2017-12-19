# Credit: https://medium.com/@devopsglobaleli/introduction-17b4d0c592b6
import boto3


s3_input = 's3://data-swarm/abc/'
s3_ouput = 's3://data-swarm/abc-results/'
database = 'data_swarm_test'
table = 'persons'


def run_query(query, database, s3_output):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        }
    )
    print('Execution ID: ' + response['QueryExecutionId'])
    return response


def upload_to_s3(from_='', bucket='', to=''):
    client = boto3.client('s3').upload_file(from_, bucket, to)


upload_to_s3(from_='data/data.json', bucket='data-swarm', to='abc/data.json')

create_database = f'CREATE DATABASE IF NOT EXISTS {database};'
create_table = \
    f"""CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (
    `name` string,
    `sex`string,
    `city` string,
    `country` string,
    `age` int,
    `job` string
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     WITH SERDEPROPERTIES (
     'serialization.format' = '1'
     ) LOCATION '{s3_input}'
     TBLPROPERTIES ('has_encrypted_data'='false');"""
query_1 = f"SELECT * FROM {database}.{table} where sex = 'F';"
query_2 = f"SELECT * FROM {database}.{table} where age > 30;"

queries = [create_database, create_table, query_1, query_2]
for q in queries:
   print("Executing query: %s" % (q))
   res = run_query(q, database, s3_ouput)
   print(res)
