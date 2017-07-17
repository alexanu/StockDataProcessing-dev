import psycopg2
from Conf import DbConfig, Config


# make connection between python and postgresql

dbConf = DbConfig.DbConfig()
conf = Config.Config()
connect = psycopg2.connect(database=dbConf.dbname, user=dbConf.user, host=dbConf.address, password=dbConf.password)
cursor = connect.cursor()

tName = conf.insName.lower()
query = 'SELECT * FROM {0} ORDER BY datetimestamp'.format(tName)

outputquery = 'copy ({0}) to stdout with csv header'.format(query)

with open('./Data/{0}.csv'.format(tName), 'w') as f:
    cursor.copy_expert(outputquery, f)

connect.close()
