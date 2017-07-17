import psycopg2
from Conf import DbConfig, Config
from datetime import datetime, timedelta

def checkDB_for_period():
    conf = Config.Config()
    dbConf = DbConfig.DbConfig()
    connect = psycopg2.connect(database=dbConf.dbname, user=dbConf.user, host=dbConf.address, password=dbConf.password)
    cursor = connect.cursor()
    cursor.itersize = 2000

    candleDiff = conf.candleDiff
    if conf.candlePeriod == 'M':
        candleDiff = candleDiff * 60
    if conf.candlePeriod == 'H':
        candleDiff = candleDiff * 3600

    print('Successfully connected')
    tName = conf.insName.lower()

    cmd = 'SELECT * FROM {0} ORDER BY datetimestamp;'.format(tName)
    cursor.execute(cmd)

    lastTimeStamp = datetime.min
    error = False
    for row in cursor:
        timeStamp = row[0]
        if lastTimeStamp!=datetime.min:
            delta = timeStamp - lastTimeStamp
            if delta != timedelta(seconds=candleDiff):
                print('Error: difference in time is ', delta, row)
                error = True
                # break
                connect.close()
                return delta, row
        lastTimeStamp = timeStamp
    connect.close()
    return 'OK'

# delta1, row1 = checkDB_for_period()

def fix_missing(delta, row):
    conf = Config.Config()
    dbConf = DbConfig.DbConfig()
    candleDiff = conf.candleDiff
    if conf.candlePeriod == 'M':
        candleDiff = candleDiff * 60
    if conf.candlePeriod == 'H':
        candleDiff = candleDiff * 3600

    tName = conf.insName.lower()
    cmd = ('INSERT INTO {0} VALUES').format(tName)
    cmd_bulk = ''
    dumpback = delta
    mcount = 0
    md = row[0]

    while dumpback > timedelta(seconds=candleDiff):
        #cmdel = ('DELETE FROM {0} WHERE ').format(tName)
        md -= timedelta(seconds=candleDiff)
        #cmdel = cmdel + ("(datetimestamp) = '{0}';".format(md))
        cmd_bulk = cmd_bulk + ("(TIMESTAMP '{0}',{1},{2},{3}),\n"
                               .format(md, row[1], row[2], row[3]))
        print(md)
        #connect = psycopg2.connect(database=dbConf.dbname, user=dbConf.user, host=dbConf.address, password=dbConf.password)
        #curdel = connect.cursor()
        #print(cmdel)
        #curdel.execute(cmdel)
        #connect.close()

        dumpback -= timedelta(seconds=candleDiff)
        mcount += 1


    connect = psycopg2.connect(database=dbConf.dbname, user=dbConf.user, host=dbConf.address, password=dbConf.password)
    cursor = connect.cursor()

    if len(cmd_bulk) > 0:
        cmd = cmd + cmd_bulk[:-2] + ';'
        cursor.execute(cmd)
        print("Вставка пропушенных. Количество: ", mcount)
        print("Цикл на ", row[0])
        connect.commit()
        connect.close()
    else:
        print("Нет пропущенных")
    print("Вставка пропущенных завершена")


while checkDB_for_period() != 'OK':
    delta1, row1 = checkDB_for_period()
    fix_missing(delta1, row1)


