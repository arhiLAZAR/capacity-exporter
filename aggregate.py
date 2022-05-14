from pymysql import connect, cursors
from requests import get
import json
from os import getenv, utime, remove
from datetime import datetime,timedelta
from time import sleep

def touch(path):
    with open(path, 'a'):
        utime(path, None)

def remove_file(path):
    remove(path)

def create_table(con):
    '''
    Create local database to store numbers on daily basis
    '''
    con.ping()
    with con:
        cur = con.cursor()
        cur._defer_warnings = True
        cur.execute("CREATE TABLE IF NOT EXISTS capacity_avg (date DATE NOT NULL, component VARCHAR(16),capacity VARCHAR(256), utilization_cpu VARCHAR(256), utilization_memory VARCHAR(256), UNIQUE KEY `per_day` (`date`,`component`)) ENGINE=INNODB;")

def write_to_local_db(con,component,capacity,utilization):
    '''
    write daily information to local database
    '''
    con.ping()
    with con:
        cur = con.cursor()
        cur._defer_warnings = True
        cur.execute("INSERT IGNORE INTO capacity_avg(date,component, capacity,utilization_cpu, utilization_memory) VALUES(DATE_SUB(DATE(NOW()), INTERVAL 1 DAY),'{}','{}','{}','{}');".format(component, capacity, utilization['cpu'], utilization['mem']))

def get_util(offset=1):
    '''
    Get Average CPU and memory utilization for 1 day
    '''
    global CLUSTER

    prom_url = 'http://{}-kube-prometheus-nodes.pulsepoint.com/api/v1/query?'.format(CLUSTER)
    query_cpu = "cluster:cpu_utilization:ratio_rate1m[1d] offset {}d".format(offset)
    query_mem = "cluster:memory_available:ratio[1d] offset {}d".format(offset)

    avg_utilization = {}

    avg=[]
    response = get(prom_url, params={'query': query_cpu})
    if not response.json()['data']['result']:
        avg_utilization['cpu'] = 1
    else:
        for i in response.json()['data']['result'][0]['values']:
            avg.append(float(i[1]))
        avg_utilization['cpu'] = round((sum(avg)/len(avg))*100,2)


    avg=[]
    response = get(prom_url, params={'query': query_mem})
    if not response.json()['data']['result']:
        avg_utilization['mem'] = 1
    else:
        for i in response.json()['data']['result'][0]['values']:
            avg.append(float(i[1]))
        avg_utilization['mem'] = round((1-(sum(avg)/len(avg)))*100,2)

    return avg_utilization

def get_capacity(con,component,table,offset=-1,time=7):
    '''
    Get QPS from kuberentes for the cluster
    offset=-1 is till today.
    '''
    first_date = datetime.now().date() - timedelta(offset)
    second_date = first_date + timedelta(time)
    print(first_date,second_date)

    avg=[]
    con.ping()
    with con:
        cur = con.cursor()
        cur._defer_warnings = True
        cur.execute("select * from {0} where component = '{1}' AND date between '{3}' and '{4}';".format(table,component,time,first_date,second_date))
        for i in cur.fetchall():
            avg.append(i['rps_capacity'])
    try:
        avg_capacity = round(sum(avg)/len(avg))
    except ZeroDivisionError:
        avg_capacity = 0
    return avg_capacity

if __name__ == '__main__':

    sleep(10)
    healthcheck_path = '/tmp/healthy_aggregate'
    touch(healthcheck_path)

    con = connect(
        host='localhost',
        user='root',
        db='capacity',
        charset='utf8mb4',
        cursorclass=cursors.DictCursor,
        autocommit=True
    )
    create_table(con)

    cur_week = {}
    prev_week = {}
    month_ago = {}
    two_weeks_ago = {}
    utilization = {}
    table = getenv("LOAD")
    CLUSTER = getenv("CLUSTER")

    if table is None:
        table='capacity'

    components = ['rt-seller','bid','rt-buyer','mpc','bid','predict','bh','tr']
    # Get previous day average resource utilization
    avg_utilization = get_util(1)

    # Get previous day average QPS capacity
    for c in components:
        prev_day_capacity = get_capacity(con,c,table,offset=1,time=1)
        write_to_local_db(con, c, prev_day_capacity, avg_utilization)
        print(c,prev_day_capacity,avg_utilization)
