from pymysql import connect, cursors
from requests import get
import json
from os import getenv
from datetime import datetime,timedelta


def week_capacity(con,component,offset=8,time=7):
    '''
    Calculate capacity for specified week
    offset=-1 is till today.
    '''
    first_date = datetime.now().date() - timedelta(offset)
    second_date = first_date + timedelta(time)

    avg=[]
    with con:
        cur = con.cursor()
        cur._defer_warnings = True
        cur.execute("select * from capacity_avg where component = '{0}' AND date between '{1}' and '{2}';".format(component,first_date,second_date))
        for i in cur.fetchall():
            avg.append(int(i['capacity']))
    try:
        avg_capacity = round(sum(avg)/len(avg))
    except ZeroDivisionError:
        avg_capacity = 0

    return avg_capacity

def get_weekly_util(con,offset=8,time=7):
    '''
    Get CPU utilization for the specified cluster for a week
    offset=-1 is till today.
    '''
    avg_cpu=[]
    avg_mem=[]
    avg_utilization = {}

    first_date = datetime.now().date() - timedelta(offset)
    second_date = first_date + timedelta(time)

    with con:
        cur = con.cursor()
        cur._defer_warnings = True
        cur.execute("select * from capacity_avg where date between '{0}' and '{1}';".format(first_date,second_date))
        for i in cur.fetchall():
            avg_cpu.append(float(i['utilization_cpu']))
            avg_mem.append(float(i['utilization_memory']))
    try:
        avg_utilization['cpu'] = round(sum(avg_cpu)/len(avg_cpu),2)
        avg_utilization['mem'] = round(sum(avg_mem)/len(avg_mem),2)
    except ZeroDivisionError:
        avg_utilization['cpu'] = 0
        avg_utilization['mem'] = 0

    return avg_utilization


def k8s_usage(clusters,components):


    cluster_utilization = {}
    cluster_capacity = {}

    for cluster in clusters:

        components_capacity = {}
        con = connect(
            host='{}-kubnode16'.format(cluster),
            port=30306,
            user='root',
            db='capacity',
            charset='utf8mb4',
            cursorclass=cursors.DictCursor,
            autocommit=True
        )
        cluster_utilization[cluster] = get_weekly_util(con)


        for component in components:

            components_capacity[component] = week_capacity(con,component)

        cluster_capacity[cluster] = components_capacity

    return cluster_utilization, cluster_capacity


if __name__ == '__main__':

    clusters = ['lga','sjc']
    util,capacity = k8s_usage(clusters)

    print(util)
    print(capacity)
