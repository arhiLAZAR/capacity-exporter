import requests

def get_usage_from_graphite(slaves,url,cluster,param='mem'):

    graphite_url = 'http://lga-graphite02/render?target='
    util=[]

    for slave in slaves:

        total_query = 'monitoring.Mesos.{2}.Production.{0}.slave.{1}_total&format=json'.format(slave,param,cluster.upper())
        used_query = 'monitoring.Mesos.{2}.Production.{0}.slave.{1}_used&format=json&from=-1hours'.format(slave,param,cluster.upper())
        used=[]

        result = requests.get(graphite_url+total_query)
        for i in result.json()[0]['datapoints']:
            if i[0] is not None:
                total=i[0]
                break

        result = requests.get(graphite_url+used_query)
        for i in result.json()[0]['datapoints']:
            if i[0] is not None:
                used.append(i[0])
        try:
            average=sum(used)/len(used)
            utilization = (average * 100)/total
            util.append(utilization)
        except ZeroDivisionError:
            continue

    return sum(util)/len(util)


def mesos_usage(clusters):

    mesos_utilization = {}
    for cluster in clusters:

        url = "http://{0}-mms01.pulse.prod:5050/".format(cluster)
        state_query='master/state'
        slave_query='master/slaves'

        slaves=[]
        response = requests.get(url+slave_query)
        for i in response.json()['slaves']:
            slaves.append(i['hostname'].replace('.pulse.prod',''))

        cluster_utilization = {}
        cluster_utilization['mem'] = get_usage_from_graphite(slaves,url,cluster,'mem')
        cluster_utilization['cpu'] = get_usage_from_graphite(slaves,url,cluster,'cpus')

        mesos_utilization[cluster] = cluster_utilization

    return mesos_utilization

if __name__ == '__main__':

    clusters = ['lga','sjc']
    usage = mesos_usage(clusters)
    print(usage)
