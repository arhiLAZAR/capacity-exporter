from random import randint
from kubernetes import client, config
from re import sub
import json
from os import getenv, utime
from datetime import datetime
import pymysql
from requests import get
from time import sleep

def touch(path):
    with open(path, 'a'):
        utime(path, None)

def create_database(con):

    con.ping()
    with con:
        cur = con.cursor()
        cur._defer_warnings = True
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()
        print("Database version: {}".format(version['VERSION()']))
        cur.execute("CREATE TABLE IF NOT EXISTS capacity (date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,component VARCHAR(16), capacity INT, rps_capacity INT)  ENGINE=INNODB;")
        cur.execute("CREATE TABLE IF NOT EXISTS o_capacity (date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,component VARCHAR(16), capacity INT, rps_capacity INT)  ENGINE=INNODB;")
        cur.execute("CREATE TABLE IF NOT EXISTS RPS_per_CPU (date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,component VARCHAR(16), rps_per_cpu INT)  ENGINE=INNODB;")

def get_total_capacity(labels=[]):
    '''
    Return total number of CPU units and Memory(GB)
    '''
    serviceHost = getenv("KUBERNETES_SERVICE_HOST")
    if serviceHost is not None:
        config.load_incluster_config() #API Access from pod
    else:
        config.load_kube_config()

    api = client.CoreV1Api()
    ret = api.list_node(watch=False)

    nodes = {}
    for n in ret.items:
        node = {}
        c_usage = float(sub('m', '', n.status.allocatable['cpu']))/(1000)

        try:
            m_usage = int(sub('Ki', '', n.status.allocatable['memory']))/(1024*1024)
        except ValueError:
            m_usage = int(sub('Mi', '', n.status.allocatable['memory']))/(1024)

        node['cpu'] = c_usage
        node['memory'] = m_usage
        if len(labels) == 0:
            nodes[n.metadata.name] = node
        else:
            try:
                if n.metadata.labels['hw'] not in labels:
                    nodes[n.metadata.name] = node
            except KeyError:
                nodes[n.metadata.name] = node

    cpu = 0
    mem = 0
    for node in nodes:
        cpu+=float(nodes[node]['cpu']) # Units
        mem+=float(nodes[node]['memory']) # GB

    return nodes, cpu, mem

def get_used(labeled_nodes):
    '''
    Return free CPU units and memory in %
    '''
    serviceHost = getenv("KUBERNETES_SERVICE_HOST")
    if serviceHost is not None:
        config.load_incluster_config() #API Access from pod
    else:
        config.load_kube_config()
    api = client.ApiClient()

    ret = api.call_api('/apis/metrics.k8s.io/v1beta1/nodes/', 'GET', response_type='json', _preload_content=False,auth_settings=['BearerToken'])
    response = ret[0].data.decode('utf-8')
    resp = json.loads(response)

    cpu = []
    mem = []
    for n in resp['items']:

        if n['metadata']['name'] in labeled_nodes:

            try:
                cpu.append(float(sub('n', '', n['usage']['cpu']))/(1000*1000*1000))
            except ValueError:
                cpu.append(int(sub('u', '', n['usage']['cpu']))/(1000*1000))

            if 'Ki' in n['usage']['memory']:
                mem.append(int(sub('Ki', '', n['usage']['memory']))/(1024*1024))
            elif 'Mi' in n['usage']['memory']:
                mem.append(int(sub('Mi', '', n['usage']['memory']))/1024)
            elif 'Gi' in n['usage']['memory']:
                mem.append(int(sub('Gi', '', n['usage']['memory'])))

    return sum(cpu), sum(mem)

def get_requested(labeled_nodes=''):
    '''
    Get amount of requested resources even if they are not used.
    The reason for this is that k8s schedule pods according to requested resources
    but not used
    '''
    serviceHost = getenv("KUBERNETES_SERVICE_HOST")
    if serviceHost is not None:
        config.load_incluster_config() #API Access from pod
    else:
        config.load_kube_config()
    api = client.CoreV1Api()

    ret = api.list_pod_for_all_namespaces(limit=10000,watch=False)

    pods = []
    memory = []
    req = {}
    for p in ret.items:
        if p.spec.node_name in labeled_nodes:
            for c in p.spec.containers:
                try:
                    pods.append(float(c.resources.requests['cpu']))
                except ValueError:
                    pods.append(float(sub('m', '', c.resources.requests['cpu']))/(1000))
                except TypeError:
                    pods.append(0)
                except KeyError:
                    pods.append(0)
                try:
                    if 'Ki' in c.resources.requests['memory']:
                        memory.append(int(sub('Ki', '', c.resources.requests['memory']))/(1024*1024))
                    elif 'Mi' in c.resources.requests['memory']:
                        memory.append(int(sub('Mi', '', c.resources.requests['memory']))/1024)
                    elif 'Gi' in c.resources.requests['memory']:
                        memory.append(int(sub('Gi', '', c.resources.requests['memory'])))
                except TypeError:
                    memory.append(0)
                except KeyError:
                    memory.append(0)
    req['cpu'] = sum(pods)
    req['memory'] = sum(memory)
    return req

def get_pods(namespace):
    '''
    Get pods for specified namespace
    '''
    serviceHost = getenv("KUBERNETES_SERVICE_HOST")
    if serviceHost is not None:
        config.load_incluster_config() #API Access from pod
    else:
        config.load_kube_config()
    api = client.CoreV1Api()

    ret = api.list_namespaced_pod(namespace,watch=False)
    return ret.items

def get_usage(namespace):
    '''
    Get resource usage for all pods in specified namespace
    '''
    serviceHost = getenv("KUBERNETES_SERVICE_HOST")
    if serviceHost is not None:
        config.load_incluster_config() #API Access from pod
    else:
        config.load_kube_config()
    api = client.ApiClient()

    ret = api.call_api('/apis/metrics.k8s.io/v1beta1/namespaces/' + namespace + '/pods', 'GET', response_type='json', _preload_content=False,auth_settings=['BearerToken'])
    response = ret[0].data.decode('utf-8')
    resp = json.loads(response)

    cpu_usage = []
    memory_usage = []
    usage = {}
    for p in resp['items']:
        for c in p['containers']:
            #print(p['metadata']['name'],c['usage']['cpu'])
            try:
                if 'n' in c['usage']['cpu']:
                    cpu_usage.append(float(sub('n', '', c['usage']['cpu']))/(1000*1000*1000))
                elif 'u' in c['usage']['cpu']:
                    cpu_usage.append(float(sub('u', '', c['usage']['cpu']))/(1000*1000))
                elif 'm' in c['usage']['cpu']:
                    cpu_usage.append(float(sub('m', '', c['usage']['cpu']))/1000)
            except TypeError:
                cpu_usage.append(0)
            except KeyError:
                cpu_usage.append(0)

            try:
                if 'Ki' in c['usage']['memory']:
                    memory_usage.append(int(sub('Ki', '', c['usage']['memory']))/(1024*1024))
                elif 'Mi' in c['usage']['memory']:
                    memory_usage.append(int(sub('Mi', '', c['usage']['memory']))/1024)
                elif 'Gi' in c['usage']['memory']:
                    memory_usage.append(int(sub('Gi', '', c['usage']['memory'])))
            except TypeError:
                memory_usage.append(0)
            except KeyError:
                memory_usage.append(0)
    usage['cpu'] = sum(cpu_usage)
    usage['memory'] = sum(memory_usage)
    return usage

def get_limit(namespace):
    '''
    Get resoruce limits for deployments in specified namespace
    '''
    serviceHost = getenv("KUBERNETES_SERVICE_HOST")
    if serviceHost is not None:
        config.load_incluster_config() #API Access from pod
    else:
        config.load_kube_config()
    api = client.AppsV1Api()

    cpu=[]
    memory=[]
    req = {}

    ret = api.list_namespaced_deployment(namespace,watch=False)
    for deployment in ret.items:
        if deployment.metadata.name == namespace+'-deployment':
            for c in deployment.spec.template.spec.containers:
                try:
                    cpu.append(float(c.resources.requests['cpu']))
                except ValueError:
                    cpu.append(float(sub('m', '', c.resources.requests['cpu']))/(1000))
                except TypeError:
                    cpu.append(0)
                except KeyError:
                    cpu.append(0)

                try:
                    if 'Ki' in c.resources.requests['memory']:
                        memory.append(int(sub('Ki', '', c.resources.requests['memory']))/(1024*1024))
                    elif 'Mi' in c.resources.requests['memory']:
                        memory.append(int(sub('Mi', '', c.resources.requests['memory']))/1024)
                    elif 'Gi' in c.resources.requests['memory']:
                        memory.append(int(sub('Gi', '', c.resources.requests['memory'])))
                except TypeError:
                    memory.append(0)
                except KeyError:
                    memory.append(0)

    req['cpu'] = sum(cpu)
    req['memory'] = sum(memory)

    return req

def get_anti_affinity(namespace):
    '''
    Get Pod's preferences for scheduling
    '''
    serviceHost = getenv("KUBERNETES_SERVICE_HOST")
    if serviceHost is not None:
        config.load_incluster_config() #API Access from pod
    else:
        config.load_kube_config()
    api = client.AppsV1Api()

    labels = []
    ret = api.list_namespaced_deployment(namespace,watch=False)
    for deployment in ret.items:
        if deployment.metadata.name == namespace+'-deployment':
            try:
                # for affinity in deployment.spec.template.spec.affinity.node_affinity.preferred_during_scheduling_ignored_during_execution:
                for affinity in deployment.spec.template.spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution:
                    for weight in affinity.preference.match_expressions:
                        if weight.key == 'hw':
                            if weight.operator == 'NotIn':
                                for node in weight.values:
                                    labels.append(node)
            except TypeError:
                continue

    return labels

def get_ingress(component,usage):
    '''
    Get component's impact on ingress
    '''
    serviceHost = getenv("KUBERNETES_SERVICE_HOST")
    if serviceHost is not None:
        config.load_incluster_config() #API Access from pod
    else:
        config.load_kube_config()
    api = client.ApiClient()

    ret = api.call_api('/apis/metrics.k8s.io/v1beta1/namespaces/ingress/pods', 'GET', response_type='json', _preload_content=False,auth_settings=['BearerToken'])
    response = ret[0].data.decode('utf-8')
    resp = json.loads(response)

    cpu_usage = []
    memory_usage = []
    impact = {}

    if component == 'rt-seller':
        comp = 'rts'
    else:
        comp = component

    for p in resp['items']:
        if comp in p['metadata']['name']:
            for c in p['containers']:
                #print(p['metadata']['name'],c['usage']['cpu'])
                try:
                    cpu_usage.append(float(sub('n', '', c['usage']['cpu']))/(1000*1000*1000))
                except ValueError:
                    cpu_usage.append(float(sub('u', '', c['usage']['cpu']))/(1000*1000))
                try:
                    if 'Ki' in c['usage']['memory']:
                        memory_usage.append(int(sub('Ki', '', c['usage']['memory']))/(1024*1024))
                    elif 'Mi' in c['usage']['memory']:
                        memory_usage.append(int(sub('Mi', '', c['usage']['memory']))/1024)
                    elif 'Gi' in c['usage']['memory']:
                        memory_usage.append(int(sub('Gi', '', c['usage']['memory'])))
                except TypeError:
                    memory_usage.append(0)
                except KeyError:
                    memory_usage.append(0)

    impact['cpu'] = sum(cpu_usage)/usage[component]['cpu']
    impact['memory'] = sum(memory_usage)/usage[component]['cpu']
    return impact

def get_rps(component):

    global CLUSTER

    query = {}
    query['bid'] = 'sum by (cluster) (bid_requests{cluster=~"%s",timeinterval="recent-avg"})' % CLUSTER
    query['rt-seller'] = 'sum by (cluster) (rts_requests{cluster=~"%s",timeinterval="recent-avg"})' % CLUSTER
    query['predict'] = 'sum by (cluster) (predict_qps{cluster=~"%s",timeinterval="recent-avg"})' % CLUSTER
    query['rt-buyer'] = 'sum by (cluster) (rtb_general_stats_request_count{cluster=~"%s",timeinterval="recent-avg"})' % CLUSTER
    query['bh'] = 'sum by (cluster) (bh_total_requests{cluster=~"%s",timeinterval="recent-avg"})' % CLUSTER
    query['tr'] = 'sum by (cluster) (tr_total_requests{cluster=~"%s",timeinterval="recent-avg"})' % CLUSTER
    query['mpc'] = 'sum by (cluster) (mpc_overall_request_count{cluster=~"%s",timeinterval="recent-avg"})' % CLUSTER


    prom_url = 'http://{}-kube-prometheus.pulsepoint.com/api/v1/query?'.format(CLUSTER)
    response = get(prom_url, params={'query': query[component]})
    if not response.json()['data']['result']:
        return 0
    else:
        print("RPS for ",component," : ",int(response.json()['data']['result'][0]['value'][1]))
        return int(response.json()['data']['result'][0]['value'][1])

def percentage(part, whole):
    try:
        return float(part)/float(whole)
    except ZeroDivisionError:
        return 1

def adjust_cluster_usage(deps,cluster,pods,usage,requested,RPS):

    ingress = ['rt-seller','bid']
    incoming_rps = 0
    percents = {}
    multiplier = {}

    for i in ingress:
        incoming_rps+=RPS[i]

    for i in deps:
        if i in ingress:
            percents[i] = percentage(RPS[i],incoming_rps)
            if usage[i]['cpu'] == 0:
                usage[i]['cpu'] = 1
            multiplier[i] = requested[i]['cpu']*pods[i]/usage[i]['cpu']

        else:
            percents[i] = 1

    max_usage = {}
    for i in ingress:
        i_max_usage = {}
        for d in deps[i]:
            i_max_usage[d] = float(usage[d]['cpu']*percents[i]*multiplier[i])
        max_usage[i] = i_max_usage

    sum_of_usage = {}
    for i in max_usage:
        for e in max_usage[i]:
            sum_of_usage.setdefault(e, []).append(max_usage[i][e])

    for i in sum_of_usage:
        if sum(sum_of_usage[i]) > (requested[i]['cpu']*pods[i]):
            cluster['cpu'] = cluster['cpu'] - requested[i]['cpu']*pods[i] + sum(sum_of_usage[i])

    return cluster, percents

def get_impact(percents,deps,pods,usage):

    ingress = ['rt-seller','bid']
    imp = {}
    ingress_imp = {}
    for i in deps:
        if i in ingress:
            ingress_imp[i] = get_ingress(i,usage)

            impact_per_cpu = 0
            for d in deps[i]:
                impact_per_cpu+=float((usage[d]['cpu']*percents[i])/usage[i]['cpu'])
            imp[i] = impact_per_cpu+ingress_imp[i]['cpu']
        else:
            imp[i] = 1
    return imp

def get_query_cost(con,component,deps,usage,percents,RPS):

    cpu_usage = 0
    for i in deps[component]:
        cpu_usage+=usage[i]['cpu']
    cpu_usage=cpu_usage*percents[component]+usage[component]['cpu']
    cost = RPS[component]/cpu_usage
    print(component,'queries per CPU:',cost)
    con.ping()
    with con:
        cur = con.cursor()
        cur.execute("INSERT INTO RPS_per_CPU(component,rps_per_cpu) VALUES('{}','{}');".format(component, cost))

    return cost

def get_clusters_usage():

    global cluster

    overal_usage = 0
    clusters = {
        'lga': 'http://lga-kube-prometheus.pulsepoint.com/',
        'sjc': 'http://sjc-kube-prometheus.pulsepoint.com/'
    }
    del clusters[CLUSTER]

    for c in clusters:

        response = get(clusters[c] + '/api/v1/query',
        params={'query': "sum(cluster_node_mode:node_cpu_seconds:rate1m{mode!='idle'})"})
        results = response.json()['data']['result']
        for result in results:
            usage = round(float(result['value'][1]))
            #print(c,usage)
            overal_usage+=usage

    return overal_usage

def calc(con,component,deps):

    # Get overall amount of CPU/Memory and amount of currently used CPU/Memory
    labels = get_anti_affinity(component)
    nodes, cores, memory = get_total_capacity(labels=labels)

    pods = {}
    requested = {}
    cluster_req = {}
    adjusted_cluster_req = {}
    usage = {}
    revers_deps = {}
    impact = {}
    impact = {}
    RPS = {}

    # get a list of reversal dependencies (WHO generates load to THIS component)
    for comp in deps:
        for dep in deps[comp]:
            if dep in deps[component]:
                revers_deps.setdefault(dep, []).append(comp)

    cluster_req = get_requested(nodes)
    for c in deps:
        pods[c] = len(get_pods(c))
        requested[c] = get_limit(c)
        usage[c] = get_usage(c)
        RPS[c] = get_rps(c)

    cluster_req, percents = adjust_cluster_usage(deps,cluster_req,pods,usage,requested,RPS)

    impact = get_impact(percents,deps,pods,usage)

    cost = get_query_cost(con,component,deps,usage,percents,RPS)


    # FInal calculations

    rps_capacity = round((cores-cluster_req['cpu'])*cost)

    capacity = {
        'rps_capacity_percents': (rps_capacity/(RPS[component]+1))*100,
        'cpu_capacity': round((cores-cluster_req['cpu'])/(requested[component]['cpu']+(impact[component]*requested[component]['cpu']))),
        'memory_capacity': round((memory-cluster_req['memory'])/(requested[component]['cpu']+(impact[component]*requested[component]['memory'])))
    }

    # get usage of other clusters to figure out whether THIS cluster can take load from other clusters at the momnent
    others_cluster_usage = get_clusters_usage()
    cluster_req['cpu'] = cluster_req['cpu'] + others_cluster_usage
    rps_capacity = round((cores-cluster_req['cpu'])*cost)

    o_capacity = {
        'rps_capacity_percents': (rps_capacity/(RPS[component]+1))*100,
        'cpu_capacity': round((cores-cluster_req['cpu'])/(requested[component]['cpu']+(impact[component]*requested[component]['cpu']))),
        'memory_capacity': round((memory-cluster_req['memory'])/(requested[component]['cpu']+(impact[component]*requested[component]['memory'])))
    }

    print('-----')
    print(capacity)
    print(o_capacity)
    print('-----')
    # print('cluster:',cluster_req)
    # print('pods:',pods)
    # print('comp_requested:',requested)
    # print('usage:',usage)
    # print('RPS:',RPS)
    # print('impact:',impact)
    return capacity,o_capacity


if __name__ == '__main__':

    sleep(10)
    healthcheck_path = '/tmp/healthy_capacity'
    touch(healthcheck_path)

    con = pymysql.connect(
        host='localhost',
        user='root',
        db='capacity',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )
    create_database(con)

    CLUSTER = getenv("CLUSTER")

    deps = {}
    capacity = {}
    deps['rt-seller'] = ['predict','rt-buyer','mpc']
    deps['bid'] = ['predict','rt-buyer','mpc']
    deps['bh'] = []
    deps['tr'] = []

    deps['predict'] = []
    deps['rt-buyer'] = ['bh','tr']
    deps['mpc'] = ['bh','tr']
    for component in deps:

        comp_capacity = {}
        capacity, o_capacity = calc(con,component,deps)

        if capacity['cpu_capacity'] < capacity['memory_capacity']:
            max_capacity = capacity['cpu_capacity']
            o_max_capacity = o_capacity['cpu_capacity']
        else:
            max_capacity = capacity['memory_capacity']
            o_max_capacity = o_capacity['memory_capacity']

        con.ping()
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO capacity(component,capacity,rps_capacity) VALUES('{}','{}','{}');".format(component, max_capacity,capacity['rps_capacity_percents']))
            cur.execute("INSERT INTO o_capacity(component,capacity,rps_capacity) VALUES('{}','{}','{}');".format(component, o_max_capacity,o_capacity['rps_capacity_percents']))

        # print('CAPACITY:',component, max_capacity, capacity['rps_capacity_percents'])
        # print('O_CAPACITY:',component, o_max_capacity, o_capacity['rps_capacity_percents'])
