import requests

import pyVmomi
import atexit
import itertools
from pyVmomi import vim, vmodl
from pyVim.connect import SmartConnect, Disconnect, SmartConnectNoSSL
import humanize

def get_vmware_hosts(server,username,password):

    si = SmartConnectNoSSL(host=server, user=username, pwd=password)
    atexit.register(Disconnect, si)
    content = si.RetrieveContent()

    hosts_per_dc = {}
    for datacenter in content.rootFolder.childEntity:


        clusters = datacenter.hostFolder.childEntity
        for cluster in clusters:
            hosts = []
            for host in cluster.host:
                hostname = host.summary.config.name
                hosts.append(hostname)

            hosts_per_dc[cluster.name] = hosts

    return hosts_per_dc

def get_utilization(hosts,param):

    graphite_url = 'http://lga-graphite02/render?target='
    utilization = []

    for host in hosts:

        used=[]
        host = host.replace('.','_')
        query = {
            'cpu': 'icinga2.{}.services.esx-cpu-check-p2.check-esx-host.perfdata.cpu_usage.value&format=json&from=-168hours'.format(host),
            'mem': 'icinga2.{}.services.esx-memory-check-p2.check-esx-host.perfdata.mem_usage.value&format=json&from=-168hours'.format(host)
        }
        result = requests.get(graphite_url+query[param])
        try:
            for i in result.json()[0]['datapoints']:
                if i[0] is not None:
                    used.append(i[0])
            utilization.append(sum(used)/len(used))
        except ZeroDivisionError:
            continue
        except IndexError:
            continue
    try:
        utilization = round(sum(utilization)/len(utilization))
    except:
        utilization = 'N/A'
    return utilization

def vmware_usage():

    server='lga-vcenter02.pulse.prod'
    with open('/icinga_passwd/username','r') as password_file:
        username = password_file.readlines()[0]
    with open('/icinga_passwd/password','r') as password_file:
        password = password_file.readlines()[0]

    utilization_per_dc = {}
    hosts = get_vmware_hosts(server,username,password)
    for dc in hosts:
        if dc in ['SJC-Production','LGA-Production']:
            print(dc)
            utilization = {}
            utilization['cpu'] = get_utilization(hosts[dc],'cpu')
            utilization['mem'] = get_utilization(hosts[dc],'mem')
            utilization_per_dc[dc] = utilization

    return utilization_per_dc

if __name__ == '__main__':

    utilization = vmware_usage()
    print(utilization)
