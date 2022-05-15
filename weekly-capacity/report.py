import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from os import getenv

from tabulate import tabulate

import kubernetes
import vmware
import mesos

def send_email(message,receivers):

    host = 'mail.pulse.prod'
    port = '25'

    sender = 'capacity-report@pulsepoint.com'

    for receiver in receivers:
        msg = MIMEMultipart()
        msg['Subject'] = 'Scaling Summary'
        msg['From'] = sender
        msg['To'] = receiver
        msg.attach(MIMEText(message))

        server = smtplib.SMTP(host=host,port=port)
        server.sendmail(sender, receiver, msg.as_string())
        print("Successfully sent email to {}".format(receiver))

if __name__ == '__main__':

    receiver = ['alukyanov@pulsepoint.com','anesterova@pulsepoint.com','llitvin@pulsepoint.com']

    clusters = ['lga','sjc']
    k8s_components = ['rt-seller','rt-buyer','mpc','predict','bid','bh','tr']

    k8s_util,k8s_capacity = kubernetes.k8s_usage(clusters,k8s_components)
    mesos_util = mesos.mesos_usage(clusters)
    vmware_util = vmware.vmware_usage()

    k8s_usage = []
    for cluster in k8s_util:
        utilization = []
        utilization.append(cluster.upper())
        utilization.append(str(round(k8s_util[cluster]['cpu']))+'%')
        utilization.append(str(round(k8s_util[cluster]['mem']))+'%')
        k8s_usage.append(utilization)

    mesos_usage = []
    for cluster in mesos_util:
        utilization = []
        utilization.append(cluster.upper())
        utilization.append(str(round(mesos_util[cluster]['cpu']))+'%')
        utilization.append(str(round(mesos_util[cluster]['mem']))+'%')
        mesos_usage.append(utilization)

    vmware_usage = []
    for cluster in vmware_util:
        utilization = []
        utilization.append(cluster.upper())
        utilization.append(str(round(vmware_util[cluster]['cpu']))+'%')
        utilization.append(str(round(vmware_util[cluster]['mem']))+'%')
        vmware_usage.append(utilization)



    capacity = []
    for cluster in k8s_capacity:
        cluster_capacity=[]

        cluster_capacity.append(cluster.upper())
        for component in k8s_components:
            cluster_capacity.append(str(k8s_capacity[cluster][component])+'%')

        capacity.append(cluster_capacity)

    message='Hello,\n\nPlease enjoy this summary report!\n\n'
    message=message+'Tables below represent average numbers for the last week\n\n'
    message=message+'K8s utilization:\n'
    message=message+tabulate(k8s_usage,headers=['сluster','CPU','Memory'])
    message=message+'\n\n\n'
    message=message+'Mesos utilization:\n'
    message=message+tabulate(mesos_usage,headers=['сluster','CPU','Memory'])
    message=message+'\n\n\n'
    message=message+'VMware utilization:\n'
    message=message+tabulate(vmware_usage,headers=['сluster','CPU','Memory'])
    message=message+'\n\n\n'
    message=message+'Available QPS capacity:\n'
    message=message+tabulate(capacity,headers=k8s_components)
    message=message+'\n\n'
    message=message+'Yours faithfully,\nCapacity Monitoring'

    print(message)
    send_email(message,receiver)
