import datetime
import time
import requests

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from os import getenv

from tabulate import tabulate

def send_email(message,receiver):

    host = 'mail.pulse.prod'
    port = '25'

    sender = 'Prometheus-report@pulsepoint.com'

    msg = MIMEMultipart()
    msg['Subject'] = 'Prometheus weekly report'
    msg['From'] = sender
    msg['To'] = receiver
    msg.attach(MIMEText(message))

    server = smtplib.SMTP(host=host,port=port)
    server.sendmail(sender, receiver, msg.as_string())
    print("Successfully sent email")

def get_alerts(cluster, timeframe):

    PROMETHEUS = 'https://{}-kube-prometheus.pulsepoint.com'.format(cluster)
    alerts=set()

    response = requests.get(PROMETHEUS + '/api/v1/rules')
    results = response.json()['data']


    for group in results['groups']:
        for alert in group['rules']:
            #print(alert['name'])
            alerts.add(alert['name'])

    print('A number of active rules in {}:'.format(cluster),len(alerts))

    alerts_arr = []
    for alert in alerts:
        response = requests.get(PROMETHEUS + '/api/v1/query',
        params={'query': 'sum(changes(ALERTS_FOR_STATE{alertname="%s"}[%s]) and ignoring(alertstate) changes(ALERTS{alertstate="firing"}[%s]))' % (alert,timeframe,timeframe)})
        try:
            results = response.json()['data']['result']
        except KeyError:
            if response.json()['status'] == 'error':
                continue
            else:
                print(response.json())
        for result in results:
            alerts_arr.append([alert,int(result['value'][1])+1])
            #print(alert,result['value'][1])

    return alerts_arr


if __name__ == '__main__':

    #receiver = 'alukyanov@pulsepoint.com'
    receivers = ['anesterova@pulsepoint.com','alukyanov@pulsepoint.com', 'ExchangeTeam@pulsepoint.com', 'DataScienceEngineering@pulsepoint.com','llitvin@pulsepoint.com']
    clusters = ['sjc','lga']
    alerts_by_cluster = {}

    for cluster in clusters:
        alerts_by_cluster[cluster] = get_alerts(cluster,'1d')

    message = 'Hello,\n\nBelow you will find amount of triggered errors grouped by name.\n\n'
    for cluster in clusters:
        message=message+cluster.upper()+':\n'
        message = message+tabulate(alerts_by_cluster[cluster],headers=['Alert name','Quantity'])
        message = message+'\n\n'

    message = message+'\n\nYours faithfully,\nPrometheus'

    print(message)
    for receiver in receivers:
        send_email(message,receiver)
