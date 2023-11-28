import json
import os
import time
import urllib.request
from urllib import request, error


def clear(port):
    url = 'http://localhost:' + port + '/v2/queue/'

    delayed = list()
    with request.urlopen(url) as response:
        your_json = response.read()
        parsed = json.loads(your_json)
        for entry in parsed['queue']:
            app = entry['app']
            if "/driver" in app["id"] and entry['delay']['timeLeftSeconds'] > 0:
                delayed.append(app["id"])

    for app in delayed:
        group = app.split("/")[1]
        req = urllib.request.Request(url='http://localhost:' + port + '/v2/groups/' + group, method='DELETE')
        with request.urlopen(req) as response:
            pass


def clearAll(port):
    url_group = 'http://localhost:' + port + '/v2/groups/'
    url_deploy = 'http://localhost:' + port + '/v2/deployments/'
    while True:
        try:
            # Delete all deployments
            with request.urlopen(url_deploy) as response:
                your_json = response.read()
                parsed = json.loads(your_json)
                for dep in parsed:
                    req = urllib.request.Request(url=url_deploy + dep['id'], method='DELETE')
                    with request.urlopen(req) as response2:
                        pass
            # Delete all groups
            req = urllib.request.Request(url=url_group, method='DELETE')
            with request.urlopen(req) as response:
                pass
            time.sleep(10)
            with request.urlopen(url_group) as response:
                your_json = response.read()
                parsed = json.loads(your_json)
                if len(parsed['groups']) == 0:
                    break
        except error.URLError as ex:
            time.sleep(10)


service = os.environ['PORT_SERVICE']
if "MARATHON_CLEAR" in os.environ:
    clearAll(service)

while True:
    try:
        clear(service)
    except:
        pass
    time.sleep(120)
