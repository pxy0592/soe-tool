#Author: Xin Yu Pan (panxiny@cn.ibm.com)
#Brief: measure spark service scalability in Bluemix environment
#

import requests
import argparse
import json
import threading

lock = threading.Lock()

def get_url(gui_url, gui_port, tenant_id):
    url = "http://{0}:{1}/platform/rest/ego/services/{2}/instances/1".format(gui_url, gui_port, tenant_id)
    r = requests.get(url, auth=('Admin', 'Admin'))
    if r.status_code != 200:
       print r.text
       exit(-1)

    url_info = json.loads(r.text)['userinfo'].split('/')[2]

    lock.acquire()
    f.write(tenant_id + ":" + url_info + ':\n')
    lock.release()

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='''
        TODO: add program description
    ''')
    argparser.add_argument('-a', '--guinode', help='sym gui url', default='yp-spark-dal09-env5-0051')
    argparser.add_argument('-p', '--guiport', help='sym gui  port', default='9595')
    argparser.add_argument('-t', '--tenantid', help='spark tenant id', default='tenant')
    argparser.add_argument('-s', '--startnum', help='job start num', default='1')
    argparser.add_argument('-n', '--loopnum', help='job loop number', default='1')
    args = argparser.parse_args()

    gui_url = args.guinode
    gui_port = args.guiport
    tenant_id = args.tenantid
    loop_num = args.loopnum
    start_num = args.startnum
    
    filename = 'url-list/' + loop_num + "_url_list.txt" 
    f = open(filename, 'w')
    thread_list = []
    for i in range(int(start_num), int(loop_num)):
        tname = tenant_id + str(i)
        t = threading.Thread(name=tname, target=get_url, args=(gui_url, gui_port, tname))
        thread_list.append(t)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    while threading.activeCount() > 1:
        pass
    else:
        f.close()

