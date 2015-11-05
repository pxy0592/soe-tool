#Author: Xin Yu Pan (panxiny@cn.ibm.com)
#Brief: measure spark service scalability in Bluemix environment
#

import sys
import requests
import argparse
import json
import time
import random
import socket
import threading
import websocket
from websocket import _abnf
import uuid
import logging

def _verify_response(status_code, content):
        if str(status_code)[0] in ['4', '5']:
            #print("Unexpected http status code: {0} and response mesage: {1}".format(status_code, content))
            logger.info("Unexpected http status code: {0} and response mesage: {1}".format(status_code, content))
            exit(1)

class Client(object):
    def __init__(self, app_url, app_port, tenant_id, logger=None):
        self.app_url = app_url
        self.app_port = app_port
        self.url = self.app_url + ":" + self.app_port
        self.s = requests.session()
        self.ipython_url = "http://{0}/ipython".format(self.url)
        #print "ipython url %s" % self.ipython_url
        self.logger = logger
        self.logger.info("6.1) ipython url %s" % self.ipython_url)
        self.tenant_id = tenant_id
 
    def create_nb(self, tenant_id):
        nb_url = self.ipython_url + '/api/contents/'
        data = {
            'type': 'notebook',
        }
 
        r = self.s.post(nb_url, json.dumps(data))

        _verify_response(r.status_code, r.content)

        self.nb_name = json.loads(r.content)['name']
        #print("6.2) Create new notebook: ".format(self.nb_name))
        self.logger.info("6.2) Create new notebook: {0}".format(self.nb_name))
        return self.nb_name

    def delete_nb(self, nb_name):
        nb_url = self.ipython_url + '/api/contents/' + nb_name
        r = self.s.delete(nb_url)
        
        _verify_response(r.status_code, r.content)
        #print("6.10) Delete the notebook: ".format(self.nb_name))
        self.logger.info("6.10) Delete the notebook: {0}".format(self.nb_name))

    def get_kernel_specs(self):
        kernel_url = self.ipython_url + '/api/kernelspecs'
        r = self.s.get(kernel_url)

        _verify_response(r.status_code, r.content)

        self.kernel_name = json.loads(r.text)['default']
        #print("6.3) Kernel {0} is used".format(self.kernel_name))
        self.logger.info("6.3) Kernel {0} is used".format(self.kernel_name))

    def open_kernel_session(self):
        session_url = self.ipython_url + '/api/sessions'
        data = {
            'notebook': {
                'path': self.nb_name
            },
            'kernel': {
                'id': 'null',
                'name': self.kernel_name
            }
        }

        r = self.s.post(session_url, json.dumps(data))

        _verify_response(r.status_code, r.content)

        self.kernel_id = json.loads(r.text)['kernel']['id']

        #print("6.4) Kernel id {0} is created".format(self.kernel_id))
        self.logger.info("6.4) Kernel id {0} is created".format(self.kernel_id))
        return self.kernel_id

    def shutdown_kernel(self, kernel_id):
        kernel_url = self.ipython_url + '/api/kernels/' + kernel_id
        r = self.s.delete(kernel_url)
         
        _verify_response(r.status_code, r.content)
         
        #print("6.9) Kernel {0} is shutdown".format(kernel_id))
        self.logger.info("6.9) Kernel {0} is shutdown".format(kernel_id))
     
    def create_ws_spark_twitter_analyzer(self,fn, gui_url):
        """
        Construct the webstocket and get the response
        :return:
        """
        # websocket.enableTrace(True)
        ws = websocket.WebSocket()
        session_id = uuid.uuid4().get_hex().upper()

        wss_url = "ws://{0}/ipython/api/kernels/{1}/channels?session_id={2}".format(
            self.url, self.kernel_id, session_id
        )
        
        #print("wss_url=" + wss_url)
        self.logger.info("wss_url=" + wss_url)
        ws.connect(wss_url)
         
        frame_json = {
            "header": {
                "msg_id": "A074F6E4F378499C97A5E97ACD59F433",  # could be any string?
                "username": "username",
                "session": session_id,
                "msg_type": "execute_request",
                "version": "5.0"
            },
            "metadata": {},
            "content": {
              "code": "sc",
              "code": "from pyspark.sql import *\nimport json\ntweets = sc.textFile(\"hdfs://{0}:7020/data/".format(gui_url)+fn+"\").filter(lambda line : line !=\"\")\nsqlContext = SQLContext(sc)\ntweetTable = sqlContext.jsonRDD(tweets)\ntweetTable.registerTempTable(\"tweetTable\")\ntop20langs = sqlContext.sql(\"SELECT actor.languages, COUNT(*) as cnt FROM tweetTable GROUP BY actor.languages ORDER BY cnt DESC LIMIT 20\")\ntop20langs.collect()",
              "silent": False,
              "store_history": True,
              "user_expressions": {},
              "allow_stdin": True,
              "stop_on_error": True
            },
            "buffers": [],
            "parent_header": {},
            "channel": "shell"
        }

        frame = _abnf.ABNF.create_frame(json.dumps(frame_json), _abnf.ABNF.OPCODE_TEXT)

        #print("6.8) Creating websocket:")
        self.logger.info("6.8) Creating websocket:")
        #print("Issuing <Spark Twitter Analzyer - top 20 languages used in this set > command [expected en ja null in returning result]")
        self.logger.info("Issuing <Spark Twitter Analzyer - top 20 languages used in this set > command [expected en ja null in returning result]")

        ws.send_frame(frame)

        result = ws.recv_data()
        #print("Xinyu => Returning: ", result)
        #self.logger.info("Xinyu => Returning: ", result)

def run_job(gui_url, app_url, app_port, tenant_id, logger):
    #print "app_url=" + app_url + " app_port="  + app_port + "  app_hostname: " + socket.gethostbyaddr(app_url)[0]
    logger.info("tenant_id=%s app_url=%s app_port=%s app_hostname=%s", tenant_id, app_url, app_port, socket.gethostbyaddr(app_url)[0])
    time.sleep(random.randint(1,5))
    client  = Client(app_url, app_port, tenant_id, logger)

    nb_name = client.create_nb(tenant_id)
    client.get_kernel_specs()
    kernel_id = client.open_kernel_session()
    
    #flist = ['tweets_10_37kb.dat', 'tweets_10k_20mb.dat', 'tweets_30k_60mb.dat', 'tweets_45k_100mb.dat', 'tweets_100_200mb.dat']
    flist = ['tweets_10k_20mb.dat', 'tweets_30k_60mb.dat', 'tweets_45k_100mb.dat']
    #flist = ['tweets_30k_60mb.dat']

    for fn in flist:
        st=time.time()
        client.create_ws_spark_twitter_analyzer(fn, gui_url)
        #print("Spark SQL query 1 on "+fn+" took {0} ms".format((time.time()-st)*1000.0))
        logger.info("Spark SQL query 1 on " + fn + " took {0} ms".format((time.time()-st)*1000.0))
        
    #client.shutdown_kernel(kernel_id)
    #client.delete_nb(nb_name)

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='''
        TODO: add program description
    ''')
    argparser.add_argument('-f', '--file', help='input file', default='1000_url_list.txt')
    argparser.add_argument('-a', '--guiurl', help='gui url', default='yp-spark-dal09-env5-0051')
    argparser.add_argument('-o', '--logfile', help='logfile', default='1000tenants.log')
    args = argparser.parse_args()

    file = args.file
    gui_url = args.guiurl
    logfile = args.logfile
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.FileHandler(logfile)
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter("%(asctime)s %(name)s %(process)d %(threadName)s %(thread)d %(levelname)s %(message)s")
    handler.setFormatter(formatter)

    # add handler to the logger
    logger.addHandler(handler)
    logger.info("this is main.")

    thread_list = []
    fp = open(file, 'r')

    for line in fp:
        msg = line.split(':')
        t = threading.Thread(name=msg[0], target=run_job, args=(gui_url, msg[1], msg[2], msg[0], logger))
        thread_list.append(t)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

