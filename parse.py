#Author: Xin Yu Pan (panxiny@cn.ibm.com)
#Brief: analyze the tenant application runtime
#

import sys
import os
import argparse
import time
import threading
import socket
import subprocess
import logging

lock = threading.Lock()

def summary(rfp, logger, message):
    logger.info("Starting parse result to get max runtime, min runtime and average runtim for {0}.".format(message))
    tenant_dict = {}
    for line in rfp:
       substr = line.strip('\n').split(':')
       tenant_dict[substr[0]] = substr[1]

    sum=0.00; max=0.00; min=1000.000; max_tenant=min_tenant=''
    for (k, v) in tenant_dict.items():
       #print k,v
       sum = sum + float(v)
       if float(v) > max:
         max = float(v)
         max_tenant = k
       if float(v) < min:
         min = float(v)
         min_tenant = k

    avg_tenant = sum/len(tenant_dict)

    logger.info("max tenant name:value {0}:{1}, min tenant name:value {2}:{3}, average tenant runtime {4} and total {5} tenants for {6}.".format(max_tenant, max, min_tenant, min, avg_tenant, len(tenant_dict), message))
    rfp.write("max tenant name:value {0}:{1}, min tenant name:value {2}:{3}, average tenant runtime {4} and total {5} tenants for {6}.\n".format(max_tenant, max, min_tenant, min, avg_tenant, len(tenant_dict), message))

def get_app_runtime(tenant_id, driver_log, logger):
    logger.info("Starting analyze {0} driver log to get application runtime".format(tenant_id))
    f1 = open(driver_log, 'r')

    base = 0
    if version == "1.4.1":
      end = base + 3
    elif version == "1.3.1":
      end = base + 4
    sum = 0

    cnt = 0
    f2 = wfp_list[cnt]
    #print " f2 {0}, lenght is {1}".format(wfp_list, len(wfp_list))

    for line in f1:
      if base == end:
        logger.info("the tenant tooks {0} s for dataset.".format(sum))
        #print "base={0}, end={1}, tenant id={2}, sum={3}".format(base, end, tenant_id, sum)
        lock.acquire()
        f2.write("{0}:{1}\n".format(tenant_id, sum))
        lock.release()

        sum = 0
        base = end
        if version == '1.4.1':
          end = base + 3
        elif version == '1.3.1':
          end = base + 4

        cnt += 1
        if cnt <= len(wfp_list)-1:
          f2 = wfp_list[cnt]

      msg = line.split()
      if len(msg) != 13:
        continue
      elif (msg[4] == 'Job') and (int(msg[5]) == base) and (msg[6] == 'finished:'):
        #print "{0} {1} {4} {3} tooks {2} s".format(msg[4], msg[5], msg[-2], msg[6], msg[7])
        sum = sum + float(msg[-2])
        base += 1
      elif (msg[4] == 'Job') and (int(msg[5]) == base) and (msg[6] == 'failed:'):
        #print "{0} {1} {4} {3} tooks {2} s".format(msg[4], msg[5], msg[-2], msg[6], msg[7])
        base += 1
        end += 1
        continue

    f1.close()

def download(app_url, tenant_id, out_dir, logger):
    remote_hostname = socket.gethostbyaddr(app_url)[0]
    tmp_file = 'spark-{0}-{1}.out'.format(tenant_id, remote_hostname)

    arg1 = "{0}:/home/{1}/notebook/logs/{2}".format(remote_hostname, tenant_id, tmp_file)
    arg2 = "{0}/{1}".format(out_dir, tmp_file)
    cmd = "scp {0} {1}".format(arg1, arg2)
    logger.info("Trying download {0} log file {1} at {2} to {3} dir".format(tenant_id, tmp_file, app_url, out_dir))
    logger.info("Download command is {0}".format(cmd))
    ret = subprocess.call(cmd, shell=True)
    if ret != 0:
      logger.error("{0} log file {1} download failure in {2} dir with {3} return code".format(tenant_id, tmp_file, out_dir, ret))
    else:
      logger.info("{0} log file {1} download success in {2} dir with {3} return code".format(tenant_id, tmp_file, out_dir, ret))

    get_app_runtime(tenant_id, arg2, logger)
    
if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='''
        TODO: add program description
    ''')
    argparser.add_argument('-i', '--infile', help='input file', default='1000_url_list.txt')
    argparser.add_argument('-l', '--logfile', help='log file', default='download.log')
    argparser.add_argument('-r', '--resultfile', help='result files, separated by comma', default='20m.log,60m.log,100m.log')
    argparser.add_argument('-v', '--version', help='spark version', default='1.4.1')
    args = argparser.parse_args()

    input_file = args.infile
    logfile = args.logfile
    resultfile = args.resultfile
    app_resultfile = resultfile.split(',')
    version = args.version

    wfp_list = []
    wfp1 = open(app_resultfile[0], 'wb')
    wfp2 = open(app_resultfile[1], 'wb')
    wfp3 = open(app_resultfile[2], 'wb')
    wfp_list.append(wfp1)
    wfp_list.append(wfp2)
    wfp_list.append(wfp3)

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

    out_dir = "{0}/tenant-{1}".format(os.getcwd(), time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))
    if not os.path.exists(out_dir):
       os.makedirs(out_dir)

    thread_list = []
    fp = open(input_file, 'r')

    for line in fp:
        msg = line.split(':')
        t = threading.Thread(name=msg[0], target=download, args=(msg[1], msg[0], out_dir, logger))
        thread_list.append(t)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    fp.close()
    for i in wfp_list:
      i.close()

    for i in app_resultfile:
      rfp = open(i, 'r+')
      summary(rfp, logger, i)
      rfp.close()

    print("Done...")

