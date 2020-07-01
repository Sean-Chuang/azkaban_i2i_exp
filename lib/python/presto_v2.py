#!/bin/env python
from __future__ import print_function
from __future__ import with_statement

import os
from pyhive import presto
from pandas import DataFrame
import numpy as np
import sys
import getopt
import re
import os
import codecs
import boto3
import time
import multiprocessing
import logging
import signal
import time
from contextlib import contextmanager

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../lib/python')
sys.path.append(os.path.dirname(os.path.abspath(__file__)) +
                '/../audience/lib/python')
import slack
import smartcommon

log = smartcommon.get_logger("smartnews.exec_presto")

USAGE = __file__ + \
    """ -f <SQL_FILE> [-e <ENGINE(dev, ad or sn)>] [-t <SECONDS_TO_WAIT>] [-s] [-k KEY=VALUE] [-d] [-F <FORMAT(csv, tsv)>]"""

ENGINES = {
    'dev': {'host': 'localhost', 'port': 28081},
    'ad': {'host': 'ad-presto.smartnews-ads.internal', 'port': 18081},
    'sn': {'host': 'presto.smartnews.internal', 'port': 8081},
    'batch': {'host': 'batch-presto.smartnews-ads.internal', 'port': 18081}
}

FORMATS = ['csv', 'tsv']

DEFAULT_PRESTO_ENGINE = os.getenv('PRESTO_ENV', 'sn')

class TimeoutException(Exception):
    pass


class ParameterException(Exception):
    pass


@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutException("Query out")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


def output(df, fmt):
    if fmt == 'csv':
        df.to_csv(sys.stdout, header=False, index=False, encoding='utf-8')
    elif fmt == 'tsv':
        df.to_csv(sys.stdout, header=False, index=False, encoding='utf-8', sep='\t')
    else:
        print('------------------------------------------')
        df.to_csv(sys.stdout, index=False, encoding='utf-8')
        print('------------------------------------------')


def presto_execute(cursor, sql, timeout, retries):
    cnt = 0
    ret = {}
    while cnt <= retries:
        try:
            log.info("Executing sql (Try #%d)" % (cnt + 1))
            start = time.time()
            with time_limit(timeout):
                cursor.execute(sql)
                ret['data'] = cursor.fetchall()

            ret['meta'] = cursor._columns
            ret['status'] = 0
            ret['time'] = time.time() - start
            log.info("Finished query in %d seconds.", ret['time'])
            return ret
        except presto.DatabaseError as e:
            cnt += 1
            if 'message'in e.args[0]:
                ret['status'] = 1
                ret['error'] = e.args[0]['message']
                log.error("Query error:\n" + e.args[0]['message'])
            else:
                ret['error'] = str(e)
                ret['status'] = 2
                log.error('Presto error Orz:')
                log.exception(e)
            if try_fix_create_table(e.args[0]['message']):
                cnt -= 1
        except TimeoutException as msg:
            cnt += 1
            ret['error'] = 'timeout'
            ret['status'] = 3
            log.error("Ran into timeout (%ds), killing execute." % timeout)
            cursor.cancel()
        except Exception as e:
            log.exception(e)
            ret['error'] = str(e)
            ret['status'] = 1
            cnt += 1
    log.error('Query execution failed %d times Orz. Bye!' % (retries + 1))
    sys.exit(ret['status'])


def run_sql(cursor, sql, timeout, retries):
    sql = sql.strip().rstrip(';')

    # support `sleep xx` statement
    sleep = re.search('^sleep (\d+)$', sql, re.IGNORECASE)
    if sleep:
        sec = int(sleep.group(1))
        time.sleep(sec)
        sql = "select %d as slept" % sec

    result = presto_execute(
        cursor, sql, timeout, retries)
    df = DataFrame(result['data'])
    if len(result['data']) != 0:
        df.columns = map(lambda x: x['name'], result['meta'])
    return df


def try_fix_create_table(err):
    regex = re.compile(
        r'^Target directory for table \'([\w\.]+)\' already exists: s3://([^/]+)/(.*)$')
    m = regex.match(err.strip())
    if m:
        table = m.group(1)
        bucket = m.group(2)
        key = m.group(3)
        print('Got directory already existed error when create table %s : s3://%s/%s' %
              (table, bucket, key))
        print('Trying fix ' + '-' * 32)
        s3 = boto3.client('s3')
        objs = s3.list_objects(Bucket=bucket, Prefix=key + '/')
        if objs:
            for obj in objs['Contents']:
                k = obj[u'Key']
                print('deleting %s' % k)
                s3.delete_object(Bucket=bucket, Key=k)
            print('Completed fix ' + '-' * 32)
            return True

    return False


def __extract_sqls(sql_text):
    regex = re.compile(r'^-- next --$', re.MULTILINE)
    return re.split(regex, sql_text)


def run(cursor, sql, fname=None, printout=True, slackNotify=False, slackRoom=None, timeout=1800, retries=1, fmt=None):
    ret = []
    for q in __extract_sqls(sql):
        df = run_sql(cursor, q, timeout, retries)
        if printout:
            output(df, fmt)
        else:
            ret.append(df)
        if slackNotify:
            slack.print_df(df, title=fname, author="py-presto",
                           color="#00bfff", channel=slackRoom)
    if not printout:
        return ret


def get_cursor(engine_name, username='dmp-kun', catalog="hive"):

    if engine_name in ENGINES:
        engine = ENGINES[engine_name]
    elif ':' in engine_name:
        engine = {
            'host': engine_name.split(':')[0],
            'port': engine_name.split(':')[1]
        }
    else:
        raise ParameterException('Unknown engine')


    return presto.connect(engine['host'], port=engine['port'],
                          username=username, catalog=catalog).cursor()


def sql(sql, engine_name=DEFAULT_PRESTO_ENGINE, username='dmp-kun', catalog="hive", timeout=1800, retries=1):
    cursor = get_cursor(engine_name, username, catalog)

    sqls = __extract_sqls(sql)
    assert len(sqls) >= 1

    if len(sqls) == 1:
        return run_sql(cursor, sqls[0], timeout, retries)
    else:
        ret = []
        for sql in sqls:
            ret.append(run_sql(cursor, sql, timeout, retries))
        return ret


def main(argv):
    engine = DEFAULT_PRESTO_ENGINE
    sql = ''
    fname = None
    slackNotify = False
    slackRoom = None
    catalog = 'hive_ad'
    kvs = {}
    debug = False
    timeout = 1800
    retries = 1
    fmt = None
    try:
        opts, args = getopt.getopt(
            argv, "hf:e:s:k:dt:r:c:F:")
    except getopt.GetoptError:
        print(USAGE)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(USAGE)
            sys.exit()
        elif opt in ("-e"):
            if arg in ENGINES:
                engine = arg
            else:
                raise ValueError("Unknown engine: " + arg)
        elif opt in ("-t"):
            timeout = int(arg)
        elif opt in ("-c"):
            catalog = arg
        elif opt in ("-r"):
            retries = int(arg)
        elif opt in ("-k"):
            idx = arg.find("=")
            if idx > 0:
                kvs[arg[0:idx]] = arg[idx + 1:]
            else:
                raise ValueError("Bad key value string " + arg)
        elif opt in ("-d"):
            debug = True
        elif opt in ("-f"):
            fname = os.path.basename(arg)
            with codecs.open(arg, 'r', 'utf-8') as f:
                sql = f.read()
        elif opt == '-s':
            slackNotify = True
            slackRoom = "#" + arg
        elif opt == '-F':
            if arg in FORMATS:
                fmt = arg
            else:
                raise ValueError("Unknown format: " + arg)
    if not sql:
        print(USAGE)
        sys.exit(2)

    for key, value in kvs.items():
        sql = sql.replace("${%s}" % key, value)

    cursor = get_cursor(engine, catalog=catalog)

    if not debug:
        run(cursor, sql, fname=fname, slackNotify=slackNotify,
            slackRoom=slackRoom, timeout=timeout, retries=retries, fmt=fmt)
    else:
        print("debug:", debug)
        print("engine:", engine)
        print("timeout:", timeout)
        print("sql file:", fname)
        print("slack notify:", slackNotify)
        print("slack room:", slackRoom)
        print("key-values:", kvs)
        print("parse sql text:")
        print("=================")
        print(sql)
        print("=================")

if __name__ == "__main__":
    main(sys.argv[1:])
