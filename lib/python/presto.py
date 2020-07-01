from __future__ import print_function
from pyhive import presto
from pandas import DataFrame
from datetime import datetime
import logging
import smartcommon
import os

log = smartcommon.get_logger("smartnews.presto")

ENGINES = {
    'dev': {'host': 'localhost', 'port': 28081},
    'ad': {'host': 'ad-presto.smartnews-ads.internal', 'port': 18081},
    'sn': {'host': 'presto.smartnews.internal', 'port': 8081}
}


def get_ad_presto():
    host = 'ad-presto.smartnews-ads.internal'
    if is_dev():
        host = 'localhost'
    return presto.connect(host, port=18081, catalog='hive_ad').cursor()


# deprecated
def qubole_q(sql):
    return execute_presto(sql, engine=None)


def execute_presto(sql, engine):
    if engine is None:
        if 'PRESTO_ENGINE' in os.environ:
            engine = os.environ['PRESTO_ENGINE']
        else:   
            engine = "sn"

    log.info('executing...')
    addr = ENGINES[engine]
    p = presto.connect(addr['host'], port=addr[
        'port'], catalog='hive_ad', username='dmp-kun').cursor()
    try:
        p.execute(sql)
        d = p.fetchall()
        df = DataFrame(d)
        log.info('done')
        if len(d) != 0:
            df.columns = map(lambda x: x['name'], p._columns)
        return df
    except presto.DatabaseError as e:
        log.exception(e)


# deprecated
def q(sql, engine=None):
    return execute_presto(sql, engine)


def query(sql, engine=None):
    return execute_presto(sql, engine)
