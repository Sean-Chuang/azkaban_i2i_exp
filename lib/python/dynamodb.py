#!/bin/env python2.7
from __future__ import print_function

import smartaws
import boto3
import decimal
from botocore.compat import six
from boto3.dynamodb.conditions import Key, Attr
import logging
import gzip
import article_history_pb2

#from boto.dynamodb.types import DYNAMODB_CONTEXT
#DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
#DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0


class DDBTables(dict):

    def __init__(self):
        super().__init__()

        self.dynamodb = boto3.resource(
            'dynamodb', region_name='ap-northeast-1')
        self.mapping = {
            'se_articles': 'smartengine_article_prd',
            'user_history': 'sn_feed_user_history',
            'ad_audience': 'prod_smartad_dmp_audience',
            'ad_retargeting': 'prod_smartad_dmp_retargeting'
        }

    def __getitem__(self, key):
        if not dict.__contains__(self, key):
            tbl_name = self.mapping[key]
            logging.getLogger('sn.dynamodb').info(
                'intial dynamodb table {name}'.format(name=tbl_name))
            tbl = self.dynamodb.Table(tbl_name)
            dict.__setitem__(self, key, tbl)

        return dict.__getitem__(self, key)


class SnDDB:

    def __init__(self):
        self.dynamodb = boto3.resource(
            'dynamodb', region_name='ap-northeast-1')

        self.tables = DDBTables()

    def get_article_by_url(self, url):
        response = self.tables['se_articles'].get_item(Key={'url': url})

        if 'Item' in response:
            return response['Item']
        else:
            return None

    def get_article(self, link_id):
        response = self.tables['se_articles'].query(
            IndexName='sequence-index',
            KeyConditionExpression=Key('sequence').eq(link_id)
        )
        if len(response['Items']) > 0:
            return response['Items'][0]
        else:
            return None

    def get_user_history(self, device_token, edition):
        response = self.tables['user_history'].get_item(
            Key={'device_token': device_token, 'edition': edition},
            AttributesToGet=['read']
        )
        if 'Item' in response and response['Item']:
            item = response['Item']['read']
            msg = gzip.decompress(item.value)
            ah = article_history_pb2.ArticleHistory.FromString(msg)
            return ah
        else:
            return None

    def device_token_to_ad_id(self, device_token):
        response = tables['ad_audience'].query(
            IndexName='device_token-last_seen-index',
            KeyConditionExpression=Key('device_token').eq(device_token),
            Limit=1,
            ScanIndexForward=False
        )
        if len(response['Items']) == 1:
            return response['Items'][0]['ad_id']
        else:
            return None

    def ddb_get_ad_retargeting(self, ad_id, label):
        response = tables['ad_retargeting'].get_item(
            Key={'user': ad_id, 'label': label},
            AttributesToGet=['i']
        )
        if 'Item' in response and response['Item']:
            return response['Item']['i']
        else:
            return None

    def update_feed_ddb_param(self, key, label, value):
        client = boto3.client('dynamodb')
        import time
        import json
        mtime = int(time.time())

        client.put_item(
            TableName='sn_feed_config',
            Item={
                'key': {
                    'S': key
                },
                'label': {
                    'S':  label
                },
                'mtime': {
                    'N': str(mtime)
                },
                'value': {
                    'S': json.dumps(value)
                }
            }
        )


def from_decimals(obj):
    if isinstance(obj, list):
        for i in xrange(len(obj)):
            obj[i] = from_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.iterkeys():
            obj[k] = from_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj


def to_decimals(obj):
    if isinstance(obj, list):
        for i in xrange(len(obj)):
            obj[i] = to_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = to_decimals(obj[k])
        return obj
    elif isinstance(obj, (six.integer_types, decimal.Decimal)):
        return obj
    elif isinstance(obj, float):
        return decimal.Decimal(str(obj))
    else:
        return obj

miscTable = None


def prepare_table():
    global miscTable
    if miscTable is None:
        dynamodb = boto3.resource('dynamodb')
        miscTable = dynamodb.Table(smartaws.DYNAMODB_DMP_OPMISC_TABLE)

    return miscTable


def misc_get(key, range):
    table = prepare_table()

    return table.get_item(Key={'key': key, 'range': range}).get('Item')


def misc_put(key, range, item):
    table = prepare_table()
    item['key'] = key
    item['range'] = range
    return table.put_item(TableName=smartaws.DYNAMODB_DMP_OPMISC_TABLE,
                          Item=to_decimals(item))


if __name__ == '__main__':
    misc_put('test', '0', {'v': 'smartnews', 'x': '??'})
    print(misc_get('test', '0'))
    misc_put('test', '0', {'v': 'smartad'})
    print(misc_get('test', '0'))
