#!/bin/env python2.7

import boto3
import smartaws
import json

SMART_TOPICS = {
    "ad_dmp_dev": "arn:aws:sns:ap-northeast-1:165463520094:smartad-dmp-dev",
    "partner_receive": "arn:aws:sns:ap-northeast-1:165463520094:ad_partners_receive",
    "partner_receive_stg": "arn:aws:sns:ap-northeast-1:165463520094:ad_partners_receive_stg",
    "partner_receive_dev": "arn:aws:sns:ap-northeast-1:165463520094:ad_partners_receive_dev"
}

SMART_EVENT = {
    'ABF_DAILY_COMPLETE': "abf_daily_update.complete",
    'APP_ABF_COMPLETE': "app_abf_update.complete",
    "ONLINE_PREDICTION_RELOAD": "online_prediction_model.reload"
}

client = None


def _initial():
    global client
    if client is None:
        client = boto3.client('sns')


def publish(topic, data, subject=None, printout=False):
    global client
    msg = json.dumps(data)
    _initial()
    client.publish(
        TopicArn=topic,
        Message=msg,
        Subject=subject or data.get('event') or 'msg'
    )
    if printout:
        print msg


if __name__ == '__main__':
    print "hi"
    publish(SMART_TOPICS['ad_dmp_dev'], {'msg': 'hello world'}, printout=True)
