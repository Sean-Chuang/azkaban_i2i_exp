from __future__ import print_function

import smartaws
import boto3
import os

#from boto.s3.key import Key
#from boto.s3.connection import S3Connection


def put_file(local_file, s3_path, cb=None, bucket=smartaws.DMP_BUCKET):
    with open(local_file, 'rb') as data:
        boto3.resource('s3').Bucket(bucket).upload_fileobj(
            data, s3_path, Callback=cb)


def get_file_content(key, bucket=smartaws.DMP_BUCKET):
    return boto3.resource('s3').Object(bucket, key).get()["Body"].read().decode("utf-8")


def __debug_cb(bytes):
    print("complete: %s bytes" % bytes)

# test..
if __name__ == '__main__':
    put_file(__file__, 'test/lan/s3_test.py', cb=__debug_cb)
    print(get_file_content('test/lan/s3_test.py'))
