from sqlalchemy import create_engine
import os

CONFIGURATION = {'development': {'db': 'smartad_staging',
                                 'user': 'smartad',
                                 'pwd': 'phCURg-8R8',
                                 'host': 'localhost',
                                 'port': '5439'
                                 },
                 'production': {'db': 'smartad_production',
                                'user': 'smartad',
                                'pwd': 'phCURg-8R8',
                                'host': 'smartad-log.clexu9ntkg8m.ap-northeast-1.redshift.amazonaws.com',
                                'port': '5439'

                                }}

env = os.getenv('ENV', 'development')
config = CONFIGURATION[env]


def get_engine(*args, **kwargs):
    return create_engine("redshift+psycopg2://%s:%s@%s:%s/%s" %
                         (config['user'], config['pwd'], config['host'], config['port'], config['db']))


def query(*args, **kwargs):
    q = args[0]
    connection = get_engine().connect()
    result = connection.execute(q)
    connection.close()
    return result


def query_as_df(*args, **kwargs):
    from pandas import DataFrame
    result = query(*args, **kwargs)
    df = DataFrame(result.fetchall())
    df.columns = result.keys()
    return df
