import socket
import logging

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

logging.getLogger("botocore").setLevel(logging.WARN)

def get_logger(name):
    return logging.getLogger(name)


def set_logger_level(name, level):
	logging.getLogger(name).setLevel(level)


def is_dev():
    hn = socket.getfqdn()
    return not socket.getfqdn().endswith('compute.internal')
