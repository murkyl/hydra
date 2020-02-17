# -*- coding: utf8 -*-
"""
Module description here
"""
__title__ = "HydraUtils"
__version__ = "1.0.0"
__all__ = [
  "config_logger",
  "create_uuid_secret",
  "get_processing_paths",
  "parse_path_file",
  "setup_logger",
  "socket_recv",
  "socket_send",
]
__author__ = "Andrew Chung <acchung@gmail.com>"
__license__ = "MIT"
__copyright__ = """Copyright 2018 Andrew Chung
Permission is hereby granted, free of charge, to any person obtaining a copy of 
this software and associated documentation files (the "Software"), to deal in 
the Software without restriction, including without limitation the rights to 
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
of the Software, and to permit persons to whom the Software is furnished to do 
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
SOFTWARE."""
import os
import sys
import logging
import datetime
import socket
import uuid
import pickle
import struct
import select
import threading
import SocketServer


MAX_BUFFER_READ_SIZE = 4194304
SHORT_WAIT_TIMEOUT = 0.05
LONG_WAIT_THRESHOLD = 20
LONG_PROCESSING_THRESHOLD = 2.5
HEARTBEAT_INTERVAL = 5                                                          # Time in seconds between each heartbeat
SELECT_POLL_INTERVAL = 0.05
IDLE_SHUTDOWN_THRESHOLD = 5*(HEARTBEAT_INTERVAL/SELECT_POLL_INTERVAL)
DIRS_PER_IDLE_WORKER = 5
DIRS_PER_IDLE_CLIENT = 5
STATS_HEARTBEAT_INTERVAL = 1                                                    # Number of heartbeats intervals per worker stat update
DEFAULT_LISTEN_ADDR = '0.0.0.0'
DEFAULT_LISTEN_PORT = 8101
DEFAULT_WEB_UI_PORT = 8100
LOOPBACK_ADDR = '127.0.0.1'
LOOPBACK_PORT = 0
SECRET_PREFIX = '********START+'
SECRET_SUFFIX = '=============='

HYDRA_OPERATION_FORMATS = [
  'dict',
  'pickle',
  'zpickle',
]
BASIC_STATS = [
  'processed_dirs',
  'queued_dirs',
  'processed_files',
  'queued_files',
  'filtered_dirs',
  'filtered_files',
  'skipped_files',
]
HYDRA_WORKER_STATES = {
  1: 'initializing',
  2: 'idle',
  3: 'processing',
  4: 'paused',
  5: 'shutdown'
}
HYDRA_WORKER_OPERATIONS = {
  10: 'proc_dir',
  11: 'proc_work',
  12: 'proc_file',
  30: 'pause',
  31: 'resume',
  32: 'shutdown',
  33: 'force_shutdown',
  50: 'return_stats',
  51: 'return_work',
  52: 'return_state',
}
HYRDRA_WORKER_RETURN_OPERATIONS = {
  10: 'status_idle',
  11: 'status_processing',
  12: 'status_paused',
  13: 'status_shutdown_complete',
  30: 'work_items',
  50: 'stats',
  51: 'state',
}
HYDRA_WORKER_WORK_TYPES = {
  1: 'dir',
  2: 'partial_dir',
  3: 'file',
}

LOGGING_CONFIG = {
  'version': 1,
  'disable_existing_loggers': False,
  'formatters': {
    'standard': {
      'format': '%(asctime)s [%(levelname)s] %(name)s - %(process)d : %(message)s',
    },
    'message': {
      'format': '%(message)s',
    },
  },
  'handlers': {
    'default': { 
      'formatter': 'standard',
      'class': 'logging.StreamHandler',
      'stream': 'ext://sys.stdout',
    },
    'file': {
      'formatter': 'standard',
      'class': 'logging.handlers.RotatingFileHandler',
      'delay': True,
      'filename': '',
      #'maxBytes': 10737418240,
      'backupCount': 10,
    },
    'audit': {
      'formatter': 'message',
      'class': 'logging.handlers.RotatingFileHandler',
      'delay': True,
      'filename': '',
      #'maxBytes': 10737418240,
      'backupCount': 10,
    },
  },
  'loggers': {
    # Root logger
    '': {
      'handlers': ['default'],
      'level': 'WARN',
      #'propagate': False,
    },
    # Default logger used when asking for an unknown logger
    'default': {
      'handlers': ['default'],
      'level': 'WARN',
      #'propagate': False,
    },
    # if __name__ == '__main__' logger for running as stand alone program vs import as a module
    '__main__': {
      'handlers': ['default'],
      'level': 'WARN',
      #'propagate': False,
    },
    # Audit log output
    'audit': {
      'handlers': ['audit'],
      'level': 'INFO',
      'propagate': False,
    },
  }
}

LOGGING_ALT_CONFIG = {
  'handlers': {
    'file': {
      'formatter': 'standard',
      'class': 'logging.handlers.RotatingFileHandler',
      'delay': True,
      'filename': '',
      #'maxBytes': 10737418240,
      #'backupCount': 10,
    },
  #  'audit': {
  #    'level': 'INFO',
  #    'formatter': 'message',
  #    'class': 'logging.handlers.RotatingFileHandler',
  #    'filename': None,
  #    #'maxBytes': 10737418240,
  #    #'backupCount': 10,
  #  },
  },
  'loggers': {
  #  'audit': {
  #    'handlers': ['audit'],
  #    'level': 'INFO',
  #    'propagate': False,
  #  },
  }
}

def config_logger(log_cfg, name, log_level=logging.WARN, file=None):
  c = log_cfg['loggers'].get(name)
  if not c:
    c = LOGGING_ALT_CONFIG['loggers'].get(name)
    if not c:
      return
    #cfg['loggers'][name] = dict(log_cfg['loggers']['default'])
    log_cfg['loggers'][name] = c
  log_cfg['loggers'][name]['level'] = log_level
  
  handlers = log_cfg['loggers'][name].get('handlers')
  if len(handlers) == 1:
    handler_name = handlers[0]
    h = log_cfg['handlers'].get(handler_name)
    if not h:
      h = LOGGING_ALT_CONFIG['handlers'].get(name)
      if not h:
        h = log_cfg['handlers']['default']
      log_cfg['handlers'][name] = h
    if file:
      if name == 'audit':
        log_cfg['handlers'][name]['filename'] = file
      else:
        log_cfg['loggers'][name]['handlers'] = ['file']
        log_cfg['handlers']['file']['filename'] = file

def create_uuid_secret():
    return bytes(SECRET_PREFIX + str(uuid.uuid4()) + SECRET_SUFFIX, encoding='utf-8')
    
def get_processing_paths(path_array, path_file):
  """
  Fill in docstring
  """
  path_files = parse_path_file(path_file)
  if not path_array:
    path_array = []
  all_paths = path_array + path_files
  # Remove duplicates
  all_paths = list(set(all_paths))
  return all_paths

def parse_path_file(filename):
  """
  Fill in docstring
  """
  paths = []
  if filename:
    with open(filename) as f:
      paths = [x.rstrip('\n') for x in f.readlines()]
  return paths

def setup_logger(log_cfg, name, log_level = 0, filename=None):
  """
  Fill in docstring
  """
  if name:
    log = logging.getLogger(name)
    log.propagate = False
  else:
    log = logging.getLogger('')
  if filename:
    log_filename = datetime.datetime.now().strftime(filename)
    log_filename = log_filename.replace('{pid}', str(os.getpid()))
    log_filename = log_filename.replace('{host}', socket.gethostname())
    ch = logging.FileHandler(log_filename)
  else:
    ch = logging.StreamHandler(sys.stdout)
  if log_level > 2:
    log.setLevel(5)
  elif log_level > 1:
    log.setLevel(9)
  elif log_level > 0:
    log.setLevel(logging.DEBUG)
  else:
    log.setLevel(logging.INFO)
  formatter = logging.Formatter(format)
  ch.setFormatter(formatter)
  if len(log.handlers) == 0:
    log.addHandler(ch)
  return log

def socket_recv(sock, len, chunk=131072, raw=False):
  data = bytearray(len)
  view = memoryview(data)
  while(len):
    read_len = chunk
    if chunk > len:
      read_len = len
    bytes_recv = sock.recv_into(view, read_len)
    view = view[bytes_recv:]
    len -= bytes_recv
  if not raw:
    data = pickle.loads(data)
  return data

def socket_send(sock, data):
  bytes_data = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
  bytes_len = len(bytes_data)
  header = struct.pack('!L', bytes_len)
  sock.sendall(header + bytes_data)
  return (bytes_len + 4)
