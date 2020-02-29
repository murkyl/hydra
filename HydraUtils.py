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
  "is_invalid_windows_filename",
  "parse_path_file",
  "set_logger_handler_to_socket",
  "set_logger_logger_level",
  "setup_logger",
  "IndentedHelpFormatterWithNL",
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
import logging.handlers
import datetime
import socket
import uuid
import pickle
import struct
import select
import threading
import optparse
import textwrap
import re
try:
  import socketserver
except:
  import SocketServer as socketserver



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

MAX_WINDOWS_FILEPATH_LENGTH = 259                           # Excludes the NUL
INVALID_WINDOWS_FILENAME_CHARS = '\x00-\x1f,\|\\/:\*\?"<>'
INVALID_WINDOWS_FILENAME_ROOTS = [
  'CON',
  'PRN',
  'AUX',
  'NUL',
  'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
  'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9',
]
INVALID_WINDOWS_FILENAME_REGEX = None
INVALID_WINDOWS_FILECHAR_REGEX = None

LOGGING_CONFIG = {
  'version': 1,
  'disable_existing_loggers': False,
  'formatters': {
    'standard': {
      'format': '%(asctime)s [%(levelname)s] %(name)s - %(process)d : %(message)s',
    },
    'debug': {
      'format': '%(asctime)s [%(levelname)s] %(name)s [%(funcName)s (%(lineno)d)] - %(process)d : %(message)s',
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
    'null': {
      'class': 'logging.NullHandler',
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
      'propagate': False,
    },
    # Audit log output
    'audit': {
      'handlers': ['null'],
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

LOGGING_WORKER_CONFIG = {
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
      'class': 'HydraUtils.SecureSocketHandler',
      'host': '127.0.0.1',
      'port': 0,
      'secret': '',
    },
  },
  'loggers': {
    '': {
      'handlers': ['default'],
      'level': 'WARN',
    },
  },
}


def is_invalid_windows_filename(file):
  global INVALID_WINDOWS_FILENAME_REGEX
  global INVALID_WINDOWS_FILECHAR_REGEX
  if INVALID_WINDOWS_FILENAME_REGEX is None:
    roots = '|'.join(INVALID_WINDOWS_FILENAME_ROOTS)
    INVALID_WINDOWS_FILENAME_REGEX = re.compile("^(%s)($|\..*$)"%roots, re.I)
  if INVALID_WINDOWS_FILECHAR_REGEX is None:
    INVALID_WINDOWS_FILECHAR_REGEX = re.compile(".*[%s].*"%INVALID_WINDOWS_FILENAME_CHARS)
  if INVALID_WINDOWS_FILENAME_REGEX.match(file):
    return True
  if INVALID_WINDOWS_FILECHAR_REGEX.match(file):
    return True
  return False

# Used mainly as a helper for configuring tests
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
  if type(b'1') is str:
    return bytearray(SECRET_PREFIX + str(uuid.uuid4()) + SECRET_SUFFIX)
  return bytes(SECRET_PREFIX + str(uuid.uuid4()) + SECRET_SUFFIX, encoding='utf-8')
    
def get_processing_paths(path_array, path_file=None):
  """
  Fill in docstring
  """
  path_files = parse_path_file(path_file)
  if not isinstance(path_array, list):
    path_array = [path_array]
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

def set_logger_handler_to_socket(log_cfg, name='default', host=LOOPBACK_ADDR, port=LOOPBACK_PORT, secret=''):
  if name not in log_cfg['handlers']:
    log_cfg['handlers'][name] = {}
  log_cfg['handlers'][name]['class'] = 'HydraUtils.SecureSocketHandler'
  log_cfg['handlers'][name]['host'] = host
  log_cfg['handlers'][name]['port'] = port
  log_cfg['handlers'][name]['secret'] = secret

def set_logger_logger_level(log_cfg, name='', level=logging.WARN):
  log_cfg['loggers'][name]['level'] = level

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
  
class IndentedHelpFormatterWithNL(optparse.IndentedHelpFormatter):
    def format_description(self, description):
        if not description: return ""
        desc_width = self.width - self.current_indent
        indent = " "*self.current_indent
        # the above is still the same
        bits = description.split('\n')
        formatted_bits = [
            textwrap.fill(bit,
                desc_width,
                initial_indent=indent,
                subsequent_indent=indent)
            for bit in bits]
        result = "\n".join(formatted_bits) + "\n"
        return result
    
    def format_option(self, option):
        # The help for each option consists of two parts:
        #   * the opt strings and metavars
        #   eg. ("-x", or "-fFILENAME, --file=FILENAME")
        #   * the user-supplied help string
        #   eg. ("turn on expert mode", "read data from FILENAME")
        #
        # If possible, we write both of these on the same line:
        #   -x    turn on expert mode
        #
        # But if the opt string list is too long, we put the help
        # string on a second line, indented to the same column it would
        # start in if it fit on the first line.
        #   -fFILENAME, --file=FILENAME
        #       read data from FILENAME
        result = []
        opts = self.option_strings[option]
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", opts)
            indent_first = self.help_position
        else: # start help on same line as opts
            opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts)
            indent_first = 0
        result.append(opts)
        if option.help:
            help_text = self.expand_default(option)
            # Everything is the same up through here
            help_lines = []
            for para in help_text.split("\n"):
                help_lines.extend(textwrap.wrap(para, self.help_width))
            # Everything is the same after here
            result.append("%*s%s\n" % (indent_first, "", help_lines[0]))
            result.extend(["%*s%s\n" % (self.help_position, "", line) for line in help_lines[1:]])
        elif opts[-1] != "\n":
            result.append("\n")
        return "".join(result)

class LogRecordStreamHandler():
  def __init__(self, name='', addr='127.0.0.1', port=0, timeout=0.05):
    self.log_socket = None
    self.log_port = None
    self.log_secret = ''
    self.timeout = timeout
    self.shutdown = False
    self.name = name
    self.server_thread = None
    self.inputs = []
    self._setup_log_socket(addr, port)
    
  def get_port(self):
    return self.log_port
    
  def get_secret(self):
    return self.log_secret

  def handle(self):
    while not self.shutdown and self.inputs:
      readable, _, _ = select.select(self.inputs, [], [], self.timeout)
      for s in readable:
        if s is self.log_socket:
          connection, client_address = s.accept()
          connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
          security = connection.recv(len(self.log_secret))
          if security != self.log_secret:
            logging.getLogger(self.name).error('Log client did not send proper secret: %s'%client_address)
            connection.close()
            continue
          self.inputs.append(connection)
        else:
          msg_size = s.recv(4)
          if len(msg_size) != 4:
            self.inputs.remove(s)
            s.close()
            continue
          slen = struct.unpack('>L', msg_size)[0]
          data = socket_recv(s, slen)
          record = logging.makeLogRecord(data)
          logger = logging.getLogger(self.name)
          logger.handle(record)
    
  def start_logger(self):
    self.server_thread = threading.Thread(target=self.handle)
    self.server_thread.daemon = True
    self.server_thread.start()
  
  def stop_logger(self):
    self.shutdown = True

  def _setup_log_socket(self, addr, port):
    # Create socket to listen for log and audit events from workers.
    # Workers are in separate processes to avoid GIL issues and the Python
    # logger is not process safe. It is however thread safe.
    self.log_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.log_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.log_socket.bind((addr, port))
    self.log_port = self.log_socket.getsockname()[1]
    self.log_socket.listen(5)
    self.log_secret = create_uuid_secret()
    self.inputs.append(self.log_socket)

class SecureSocketHandler(logging.handlers.SocketHandler):
  def __init__(self, host=LOOPBACK_ADDR, port=LOOPBACK_PORT, secret='', **kwargs):
    # To allow simple dictconfig configuration, accept and discard kwargs
    super(SecureSocketHandler, self).__init__(host, port)
    self.secret = secret

  # Override of base class makeSocket to send a simple secret value on connect
  def makeSocket(self, timeout=1):
    """
    A factory method which allows subclasses to define the precise
    type of socket they want.
    """
    if self.port is not None:
      result = socket.create_connection(self.address, timeout=timeout)
      if result:
        result.sendall(self.secret)
    else:
      result = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
      result.settimeout(timeout)
      try:
        result.connect(self.address)
        if result:
          result.sendall(self.secret)
      except OSError:
        result.close()
        raise
    return result
