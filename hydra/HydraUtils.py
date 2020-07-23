# -*- coding: utf8 -*-
"""
Module description here
"""
__title__ = "HydraUtils"
__version__ = "1.1.0"
__all__ = [
  "create_uuid_secret",
  "get_processing_paths",
  "is_invalid_windows_filename",
  "parse_path_file",
  "socket_recv",
  "socket_send",
  "IndentedHelpFormatterWithNL",
  "LogRecordStreamServer",
  "SecureSocketHandler",
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
import logging
import logging.handlers
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
MAX_DEFAULT_WORKERS = 8                                                         # Reasonable max number of workers when auto scaling
SHORT_WAIT_TIMEOUT = 0.05
LONG_WAIT_THRESHOLD = 20
LONG_PROCESSING_THRESHOLD = 2.5
HEARTBEAT_INTERVAL = 5                                                          # Time in seconds between each heartbeat
SELECT_POLL_INTERVAL = 0.05
WORKER_SHUTDOWN_TIMEOUT = 5
WORKER_SHUTDOWN_SLEEP_INTERVAL = 0.1
LOG_SVR_SHUTDOWN_TIMEOUT = 5
LOG_SVR_SHUTDOWN_SLEEP_INTERVAL = 0.1
IDLE_SHUTDOWN_THRESHOLD = 5*(HEARTBEAT_INTERVAL/SELECT_POLL_INTERVAL)
DIRS_PER_IDLE_WORKER = 5
DIRS_PER_IDLE_CLIENT = 5
STATS_HEARTBEAT_INTERVAL = 3                                                    # Number of heartbeats intervals per worker stat update
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
  'filtered_dirs',
  'processed_files',
  'filtered_files',
  'skipped_files',
  'queued_dirs',
  'queued_files',
]

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
INVALID_WINDOWS_FILENAME_REGEX = re.compile("^(%s)($|\..*$)"%('|'.join(INVALID_WINDOWS_FILENAME_ROOTS)), re.I)
INVALID_WINDOWS_FILECHAR_REGEX = re.compile(".*[%s].*"%INVALID_WINDOWS_FILENAME_CHARS)


def is_invalid_windows_filename(file):
  if INVALID_WINDOWS_FILENAME_REGEX.match(file):
    return True
  if INVALID_WINDOWS_FILECHAR_REGEX.match(file):
    return True
  return False

def create_uuid_secret():
  if type(b'1') is str:
    return bytearray(SECRET_PREFIX + str(uuid.uuid4()) + SECRET_SUFFIX)
  return bytes(SECRET_PREFIX + str(uuid.uuid4()) + SECRET_SUFFIX, encoding='utf-8')
    
def get_processing_paths(path_array, path_file=None):
  """
  Fill in docstring
  """
  if not path_array and not path_file:
    return []
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
  return (bytes_len + len(header))
  
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

class LogRecordStreamServer():
  def __init__(self, name='', addr=LOOPBACK_ADDR, port=LOOPBACK_PORT, timeout=0.05):
    self.log_socket = None
    self.log_port = None
    self.log_secret = ''
    self.timeout = timeout
    self.shutdown = False
    self.name = name
    self.server_thread = None
    self.inputs = []
    self._setup_log_socket(addr, port)
    self.log_bytes = 0
    self.log_entries = 0
    self.log = logging.getLogger(self.name)
    
  def get_port(self):
    return self.log_port
    
  def get_secret(self):
    return self.log_secret
  
  def is_alive(self):
    if self.server_thread:
      return self.server_thread.is_alive()
    return False

  def handle(self):
    while not self.shutdown and self.inputs:
      readable, _, _ = select.select(self.inputs, [], [], self.timeout)
      for s in readable:
        if s is self.log_socket:
          connection, client_address = s.accept()
          connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
          security = connection.recv(len(self.log_secret))
          if security != self.log_secret:
            self.log.error('Log client did not send proper secret: %s'%client_address)
            connection.close()
            continue
          self.inputs.append(connection)
        else:
          slen = 0
          msg_size = s.recv(4)
          if len(msg_size) == 4:
            slen = struct.unpack('>L', msg_size)[0]
          if slen == 0:
            self.inputs.remove(s)
            try:
              s.shutdown(socket.SHUT_RDWR)
            except:
              pass
            try:
              s.close()
            except:
              pass
            self.shutdown = True
            continue
          data = socket_recv(s, slen)
          record = logging.makeLogRecord(data)
          self.log_bytes += (slen + 4)
          self.log_entries += 1
          if record.name == self.name:
            self.log.handle(record)
          else:
            logging.getLogger(record.name).handle(record)
    for s in self.inputs:
      try:
        s.shutdown(socket.SHUT_RDWR)
      except:
        pass
      try:
        s.close()
      except:
        pass
    logging.getLogger(self.name).debug("Bytes logged: %s in %s messages"%(self.log_bytes, self.log_entries))
    
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
    self.msg_queue = []

  # Override of base class makeSocket to send a simple secret value on connect
  def makeSocket(self, timeout=1):
    """
    A factory method which allows subclasses to define the precise
    type of socket they want.
    """
    if self.port is not None:
      result = socket.create_connection((self.host, self.port), timeout=timeout)
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

  def emit(self, record):
    """
    Emit a record.
    Pickles the record and writes it to the socket in binary format.
    If there is an error with the socket, silently drop the packet.
    If there was a problem with the socket, re-establishes the
    socket.
    
    This can still lose messages if the self.sock.sendall() command
    fails. May need to override the base class for send and also
    queue messages there.
    """
    while self.msg_queue:
      try:
        msg = self.msg_queue[0]
        self.send(msg)
        self.msg_queue.pop(0)
      except:
        self.msg_queue.append(self.makePickle(record))
        return
    try:
      s = self.makePickle(record)
      self.send(s)
    except Exception:
      self.msg_queue.append(record)
      self.handleError(record)
