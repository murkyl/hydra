# -*- coding: utf8 -*-
"""
Module description here
"""
__title__ = "HydraWoker"
__version__ = "1.1.0"
__all__ = ["HydraWorker"]
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

"""
TODO:
For the partial directory case, we can further break this up into processing
a subset of the files in the directory. This case could happen if the per file
handling routine takes a long time or there are a very high number of files in
a single directory. A method to do this would be during a return_work request
we check if we have no other work to return other than a lot of files in the
current work item. We then break it up into 2 partial_dir work items and return
1 of them. We do need to tag this as partial partial directory with 2+ parts.
Then we add some handling to make sure we count when all these partial partials
completes before we consider the directory completed. This could end up getting
split over many processes. A simplification would be to require 1 directory
to be fully processed on a single machine versus having these partial partial
directories processed over several machines and several processes/machine.

In Python 3.8 consider using shared memory to improve performance between client
and workers

"""

import inspect
import traceback
import os
import platform
from time import time as time_now
from copy import deepcopy
import socket
from select import select
from struct import unpack
from multiprocessing import Process
import logging
import logging.config
try:
   import cPickle as pickle
except:
   import pickle
import zlib
from collections import deque
from . import HydraUtils

try:
  from os import scandir
  fswalk = os.walk          # If scandir exists os.walk will use it
except ImportError:
  # Figure out which version of scandir to use because we need to
  # account for the different binaries used.
  current_file = inspect.getfile(inspect.currentframe())
  base_path = os.path.dirname(os.path.abspath(current_file))
  scandir_path = 'scandir_generic'
  local_os = platform.platform()
  if 'Linux' in local_os:
    if 'x86_64' in local_os:
      scandir_path = 'scandir_linux_x86_64'
  elif 'Windows' in local_os:
    scandir_path = 'scandir_windows'
  elif 'Isilon' in local_os:
    # OneFS < 8.0 is based on FreeBSD 7.1, the FreeBSD 7.1 scandir
    # binary is slightly older than the other binaries (scandir v1.5
    # vs. v1.9) due to the EOSL of FreeBSD 7.1
    # OneFS 8.0 is based on FreeBSD 10
    # OneFS 8.1 is based on FreeBSD 11
    # OneFS 8.2 is based on FreeBSD 12
    if 'OneFS-v9.0' in local_os:
      scandir_path = None
    elif 'OneFS-v8.2' in local_os:
      scandir_path = None
    elif 'OneFS-v8.1' in local_os:
      scandir_path = 'scandir_freebsd11'
    else:
      scandir_path = 'scandir_generic'
  elif 'FreeBSD' in local_os:
    if 'FreeBSD-11' in local_os:
      scandir_path = 'scandir_freebsd11'
    elif 'FreeBSD-10' in local_os:
      scandir_path = 'scandir_freebsd10'
    elif 'FreeBSD-7' in local_os:
      scandir_path = 'scandir_freebsd7'
  if scandir_path:
    from sys import path
    path.insert(0, os.path.join(base_path, 'lib', scandir_path))
    try:
      import scandir
    except:
      scandir = os
  else:
    scandir = os
  fswalk = scandir.walk

# Possible state machine states. Sub states are split using the _ character
STATE_INIT = 'init'
STATE_CONNECTING = 'connecting'
STATE_IDLE = 'idle'
STATE_PAUSED = 'paused'
STATE_PROCESSING = 'processing'
STATE_PROCESSING_PAUSED = 'processing_paused'
STATE_SHUTDOWN = 'shutdown'

EVENT_CONNECT = 'connect'
EVENT_PAUSE = 'pause'
EVENT_PROCESS_DIR = 'proc_dir'
EVENT_PROCESS_WORK = 'proc_work'
EVENT_QUERY_STATE = 'return_state'
EVENT_QUERY_STATS = 'return_stats'
EVENT_RESUME = 'resume'
EVENT_RETURN_WORK = 'return_work'
EVENT_SHUTDOWN = 'shutdown'
EVENT_UPDATE_SETTINGS = 'update_settings'

# Commands sent from the worker to a client
CMD_STATE = 'worker_state'
CMD_STATS = 'worker_stats'
CMD_WORK_ITEMS = 'worker_work_items'
CMD_WORK_QUEUE_EMPTY = 'worker_queue_empty'

'''
CMD_STATE - Send state as a string using the STATE_* definitions above
Format: {'op': CMD_STATE, 'data': <state>}

CMD_STATS - 

CMD_WORK_QUEUE_EMPTY  - Used as an internal command only

CMD_WORK_ITEMS - Send unprocessed directories
Format: {'op': CMD_WORK_ITEMS, 'data': <work_item>}
work_item: [{'type': 'dir', 'path': <string>},...]

'''

HYDRA_WORKER_STATE_TABLE = {
  STATE_INIT: {
      EVENT_CONNECT:          {'a': '_h_connect',             'ns': STATE_IDLE},
  },
  STATE_IDLE: {
      #EVENT_CONNECT:          {'a': '_h_',                    'ns': None},
      EVENT_PAUSE:            {'a': '_h_idle_pause',          'ns': STATE_PAUSED},
      EVENT_PROCESS_DIR:      {'a': '_h_proc_dir',            'ns': STATE_PROCESSING},
      EVENT_PROCESS_WORK:     {'a': '_h_proc_work',           'ns': STATE_PROCESSING},
      EVENT_QUERY_STATE:      {'a': '_h_query_state',         'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      #EVENT_RESUME:           {'a': '_h_',                    'ns': None},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
      CMD_WORK_QUEUE_EMPTY:   {'a': '_h_no_op',               'ns': None},
  },
  STATE_PROCESSING: {
      #EVENT_CONNECT:          {'a': '_h_',                    'ns': None},
      EVENT_PAUSE:            {'a': '_h_processing_pause',    'ns': STATE_PROCESSING_PAUSED},
      EVENT_PROCESS_DIR:      {'a': '_h_proc_dir',            'ns': None},
      EVENT_PROCESS_WORK:     {'a': '_h_proc_work',           'ns': None},
      EVENT_QUERY_STATE:      {'a': '_h_query_state',         'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      #EVENT_RESUME:           {'a': '_h_',                    'ns': None},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
      CMD_WORK_QUEUE_EMPTY:   {'a': '_h_no_op',               'ns': STATE_IDLE},
  },
  STATE_PROCESSING_PAUSED: {
      #EVENT_CONNECT:          {'a': '_h_',                    'ns': None},
      EVENT_PAUSE:            {'a': '_h_no_op',               'ns': None},
      EVENT_PROCESS_DIR:      {'a': '_h_proc_dir',            'ns': None},
      EVENT_PROCESS_WORK:     {'a': '_h_proc_work',           'ns': None},
      EVENT_QUERY_STATE:      {'a': '_h_query_state',         'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_RESUME:           {'a': '_h_resume',              'ns': STATE_PROCESSING},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
  },
  STATE_PAUSED: {
      #EVENT_CONNECT:          {'a': '_h_',                    'ns': None},
      EVENT_PAUSE:            {'a': '_h_no_op',               'ns': None},
      EVENT_PROCESS_DIR:      {'a': '_h_proc_dir',            'ns': None},
      EVENT_PROCESS_WORK:     {'a': '_h_proc_work',           'ns': None},
      EVENT_QUERY_STATE:      {'a': '_h_query_state',         'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_RESUME:           {'a': '_h_resume',              'ns': STATE_IDLE},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
  },
  STATE_SHUTDOWN: {
      #EVENT_CONNECT:          {'a': '_h_',                    'ns': None},
      #EVENT_PAUSE:            {'a': '_h_no_op',               'ns': None},
      #EVENT_PROCESS_DIR:      {'a': '_h_no_op',               'ns': None},
      #EVENT_PROCESS_WORK:     {'a': '_h_no_op',               'ns': None},
      EVENT_QUERY_STATE:      {'a': '_h_query_state',         'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      #EVENT_RESUME:           {'a': '_h_no_op',               'ns': None},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_no_op',               'ns': None},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
  },
}

class HydraWorker(Process):
  def __init__(self, args={}):
    """
    Fill in docstring
    """
    super(HydraWorker, self).__init__()
    self.log = logging.getLogger(__name__)
    self.args = deepcopy(args)
    self.fswalk = fswalk
    self.loopback_addr = args.get('loopback_addr', HydraUtils.LOOPBACK_ADDR)
    self.loopback_port = args.get('loopback_port', HydraUtils.LOOPBACK_PORT)
    self.client_conn = None             # Socket used to communicate between the client and worker
    self.worker_conn = None             # Socket used to communicate between the client and worker
    self.work_queue = deque()           # Queue used to hold work items
    self.inputs = []                    # Sockets from which we expect to read from through a select call
    self.stats = {}
    self.state = STATE_INIT
    self.state_table = {}
    self.init_stats()
    self._init_state_table(HYDRA_WORKER_STATE_TABLE)
    self._process_state_event(EVENT_CONNECT)
    
  def filter_subdirectories(self, root, dirs, files):
    """
    This method should filter a list of files and directories to process.
    The filter can do nothing and just pass the same inputs back, which is what
    the default implementation does.
    
    This method should not be called from user code. It is exposed for
    subclassing :class:`HydraWorker <multiprocessing.Process>`.
    
    :param root: The UTF8 string representing the base directory
    :param dirs: A list of subdirectories in the root parameter
    :param files: A list of files in the root parameter
    """
    return dirs, files

  def handle_directory_pre(self, dir):
    """
    Return True if this method handled the entire directory and no
    files or subdirs from this point onward should be processed.
    Return False if normal processing should occur
    The default implementation immediately returns False.
    
    This method should not be called from user code. It is exposed for
    subclassing :class:`HydraWorker <multiprocessing.Process>`.
    
    :param dir: Path to the directory where pre-processing should be
    performed.
    """
    return False

  def handle_directory_post(self, dir):
    """
    This method is called after a directory's files have been 
    completely processed. This does not include any subdirectories.
    A user could use this to update some statistics as an example.
    
    Return False for normal processing
    There is currently no effect if True is returned, but this may
    change in the future.
    The default implementation immediately returns False.
    
    :param dir: Path to the directory where post-processing should
    be performed.
    """
    return False

  def handle_file(self, dir, file):
    """
    Returns True if file is properly handled
    Returns False if file was skipped
    
    It is the methods responsibility to increment any stats as required
    when returning False.
    Otherwise this method should be subclassed and perform whatever work is
    necessary for each file.
    """
    return True
    
  def handle_extended_ops(self, raw_data):
    """
    Called by the main loop when an unknown command is found
    This can be used to support custom commands from a HydraClient without
    having to re-write the operation parser. This can be overridden as necessary
    when subclassing :class:`HydraWorker <multiprocessing.Process>`.
    
    Return True if the command was handled
    Return False if the command was not handled. This is the default.
    """
    return False
    
  def handle_stats_collection(self):
    """
    Called right before stats are sent back to the client.
    Add any stats filtering or custom processing here.
    """
    pass
    
  def handle_update_settings(self, cmd):
    """
    Handle a settings update from the client
    """
    return True
    
  def close(self):
    """
    Fill in docstring
    """
    try:
      self.send({'op': STATE_SHUTDOWN})
    except:
      pass
    
  def fileno(self):
    """
    We return the fileno of the loopback socket so the object itself can be
    used in a select statement.
    """
    if self.client_conn:
      return self.client_conn.fileno()
    return None
    
  def init_process(self):
    """
    Called by the main loop at the beginning after logging is configured.
    Place any init routines that are required to be run in the context of the
    worker process versus the context of the main program here.
    """
    pass
    
  def init_stats(self):
    """
    Fill in docstring
    """
    for s in HydraUtils.BASIC_STATS:
      self.stats[s] = 0
  
  def recv(self, timeout=-1, convert=True):
    """
    A HydraClient can use this method to read data from the worker
    
    When timeout < 0, block waiting for data
    When timeout >= 0, poll the pipe for this many seconds. A poll 
    returns with False if there is no data to read during the timeout
    
    An EOFError is propagated up from the underlying recv call if the
    pipe is closed and there is no data in the pipe
    """
    if timeout >= 0:
      readable, _, _ = select([self.client_conn], [], [], timeout)
      if not readable:
        return False
    msg_size = self.client_conn.recv(4)
    if len(msg_size) != 4:
      return b''
    try:
      data_len = unpack('!L', msg_size)[0]
      data = HydraUtils.socket_recv(self.client_conn, data_len)
    except Exception as e:
      self.log.exception(e)
      return b''
    
    if convert:
      if 'format' in data:
        if data['format'] == 'zpickle':
          data['data'] = pickle.loads(zlib.decompress(data['data']))
          data['format'] = 'dict'
        elif data['format'] == 'pickle':
          data['data'] = pickle.loads(data['data'])
          data['format'] = 'dict'
    return data
    
  def send(self, data):
    """
    A HydraClient can use this method to send commands to the worker
    """
    try:
      HydraUtils.socket_send(self.client_conn, data)
    except Exception as e:
      self.log.exception(e)
      return False
    return True
    
  def run(self):
    """
    Fill in docstring
    """
    self._init_process_logging()
    self.init_process()
    self.log.debug("PID: %d, Process name: %s"%(self.pid, self.name))
    wait_count = 0
    forced_shutdown = False
    while self._get_state() != STATE_SHUTDOWN:
      readable = []
      exceptional = []
      try:
        self.log.log(5, "Waiting on select")
        readable, _, exceptional = select(self.inputs, [], self.inputs, wait_count)
        self.log.log(5, "Select returned")
      except KeyboardInterrupt:
        self.log.debug("Caught keyboard interrupt waiting for event")
        break

      # Handle inputs
      for s in readable:
        if s is self.worker_conn:
          msg_size = s.recv(4)
          if len(msg_size) != 4:
            continue
          data_len = unpack('!L', msg_size)[0]
          data = HydraUtils.socket_recv(s, data_len)
          if len(data) > 0:
            self.log.log(9, "Worker got data: %r"%data)
            try:
              self._process_state_event(data.get('op', None), data)
            except Exception as e:
              self.log.exception(e)
          else:
            self.log.error("Input ready but no data received")
      # Handle exceptions
      for s in exceptional:
        self.log.critical('Handling exceptional condition for %r'%s)
      
      # This section decides when to do the actual processing of the file
      # structure.
      # While in the 'idle' or 'pause' state, each iteration increments a
      # counter which slows down the polling interval used by the select
      # statement above. This is done to prevent the poll from consuming too
      # many resources when it is idle for a long period of time.
      if self._get_state() == STATE_SHUTDOWN:
        break
      elif self._get_state() == STATE_PAUSED:
        if wait_count < HydraUtils.LONG_WAIT_THRESHOLD:
          wait_count += HydraUtils.SHORT_WAIT_TIMEOUT
      else:
        # After handling any messages, try to process our work queue
        try:
          handled = self._process_work_queue()
          if handled:
            wait_count = 0
          else:
            if len(self.work_queue) <= 0:
              self._process_state_event(CMD_WORK_QUEUE_EMPTY)
            wait_count += HydraUtils.SHORT_WAIT_TIMEOUT*(wait_count < HydraUtils.LONG_WAIT_THRESHOLD)
        except KeyboardInterrupt:
          self._set_state(STATE_SHUTDOWN)
          break
        except Exception as e:
          self.log.critical("Exception when processing work queue: %s"%traceback.format_exc())
    # Perform cleanup operations before the process terminates
    self._cleanup(forced=forced_shutdown)
    self.log.debug("Ending PID: %d, Process name: %s"%(self.pid, self.name))
    
  #
  # Internal methods
  #
  def _cleanup(self, forced=False):
    """
    Fill in docstring
    """
    try:
      if not forced:
        self.log.log(9, "Returning work items")
        self._return_work_items(divisor=1)
        self.log.log(9, "Returning stats")
        self._return_stats()
        self.log.log(9, "Sending shutdown complete")
        self._send_client(CMD_STATE, STATE_SHUTDOWN)
      if self.client_conn:
        try:
          self.client_conn.close()
        except:
          pass
        self.client_conn = None
      if self.worker_conn:
        try:
          self.worker_conn.close()
        except:
          pass
        self.worker_conn = None
    except Exception as e:
      self.log.exception(e)
    if self.log.handlers and isinstance(self.log.handlers[0], HydraUtils.SecureSocketHandler):
      self.log.handlers[0].close()
  
  def _connect_client(self):
    self.log.debug('Connecting to loopback client')
    # Create temporary socket to listen for client<->worker connection
    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_sock.bind((self.loopback_addr, self.loopback_port))
    self.loopback_port = listen_sock.getsockname()[1]
    listen_sock.listen(1)
    # Create connection between client<->worker and exchange secret
    self.worker_conn = socket.create_connection((self.loopback_addr, self.loopback_port))
    if not self.worker_conn:
      self.log.critical('Unable to create loopback socket connection')
    self.worker_conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    self.client_conn, _ = listen_sock.accept()
    self.log.debug('Connection complete')
    # Close the listening socket as we no longer require it
    listen_sock.close()
    secret = HydraUtils.create_uuid_secret()
    self.worker_conn.sendall(secret)
    security = self.client_conn.recv(len(secret))
    if security != secret:
      self.log.warn("Invalid secret exchange for socket connection!")
    self.inputs.append(self.worker_conn)
      
  def _get_state(self):
    """
    Fill in docstring
    """
    return self.state
    
  def _get_stats(self, format='dict'):
    """
    Fill in docstring
    """
    self.handle_stats_collection()
    if format == 'pickle':
      return(pickle.dumps(self.stats, protocol=pickle.HIGHEST_PROTOCOL))
    elif format == 'zpickle':
      return(zlib.compress(pickle.dumps(self.stats, protocol=pickle.HIGHEST_PROTOCOL)))
    return self.stats
    
  def _init_process_logging(self):
    # Reset all the handlers for each logger to leave only the root handler
    # as the workers should communicate to the client over the socket handler
    logger_nodes = list(logging.root.manager.loggerDict.items())
    for name, logger in logger_nodes:
      logger.handlers = []
    # Setup the socket handler and fall-back to the console
    root = logging.getLogger()
    self.log = logging.getLogger(__name__)
    if self.args.get('logger_cfg') and self.args.get('port'):
      root.setLevel(self.args['logger_cfg'].get('loggers', {}).get('', {}).get('level', logging.WARN))
      root.handlers = [
        HydraUtils.SecureSocketHandler(
          host=self.args.get('host', HydraUtils.LOOPBACK_ADDR),
          port=self.args.get('port'),
          secret=self.args.get('secret'),
        )
      ]
    else:
      root.handlers = [logging.StreamHandler()]
    
        
  def _queue_dirs(self, data):
    """
    Fill in docstring
    """
    dirs = data.get('dirs', None)
    if not dirs:
      self.log.error("Malformed message for operation 'proc_dir': %r"%data)
      return False
    for dir in dirs:
      if dir in [None]:
        self.log.warn('Invalid directory process request: %s'%dir)
        continue
      if isinstance(dir, dict):
        if all(k in dir for k in ('type', 'path')):
          self.work_queue.append(dir)
        else:
          self.log.critical("Queue dirs could not queue object: %s"%dir)
          continue
      else:
        self.work_queue.append({'type': 'dir', 'path': dir})
      self.stats['queued_dirs'] += 1
      
  def _queue_work(self, data):
    """
    Fill in docstring
    """
    for item in data.get('work_items', []):
      work_type = item.get('type', None)
      if work_type in ['dir', 'partial_dir']:
        self.stats['queued_dirs'] += 1
      elif work_type == 'file':
        # TODO: Files should be a list of files not a single one
        self.stats['queued_files'] += 1
      self.work_queue.append(item)
      
  def _return_state(self):
    """
    Fill in docstring
    """
    self._send_client(CMD_STATE, self._get_state())
    
  def _return_stats(self, data=None):
    """
    Fill in docstring
    """
    format = 'zpickle'
    if data is not None:
      format = data.get('format', 'pickle')
    if format not in HydraUtils.HYDRA_OPERATION_FORMATS:
      format = 'dict'
    self._send_client(CMD_STATS, self._get_stats(format), format=format)
    
  def _return_work_items(self, divisor=2):
    """
    Fill in docstring
    """
    # Use simple algorithm by returning roughly half of our work items
    return_queue = len(self.work_queue)//divisor
    self.log.debug('Returning %d work items to client'%return_queue)
    if return_queue > 0:
      return_items = []
      for i in range(return_queue):
        # We pull off work items from the right side of the queue.
        # These items should be the oldest and represent paths closer to the
        # root/originally queued. Directories found during a walk are pushed
        # to the left side of the queue.
        work_item = self.work_queue.pop()
        work_type = work_item.get('type', None)
        if work_type in ['dir', 'partial_dir']:
          self.stats['queued_dirs'] -= 1
        elif work_type in ['partial_dir']:
          # If we hit a partial directory entry, we do not return it to the
          # client because partial work directories are currently not supported
          # to be passed between workers
          self.work_queue.append(work_item)
          break
        elif work_type in ['file']:
          self.stats['queued_files'] -= 1
        return_items.insert(0, work_item)
      self._send_client(CMD_WORK_ITEMS, return_items)
    
  def _send_client(self, op, data=None, format=None):
    """
    Fill in docstring
    """
    self.log.log(5, "Sending to client operation: %s"%op)
    if format is None:
      msg = {'op': op, 'id': self.name, 'pid': self.pid, 'data': data}
    else:
      msg = {'op': op, 'id': self.name, 'pid': self.pid, 'data': data, 'format': format}
    HydraUtils.socket_send(self.worker_conn, msg)
      
  def _process_work_queue(self):
    """
    Fill in docstring
    """
    self.log.log(5, "_process_work_queue invoked")
    start_time = time_now()
    temp_work = []
    try:
      work_item = self.work_queue.popleft()
    except:
      # No work items
      end_time = time_now()
      return False
    work_type = work_item.get('type', None)
    self.log.log(9, "Work type: %s"%work_type)
    if work_type == 'dir':
      work_dir = work_item.get('path')
      self.log.log(9, "Processing directory: %s"%work_dir)
      handled = self.handle_directory_pre(work_dir)
      if handled:
        self.stats['filtered_dirs'] += 1
        # Subtract 1 from the processed_dirs stat to account for an increment later
        self.stats['processed_dirs'] -= 1
      else:
        temp_work = []
        try:
          for root, dirs, files in self.fswalk(work_dir):
            # Filter subdirectories and files by calling the method in the derived class
            before_filter_dirs = len(dirs)
            before_filter_files = len(files)
            dirs[:], files[:] = self.filter_subdirectories(root, dirs, files)
            after_filter_dirs = len(dirs)
            after_filter_files = len(files)
            if before_filter_dirs != after_filter_dirs:
              self.stats['filtered_dirs'] += after_filter_dirs - before_filter_dirs
            if before_filter_files != after_filter_files:
              self.stats['filtered_files'] += after_filter_files - before_filter_files
              
            # We queue up any new directories to the left in our work queue.
            # This leaves the directories closer to the initial ones on the right
            # side of the work queue.
            for dir in reversed(dirs):
              self.work_queue.appendleft({'type': 'dir', 'path': os.path.join(work_dir, dir)})
            self.stats['queued_dirs'] += len(dirs)
            for file in files:
              # Keep track of how long we have been processing this
              # directory.  If the time exceeds LONG_PROCESSING_THRESHOLD
              # we will queue up the files and push them onto the 
              # processing queue and let the main processing loop have a
              # chance to pick up new commands
              proc_time = time_now()
              if (proc_time - start_time) > HydraUtils.LONG_PROCESSING_THRESHOLD:
                temp_work.append(file)
                continue
              try:
                if(self.handle_file(root, file)):
                  self.stats['processed_files'] += 1
                else:
                  self.log.debug('Skipped file: %s'%os.path.join(root, file))
                  self.stats['skipped_files'] += 1
              except Exception as e:
                self.log.debug('Skipped file: %s'%os.path.join(root, file))
                self.stats['skipped_files'] += 1
                self.log.critical('Exception encountered while handling file: %s'%traceback.format_exc())
            # We actually want to abort the tree walk as we want to handle the directory structure 1 directory at a time
            dirs[:] = []
        except Exception as e:
          self.log.exception(e)
          raise
      if temp_work:
        self.work_queue.appendleft({'type': 'partial_dir', 'path': work_dir, 'files': temp_work})
      else:
        self.stats['queued_dirs'] -= 1
        self.stats['processed_dirs'] += 1
        handled = self.handle_directory_post(work_dir)
    elif work_type == 'partial_dir':
      # For a partial directory, we need to continue processing all the files
      # remaining in the directory. 
      work_dir = work_item.get('path')
      self.log.log(logging.DEBUG, "Processing directory (continued): %s"%work_dir)
      for file in work_item.get('files'):
        # Keep track of how long we have been processing this directory.
        # If the time exceeds LONG_PROCESSING_THRESHOLD, we will queue up
        # the files and push them onto the processing queue and let the
        # main processing loop have a chance to pick up new commands
        proc_time = time_now()
        if (proc_time - start_time) > HydraUtils.LONG_PROCESSING_THRESHOLD:
          temp_work.append(file)
          continue
        if(self.handle_file(work_dir, file)):
          self.stats['processed_files'] += 1
        else:
          self.log.debug('Skipped file: %s'%os.path.join(work_dir, file))
          self.stats['skipped_files'] += 1
      # If temp_work is empty, we finished the remainder of the directory
      # so we will do the post directory processing
      # If not then we will re-queue the work and continue processing after
      # checking the command queue
      if temp_work:
        self.work_queue.appendleft({'type': 'partial_dir', 'path': work_dir, 'files': temp_work})
      else:
        self.stats['queued_dirs'] -= 1
        self.stats['processed_dirs'] += 1
        handled = self.handle_directory_post(work_dir)
    #elif work_type == 'file':
    #  # TODO: NOT YET IMPLEMENTED
    #  self.handle_file(work_dir, file)
    #  self.stats['processed_files'] += 1
    else:
      self.log.error("Unknown work type found in work queue. Queued work item: %r"%work_item)
    end_time = time_now()
    return True

  # State machine methods
  def _init_state_table(self, state_dict):
    """
    Fill in docstring
    """
    for state in state_dict.keys():
      self._sm_copy_state(self.state_table, state, state_dict[state])
    
  def _sm_copy_state(self, state_table, state, ev_handlers):
    for event in ev_handlers.keys():
      self._sm_add_event_handler(state_table, state, event, ev_handlers[event])
    
  def _sm_add_event_handler(self, state_table, state, event, handler):
    if not state in state_table:
      state_table[state] = {}
    sm_state = state_table[state]
    sm_state[event] = dict(handler)
    if isinstance(sm_state[event].get('a'), str):
      # Convert any string state handlers to actual bound methods
      sm_state[event]['a'] = getattr(self, sm_state[event].get('a'))
    
  def _process_state_event(self, event, data=None):
    """
    Fill in docstring
    """
    table = self.state_table[self.state]
    handler = table.get(event)
    if handler:
      self.log.debug("Worker handling event '%s' @ '%s' with '%s'"%(event, self.state, handler['a']))
      next_state = handler['a'](event, data, handler.get('ns'))
      self._set_state(next_state or handler.get('ns'))
    else:
      if not self.handle_extended_ops(data):
        self.log.critical("Unhandled event (%s) received in '%s' state."%(event, self.state))
    
  def _set_state(self, state):
    """
    Fill in docstring
    """
    if state == None:
      self.log.debug('No state transition: %s'%(self.state))
      return
    old_state = self.state
    if old_state != state:
      if state == STATE_IDLE or state == STATE_SHUTDOWN:
        self._return_stats()
      self.state = state
      self.log.debug('Worker state change: %s => %s'%(old_state, state))
      self._return_state()
    return old_state
    
  def _h_no_op(self, event, data, next_state):
    return next_state

  def _h_connect(self, event, data, next_state):
    """
    Fill in docstring
    """
    self._connect_client()
    return next_state
    
  def _h_idle_pause(self, event, data, next_state):
    return next_state
    
  def _h_proc_dir(self, event, data, next_state):
    self._queue_dirs(data)
    return next_state

  def _h_proc_work(self, event, data, next_state):
    self._queue_work(data)
    return next_state

  def _h_processing_pause(self, event, data, next_state):
    return next_state
    
  def _h_query_state(self, event, data, next_state):
    self._return_state()
    return next_state

  def _h_query_stats(self, event, data, next_state):
    self._return_stats(data.get('data', None))
    return next_state
    
  def _h_resume(self, event, data, next_state):
    return next_state

  def _h_return_work(self, event, data, next_state):
    self._return_work_items()
    return next_state
    
  def _h_shutdown(self, event, data, next_state):
    return next_state

  def _h_update_settings(self, event, data, next_state):
    self.handle_update_settings(data)
    return next_state
