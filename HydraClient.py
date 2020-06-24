# -*- coding: utf8 -*-
"""
Module description here
"""
__title__ = "HydraClient"
__version__ = "1.0.0"
__all__ = ["HydraClient", "HydraClientProcess"]
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
import multiprocessing
import time
import logging
import socket
import errno
import select
import struct
try:
   import cPickle as pickle
except:
   import pickle
import zlib
from collections import deque
import HydraWorker
import HydraUtils


# Possible state machine states. Sub states are split using the _ character
STATE_IDLE = 'idle'
STATE_IDLE_WAITING = 'idle_waiting'
STATE_INIT = 'init'
STATE_CONNECTED = 'connected'
STATE_PROCESSING = 'processing'
STATE_PROCESSING_PAUSED = 'processing_paused'
STATE_PROCESSING_WAITING = 'processing_waiting'
STATE_SHUTDOWN = 'shutdown'

EVENT_CONNECTED = 'client_connected'
EVENT_HEARTBEAT = 'client_heartbeat'
# Sent by the server to client to signal no new work units and to transition to IDLE
EVENT_NO_WORK = 'client_no_work'
EVENT_QUERY_STATS = 'return_stats'
EVENT_REQUEST_WORK = 'request_work'
EVENT_RETURN_WORK = 'return_work'
EVENT_SHUTDOWN = 'shutdown'
EVENT_SUBMIT_WORK = 'submit_work'
EVENT_UPDATE_SETTINGS = 'update_settings'
EVENT_WORKER_STATE = HydraWorker.CMD_STATE
EVENT_WORKER_STATS = HydraWorker.CMD_STATS
EVENT_WORKER_WORK_ITEMS = HydraWorker.CMD_WORK_ITEMS
EVENT_WORKERS_IDLE = 'client_workers_ready'

CMD_CLIENT_REQUEST_WORK = 'client_request_work'
CMD_CLIENT_STATE = 'client_state'
CMD_CLIENT_STATS = 'client_stats'
CMD_CLIENT_WORK = 'client_work_items'

HYDRA_CLIENT_STATE_TABLE = {
  STATE_INIT: {
      EVENT_CONNECTED:        {'a': '_h_connected',           'ns': STATE_CONNECTED},
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_NO_WORK:          {'a': '_h_no_op',               'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
      #EVENT_WORKER_WORK_ITEMS:{'a': '_h_no_op',               'ns': None},
      #EVENT_WORKERS_IDLE:     {'a': '_h_w_idle_conn',         'ns': STATE_IDLE},
  },
  STATE_CONNECTED: {
      #EVENT_CONNECTED:        {'a': '_h_no_op',               'ns': None},
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_NO_WORK:          {'a': '_h_no_op',               'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      #EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      EVENT_SUBMIT_WORK:      {'a': '_h_requeue',             'ns': None},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
      EVENT_WORKER_STATE:     {'a': '_h_w_state_conn',        'ns': None},
      EVENT_WORKER_STATS:     {'a': '_h_w_stats',             'ns': None},
      #EVENT_WORKER_WORK_ITEMS:{'a': '_h_no_op',               'ns': None},
      EVENT_WORKERS_IDLE:     {'a': '_h_w_idle_conn',         'ns': STATE_IDLE},
  },
  STATE_IDLE: {
      #EVENT_CONNECTED:        {'a': '_h_no_op',               'ns': None},
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_NO_WORK:          {'a': '_h_no_op',               'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_REQUEST_WORK:     {'a': '_h_request_work',        'ns': STATE_IDLE_WAITING},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      EVENT_SUBMIT_WORK:      {'a': '_h_submit_work',         'ns': STATE_PROCESSING},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
      EVENT_WORKER_STATE:     {'a': '_h_w_state',             'ns': None},
      EVENT_WORKER_STATS:     {'a': '_h_w_stats',             'ns': None},
      #EVENT_WORKERS_IDLE:     {'a': '_h_w_idle_conn',         'ns': STATE_IDLE},
  },
  STATE_IDLE_WAITING: {
      #EVENT_CONNECTED:        {'a': '_h_no_op',               'ns': None},
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_NO_WORK:          {'a': '_h_no_op',               'ns': STATE_IDLE},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_REQUEST_WORK:     {'a': '_h_no_op',               'ns': None},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      EVENT_SUBMIT_WORK:      {'a': '_h_submit_work',         'ns': STATE_PROCESSING},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
      EVENT_WORKER_STATE:     {'a': '_h_w_state',             'ns': None},
      EVENT_WORKER_STATS:     {'a': '_h_w_stats',             'ns': None},
      #EVENT_WORKERS_IDLE:     {'a': '_h_w_idle_conn',         'ns': STATE_IDLE},
  },
  STATE_PROCESSING: {
      #EVENT_CONNECTED:        {'a': '_h_no_op',               'ns': None},
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_REQUEST_WORK:     {'a': '_h_request_work_s',      'ns': STATE_PROCESSING_WAITING},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      EVENT_SUBMIT_WORK:      {'a': '_h_submit_work',         'ns': None},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
      EVENT_WORKER_STATE:     {'a': '_h_w_state',             'ns': None},
      EVENT_WORKER_STATS:     {'a': '_h_w_stats',             'ns': None},
      EVENT_WORKER_WORK_ITEMS:{'a': '_h_w_work_items',        'ns': None},
      #EVENT_WORKERS_IDLE:     {'a': '_h_w_idle_conn',         'ns': STATE_IDLE},
  },
  STATE_PROCESSING_PAUSED: {
      #EVENT_CONNECTED:        {'a': '_h_no_op',               'ns': None},
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_REQUEST_WORK:     {'a': '_h_no_op',               'ns': None},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      #EVENT_SUBMIT_WORK:      {'a': '_h_submit_work',         'ns': STATE_PROCESSING},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
      EVENT_WORKER_STATE:     {'a': '_h_w_state',             'ns': None},
      EVENT_WORKER_STATS:     {'a': '_h_w_stats',             'ns': None},
      EVENT_WORKER_WORK_ITEMS:{'a': '_h_w_work_items',        'ns': None},
      #EVENT_WORKERS_IDLE:     {'a': '_h_w_idle_conn',         'ns': STATE_IDLE},
  },
  STATE_PROCESSING_WAITING: {
      #EVENT_CONNECTED:        {'a': '_h_no_op',               'ns': None},
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_NO_WORK:          {'a': '_h_no_op',               'ns': STATE_IDLE},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_REQUEST_WORK:     {'a': '_h_request_work',        'ns': None},
      EVENT_RETURN_WORK:      {'a': '_h_return_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN},
      EVENT_SUBMIT_WORK:      {'a': '_h_submit_work',         'ns': STATE_PROCESSING},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
      EVENT_WORKER_STATE:     {'a': '_h_w_state',             'ns': None},
      EVENT_WORKER_STATS:     {'a': '_h_w_stats',             'ns': None},
      EVENT_WORKER_WORK_ITEMS:{'a': '_h_w_work_items',        'ns': None},
      #EVENT_WORKERS_IDLE:     {'a': '_h_w_idle_conn',         'ns': STATE_IDLE},
  },
  STATE_SHUTDOWN: {
      #EVENT_CONNECTED:        {'a': '_h_no_op',               'ns': None},
      EVENT_HEARTBEAT:        {'a': '_h_no_op',               'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_no_op',               'ns': None},
      EVENT_REQUEST_WORK:     {'a': '_h_no_op',               'ns': None},
      EVENT_RETURN_WORK:      {'a': '_h_no_op',               'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_no_op',               'ns': None},
      #EVENT_SUBMIT_WORK:      {'a': '_h_no_op',               'ns': STATE_PROCESSING},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_no_op',               'ns': None},
      EVENT_WORKER_STATE:     {'a': '_h_w_state',             'ns': None},
      EVENT_WORKER_STATS:     {'a': '_h_w_stats',             'ns': None},
      #EVENT_WORKER_WORK_ITEMS:{'a': '_h_no_op',               'ns': None},
      #EVENT_WORKERS_IDLE:     {'a': '_h_no_op',               'ns': STATE_IDLE},
  },
}

class HydraClient(object):
  def __init__(self, worker_class, args={}):
    """
    Fill in docstring
    """
    self.args = dict(args)
    self.log = logging.getLogger(__name__)
    self.log_svr = None
    self.log_addr = HydraUtils.LOOPBACK_ADDR
    self.log_port = HydraUtils.LOOPBACK_PORT
    self.log_secret = None
    self.heartbeat_interval = args.get('heartbeat_interval', HydraUtils.HEARTBEAT_INTERVAL)                   # Seconds for each heartbeat
    self.idle_shutdown_interval = args.get('idle_shutdown_interval', HydraUtils.IDLE_SHUTDOWN_THRESHOLD)      # Number of idle heartbeats before forcing a pending shutdown
    self.dirs_per_idle_worker = args.get('dirs_per_idle_worker', HydraUtils.DIRS_PER_IDLE_WORKER)
    self.select_poll_interval = args.get('select_poll_interval', HydraUtils.SELECT_POLL_INTERVAL)
    self.stats_heartbeat_interval = args.get('stats_heatbeat_interval', HydraUtils.STATS_HEARTBEAT_INTERVAL)  # Number of heartbeat intervals per worker stat update
    self.heartbeat_count = 1
    self.num_workers = 0
    self.worker_base_class = worker_class
    self.workers = {}                                                           # Active worker list
    self.shutdown_pending = {}                                                  # Workers that are pending shutdown
    self.shutdown_workers = {}                                                  # Workers that are fully cleaned up, saving only the stats
    self.stats = {}
    self.state = STATE_INIT
    self.state_table = {}
    self.event_queue = []
    self.server_waiting_for_work = False
    
    self.work_queue = deque()
    self._init_logger()
    self.init_stats(self.stats)
    self._init_state_table(HYDRA_CLIENT_STATE_TABLE)

    # Variables dealing with server and worker communication
    self.server = None
    # Sockets from which we expect to read from through a select call
    self.inputs = []
    
  def init_process(self):
    """
    Called by the main loop at the beginning after logging is configured.
    Place any init routines that are required to be run in the context of the
    client process versus the context of the main program here.
    """
    pass
    
  def init_stats(self, stat_state):
    """
    This method is called to initialize the statistics of this client.
    This method can overridden in order to handle custom statistics but either
    calling the base class to handle the generic stats should be done or the
    derived class must do the same work.
    """
    for s in HydraUtils.BASIC_STATS:
      stat_state[s] = 0
  
  def consolidate_stats(self):
    """
    This method is called to consolidate all the worker stats into a single
    coherent stats object.
    This method can overridden in order to handle custom statistics
    """
    try:
      self.init_stats(self.stats)
      for set in [self.workers, self.shutdown_pending, self.shutdown_workers]:
        for w in set:
          if not set[w]['stats']:
            continue
          for s in HydraUtils.BASIC_STATS:
            self.stats[s] += set[w]['stats'][s]
    except Exception as e:
      self.log.exception(e)

  def handle_extended_worker_msg(self, wrk_msg):
    """
    Called by the main loop when an unknown command is found
    This can be used to support custom commands from a HydraWorker without
    having to re-write the operation parser. This can be overridden as necessary
    when subclassing :class:`HydraWorker <multiprocessing.Process>`.
    
    Return True if the command was handled
    Return False if the command was not handled. This is the default.
    """
    return False
    
  def handle_extended_server_cmd(self, svr_msg):
    """
    Called by the main loop when an unknown command is found
    This can be used to support custom commands from a HydraClient without
    having to re-write the operation parser. This can be overridden as necessary
    when subclassing :class:`HydraWorker <multiprocessing.Process>`.
    
    Return True if the command was handled
    Return False if the command was not handled. This is the default.
    """
    return False
    
  def handle_update_settings(self, cmd):
    """
    Handle a settings update from the server
    """
    self.send_all_workers({
        'op': HydraWorker.EVENT_UPDATE_SETTINGS,
        'settings': cmd['settings'],
    })
    return True
    
  def handle_workers_connected(self):
    """
    Called once when the all the workers connect to the client
    """
    pass
    
  def connect(self, server_addr, server_port=HydraUtils.DEFAULT_LISTEN_PORT):
    """
    Fill in docstring
    """
    self.server = socket.create_connection((server_addr, server_port))
    if not self.server:
      raise Except('Unable to connect to server %s:%s. Check address, port and firewalls.'%(server_addr, server_port))
    self.server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    self.inputs.append(self.server)
    self.event_queue.append({'c': EVENT_CONNECTED, 'd': None})
    
  def get_max_workers(self):
    """
    Fill in docstring
    """
    return(len(self.workers) + len(self.shutdown_pending) + len(self.shutdown_workers))
  
  def get_num_workers(self):
    """
    Fill in docstring
    """
    return self.num_workers
    
  def set_workers(self, num='auto'):
    """
    Fill in docstring
    """
    if num == 'auto':
      num = multiprocessing.cpu_count()
    new_workers = num - self.num_workers
    if new_workers > 0:
      self.add_worker(new_workers)
    elif new_workers < 0:
      self.remove_worker(new_workers*-1)
    
  def add_worker(self, num=1):
    """
    Fill in docstring
    """
    for w in range(num):
      worker_args = dict(self.args)
      worker_args['logger_config'] = worker_args['logger_worker_config']
      worker = self.worker_base_class(worker_args)
      self.inputs.append(worker)
      self._init_worker_state(worker)
      self.num_workers += 1
      worker.start()
    
  def remove_worker(self, num=1):
    """
    When removing a worker we must remove it out of the normal worker
    list and put it on a special shutdown list. This needs to be done
    so additional remove_worker calls will not accidentally pull a key
    for a worker that is already being shutdown.
    """
    if num > self.num_workers:
      num = self.num_workers
    retire_keys = []
    non_idle_keys = []
    for w in self.workers:
      if self.workers[w]['state'] in [HydraWorker.STATE_INIT, HydraWorker.STATE_IDLE]:
        retire_keys.append(w)
      else:
        non_idle_keys.append(w)
    # Make sure our retire_keys array has enough keys to shutdown the number
    # of workers requested. If the number of idle workers is less than the
    # number of workers we want to shut down, we will have to force some active
    # workers to shutdown as well
    for i in range(num - len(retire_keys)):
      retire_keys.append(non_idle_keys.pop())
    for k in retire_keys:
      # Pull some workers out of the working set and shut them down
      # We subtract the num_workers variable even before the worker
      # completes shutting down in case we are asked to remove more
      # workers before the existing ones have finished shutting down.
      self.log.debug("Shutting down worker: %s"%k)
      worker_state = self.workers.pop(k)
      worker_state['obj'].send({'op': HydraWorker.EVENT_SHUTDOWN})
      self.shutdown_pending[k] = worker_state
      self.num_workers -= 1
      
  def send_active_workers(self, cmd):
    for set in [self.workers]:
      for w in set:
        set[w]['obj'].send(cmd)

  def send_all_workers(self, cmd):
    for set in [self.workers, self.shutdown_pending]:
      for w in set:
        set[w]['obj'].send(cmd)

  def send_worker(self, worker_id, cmd):
    worker_found = False
    for set in [self.workers, self.shutdown_pending, self.shutdown_workers]:
      if worker_id in set:
        set[worker_id]['obj'].send(cmd)
        worker_found = True
        break
    if not worker_found:
      self.log.error('Send worker attempted with invalid worker_id: %s'%worker_id)
  
  def serve_forever(self):
    """
    Fill in docstring
    """
    if self.num_workers == 0:
      self.set_workers()
    # Client will enter idle state when all workers have connected
    
    idle_count = 0
    start_time = time.time()
    self.init_process()
    while not (self.state == STATE_SHUTDOWN):
      readable = []
      exceptional = []
      try:
        self.log.log(5, "Waiting on select")
        readable, _, exceptional = select.select(self.inputs, [], self.inputs, self.select_poll_interval)
        self.log.log(5, "Select returned")
        
        if len(readable) == 0 and len(exceptional) == 0:
          idle_count = idle_count + 1
        else:
          idle_count = 0
        if (time.time() - start_time) >= (self.heartbeat_count*self.heartbeat_interval):
          self.heartbeat_count += 1
          self.event_queue.append({'c': EVENT_HEARTBEAT, 'd': {'idle_count': idle_count, 'hb_count': self.heartbeat_count}})
      except KeyboardInterrupt:
        self.log.debug("Caught keyboard interrupt waiting for event")
        self._shutdown()
        break
      except IOError as ioe:
        for i in self.inputs:
          try:
            # Find the invalid handle causing the issue by probing the object
            x = i.fileno()
          except:
            self.log.warn("Found bad handle in select: %r"%i)
            try:
              self.inputs.remove(i)
            except:
              self.log.error("Unable to remove handle from select: %s"%i)
              self._shutdown()

      # Handle inputs
      for s in readable:
        try:
          if s is self.server:
            try:
              msg_size = s.recv(4)
              if len(msg_size) != 4:
                self.log.debug("Closing server handle due to EOF of command stream: %r"%s)
                self._shutdown()
                continue
              data_len = struct.unpack('!L', msg_size)[0]
              data = HydraUtils.socket_recv(s, data_len)
              self.event_queue.append({'c': data.get('op'), 'd': data, 'src': 'server'})
            except EOFError as eof:
              self.log.debug("Closing server handle due to EOF of command stream: %r"%s)
              self._shutdown()
            except socket.error as serr:
              if serr.errno != errno.ECONNRESET:
                raise
              self.log.debug("Closing server handle due to EOF of command stream: %r"%s)
              self._shutdown()
            except Exception:
              self.log.exception("Exception encountered during command processing.")
              self._shutdown()
          else:
            cmd = s.recv()
            if len(cmd) > 0:
              self.log.log(9, "Got cmd from worker: %s"%cmd)
              self.event_queue.append({'c': cmd.get('op'), 'd': cmd, 'src': 'worker'})
            else:
              s.close()
              self.inputs.remove(s)
              self.log.info("Empty read on socket. Closing connection: %s"%s)
        except Exception as e:
          self.log.exception("Unhandled exception processing received cmd")
          raise(e)
            
      # Handle exceptions
      for s in exceptional:
        self.log.error('Handling exceptional condition for %r'%s)
        if s is self.server:
          self._shutdown()
          break
          
      # Process all items in the event_queue. Reset the current event queue
      # and set it to a new empty array in case handlers requeue events
      cur_queue = self.event_queue
      self.event_queue = []
      for entry in cur_queue:
        try:
          self._process_state_event(entry['c'], entry.get('d'), entry.get('src'))
        except Exception as e:
          self.log.exception(e)
          break
    self.log.debug("Client exiting")
    self._cleanup_all_workers()
  
  #
  # Internal methods
  #
  def _cleanup_all_workers(self):
    keys = list(self.workers.keys())
    for key in keys:
      self._cleanup_worker(key)
  
  def _cleanup_worker(self, worker_id):
    """
    Fill in docstring
    """
    self.log.log(9, "Cleanup worker called: %s"%worker_id)
    if worker_id in self.workers.keys():
      self.log.log(9, "Cleaning worker dict: %s"%worker_id)
      w = self.workers.pop(worker_id)
      self.inputs.remove(w['obj'])
      w['obj'].close()
      self.shutdown_workers[worker_id] = {'stats': w['stats']}
    elif worker_id in self.shutdown_pending.keys():
      self.log.log(9, "Cleaning shutdown pending worker dict: %s"%worker_id)
      w = self.shutdown_pending.pop(worker_id)
      self.inputs.remove(w['obj'])
      w['obj'].close()
      self.shutdown_workers[worker_id] = {'stats': w['stats']}
    self.log.log(9, "Remaining worker dict: %s"%self.workers)
    self.log.log(9, "Remaining shutdown pending worker dict: %s"%self.shutdown_pending)
    self.log.log(9, "Active inputs: %s"%self.inputs)
    
  def _get_idle_working_worker_keys(self):
    idle = []
    processing = []
    for k in self.workers.keys():
      worker_state = self.workers.get(k, {})['state']
      if worker_state == HydraWorker.STATE_IDLE:
        idle.append(k)
      elif worker_state in [
          HydraWorker.STATE_PAUSED,
          HydraWorker.STATE_PROCESSING,
          HydraWorker.STATE_PROCESSING_PAUSED,
          'idle_queued_work',       # Internally created state for worker
        ]:
        processing.append(k)
      else:
        self.log.log(9, "Found worker in state: %s"%worker_state)
    return idle, processing

      
  def _get_worker_stats(self):
    """
    Fill in docstring
    """
    self.send_active_workers({'op': HydraWorker.EVENT_QUERY_STATS})
      
  def _get_worker_work_items(self, forced=False):
    """
    Fill in docstring
    """
    # Check if we have idle workers. If we do query any active workers and have
    # them return work items
    try:
      num_idle_workers = 0
      self.log.debug("Checking for idle workers")
      idle, processing = self._get_idle_working_worker_keys()
      num_idle_workers = len(idle)
      if len(self.work_queue) < num_idle_workers or forced:
        for w in processing:
          self.log.debug("Requesting active worker to return work: %s"%w)
          self.send_worker(w, {'op': HydraWorker.EVENT_RETURN_WORK})
      else:
        self.log.debug("Existing work queue items sufficient to send to idle workers")
    except Exception as e:
      self.log.exception(e)
    
  def _heartbeat(self, idle_count):
    """
    Method that is called periodically to perform housekeeping
    """
    self.log.debug("Heartbeat count: %d, idle count: %d"%(self.heartbeat_count, idle_count))
    # Check and retrieve work items from workers if not all workers are active
    self._get_worker_work_items()
    # If we have any unprocessed work items, send them to the workers
    self.log.debug("Distribute work, items in queue: %d"%len(self.work_queue))
    self._process_dirs()
    # Request worker stats if required
    if self.heartbeat_count%self.stats_heartbeat_interval == 0:
      # Heartbeat based stats update are always behind real time
      self._send_server_stats()
      self._get_worker_stats()
    
  def _init_logger(self):
    self.log_svr = HydraUtils.LogRecordStreamHandler(
        name=__name__,
        addr = self.log_addr,
        port = self.log_port,
    )
    self.log_port = self.log_svr.get_port()
    self.log_secret = self.log_svr.get_secret()
    # Create a worker logging configuration
    self.args['logger_worker_config'] = dict(HydraUtils.LOGGING_WORKER_CONFIG)
    HydraUtils.set_logger_handler_to_socket(
        self.args['logger_worker_config'],
        'default',
        host=self.log_addr,
        port=self.log_port,
        secret=self.log_secret,
    )
    HydraUtils.set_logger_logger_level(
        self.args['logger_worker_config'],
        '',
        self.log.level,
    )
    self.log_svr.start_logger()
    
  def _init_worker_state(self, worker):
    """
    Fill in docstring
    """
    self.workers[worker.name] = {
      'obj': worker,
      'state': 'initializing',
      'stats': {},
    }
  
  def _process_dirs(self):
    """
    Fill in docstring
    """
    idle, processing = self._get_idle_working_worker_keys()
    queue_len = len(self.work_queue)
    if (queue_len == 0) or (len(idle) == 0):
      if len(processing) == 0:
        # The heartbeat will catch the fact that some workers are idle and ask
        # non-idle workers to return some work.
        self.log.debug("Process dirs called and all workers idle")
        if self.state not in [STATE_INIT, STATE_CONNECTED]:
          self.event_queue.append({'c': EVENT_REQUEST_WORK, 'd': None})
      return
    self.log.log(5, 'Work Q len: %d, Idle: %d'%(queue_len, len(idle)))
    split = 0
    # We want to use an algorithm to fairly distribute work among all the
    # workers. The goal is to have as little message passing between the workers
    # as possible. We want to try and send more than 1 path to process per
    # worker whenever possible to lower message passing overhead as long as
    # there is enough work to send more than 1 per worker.
    #
    # If we have fewer queued work items compared to idle workers, just send
    # each worker 1 work item until we run out
    split = (queue_len)//(len(idle))
    if split > self.dirs_per_idle_worker:
      split = self.dirs_per_idle_worker
    elif split <= 0:
      split = 1
    self.log.log(5, 'Work split: %d'%split)
    for k in idle:
      if self.workers.get(k, {})['state'] == 'idle':
        work_items = []
        for i in range(split):
          try:
            item = self.work_queue.popleft()
            work_items.append(item['path'])
          except:
            break
        if work_items:
          self.log.debug("Sending idle worker: %s directories: %s"%(k, work_items))
          self.send_worker(k, {'op': HydraWorker.EVENT_PROCESS_DIR, 'dirs': work_items})
          self._process_worker_state(k, 'idle_queued_work')
  
  def _process_returned_work(self, msg):
    """
    Fill in docstring
    """
    for i in msg.get('data', []):
      if i['type'] == 'dir':
        self._queue_work_paths(i['path'])
      elif i['type'] == 'partial_dir':
        # TODO: Need to support partial work path
        self.log.critical('Partial work path returned. This is currently not supported. Partial data:\n%s'%i)
      else:
        self.log.warn("Unknown work item returned: %s"%i)
        
  def _process_worker_state(self, worker_id, state):
    """
    Fill in docstring
    """
    worker = self.workers.get(worker_id, self.shutdown_pending.get(worker_id, None))
    if not worker:
      return None
    old_state = worker['state']
    if old_state == state:
      self.log.debug("No state change for worker %s in state: %s --> %s"%(worker_id, state))
      return state
    worker['state'] = state
    self.log.debug("Worker %s state change: %s --> %s"%(worker_id, old_state, state))
    if state == HydraWorker.STATE_SHUTDOWN:
      self.log.debug("Worker shutdown complete")
      self._cleanup_worker(worker_id)
      if (self.get_num_workers() < 1) and (len(self.shutdown_pending) == 0):
        self.event_queue.append({'c': EVENT_SHUTDOWN, 'd': None})
    elif state == HydraWorker.STATE_IDLE:
      self.log.log(5, 'Idle worker triggering process_dirs')
      self._process_dirs()
    return old_state
  
  def _process_worker_stats(self, msg):
    """
    Save the stats object the worker sends without processing immediately.
    Stats will be consolidated only when needed to save computation time
    """
    self.log.debug("Process worker stats: %s"%msg)
    k = msg.get('id')
    worker = self.workers.get(k, self.shutdown_pending.get(k, None))
    if worker:
      worker['stats'] = msg.get('data')
      
  def _queue_work_paths(self, paths):
    """
    Fill in docstring
    """
    if not isinstance(paths, (list, tuple)):
      paths = [paths]
    for path in paths:
      if not path:
        continue
      self.work_queue.append({'type': 'dir', 'path': path})

  def _request_work_from_server(self):
    idle, processing = self._get_idle_working_worker_keys()
    self._send_server(CMD_CLIENT_REQUEST_WORK, {'worker_status': {'idle': len(idle), 'processing': len(processing)}})
  
  def _return_server_work(self, divisor=2):
    """
    Use simple algorithm by returning roughly half of our work items
    """
    return_queue = len(self.work_queue)//divisor
    if return_queue > 0:
      return_items = []
      for i in range(return_queue):
        work_item = self.work_queue.pop()
        work_type = work_item.get('type', None)
        return_items.append(work_item)
      self._send_server(CMD_CLIENT_WORK, {'work_items': return_items})
    else:
      # Check and retrieve work items from workers if not all workers are active
      self._get_worker_work_items(forced=True)
      self.server_waiting_for_work = True
    
  def _send_server(self, cmd, msg):
    """
    Fill in docstring
    """
    self.log.debug("Sending server message: %s"%msg)
    try:
      if self.server:
        HydraUtils.socket_send(self.server, {'cmd': cmd, 'msg': msg})
      else:
        self.log.info("Request to send server message with a closed server connection: %s"%msg)
    except Exception as e:
      if e.errno == errno.EPIPE or e.errno == errno.ECONNRESET:
        # Pipe to server broken, shut ourselves down
        self.log.error('Connection to server unexpectedly closed. Shutting down client')
      self.event_queue.append({'c': EVENT_SHUTDOWN, 'd': None})
      self.log.exception(e)
      
  def _send_server_stats(self):
    """
    Fill in docstring
    """
    self.consolidate_stats()
    self._send_server(CMD_CLIENT_STATS, {'stats': self.stats})
    
  def _shutdown(self):
    """
    Fill in docstring
    """
    self.set_workers(0)
    if self.server:
      self.inputs.remove(self.server)
      try:
        self.server.close()
      except:
        pass
      self.server = None

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
    
  def _process_state_event(self, event, data=None, src='server'):
    """
    Fill in docstring
    """
    table = self.state_table[self.state]
    handler = table.get(event)
    if handler:
      self.log.debug("Client handling event '%s' @ '%s' with '%s'"%(event, self.state, handler['a']))
      next_state = handler['a'](event, data, handler.get('ns'))
      self._set_state(next_state or handler.get('ns'))
    else:
      if src == 'server':
        if not self.handle_extended_server_cmd(data):
          self.log.critical("Unhandled server event (%s) with data (%s) received in '%s' state."%(event, data, self.state))
      else:
        if not self.handle_extended_worker_msg(data):
          self.log.critical("Unhandled worker event (%s) received in '%s' state."%(event, self.state))
    
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
        self._send_server_stats()
      self.state = state
      self._send_server(CMD_CLIENT_STATE, {'state': self.state, 'prev_state': old_state})
      self.log.debug('Client state change: %s => %s'%(old_state, state))
    return old_state
    
  def _h_no_op(self, event, data, next_state):
    return next_state

  def _h_connected(self, event, data, next_state):
    self._set_state(STATE_CONNECTED)
    idle, processing = self._get_idle_working_worker_keys()
    if len(idle) >= self.get_num_workers():
      self.event_queue.append({'c': EVENT_WORKERS_IDLE, 'd': None})
    return next_state
    
  def _h_heartbeat(self, event, data, next_state):
    self._heartbeat(data['idle_count'])
    return next_state

  def _h_query_stats(self, event, data, next_state):
    self._send_server_stats()
    return next_state
    
  def _h_requeue(self, event, data, next_state):
    self.log.debug("Event being re-queued: %s, %s"%(event, data))
    self.event_queue.append({'c': event, 'd': data})
    return next_state

  def _h_request_work(self, event, data, next_state):
    self._request_work_from_server()
    return next_state
    
  def _h_request_work_s(self, event, data, next_state):
    self._request_work_from_server()
    self._send_server_stats()
    return next_state
    
  def _h_return_work(self, event, data, next_state):
    self._return_server_work()
    return next_state
    
  def _h_shutdown(self, event, data, next_state):
    self._set_state(STATE_SHUTDOWN)
    self._shutdown()
    return next_state

  def _h_submit_work(self, event, data, next_state):
    self._queue_work_paths(data.get('paths', []))
    self._process_dirs()
    return next_state
    
  def _h_update_settings(self, event, data, next_state):
    self.handle_update_settings(data)
    return next_state

  def _h_w_idle_conn(self, event, data, next_state):
    # All workers idle run special handler to allow further configuration
    self.handle_workers_connected()
    return next_state

  def _h_w_state(self, event, data, next_state):
    self._process_worker_state(data.get('id'), data.get('data'))
    return next_state

  def _h_w_state_conn(self, event, data, next_state):
    self._process_worker_state(data.get('id'), data.get('data'))
    idle, processing = self._get_idle_working_worker_keys()
    if len(idle) >= self.get_num_workers():
      self.event_queue.append({'c': EVENT_WORKERS_IDLE, 'd': None})
    return next_state

  def _h_w_stats(self, event, data, next_state):
    self._process_worker_stats(data)
    return next_state

  def _h_w_work_items(self, event, data, next_state):
    self._process_returned_work(data)
    if self.server_waiting_for_work:
      self._return_server_work()
    return next_state


class HydraClientProcess(multiprocessing.Process):
  def __init__(self, args=None):
    super(HydraClientProcess, self).__init__()
    if 'svr' not in args:
      raise Exception('Missing "svr" argument')
    if 'port' not in args:
      raise Exception('Missing "port" argument')
    if 'file_handler' not in args:
      raise Exception('Missing "file_handler" argument')
    self.args = dict(args)
    self.handler = self.args.get('handler', HydraClient)
    self.server_addr = self.args['svr']
    self.server_port = self.args['port']
    self.file_handler = self.args['file_handler']
    self.num_workers = 0
    self.client = None
    
  def init_process_logging(self):
    if len(logging.getLogger().handlers) == 0:
      if self.args.get('logger_cfg'):
        logging.config.dictConfig(self.args['logger_cfg'])
  
  def set_workers(self, num_workers=0):
    '''Number of worker processes per client. A value of 0 lets the client
    automatically adjust to the number of CPU cores'''
    self.num_workers = num_workers
    
  def run(self):
    self.init_process_logging()
    self.client = self.handler(self.file_handler, self.args)
    logging.getLogger().debug("PID: %d, Process name: %s"%(self.pid, self.name))
    try:
      self.client.connect(self.server_addr, self.server_port)
    except socket.error as e:
      self.client = None
      if e.errno == errno.ECONNREFUSED:
        msg = 'Connection to server refused. Check address, port and firewalls'
        logging.getLogger().critical(msg)
        sys.exit(1)
      else:
        raise e
    except Exception as se:
      logging.getLogger().exception(se)
      raise(se)
    if self.num_workers == 0:
      self.num_workers = 'auto'
    self.client.set_workers(self.num_workers)
    self.client.serve_forever()
