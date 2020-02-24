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


'''
TODO: Need to add support for keeping local stats about queued dirs and combine with workers
TODO: Need to add code to return all unprocessed items when shutting down client
TODO: Change code to run a state machine to simplify logic
TODO: Need to set state where we need to go from initializing -> Connected -> idle
      If we are not connected when serve_forever is called we need to wait to get into
      connected state
'''

"""
State table

<Start state> initializing
initializing -> connected
connected -> idle
idle -> processing
idle -> shutdown
idle -> shutdown_pending
processing -> idle
processing -> shutdown
processing -> shutdown_pending
shutdown_pending -> shutdown
shutdown <End State>
"""
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
    self.state = 'initializing'
    self.work_queue = deque()
    self._init_logger()
    self.init_stats(self.stats)

    # Variables dealing with server and worker communication
    self.server = None
    # Sockets from which we expect to read from through a select call
    self.inputs = []
    
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

  def handle_extended_worker_msg(self, raw_data):
    """
    Called by the main loop when an unknown command is found
    This can be used to support custom commands from a HydraWorker without
    having to re-write the operation parser. This can be overridden as necessary
    when subclassing :class:`HydraWorker <multiprocessing.Process>`.
    
    Return True if the command was handled
    Return False if the command was not handled. This is the default.
    """
    return False
    
  def handle_extended_server_cmd(self, raw_data):
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
    return True
    
  def connect(self, server_addr, server_port=HydraUtils.DEFAULT_LISTEN_PORT):
    """
    Fill in docstring
    """
    self.server = socket.create_connection((server_addr, server_port))
    self.server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    self.inputs.append(self.server)
    self._set_state('connected')
  
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
      if self.workers[w]['state'] == 'idle' or self.workers[w]['state'] == 'initializing':
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
      worker_state['obj'].send({'op': 'shutdown'})
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
    self._set_state('idle')
    
    idle_count = 0
    start_time = time.time()
    while not self.state == 'shutdown':
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
          self._heartbeat(idle_count)
      except KeyboardInterrupt:
        self.log.debug("Caught keyboard interrupt waiting for event")
        self._shutdown(type='immediate')
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
              self._shutdown(type='immediate')

      # Handle inputs
      for s in readable:
        try:
          if s is self.server:
            try:
              msg_size = s.recv(4)
              if len(msg_size) != 4:
                self.log.debug("Closing server handle due to EOF of command stream: %r"%s)
                self._shutdown(type='immediate')
                continue
              data_len = struct.unpack('!L', msg_size)[0]
              data = HydraUtils.socket_recv(s, data_len)
              self._process_command(data)
            except EOFError as eof:
              self.log.debug("Closing server handle due to EOF of command stream: %r"%s)
              self._shutdown(type='immediate')
              continue
            except socket.error as serr:
              if serr.errno != errno.ECONNRESET:
                raise
              self.log.debug("Closing server handle due to EOF of command stream: %r"%s)
              self._shutdown(type='immediate')
              continue
            except Exception:
              self.log.exception("Exception encountered during command processing.")
              self._shutdown(type='immediate')
              continue
          else:
            cmd = s.recv()
            if len(cmd) > 0:
              self.log.log(9, "Got cmd from worker: %s"%cmd)
              self._process_worker_command(s, cmd)
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
          self._shutdown(type='immediate')
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
      if worker_state == 'idle':
        idle.append(k)
      elif (worker_state == 'processing') or (worker_state == 'paused'):
        processing.append(k)
      else:
        self.log.log(9, "Found worker in state: %s"%worker_state)
    return idle, processing

      
  def _get_worker_stats(self):
    """
    Fill in docstring
    """
    self.send_active_workers({'op': 'return_stats'})
      
  def _get_worker_work_items(self):
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
      if len(self.work_queue) < num_idle_workers:
        for w in processing:
          self.log.debug("Requesting active worker to return work: %s"%w)
          self.send_worker(w, {'op': 'return_work'})
      else:
        self.log.debug("Existing work queue items sufficient to send to idle workers")
    except Exception as e:
      self.log.exception(e)
    
  def _heartbeat(self, idle_count):
    """
    Method that is called periodically to perform housekeeping
    """
    self.log.debug("Heartbeat count: %d, idle count: %d"%(self.heartbeat_count, idle_count))
    # First check if we need to shutdown this client
    self.log.debug("Checking for shutdown state: %s"%self.state)
    if self.state == "shutdown_pending" and idle_count > self.idle_shutdown_interval:
      self._set_state('shutdown')
      return
    # Check and retrieve work items from workers if not all workers are active
    self._get_worker_work_items()
    # If we have any unprocessed work items, send them to the workers
    self.log.debug("Distribute work, items in queue: %d"%len(self.work_queue))
    self._process_dirs()
    # Request worker stats if required
    if self.heartbeat_count%self.stats_heartbeat_interval == 0:
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
    
  def _process_command(self, cmd):
    """
    Fill in docstring
    """
    if not 'cmd' in cmd:
      self.log.error("Unknown command format received from server: %r"%cmd)
      return
    command = cmd.get('cmd')
    if command == "submit_work":
      self._queue_work_paths(cmd.get('paths', []))
      self._process_dirs()
    elif command == "shutdown":
      self._shutdown()
    elif command == "return_stats":
      self._send_server_stats()
    elif command == "return_work":
      self._return_server_work()
    elif command == "update_settings":
      self.handle_update_settings(cmd)
    else:
      if not self.handle_extended_server_cmd(cmd):
        self.log.warn("Unhandled server command: %s"%cmd)
      
  def _process_dirs(self):
    """
    Fill in docstring
    """
    workers_sent_work = {}
    idle, processing = self._get_idle_working_worker_keys()
    queue_len = len(self.work_queue)
    if (queue_len == 0) or (len(idle) == 0):
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
    for k in self.workers.keys():
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
          self.send_worker(k, {'op': 'proc_dir', 'dirs': work_items})
          workers_sent_work[k] = True
    # Semi-hack to track the fact we have sent work to an idle worker
    for k in workers_sent_work.keys():
      self.log.debug("Setting worker (%s) to idle_queued_work"%k)
      self.workers[k]['state'] = 'idle_queued_work'
  
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
        
  def _process_worker_command(self, worker, cmd):
    """
    Fill in docstring
    """
    op = cmd.get('op', None)
    if op == 'state':
      old_state = self._process_worker_state(cmd['id'], cmd['data'])
    elif op == 'work_items':
      self._process_returned_work(cmd)
    elif op == 'stats':
      self._process_worker_stats(cmd)
    else:
      if not self.handle_extended_worker_msg(cmd):
        self.log.warn("Client got unexpected cmd from worker: %s"%worker)
  
  def _process_worker_state(self, worker_id, state):
    """
    Fill in docstring
    """
    old_state = None
    worker = self.workers.get(worker_id, self.shutdown_pending.get(worker_id, None))
    if worker:
      old_state = worker['state']
      if state in HydraUtils.HYDRA_WORKER_STATES.values():
        worker['state'] = state
      else:
        self.log.error("Invalid HydraWorker state update received for worker: %s:%s"%(state, worker_id))
    self.log.debug("Worker %s state change: %s --> %s"%(worker_id, old_state, state))
    if state == 'shutdown':
      self.log.debug("Worker shutdown complete")
      self._cleanup_worker(worker_id)
      if (self.get_num_workers() < 1) and (len(self.shutdown_pending) == 0):
        self._send_server_stats()
        self._set_state('shutdown')
        self._shutdown(type='immediate')
    if state == 'idle':
      if old_state != 'idle':
        self.log.log(5, 'Idle worker triggering process_dirs')
        self._process_dirs()
    self._update_client_state()
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
      self._send_server({'cmd': 'work_items', 'data': return_items})
    else:
      # Check and retrieve work items from workers if not all workers are active
      self._get_worker_work_items()
    
  def _send_server(self, msg):
    """
    Fill in docstring
    """
    self.log.debug("Sending server message: %s"%msg)
    try:
      if self.server:
        HydraUtils.socket_send(self.server, msg)
      else:
        self.log.info("Request to send server message with a closed server connection: %s"%msg)
    except Exception as e:
      if e.errno == errno.EPIPE or e.errno == errno.ECONNRESET:
        # Pipe to server broken, shut ourselves down
        self.log.error('Connection to server unexpectedly closed. Shutting down client')
      self._shutdown('immediate')
      self.log.exception(e)
      
  def _send_server_stats(self):
    """
    Fill in docstring
    """
    self.consolidate_stats()
    self._send_server({'cmd': 'stats', 'stats': self.stats})
    
  def _set_state(self, next_state):
    """
    Fill in docstring
    """
    old_state = self.state
    if old_state == 'shutdown_pending' or old_state == 'shutdown':
      if next_state != 'shutdown_pending' and next_state != 'shutdown':
        self.log.debug("Cannot change state out of shutdown pending or shutdown to: %s"%next_state)
        next_state = self.state
    self.state = next_state
    if next_state == 'idle':
      self._send_server_stats()
    if (old_state != next_state):
      self._send_server({'cmd': 'state', 'state': self.state, 'prev_state': old_state})
      self.log.debug('Client state change: %s ==> %s'%(old_state, next_state))
    return old_state
    
  def _shutdown(self, type='normal'):
    """
    Fill in docstring
    """
    self.set_workers(0)
    if type == 'immediate':
      if self.server:
        self.inputs.remove(self.server)
        try:
          self.server.close()
        except:
          pass
        self.server = None
      self._set_state('shutdown')
    else:
      self._set_state('shutdown_pending')

  def _update_client_state(self):
    """
    Fill in docstring
    """
    try:
      idle, processing = self._get_idle_working_worker_keys()
      self.log.log(5, 'Idle worker keys: %s, Processing worker keys: %s'%(idle, processing))
      if len(processing) > 0:
        next_state = 'processing'
      elif (len(idle) > 0) and (len(self.work_queue) > 0):
        self.log.debug('Some/all workers idle but we have %d queued work items'%len(self.work_queue))
        next_state = 'processing'
      elif (len(idle)) == self.num_workers:
        next_state = 'idle'
      else:
        next_state = self.state
      self.log.debug("Idle workers: %s, Non-idle workers: %s, Num workers: %s"%(len(idle), len(processing), self.num_workers))
      self._set_state(next_state)
    except Exception as e:
      self.log.exception(e)
      raise


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
