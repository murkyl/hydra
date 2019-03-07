# -*- coding: utf8 -*-
"""
Module description here
"""
'''
TODO: Need to add support for keeping local stats about queued dirs and combine with workers
TODO: Need to add code to return all unprocessed items when shutting down client
TODO: Change code to run a state machine to simplify logic
TODO: Need to set state where we need to go from initializing -> Connected -> idle
      If we are not connected when server_forever is called we need to wait to get into
      connected state
'''
import os
import sys
import multiprocessing
import time
import logging
import socket
import errno
import select
try:
   import cPickle as pickle
except:
   import pickle
import zlib
from collections import deque
import HydraWorker
import HydraUtils


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

class HydraClient(object):
  def __init__(self, worker_class, worker_args={}):
    """
    Fill in docstring
    """
    self.log = logging.getLogger(__name__)
    self.worker_base_class = worker_class
    self.worker_args = worker_args
    self.heartbeat_count = 1
    self.heartbeat_interval = HydraUtils.HEART_BEAT_INTERVAL                    # Seconds for each heartbeat
    self.max_shutdown_idle_interval = HydraUtils.IDLE_SHUTDOWN_THRESHOLD        # Number of idle heartbeats before forcing a pending shutdown
    self.dirs_per_idle_worker = HydraUtils.DIRS_PER_IDLE_WORKER
    self.select_poll_interval = HydraUtils.SELECT_POLL_INTERVAL
    self.stats_heartbeat_interval = HydraUtils.STATS_HEARTBEAT_INTERVAL         # Number of heartbeat intervals per worker stat update
    self.num_workers = 0
    self.workers = {}                                                           # Active worker list
    self.shutdown_pending = {}                                                  # Workers that are pending shutdown
    self.shutdown_workers = {}                                                  # Workers that are fully cleaned up, saving only the stats
    self.state = 'initializing'
    self.work_queue = deque()
    self.stats = {}
    self.init_stats(self.stats)

    # Variables dealing with server and worker communication
    self.server = None
    self.server_handle = None
    # Used instead of or in addition to a server connection to receive
    # commands
    self.local_client_pipe, self.local_loop_pipe = multiprocessing.Pipe()
    # Sockets from which we expect to read from through a select call
    self.inputs = []
    self.inputs.append(self.local_client_pipe)
    
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

  def handle_extended_client_msg(self, raw_data):
    """
    Called by the main loop when an unknown command is found
    This can be used to support custom commands from a HydraClient without
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
    
    
  def connect(self, server_addr, server_port=HydraUtils.DEFAULT_LISTEN_PORT):
    """
    Fill in docstring
    """
    self.server = socket.create_connection((server_addr, server_port))
    #self.server.setblocking(0)
    self.server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    self.server_handle = self.server.makefile('wb+')
    self._set_client_state('connected')
    self.inputs.append(self.server)
  
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
      worker = self.worker_base_class(self.worker_args)
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
    for i in range(len(retire_keys) - num):
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
  
  def serve_forever(self):
    """
    Fill in docstring
    """
    if self.num_workers == 0:
      self.set_workers()
    self._set_client_state('idle')
    
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
        if (time.time() - start_time >= self.heartbeat_count*self.heartbeat_interval):
          self.heartbeat_count += 1
          self._heartbeat(idle_count)
      except KeyboardInterrupt:
        self.log.debug("Caught keyboard interrupt waiting for event")
        self._shutdown(type='full')
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
              self._shutdown(type='full')

      # Handle inputs
      for s in readable:
        try:
          if s is self.server:
            try:
              cmd = pickle.load(self.server_handle)
              self._process_command(cmd)
            except EOFError as eof:
              self.log.debug("Closing server handle due to EOF of command stream: %r"%s)
              self._shutdown(type='full')
              continue
            except Exception:
              self.log.exception("Exception encountered during command processing.")
              self._shutdown(type='full')
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
          self._shutdown(type='full')
          break
    self.log.debug("Client exiting")
      
  ''' Internal methods '''
  def _init_worker_state(self, worker):
    """
    Fill in docstring
    """
    self.workers[worker.name] = {
      'obj': worker,
      'state': 'initializing',
      'stats': {},
    }
    
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
    self.log.debug("Worker %s state change %s -> %s"%(worker_id, old_state, state))
    if (old_state != 'idle' and state == 'idle'):
      self._process_dirs()
    self._update_client_state()
    return old_state
  
  def _process_returned_work(self, msg):
    """
    Fill in docstring
    """
    for i in msg.get('data', []):
      if i['type'] == 'dir':
        self._queue_work_paths(i['path'])
      elif i['type'] == 'partial_dir':
        # TODO: Need to support partial work path
        self.log.critical('Partial work path returned. This is currently not supported')
        #self._queue_work_paths(i['path'])
      else:
        self.log.warn("Unknown work item returned: %s"%i)
        
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
      
  def _process_worker_command(self, worker, cmd):
    """
    Fill in docstring
    """
    op = cmd.get('op', None)
    if op in ('status_idle', 'status_processing', 'status_paused'):
      old_state = self._process_worker_state(cmd['id'], cmd['op'][7:])
    elif op == 'status_shutdown_complete':
      self.log.debug("Worker shutdown complete: %r"%cmd)
      self._cleanup_worker(cmd['id'])
      if (self.get_num_workers() < 1) and (len(self.shutdown_pending) == 0):
        self._send_server_stats()
        self._set_client_state('shutdown')
        self._shutdown(type='full')
    elif op == 'work_items':
      self._process_returned_work(cmd)
    elif op == 'stats':
      self._process_worker_stats(cmd)
    elif op == 'state':
      old_state = self._process_worker_state(cmd['id'], cmd['data'])
    else:
      if not self.handle_extended_client_msg(cmd):
        self.log.warn("Client got unexpected cmd from worker: %s"%worker)
  
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
  
  def _process_dirs(self):
    """
    Fill in docstring
    """
    for i in range(self.dirs_per_idle_worker):
      for k in self.workers.keys():
        if self.workers.get(k, {})['state'] == 'idle':
          try:
            work_item = self.work_queue.popleft()
          except:
            self.log.debug("No directories queued for processing. Search for idle workers stopped")
            return
          self.log.debug("Sending idle worker directory: %s"%work_item['path'])
          self.workers[k]['obj'].send({'op': 'proc_dir', 'dirs': [work_item['path']]})
  
  def _send_server(self, msg, flush=True):
    """
    Fill in docstring
    """
    self.log.debug("Sending server message: %s"%msg)
    try:
      if self.server_handle:
        pickle.dump(msg, self.server_handle, pickle.HIGHEST_PROTOCOL)
        if flush:
          self.server_handle.flush()
      else:
        self.log.warn("Request to send server message with a closed server connection: %s"%msg)
        self._shutdown(type='full')
    except Exception as e:
      self.log.exception(e)
      raise
      
  def _send_server_stats(self):
    """
    Fill in docstring
    """
    self.consolidate_stats()
    self._send_server({'cmd': 'stats', 'stats': self.stats})
    
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
        # TODO: Need to account for queued dirs and files at the client level!
        #if work_type in ['dir', 'partial_dir']:
        #  self.stats['queued_dirs'] -= 1
        #elif work_type == 'file':
        #  self.stats['queued_files'] -= 1
        return_items.append(work_item)
      self._send_server({'cmd': 'work_items', 'data': return_items})
    else:
      # Check and retrieve work items from workers if not all workers are active
      self._get_worker_work_items()
    
  def _get_worker_stats(self):
    """
    Fill in docstring
    """
    try:
      for set in [self.workers, self.shutdown_pending]:
        for w in set:
          set[w]['obj'].send({'op': 'return_stats'})
    except Exception as e:
      self.log.exception(e)
      
  def _get_worker_work_items(self):
    """
    Fill in docstring
    """
    # Check if we have idle workers. If we do query any active workers and have
    # them return work items
    try:
      num_idle_workers = 0
      processing_workers = []
      self.log.debug("Checking for idle workers")
      for k in self.workers.keys():
        worker_state = self.workers.get(k, {})['state']
        self.log.debug("Current worker state: %s - %s"%(k, worker_state))
        if worker_state == 'processing' or worker_state == 'paused':
          processing_workers.append(k)
        elif worker_state == 'idle':
          num_idle_workers += 1
      if len(self.work_queue) < num_idle_workers:
        for w in processing_workers:
          self.log.debug("Requesting active worker to return work: %s"%w)
          self.workers[w]['obj'].send({'op': 'return_work'})
      else:
        self.log.debug("Existing work queue items sufficient to send to idle workers")
    except Exception as e:
      self.log.exception(e)
    
  def _set_client_state(self, next_state):
    """
    Fill in docstring
    """
    old_state = self.state
    # TODO: This should be done in a state machine
    if old_state == 'shutdown_pending' or old_state == 'shutdown':
      if next_state != 'shutdown_pending' and next_state != 'shutdown':
        self.log.debug("Cannot change state out of shutdown pending or shutdown to: %s"%next_state)
        next_state = self.state
    self.state = next_state
    if (old_state != next_state):
      self.log.debug("Client state change %s -> %s"%(old_state, next_state))
      self._send_server({'cmd': 'state', 'state': self.state, 'prev_state': old_state})
    return old_state

  def _update_client_state(self):
    """
    Fill in docstring
    """
    try:
      old_state = self.state
      idle = 0
      not_idle = 0
      for w in self.workers:
        if self.workers[w]['state'] == 'idle' or self.workers[w]['state'] == 'initializing':
          idle += 1
        else:
          not_idle += 1
      if not_idle > 0:
        next_state = 'processing'
      else:
        next_state = 'idle'
        self._send_server_stats()
      self.log.debug("Idle workers: %s, Non-idle workers: %s, Num workers: %s"%(idle, not_idle, self.num_workers))
      self._set_client_state(next_state)
    except Exception as e:
      self.log.exception(e)
      raise
    
  def _heartbeat(self, idle_count):
    """
    Method that is called periodically to perform housekeeping
    """
    self.log.debug("Heartbeat count: %d, idle count: %d"%(self.heartbeat_count, idle_count))
    # First check if we need to shutdown this client
    self.log.debug("Checking for shutdown state: %s"%self.state)
    if self.state == "shutdown_pending" and idle_count > self.max_shutdown_idle_interval:
      self._set_client_state('shutdown')
      return
    # Check and retrieve work items from workers if not all workers are active
    self._get_worker_work_items()
    # If we have any unprocessed work items, send them to the workers
    self.log.debug("Distribute work, items in queue: %d"%len(self.work_queue))
    self._process_dirs()
    # Request worker stats if required
    if self.heartbeat_count%self.stats_heartbeat_interval == 0:
      self._get_worker_stats()
    
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
    else:
      if not self.handle_extended_server_cmd(cmd):
        self.log.warn("Unhandled server command: %s"%cmd)
      
  def _shutdown(self, type='normal'):
    """
    Fill in docstring
    """
    self.set_workers(0)
    if type == 'full':
      if self.server_handle:
        self.server_handle.close()
        self.server_handle = None
      if self.server:
        self.inputs.remove(self.server)
      if self.server:
        self.server.close()
        self.server = None
      self._set_client_state('shutdown')
    else:
      self._set_client_state('shutdown_pending')


class HydraClientProcess(multiprocessing.Process):
  def __init__(self, args=None):
    super(HydraClientProcess, self).__init__()
    if 'svr' not in args:
      raise Exception('Missing "svr" argument')
    if 'port' not in args:
      raise Exception('Missing "port" argument')
    if 'file_handler' not in args:
      raise Exception('Missing "file_handler" argument')
    if 'handler' in args:
      self.handler = args['handler']
    else:
      self.handler = HydraClient
    self.server_addr = args['svr']
    self.server_port = args['port']
    self.file_handler = args['file_handler']
    self.file_handler_args = args.get('file_handler_args', {})
    self.num_workers = 0
    self.client = None
    
  def set_workers(self, num_workers=0):
    '''Number of worker processes per client. A value of 0 lets the client
    automatically adjust to the number of CPU cores'''
    self.num_workers = num_workers
    
  def run(self):
    self.client = self.handler(self.file_handler, self.file_handler_args)
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
    self.client.set_workers(self.num_workers)
    self.client.serve_forever()
