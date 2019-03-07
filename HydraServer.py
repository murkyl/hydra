# -*- coding: utf8 -*-
"""
Module description here
"""

import os
import sys
import multiprocessing
import time
import logging
import socket
import select
try:
   import cPickle as pickle
except:
   import pickle
import zlib
from collections import deque
import HydraClient
import HydraWorker
import HydraUtils


__title__ = "HydraServer"
__version__ = "1.0.0"
__all__ = ["HydraServer", "HydraServerProcess"]
__author__ = "Andrew Chung <acchung@gmail.com>"
__license__ = "MIT"
__copyright__ = """Copyright 2019 Andrew Chung
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

class HydraServer(object):
  def __init__(self):
    """
    Fill in docstring
    """
    self.log = logging.getLogger(__name__)
    self.heartbeat_count = 1
    self.heartbeat_interval = HydraUtils.HEART_BEAT_INTERVAL                    # Seconds for each heartbeat
    self.max_shutdown_idle_interval = HydraUtils.IDLE_SHUTDOWN_THRESHOLD        # Number of idle heartbeats before forcing a pending shutdown
    self.dirs_per_idle_client = HydraUtils.DIRS_PER_IDLE_CLIENT
    self.select_poll_interval = HydraUtils.SELECT_POLL_INTERVAL
    self.stats_heartbeat_interval = HydraUtils.STATS_HEARTBEAT_INTERVAL         # Number of heartbeat intervals per worker stat update
    self.num_clients = 0
    self.clients = {}                                                           # Connected client list
    self.shutdown_clients = {}
    self.state = 'initializing'
    self.work_queue = deque()
    self.stats = {}
    self.init_stats(self.stats)
    
    # Variables dealing with server and worker communication
    self.server = None
    self.server_handle = None
    self.server_listen_addr = HydraUtils.DEFAULT_LISTEN_ADDR
    self.server_listen_port = HydraUtils.DEFAULT_LISTEN_PORT
    # Used instead to send and receive commands from the Web UI
    self.server_pipe, self.api_pipe = multiprocessing.Pipe()
    # Sockets from which we expect to read from through a select call
    self.inputs = []
    self.inputs.append(self.server_pipe)
    
  def init_stats(self, stat_state):
    """
    This method is called to initialize the statistics of the server.
    This method can overridden in order to handle custom statistics but either
    calling the base class to handle the generic stats should be done or the
    derived class must do the same work.
    """
    for s in HydraUtils.BASIC_STATS:
      stat_state[s] = 0
  
  def consolidate_stats(self):
    """
    This method is called to consolidate all the client stats into a single
    coherent stats object.
    This method can overridden in order to handle custom statistics
    """
    try:
      self.init_stats(self.stats)
      for set in [self.clients, self.shutdown_clients]:
        for c in set:
          if not set[c]['stats']:
            continue
          for s in HydraUtils.BASIC_STATS:
            self.stats[s] += set[c]['stats'][s]
    except Exception as e:
      self.log.exception(e)

  def handle_extended_client_cmd(self, cmd):
    """
    Called by the main loop when an unknown command is found
    This can be used to support custom commands from a HydraClient without
    having to re-write the operation parser. This can be overridden as necessary
    when subclassing :class:`HydraServer <multiprocessing.Process>`.
    
    Return True if the command was handled
    Return False if the command was not handled. This is the default.
    """
    return False
    
  def handle_extended_server_cmd(self, cmd):
    """
    Called by the main loop when an unknown command is found
    This can be used to support custom commands from a HydraClient without
    having to re-write the operation parser. This can be overridden as necessary
    when subclassing :class:`HydraServer <multiprocessing.Process>`.
    
    Return True if the command was handled
    Return False if the command was not handled. This is the default.
    """
    return False
    
  def set_server_connection(self, addr=None, port=None):
    """
    Fill in docstring
    """
    if addr is not None:
      self.sever_listen_addr = addr
    if port is not None:
      self.server_listen_port = port
      
  def get_api_pipe_handle(self):
    """
    Fill in docstring
    """
    return self.api_pipe
  
  def serve_forever(self):
    """
    Fill in docstring
    """
    if not self.server:
      self._setup_server()
  
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
        if s is self.server:
          self._setup_client()
        elif s is self.server_pipe:
          msg = s.recv()
          self._process_command(msg)
        else:
          try:
            self.log.log(9, "Got cmd from client: %s"%s)
            msg = pickle.load(self.clients[s]['handle'])
            self._process_client_command(s, msg)
          except EOFError as eof:
            self.log.debug("Attempted to read closed socket: %s"%s)
            self._cleanup_client(s)
          except Exception as e:
            self.log.exception(e)
            self._cleanup_client(s)
        
      # Handle exceptions
      for s in exceptional:
        self.log.error('Handling exceptional condition for %r'%s)
        if s is self.server:
          self._shutdown(type='full')
          break
    self.log.debug("Server exiting")
  
  ''' Internal methods '''
  def _setup_client(self):
    """
    Fill in docstring
    """
    connection, client_address = self.server.accept()
    self.log.debug('New connection from %s, Socket: %s'%(client_address, connection))
    connection.setblocking(0)
    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    self.inputs.append(connection)
    client_obj = {
      'client': connection,
      'addr': client_address,
      'handle': connection.makefile('wb+'),
      'stats': {},
      'state': 'connected',
    }
    self.init_stats(client_obj)
    self.clients[connection] = client_obj
    self.num_clients += 1

  def _send_client_command(self, client, cmd):
    """
    Fill in docstring
    """
    if self.clients:
      c = self.clients.get(client)
      if c:
        try:
          pickle.dump(cmd, c['handle'], pickle.HIGHEST_PROTOCOL)
          c['handle'].flush()
        except Exception as e:
          self.log.exception(e)
    
  def _process_client_state(self, client, cmd):
    """
    Fill in docstring
    """
    old_state = self.clients[client]['state']
    state = cmd.get('state')
    self.clients[client]['state'] = state
    self.log.debug("Client %s state change %s -> %s"%(client, old_state, state))
    if state == 'shutdown':
      self._cleanup_client(client)
    if (old_state != 'idle' and state == 'idle'):
      self._process_dirs()
    self._update_server_state()
    return old_state
    
  def _process_returned_work(self, cmd):
    """
    Fill in docstring
    """
    for i in cmd.get('data', []):
      if i['type'] == 'dir':
        self._queue_work_paths(i['path'])
      else:
        self.log.warn("Unknown work item returned: %s"%i)
        
  def _process_client_stats(self, client, cmd):
    """
    Fill in docstring
    """
    # Stats will be consolidated only when needed to save computation time
    self.log.debug("Process client stats: %s"%cmd)
    self.clients[client]['stats'] = cmd.get('stats')
      
  def _process_client_command(self, client, cmd):
    """
    Fill in docstring
    """
    if not 'cmd' in cmd:
      self.log.error("Unknown command format received from server: %r"%cmd)
      return
    command = cmd.get('cmd')
    if command == 'state':
      self._process_client_state(client, cmd)
    elif command == 'work_items':
      self._process_returned_work(cmd)
    elif command == 'stats':
      self._process_client_stats(client, cmd)
    else:
      if not self.handle_extended_client_cmd(cmd):
        self.log.warn("Unhandled client command: %s"%cmd)
  
  def _cleanup_client(self, client):
    """
    Fill in docstring
    """
    c = self.clients.get(client)
    if c:
      self.log.debug("Cleaning up client: :%s"%client)
      self.inputs.remove(client)
      self.num_clients -= 1
      c = self.clients.pop(client)
      key = "%s:%s"%(str(client), c['addr'])
      self.shutdown_clients[key] = {'stats': c['stats']}
      try:
        c['handle'].close()
        client.close()
      except Exception as e:
        self.log.debug("Exception closing client %s: %s"%(client, e))
        
  def _shutdown_client(self, client):
    """
    Fill in docstring
    """
    self._send_client_command(client, {'cmd': 'shutdown'})
    self.log.debug("Shutdown command sent to: :%s"%client)
    
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
    for i in range(self.dirs_per_idle_client):
      for k in self.clients.keys():
        if self.clients.get(k, {})['state'] == 'idle':
          try:
            work_item = self.work_queue.popleft()
          except:
            self.log.debug("No directories queued for processing. Search for idle clients stopped")
            return
          self.log.debug("Sending idle client directory: %s"%work_item['path'])
          self._send_client_command(k, {'cmd': 'submit_work', 'paths': [work_item['path']]})
    
  def _send_ui(self, msg, flush=True):
    """
    Fill in docstring
    """
    self.log.debug("Sending UI message: %s"%msg)
    try:
      self.server_pipe.send(msg)
    except Exception as e:
      self.log.exception(e)
      
  def _send_ui_stats(self, cmd={}):
    """
    Fill in docstring
    """
    self.consolidate_stats()
    if cmd.get('data', 'simple') == 'simple':
      self._send_ui({'cmd': 'stats', 'stats': self.stats})
    elif cmd.get('data') == 'individual_clients':
      for set in [self.clients, self.shutdown_clients]:
        for c in set:
          if not set[c]['stats']:
            continue
          self._send_ui({'cmd': 'stats_individual_clients', 'stats': set[c]['stats'], 'client': set[c]['addr']})
    
  def _get_client_stats(self):
    """
    Fill in docstring
    """
    cmd = {'cmd': 'return_stats'}
    for c in self.clients:
      self._send_client_command(c, cmd)
    
  def _setup_server(self):
    """
    Fill in docstring
    """
    self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #self.server.setblocking(0)
    self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.server.bind((self.server_listen_addr, self.server_listen_port))
    if self.server_listen_port == 0:
      self.server_listen_port = self.server.getsockname()[1]
    self.server.listen(5)
    self.inputs.append(self.server)
    self._set_server_state('idle')
    
  def _set_server_state(self, next_state):
    """
    Fill in docstring
    """
    old_state = self.state
    if old_state == 'shutdown_pending' or old_state == 'shutdown':
      if next_state != 'shutdown_pending' and next_state != 'shutdown':
        self.log.debug("Cannot change state out of shutdown pending or shutdown to: %s"%next_state)
        next_state = self.state
    self.state = next_state
    if (old_state != next_state):
      self.log.debug("Server state change %s -> %s"%(old_state, next_state))
      self._send_ui({'cmd': 'state', 'state': self.state, 'prev_state': old_state})
    return old_state

  def _update_server_state(self):
    """
    Fill in docstring
    """
    old_state = self.state
    next_state = self.state
    idle = 0
    not_idle = 0
    for c in self.clients:
      if self.clients[c]['state'] in ['initializing', 'connected', 'idle']:
        idle += 1
      else:
        not_idle += 1
    self.log.debug("Idle clients: %s, Non-idle clients: %s, Num clients: %s"%(idle, not_idle, self.num_clients))
    if self.state == 'shutdown_pending':
      if self.num_clients == 0:
        next_state = 'shutdown'
    else:
      if not_idle > 0:
        next_state = 'processing'
      else:
        next_state = 'idle'
    if old_state != next_state and next_state == 'shutdown':
      self._send_ui_stats()
    self._set_server_state(next_state)
      
  def _heartbeat(self, idle_count):
    """
    Fill in docstring
    """
    self.log.debug("Heartbeat count: %d, idle count: %d"%(self.heartbeat_count, idle_count))
    # First check if we need to shutdown this client
    self.log.debug("Checking for shutdown state: %s"%self.state)
    if self.state == 'shutdown_pending' and idle_count > self.max_shutdown_idle_interval:
      self._set_server_state('shutdown')
      return
    # Check if we have idle clients. If we do query any active clients and have
    # them return work items
    try:
      num_idle_clients = 0
      processing_clients = []
      self.log.debug("Checking for idle clients")
      for k in self.clients.keys():
        client_state = self.clients.get(k, {})['state']
        self.log.debug("Current client state: %s - %s"%(k, client_state))
        if client_state == 'processing' or client_state == 'paused':
          processing_clients.append(k)
        elif client_state == 'idle':
          num_idle_clients += 1
      if len(self.work_queue) < num_idle_clients:
        for c in processing_clients:
          self.log.debug("Requesting active client to return work: %s"%c)
          self._send_client_command(c, {'cmd': 'return_work'})
      else:
        self.log.debug("Existing work queue items sufficient to send to idle workers")
    except Exception as e:
      self.log.exception(e)
    # If we have any unprocessed work items, send them to the workers
    self.log.debug("Distribute work, items in queue: %d"%len(self.work_queue))
    self._process_dirs()
    # Request worker stats if required
    if self.heartbeat_count%self.stats_heartbeat_interval == 0:
      self._get_client_stats()
      
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
    elif command == 'shutdown':
      self._shutdown()
    elif command == 'get_stats':
      self._send_ui_stats(cmd)
    elif command == 'start':
      pass
    elif command == 'pause':
      pass
    else:
      if not self.handle_extended_server_cmd(cmd):
        self.log.warn("Unhandled server command: %s"%cmd)
      
  def _shutdown(self, type='normal'):
    """
    Fill in docstring
    """
    if type == 'full':
      self._set_server_state('shutdown')
    else:
      self._set_server_state('shutdown_pending')
    if self.clients:
      for c in self.clients:
        self.log.debug("Shutting down client: %s"%c)
        self._shutdown_client(c)
    else:
      self._set_server_state('shutdown')


class HydraServerProcess(multiprocessing.Process):
  def __init__(self,
        handler=None,
        addr=HydraUtils.DEFAULT_LISTEN_ADDR,
        port=HydraUtils.DEFAULT_LISTEN_PORT,
    ):
    super(HydraServerProcess, self).__init__()
    if handler:
      self.server = handler()
    else:
      self.server = HydraServer()
    self.listen_addr = addr
    self.listen_port = port
    self.server.set_server_connection(addr=self.listen_addr, port=self.listen_port)
    self.api_pipe = self.server.get_api_pipe_handle()
  
  def fileno(self):
    return self.api_pipe.fileno()
    
  def send(self, msg):
    self.api_pipe.send(msg)
    
  def recv(self):
    return self.api_pipe.recv()
    
  def get_api_pipe(self):
    return self.api_pipe
    
  def run(self):
    self.server.serve_forever()
