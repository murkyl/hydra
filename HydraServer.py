# -*- coding: utf8 -*-
"""
Module description here
"""
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
import os
import sys
import multiprocessing
import time
import logging
import socket
import errno
import select
import struct
import uuid
try:
   import cPickle as pickle
except:
   import pickle
import zlib
from collections import deque
import HydraClient
import HydraWorker
import HydraUtils


class HydraServer(object):
  def __init__(self, args={}):
    """
    Fill in docstring
    """
    self.args = dict(args)
    self.log = logging.getLogger(__name__)
    self.heartbeat_interval = args.get('heartbeat_interval', HydraUtils.HEARTBEAT_INTERVAL)                   # Seconds for each heartbeat
    self.idle_shutdown_interval = args.get('idle_shutdown_interval', HydraUtils.IDLE_SHUTDOWN_THRESHOLD)      # Number of idle heartbeats before forcing a pending shutdown
    self.dirs_per_idle_client = args.get('dirs_per_idle_client', HydraUtils.DIRS_PER_IDLE_CLIENT)
    self.select_poll_interval = args.get('select_poll_interval', HydraUtils.SELECT_POLL_INTERVAL)
    self.stats_heartbeat_interval = args.get('stats_heatbeat_interval', HydraUtils.STATS_HEARTBEAT_INTERVAL)  # Number of heartbeat intervals per client stat update
    self.heartbeat_count = 1
    self.num_clients = 0
    self.last_consolidate_stats = 0
    self.last_client_stat_update = 0
    self.clients = {}                 # Connected client list
    self.shutdown_clients = {}        # List of clients that have been shutdown. Used for statistics
    self.stats = {}
    self.state = 'initializing'
    self.work_queue = deque()
    self.init_stats(self.stats)
    
    # Variables dealing with server and client communication
    self.server = None
    self.server_listen_addr = args.get('server_listen_addr', HydraUtils.DEFAULT_LISTEN_ADDR)
    self.server_listen_port = args.get('server_listen_port', HydraUtils.DEFAULT_LISTEN_PORT)
    self.loopback_addr = args.get('loopback_addr', '127.0.0.1')
    self.loopback_port = args.get('loopback_port', 0)
    self.svr_side_conn = None         # Sockets used to communicate between the server and UI
    self.ui_side_conn = None          # Sockets used to communicate between the server and UI

    self.inputs = []                  # Sockets from which we expect to read from through a select call
    self._connect_ui()
    
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
    Returns: the stats object that should be used after processing.
    This method can be overridden in order to handle custom statistics.
    """
    if self.last_client_stat_update <= self.last_consolidate_stats:
      return self.stats
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
      self.last_consolidate_stats = time.time()
    return self.stats
      
  def handle_client_connected(self, client):
    """
    Called after a client connects to the server.
    The key to the self.clients array is passed back.
    This key can be used in the send_client_command to issue a command
    """
    return True

  def handle_extended_client_cmd(self, client, cmd):
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
    This can be used to support custom commands from without
    having to re-write the operation parser. This can be overridden as necessary
    when subclassing :class:`HydraServer <multiprocessing.Process>`.
    
    Return True if the command was handled
    Return False if the command was not handled. This is the default.
    """
    return False
    
  def handle_update_settings(self, cmd):
    """
    Handle a settings update from the UI
    """
    return True
    
  def fileno(self):
    """
    Fill in docstring
    """
    if self.ui_side_conn:
      return self.ui_side_conn.fileno()
    return None
    
  def recv(self, timeout=-1):
    """
    Fill in docstring
    """
    if timeout >= 0:
      readable, _, _ = select.select([self.ui_side_conn], [], [], timeout)
      if not readable:
        return False
    msg_size = self.ui_side_conn.recv(4)
    if len(msg_size) != 4:
      return b''
    data_len = struct.unpack('!L', msg_size)[0]
    data = HydraUtils.socket_recv(self.ui_side_conn, data_len)
    return data
    
    
  def send(self, msg):
    """
    Fill in docstring
    """
    try:
      HydraUtils.socket_send(self.ui_side_conn, msg)
    except Exception as e:
      self.log.exception(e)
      return False
    return True
    
  def send_all_clients_command(self, cmd):
    for c in self.clients:
      send_client_command(c, cmd)
    
  def send_client_command(self, client, cmd):
    """
    Fill in docstring
    """
    c = self.clients.get(client)
    if c:
      try:
        HydraUtils.socket_send(c['conn'], cmd)
      except Exception as e:
        self.log.exception(e)
    
  def set_server_connection(self, addr=None, port=None):
    """
    Fill in docstring
    """
    if addr is not None:
      self.sever_listen_addr = addr
    if port is not None:
      self.server_listen_port = port
      
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
        if s is self.server:
          self._setup_client()
        elif s is self.svr_side_conn:
          # Command received from the UI
          msg_size = s.recv(4)
          if len(msg_size) != 4:
            continue
          data_len = struct.unpack('!L', msg_size)[0]
          data = HydraUtils.socket_recv(s, data_len)
          self._process_command(data)
        else:
          # Command received from a client
          try:
            self.log.log(9, "Got cmd from client: %s"%s.fileno())
            msg_size = s.recv(4)
            if len(msg_size) != 4:
              self._cleanup_client(s)
              continue
            data_len = struct.unpack('!L', msg_size)[0]
            data = HydraUtils.socket_recv(s, data_len)
            self._process_client_command(s, data)
          except EOFError as eof:
            self.log.debug("Attempted to read closed socket: %s"%s)
            self._cleanup_client(s)
          except socket.error as serr:
            if serr.errno != errno.ECONNRESET:
              raise
            self._cleanup_client(s)
          except Exception as e:
            self.log.exception(e)
            self._cleanup_client(s)
        
      # Handle exceptions
      for s in exceptional:
        self.log.error('Handling exceptional condition for %r'%s)
        if s is self.server:
          self._shutdown(type='immediate')
          break
    self.log.debug("Server exiting")
  
  ''' Internal methods '''
  def _connect_ui(self):
    # Create temporary socket to listen for server<->ui connection
    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_sock.bind((self.loopback_addr, self.loopback_port))
    self.loopback_port = listen_sock.getsockname()[1]
    listen_sock.listen(1)
    # Create connection between server<->ui and exchange secret
    self.ui_side_conn = socket.create_connection((self.loopback_addr, self.loopback_port))
    self.ui_side_conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    self.svr_side_conn, _ = listen_sock.accept()
    # Close the listening socket as we no longer require it
    listen_sock.close()
    secret = HydraUtils.create_uuid_secret()
    self.ui_side_conn.sendall(secret)
    security = self.svr_side_conn.recv(len(secret))
    if security != secret:
      self.log.warn("Invalid secret exchange for socket connection!")
    self.inputs.append(self.svr_side_conn)
    self._send_ui_stats()
  
  def _setup_client(self):
    """
    Fill in docstring
    """
    connection, client_address = self.server.accept()
    self.log.debug('New connection from client %s, FD: %s'%(client_address, connection.fileno()))
    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
    self.inputs.append(connection)
    client_obj = {
      'conn': connection,
      'addr': client_address,
      'stats': {},
      'state': 'connecting',
    }
    self.init_stats(client_obj)
    self.clients[connection] = client_obj
    self.num_clients += 1
    self.handle_client_connected(connection)

  def _process_client_state(self, client, cmd):
    """
    Fill in docstring
    """
    old_state = self.clients[client]['state']
    state = cmd.get('state')
    self.clients[client]['state'] = state
    self.log.debug("Client %s state change %s -> %s"%(client.fileno(), old_state, state))
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
    self.log.debug("Process client (%s) stats: %s"%(client.fileno(), cmd))
    self.clients[client]['stats'] = cmd.get('stats')
    self.last_client_stat_update = time.time()
      
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
      if not self.handle_extended_client_cmd(client, cmd):
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
        c['conn'].close()
        client.close()
      except Exception as e:
        self.log.debug("Exception closing client %s: %s"%(client, e))
        
  def _shutdown_client(self, client):
    """
    Fill in docstring
    """
    self.send_client_command(client, {'cmd': 'shutdown'})
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
    clients_sent_work = {}
    for i in range(self.dirs_per_idle_client):
      for k in self.clients.keys():
        if self.clients.get(k, {})['state'] == 'idle':
          try:
            work_item = self.work_queue.popleft()
          except:
            self.log.debug("No directories queued for processing. Search for idle clients stopped")
            return
          self.log.debug("Sending idle client (%s) directory: %s"%(k.fileno(), work_item['path']))
          self.send_client_command(k, {'cmd': 'submit_work', 'paths': [work_item['path']]})
          clients_sent_work[k] = True
    # Semi-hack to track the fact we have sent work to an idle client 
    for k in clients_sent_work.keys():
      self.log.debug("Setting client (%s) to idle_queued_work"%k.fileno())
      self.clients[k]['state'] = 'idle_queued_work'
    
  def _send_ui(self, msg, flush=True):
    """
    Fill in docstring
    """
    self.log.log(5, "Sending to UI")
    HydraUtils.socket_send(self.svr_side_conn, msg)
      
  def _send_ui_stats(self, cmd={}):
    """
    Fill in docstring
    """
    send_stats = self.consolidate_stats()
    if cmd.get('data', 'simple') == 'simple':
      self.log.log(9, 'Sending UI consolidate stats: %s'%send_stats)
      self._send_ui({'cmd': 'stats', 'stats': send_stats})
    elif cmd.get('data') == 'individual_clients':
      for set in [self.clients, self.shutdown_clients]:
        for c in set:
          if not set[c]['stats']:
            continue
          self.log.log(9, 'Sending UI stats for client %s: %s'%(set[c]['addr'], set[c]['stats']))
          self._send_ui({'cmd': 'stats_individual_clients', 'client': set[c]['addr'], 'stats': set[c]['stats']})
    
  def _get_client_stats(self):
    """
    Fill in docstring
    """
    self.log.info('Server sending statistics request to clients')
    cmd = {'cmd': 'return_stats'}
    for c in self.clients:
      self.send_client_command(c, cmd)
    
  def _setup_server(self):
    """
    Fill in docstring
    """
    self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.server.bind((self.server_listen_addr, self.server_listen_port))
    if self.server_listen_port == 0:
      self.server_listen_port = self.server.getsockname()[1]
    self.server.listen(10)
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
    Current possible states:
      initializing
      idle
      processing
      shutdown_pending
      shutdown
    """
    old_state = self.state
    next_state = self.state
    idle = 0
    not_idle = 0
    shutdown = 0
    queued_work = len(self.work_queue)
    for c in self.clients:
      c_state = self.clients[c]['state']
      if c_state in ['initializing', 'connected', 'idle']:
        idle += 1
      elif c_state in ['shutdown', 'shutdown_pending']:
        shutdown += 1
      else: # ['processing']
        not_idle += 1
    self.log.debug("Clients Idle/Proc/Shutdown/Total: %d/%d/%d/%d, Work Q: %d"%(idle, not_idle, shutdown, self.num_clients, queued_work))
    
    if self.state in ['shutdown_pending', 'shutdown']:
      if self.num_clients == 0:
        next_state = 'shutdown'
    else:
      if not_idle > 0:
        # idle -> processing, processing -> processing
        next_state = 'processing'
      elif queued_work == 0:
        next_state = 'idle'
      else:
        self.log.debug('Server requires no state change')
        
    if old_state != next_state:
      if next_state == 'shutdown':
        self._send_ui_stats()
        if self.log.level >= logging.DEBUG:
          for set in [self.clients, self.shutdown_clients]:
            for c in set:
              if not set[c]['stats']:
                continue
              self.log.debug("Client (%s) stats: %s"%(set[c]['addr'], set[c]['stats']))
      self._set_server_state(next_state)
      
  def _heartbeat(self, idle_count):
    """
    Fill in docstring
    """
    self.log.debug("Heartbeat count: %d, idle count: %d"%(self.heartbeat_count, idle_count))
    # First check if we need to shutdown this client
    self.log.debug("Checking for shutdown state: %s"%self.state)
    if self.state == 'shutdown_pending' and idle_count > self.idle_shutdown_interval:
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
          self.send_client_command(c, {'cmd': 'return_work'})
      else:
        if num_idle_clients > 0:
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
      self.log.error("Unknown command format received from UI: %r"%cmd)
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
    elif command == 'update_settings':
      self.handle_update_settings(command)
    else:
      if not self.handle_extended_server_cmd(cmd):
        self.log.warn("Unhandled UI command: %s"%cmd)
      
  def _shutdown(self, type='normal'):
    """
    Fill in docstring
    """
    if type == 'immediate':
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
        addr=HydraUtils.DEFAULT_LISTEN_ADDR,
        port=HydraUtils.DEFAULT_LISTEN_PORT,
        args={},
        handler=None,
        ):
    super(HydraServerProcess, self).__init__()
    self.args = dict(args)
    self.handler = handler
    if handler:
      self.server = handler(args=self.args)
    else:
      self.server = HydraServer(args=self.args)
    self.listen_addr = addr
    self.listen_port = port
    self.server.set_server_connection(addr=self.listen_addr, port=self.listen_port)
  
  def fileno(self):
    return self.server.fileno()
    
  def init_process_logging(self):
    if len(logging.getLogger().handlers) == 0:
      if self.args.get('logger_cfg'):
        logging.config.dictConfig(self.args['logger_cfg'])
  
  def send(self, msg):
    self.server.send(msg)
    
  def recv(self):
    return self.server.recv()
    
  def run(self):
    self.init_process_logging()
    self.server.serve_forever()
