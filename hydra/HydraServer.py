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
from . import HydraClient
from . import HydraWorker
from . import HydraUtils

# Possible state machine states. Sub states are split using the _ character
STATE_IDLE = 'idle'
STATE_INIT = 'init'
STATE_PAUSED = 'paused'
STATE_PROCESSING = 'processing'
STATE_SHUTDOWN = 'shutdown'
STATE_SHUTDOWN_PENDING = 'shutdown_pending'

EVENT_CLIENT_CONNECTED = 'client_connected'
EVENT_CLIENT_REQ_WORK = HydraClient.CMD_CLIENT_REQUEST_WORK
EVENT_CLIENT_STATE = HydraClient.CMD_CLIENT_STATE
EVENT_CLIENT_STATS = HydraClient.CMD_CLIENT_STATS
EVENT_CLIENT_WORK = HydraClient.CMD_CLIENT_WORK
EVENT_HEARTBEAT = 'heartbeat'
EVENT_INIT_COMPLETE = 'init_complete'
EVENT_PAUSE = 'pause'
EVENT_QUERY_STATS = 'stats'
EVENT_RESUME = 'resume'
EVENT_SHUTDOWN = 'shutdown'
EVENT_SHUTDOWN_COMPLETE = 'shutdown_complete'
EVENT_SUBMIT_WORK = 'submit_work'
EVENT_UPDATE_SETTINGS = 'update_settings'

# Commands sent from the server to the UI
CMD_SVR_STATE = 'state'
CMD_SVR_STATS = 'stats'
CMD_SVR_STATS_INDIVIDUAL = 'stats_individual'

HYDRA_SERVER_STATE_TABLE = {
  STATE_INIT: {
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_INIT_COMPLETE:    {'a': '_h_no_op',               'ns': STATE_IDLE},
      EVENT_SHUTDOWN:         {'a': '_h_no_op',               'ns': STATE_SHUTDOWN_PENDING},
  },
  STATE_IDLE: {
      EVENT_CLIENT_CONNECTED: {'a': '_h_client_connect',      'ns': None},
      EVENT_CLIENT_REQ_WORK:  {'a': '_h_client_req_work',     'ns': None},
      EVENT_CLIENT_STATE:     {'a': '_h_client_state',        'ns': None},
      EVENT_CLIENT_STATS:     {'a': '_h_client_stats',        'ns': None},
      EVENT_CLIENT_WORK:      {'a': '_h_client_work',         'ns': None},
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_SUBMIT_WORK:      {'a': '_h_submit_work',         'ns': STATE_PROCESSING},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN_PENDING},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
  },
  STATE_PAUSED: {
      EVENT_CLIENT_CONNECTED: {'a': '_h_client_connect',      'ns': None},
      EVENT_CLIENT_REQ_WORK:  {'a': '_h_client_req_work',     'ns': None},
      EVENT_CLIENT_STATE:     {'a': '_h_client_state',        'ns': None},
      EVENT_CLIENT_STATS:     {'a': '_h_client_stats',        'ns': None},
      EVENT_CLIENT_WORK:      {'a': '_h_client_work',         'ns': None},
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_RESUME:           {'a': '_h_no_op',               'ns': STATE_PROCESSING},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_SUBMIT_WORK:      {'a': '_h_submit_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN_PENDING},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
  },
  STATE_PROCESSING: {
      EVENT_CLIENT_CONNECTED: {'a': '_h_client_connect',      'ns': None},
      EVENT_CLIENT_REQ_WORK:  {'a': '_h_client_req_work',     'ns': None},
      EVENT_CLIENT_STATE:     {'a': '_h_client_state',        'ns': None},
      EVENT_CLIENT_STATS:     {'a': '_h_client_stats',        'ns': None},
      EVENT_CLIENT_WORK:      {'a': '_h_client_work',         'ns': None},
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_PAUSE:            {'a': '_h_no_op',               'ns': STATE_PAUSED},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_SUBMIT_WORK:      {'a': '_h_submit_work',         'ns': None},
      EVENT_SHUTDOWN:         {'a': '_h_shutdown',            'ns': STATE_SHUTDOWN_PENDING},
      EVENT_SHUTDOWN_COMPLETE:{'a': '_h_no_op',               'ns': STATE_SHUTDOWN},
      EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',     'ns': None},
  },
  STATE_SHUTDOWN: {
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_SHUTDOWN_COMPLETE:{'a': '_h_no_op',               'ns': None},
  },
  STATE_SHUTDOWN_PENDING: {
      EVENT_HEARTBEAT:        {'a': '_h_heartbeat',           'ns': None},
      EVENT_CLIENT_REQ_WORK:  {'a': '_h_no_op',               'ns': None},
      EVENT_CLIENT_STATE:     {'a': '_h_client_state',        'ns': None},
      EVENT_CLIENT_STATS:     {'a': '_h_client_stats',        'ns': None},
      EVENT_QUERY_STATS:      {'a': '_h_query_stats',         'ns': None},
      EVENT_SHUTDOWN_COMPLETE:{'a': '_h_no_op',               'ns': STATE_SHUTDOWN},
  },
}

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
    self.state = STATE_INIT
    self.state_table = {}
    self.event_queue = []
    self.work_queue = deque()
    self.init_stats(self.stats)
    self._init_state_table(HYDRA_SERVER_STATE_TABLE)
    self.work_paths = []
    self.log.critical("SVR CRIT")
    
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

  def handle_extended_client_cmd(self, event, data, src):
    """
    Called by the main loop when an unknown command is found
    This can be used to support custom commands from a HydraClient without
    having to re-write the operation parser. This can be overridden as necessary
    when subclassing :class:`HydraServer <multiprocessing.Process>`.
    
    Return True if the command was handled
    Return False if the command was not handled. This is the default.
    """
    return False
    
  def handle_extended_ui_cmd(self, event, data, src):
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
    
  def send(self, cmd, msg):
    """
    Fill in docstring
    """
    try:
      HydraUtils.socket_send(self.ui_side_conn, {'cmd': cmd, 'msg': msg})
    except Exception as e:
      self.log.exception(e)
      return False
    return True
    
  def send_all_clients_command(self, cmd, msg=None):
    for c in self.clients:
      self.send_client_command(c, cmd, msg)
    
  def send_client_command(self, client, cmd, msg=None):
    """
    Fill in docstring
    """
    c = self.clients.get(client)
    if not msg:
      data = {}
    else:
      data = dict(msg)
    if c:
      try:
        data['op'] = cmd
        HydraUtils.socket_send(c['conn'], data)
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
    idle_count = 0
    start_time = time.time()
    if not self.server:
      self._setup_server()
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
          self.event_queue.append({'c': EVENT_HEARTBEAT, 'd': {'idle_count': idle_count, 'hb_count': self.heartbeat_count, 'src': 'server'}})
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
            self.event_queue.append({'c': EVENT_CLIENT_CONNECTED, 'd': s, 'src': {'type': 'server', 'id': s}})
          elif s is self.svr_side_conn:
            # Command received from the UI
            try:
              msg_size = s.recv(4)
              if len(msg_size) != 4:
                self.log.debug("Closing server handle due to EOF of command stream: %r"%s)
                self._shutdown()
                continue
              data_len = struct.unpack('!L', msg_size)[0]
              data = HydraUtils.socket_recv(s, data_len)
              self.event_queue.append({'c': data.get('cmd'), 'd': data.get('msg'), 'src': {'type': 'ui'}})
            except EOFError as eof:
              self.log.debug("Closing UI handle due to EOF of command stream: %r"%s)
              self._shutdown()
            except socket.error as serr:
              if serr.errno != errno.ECONNRESET:
                raise
              self.log.debug("Closing UI handle due to EOF of command stream: %r"%s)
              self._shutdown()
            except Exception:
              self.log.exception("Exception encountered during command processing.")
              self._shutdown()
          else:
            # Command received from a client
            try:
              self.log.log(9, "CMD from client: %s"%s)
              msg_size = s.recv(4)
              if len(msg_size) != 4:
                self._cleanup_client(s)
                continue
              data_len = struct.unpack('!L', msg_size)[0]
              data = HydraUtils.socket_recv(s, data_len)
              self.event_queue.append({'c': data.get('cmd'), 'd': data.get('msg'), 'src': {'type': 'client', 'id': s}})
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
          self._process_state_event(entry['c'], entry.get('d'), entry.get('src', {'type': 'server'}))
        except Exception as e:
          self.log.exception(e)
          break
    self.log.debug("Server exiting")
    self._shutdown_cleanup()
  
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
  
  def _connect_client(self):
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
    return(client_obj)

  def _process_client_state(self, client, state):
    """
    Fill in docstring
    """
    old_state = self.clients[client]['state']
    if old_state == state:
      self.log.debug("No state change for client %s in state: %s"%(client.fileno(), state))
      return state
    self.clients[client]['state'] = state
    self.log.debug("Client %s state change %s -> %s"%(client.fileno(), old_state, state))
    if state == HydraClient.STATE_SHUTDOWN:
      self._cleanup_client(client)
      if len(self.clients) < 1:
        if self.state == STATE_SHUTDOWN_PENDING:
          self.event_queue.append({'c': EVENT_SHUTDOWN_COMPLETE, 'd': None})
        else:
          self.event_queue.append({'c': EVENT_SHUTDOWN, 'd': None})
    return old_state
    
  def _process_returned_work(self, work_items=[]):
    """
    Fill in docstring
    """
    for i in work_items:
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
      self.shutdown_clients[key] = c
      try:
        c['conn'].close()
        client.close()
      except Exception as e:
        self.log.debug("Exception closing client %s: %s"%(client, e))
        
  def _shutdown_client(self, client):
    """
    Fill in docstring
    """
    self.send_client_command(client, HydraClient.EVENT_SHUTDOWN)
    self.log.debug("Shutdown command sent to: %s"%client)
    
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
  
  def _process_dirs(self, client_key=None):
    """
    Fill in docstring
    """
    for i in range(self.dirs_per_idle_client):
      for k in self.clients.keys():
        if self.clients.get(k, {}).get('state') in [HydraClient.STATE_PROCESSING_WAITING, HydraClient.STATE_IDLE_WAITING] or k == client_key:
          try:
            work_item = self.work_queue.popleft()
          except:
            self.log.debug("No directories queued for processing. Search for idle clients stopped")
            return
          self.log.debug("Sending waiting client (%s) directory: %s"%(k.fileno(), work_item['path']))
          self.send_client_command(k, HydraClient.EVENT_SUBMIT_WORK, {'paths': [work_item['path']]})
          self._process_client_state(k, 'queued_work')
  
  def _check_end_of_processing(self):
    """
    """
    idle_clients = []
    other_clients = []
    for k in self.clients.keys():
      client_state = self.clients.get(k, {}).get('state')
      if client_state in [HydraClient.STATE_IDLE_WAITING, HydraClient.STATE_PROCESSING_WAITING]:
        idle_clients.append(k)
      else:
        other_clients.append(k)
    if len(other_clients) == 0 and self.num_clients > 0:
      self.log.debug("No active clients. Send NO_WORK to all clients")
      self.send_all_clients_command(HydraClient.EVENT_NO_WORK)
      if self.state == STATE_PROCESSING and len(other_clients) == 0:
        self._set_state(STATE_IDLE)
    else:
      self._get_client_work_items()
    
  def _send_ui(self, cmd, msg, flush=True):
    """
    Fill in docstring
    """
    self.log.log(5, "Sending to UI")
    HydraUtils.socket_send(self.svr_side_conn, {'cmd': cmd, 'msg': msg})
      
  def _send_ui_stats(self, type='normal'):
    """
    Fill in docstring
    """
    send_stats = self.consolidate_stats()
    if type == 'normal':
      self.log.log(9, 'Sending UI consolidate stats: %s'%send_stats)
      self._send_ui(CMD_SVR_STATS, {'stats': send_stats})
    elif type == 'individual':
      for set in [self.clients, self.shutdown_clients]:
        for c in set:
          if not set[c]['stats']:
            continue
          self.log.log(9, 'Sending UI stats for client %s: %s'%(set[c]['addr'], set[c]['stats']))
          self._send_ui(CMD_SVR_STATS_INDIVIDUAL, {'client': set[c]['addr'], 'stats': set[c]['stats']})
    
  def _get_client_stats(self):
    """
    Fill in docstring
    """
    self.log.info('Server sending statistics request to clients')
    for c in self.clients:
      self.send_client_command(c, HydraClient.EVENT_QUERY_STATS)
   
  def _get_client_idle_active(self):
    active_clients = []
    idle_clients = []
    for k in self.clients.keys():
      client_state = self.clients.get(k, {}).get('state')
      if client_state in [HydraClient.STATE_PROCESSING, HydraClient.STATE_PROCESSING_PAUSED]:
        active_clients.append(k)
      else:
        idle_clients.append(k)
    return idle_clients, active_clients
      
  def _get_client_work_items(self, forced=False):
    # Check if we have idle clients. If we do query any active clients and have
    # them return work items
    try:
      self.log.debug("Checking for idle clients")
      idle_clients, active_clients = self._get_client_idle_active()
      if len(self.work_queue) < len(idle_clients) or forced:
        for c in active_clients:
          self.log.debug("Requesting active client to return work: %s"%c)
          self.send_client_command(c, HydraClient.EVENT_RETURN_WORK)
      else:
        self.log.debug("Existing work queue items (%d) sufficient to send to idle clients (%d)"%(len(self.work_queue), len(idle_clients)))
    except Exception as e:
      self.log.exception(e)
    
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
    self.event_queue.insert(0, {'c': EVENT_INIT_COMPLETE, 'd': None})
    
  def _heartbeat(self, idle_count):
    """
    Fill in docstring
    """
    self.log.debug("Heartbeat count: %d, idle count: %d"%(self.heartbeat_count, idle_count))
    # First check if we need to shutdown this client
    self.log.debug("Checking for shutdown state: %s"%self.state)
    if self.state == STATE_SHUTDOWN_PENDING and idle_count > self.idle_shutdown_interval:
      self.event_queue.append({'c': EVENT_SHUTDOWN_COMPLETE, 'd': None})
      return
    self._get_client_work_items()
    # If we have any unprocessed work items, send them to the clients
    self.log.debug("Distribute work, items in queue: %d"%len(self.work_queue))
    self._process_dirs()
    # Request client stats if required
    if self.heartbeat_count%self.stats_heartbeat_interval == 0:
      self._get_client_stats()
      self._send_ui_stats()
  
  def _shutdown(self):
    """
    Fill in docstring
    """
    if self.clients:
      for c in self.clients:
        self.log.debug("Shutting down client: %s"%c)
        self._shutdown_client(c)
    else:
      self.event_queue.append({'c': EVENT_SHUTDOWN_COMPLETE, 'd': None})
  
  def _shutdown_cleanup(self):
    try:
      self.svr_side_conn.shutdown(socket.SHUT_RDWR)
      self.svr_side_conn.close()
    except:
      pass
    self.svr_side_conn = None
    try:
      self.ui_side_conn.shutdown(socket.SHUT_RDWR)
      self.ui_side_conn.close()
    except:
      pass
    self.ui_side_conn = None
    try:
      self.server.shutdown(socket.SHUT_RDWR)
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
    
  def _process_state_event(self, event, data=None, src={'type': 'server'}):
    """
    Fill in docstring
    """
    table = self.state_table[self.state]
    handler = table.get(event)
    if handler:
      self.log.debug("Server handling event '%s' @ '%s' with '%s'"%(event, self.state, handler['a']))
      next_state = handler['a'](event, data, handler.get('ns'), src)
      self._set_state(next_state or handler.get('ns'))
    else:
      if src.get('type') == 'client':
        if not self.handle_extended_client_cmd(event, data, src):
          self.log.critical("Unhandled client event (%s) received in '%s' state."%(event, self.state))
      else:
        if not self.handle_extended_ui_cmd(event, data, src):
          self.log.critical("Unhandled UI event (%s) received in '%s' state."%(event, self.state))
    
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
        self._send_ui_stats()
      self.state = state
      self._send_ui(CMD_SVR_STATE, {'state': self.state, 'prev_state': old_state})
      self.log.debug('Server state change: %s => %s'%(old_state, state))
    return old_state
    
  def _h_no_op(self, event, data, next_state, src):
    return next_state
    
  def _h_client_connect(self, event, data, next_state, src):
    client = self._connect_client()
    self.handle_client_connected(client['conn'])
    return next_state
    
  def _h_client_req_work(self, event, data, next_state, src):
    self._process_dirs()
    return next_state

  def _h_client_state(self, event, data, next_state, src):
    client_id = src.get('id')
    client_state = data.get('state')
    old_state = self._process_client_state(client_id, client_state)
    # If a client just transitioned out of the CONNECTED state we will try and
    # send any work we have queued up already immediately.
    # If a client is transitioning into the PROCESSING_WAITING state, that means
    # the client is waiting for more work. Send work immediately if any is
    # available.
    if old_state in [HydraClient.STATE_CONNECTED] or client_state in [HydraClient.STATE_PROCESSING_WAITING]:
      self._process_dirs(client_id)
    self._check_end_of_processing()
    return next_state

  def _h_client_stats(self, event, data, next_state, src):
    self._process_client_stats(src.get('id'), data)
    return next_state

  def _h_client_work(self, event, data, next_state, src):
    self._process_returned_work(data.get('work_items'))
    return next_state
    
  def _h_heartbeat(self, event, data, next_state, src):
    self._heartbeat(data['idle_count'])
    return next_state

  def _h_query_stats(self, event, data, next_state, src):
    """
      data: Valid values are None, 'normal' or 'individual'
    """
    self._send_ui_stats(type=data.get('type', 'normal'))
    return next_state

  def _h_shutdown(self, event, data, next_state, src):
    self._shutdown()
    return next_state

  def _h_submit_work(self, event, data, next_state, src):
    proc_paths = data.get('paths', [])
    self._queue_work_paths(proc_paths)
    self._process_dirs()
    if not isinstance(proc_paths, (list, tuple)):
      proc_paths = [proc_paths]
    for path in proc_paths:
      if not path:
        continue
      self.work_paths.append(path)
    return next_state
    
  def _h_update_settings(self, event, data, next_state, src):
    self.handle_update_settings(data)
    return next_state


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
    if not len(logging.getLogger().handlers):
      if self.args.get('logger_cfg'):
        logging.config.dictConfig(self.args['logger_cfg'])
  
  def send(self, cmd, msg=None):
    self.server.send(cmd, msg)
    
  def recv(self):
    return self.server.recv()
    
  def run(self):
    self.init_process_logging()
    self.server.serve_forever()
