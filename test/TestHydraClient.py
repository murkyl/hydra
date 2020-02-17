# -*- coding: utf8 -*-
import inspect
import os
import sys
import random
import time
import multiprocessing
import logging
import socket
import select
import struct
import pickle
import zlib
import unittest

BASE_TEST_PATH = os.path.join('test', 'data')
RANDOM_SEED = 42
NUM_DIRS = 10
NUM_SUBDIRS = 10
FILES_PER_DIR = 10
FILE_SIZE = 1024*8
RANDOM_DATA_BUF_SIZE = 1024*1024*4
POLL_WAIT_SECONDS = 5
FILE_DELAY = 0.5
LOGGER_CONFIG = None

if __package__ is None:
    # test code is run from the ./test directory.  add the parent
    # directory to the path
    current_file = inspect.getfile(inspect.currentframe())
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
    sys.path.insert(0, base_path)
from HydraWorker import HydraWorker
from HydraClient import HydraClient
from HydraClient import HydraClientProcess
import HydraUtils

"""
This method creates files with random data in them using a single buffer
"""
def create_files(path, num, size, buffer = None, buf_size = 1024*1024, prefix = 'file', force_overwrite = False):
  if buffer is None:
    buffer = bytearray(random.getrandbits(8) for x in range(buf_size))
  for i in range(num):
    offset = random.randrange(buf_size)
    bytes_to_write = size
    if force_overwrite is False:
      try:
        file_lstat = os.lstat(os.path.join(path, '%s%d'%(prefix, i)))
        if file_lstat.st_size == size:
          continue
      except:
        pass
    if not os.path.isdir(path):
      os.makedirs(path)
    with open(os.path.join(path, '%s%d'%(prefix, i)), 'wb') as f:
      while bytes_to_write > 0:
        remainder = buf_size - offset
        if bytes_to_write < remainder:
          f.write(buffer[offset:(offset+bytes_to_write)])
          bytes_to_write = 0
        else:
          f.write(buffer[offset:buf_size])
          bytes_to_write -= remainder
    

class HydraTestClassSlowFileProcess(HydraWorker):
  def __init__(self, args={}):
    super(HydraTestClassSlowFileProcess, self).__init__(args)
    # Set a default delay of 0.5 seconds per file processed
    self.file_delay = FILE_DELAY
    
  def setFileDelay(self, delay_in_seconds):
    self.file_delay = delay_in_seconds
  
  def filter_subdirectories(self, root, dirs, files):
    """
    Fill in docstring
    """
    return dirs, files

  def handle_directory_pre(self, dir):
    """
    Fill in docstring
    """
    return False

  def handle_directory_post(self, dir):
    """
    Fill in docstring
    """
    return False

  def handle_file(self, dir, file):
    """
    Fill in docstring
    """
    if file == "skip_check":
      return False
    if self.file_delay > 0:
      time.sleep(self.file_delay)
    file_lstats = os.lstat(os.path.join(dir, file))
    #logging.getLogger().debug("Proc file: %s"%os.path.join(dir, file))
    return True
    
  def handle_extended_ops(self, data):
    if data.get('op') == 'setdelay':
      self.file_delay = data.get('payload')
    return True

class HydraTestClass(HydraWorker):
  def __init__(self, args={}):
    super(HydraTestClass, self).__init__(args)
    
  def filter_subdirectories(self, root, dirs, files):
    """
    Fill in docstring
    """
    return dirs, files

  def handle_directory_pre(self, dir):
    """
    Fill in docstring
    """
    return False

  def handle_directory_post(self, dir):
    """
    Fill in docstring
    """
    return False

  def handle_file(self, dir, file):
    """
    Fill in docstring
    """
    if file == "skip_check":
      return False
    file_lstats = os.lstat(os.path.join(dir, file))
    #logging.getLogger().debug("Proc file: %s"%os.path.join(dir, file))
    return True
    
class TestHydraClient(unittest.TestCase):
  def setUp(self):
    self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.server.bind(('127.0.0.1', 0))
    self.server_port = self.server.getsockname()[1]
    self.server.listen(1)
    #print("Local port: %d"%self.server_port)
  
    self.buffer_size = RANDOM_DATA_BUF_SIZE
    random.seed(RANDOM_SEED)
    self.base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
    self.test_path = os.path.join(self.base_path, BASE_TEST_PATH)
    # Check for skip file named 'skip_check' and bypass creation/check if it is present
    if os.path.isfile(os.path.join(self.test_path, 'skip_check')):
      return
    logging.getLogger().info("Setting up file structure. This may take time")
    self.rand_buffer = bytearray(random.getrandbits(8) for x in range(self.buffer_size))
    for i in range(NUM_DIRS):
      cur_path = os.path.join(self.test_path, "dir%s"%i)
      try:
        os.makedirs(cur_path, exists_ok = True)
      except:
        pass
      create_files(cur_path, FILES_PER_DIR, FILE_SIZE, self.rand_buffer, self.buffer_size)
      for j in range(NUM_SUBDIRS):
        sub_path = os.path.join(cur_path, "subdir%s"%j)
        try:
          os.makedirs(sub_path)
        except:
          pass
        create_files(sub_path, FILES_PER_DIR, FILE_SIZE)

  def tearDown(self):
    #print("tearDown called")
    try:
      self.server.close()
      self.server = None
    except:
      pass
    self.rand_buffer = None
    self = None
  
  def send_client_msg(self, data, conn):
    bytes_data = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
    bytes_len = struct.pack('!L', len(bytes_data))
    conn.send(bytes_len + bytes_data)
    
  def recv_server_msg(self, conn):
    data_bytes = conn.recv(4)
    if len(data_bytes) == 0:
      raise EOFError
    data_len = struct.unpack('!L', data_bytes)[0]
    data = conn.recv(data_len)
    return pickle.loads(data)

  #@unittest.skip("")
  def test_1_spawn_client_and_shutdown(self):
    '''
    This test closes the connection abruptly to the client
    It should cause some socket closed errors
    '''
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClassSlowFileProcess, 'logger_cfg': LOGGER_CONFIG})
    client.set_workers(1)
    client.start()
    connection = None
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      logging.getLogger().debug("Sleep 2 seconds before closing connection")
      time.sleep(2)
      connection.close()
      logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
      client.join(10)
      try:
        self.assertFalse(client.is_alive())
      except:
        client.terminate()
        raise
    except Exception as e:
      logging.getLogger().exception(e)
      if connection:
        connection.close()

  #@unittest.skip("")
  def test_2_client_process_single_dir(self):
    '''
    This test closes the connection abruptly to the client
    It should cause some socket closed errors
    '''
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClassSlowFileProcess, 'logger_cfg': LOGGER_CONFIG})
    client.start()
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0', 'subdir0')]}
      self.send_client_msg(cmd, connection)
      logging.getLogger().debug("Sleep 5 seconds before closing connection")
      time.sleep(5)
      connection.close()
      logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
      client.join(10)
      try:
        self.assertFalse(client.is_alive())
      except:
        client.terminate()
        raise
    except Exception as e:
      logging.getLogger().exception(e)
      if connection:
        connection.close()
  
  #@unittest.skip("")
  def test_3_client_process_multiple_simple_dir(self):
    '''
    This test closes the connection abruptly to the client
    It should cause some socket closed errors
    '''
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClassSlowFileProcess, 'logger_cfg': LOGGER_CONFIG})
    client.start()
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0', 'subdir0')]}
      self.send_client_msg(cmd, connection)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0', 'subdir1')]}
      self.send_client_msg(cmd, connection)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0', 'subdir2')]}
      self.send_client_msg(cmd, connection)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0', 'subdir3')]}
      self.send_client_msg(cmd, connection)
      logging.getLogger().debug("Sleep 5 seconds before closing connection")
      time.sleep(5)
      connection.close()
      logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
      client.join(10)
      try:
        self.assertFalse(client.is_alive())
      except:
        client.terminate()
        raise
    except:
      if connection:
        connection.close()
  
  #@unittest.skip("")
  def test_4_dir_with_subdirs_splitting_work(self):
    '''
    This test aborts the run in the middle of processing. This causes will very
    likely cause a partially processed directory to be returned.
    
    '''
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClassSlowFileProcess, 'logger_cfg': LOGGER_CONFIG})
    client.set_workers(2)
    client.start()
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      inputs = [connection]
      for i in range(2):
        logging.getLogger().debug("Waiting idle state")
        # Wait for stats update before client shutdown
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
        for s in  readable:
          data = self.recv_server_msg(connection)
          self.assertIsNot(data, False)
          if data.get('cmd') == 'state':
            self.assertEqual(data.get('state'), ['connected', 'idle'][i])

      for i in range(0, 3):
        cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, "dir%d"%i)]}
        self.send_client_msg(cmd, connection)
      logging.getLogger().debug("Wait up to 40 seconds to allow for file processing")
      start = time.time()
      found_stats = False
      for i in range(50):
        if (time.time() - start) >= 40:
          break
        logging.getLogger().debug("Waiting for stats update")
        # Wait for stats update before client shutdown
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
        for s in readable:
          data = self.recv_server_msg(s)
          logging.getLogger().debug("Received interim message: :%s"%data)
          self.assertIsNot(data, False)
          if data.get('cmd') == 'stats':
            found_stats = True
            break
        if found_stats:
          break
      
      logging.getLogger().debug("Shutting down client")
      cmd = {'cmd': 'shutdown'}
      self.send_client_msg(cmd, connection)
      
      found_stats = False
      for i in range(15):
        logging.getLogger().debug("Waiting for stats update and shutdown")
        # Wait for stats update before client shutdown
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
        for s in  readable:
          try:
            data = self.recv_server_msg(connection)
          except EOFError:
            break
          self.assertIsNot(data, False)
          cmd = data.get('cmd')
          self.assertTrue(cmd is not None, msg='Invalid command from worker.')
          if cmd == 'stats':
            found_stats = True
          elif cmd == 'state':
            if data.get('state') == 'shutdown':
              break
      self.assertTrue(found_stats, msg='Expected to receive some stats from workers. None received.')

      logging.getLogger().debug("Closing server to client connection")
      connection.close()
      logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
      client.join(10)
      try:
        self.assertFalse(client.is_alive(), msg='Client is still alive')
      except:
        client.terminate()
        raise
    except KeyboardInterrupt:
      if connection:
        connection.close()
        client.terminate()
    except Exception as e:
      logging.getLogger().exception(e)
      if connection:
        connection.close()
      
  #@unittest.skip("")
  def test_5_periodic_stats_query(self):
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClassSlowFileProcess, 'logger_cfg': LOGGER_CONFIG})
    num_workers = 2
    client.set_workers(num_workers)
    client.start()
    inputs = []
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      inputs.append(connection)
      for i in range(0,3):
        cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir%d'%i)]}
        self.send_client_msg(cmd, connection)
      logging.getLogger().debug("Sleep 10 seconds to allow for file processing")
      time.sleep(10)
      
      stats_to_find = 2
      cmd = {'cmd': 'return_stats'}
      self.send_client_msg(cmd, connection)
      # Wait for multiple stats update before client shutdown
      for i in range(10):
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
        for s in readable:
          self.assertIsNot(len(readable), 0)
          data = self.recv_server_msg(s)
          self.assertIsNot(data, False)
          if data.get('cmd') == 'stats':
            # Request another stats update
            cmd = {'cmd': 'return_stats'}
            self.send_client_msg(cmd, s)
            stats_to_find -= 1
            if stats_to_find <= 0:
              break
        if stats_to_find <= 0:
          break
      self.assertTrue(stats_to_find <= 0)

      # Send shutdown
      logging.getLogger().debug("Waiting for shutdown")
      cmd = {'cmd': 'shutdown'}
      self.send_client_msg(cmd, connection)
      shutdown_found = 0
      for i in range(20):
        
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*4)
        if readable:
          data = self.recv_server_msg(connection)
          self.assertIsNot(data, False)
          if data.get('cmd') == 'state' and data.get('state') == 'shutdown':
            shutdown_found = 1
            break
      self.assertTrue(shutdown_found >= 1)
      
      logging.getLogger().debug("Closing server to client connection")
      connection.close()
      logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
      client.join(10)
      try:
        self.assertFalse(client.is_alive())
      except:
        client.terminate()
        raise
    except Exception as e:
      logging.getLogger().exception(e)
      if connection:
        connection.close()
      
  #@unittest.skip("")
  def test_6_full_run_2_workers(self):
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClass, 'logger_cfg': LOGGER_CONFIG})
    num_workers = 2
    client.set_workers(num_workers)
    client.start()
    inputs = []
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      inputs.append(connection)
      
      complete = False
      # Wait for client to report idle
      for i in range(100):
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
        for s in readable:
          self.assertIsNot(len(readable), 0)
          data = self.recv_server_msg(s)
          self.assertIsNot(data, False)
          if data.get('cmd') == 'state':
            if data.get('state') == 'idle':
              complete = True
              break
        if complete:
          break

      for i in range(0,10):
        cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir%d'%i)]}
        self.send_client_msg(cmd, connection)

      complete = False
      # Wait for client to report idle
      for i in range(100):
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
        for s in readable:
          self.assertIsNot(len(readable), 0)
          data = self.recv_server_msg(s)
          self.assertIsNot(data, False)
          if data.get('cmd') == 'state':
            if data.get('state') == 'idle' and data.get('prev_state') != 'connected':
              complete = True
          elif data.get('cmd') == 'stats':
            logging.getLogger().debug("Stats: %s"%data.get('stats'))
        if complete:
          break

      # Send shutdown
      logging.getLogger().debug("Waiting for shutdown")
      cmd = {'cmd': 'shutdown'}
      self.send_client_msg(cmd, connection)
      
      shutdown_found = 0
      for i in range(20):
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*4)
        if readable:
          data = self.recv_server_msg(connection)
          self.assertIsNot(data, False)
          if data.get('cmd') == 'state' and data.get('state') == 'shutdown':
            shutdown_found = 1
            break
          elif data.get('cmd') == 'stats':
            self.assertEqual(1100, data['stats']['processed_files'])
            self.assertEqual(110, data['stats']['processed_dirs'])
          else:
            logging.getLogger().debug("Other msg: %s"%data)

      self.assertTrue(shutdown_found >= 1)
      
      logging.getLogger().debug("Closing server to client connection")
      connection.close()
      logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
      client.join(10)
      try:
        self.assertFalse(client.is_alive())
      except:
        client.terminate()
        raise
    except Exception as e:
      logging.getLogger().exception(e)
      if connection:
        connection.close()
        
  #@unittest.skip("")
  def test_7_full_run_20_workers(self):
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClass, 'logger_cfg': LOGGER_CONFIG})
    num_workers = 20
    client.set_workers(num_workers)
    client.start()
    inputs = []
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      inputs.append(connection)
      
      complete = False
      # Wait for client to report idle
      for i in range(100):
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
        for s in readable:
          self.assertIsNot(len(readable), 0)
          data = self.recv_server_msg(s)
          self.assertIsNot(data, False)
          if data.get('cmd') == 'state':
            if data.get('state') == 'idle':
              complete = True
              break
        if complete:
          break

      for i in range(0,10):
        cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir%d'%i)]}
        self.send_client_msg(cmd, connection)

      complete = False
      # Wait for client to report idle
      for i in range(100):
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
        for s in readable:
          self.assertIsNot(len(readable), 0)
          data = self.recv_server_msg(s)
          self.assertIsNot(data, False)
          if data.get('cmd') == 'state':
            logging.getLogger().debug("Client sent state update: %s"%data)
            if (data.get('state') == 'idle') and (data.get('prev_state') == 'processing'):
              logging.getLogger().debug("Client transitioned to idle from %s"%data.get('prev_state'))
              complete = True
          elif data.get('cmd') == 'stats':
            logging.getLogger().debug("Stats: %s"%data.get('stats'))
        if complete:
          break

      # Send shutdown
      logging.getLogger().debug("Waiting for shutdown")
      cmd = {'cmd': 'shutdown'}
      self.send_client_msg(cmd, connection)
      
      shutdown_found = 0
      for i in range(20):
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*4)
        if readable:
          data = self.recv_server_msg(connection)
          self.assertIsNot(data, False)
          if data.get('cmd') == 'state' and data.get('state') == 'shutdown':
            shutdown_found = 1
            break
          elif data.get('cmd') == 'stats':
            self.assertEqual(1100, data['stats']['processed_files'])
            self.assertEqual(110, data['stats']['processed_dirs'])
          else:
            logging.getLogger().debug("Other msg: %s"%data)

      self.assertTrue(shutdown_found >= 1)
      
      logging.getLogger().debug("Closing server to client connection")
      connection.close()
      logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
      client.join(10)
      try:
        self.assertFalse(client.is_alive())
      except:
        client.terminate()
        raise
    except Exception as e:
      logging.getLogger().exception(e)
      if connection:
        connection.close()

if __name__ == '__main__':
  debug_count = sys.argv.count('--debug')
  log_lvl = logging.WARN
  if debug_count > 2:
    log_lvl = 5
  elif debug_count > 1:
    log_lvl = 9
  elif debug_count > 0:
    log_lvl = logging.DEBUG
  LOGGER_CONFIG = dict(HydraUtils.LOGGING_CONFIG)
  HydraUtils.config_logger(LOGGER_CONFIG, '', log_level=log_lvl)
  logging.config.dictConfig(LOGGER_CONFIG)
  root = logging.getLogger('')

  suite1 = unittest.TestLoader().loadTestsFromTestCase(TestHydraClient)
  all_tests = unittest.TestSuite([suite1])
  unittest.TextTestRunner(verbosity=2).run(all_tests)
  