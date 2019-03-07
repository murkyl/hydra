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

if __package__ is None:
    # test code is run from the ./test directory.  add the parent
    # directory to the path so that we can see all the isi_ps code.
    current_file = inspect.getfile(inspect.currentframe())
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
    sys.path.insert(0, base_path)
    from HydraWorker import HydraWorker
    from HydraClient import HydraClient
    from HydraClient import HydraClientProcess

"""
This method creates files with random data in them using a single buffer
"""
def create_files(path, num, size, buffer = None, buf_size = 1024*1024, prefix = 'file', force_overwrite = False):
  if buffer is None:
    buffer = bytearray(random.getrandbits(8) for x in xrange(buf_size))
  for i in xrange(num):
    offset = random.randrange(buf_size)
    bytes_to_write = size
    if force_overwrite is False:
      try:
        file_lstat = os.lstat(os.path.join(path, '%s%d'%(prefix, i)))
        if file_lstat.st_size == size:
          continue
      except:
        pass
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

'''    
class HydraClientProcess(multiprocessing.Process):
  def __init__(self, args):
    super(HydraClientProcess, self).__init__()
    self.server_addr = args['svr']
    self.server_port = args['port']
    self.client = None
    
  def run(self):
    self.client = HydraClient(HydraTestClassSlowFileProcess)
    self.client.connect(self.server_addr, self.server_port)
    #self.client.set_workers(4)
    self.client.serve_forever()
    
  def shutdown(self):
    self.client.shutdown()
'''
class TestHydraClient(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cls.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    cls.server.bind(('127.0.0.1', 0))
    cls.server_port = cls.server.getsockname()[1]
    cls.server.listen(1)
    #print("Local port: %d"%cls.server_port)
  
    cls.buffer_size = RANDOM_DATA_BUF_SIZE
    random.seed(RANDOM_SEED)
    cls.base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
    cls.test_path = os.path.join(cls.base_path, BASE_TEST_PATH)
    # Check for skip file named 'skip_check' and bypass creation/check if it is present
    if os.path.isfile(os.path.join(cls.test_path, 'skip_check')):
      return
    logging.getLogger().info("Setting up file structure. This may take time")
    cls.rand_buffer = bytearray(random.getrandbits(8) for x in xrange(cls.buffer_size))
    for i in xrange(NUM_DIRS):
      cur_path = os.path.join(cls.test_path, "dir%s"%i)
      try:
        os.makedirs(cur_path, exists_ok = True)
      except:
        pass
      create_files(cur_path, FILES_PER_DIR, FILE_SIZE, cls.rand_buffer, cls.buffer_size)
      for j in xrange(NUM_SUBDIRS):
        sub_path = os.path.join(cur_path, "subdir%s"%j)
        try:
          os.makedirs(sub_path)
        except:
          pass
        create_files(sub_path, FILES_PER_DIR, FILE_SIZE)

  @classmethod
  def tearDownClass(cls):
    print("tearDownClass called")
    try:
      cls.server.close()
      cls.server = None
    except:
      pass
    cls.rand_buffer = None
    cls = None

  #@unittest.skip("")
  def test_1_spawn_client_and_shutdown(self):
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClassSlowFileProcess})
    client.start()
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      conn_handle = connection.makefile('wb+')
      conn_handle.flush()
      logging.getLogger().debug("Sleep 2 seconds before closing connection")
      time.sleep(2)
      conn_handle.close()
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
  def test_2_client_process_single_dir(self):
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClassSlowFileProcess})
    client.start()
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      conn_handle = connection.makefile('wb+')
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0', 'subdir0')]}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      conn_handle.flush()
      logging.getLogger().debug("Sleep 20 seconds before closing connection")
      time.sleep(20)
      conn_handle.close()
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
  def test_3_client_process_multiple_simple_dir(self):
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClassSlowFileProcess})
    client.start()
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      conn_handle = connection.makefile('wb+')
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0', 'subdir0')]}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0', 'subdir1')]}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0', 'subdir2')]}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0', 'subdir3')]}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      conn_handle.flush()
      logging.getLogger().debug("Sleep 20 seconds before closing connection")
      time.sleep(20)
      conn_handle.close()
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
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClassSlowFileProcess})
    client.start()
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      conn_handle = connection.makefile('wb+')
      inputs = [connection]
      for i in range(2):
        logging.getLogger().debug("Waiting idle state")
        # Wait for stats update before client shutdown
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
        for s in  readable:
          data = pickle.load(conn_handle)
          self.assertIsNot(data, False)
          self.assertEqual(data.get('state'), ['connected', 'idle'][i])

      for d in ['dir0', 'dir1', 'dir2', 'dir3']:
        cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, d)]}
        pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      conn_handle.flush()
      logging.getLogger().debug("Wait up to 40 seconds to allow for file processing")
      start = time.time()
      for i in range(40):
        if (time.time() - start) >= 40:
          break
        logging.getLogger().debug("Waiting for stats update")
        # Wait for stats update before client shutdown
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
        for s in  readable:
          data = pickle.load(conn_handle)
          self.assertIsNot(data, False)
          logging.getLogger().debug("Received interim message: :%s"%data)
      
      logging.getLogger().debug("Shutting down client")
      cmd = {'cmd': 'shutdown'}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      conn_handle.flush()
      
      found_stats = False
      for i in range(15):
        logging.getLogger().debug("Waiting for stats update and shutdown")
        # Wait for stats update before client shutdown
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
        for s in  readable:
          try:
            data = pickle.load(conn_handle)
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
      conn_handle.close()
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
    client = HydraClientProcess({'svr': '127.0.0.1', 'port': self.server_port, 'file_handler': HydraTestClassSlowFileProcess})
    client.start()
    inputs = []
    try:
      readable, _, _ = select.select([self.server], [], [], 5)
      connection, client_address = self.server.accept()
      logging.getLogger().debug('New connection from %s, Socket: %s'%(client_address, connection))
      connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
      conn_handle = connection.makefile('wb+')
      inputs.append(connection)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0')]}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir1')]}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir2')]}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      cmd = {'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir3')]}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      conn_handle.flush()
      logging.getLogger().debug("Sleep 10 seconds to allow for file processing")
      time.sleep(10)
      
      for i in range(2):
        # Wait for multiple stats update before client shutdown
        cmd = {'cmd': 'return_stats'}
        pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
        conn_handle.flush()
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
        #self.assertIsNot(len(readable), 0)
        data = pickle.load(conn_handle)
        #self.assertIsNot(data, False)
        #self.assertEqual('stats', data.get('cmd'))
        time.sleep(3)

      # Wait for stats update before client shutdown
      logging.getLogger().debug("Waiting for shutdown")
      cmd = {'cmd': 'shutdown'}
      pickle.dump(cmd, conn_handle, pickle.HIGHEST_PROTOCOL)
      conn_handle.flush()
      inputs = [connection]
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*4)
      self.assertIsNot(len(readable), 0)
      data = pickle.load(conn_handle)
      self.assertIsNot(data, False)

      logging.getLogger().debug("Closing server to client connection")
      conn_handle.close()
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
  root = logging.getLogger()
  root.setLevel(logging.WARN)
  #root.setLevel(logging.DEBUG)
  #root.setLevel(5)
  #root.setLevel(9)

  ch = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')
  ch.setFormatter(formatter)
  root.addHandler(ch)

  suite1 = unittest.TestLoader().loadTestsFromTestCase(TestHydraClient)
  #suite2 = unittest.TestLoader().loadTestsFromTestCase(TestHydraWorkerProcessDirectory)
  all_tests = unittest.TestSuite([suite1])
  unittest.TextTestRunner(verbosity=2).run(all_tests)
  