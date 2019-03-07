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
import unittest

BASE_TEST_PATH = os.path.join('test', 'data')
RANDOM_SEED = 42
NUM_DIRS = 10
NUM_SUBDIRS = 10
FILES_PER_DIR = 10
FILE_SIZE = 1024*8
RANDOM_DATA_BUF_SIZE = 1024*1024*4
POLL_WAIT_SECONDS = 5
FILE_DELAY = 0.25

if __package__ is None:
    # test code is run from the ./test directory.  add the parent
    # directory to the path so that we can see all the isi_ps code.
    current_file = inspect.getfile(inspect.currentframe())
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
    sys.path.insert(0, base_path)
    from HydraWorker import HydraWorker
    from HydraClient import HydraClient
    from HydraClient import HydraClientProcess
    from HydraServer import HydraServer
    from HydraServer import HydraServerProcess

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
    #if file == "skip_check":
    #  return False
    if self.file_delay > 0:
      time.sleep(self.file_delay)
    file_lstats = os.lstat(os.path.join(dir, file))
    #logging.getLogger().debug("Proc file: %s"%os.path.join(dir, file))
    return True
    
  def handle_extended_ops(self, data):
    if data.get('op') == 'setdelay':
      self.file_delay = data.get('payload')
    return True

class TestHydraServer(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
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
      #cls.server.close()
      cls.server = None
    except:
      pass
    cls.rand_buffer = None
    cls = None

  #@unittest.skip("")
  def test_1_spawn_server_and_shutdown(self):
    svr = HydraServerProcess()
    svr.start()
    
    svr.send({'cmd': 'shutdown'})
    svr.join(5)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise

  #@unittest.skip("")
  def test_2_single_client_connection_and_shutdown(self):
    svr = HydraServerProcess()
    svr.start()
    clients = []
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    for c in clients:
      c.start()
    
    logging.getLogger().debug("Waiting 2 seconds for clients to connect before shutdown")
    time.sleep(2)
    svr.send({'cmd': 'shutdown'})
    
    logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise
  
  #@unittest.skip("")
  def test_3_multiple_client_connection_and_shutdown(self):
    svr = HydraServerProcess()
    svr.start()
    clients = []
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    for c in clients:
      c.start()
    
    logging.getLogger().debug("Waiting 2 seconds for clients to connect before shutdown")
    time.sleep(2)
    svr.send({'cmd': 'shutdown'})
    
    logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise
      
  #@unittest.skip("")
  def test_4_single_client_single_dir(self):
    svr = HydraServerProcess()
    svr.start()
    clients = []
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    for c in clients:
      c.start()
    
    inputs = [svr]

    # Wait for server to be idle before submitting work
    found = False
    for i in range(2):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
      if len(readable) > 0:
        cmd = svr.recv()
        if cmd['cmd'] == 'state':
          if cmd['state'] == 'idle':
            found = True
            break
    self.assertTrue(found, msg="Server never returned to idle state")
    
    found = False
    logging.getLogger().debug("Submitting work")
    svr.send({'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0')]})
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
      if len(readable) > 0:
        cmd = svr.recv()
        if cmd['cmd'] == 'state':
          if cmd['state'] == 'idle':
            found = True
            break
      else:
        break
    self.assertTrue(found, msg="Server never returned to idle state")

    logging.getLogger().debug("Server is idle. Requesting shutdown")
    svr.send({'cmd': 'shutdown'})
    
    logging.getLogger().debug("Waiting for final stats update")
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
      if len(readable) > 0:
        cmd = svr.recv()
        if cmd['cmd'] == 'stats':
          self.assertEqual(cmd['stats']['processed_files'], 110)
          self.assertEqual(cmd['stats']['processed_dirs'], 11)
          break
    logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise

  #@unittest.skip("")
  def test_5_multiple_client_multiple_dir(self):
    svr = HydraServerProcess()
    svr.start()
    clients = []
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    for c in clients:
      c.start()
    
    inputs = [svr]
    
    logging.getLogger().debug("Submitting work")
    svr.send({'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir0')]})
    svr.send({'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir1')]})
    svr.send({'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir2')]})
    svr.send({'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir3')]})
    svr.send({'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir4')]})
    svr.send({'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir5')]})
    svr.send({'cmd': 'submit_work', 'paths': [os.path.join(self.test_path, 'dir6')]})
    found = False
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
      if len(readable) > 0:
        cmd = svr.recv()
        if cmd['cmd'] == 'state':
          if cmd['state'] == 'processing':
            found = True
            break
    self.assertTrue(found, msg="Server never sent state change to processing state")        
    
    found = False
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
      if len(readable) > 0:
        cmd = svr.recv()
        if cmd['cmd'] == 'state':
          if cmd['state'] == 'idle':
            found = True
            break
    self.assertTrue(found, msg="Server never returned to idle state")
    logging.getLogger().debug("Server is idle. Shutting down.")
    svr.send({'cmd': 'shutdown'})
    
    logging.getLogger().debug("Waiting for final stats update")
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
      if len(readable) > 0:
        cmd = svr.recv()
        if cmd['cmd'] == 'stats':
          self.assertEqual(cmd['stats']['processed_files'], 770)
          self.assertEqual(cmd['stats']['processed_dirs'], 77)
          break
    logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise

  #@unittest.skip("")
  def test_6_multiple_client_large_dir(self):
    svr = HydraServerProcess()
    svr.start()
    clients = []
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    clients.append(HydraClientProcess({'svr': '127.0.0.1', 'port': 8101, 'file_handler': HydraTestClassSlowFileProcess}))
    for c in clients:
      c.start()
    
    inputs = [svr]
    
    logging.getLogger().debug("Submitting work")
    svr.send({'cmd': 'submit_work', 'paths': [os.path.join(self.test_path)]})
    found = False
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
      if len(readable) > 0:
        cmd = svr.recv()
        if cmd['cmd'] == 'state':
          if cmd['state'] == 'processing':
            found = True
            break
    self.assertTrue(found, msg="Server never sent state change to processing state")        
    
    found = False
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
      if len(readable) > 0:
        cmd = svr.recv()
        if cmd['cmd'] == 'state':
          if cmd['state'] == 'idle':
            found = True
            break
    self.assertTrue(found, msg="Server never sent state change to idle state")
            
    logging.getLogger().debug("Server is idle. Get individual client stats then shutdown")
    svr.send({'cmd': 'get_stats', 'data': 'individual_clients'})
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS*5)
      if len(readable) > 0:
        cmd = svr.recv()
        if cmd['cmd'] == 'stats_individual_clients':
          logging.getLogger().debug("Individual client stat: %s"%cmd['stats'])
      else:
        break
    
    logging.getLogger().debug("Shutting down")
    svr.send({'cmd': 'shutdown'})
    
    logging.getLogger().debug("Waiting for final stats update")
    for i in range(20):
      try:
        readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
        if len(readable) > 0:
          cmd = svr.recv()
          if cmd['cmd'] == 'stats':
            self.assertEqual(cmd['stats']['processed_files'], 1101)
            self.assertEqual(cmd['stats']['processed_dirs'], 111)
            break
      except KeyboardInterrupt:
        break
    svr.send({'cmd': 'shutdown'})
    logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise

if __name__ == '__main__':
  root = logging.getLogger()
  root.setLevel(logging.WARN)
  #root.setLevel(logging.DEBUG)
  #root.setLevel(9)

  ch = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')
  ch.setFormatter(formatter)
  root.addHandler(ch)

  suite1 = unittest.TestLoader().loadTestsFromTestCase(TestHydraServer)
  all_tests = unittest.TestSuite([suite1])
  unittest.TextTestRunner(verbosity=2).run(all_tests)
  