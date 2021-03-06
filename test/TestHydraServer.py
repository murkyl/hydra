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

# test code is run from the ./test directory.  add the parent
# directory to the path
current_file = inspect.getfile(inspect.currentframe())
base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
sys.path.insert(0, base_path)
import hydra

LOGGER_CONFIG = {
  'version': 1,
  'disable_existing_loggers': False,
  'formatters': {
    'default': {
      'format': '%(asctime)s [%(levelname)8s] %(name)s - %(process)d : %(message)s',
    },
    'debug': {
      'format': '%(asctime)s [%(levelname)8s] %(name)s [%(funcName)s (%(lineno)d)] - %(process)d : %(message)s',
    },
    'simple': {
      'format': '%(message)s',
    },
  },
  'handlers': {
    'default': { 
      'formatter': 'default',
      'class': 'logging.StreamHandler',
      'stream': 'ext://sys.stdout',
    },
  },
  'loggers': {
    '': {
      'handlers': ['default'],
      'level': 'DEBUG',
    },
    'hydra': {
      'level': 'DEBUG',   # Skip logging unless --debug and --verbose flags are set
    },
    'hydra.HydraClient': {
      'level': 'DEBUG',   # Skip logging unless --debug and --verbose flags are set
    },
  }
}
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
    

class HydraTestClass(hydra.Worker.HydraWorker):
  def __init__(self, args={}):
    super(HydraTestClass, self).__init__(args)
    # Set a default delay of 0.5 seconds per file processed
    #self.file_delay = FILE_DELAY
    self.file_delay = 0
    
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
    cls.rand_buffer = bytearray(random.getrandbits(8) for x in range(cls.buffer_size))
    for i in range(NUM_DIRS):
      cur_path = os.path.join(cls.test_path, "dir%s"%i)
      try:
        os.makedirs(cur_path, exists_ok = True)
      except:
        pass
      create_files(cur_path, FILES_PER_DIR, FILE_SIZE, cls.rand_buffer, cls.buffer_size)
      for j in range(NUM_SUBDIRS):
        sub_path = os.path.join(cur_path, "subdir%s"%j)
        try:
          os.makedirs(sub_path)
        except:
          pass
        create_files(sub_path, FILES_PER_DIR, FILE_SIZE)

  @classmethod
  def tearDownClass(cls):
    #print("tearDownClass called")
    cls.rand_buffer = None
    cls = None
    
  def setupServer(self):
    #svr = hydra.ServerProcess(args={'logger_cfg': LOGGER_CONFIG})
    svr = hydra.ServerProcess()
    return svr
    
  def setupClient(self):
    client = hydra.ClientProcess({
      'svr': '127.0.0.1',
      'port': 8101,
      'file_handler': HydraTestClass,
      #'logger_cfg': LOGGER_CONFIG,
      #'logger_cfg': {
      #  'host': ,
      #  'port': ,
      #  'secret': ,
      #},
    })
    return client

  #@unittest.skip("")
  def test_1_spawn_server_and_shutdown(self):
    svr = self.setupServer()
    svr.start()
    
    svr.send(hydra.Server.EVENT_SHUTDOWN)
    svr.join(5)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise

  #@unittest.skip("")
  def test_2_single_client_connection_and_shutdown(self):
    svr = self.setupServer()
    svr.start()
    clients = []
    clients.append(self.setupClient())
    for c in clients:
      c.start()
    
    logging.getLogger().debug("Waiting 2 seconds for clients to connect before shutdown")
    time.sleep(2)
    svr.send(hydra.Server.EVENT_SHUTDOWN)
    
    logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise
  
  #@unittest.skip("")
  def test_3_multiple_client_connection_and_shutdown(self):
    svr = self.setupServer()
    svr.start()
    clients = []
    num_clients = 4
    for i in range(num_clients):
      clients.append(self.setupClient())
    for c in clients:
      c.start()
    
    logging.getLogger().debug("Waiting 2 seconds for clients to connect before shutdown")
    time.sleep(2)
    svr.send(hydra.Server.EVENT_SHUTDOWN)
    
    logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise
    for c in clients:
      if c.is_alive():
        c.join(10)
        self.assertFalse(c.is_alive())
      
  #@unittest.skip("")
  def test_4_single_client_single_dir(self):
    svr = self.setupServer()
    svr.start()
    clients = []
    clients.append(self.setupClient())
    for c in clients:
      c.start()
    inputs = [svr]

    # Wait for server to be idle before submitting work
    found = False
    for i in range(40):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
      if len(readable) > 0:
        data = svr.recv()
        if data['cmd'] == hydra.Server.CMD_SVR_STATE:
          if data['msg']['state'] == hydra.Server.STATE_IDLE:
            found = True
            break
    self.assertTrue(found, msg="Server never went idle")
    
    found = False
    logging.getLogger().debug("Submitting work")
    svr.send(hydra.Server.EVENT_SUBMIT_WORK, {'paths': [os.path.join(self.test_path, 'dir0')]})
    for i in range(40):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
      if readable:
        data = svr.recv()
        if data['cmd'] == 'state':
          if data['msg']['state'] in [hydra.Server.STATE_IDLE]:
            found = True
            break
      else:
        break
    self.assertTrue(found, msg="Server never returned to idle state after work submission")

    logging.getLogger().debug("Server is idle. Requesting shutdown")
    svr.send(hydra.Server.EVENT_SHUTDOWN)
    
    logging.getLogger().debug("Waiting for final stats update")
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
      if len(readable) > 0:
        data = svr.recv()
        if data['cmd'] == hydra.Server.CMD_SVR_STATS:
          self.assertEqual(data['msg']['stats']['processed_files'], 110)
          self.assertEqual(data['msg']['stats']['processed_dirs'], 11)
          break
    logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise
    for c in clients:
      if c.is_alive():
        c.join(10)
        self.assertFalse(c.is_alive())

  #@unittest.skip("")
  def test_5_multiple_client_2_worker_multiple_dir(self):
    svr = self.setupServer()
    svr.start()
    clients = []
    num_clients = 3
    for i in range(num_clients):
      clients.append(self.setupClient())
    for c in clients:
      c.set_workers(2)
      c.start()
    inputs = [svr]
    num_test_dirs = 6
    dirs_per_test_dir = 11
    files_per_test_dir = 110
    
    logging.getLogger().debug("Submitting work")
    for i in range(num_test_dirs):
      svr.send(hydra.Server.EVENT_SUBMIT_WORK, {'paths': [os.path.join(self.test_path, 'dir%d'%i)]})
    found = False
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
      if len(readable) > 0:
        data = svr.recv()
        if data['cmd'] == 'state':
          if data['msg']['state'] == hydra.Server.STATE_PROCESSING:
            found = True
            break
    self.assertTrue(found, msg="Server never sent state change to processing state")
    
    found = False
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
      if len(readable) > 0:
        data = svr.recv()
        if data['cmd'] == 'state':
          if data['msg']['state'] == hydra.Server.STATE_IDLE:
            found = True
            break
    self.assertTrue(found, msg="Server never returned to idle state")
    logging.getLogger().debug("Server is idle. Shutting down.")
    svr.send(hydra.Server.EVENT_SHUTDOWN)
    
    logging.getLogger().debug("Waiting for final stats update")
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
      if len(readable) > 0:
        data = svr.recv()
        if data['cmd'] == hydra.Server.CMD_SVR_STATS:
          self.assertEqual(data['msg']['stats']['processed_files'], (num_test_dirs)*files_per_test_dir)
          self.assertEqual(data['msg']['stats']['processed_dirs'], (num_test_dirs)*dirs_per_test_dir)
          break
    logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise
    for c in clients:
      if c.is_alive():
        c.join(10)
        self.assertFalse(c.is_alive())

  #@unittest.skip("")
  def test_6_multiple_client_8_worker_large_dir(self):
    svr = hydra.ServerProcess(args={
        'dirs_per_idle_client': 1,
    })
    svr.start()
    clients = []
    num_clients = 3
    for i in range(num_clients):
      clients.append(hydra.ClientProcess({
          'svr': '127.0.0.1',
          'port': 8101,
          'file_handler': HydraTestClass,
      }))
      #time.sleep(0.25)
    for c in clients:
      c.set_workers(8)
      c.start()
    inputs = [svr]
    num_test_dirs = 10
    dirs_per_test_dir = 11
    files_per_test_dir = 110
    
    logging.getLogger().debug("Submitting work")
    for i in range(num_test_dirs):
      svr.send(hydra.Server.EVENT_SUBMIT_WORK, {'paths': [os.path.join(self.test_path, 'dir%d'%i)]})
    found = False
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
      if len(readable) > 0:
        data = svr.recv()
        if data['cmd'] == hydra.Server.CMD_SVR_STATE:
          if data['msg']['state'] == hydra.Server.STATE_PROCESSING:
            found = True
            break
    self.assertTrue(found, msg="Server never sent state change to processing state")
    
    found = False
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
      if len(readable) > 0:
        data = svr.recv()
        if data['cmd'] == hydra.Server.CMD_SVR_STATE:
          if data['msg']['state'] == hydra.Server.STATE_IDLE:
            found = True
            break
    self.assertTrue(found, msg="Server never sent state change to idle state")
            
    logging.getLogger().debug("Server is idle. Get individual client stats then shutdown")
    svr.send(hydra.Server.EVENT_QUERY_STATS, {'data': 'individual'})
    logging.getLogger().debug("Shutting down")
    svr.send(hydra.Server.EVENT_SHUTDOWN)
    for i in range(20):
      readable, _, _ = select.select(inputs, [], [], POLL_WAIT_SECONDS)
      if len(readable) > 0:
        data = svr.recv()
        if data['cmd'] == hydra.Server.CMD_SVR_STATS_INDIVIDUAL:
          logging.getLogger().debug("Individual client stat: %s"%data['msg']['stats'])
        elif data['cmd'] == hydra.Server.CMD_SVR_STATS:
            logging.getLogger().debug("Final server stats: %s"%data['msg']['stats'])
            self.assertEqual(data['msg']['stats']['processed_files'], (num_test_dirs)*files_per_test_dir)
            self.assertEqual(data['msg']['stats']['processed_dirs'], (num_test_dirs)*dirs_per_test_dir)
            break
      else:
        break
    svr.join(10)
    try:
      self.assertFalse(svr.is_alive())
    except:
      svr.terminate()
      raise
    for c in clients:
      if c.is_alive():
        c.join(10)
        self.assertFalse(c.is_alive())

if __name__ == '__main__':
  debug_count = sys.argv.count('--debug')
  log_lvl = logging.WARN
  if debug_count > 2:
    log_lvl = 5
  elif debug_count > 1:
    log_lvl = 9
  elif debug_count > 0:
    log_lvl = logging.DEBUG
  logging.basicConfig(
    format='%(asctime)s [%(levelname)8s] %(name)s [%(funcName)s (%(lineno)d)] - %(process)d : %(message)s',
    level=log_lvl,
  )
  root = logging.getLogger()
  worker = logging.getLogger('HydraWorker')
  worker.level= logging.WARN
  client = logging.getLogger('HydraClient')
  client.level= logging.WARN

  suite1 = unittest.TestLoader().loadTestsFromTestCase(TestHydraServer)
  all_tests = unittest.TestSuite([suite1])
  unittest.TextTestRunner(verbosity=2).run(all_tests)
  