# -*- coding: utf8 -*-
import inspect
import os
import sys
import unittest
import multiprocessing
import time
import random
import pickle
import logging
import logging.config
import select
import socket

# Test code is run from the ./test directory. Add the parent directory to the path
current_file = inspect.getfile(inspect.currentframe())
base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
sys.path.insert(0, base_path)
import hydra

TEST_PATH = os.path.join('test', 'data')
RANDOM_SEED = 42
NUM_DIRS = 10
NUM_SUBDIRS = 10
FILES_PER_DIR = 10
FILE_SIZE = 1024*8
RANDOM_DATA_BUF_SIZE = 1024*1024*4
POLL_WAIT_SECONDS = 5
FILE_DELAY = 1          # Time in seconds


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
    
class HydraTestClassSlowFileProcess(hydra.WorkerClass):
  def __init__(self, args={}):
    super(HydraTestClassSlowFileProcess, self).__init__(args)
    self.file_delay = 0
    for state in [hydra.Worker.STATE_IDLE, hydra.Worker.STATE_PROCESSING]:
      self._sm_add_event_handler(self.state_table, state, 'setdelay', {'a': self._h_setdelay})
    
  def setFileDelay(self, delay_in_seconds):
    self.file_delay = delay_in_seconds
  
  def handle_file(self, dir, file):
    if file == "skip_check":
      return False
    if self.file_delay > 0:
      time.sleep(self.file_delay)
    file_lstats = os.lstat(os.path.join(dir, file))
    logging.getLogger().debug("Proc file: %s"%os.path.join(dir, file))
    return True
    
  def _h_setdelay(self, event, data, next_state):
    self.file_delay = data.get('payload')
    return next_state

class TestHydraWorkerSpawnAndShutdown(unittest.TestCase):
  def setUp(self):
    self.worker = None
    self.workers = []
    self.log_svr = None
    self.log_svr = hydra.LogRecordStreamServer(name=__name__)
    self.log_svr.start_logger()
    self.worker_args = {
      'logger_cfg': {
        'host': hydra.Utils.LOOPBACK_ADDR,
        'port': self.log_svr.get_port(),
        'secret': self.log_svr.get_secret(),
      }
    }
    # Uncomment out to bypass logging via sockets
    #self.worker_args = {}

  def tearDown(self):
    if self.worker:
      self.worker.terminate()
    self.cleanup_workers()
    if self.log_svr:
      self.log_svr.stop_logger()
   
  def cleanup_workers(self):
    for worker in self.workers:
      worker.terminate()
    self.workers = []
      
  #@unittest.skip("")
  def test_1_spawn_1_workers_and_timeout_recv(self):
    self.worker = hydra.WorkerClass(self.worker_args)
    self.worker.start()
    
    # Grab the initial stats before idle
    data = self.worker.recv(timeout=2)
    self.assertIsInstance(data, dict)
    self.assertEqual(hydra.Worker.CMD_STATS, data.get('op'))

    data = self.worker.recv(timeout=2)
    self.assertIsInstance(data, dict)
    self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
    self.assertEqual(hydra.Worker.STATE_IDLE, data.get('data'))
    
    data = self.worker.recv(timeout=2)
    self.assertEqual(data, False)
    self.worker.terminate()
    self.worker = None
  
  #@unittest.skip("")
  def test_2_spawn_1_workers_get_state_and_timeout_recv(self):
    self.worker = hydra.WorkerClass(self.worker_args)
    self.worker.start()
    
    # Grab the initial stats before idle
    data = self.worker.recv(timeout=2)
    self.assertIsInstance(data, dict)
    self.assertEqual(hydra.Worker.CMD_STATS, data.get('op'))

    data = self.worker.recv(timeout=2)
    self.assertIsInstance(data, dict)
    self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
    self.assertEqual(hydra.Worker.STATE_IDLE, data.get('data'))
    
    self.worker.send({'op': hydra.Worker.EVENT_QUERY_STATS})

    data = self.worker.recv(timeout=2)
    self.assertIsInstance(data, dict)
    self.assertEqual(hydra.Worker.CMD_STATS, data.get('op', None))
    
    data = self.worker.recv(timeout=2)
    self.assertEqual(data, False)
    self.worker.terminate()
    self.worker = None
  
  #@unittest.skip("")
  def test_3_spawn_4_workers_and_shutdown(self):
    sleep_seconds = 2
    num_workers = 4
    
    for i in range(num_workers):
      # Create pipe for client to worker communications
      worker = hydra.WorkerClass(self.worker_args)
      self.workers.append(worker)
      worker.start()
    time.sleep(sleep_seconds)
    for i in range(num_workers):
      data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsInstance(data, dict)
      self.assertEqual(hydra.Worker.CMD_STATS, data.get('op'))
    for i in range(num_workers):
      # Expect startup idle message to be sent back to us
      data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsInstance(data, dict)
      self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
      self.assertEqual(hydra.Worker.STATE_IDLE, data.get('data'))
    for i in range(num_workers):
      # Ask worker process to shutdown
      self.workers[i].send({'op': hydra.Worker.EVENT_SHUTDOWN})
    for i in range(num_workers):
      data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsInstance(data, dict)
      self.assertEqual(hydra.Worker.CMD_STATS, data.get('op', None))
      # Verify worker process successfully shutdown
      data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsInstance(data, dict)
      self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
      self.assertEqual('shutdown', data.get('data'))
    self.cleanup_workers()
      
  #@unittest.skip("")
  def test_4_spawn_4_workers_poll_using_select(self):
    sleep_seconds = 1
    num_workers = 4
    
    for i in range(num_workers):
      worker = hydra.WorkerClass(self.worker_args)
      worker.start()
      self.workers.append(worker)
      
    for retry in range(50):
      readable = []
      writable = []
      exceptional = []
      try:
        readable, _, _ = select.select(self.workers, [], [], sleep_seconds)
      except KeyboardInterrupt:
        logging.getLogger().debug("Child processed caught keyboard interrupt waiting for event")
        continue
        
      for s in readable:
        data = s.recv()
        if data.get('op') == hydra.Worker.CMD_STATE and data.get('data') == hydra.Worker.STATE_IDLE:
          s.send({'op': hydra.Worker.EVENT_SHUTDOWN})
        elif data.get('op') == hydra.Worker.CMD_STATE and data.get('data') == 'shutdown':
          s.close()
          self.workers.remove(s)
      if not self.workers:
        break
    self.assertEqual(len(self.workers), 0)
    self.cleanup_workers()

      
class TestHydraWorkerProcessDirectory(unittest.TestCase):
  def setUp(self):
    self.worker = None
    self.workers = []
    self.log_svr = None
    self.buffer_size = RANDOM_DATA_BUF_SIZE
    random.seed(RANDOM_SEED)
    self.base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
    self.test_path = os.path.join(self.base_path, TEST_PATH)
    logging.getLogger().info("Setting up file structure. This may take time")
    
    self.log_svr = hydra.LogRecordStreamServer(name=__name__)
    self.log_svr.start_logger()
    self.worker_args = {
      'logger_cfg': {
        'host': hydra.Utils.LOOPBACK_ADDR,
        'port': self.log_svr.get_port(),
        'secret': self.log_svr.get_secret(),
      }
    }
    # Uncomment out to bypass logging via sockets
    #self.worker_args = {}

    # Check for skip file named 'skip_check' and bypass creation/check if it is present
    if not os.path.isfile(os.path.join(self.test_path, 'skip_check')):
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
    if self.worker:
      self.worker.terminate()
    self.cleanup_workers()
    if self.log_svr:
      self.log_svr.stop_logger()
   
  def cleanup_workers(self):
    for worker in self.workers:
      worker.terminate()
    self.workers = []
      
  #@unittest.skip("")
  def test_1_worker_pause_resume_process_one_directory(self):
    sleep_seconds = 1
    num_workers = 1
    # One work dir per worker required
    work_dirs = [self.test_path]
    
    for i in range(num_workers):
      # Create pipe for client to worker communications
      worker = HydraTestClassSlowFileProcess(self.worker_args)
      worker.start()
      self.workers.append(worker)
  
      # Get initial stats from workers
      for i in range(num_workers):
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(data.get('op'), hydra.Worker.CMD_STATS)

    try:
      for i in range(num_workers):
        self.workers[i].send({'op': hydra.Worker.EVENT_PAUSE})
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_PAUSED, data.get('data'))

      for i in range(num_workers):
        workers[i].send({'op': hydra.Worker.EVENT_PROCESS_DIR, 'dirs': [work_dirs[i]]})
        workers[i].send({'op': hydra.Worker.EVENT_RESUME})

        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_IDLE, data.get('data'))
        
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_PROCESSING, data.get('data'))

      for i in range(num_workers):
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATS, data.get('op', None))
      
      for i in range(num_workers):
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_IDLE, data.get('data'))
      
      for i in range(num_workers):
        # Ask worker process to shutdown
        self.workers[i].send({'op': hydra.Worker.EVENT_SHUTDOWN})
        # Verify worker process successfully shutdown
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATS, data.get('op', None))
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_SHUTDOWN, data.get('data'))
        self.workers[i].close()
    except:
      pass
    finally:
      self.cleanup_workers()

  #@unittest.skip("")
  def test_2_process_one_directory(self):
    sleep_seconds = 1
    num_workers = 1
    # One work dir per worker required
    work_dirs = [self.test_path]

    try:
      for i in range(num_workers):
        worker = HydraTestClassSlowFileProcess(self.worker_args)
        worker.start()
        self.workers.append(worker)
        
      # Get initial stats from workers
      for i in range(num_workers):
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(data.get('op'), hydra.Worker.CMD_STATS)

      # Get initial idle state from workers
      for i in range(num_workers):
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_IDLE, data.get('data'))

      for i in range(num_workers):
        self.workers[i].send({'op': hydra.Worker.EVENT_PROCESS_DIR, 'dirs': [work_dirs[i]]})
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_PROCESSING, data.get('data'))
        
      for i in range(num_workers):
        for j in range(2):
          data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
          self.assertIsInstance(data, dict)
          if data.get('op') == hydra.Worker.CMD_STATS:
            stats = data['data']
            self.assertEqual(1100, stats['processed_files'])
            self.assertEqual(111, stats['processed_dirs'])
            break
          elif data.get('data') == hydra.Worker.STATE_IDLE:
            # Ask worker process to return stats
            self.workers[i].send({'op': hydra.Worker.EVENT_QUERY_STATS})
        
      for i in range(num_workers):
        # Ask worker process to shutdown
        self.workers[i].send({'op': hydra.Worker.EVENT_SHUTDOWN})
        # Verify worker process successfully shutdown
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_IDLE, data.get('data'))
        
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATS, data.get('op', None))

        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_SHUTDOWN, data.get('data'))
    finally:
      self.cleanup_workers()

  #@unittest.skip("")
  def test_3_2_worker_process_one_directory_with_split_work(self):
    sleep_seconds = 1
    num_workers = 2
    # One work dir per worker required
    work_dirs = [self.test_path, None]
    
    try:
      # Start up all workers
      for i in range(num_workers):
        worker = HydraTestClassSlowFileProcess(self.worker_args)
        worker.setFileDelay(FILE_DELAY)
        worker.start()
        self.workers.append(worker)
        
      # Get initial stats from workers
      for i in range(num_workers):
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(data.get('op'), hydra.Worker.CMD_STATS)

      # Wait for all workers to be idle
      logging.getLogger().debug('Waiting for %d to become idle'%num_workers)
      for i in range(num_workers):
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_IDLE, data.get('data'))

      # Ask worker 0 to start processing
      logging.getLogger().debug('Asking worker 0 to start processing')
      self.workers[0].send({'op': hydra.Worker.EVENT_PROCESS_DIR, 'dirs': [work_dirs[0]]})

      data = self.workers[0].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
      self.assertEqual(hydra.Worker.STATE_PROCESSING, data.get('data'))

      # Ask worker 0 to return some work back to us
      logging.getLogger().debug('Asking worker 0 to return work')
      self.workers[0].send({'op': hydra.Worker.EVENT_RETURN_WORK})
      data = self.workers[0].recv(timeout=POLL_WAIT_SECONDS*2)
      self.assertIsNot(data, False)
      self.assertEqual(hydra.Worker.CMD_WORK_ITEMS, data.get('op', None))
      logging.getLogger().debug('Sending second worker %d work items'%len(data.get('data')))
      self.workers[1].send({'op': hydra.Worker.EVENT_PROCESS_WORK, 'work_items': data.get('data')})

      # Wait a small amount of time to let the workers process slowly
      #time.sleep(5)
      # Remove file processing delay to finish test faster
      logging.getLogger().debug('Remove file processing delays')
      for i in range(num_workers):
        self.workers[i].send({'op': 'setdelay', 'payload': 0})

      # Wait for both workers to complete
      logging.getLogger().debug('Wait for both workers to complete work')
      done = [False, False]
      for j in range(50):
        if (done[0] is True) and (done[1] is True):
          break
        for i in range(num_workers):
          if done[i] is False:
            data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
            if data:
              if data.get('op') == hydra.Worker.CMD_STATE and data.get('data') == hydra.Worker.STATE_IDLE:
                logging.getLogger().debug('Worker %d is now idle'%i)
                done[i] = True

      all_stats = []
      for i in range(num_workers):
        # Ask worker process to return stats
        self.workers[i].send({'op': hydra.Worker.EVENT_QUERY_STATS})
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        stats = data['data']
        all_stats.append(stats)
      num_files = 0
      num_dirs = 0
      for i in range(num_workers):
        num_files += all_stats[i].get('processed_files')
        num_dirs += all_stats[i].get('processed_dirs')

      for i in range(num_workers):
        # Ask worker process to shutdown
        self.workers[i].send({'op': hydra.Worker.EVENT_SHUTDOWN})
        # Verify worker process successfully shutdown
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS*2)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATS, data.get('op', None))
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS*2)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_SHUTDOWN, data.get('data'))
        self.workers[i].close()
      self.assertEqual(1100, num_files)
      self.assertEqual(111, num_dirs)
    finally:
      self.cleanup_workers()
      
  #@unittest.skip("")
  def test_4_worker_process_one_directory_with_early_shutdown(self):
    sleep_seconds = 1
    num_workers = 1
    # One work dir per worker required
    work_dirs = [self.test_path, None]
    
    try:
      # Start up all workers
      for i in range(num_workers):
        worker = HydraTestClassSlowFileProcess(self.worker_args)
        worker.setFileDelay(FILE_DELAY)
        worker.start()
        self.workers.append(worker)
        
      # Get initial stats from workers
      for i in range(num_workers):
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(data.get('op'), hydra.Worker.CMD_STATS)

      # Wait for all workers to be idle
      for i in range(num_workers):
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_IDLE, data.get('data'))

      # Ask worker 0 to start processing
      self.workers[0].send({'op': hydra.Worker.EVENT_PROCESS_DIR, 'dirs': [work_dirs[0]]})
      data = self.workers[0].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
      self.assertEqual(hydra.Worker.STATE_PROCESSING, data.get('data'))

      # Ask worker 0 to return some work back to us
      self.workers[0].send({'op': 'return_work'})
      data = self.workers[0].recv(timeout=POLL_WAIT_SECONDS*2)
      self.assertIsNot(data, False)
      self.assertEqual(hydra.Worker.CMD_WORK_ITEMS, data.get('op', None))

      for i in range(num_workers):
        # Ask worker process to shutdown
        self.workers[i].send({'op': hydra.Worker.EVENT_SHUTDOWN})
        # Verify worker process successfully shutdown
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS*2)
        self.assertIsNot(data, False)
        self.assertEqual(hydra.Worker.CMD_STATS, data.get('op', None))
        
        data = self.workers[i].recv(timeout=POLL_WAIT_SECONDS*2)
        self.assertEqual(hydra.Worker.CMD_STATE, data.get('op'))
        self.assertEqual(hydra.Worker.STATE_SHUTDOWN, data.get('data'))
        self.workers[i].close()
    finally:
      self.cleanup_workers()
      

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
  root.setLevel(log_lvl)

  suite1 = unittest.TestLoader().loadTestsFromTestCase(TestHydraWorkerSpawnAndShutdown)
  suite2 = unittest.TestLoader().loadTestsFromTestCase(TestHydraWorkerProcessDirectory)
  all_tests = unittest.TestSuite([suite1, suite2])
  unittest.TextTestRunner(verbosity=2).run(all_tests)
  