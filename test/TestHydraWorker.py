# -*- coding: utf8 -*-
import inspect
import os
import sys
import unittest
import multiprocessing
import time
import random
import pickle
import zlib
import logging
import select

TEST_PATH = os.path.join('test', 'data')
RANDOM_SEED = 42
NUM_DIRS = 10
NUM_SUBDIRS = 10
FILES_PER_DIR = 10
FILE_SIZE = 1024*8
RANDOM_DATA_BUF_SIZE = 1024*1024*4
POLL_WAIT_SECONDS = 5

if __package__ is None:
    # test code is run from the ./test directory.  add the parent
    # directory to the path so that we can see all the isi_ps code.
    current_file = inspect.getfile(inspect.currentframe())
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
    sys.path.insert(0, base_path)
    from HydraWorker import HydraWorker

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
    if file == "skip_check":
      return False
    if self.file_delay > 0:
      time.sleep(self.file_delay)
    file_lstats = os.lstat(os.path.join(dir, file))
    logging.getLogger().debug("Proc file: %s"%os.path.join(dir, file))
    return True
    
  def handle_extended_ops(self, data):
    if data.get('op') == 'setdelay':
      self.file_delay = data.get('payload')
    return True

class TestHydraWorkerSpawnAndShutdown(unittest.TestCase):
  #@unittest.skip("Debugging")
  def test_1_spawn_1_workers_and_timeout_recv(self):
    worker = HydraWorker()
    worker.start()
    data = worker.recv(timeout=2)
    self.assertEqual('status_idle', data.get('op', None))
    data = worker.recv(timeout=2)
    self.assertEqual(data, False)
    worker.terminate()
  
  #@unittest.skip("Debugging")
  def test_2_spawn_1_workers_get_state_and_timeout_recv(self):
    worker = HydraWorker()
    worker.start()
    data = worker.recv(timeout=2)
    self.assertEqual('status_idle', data.get('op', None))
    worker.send({'op': 'return_state'})
    data = worker.recv(timeout=2)
    self.assertEqual('state', data.get('op', None))
    data = worker.recv(timeout=2)
    self.assertEqual(data, False)
    worker.terminate()
  
  #@unittest.skip("Debugging")
  def test_3_spawn_4_workers_and_shutdown(self):
    sleep_seconds = 1
    num_workers = 4
    workers = []
    
    for i in range(num_workers):
      # Create pipe for client to worker communications
      worker = HydraWorker()
      worker.start()
      workers.append(worker)
    time.sleep(sleep_seconds)
    for i in range(num_workers):
      # Expect startup idle message to be sent back to us
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      self.assertEqual('status_idle', data.get('op', None))
    for i in range(num_workers):
      # Ask worker process to shutdown
      workers[i].send({'op': 'shutdown'})
    for i in range(num_workers):
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      self.assertEqual('stats', data.get('op', None))
      # Verify worker process successfully shutdown
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      self.assertEqual('status_shutdown_complete', data.get('op', None))
      
  #@unittest.skip("Debugging")
  def test_4_spawn_4_workers_poll_using_select(self):
    sleep_seconds = 1
    num_workers = 4
    workers = []
    
    for i in range(num_workers):
      # Create pipe for client to worker communications
      worker = HydraWorker()
      worker.start()
      workers.append(worker)
      
    for retry in range(50):
      readable = []
      writable = []
      exceptional = []
      try:
        readable, writable, exceptional = select.select(workers, [], [], sleep_seconds)
      except KeyboardInterrupt:
        self.log.debug("Child processed caught keyboard interrupt waiting for event")
        self._set_state('shutdown')
        continue
        
      for s in readable:
        data = s.recv()
        if data.get('op') == 'status_idle':
          s.send({'op': 'shutdown'})
        elif data.get('op') == 'status_shutdown_complete':
          s.close()
          workers.remove(s)
      if not workers:
        break

      
class TestHydraWorkerProcessDirectory(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.buffer_size = RANDOM_DATA_BUF_SIZE
    random.seed(RANDOM_SEED)
    cls.base_path = os.path.dirname(os.path.dirname(os.path.abspath(current_file)))
    cls.test_path = os.path.join(cls.base_path, TEST_PATH)
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
      
  #@unittest.skip("")
  def test_1_spawn_worker_pause_resume_process_one_directory(self):
    sleep_seconds = 1
    num_workers = 1
    workers = []
    # One work dir per worker required
    work_dirs = [self.test_path]
    
    for i in range(num_workers):
      # Create pipe for client to worker communications
      worker = HydraTestClassSlowFileProcess()
      worker.start()
      workers.append(worker)
  
    try:
      for i in range(num_workers):
        workers[i].send({'op': 'pause'})
        data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual('status_paused', data.get('op', None))

      for i in range(num_workers):
        workers[i].send({'op': 'proc_dir', 'dirs': [work_dirs[i]]})
        workers[i].send({'op': 'resume'})

        data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual('status_idle', data.get('op', None))
        
        data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual('status_processing', data.get('op', None))

      for i in range(num_workers):
        data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual('stats', data.get('op', None))
      
      for i in range(num_workers):
        data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual('status_idle', data.get('op', None))
      
      for i in range(num_workers):
        # Ask worker process to shutdown
        workers[i].send({'op': 'shutdown'})
        # Verify worker process successfully shutdown
        data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual('stats', data.get('op', None))
        data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
        self.assertIsNot(data, False)
        self.assertEqual('status_shutdown_complete', data.get('op', None))
        workers[i].close()
    except:
      for i in range(num_workers):
        workers[i].close()
      raise

  @unittest.skip("")
  def test_2_spawn_worker_and_process_one_directory(self):
    sleep_seconds = 1
    num_workers = 1
    workers = []
    # One work dir per worker required
    work_dirs = [self.test_path]
    
    for i in range(num_workers):
      worker = HydraTestClassSlowFileProcess()
      worker.start()
      workers.append(worker)
      
    for i in range(num_workers):
      workers[i].send({'op': 'proc_dir', 'dirs': [work_dirs[i]]})
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      self.assertEqual('status_processing', data.get('op', None))
      
    for i in range(num_workers):
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      self.assertEqual('status_idle', data.get('op', None))
    
    for i in range(num_workers):
      # Ask worker process to return stats
      workers[i].send({'op': 'return_stats'})
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      stats = data['data']
      self.assertEqual(1100, stats['processed_files'])
      self.assertEqual(111, stats['processed_dirs'])
      
    for i in range(num_workers):
      # Ask worker process to shutdown
      workers[i].send({'op': 'shutdown'})
      # Verify worker process successfully shutdown
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      self.assertEqual('stats', data.get('op', None))
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      self.assertEqual('status_shutdown_complete', data.get('op', None))
      workers[i].close()

  @unittest.skip("")
  def test_3_spawn_2_worker_process_one_directory_with_split_work(self):
    sleep_seconds = 1
    num_workers = 2
    workers = []
    # One work dir per worker required
    work_dirs = [self.test_path, None]
    
    # Start up all workers
    for i in range(num_workers):
      worker = HydraTestClassSlowFileProcess()
      worker.setFileDelay(1)
      worker.start()
      workers.append(worker)
      
    # Wait for all workers to be idle
    for i in range(num_workers):
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)

    # Ask worker 0 to start processing
    workers[0].send({'op': 'proc_dir', 'dirs': [work_dirs[0]]})
    data = workers[0].recv(timeout=POLL_WAIT_SECONDS)
    self.assertIsNot(data, False)
    self.assertEqual('status_processing', data.get('op', None))

    # Ask worker 0 to return some work back to us
    workers[0].send({'op': 'return_work'})
    data = workers[0].recv(timeout=POLL_WAIT_SECONDS*2)
    self.assertIsNot(data, False)
    self.assertEqual('work_items', data.get('op', None))
    workers[1].send({'op': 'proc_work', 'work_items': data.get('data')})

    # Wait a small amount of time to let the workers process slowly
    #time.sleep(5)
    # Remove file processing delay to finish test faster
    for i in range(num_workers):
      workers[i].send({'op': 'setdelay', 'payload': 0})

    # Wait for both workers to complete
    done = [False, False]
    for j in range(50):
      if (done[0] is True) and (done[1] is True):
        break
      for i in range(num_workers):
        if done[i] is False:
          data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
          if data:
            if data.get('op') == 'status_idle':
              done[i] = True

    all_stats = []
    for i in range(num_workers):
      # Ask worker process to return stats
      workers[i].send({'op': 'return_stats'})
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)
      stats = data['data']
      #print(stats)
      all_stats.append(stats)
    num_files = 0
    num_dirs = 0
    for i in range(num_workers):
      num_files += all_stats[i].get('processed_files')
      num_dirs += all_stats[i].get('processed_dirs')

    for i in range(num_workers):
      # Ask worker process to shutdown
      workers[i].send({'op': 'shutdown'})
      # Verify worker process successfully shutdown
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS*2)
      self.assertIsNot(data, False)
      self.assertEqual('stats', data.get('op', None))
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS*2)
      self.assertIsNot(data, False)
      self.assertEqual('status_shutdown_complete', data.get('op', None))
      workers[i].close()
    self.assertEqual(1100, num_files)
    self.assertEqual(111, num_dirs)
      
  @unittest.skip("")
  def test_4_spawn_1_worker_process_one_directory_with_early_shutdown(self):
    sleep_seconds = 1
    num_workers = 1
    workers = []
    # One work dir per worker required
    work_dirs = [self.test_path, None]
    
    # Start up all workers
    for i in range(num_workers):
      worker = HydraTestClassSlowFileProcess()
      worker.setFileDelay(1)
      worker.start()
      workers.append(worker)
      
    # Wait for all workers to be idle
    for i in range(num_workers):
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS)
      self.assertIsNot(data, False)

    # Ask worker 0 to start processing
    workers[0].send({'op': 'proc_dir', 'dirs': [work_dirs[0]]})
    data = workers[0].recv(timeout=POLL_WAIT_SECONDS)
    self.assertIsNot(data, False)
    self.assertEqual('status_processing', data.get('op', None))

    # Ask worker 0 to return some work back to us
    workers[0].send({'op': 'return_work'})
    data = workers[0].recv(timeout=POLL_WAIT_SECONDS*2)
    self.assertIsNot(data, False)
    self.assertEqual('work_items', data.get('op', None))

    for i in range(num_workers):
      # Ask worker process to shutdown
      workers[i].send({'op': 'shutdown'})
      # Verify worker process successfully shutdown
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS*2)
      self.assertIsNot(data, False)
      self.assertEqual('work_items', data.get('op', None))
      
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS*2)
      self.assertIsNot(data, False)
      self.assertEqual('stats', data.get('op', None))
      
      data = workers[i].recv(timeout=POLL_WAIT_SECONDS*2)
      self.assertEqual('status_shutdown_complete', data.get('op', None))
      workers[i].close()
      

if __name__ == '__main__':
  root = logging.getLogger()
  root.setLevel(logging.WARNING)
  #root.setLevel(logging.DEBUG)
  #root.setLevel(9)

  ch = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')
  ch.setFormatter(formatter)
  root.addHandler(ch)

  suite1 = unittest.TestLoader().loadTestsFromTestCase(TestHydraWorkerSpawnAndShutdown)
  suite2 = unittest.TestLoader().loadTestsFromTestCase(TestHydraWorkerProcessDirectory)
  all_tests = unittest.TestSuite([suite1, suite2])
  unittest.TextTestRunner(verbosity=2).run(all_tests)
  