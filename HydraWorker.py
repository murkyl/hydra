# -*- coding: utf8 -*-
"""
Module description here
"""
import inspect
import os
import sys
import platform
import time
import socket
import select
import multiprocessing
import logging
try:
   import cPickle as pickle
except:
   import pickle
import zlib
from collections import deque
import HydraUtils

try:
  from os import scandir
except ImportError:
  # Figure out which version of scandir to use because we need to
  # account for the different binaries used.
  current_file = inspect.getfile(inspect.currentframe())
  base_path = os.path.dirname(os.path.abspath(current_file))
  scandir_path = 'scandir_generic'
  local_os = platform.platform()
  if 'Linux' in local_os:
    if 'x86_64' in local_os:
      scandir_path = 'scandir_linux_x86_64'
  elif 'Windows' in local_os:
    scandir_path = 'scandir_windows'
  elif 'Isilon' in local_os:
    # OneFS < 8.0 is based on FreeBSD 7.1, the FreeBSD 7.1 scandir
    # binary is slightly older than the other binaries (scandir v1.5
    # vs. v1.9) due to the EOSL of FreeBSD 7.1
    # OneFS >= 8.0 is based on FreeBSD 10
    # Future version of OneFS will be based on FreeBSD 11
    if 'OneFS-v8' in local_os:
      scandir_path = None      
    else:
      scandir_path = 'scandir_generic'
  elif 'FreeBSD' in local_os:
    if 'FreeBSD-11' in local_os:
      scandir_path = 'scandir_freebsd11'
    elif 'FreeBSD-10' in local_os:
      scandir_path = 'scandir_freebsd10'
    elif 'FreeBSD-7' in local_os:
      scandir_path = 'scandir_freebsd7'
  if scandir_path:
    sys.path.insert(0, os.path.join(base_path, 'lib', scandir_path))
    import scandir
  else:
    scandir = os
  
__title__ = "HydraWoker"
__version__ = "1.0.0"
__all__ = ["HydraWorker"]
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

"""
TODO:
For the partial directory case, we can further break this up into processing
a subset of the files in the directory. This case could happen if the per file
handling routine takes a long time or there are a very high number of files in
a single directory. A method to do this would be during a return_work request
we check if we have no other work to return other than a lot of files in the
current work item. We then break it up into 2 partial_dir work items and return
1 of them. We do need to tag this as partial partial directory with 2+ parts.
Then we add some handling to make sure we count when all these partial partials
completes before we consider the directory completed. This could end up getting
split over many processes. A simplification would be to require 1 directory
to be fully processed on a single machine versus having these partial partial
directories processed over several machines and several processes/machine.
"""

"""
State table

None -> initializing
initializing -> idle
idle -> processing
idle -> paused
idle -> shutdown
processing -> idle
processing -> paused
processing -> shutdown
paused -> idle
paused -> processing
paused -> shutdown
shutdown -> None
"""


class HydraWorker(multiprocessing.Process):
  def __init__(self, args={}):
    """
    Fill in docstring
    """
    super(HydraWorker, self).__init__()
    self.log = logging.getLogger(__name__)
    self.manager_pipe, self.worker_pipe = multiprocessing.Pipe()
    # Sockets from which we expect to read from through a select call
    self.inputs = [self.manager_pipe]
    self.work_queue = deque()
    self.state = 'initializing'
    self.stats = {}
    self.init_stats()
    
  def filter_subdirectories(self, root, dirs, files):
    """
    This method should filter a list of files and directories to process.
    The filter can do nothing and just pass the same inputs back, which is what
    the default implementation does.
    
    This method should not be called from user code. It is exposed for
    subclassing :class:`HydraWorker <multiprocessing.Process>`.
    
    :param root: The UTF8 string representing the base directory
    :param dirs: A list of subdirectories in the root parameter
    :param files: A list of files in the root parameter
    """
    return dirs, files

  def handle_directory_pre(self, dir):
    """
    Return True if this method handled the entire directory and no
    files or subdirs from this point onward should be processed.
    Return False if normal processing should occur
    The default implementation immediately returns False.
    
    This method should not be called from user code. It is exposed for
    subclassing :class:`HydraWorker <multiprocessing.Process>`.
    
    :param dir: Path to the directory where pre-processing should be
    performed.
    """
    return False

  def handle_directory_post(self, dir):
    """
    This method is called after a directory's files have been 
    completely processed. This does not include any subdirectories.
    A user could use this to update some statistics as an example.
    
    Return False for normal processing
    There is currently no effect if True is returned, but this may
    change in the future.
    The default implementation immediately returns False.
    
    :param dir: Path to the directory where post-processing should
    be performed.
    """
    return False

  def handle_file(self, dir, file):
    """
    Returns True if file is properly handled
    Returns False if file was skipped
    
    It is the methods responsibility to increment any stats as required
    when returning False.
    Otherwise this method should be subclassed and perform whatever work is
    necessary for each file.
    """
    return True
    
  def handle_extended_ops(self, raw_data):
    """
    Called by the main loop when an unknown command is found
    This can be used to support custom commands from a HydraClient without
    having to re-write the operation parser. This can be overridden as necessary
    when subclassing :class:`HydraWorker <multiprocessing.Process>`.
    
    Return True if the command was handled
    Return False if the command was not handled. This is the default.
    """
    return False
    
  def init_stats(self):
    """
    Fill in docstring
    """
    for s in HydraUtils.BASIC_STATS:
      self.stats[s] = 0
    
  def cleanup(self, forced=False):
    """
    Fill in docstring
    """
    try:
      if not forced:
        self.log.log(9, "Returning work items")
        self._return_work_items(divisor=1)
        self.log.log(9, "Returning stats")
        self._return_stats()
        self.log.log(9, "Sending shutdown complete")
        self._send_manager('status_shutdown_complete')
      self.worker_pipe.close()
      self.manager_pipe.close()
    except Exception as e:
      self.log.exception(e)
      
  def send(self, data):
    """
    A HydraClient can use this method to send commands to the worker
    """
    try:
      x = self.worker_pipe.send(data)
    except Exception as e:
      self.log.exception(e)
    return x
    
  def recv(self, timeout=-1, convert=True):
    """
    A HydraClient can use this method to read data from the worker
    
    When timeout < 0, block waiting for data
    When timeout >= 0, poll the pipe for this many seconds. A poll 
    returns with False if there is no data to read during the timeout
    
    An EOFError is propagated up from the underlying recv call if the
    pipe is closed and there is no data in the pipe
    """
    if timeout >= 0:
      status = self.worker_pipe.poll(timeout)
      if status is False:
        return status
    data = self.worker_pipe.recv()
    # Decode the data before returning to the caller
    if convert:
      if 'format' in data:
        if data['format'] == 'zpickle':
          data['data'] = pickle.loads(zlib.decompress(data['data']))
          data['format'] = 'dict'
        elif data['format'] == 'pickle':
          data['data'] = pickle.loads(data['data'])
          data['format'] = 'dict'
    return data
    
  def poll(self, timeout=0):
    """
    Fill in docstring
    """
    return self.worker_pipe.poll(timeout)
    
  def close(self):
    """
    Fill in docstring
    """
    try:
      self.send({'op': 'shutdown'})
    except:
      pass
    self.worker_pipe.close()
    self.manager_pipe.close()
    
  def fileno(self):
    """
    A HydraWorker is based on multiprocessing.Process. When it runs it
    makes a copy of both the worker_pipe and manager_pipe. The fileno
    method will be called only by the parent process. The parent
    process needs to use their end of the worker_pipe to communicate
    with the child process.
    
    We return the fileno of the worker_pipe so the object itself can be
    used in a select statement.
    """
    return self.worker_pipe.fileno()
    
  def _queue_dirs(self, data):
    """
    Fill in docstring
    """
    dirs = data.get('dirs', None)
    if not dirs:
      self.log.error("Malformed message for operation 'proc_dir': %r"%data)
      return False
    for dir in dirs:
      if dir in [None, '.', '..']:
        self.log.warn('Invalid directory process request: %s'%dir)
        continue
      self.work_queue.append({'type': 'dir', 'path': dir})
      self.stats['queued_dirs'] += 1
      
  def _queue_work(self, data):
    """
    Fill in docstring
    """
    for item in data.get('work_items', []):
      work_type = item.get('type', None)
      if work_type in ['dir', 'partial_dir']:
        self.stats['queued_dirs'] += 1
      elif work_type == 'file':
        # TODO: Files should be a list of files not a single one
        self.stats['queued_files'] += 1
      self.work_queue.append(item)
      
  def _return_work_items(self, divisor=2):
    """
    Fill in docstring
    """
    # Use simple algorithm by returning roughly half of our work items
    return_queue = len(self.work_queue)//divisor
    if return_queue > 0:
      return_items = []
      for i in range(return_queue):
        # We pull off work items from the right side of the queue.
        # These items should be the oldest and represent paths closer to the
        # root/originally queued. Directories found during a walk are pushed
        # to the left side of the queue.
        work_item = self.work_queue.pop()
        work_type = work_item.get('type', None)
        if work_type in ['dir', 'partial_dir']:
          self.stats['queued_dirs'] -= 1
        elif work_type == 'file':
          self.stats['queued_files'] -= 1
        return_items.append(work_item)
      self._send_manager('work_items', return_items)
    
  def _return_stats(self, data=None):
    """
    Fill in docstring
    """
    format = 'zpickle'
    if data is not None:
      format = data.get('format', 'pickle')
    if format not in HydraUtils.HYDRA_OPERATION_FORMATS:
      format = 'dict'
    self._send_manager('stats', self._get_stats(format), format=format)
    
  def _return_state(self):
    """
    Fill in docstring
    """
    self._send_manager('state', self._get_state())
    
  def _get_stats(self, format='dict'):
    """
    Fill in docstring
    """
    if format == 'pickle':
      return(pickle.dumps(self.stats, protocol=pickle.HIGHEST_PROTOCOL))
    elif format == 'zpickle':
      return(zlib.compress(pickle.dumps(self.stats, protocol=pickle.HIGHEST_PROTOCOL)))
    return self.stats
    
  def _set_state(self, state):
    """
    Fill in docstring
    """
    old_state = self.state
    if old_state != state:
      if state is 'idle':
        # Notify master of our idle state
        if old_state == 'processing':
          self._return_stats()
        self._send_manager('status_idle')
      elif state is 'processing':
        # Notify master of our working state
        self._send_manager('status_processing')
      elif state is 'paused':
        # Set state to pause and wait until we get a 'resume' before continuing processing
        self._send_manager('status_paused')
      elif state is 'resume':
        # Allow normal processing loop to move us from 'resume' state to 'idle' or 'processing'
        pass
      elif state is 'shutdown':
        # Actual work is done in the cleanup() method. Do nothing here.
        pass
      self.state = state
    return old_state
    
  def _get_state(self):
    """
    Fill in docstring
    """
    return self.state
    
  def _send_manager(self, op, data=None, format=None):
    """
    Fill in docstring
    """
    self.log.log(8, "Sending to manager: %s"%op)
    if format is None:
      self.manager_pipe.send({'op': op, 'id': self.name, 'pid': self.pid, 'data': data})
    else:
      self.manager_pipe.send({'op': op, 'id': self.name, 'pid': self.pid, 'data': data, 'format': format})
      
  def _process_work_queue(self):
    """
    Fill in docstring
    """
    self.log.log(9, "_process_work_queue invoked")
    start_time = time.time()
    temp_work = []
    try:
      work_item = self.work_queue.popleft()
    except:
      # No work items
      end_time = time.time()
      self._set_state('idle')
      return False
    work_type = work_item.get('type', None)
    self.log.log(9, "Work type: %s"%work_type)
    self._set_state('processing')
    if work_type == 'dir':
      work_dir = work_item.get('path')
      self.log.log(9, "Processing directory: %s"%work_dir)
      handled = self.handle_directory_pre(work_dir)
      if handled:
        self.stats['filtered_dirs'] += 1
      else:
        temp_work = []
        for root, dirs, files in scandir.walk(work_dir):
          # Filter subdirectories and files by calling the method in the derived class
          before_filter_dirs = len(dirs)
          before_filter_files = len(files)
          dirs[:], files[:] = self.filter_subdirectories(work_dir, dirs, files)
          after_filter_dirs = len(dirs)
          after_filter_files = len(files)
          if before_filter_dirs != after_filter_dirs:
            self.stats['filtered_dirs'] += after_filter_dirs - before_filter_dirs
          if before_filter_files != after_filter_files:
            self.stats['filtered_files'] += after_filter_files - before_filter_files
            
          # We queue up any new directories to the left in our work queue.
          # This leaves the directories closer to the initial ones on the right
          # side of the work queue.
          for dir in reversed(dirs):
            self.work_queue.appendleft({'type': 'dir', 'path': os.path.join(root, dir)})
          self.stats['queued_dirs'] += len(dirs)
          for file in files:
            # Keep track of how long we have been processing this
            # directory.  If the time exceeds LONG_PROCESSING_THRESHOLD
            # we will queue up the files and push them onto the 
            # processing queue and let the main processing loop have a
            # chance to pick up new commands
            proc_time = time.time()
            if (proc_time - start_time) > HydraUtils.LONG_PROCESSING_THRESHOLD:
              temp_work.append(file)
              continue
            if(self.handle_file(work_dir, file)):
              self.stats['processed_files'] += 1
            else:
              self.stats['skipped_files'] += 1
          # We actually want to abort the tree walk as we want to handle the directory structure 1 directory at a time
          dirs[:] = []
      if temp_work:
        self.work_queue.appendleft({'type': 'partial_dir', 'path': work_dir, 'files': temp_work})
      else:
        self.stats['queued_dirs'] -= 1
        self.stats['processed_dirs'] += 1
        handled = self.handle_directory_post(work_dir)
    elif work_type == 'partial_dir':
      # For a partial directory, we need to continue processing all the files
      # remaining in the directory. 
      work_dir = work_item.get('path')
      self.log.log(logging.DEBUG, "Processing directory (continued): %s"%work_dir)
      for file in work_item.get('files'):
        # Keep track of how long we have been processing this directory.
        # If the time exceeds LONG_PROCESSING_THRESHOLD, we will queue up
        # the files and push them onto the processing queue and let the
        # main processing loop have a chance to pick up new commands
        proc_time = time.time()
        if (proc_time - start_time) > HydraUtils.LONG_PROCESSING_THRESHOLD:
          temp_work.append(file)
          continue
        if(self.handle_file(work_dir, file)):
          self.stats['processed_files'] += 1
      # If temp_work is empty, we finished the remainder of the directory
      # so we will do the post directory processing
      # If not then we will re-queue the work and continue processing after
      # checking the command queue
      if temp_work:
        self.work_queue.appendleft({'type': 'partial_dir', 'path': work_dir, 'files': temp_work})
      else:
        self.stats['queued_dirs'] -= 1
        self.stats['processed_dirs'] += 1
        handled = self.handle_directory_post(work_dir)
    #elif work_type == 'file':
    #  # TODO: NOT YET IMPLEMENTED
    #  self.handle_file(work_dir, file)
    #  self.stats['processed_files'] += 1
    else:
      self.log.error("Unknown work type found in work queue. Queued work item: %r"%work_item)
    end_time = time.time()
    return True

  def run(self):
    """
    Fill in docstring
    """
    self.log.log(9, "PID: %d, Process name: %s"%(self.pid, self.name))
    wait_count = 0
    forced_shutdown = False
    while self._get_state() != 'shutdown':
      readable = []
      exceptional = []
      try:
        self.log.log(9, "Waiting on select")
        readable, _, exceptional = select.select(self.inputs, [], self.inputs, wait_count)
        self.log.log(9, "Select returned")
      except KeyboardInterrupt:
        self.log.debug("Caught keyboard interrupt waiting for event")
        self._set_state('shutdown')
        continue

      # Handle inputs
      for s in readable:
        if s is self.manager_pipe:
          data = s.recv()
          if len(data) > 0:
            self.log.log(9, "Worker got data: %r"%data)
            op = data.get('op', None)
            if op == 'proc_dir':
              self._queue_dirs(data)
            elif op == 'proc_work':
              self._queue_work(data)
            elif op == 'return_work':
              self._return_work_items()
            elif op == 'return_stats':
              self._return_stats(data.get('data', None))
            elif op == 'return_state':
              self._return_state()
            elif op == 'pause':
              self._set_state('paused')
            elif op == 'resume':
              self._set_state('idle')
            elif op == 'shutdown':
              self.log.debug("Shutdown request received for (%s)"%self.name)
              self._set_state('shutdown')
              continue
            elif op == 'force_shutdown':
              forced_shutdown = True
              self._set_state('shutdown')
              continue
            else:
              if not self.handle_extended_ops(data):
                self.log.warn("Unknown command received: %r"%data)
          else:
            self.log.error("Input ready but no data received")
        else:
          data = s.recv()
          self.log.warn("Worker got unexpected data: %r"%data)
      
      # Handle exceptions
      for s in exceptional:
        self.log.critical('Handling exceptional condition for %r'%s)
      
      # This section decides when to do the actual processing of the
      # file structure.
      # If the current state is 'pause' then we skip processing until
      # we get a 'resume' request.
      # If the current state is 'shutdown' we skip processing and exit
      # and perform cleanup.
      # All other states will perform processing. The way this works
      # is we walk a single directory and get the files and any
      # subdirectories. If the processing does any work we will go to
      # the 'processing' state. If no work is done then we move to the
      # 'idle' state.
      # While in the 'idle' or 'pause' state, each iteration increments
      # a counter which slows down the polling interval used by the
      # select statement above. This is done to prevent the poll from
      # consuming too many resources when it is idle for a long period
      # of time.
      if self._get_state() == 'shutdown':
        break
      elif self._get_state() == 'paused':
        if wait_count < HydraUtils.LONG_WAIT_THRESHOLD:
          wait_count += HydraUtils.SHORT_WAIT_TIMEOUT
      else:
        # After handling any messages, try to process our work queue
        try:
          handled = self._process_work_queue()
          if handled:
            wait_count = 0
          else:
            if wait_count < HydraUtils.LONG_WAIT_THRESHOLD:
              wait_count += HydraUtils.SHORT_WAIT_TIMEOUT
        except KeyboardInterrupt:
          self._set_state('shutdown')
          break
        except Exception as e:
          self.log.exception(e)
    # Perform cleanup operations before the process terminates
    self.cleanup(forced=forced_shutdown)
