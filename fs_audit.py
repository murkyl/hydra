# -*- coding: utf8 -*-
__title__ = "fs_audit"
__version__ = "1.0.0"
__all__ = []
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
__usage__="""%prog [options]"""
__description__="""====================
Requirements:
  python 2.7+

====================
"""

import inspect
import os
import sys
import multiprocessing
import time
import datetime
import logging
import socket
import select
import optparse
import HydraUtils
import stat
import pathlib
import json
from HydraWorker import HydraWorker
from HydraClient import HydraClient
from HydraClient import HydraClientProcess
from HydraServer import HydraServer
from HydraServer import HydraServerProcess
from HistogramStat import HistogramStat
from HistogramStat import HistogramStatCountAndValue
from HistogramStat import HistogramStat2D
from HistogramStat import HashBinCountAndValue
try:
  import pymongo
except:
  pass
# On Windows systems check for WindowsError for platform specific exception handling
try:
  dir(WindowsError)
except:
  class WindowsError(OSError): pass
# Try to import Windows libraries to get SID
try:
  import win32api
  import win32con
  import win32security
  GET_FILE_OWNER = lambda filename, stats:win32security.GetFileSecurity(filename, win32security.OWNER_SECURITY_INFORMATION).GetSecurityDescriptorOwner()
  GET_FILE_SID = lambda owner: str(owner)
except:
  if os.name == 'nt':
    print("Unable to import Windows libraries. Have you installed them with 'pip install pywin32'?")
  GET_FILE_OWNER = lambda filename, stats: stats.st_uid
  GET_FILE_SID = lambda owner: "UID:%s"%owner


FILE_SIZE_HISTOGRAM = [ # File size histogram table to see how many files fall within each size range
  0,                    # 0 byte file
  1024,                 # <= 1 kiB
  4096,                 # <= 4 kiB
  8192,                 # <= 8 kiB
  16384,                # <= 16 kiB
  32768,                # <= 32 kiB
  65536,                # <= 64 kiB
  131072,               # <= 128 kiB
  1048576,              # <= 1 MiB
  10485760,             # <= 10 MiB
  104857600,            # <= 100 MiB
  1073741824,           # <= 1 GiB
  10737418240,          # <= 10 GiB
  107374182400,         # <= 100 GiB
  1099511627776,        # <= 1 TiB
  #'other',             # > 1 TiB
]

FILE_AGE_HISTOGRAM = [  # File size histogram table to see how many files fall within each date range (in seconds)
  0,                    # Sometime in the future
  60,                   # Within last minute
  3600,                 # Within 1 hour
  86400,                # Within 1 day
  604800,               # Within 1 week
  2592000,              # Within 30 days
  5184000,              # Within 60 days
  7776000,              # Within 90 days
  15552000,             # Within 180 days
  31536000,             # Within 1 year (365 days)
  63072000,             # Within 2 years (365*2 days)
  940608000,            # Within 3 years (365*3 days)
  126144000,            # Within 4 years (365*4 days)
  157680000,            # Within 5 years (365*5 days)
  315360000,            # Within 10 years (365*10 days)
  #'other',             # > 10 years
]

DEFAULT_CONFIG = {
  'cache_size': 10000,                      # Number of file data sets to store in memory before flushing
  'block_size': 8192,                       # Block size of file system
  'default_stat_array_len': 16,             # Default size of statistics arrays
  'extend_stat_array_incr': 16,             # Size of each extension of the stats array
  'top_n_count': 100,
  'file_size_historgram': FILE_SIZE_HISTOGRAM,
  'file_age_histogram': FILE_AGE_HISTOGRAM,
  #DEL: 'file_size_historgram': FILE_SIZE_HISTOGRAM,
  #DEL: 'file_age_histogram': FILE_AGE_HISTOGRAM,
}

# EXAMPLE:
# Additional simple counting stats. Adding stats here requires the code below to
# increment the counters somewhere.
EXTRA_BASIC_STATS = [
  'file_size_total',                        # Total logical bytes used by all files
  'file_size_block_total',                  # Total logical bytes used by all files on block boundaries
  'dir_depth_total',                        # Sum of the depth of every directory. Used to calculate the average directory depth
  'parent_dirs_total',                      # Total number of directories that have children
  'symlink_files',                          # Number of files that are symbolic links
]

# Stats that track the maximum value
EXTRA_STATS_MAX = [
  'max_dir_depth',                          # Maximum directory depth from the root
  'max_dir_width',                          # Highest number of subdirectories in a single directory
  'max_files_in_dir',                       # The largest number of files in a single directory
]

# Stats that track the values at each directory level
EXTRA_STATS_ARRAY = [
  'dir_total_per_dir_depth',                # [Array] Count how many directories exist at a given depth from the root, e.g. 20 directories 1 level down, 40 directories 2 levels down. Used to calculate average directories at a given depth.
  'files_total_per_dir_depth',              # [Array] Count how many files exist at a given depth from the root.
  'file_size_total_per_dir_depth',          # [Array] Total of file logical bytes used at a given depth from the root. Used to calculate average file size at a given depth.
  'file_size_block_total_per_dir_depth',    # [Array] Total of file logical bytes on block boundaries used at a given depth from the root. Used to calculate average file size at a given depth.
]

# Stats that track the max values at each directory level
EXTRA_STATS_MAX_ARRAY = [
  'max_files_in_dir_per_dir_depth',         # [Array] Holds the maximum file count for any directory at a given depth from the root.
]


def incr_per_depth(data_array, index, val):
  try:
    data_array[index] += val
  except IndexError as e:
    extension = (index - len(data_array) + 1)
    if extension < self.args['extend_stat_array_incr']:
      extension = self.args['extend_stat_array_incr']
    data_array.extend([0]*extension)
    data_array[index] += val

def max_per_depth(data_array, index, val):
  try:
    cur_val = data_array[index]
  except IndexError as e:
    extension = (index - len(data_array) + 1)
    if extension < self.args['extend_stat_array_incr']:
      extension = self.args['extend_stat_array_incr']
    data_array.extend([0]*extension)
    cur_val = data_array[index]
  if val > cur_val:
    data_array[index] = val
    
def add_to_per_depth(mutable, newdata):
  l1 = len(mutable)
  l2 = len(newdata)
  if l1 < l2:
    extension = l2 - l1
    if extension < self.args['extend_stat_array_incr']:
      extension = self.args['extend_stat_array_incr']
    mutable.extend([0]*extension)
  for i in range(len(newdata)):
    mutable[i] += newdata[i]

def max_to_per_depth(mutable, newdata):
  l1 = len(mutable)
  l2 = len(newdata)
  if l1 < l2:
    extension = l2 - l1
    if extension < self.args['extend_stat_array_incr']:
      extension = self.args['extend_stat_array_incr']
    mutable.extend([0]*extension)
  for i in range(len(newdata)):
    if newdata[i] > mutable[i]:
      mutable[i] = newdata[i]

def divide_per_depth(data1, data2, divbyzero=0):
  l1 = len(data1)
  l2 = len(data2)
  if l1 < l2:
    data1.extend([0]*(l2 - l1 + 1))
  elif l2 < l1:
    data2.extend([0]*(l1 - l2 + 1))
  return [(x/y if y else divbyzero) for x, y in zip(data1, data2)]


class WorkerHandler(HydraWorker):
  def __init__(self, args={}):
    super(WorkerHandler, self).__init__(args)
    self.args = dict(DEFAULT_CONFIG)
    self.args.update(args)
    self.cache = [None]*self.args.get('cache_size')
    self.cache_idx = 0
    self.ppath_len = 0
    self.ppath_adj = 0
    self.db = None
    self.db_client = None
    
    if args.get('db') and args['db'].get('db_type') != None:
      db_type = args['db']['db_type']
      self.log.debug("Using DB of type: %s for stats collection"%db_type)
      if db_type == 'mongodb':
        if not args['db']['mongodb_name']:
          self.log.critical('DB type of %s specified without a DB name. Please use --mongodb_name parameter'%db_type)
          sys.exit(1)
    else:
      self.log.debug("No DB type specified. Using in memory stats collection.")
    
    # You can configure additional loggers by adding new variables and using
    # the correct logger name
    self.audit = logging.getLogger('audit')
    
  def init_process(self):
    if self.args.get('db') and self.args['db'].get('db_type') != None:
      db_type = self.args['db']['db_type']
      if db_type == 'mongodb':
        self.log.debug("Connecting to MongoDB instance")
        self.db_client = pymongo.MongoClient(self.args['db']['mongodb_host'], self.args['db']['mongodb_port'])
        self.db = self.db_client[self.args['db']['mongodb_name']]
        self.log.debug("MongoDB connected")
    
  def init_stats(self):
    super(WorkerHandler, self).init_stats()
    # EXAMPLE:
    # Extra simple counting stats need to be initialized properly
    for s in (EXTRA_BASIC_STATS + EXTRA_STATS_MAX):
      self.stats[s] = 0
    for s in EXTRA_STATS_ARRAY:
      self.stats[s] = [0]*self.args['default_stat_array_len']
    for s in EXTRA_STATS_MAX_ARRAY:
      self.stats[s] = [0]*self.args['default_stat_array_len']
      
  #DEL: def stats_init(self, stat_obj):
    #stat_obj['hist_file_size_config'] = self.args.get('hist_file_size_config')
    #stat_obj['hist_file_atime_config'] = self.args.get('hist_file_atime_config')
    #stat_obj['hist_file_mtime_config'] = self.args.get('hist_file_mtime_config')
    #stat_obj['hist_file_ctime_config'] = self.args.get('hist_file_ctime_config')
    #stat_obj['topn_file_size_by_uid_count'] = self.args.get('topn_file_size_by_uid_count')
    #stat_obj['topn_file_size_by_gid_count'] = self.args.get('topn_file_size_by_gid_count')
    #stat_obj['topn_file_size_count'] = self.args.get('topn_file_size')
    #stat_obj['hist_file_count_by_size'] = HistogramStatCountAndValue(self.hist_file_size_config)
    #stat_obj['hist_file_count_by_atime'] = HistogramStat2D(self.hist_file_atime_config, self.hist_file_size_config)
    #stat_obj['hist_file_count_by_mtime'] = HistogramStat2D(self.hist_file_mtime_config, self.hist_file_size_config)
    #stat_obj['hist_file_count_by_ctime'] = HistogramStat2D(self.hist_file_ctime_config, self.hist_file_size_config)
    #stat_obj['topn_file_size'] = []
    #stat_obj['extensions'] = HashBinCountAndValue()
    #stat_obj['category'] = HashBinCountAndValue()
    # TODO:
    # Need track total size and file count by file category (video, images, audio, office, etc.)
    
  def flush_cache(self):
    if self.cache_idx > 0:
      if self.db:
        result = self.db.fs_audit.insert_many(
            self.cache[0:self.cache_idx],
            ordered=False,
            bypass_document_validation=True,
        )
      else:
        #TODO: Add in memory stats handling here
        pass
      self.cache_idx = 0
      
  def filter_subdirectories(self, root, dirs, files):
    # No filtering is happening below. We are updating stats that are best
    # collected when starting at a new directory.
    num_dirs = len(dirs)
    num_files = len(files)
    if num_dirs:
      self.stats['parent_dirs_total'] += 1
    ppath = pathlib.PurePath(root)
    self.ppath_len = len(ppath.parents) - self.args['path_depth_adj']
    self.stats['dir_depth_total'] += self.ppath_len
    if num_dirs > self.stats['max_dir_width']:
      self.stats['max_dir_width'] = num_dirs
    if self.ppath_len > self.stats['max_dir_depth']:
      self.stats['max_dir_depth'] = self.ppath_len
    if num_files > self.stats['max_files_in_dir']:
      self.stats['max_files_in_dir'] = num_files
    incr_per_depth(self.stats['dir_total_per_dir_depth'], self.ppath_len, 1)
    max_per_depth(self.stats['max_files_in_dir_per_dir_depth'], self.ppath_len, num_files)
    return dirs, files

  def handle_file(self, dir, file):
    """
    Fill in docstring
    """
    full_path_file = os.path.join(dir, file)
    try:
      file_lstats = os.lstat(full_path_file)
    except WindowsError as e:
      if e.winerror == 3 and len(full_path_file) > 255:
        self.log.error('Unable to stat file due to path length > 255 characters. Try setting HKLM\System\CurrentControlSet\Control\FileSystem\LongPathsEnabled to 1')
        self.log.error(e)
      else:
        if HydraUtils.is_invalid_windows_filename(file):
          self.log.error('File contains invalid characters or invalid names for Windows: %s'%full_file_path)
        else:
          self.log.error(e)
      return False
    # We only want to look at regular files. We will ignore symlinks
    if stat.S_ISREG(file_lstats.st_mode):
      self.audit.info(os.path.join(dir, file))
      owner_sid = ''
      try:
        sd = GET_FILE_OWNER(full_path_file, file_lstats)
        pysid = GET_FILE_SID(sd)
        owner_sid = str(pysid).replace('PySID', 'SID')
      except:
        self.log.log(9, 'Unable to get file permissions for: %s'%full_path_file)

      fsize = file_lstats.st_size
      file_data = {
        'filename': full_path_file,
        'ext': os.path.splitext(full_path_file)[1].replace('.', ''),
        'filesize': fsize,
        'sid': owner_sid,
        'atime': file_lstats.st_atime,
        'ctime': file_lstats.st_ctime,
        'mtime': file_lstats.st_mtime,
        'inode': file_lstats.st_ino,
      }
      self.cache[self.cache_idx] = file_data
      self.cache_idx += 1
      if self.cache_idx >= self.args.get('cache_size'):
        self.flush_cache()
      
      # Update stats
      self.stats['file_size_total'] += fsize
      bs = self.args['block_size']
      block_fsize = (fsize//bs + (not not fsize%bs))*bs   # A not not saves on if/else check. An number + True is the same as number + 1
      self.stats['file_size_block_total'] += block_fsize
      incr_per_depth(self.stats['files_total_per_dir_depth'], self.ppath_len, 1)
      incr_per_depth(self.stats['file_size_total_per_dir_depth'], self.ppath_len, fsize)
      incr_per_depth(self.stats['file_size_block_total_per_dir_depth'], self.ppath_len, block_fsize)
    # DEL:  self.hist_file_count_by_size.insert_data(file_lstats.st_size)
    #  self.hist_file_count_by_atime.insert_data(file_lstats.st_atime, file_lstats.st_size)
    #  self.hist_file_count_by_mtime.insert_data(file_lstats.st_mtime, file_lstats.st_size)
    #  self.hist_file_count_by_ctime.insert_data(file_lstats.st_ctime, file_lstats.st_size)
    #  fname, fext = os.path.splitext(file)
    #  if not fext:
    #    fext = 'NONE'
    #  owner = GET_FILE_OWNER(full_path_file, file_lstats))
    # topn_file_size_by_uid
    # topn_file_size_by_gid
    # topn_file_size
    elif stat.S_ISLNK(file_lstats.st_mode):
      # We didn't really process a symlink so account for it here as a symlink
      self.stats['symlink_files'] += 1
      return False
    else:
      return False
    return True
    
  def handle_stats_collection(self):
    self.flush_cache()
    
  def handle_update_settings(self, cmd):
    self.args.update(cmd['settings'])
    return True
    
class ClientProcessor(HydraClient):
  def __init__(self, worker_class, args={}):
    super(ClientProcessor, self).__init__(worker_class, args)
    if args.get('db') and args['db'].get('db_type') != None:
      db_type = args['db']['db_type']
      if db_type == 'mongodb':
        if not args['db']['mongodb_name']:
          self.log.critical('DB type of %s specified without a DB name. Please use --mongodb_name parameter'%db_type)
          sys.exit(1)
        self.db_client = pymongo.MongoClient(args['db']['mongodb_host'], args['db']['mongodb_port'])
        self.db = self.db_client[args['db']['mongodb_name']]
      else:
        self.log.critical('Unknown DB type of %s specified.'%db_type)
        sys.exit(1)
    
  def init_stats(self, stat_state):
    super(ClientProcessor, self).init_stats(stat_state)
    for s in (EXTRA_BASIC_STATS + EXTRA_STATS_MAX):
      stat_state[s] = 0
    for s in EXTRA_STATS_ARRAY:
      self.stats[s] = [0]*self.args['default_stat_array_len']
    for s in EXTRA_STATS_MAX_ARRAY:
      self.stats[s] = [0]*self.args['default_stat_array_len']

  def consolidate_stats(self):
    super(ClientProcessor, self).consolidate_stats()
    for set in [self.workers, self.shutdown_pending, self.shutdown_workers]:
      for w in set:
        if not set[w]['stats']:
          continue
        for s in EXTRA_BASIC_STATS:
          self.stats[s] += set[w]['stats'][s]
        for s in EXTRA_STATS_MAX:
          if set[w]['stats'][s] > self.stats[s]:
            self.stats[s] = set[w]['stats'][s]
        for s in EXTRA_STATS_ARRAY:
          add_to_per_depth(self.stats[s], set[w]['stats'][s])
        for s in EXTRA_STATS_MAX_ARRAY:
          max_to_per_depth(self.stats[s], set[w]['stats'][s])
    
  #def handle_extended_server_cmd(self, raw_data):
  #  return True

  def handle_update_settings(self, cmd):
    self.args.update(cmd['settings'])
    self.send_all_workers({
        'op': 'update_settings',
        'settings': cmd['settings'],
    })
    return True
    
'''
Generic server processor
'''
class ServerProcessor(HydraServer):
  def init_stats(self, stat_state):
    super(ServerProcessor, self).init_stats(stat_state)
    for s in (EXTRA_BASIC_STATS + EXTRA_STATS_MAX):
      stat_state[s] = 0
    for s in EXTRA_STATS_ARRAY:
      stat_state[s] = [0]*self.args['default_stat_array_len']
    for s in EXTRA_STATS_MAX_ARRAY:
      stat_state[s] = [0]*self.args['default_stat_array_len']
  
  def consolidate_stats(self):
    '''
    The returend stats object can be access using the key values on the left
    side of the equal sign below. A description or formula for each of the keys
    is on the right side of the equal sign.

    Already computed
    ==========
    processed_files = Total file processed
    processed_dirs = Total dirs processed
    file_size_total = Total bytes processed
    file_size_block_total = Total bytes in blocks processed
    max_files_in_dir = Maximum number of files in any single directory
    max_dir_depth = Deepest directory level
    max_dir_width = Maximum number of subdirectories in any single directory
    
    Array style based on directory depth
    ----------
    max_files_in_dir_per_dir_depth = For each directory depth level, the maximum number of files found in a directory
    dir_total_per_dir_depth = Count how many directories exist at a given depth from the root, e.g. 20 directories 1 level down, 40 directories 2 levels down. Used to calculate average directories at a given depth.
    files_total_per_dir_depth = Count how many files exist at a given depth from the root.
    file_size_total_per_dir_depth = Total of file logical bytes used at a given depth from the root. Used to calculate average file size at a given depth.
    file_size_block_total_per_dir_depth = Total of file logical bytes on block boundaries used at a given depth from the root. Used to calculate average file size at a given depth.
    
    Computed
    ==========
    average_file_size = file_size_total / processed_files
    average_file_size_block = file_size_block_total / processed_files
    average_directory_depth = dir_depth_total / processed_dirs
    average_directory_width = (processed_dirs -1) / parent_dirs_total OR 0

    Array style based on directory depth
    ----------
    average_file_size_per_dir_depth = file_size_total_per_dir_depth[i] / files_total_per_dir_depth[i]
    average_file_size_block_per_dir_depth = file_size_block_total_per_dir_depth[i] / files_total_per_dir_depth[i]
    average_files_per_dir_depth = files_total_per_dir_depth[i] / dir_total_per_dir_depth[i]
    '''
    if self.last_client_stat_update <= self.last_consolidate_stats:
      return self.stats
    super(ServerProcessor, self).consolidate_stats()
    for set in [self.clients, self.shutdown_clients]:
      for c in set:
        if not set[c]['stats']:
          continue
        for s in EXTRA_BASIC_STATS:
          self.stats[s] += set[c]['stats'][s]
        for s in EXTRA_STATS_MAX:
          if set[c]['stats'][s] > self.stats[s]:
            self.stats[s] = set[c]['stats'][s]
        for s in EXTRA_STATS_ARRAY:
          add_to_per_depth(self.stats[s], set[c]['stats'][s])
        for s in EXTRA_STATS_MAX_ARRAY:
          max_to_per_depth(self.stats[s], set[c]['stats'][s])
    # Calculate all the stats
    self.stats.update({
      'average_file_size': self.stats['file_size_total']/self.stats['processed_files'] if self.stats['processed_files'] else 0,
      'average_file_size_block': self.stats['file_size_block_total']/self.stats['processed_files'] if self.stats['processed_files'] else 0,
      'average_directory_depth': self.stats['dir_depth_total']/self.stats['processed_dirs'] if self.stats['processed_dirs'] else 0,
      'average_directory_width': (self.stats['processed_dirs'] - 1)/(self.stats['parent_dirs_total']) if self.stats['parent_dirs_total'] else 0,
      'average_file_size_per_dir_depth': divide_per_depth(self.stats['file_size_total_per_dir_depth'], self.stats['files_total_per_dir_depth']),
      'average_file_size_block_per_dir_depth': divide_per_depth(self.stats['file_size_block_total_per_dir_depth'], self.stats['files_total_per_dir_depth']),
      'average_files_per_dir_depth': divide_per_depth(self.stats['files_total_per_dir_depth'], self.stats['dir_total_per_dir_depth']),
    })
    return self.stats
          
  def handle_client_connected(self, client):
    settings = {
      'path_depth_adj': self.args['path_depth_adj']
    }
    self.send_client_command(
        client,
        {
          'cmd': 'update_settings',
          'settings': settings,
        }
    )
    
  def handle_extended_client_cmd(self, cmd):
    return True
    
  def handle_extended_server_cmd(self, cmd):
    return True
    
'''
Add command line options
'''
def AddParserOptions(parser, raw_cli):
    parser.add_option("--server", "-s",
                      action="store_true",
                      default=False,
                      help="Act as the Hydra server.")
    parser.add_option("--connect", "-c",
                      default=None,
                      help="FQDN or IP address of the Hydra server.")
    parser.add_option("--port",
                      default=HydraUtils.DEFAULT_LISTEN_PORT,
                      help="Port to listen when running as a server and port to connect to as a client.")
    parser.add_option("--listen",
                      default=None,
                      help="IP address to bind to when run as a server. The default will listen to all interfaces.")

    op_group = optparse.OptionGroup(parser, "Processing paths",
                           "Options to add paths for processing.")
    op_group.add_option("--path", "-p",
                      default=None,
                      action="store",
                      help="Path to scan. Use of full paths is recommended as "
                           "clients will interpret this path according to their"
                           " own current working directory.")
    parser.add_option_group(op_group)

    op_group = optparse.OptionGroup(parser, "DB options")
    db_type_choices = ["mongodb"]
    op_group.add_option("--db_type",
                      type="choice",
                      default=None,
                      choices=db_type_choices,
                      help="DB type if any to use for storing stats [Choices: %s]"%(','.join(db_type_choices)))
    op_group.add_option("--mongodb_name",
                      default=None,
                      help="Name of the MongoDB database to perform operations")
    op_group.add_option("--mongodb_host",
                      default='127.0.0.1',
                      help="Host of MongoDB instance [Default: %default]")
    op_group.add_option("--mongodb_port",
                      type="int",
                      default=27017,
                      help="Port of MongoDB instance [Default: %default]")
    parser.add_option_group(op_group)

    op_group = optparse.OptionGroup(parser, "Tuning parameters")
    op_group.add_option("--num_workers", "-n",
                      type="int",
                      default=0,
                      help="For clients, specifies the number of worker processes to launch. A value of 0 will have"
                           " the system set this to the number of CPU cores available. [Default: %default]")
    op_group.add_option("--dirs_per_worker",
                      type="int",
                      default=HydraUtils.DIRS_PER_IDLE_WORKER,
                      help="How many directories to issue per idle worker [Default: %default]")
    op_group.add_option("--dirs_per_client",
                      type="int",
                      default=HydraUtils.DIRS_PER_IDLE_CLIENT,
                      help="How many directories to issue per idle client [Default: %default]")
    op_group.add_option("--select_poll_interval",
                      type="float",
                      default=HydraUtils.SELECT_POLL_INTERVAL,
                      help="Polling time in seconds (float) between select calls [Default: %default]")
    op_group.add_option("--default_stat_array_len",
                      type="int",
                      default=DEFAULT_CONFIG['default_stat_array_len'],
                      help=optparse.SUPPRESS_HELP)
    parser.add_option_group(op_group)

    op_group = optparse.OptionGroup(parser, "Logging, auditing and debug",
                           "File names support some variable replacement. {pid} will be replaced "
                           "with the PID of the process. {host} will be replaced by the host name of "
                           "the machine running the script. All variable substitutions for strftime "
                           "are also available for use.")
    op_group.add_option("--log", "-l",
                      default=None,
                      help="If specified, we will log to this file instead of the console. This is "
                           "required for logging on Windows platforms.")
    #op_group.add_option("--log_format",
    #                  default="%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s",
    #                  help="Format for log output. Follows Python standard logging library. [Default: %default]")
    op_group.add_option("--audit", "-a",
                      default=None,
                      help="If specified, we will log audit events to this file instead of the console.")
    #op_group.add_option("--audit_format",
    #                  default="%(message)s",
    #                  help="Format for audit output. Follows Python standard logging library. [Default: %default]")
    op_group.add_option("--debug",
                      action="count",
                      default=0,
                      help="Add flag to enable debug. Add additional flags for more detailed debug.")
    parser.add_option_group(op_group)


def main():
  cli_options = sys.argv[1:]
    
  # Create our command line parser. We use the older optparse library for compatibility on OneFS
  parser = optparse.OptionParser(
      usage=__usage__,
      description=__description__,
      version=__version__,
      formatter=HydraUtils.IndentedHelpFormatterWithNL(),
  )
  # Create main CLI parser
  AddParserOptions(parser, cli_options)
  (options, args) = parser.parse_args(cli_options)
  if options.server is False and options.connect is None:
    parser.print_help()
    print("You must specify running as a server or client")
    sys.exit(1)

  # Setup logging and use the --debug CLI option to set the logging level
  logger_config = dict(HydraUtils.LOGGING_CONFIG)
  if options.debug > 3:
    log_level = 5
    logger_config['handlers']['default']['formatter'] = 'debug'
  elif options.debug > 2:
    log_level = 9
    logger_config['handlers']['default']['formatter'] = 'debug'
  elif options.debug > 1:
    log_level = logging.DEBUG
    logger_config['handlers']['default']['formatter'] = 'debug'
  elif options.debug > 0:
    log_level = logging.INFO
  else:
    log_level = logging.INFO
    logger_config['handlers']['default']['formatter'] = 'message'
    logger_config['handlers']['file']['formatter'] = 'message'
  HydraUtils.config_logger(logger_config, '', log_level=log_level, file=options.log)
  # Setup auditing logger. Use this to output results to a separate file than
  # the normal logger
  if options.audit:
    logger_config['loggers']['audit']['handlers'] = ['audit']
    logger_config['handlers']['audit']['filename'] = options.audit
  logging.config.dictConfig(logger_config)
  log = logging.getLogger('')
  audit = logging.getLogger('audit')
  if isinstance(log.handlers[0], logging.handlers.RotatingFileHandler):
    log.handlers[0].doRollover()
  if options.audit:
    audit.handlers[0].doRollover()
    
  # Calculate an offset for the path depth. We want a depth of 0 to represent
  # the starting point of the directory scan. This naturally only occurs when
  # using '.'. Otherwise the full path is counted as the path depth.
  # Examples:
  # test/ has a depth of 1
  # test/../test has a depth of 3 even though it is the same directory as above
  # . has a depth of 0
  # /home has a depth of 1
  # What we do is take the starting path, calculates its depth and send this
  # to all components so that they can adjust for the start depth.
  if options.server:
    log.info("Starting up the server")
    # Get paths to process from parsed CLI options
    proc_paths = HydraUtils.get_processing_paths(options.path)
    if len(proc_paths) < 1:
      log.critical('A path via command line or a path file must be specified.')
      sys.exit(1)

    start_time = 0
    end_time = 0
    startup = True
    p = pathlib.PurePath(proc_paths[0])
    path_depth_adj = len(p.parents)
      
    svr = HydraServerProcess(
        addr=options.listen,
        port=options.port,
        handler=ServerProcessor,
        args={
          'logger_cfg': logger_config,
          'dirs_per_idle_client': options.dirs_per_client,
          'select_poll_interval': options.select_poll_interval,
          # EXAMPLE:
          # Application specific variables
          'path_depth_adj': path_depth_adj,
          'default_stat_array_len': options.default_stat_array_len,
          'db': {
            'db_type': options.db_type,
            'mongodb_name': options.mongodb_name,
            'mongodb_host': options.mongodb_host,
            'mongodb_port': options.mongodb_port,
          }
        },
    )
    svr.start()
    # EXAMPLE:
    # The following line sends any paths from the CLI to the server to get ready
    # for processing as soon as clients connect. This could be done instead from
    # a UI or even via a TCP message to the server. For the purpose of this
    # example, this is the simplest method.
    svr.send({'cmd': 'submit_work', 'paths': proc_paths})
    
    while True:
      try:
        readable, _, _ = select.select([svr], [], [])
        if len(readable) > 0:
          msg = svr.recv()
          
          cmd = msg.get('cmd')
          if cmd == 'state':
            state = msg.get('state')
            pstate = msg.get('prev_state')
            log.info("Server state transition: %s -> %s"%(pstate, state))
            if state == 'processing':
              if pstate == 'idle':
                start_time = time.time()
                log.info("Time start: %s"%start_time)
            elif state == 'idle':
              if startup:
                startup = False
              else:
                svr.send({'cmd': 'get_stats', 'data': 'individual_clients'})
                end_time = time.time()
                log.info("Time end: %s"%end_time)
                log.info("Total time: %s"%(end_time - start_time))
                svr.send({'cmd': 'output_final_stats'})
                # EXAMPLE:
                # The following line will shutdown the server when all
                # clients go idle. If you comment out the line the server
                # will continue to run and require some sort of interrupt of
                # this example program or a message to be sent to the server to
                # shutdown
                svr.send({'cmd': 'shutdown'})
            elif state == 'shutdown':
              break
          elif cmd == 'stats':
            audit.info('UI received stats update (%s):\n%s'%(
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                json.dumps(
                    msg.get('stats'),
                    ensure_ascii=False,
                    indent=4,
                    sort_keys=True,
                )
            ))
          else:
            log.info("UI received: %s"%cmd)
      except KeyboardInterrupt as ke:
        log.info("Terminate signal received, shutting down")
        break
      except Exception as e:
        log.exception(e)
        break
    log.debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
  else:
    log.info("Starting up client")
    client = HydraClientProcess({
        # Basic arguments
        'svr': options.connect,
        'port': options.port,
        'handler': ClientProcessor,
        'file_handler': WorkerHandler,
        # Options
        'logger_cfg': logger_config,
        'dirs_per_idle_worker': options.dirs_per_worker,
        'select_poll_interval': options.select_poll_interval,
        # EXAMPLE:
        # Application specific variables
        'default_stat_array_len': options.default_stat_array_len,
        'db': {
          'db_type': options.db_type,
          'mongodb_name': options.mongodb_name,
          'mongodb_host': options.mongodb_host,
          'mongodb_port': options.mongodb_port,
        }
    })
    client.set_workers(options.num_workers)
    client.start()
    log.info("Waiting until client exits")
    client.join()
    log.info("Client exiting")

if __name__ == "__main__" or __file__ == None:
    multiprocessing.freeze_support()
    main()
