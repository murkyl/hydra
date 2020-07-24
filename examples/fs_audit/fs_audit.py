# -*- coding: utf8 -*-
__title__ = "fs_audit"
__version__ = "2.0.0"
__all__ = []
__author__ = "Andrew Chung <acchung@gmail.com>"
__license__ = "MIT"
__copyright__ = """Copyright 2019-2020 Andrew Chung
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
__usage__="""{p} [options]""".format(p=__title__)
__description__="""====================
Requirements:
  python 2.7+
  pymongo (optional)
  pywin32 (optional on Windows platforms)
===================="""

import inspect
import os
import sys
import multiprocessing
import time
import logging
import socket
import select
import optparse
try:
  import hydra
except:
  # Example code is run from the examples directory. Add the parent directory to the path
  current_file = inspect.getfile(inspect.currentframe())
  base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(current_file))))
  sys.path.insert(0, base_path)
  import hydra
try:
  import pymongo
except:
  pymongo = None
try:
  dir(time.process_time)
except:
  time.process_time = time.clock
# On Windows systems check for WindowsError for platform specific exception handling
try:
  dir(WindowsError)
except:
  class WindowsError(OSError): pass
# EXAMPLE:
# Add any additional imports
import stat
import json
try:
  import pathlib
except:
  # On Python without pathlib, build minimal support for path depth
  # This does not handle complicated path cases.
  class pathlib:
    def __init__(self, path):
      self.parents = path
    @staticmethod
    def PurePath(path):
      p = [x for x in path.split(os.path.sep) if x != '']
      if len(p) > 0 and p[0] == '.':
        del p[0]
      return pathlib(p)
import fs_audit_export
from HistogramStat import HistogramStat
from HistogramStat import HistogramStatCountAndValue
from HistogramStat import HistogramStat2D
from HistogramStat import HashBinCountAndValue
from HistogramStat import RankItems
from HistogramStat import RankItemsByKey
from HistogramStat import get_file_category
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
  172800,               # Within 2 days
  604800,               # Within 1 week
  2592000,              # Within 30 days
  5184000,              # Within 60 days
  7776000,              # Within 90 days
  15552000,             # Within 180 days
  31536000,             # Within 1 year (365 days)
  63072000,             # Within 2 years (365*2 days)
  94608000,             # Within 3 years (365*3 days)
  126144000,            # Within 4 years (365*4 days)
  157680000,            # Within 5 years (365*5 days)
  315360000,            # Within 10 years (365*10 days)
  #'other',             # > 10 years
]

DEFAULT_CONFIG = {
  'block_size': 8192,                       # Block size of file system
  'cache_size': 10000,                      # Number of file data sets to store in memory before flushing
  'default_stat_array_len': 16,             # Default size of statistics arrays
  'extend_stat_array_incr': 16,             # Size of each extension of the stats array
  'number_base': 2,
  'db_containers': 100,
  'files_table': 'files',
  'cstats_table': 'cstats',
}

DEFAULT_STATS_CONFIG = {
  'top_n_file_size': 100,
  'top_n_file_size_by_gid': 100,
  'top_n_file_size_by_uid': 100,
  'file_size_histogram': FILE_SIZE_HISTOGRAM[:],
  'file_atime_histogram': FILE_AGE_HISTOGRAM[:],
  'file_ctime_histogram': FILE_AGE_HISTOGRAM[:],
  'file_mtime_histogram': FILE_AGE_HISTOGRAM[:],
}

# EXAMPLE:
# Additional simple counting stats. Adding stats here requires the code below to
# increment the counters somewhere.
EXTRA_BASIC_STATS = [
  'error_stat_dirs',                        # Number of directory stat errors
  'error_stat_files',                       # Number of file stat errors
  'file_size_total',                        # Total logical bytes used by all files
  'file_size_block_total',                  # Total logical bytes used by all files on block boundaries
  'dir_depth_total',                        # Sum of the depth of every directory. Used to calculate the average directory depth
  'parent_dirs_total',                      # Total number of directories that have children
  'symlink_files',                          # Number of files that are symbolic links
  'symlink_dirs',                           # Number of directories that are symbolic links
  'num_clients',                            # Number of clients in use
  'num_workers',                            # Number of workers in use
  'time_client_processing',                 # Track time clients spend doing work
  'time_data_save',                         # Track time to put file data into cache for DB insert
  'time_db_insert',                         # Track time spent inserting data into database
  'time_handle_file',                       # Track total time spent handling a file
  'time_sid_lookup',                        # Track time spent looking up SID
  'time_stat',                              # Track time spent running a stat call
  'time_stats_update',                      # Track time spent updating stats at the end of each file handled
]

# Stats that track the maximum value
EXTRA_STATS_MAX = [
  'max_dir_depth',                          # Maximum directory depth from the root
  'max_dir_width',                          # Highest number of subdirectories in a single directory
  'max_files_in_dir',                       # The largest number of files in a single directory
]

# Stats that track the values at each directory level
EXTRA_STATS_DEPTH_ARRAY = [
  'dir_total_per_dir_depth',                # [Array] Count how many directories exist at a given depth from the root, e.g. 20 directories 1 level down, 40 directories 2 levels down. Used to calculate average directories at a given depth.
  'files_total_per_dir_depth',              # [Array] Count how many files exist at a given depth from the root.
  'file_size_total_per_dir_depth',          # [Array] Total of file logical bytes used at a given depth from the root. Used to calculate average file size at a given depth.
  'file_size_block_total_per_dir_depth',    # [Array] Total of file logical bytes on block boundaries used at a given depth from the root. Used to calculate average file size at a given depth.
]

# Stats that track the max values at each directory level
EXTRA_STATS_DEPTH_MAX_ARRAY = [
  'max_files_in_dir_per_dir_depth',         # [Array] Holds the maximum file count for any directory at a given depth from the root.
]

EXTRA_STATS_ARRAY = [
  'work_paths',                             # [Array] Which root paths were scanned
]

UI_STAT_POLL_INTERVAL = 30

# Extra state machine entries, events and commands
CMD_STAT_CONSOLIDATE = 'stat_consolidate'
CMD_UPDATE_DB_SETTINGS = 'update_db_settings'
CMD_RECREATE_DB = 'recreate_db'
CMD_RETURN_STATS_DB = 'return_stats_db'
EVENT_STATS_DB = 'stats_db'
EVENT_ALL_CLIENTS_CONN = 'all_clients_connected'

STATE_DB_CONSOLIDATE = 'db_consolidate'
FS_AUDIT_CLIENT_STATES = {
  STATE_DB_CONSOLIDATE: {
    hydra.Client.EVENT_HEARTBEAT:        {'a': '_h_no_op',             'ns': None},
    hydra.Client.EVENT_NO_WORK:          {'a': '_h_no_op',             'ns': None},
    hydra.Client.EVENT_QUERY_STATS:      {'a': '_h_query_stats',       'ns': None},
    hydra.Client.EVENT_REQUEST_WORK:     {'a': '_h_no_op',             'ns': None},
    hydra.Client.EVENT_RETURN_WORK:      {'a': '_h_no_op',             'ns': None},
    hydra.Client.EVENT_SHUTDOWN:         {'a': '_h_shutdown',          'ns': hydra.Client.STATE_SHUTDOWN},
    hydra.Client.EVENT_SUBMIT_WORK:      {'a': '_h_no_op',             'ns': None},
    hydra.Client.EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',   'ns': None},
    hydra.Client.EVENT_WORKER_STATE:     {'a': '_h_w_state',           'ns': None},
    hydra.Client.EVENT_WORKER_STATS:     {'a': '_h_w_stats',           'ns': None},
  },
}
FS_AUDIT_SVR_STATES = {
  STATE_DB_CONSOLIDATE: {
    EVENT_STATS_DB:                     {'a': '_h_stats_db_received', 'ns': None},
    EVENT_ALL_CLIENTS_CONN:             {'a': '_h_all_clients_conn',  'ns': None},
    hydra.Server.EVENT_CLIENT_CONNECTED: {'a': '_h_client_connect',    'ns': None},
    hydra.Server.EVENT_CLIENT_STATE:     {'a': '_h_client_state',      'ns': None},
    hydra.Server.EVENT_CLIENT_STATS:     {'a': '_h_client_stats',      'ns': None},
    hydra.Server.EVENT_HEARTBEAT:        {'a': '_h_heartbeat',         'ns': None},
    hydra.Server.EVENT_QUERY_STATS:      {'a': '_h_query_stats',       'ns': None},
    hydra.Server.EVENT_SHUTDOWN:         {'a': '_h_shutdown',          'ns': hydra.Server.STATE_SHUTDOWN_PENDING},
    hydra.Server.EVENT_UPDATE_SETTINGS:  {'a': '_h_update_settings',   'ns': None},
  },
}

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
    'stats': {
      'format': '%(asctime)s: %(message)s',
    },
  },
  'handlers': {
    'default': { 
      'formatter': 'default',
      'class': 'logging.StreamHandler',
      'stream': 'ext://sys.stdout',
    },
    'file': {
      'formatter': 'default',
      'class': 'logging.handlers.RotatingFileHandler',
      'delay': True,
      'filename': '',
      'backupCount': 5,
    },
    'audit': {
      'formatter': 'simple',
      'class': 'logging.handlers.RotatingFileHandler',
      'delay': True,
      'filename': '',
      'backupCount': 5,
    },
    'stats': {
      'formatter': 'stats',
      'class': 'logging.StreamHandler',
      'stream': 'ext://sys.stdout',
    }
  },
  'loggers': {
    '': {
      'handlers': ['default'],
      'level': 'WARN',
    },
    'hydra': {
      'level': 100,   # Skip logging unless --debug and --verbose flags are set
    },
    'audit': {
      'level': 'INFO',
    },
    'stats': {
      'handlers': ['stats'],
      'level': 'INFO',
    }
  }
}


def ConfigureLogging(options):
  log_level = logging.WARN
  if options.verbose:
    log_level = logging.INFO
  # Turn off stats output to console if --quiet is set or if there is no logging to file, otherwise duplicate
  # stats output to the console occurs
  if options.quiet or not options.log:
    LOGGER_CONFIG['loggers']['stats']['handlers'] = []
  if options.debug > 2:
    log_level = 5
  elif options.debug > 1:
    log_level = 9
  elif options.debug > 0:
    log_level = logging.DEBUG
  LOGGER_CONFIG['loggers']['']['level'] = log_level
  if options.verbose and options.debug > 0:
    LOGGER_CONFIG['loggers']['hydra']['level'] = log_level
  if log_level <= logging.DEBUG:
    LOGGER_CONFIG['handlers']['default']['formatter'] = 'debug'
  if options.log:
    LOGGER_CONFIG['loggers']['']['handlers'] = ['file']
    LOGGER_CONFIG['handlers']['file']['filename'] = options.log
  if options.audit:
    LOGGER_CONFIG['loggers']['audit']['handlers'] = ['audit']
    LOGGER_CONFIG['handlers']['audit']['filename'] = options.audit
  logging.config.dictConfig(LOGGER_CONFIG)
  log = logging.getLogger()
  # Perform log rollover after logging system is initialized
  if options.log:
    try:
      log.handlers[0].doRollover()
    except:
      pass
  if options.audit:
    try:
      logging.getLogger('audit').handlers[0].doRollover()
    except:
      pass
  return log

'''
Add command line options
'''
def AddParserOptions(parser, raw_cli):
  parser.add_option("--port",
                    default=hydra.Utils.DEFAULT_LISTEN_PORT,
                    help="Port to listen when running as a server and port to connect to as a client.")
  op_group = optparse.OptionGroup(parser, "Server settings")
  op_group.add_option("--server", "-s",
                    action="store_true",
                    default=False,
                    help="Act as the Hydra server.")
  op_group.add_option("--listen",
                    default=None,
                    help="IP address to bind to when run as a server. The default will listen to all interfaces.")
  parser.add_option_group(op_group)

  op_group = optparse.OptionGroup(parser, "Client settings")
  op_group.add_option("--connect", "-c",
                    default=None,
                    help="FQDN or IP address of the Hydra server.")
  op_group.add_option("--src_addr",
                    default='',
                    help="Source address to bind socket. Uses system chosen address if none specified.")
  op_group.add_option("--src_port",
                    type="int",
                    default=0,
                    help="Source port to bind socket. Uses system chosen port if none specified.")
  op_group.add_option("--num_workers", "-n",
                    type="int",
                    default=0,
                    help="For clients, specifies the number of worker processes to launch. A value of 0 will have"
                         " the system set this to the number of CPU cores available. [Default: %default]")
  parser.add_option_group(op_group)

  op_group = optparse.OptionGroup(parser, "Processing",
                         "Options for processing.")
  op_group.add_option("--path", "-p",
                    default=None,
                    action="store",
                    help="Path to scan. Use of full paths is recommended as "
                         "clients will interpret this path according to their"
                         " own current working directory.")
  op_group.add_option("--path_file", "-f",
                    default=None,
                    help="File name with a CR/LF separated list of paths to process. Any leading or trailing "
                         "whitespace is preserved.")
  op_group.add_option("--path_prefix_file",
                    default=None,
                    action="store",
                    help="Path to a file holding prefixes to prepend to "
                         "the path specified by the --path parameters. This "
                         "can be used to allow this client to process the "
                         "directory walk across parallel mounts/shares to "
                         "improve directory walk performance.")
  op_group.add_option("--stat_consolidate",
                    type="int",
                    default=0,
                    help="Instead of scanning a directory path, process data from existing database information."
                         " Using this option will prevent an actual tree walk. If a path is specified as well the"
                         " path will be used to adjust for the path depth calculation only. This argument takes"
                         " the number of clients/databases to process. This number should be the same as the"
                         " number of clients used to walk the file system initially.")
  op_group.add_option("--excel_output",
                    default=None,
                    help="Specify a file name here to output stats results to an Excel formatted file")
  parser.add_option_group(op_group)

  op_group = optparse.OptionGroup(parser, "DB options")
  db_type_choices = ["mongodb"]
  op_group.add_option("--db_type",
                    type="choice",
                    default=None,
                    choices=db_type_choices,
                    help="DB type if any to use for storing stats [Choices: %s]"%(','.join(db_type_choices)))
  op_group.add_option("--db_name",
                    default=None,
                    help="Name of the MongoDB database to perform operations")
  op_group.add_option("--db_host",
                    default='127.0.0.1',
                    help="Host of MongoDB instance [Default: %default]")
  op_group.add_option("--db_port",
                    type="int",
                    default=27017,
                    help="Port of MongoDB instance [Default: %default]")
  op_group.add_option("--recreate_db",
                    action="store_true",
                    default=False,
                    help="Drop and re-create DB if it exists")
  op_group.add_option("--db_svr_name",
                    action="store_true",
                    default=False,
                    help="When enabled, the server will update the clients with the DB name to use.")
  parser.add_option_group(op_group)

  op_group = optparse.OptionGroup(parser, "Tuning parameters")
  op_group.add_option("--dirs_per_worker",
                    type="int",
                    default=hydra.Utils.DIRS_PER_IDLE_WORKER,
                    help="How many directories to issue per idle worker [Default: %default]")
  op_group.add_option("--dirs_per_client",
                    type="int",
                    default=hydra.Utils.DIRS_PER_IDLE_CLIENT,
                    help="How many directories to issue per idle client [Default: %default]")
  op_group.add_option("--select_poll_interval",
                    type="float",
                    default=hydra.Utils.SELECT_POLL_INTERVAL,
                    help="Polling time in seconds (float) between select calls [Default: %default]")
  op_group.add_option("--default_stat_array_len",
                    type="int",
                    default=DEFAULT_CONFIG['default_stat_array_len'],
                    help=optparse.SUPPRESS_HELP)
  op_group.add_option("--stat_poll_interval",
                    type="float",
                    default=UI_STAT_POLL_INTERVAL,
                    help="Polling time in seconds (float) between UI statistics calls [Default: %default]")
  parser.add_option_group(op_group)

  op_group = optparse.OptionGroup(parser, "Logging and debug")
  op_group.add_option("--log", "-l",
                    default=None,
                    help="If specified, we will log to this file instead of the console. This is "
                         "required for logging on Windows platforms.")
  op_group.add_option("--quiet", "-q",
                    action="store_true",
                    default=False,
                    help="Disable console stats output.")
  op_group.add_option("--verbose", "-v",
                    action="store_true",
                    default=False,
                    help="Show verbose output.")
  op_group.add_option("--debug",
                    action="count",
                    default=0,
                    help="Enable debug. Add additional --debug for more detailed debug up to 3 total."
                        " Use --verbose in conjunction with --debug to turn on sub module debugging.")
  parser.add_option_group(op_group)

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
  
def path_depth(path, prefix=[]):
  if len(prefix) > 0:
    for p in prefix:
      if path.find(p) == 0:
        path = path[len(p):]
  ppath = pathlib.PurePath(path)
  return len(ppath.parents)

def init_stats_histogram(stat_state, hist_args):
  # Histogram statistics
  stat_state['hist_file_count_by_size'] = HistogramStatCountAndValue(hist_args.get('file_size_histogram'))
  stat_state['hist_file_count_by_block_size'] = HistogramStatCountAndValue(hist_args.get('file_size_histogram'))
  stat_state['hist_file_count_by_atime'] = HistogramStat2D(hist_args.get('file_atime_histogram'), hist_args.get('file_size_histogram'))
  stat_state['hist_file_count_by_ctime'] = HistogramStat2D(hist_args.get('file_ctime_histogram'), hist_args.get('file_size_histogram'))
  stat_state['hist_file_count_by_mtime'] = HistogramStat2D(hist_args.get('file_mtime_histogram'), hist_args.get('file_size_histogram'))
  stat_state['extensions'] = HashBinCountAndValue()
  stat_state['category'] = HashBinCountAndValue()
  stat_state['top_n_file_size'] = RankItems(hist_args.get('top_n_file_size'))
  stat_state['top_n_file_size_by_gid'] = RankItemsByKey(hist_args.get('top_n_file_size_by_gid'))
  stat_state['top_n_file_size_by_sid'] = RankItemsByKey(hist_args.get('top_n_file_size_by_uid'))
  stat_state['top_n_file_size_by_uid'] = RankItemsByKey(hist_args.get('top_n_file_size_by_uid'))
  stat_state['total_by_sid'] = HashBinCountAndValue()
  stat_state['total_by_uid'] = HashBinCountAndValue()
  stat_state['total_by_gid'] = HashBinCountAndValue()


'''
File audit worker handler
'''
class WorkerHandler(hydra.WorkerClass):
  def __init__(self, args={}):
    self.args = dict(DEFAULT_CONFIG)
    self.args.update(args)
    self.cache = [None]*self.args.get('cache_size')
    self.cache_idx = 0
    self.ppath_len = 0
    self.ppath_adj = 0
    self.ppath_prefix = []
    self.ppath_prefix_len = 0
    self.ppath_prefix_idx = 0
    self.db = None
    self.db_client = None
    self.db_container_counter = 0
    self.db_collection_range = [0, 1]
    self.stats_advanced = {}
    super(WorkerHandler, self).__init__(args)
    
    if args.get('db') and args['db'].get('db_type') != None:
      db_type = args['db']['db_type']
      self.log.debug("Using DB of type: %s for stats collection"%db_type)
      if db_type == 'mongodb':
        if not args['db']['db_name'] and not args['db']['db_svr_name']:
          self.log.warn('DB type of %s specified without a DB name. Expecting DB name to be updated by client or use --db_name parameter'%db_type)
    else:
      self.log.debug("No DB type specified. Using in memory stats collection.")
    
  def init_db(self):
    if self.args.get('db') and self.args['db'].get('db_type') != None:
      db_type = self.args['db']['db_type']
      if db_type == 'mongodb':
        if not self.db_client:
          self.log.debug("Connecting to MongoDB instance")
          self.db_client = pymongo.MongoClient(
              self.args['db']['db_host'], self.args['db']['db_port'],
              appname=__title__,
          )
        if self.args['db']['db_name']:
          self.db = self.db_client[self.args['db']['db_name']]
          self.log.debug("MongoDB client connected")
        else:
          self.db = None
          self.log.warn("MongoDB client connected without DB name")
    
  def init_process(self):
    self.init_db()
    
  def init_stats(self):
    super(WorkerHandler, self).init_stats()
    # EXAMPLE:
    # Extra simple counting stats need to be initialized properly
    for s in (EXTRA_BASIC_STATS + EXTRA_STATS_MAX):
      self.stats[s] = 0
    for s in EXTRA_STATS_ARRAY:
      self.stats[s] = []
    for s in EXTRA_STATS_DEPTH_ARRAY:
      self.stats[s] = [0]*self.args['default_stat_array_len']
    for s in EXTRA_STATS_DEPTH_MAX_ARRAY:
      self.stats[s] = [0]*self.args['default_stat_array_len']
    self.stats_advanced = {}
      
  def consolidate_stats_db(self):
    # When we read data from a DB we need to calculate both basic and histogram
    # statistics
    if not self.db:
      self.log.warn("DB not connected and consolidate_stats_db_called")
      return
    self.init_stats()
    init_stats_histogram(self.stats_advanced, self.args)
    
    # Setup temp variables to skip dictionary overhead
    hist_file_count_by_size = self.stats_advanced['hist_file_count_by_size']
    hist_file_count_by_block_size = self.stats_advanced['hist_file_count_by_block_size']
    hist_file_count_by_atime = self.stats_advanced['hist_file_count_by_atime']
    hist_file_count_by_ctime = self.stats_advanced['hist_file_count_by_ctime']
    hist_file_count_by_mtime = self.stats_advanced['hist_file_count_by_mtime']
    extensions = self.stats_advanced['extensions']
    category = self.stats_advanced['category']
    top_n_files = self.stats_advanced['top_n_file_size']
    top_n_files_gid = self.stats_advanced['top_n_file_size_by_gid']
    top_n_files_sid = self.stats_advanced['top_n_file_size_by_sid']
    top_n_files_uid = self.stats_advanced['top_n_file_size_by_uid']
    total_by_sid = self.stats_advanced['total_by_sid']
    total_by_uid = self.stats_advanced['total_by_uid']
    total_by_gid = self.stats_advanced['total_by_gid']
    now = self.args.get('reference_time', time.time())
    bs = self.args['block_size']
    
    for i in range(self.db_collection_range[0], self.db_collection_range[0]+self.db_collection_range[1]):
      for record in self.db[self.args['files_table'] + '_%d'%i].find():
        file_size = record['filesize']
        file_block_size = (file_size//bs + (not not file_size%bs))*bs   # A not not saves on if/else check. A number + True is the same as number + 1
        hist_file_count_by_size.insert_data(file_size)
        hist_file_count_by_block_size.insert_data(file_block_size)
        hist_file_count_by_atime.insert_data(now - record['atime'], file_size)
        hist_file_count_by_ctime.insert_data(now - record['ctime'], file_size)
        hist_file_count_by_mtime.insert_data(now - record['mtime'], file_size)
        extensions.insert_data(record['ext'].lower(), file_size)
        category.insert_data(get_file_category(record['ext'].lower()), file_size)
        top_n_files.insert_data(file_size, record)
        top_n_files_gid.insert_data(record.get('gid'), file_size, record)
        top_n_files_sid.insert_data(record.get('sid'), file_size, record)
        top_n_files_uid.insert_data(record.get('uid'), file_size, record)
        total_by_sid.insert_data(record.get('sid'), file_size)
        total_by_uid.insert_data(record.get('uid'), file_size)
        total_by_gid.insert_data(record.get('gid'), file_size)
    # Flush all histogram caches
    for key in self.stats_advanced.keys():
      if hasattr(self.stats_advanced[key], 'flush'):
        self.stats_advanced[key].flush()
    
  def flush_cache(self):
    if self.cache_idx > 0:
      if self.db:
        start = time.process_time()
        table_name = self.args['files_table'] + '_%d'%(self.db_collection_range[0] + self.db_container_counter%self.db_collection_range[1])
        self.db_container_counter += 1              # This essentially counts the number of flushes
        result = self.db[table_name].insert_many(
            self.cache[0:self.cache_idx],
            ordered=False,
            bypass_document_validation=True,
        )
        self.stats['time_db_insert'] += (time.process_time() - start)
      else:
        #TODO: Add in memory stats handling here
        # Copy the histogram code from the consolidate_stats_db() method
        pass
      self.cache_idx = 0
      
  def filter_subdirectories(self, root, dirs, files):
    """
    We are updating stats that are best collected when starting at a new
    directory like counting the number of files in a directory or the number of
    subdirectories in this directory.
    """
    num_dirs = len(dirs)
    num_files = len(files)
    if num_dirs:
      self.stats['parent_dirs_total'] += 1
    self.ppath_len = path_depth(root, self.ppath_prefix) - self.ppath_adj
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

  def handle_directory_pre(self, dir):
    """
    Check to see if this directory is actually a symbolic link and filter out
    if necessary
    """
    if self.ppath_prefix_len:
      prefix = self.ppath_prefix[self.ppath_prefix_idx%self.ppath_prefix_len]
      dir = os.path.join(prefix, dir)
    try:
      dir_lstats = os.lstat(dir)
    except WindowsError as e:
      if e.winerror == 3 and len(dir) > hydra.Utils.MAX_WINDOWS_FILEPATH_LENGTH:
        self.log.error('Unable to stat dir due to path length > %d characters. Try setting HKLM\System\CurrentControlSet\Control\FileSystem\LongPathsEnabled to 1'%hydra.Utils.MAX_WINDOWS_FILEPATH_LENGTH)
      else:
        if hydra.is_invalid_windows_filename(dir):
          self.log.error('Directory contains invalid characters or invalid names for Windows: %s'%dir)
        else:
          self.log.exception(e)
      self.stats['error_stat_dirs'] += 1
      return True
    except Exception as e:
      self.log.exception(e)
      self.stats['error_stat_dirs'] += 1
    if stat.S_ISLNK(dir_lstats.st_mode):
      # We do not want to process a symlink so account for it here as a symlink
      self.stats['symlink_dirs'] += 1
      return True
    return False
    
  def handle_file(self, dir, file):
    """
    Fill in docstring
    """
    time_check_1 = time.process_time()
    file_handled = True
    full_path_file = os.path.join(dir, file)
    try:
      file_lstats = os.lstat(full_path_file)
    except WindowsError as e:
      if e.winerror == 3 and len(full_path_file) > hydra.Utils.MAX_WINDOWS_FILEPATH_LENGTH:
        self.log.error('Unable to stat file due to path length > %d characters. Try setting HKLM\System\CurrentControlSet\Control\FileSystem\LongPathsEnabled to 1'%hydra.Utils.MAX_WINDOWS_FILEPATH_LENGTH)
      else:
        if hydra.is_invalid_windows_filename(file):
          self.log.error('File contains invalid characters or invalid names for Windows: %s'%full_path_file)
        else:
          self.log.exception(e)
      self.stats['error_stat_files'] += 1
      return False
    except Exception as e:
      self.stats['error_stat_files'] += 1
      return False
    finally:
      time_check_2 = time.process_time()
      self.stats['time_stat'] += (time_check_2 - time_check_1)
      
    # We only want to look at regular files. We will ignore symlinks
    if stat.S_ISREG(file_lstats.st_mode):
      owner_sid = ''
      time_check_3 = time.process_time()
      try:
        sd = GET_FILE_OWNER(full_path_file, file_lstats)
        pysid = GET_FILE_SID(sd)
        owner_sid = str(pysid).replace('PySID', 'SID')
      except:
        self.log.log(9, 'Unable to get file permissions for: %s'%full_path_file)
      finally:
        time_check_4 = time.process_time()

      fsize = file_lstats.st_size
      file_data = {
        'filename': full_path_file,
        'ext': os.path.splitext(full_path_file)[1].replace('.', ''),
        'filesize': fsize,
        'sid': owner_sid,
        'uid': file_lstats.st_uid,
        'gid': file_lstats.st_gid,
        'atime': file_lstats.st_atime,
        'ctime': file_lstats.st_ctime,
        'mtime': file_lstats.st_mtime,
        'perm': file_lstats.st_mode,
        'links': file_lstats.st_nlink,
        'inode': file_lstats.st_ino,
      }
      self.cache[self.cache_idx] = file_data
      self.cache_idx += 1
      self.cache_idx < self.args.get('cache_size') or self.flush_cache()
      time_check_5 = time.process_time()
      
      # Update stats
      self.stats['file_size_total'] += fsize
      bs = self.args['block_size']
      block_fsize = (fsize//bs + (not not fsize%bs))*bs   # A not not saves on if/else check. A number + True is the same as number + 1
      self.stats['file_size_block_total'] += block_fsize
      incr_per_depth(self.stats['files_total_per_dir_depth'], self.ppath_len, 1)
      incr_per_depth(self.stats['file_size_total_per_dir_depth'], self.ppath_len, fsize)
      incr_per_depth(self.stats['file_size_block_total_per_dir_depth'], self.ppath_len, block_fsize)
      # Update the time counters
      time_check_6 = time.process_time()
      self.stats['time_sid_lookup'] += (time_check_4 - time_check_3)
      self.stats['time_data_save'] += (time_check_5 - time_check_4)
      self.stats['time_stats_update'] += (time_check_6 - time_check_5)
    elif stat.S_ISLNK(file_lstats.st_mode):
      # We didn't really process a symlink so account for it here as a symlink
      self.stats['symlink_files'] += 1
      file_handled = False
    else:
      file_handled = False
    # Update the time counters
    time_check_7 = time.process_time()
    self.stats['time_handle_file'] += (time_check_7 - time_check_1)
    return file_handled
    
  def handle_extended_ops(self, client_msg):
    if not client_msg:
      return False
    cmd = client_msg.get('op')
    if cmd == CMD_UPDATE_DB_SETTINGS:
      self.db_collection_range = client_msg.get('collection_range', self.db_collection_range)
      new_db_name = client_msg.get('name')
      if new_db_name:
        self.args['db']['db_name'] = new_db_name
        self.init_db()
    elif cmd == CMD_RETURN_STATS_DB:
      self._set_state(hydra.Worker.STATE_PROCESSING)
      self.consolidate_stats_db()
      self._send_client(EVENT_STATS_DB, {
          'stats': self.stats,
          'stats_advanced': self.stats_advanced,
      })
      self._set_state(hydra.Worker.STATE_IDLE)
    else:
      return False
    return True
    
  def handle_stats_collection(self):
    self.flush_cache()
    
  def handle_update_settings(self, cmd):
    self.args.update(cmd['settings'])
    if self.args.get('prefix_paths'):
      self.ppath_adj = self.args.get('path_depth_adj')
      self.ppath_prefix = self.args.get('prefix_paths')
      self.ppath_prefix_len = len(self.ppath_prefix)
      # TODO: Maybe use a rng or other method to determine starting idx instead
      # of just using the PID and hoping that we get a uniform distribution
      self.ppath_prefix_idx = self.pid
      # Update our internal fswalk pointer to support adding path prefixes
      self.fswalk_base = self.fswalk
      self.fswalk = self.walk_dir
    return True
    
  def walk_dir(self, dir):
    """
    This method overrides the normal os.walk/fswalk in HydraWalker. The purpose
    is to add path prefixes to the actual walk so that we can distribute the
    work across multiple mount points/shares/drives. This works for some
    parallel file systems where you can have multiple mount points to the same
    name space but across multiple front end processors.
    """
    if self.ppath_prefix_len:
      prefix = self.ppath_prefix[self.ppath_prefix_idx%self.ppath_prefix_len]
      self.ppath_prefix_idx += 1
      merged_path = os.path.join(prefix, dir)
      for root, dirs, files in self.fswalk_base(merged_path):
        yield merged_path, dirs, files
    else:
      yield self.fswalk_base(dir)


'''
File audit client processor
'''
class ClientProcessor(hydra.ClientClass):
  def __init__(self, worker_class, args={}):
    super(ClientProcessor, self).__init__(worker_class, args)
    self.init_db(recreate_db=args.get('db', {}).get(CMD_RECREATE_DB))
    self.stats_advanced = {}
    self.other = {}
    self.write_cstats = False
    self.start_time = time.time()
    # Add a new state to the state table to handle DB consolidations
    self._sm_copy_state(self.state_table, STATE_DB_CONSOLIDATE, FS_AUDIT_CLIENT_STATES[STATE_DB_CONSOLIDATE])
  
  def init_db(self, init_client=True, init_db=True, recreate_db=False):
    if self.args.get('db') and self.args['db'].get('db_type') != None:
      db_type = self.args['db']['db_type']
      if db_type == 'mongodb':
        if init_client:
          self.db_client = pymongo.MongoClient(
              self.args['db']['db_host'], self.args['db']['db_port'],
              appname=__title__,
          )
        if self.args['db']['db_name']:
          if recreate_db:
            self.db_client.drop_database(self.args['db']['db_name'])
          if init_db or init_client:
            self.db = self.db_client[self.args['db']['db_name']]
        else:
          self.log.warn('DB type of %s specified without a DB name. Expecting DB name to be updated by server or use --db_name parameter'%db_type)
          self.db = None
      else:
        self.log.critical('Unknown DB type of %s specified.'%db_type)
        sys.exit(1)
        
  def init_db_collection_range(self):
      split = self.args['db'].get('db_containers')//self.num_workers
      remainder = self.args['db'].get('db_containers') - (split*self.num_workers)
      rstart = 0
      for set in [self.workers, self.shutdown_pending]:
        for w in set:
          incr = split
          if remainder > 0:
            incr += 1
            remainder -= 1
          set[w]['db_collection_range'] = [rstart, incr]
          rstart += incr
          
  def init_stats(self, stat_state):
    super(ClientProcessor, self).init_stats(stat_state)
    for s in (EXTRA_BASIC_STATS + EXTRA_STATS_MAX):
      stat_state[s] = 0
    for s in EXTRA_STATS_ARRAY:
      self.stats[s] = []
    for s in EXTRA_STATS_DEPTH_ARRAY:
      self.stats[s] = [0]*self.args['default_stat_array_len']
    for s in EXTRA_STATS_DEPTH_MAX_ARRAY:
      self.stats[s] = [0]*self.args['default_stat_array_len']
  
  def consolidate_other(self):
    """
    Add very specialized consolidation routines for non-standard statistics
    """
    record = self.db[self.args['cstats_table']].find_one({'type': 'client'})
    if not record:
      self.log.critical('Could not get the "client" key in the "cstats_table"')
      return
    for k in record.keys():
      if k in ['_id', 'type', 'stats']:
        continue
      self.other[k] = record[k]
    self.stats.update(record.get('stats', {}))
  
  def consolidate_stats(self):
    super(ClientProcessor, self).consolidate_stats()
    for set in [self.workers, self.shutdown_pending, self.shutdown_workers]:
      for w in set:
        if not set[w]['stats']:
          continue
        for s in (EXTRA_BASIC_STATS + EXTRA_STATS_ARRAY):
          self.stats[s] += set[w]['stats'][s]
        for s in EXTRA_STATS_MAX:
          if set[w]['stats'][s] > self.stats[s]:
            self.stats[s] = set[w]['stats'][s]
        for s in EXTRA_STATS_DEPTH_ARRAY:
          add_to_per_depth(self.stats[s], set[w]['stats'][s])
        for s in EXTRA_STATS_DEPTH_MAX_ARRAY:
          max_to_per_depth(self.stats[s], set[w]['stats'][s])
    try:
      if self.db and self.write_cstats:
        # Write this clients statistics to the database
        self.stats['num_clients'] = 1
        self.stats['num_workers'] = self.get_max_workers()
        self.stats['time_client_processing'] = time.time() - self.start_time
        self.stats['work_paths'] = self.args.get('work_paths')
        result = self.db[self.args['cstats_table']].replace_one(
            {'type': 'client'},
            {
              'type': 'client',
              'stats': self.stats,
              # Add any additional client wide stats to save to DB here
              'prefix_paths': self.args.get('prefix_paths'),
            },
            upsert=True,
        )
    except Exception as e:
      self.log.exception(e)

  def consolidate_stats_db(self):
    init_stats_histogram(self.stats_advanced, self.args)
    for set in [self.workers, self.shutdown_pending, self.shutdown_workers]:
      for w in set:
        if not set[w].get('stats_advanced'):
          continue
        # Iterate over each histogram and merge
        for k in set[w]['stats_advanced']:
          if k in self.stats_advanced:
            self.stats_advanced[k].merge(set[w]['stats_advanced'][k])
          else:
            self.log.error('Stats merge encountered a mismatch')
          
  def handle_extended_server_cmd(self, svr_msg):
    cmd = svr_msg.get('op')
    if cmd == CMD_RECREATE_DB:
      if not self.db_client:
        self.log.warn("Recreate DB requested but no connection to DB exists")
        return True
      if not self.db:
        self.log.warn("Recreate DB requested, connection to DB exists but we have no DB name.")
        return True
      self.db_client.drop_database(self.args['db']['db_name'])
      self.db = self.db_client[self.args['db']['db_name']]
    elif cmd == CMD_UPDATE_DB_SETTINGS:
      self.args['db']['db_name'] = svr_msg.get('name')
      self.init_db(init_client=False)
      self.init_db_collection_range()
      for set in [self.workers, self.shutdown_pending]:
        for w in set:
          set[w]['obj'].send({
              'op': CMD_UPDATE_DB_SETTINGS,
              'name': svr_msg.get('name'),
              'collection_range': set[w]['db_collection_range'],
          })
    elif cmd == CMD_RETURN_STATS_DB:
      for set in [self.workers, self.shutdown_pending]:
        for w in set:
          set[w]['obj'].send({'op': CMD_RETURN_STATS_DB})
      self._set_state(STATE_DB_CONSOLIDATE)
    else:
      return False
    return True

  def handle_extended_worker_msg(self, wrk_msg):
    cmd = wrk_msg.get('op')
    if cmd == EVENT_STATS_DB:
      worker_id = wrk_msg.get('id')
      all_stat_db_received = True
      for set in [self.workers, self.shutdown_pending]:
        for w in set:
          if w == worker_id:
            set[w]['stat_db_received'] = True
            # Save the stats data
            set[w]['stats'] = wrk_msg['data']['stats']
            set[w]['stats_advanced'] = wrk_msg['data']['stats_advanced']
            # Save any other keys sent back as well
            set[w]['other'] = wrk_msg['data'].get('other', {})
          else:
            if not set[w].get('stat_db_received'):
              all_stat_db_received = False
      if all_stat_db_received:
        #TODO: Added below
        self._set_state(hydra.Client.STATE_IDLE)
        self.consolidate_stats()
        self.consolidate_stats_db()
        self.consolidate_other()
        self._send_server(EVENT_STATS_DB, {
            'stats': self.stats,
            'stats_advanced': self.stats_advanced,
            'other': self.other,
          }
        )
    else:
      return False
    return True
    
  def handle_update_settings(self, cmd):
    super(ClientProcessor, self).handle_update_settings(cmd)
    self.args.update(cmd['settings'])
    return True

  def handle_workers_connected(self):
    if self.args.get('db') and self.args['db'].get('db_type') != None:
      self.init_db_collection_range()
      self.send_workers_db_collection_range()
    
  def send_workers_db_collection_range(self):
    for set in [self.workers, self.shutdown_pending]:
      for w in set:
        set[w]['obj'].send({
            'op': CMD_UPDATE_DB_SETTINGS,
            'collection_range': set[w]['db_collection_range'],
        })
        
  def _set_state(self, state):
    super(ClientProcessor, self)._set_state(state)
    if state == hydra.Client.STATE_PROCESSING:
      self.write_cstats = True

    

'''
File audit server processor
'''
class ServerProcessor(hydra.ServerClass):
  def __init__(self, args={}):
    super(ServerProcessor, self).__init__(args)
    self.stats_histogram = {}
    self.other = {}
    # Add a new state to the state table to handle DB consolidations
    self._sm_copy_state(self.state_table, STATE_DB_CONSOLIDATE, FS_AUDIT_SVR_STATES[STATE_DB_CONSOLIDATE])
    
  def init_stats(self, stat_state):
    super(ServerProcessor, self).init_stats(stat_state)
    for s in (EXTRA_BASIC_STATS + EXTRA_STATS_MAX):
      stat_state[s] = 0
    for s in EXTRA_STATS_ARRAY:
      self.stats[s] = []
    for s in EXTRA_STATS_DEPTH_ARRAY:
      stat_state[s] = [0]*self.args['default_stat_array_len']
    for s in EXTRA_STATS_DEPTH_MAX_ARRAY:
      stat_state[s] = [0]*self.args['default_stat_array_len']
  
  def consolidate_other(self):
    """
    Add very specialized consolidation routines for non-standard statistics
    """
    for set in [self.clients, self.shutdown_clients]:
      for c in set:
        client_other = set[c].get('other', {})
        for k in client_other.keys():
          if k == 'prefix_paths':
            if not self.other.get('prefix_paths'):
              self.other['prefix_paths'] = []
            # Repeatedly converting the array to a dict and back is inefficient
            # but this will happen only once per client and the number of prefix
            # paths should be small so overall impact should be very low
            pp = client_other.get('prefix_paths', [])
            if pp:
              self.other['prefix_paths'].extend(pp)
            self.other['prefix_paths'] = sorted(list(dict.fromkeys(self.other['prefix_paths'])))
  
  def consolidate_stats(self, forced=False):
    '''
    The returned stats object can be access using the key values on the left
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
    if self.last_client_stat_update <= self.last_consolidate_stats and not forced:
      return self.stats
    super(ServerProcessor, self).consolidate_stats()
    for set in [self.clients, self.shutdown_clients]:
      for c in set:
        if not set[c]['stats']:
          continue
        for s in (EXTRA_BASIC_STATS + EXTRA_STATS_ARRAY):
          self.stats[s] += set[c]['stats'][s]
        for s in EXTRA_STATS_MAX:
          if set[c]['stats'][s] > self.stats[s]:
            self.stats[s] = set[c]['stats'][s]
        for s in EXTRA_STATS_DEPTH_ARRAY:
          add_to_per_depth(self.stats[s], set[c]['stats'][s])
        for s in EXTRA_STATS_DEPTH_MAX_ARRAY:
          max_to_per_depth(self.stats[s], set[c]['stats'][s])
    if self.stats.get('work_paths'):
      # Dedupe process paths
      self.stats['work_paths'] = list(dict.fromkeys(self.stats.get('work_paths')))
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
  
  def consolidate_stats_db(self):
    self.stats_histogram = {}
    init_stats_histogram(self.stats_histogram, self.args)
    for set in [self.clients, self.shutdown_clients]:
      for c in set:
        if not set[c]['stats_advanced']:
          continue
        # Iterate over each histogram and merge
        for k in set[c]['stats_advanced']:
          if k in self.stats_histogram:
            self.stats_histogram[k].merge(set[c]['stats_advanced'][k])
          else:
            self.log.error('Stats merge encountered a mismatch')
        
  def export_stats(self):
    if self.args.get('excel_filename'):
      fs_audit_export.export_xlsx(
          {
            'basic': self.stats,
            'detailed': self.stats_histogram,
            'other': self.other,
            'config': {
                'block_size': self.args.get('block_size'),
                'number_base': self.args.get('number_base'),
            }
          },
          self.args.get('excel_filename'),
      )
      
  def send_clients_db_consolidate_cmd(self):
    """
    Send all our connected clients the command to start processing the stats from
    their individual DBs. Mark each of the clients as having not yet returned
    the stats data
    """
    self.send_all_clients_command(CMD_RETURN_STATS_DB)
    for key in self.clients:
      self.clients[key]['stat_db_received'] = False
  
  def handle_client_connected(self, client):
    # Whenever a client connects, update their settings
    settings = {
      'path_depth_adj': self.args.get('path_depth_adj'),
      'reference_time': self.args.get('reference_time'),
      'prefix_paths': self.args.get('prefix_paths'),
      'work_paths': self.work_paths,                      # From base class
    }
    self.send_client_command(
        client, hydra.Client.EVENT_UPDATE_SETTINGS, {'settings': settings}
    )
    if self.args['db']['db_svr_name']:
      self.send_client_command(
        client, CMD_UPDATE_DB_SETTINGS, {'name': self.args['db']['db_name']}
      )
    if self.args.get(CMD_RECREATE_DB):
      self.send_client_command(client, CMD_RECREATE_DB)
    if self.state == STATE_DB_CONSOLIDATE:
      active_clients = len(self.clients)
      if active_clients >= self.args['clients_required']:
        self.event_queue.append({'c': EVENT_ALL_CLIENTS_CONN, 'd': None})
      else:
        self.log.debug("Connected clients: %d/%d"%(active_clients, self.args['clients_required']))
  
  def handle_extended_ui_cmd(self, event, data, src):
    if event == CMD_RECREATE_DB:
      self.args[CMD_RECREATE_DB] = True
      self.send_all_clients_command(CMD_RECREATE_DB)
    elif event == CMD_UPDATE_DB_SETTINGS:
      self.args['db']['db_name'] = data.get('name')
    elif event == CMD_STAT_CONSOLIDATE:
      self.args['clients_required'] = data.get('client_count')
      active_clients = len(self.clients)
      if active_clients >= self.args['clients_required']:
        self.event_queue.append({'c': EVENT_ALL_CLIENTS_CONN, 'd': None})
      self._set_state(STATE_DB_CONSOLIDATE)
    else:
      return False
    return True
    
  def _h_all_clients_conn(self, event, data, next_state, src):
    self.send_clients_db_consolidate_cmd()
    return next_state
  
  def _h_stats_db_received(self, event, data, next_state, src):
    all_stat_db_received = True
    client = src.get('id')
    for set in [self.clients, self.shutdown_clients]:
      for c in set:
        if c == client:
          set[c]['stat_db_received'] = True
          # Update the normal stats
          self._process_client_stats(client, data)
          # Save the histogram stats
          self.clients[client]['stats_advanced'] = data.get('stats_advanced')
          self.clients[client]['other'] = data.get('other', {})
        else:
          if not set[c].get('stat_db_received'):
            all_stat_db_received = False
    if all_stat_db_received:
      self.consolidate_stats()
      self.consolidate_stats_db()
      self.consolidate_other()
      self.export_stats()
      # After exporting stats, the server can terminate
      #self._set_state(hydra.Server.STATE_IDLE)
      # TODO: Should we shutdown or just wait?
      self.event_queue.append({'c': hydra.Server.EVENT_SHUTDOWN, 'd': None})
      return hydra.Server.STATE_IDLE
    return next_state


def main():
  cli_options = sys.argv[1:]
    
  # Create our command line parser. We use the older optparse library for compatibility with Python 2.7
  parser = optparse.OptionParser(
      usage=__usage__,
      description=__description__,
      version=__version__,
      formatter=hydra.IndentedHelpFormatterWithNL(),
  )
  # Create main CLI parser
  AddParserOptions(parser, cli_options)
  (options, args) = parser.parse_args(cli_options)
  if options.server is False and options.connect is None:
    parser.print_help()
    sys.exit(1)
  if options.server is False and options.connect is None:
    parser.print_help()
    print("===========\nYou must specify running as a server or client")
    sys.exit(1)
  if options.db_type == 'mongodb' and not pymongo:
    parser.print_help()
    print("===========\npymongo library not installed. Try installing with: pip install pymongo")
    sys.exit(1)

  log = ConfigureLogging(options)
    
  if options.server:
    log.info("Starting up the server")
    # Get paths to process from parsed CLI options
    proc_paths = hydra.get_processing_paths(options.path)
    # Get path prefix values
    prefix_paths = hydra.get_processing_paths([], options.path_prefix_file)
    if len(proc_paths) < 1 and options.stat_consolidate < 1:
      log.critical('A path via command line or stat_consolidate must be specified.')
      sys.exit(1)
    if prefix_paths:
      # If we are using prefix paths, we want to remove any absolute root path
      # as we assume all paths are relative to the prefix. We only strip off /
      proc_paths = [path[1:] if path[0] == os.sep else path for path in proc_paths]

    path_depth_adj = 0
    start_time = 0
    end_time = 0
    startup = True
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
    if len(proc_paths) > 0:
      path_depth_adj = path_depth(proc_paths[0])
      
    svr_args = dict(DEFAULT_CONFIG)
    svr_args.update(DEFAULT_STATS_CONFIG)
    svr_args.update({
      'logger_cfg': LOGGER_CONFIG,
      'dirs_per_idle_client': options.dirs_per_client,
      'select_poll_interval': options.select_poll_interval,
      # EXAMPLE:
      # Application specific variables
      'reference_time': time.time(),
      'path_depth_adj': path_depth_adj,
      'prefix_paths': prefix_paths,
      'default_stat_array_len': options.default_stat_array_len,
      'db': {
        'db_type': options.db_type,
        'db_name': options.db_name,
        'db_host': options.db_host,
        'db_port': options.db_port,
        'db_svr_name': options.db_svr_name,
      },
      'excel_filename': options.excel_output,
    })
    svr = hydra.ServerProcess(
        addr=options.listen,
        port=options.port,
        handler=ServerProcessor,
        args=svr_args,
    )

    # EXAMPLE:
    # Handle specific command line switches
    if options.db_svr_name:
      svr.send(CMD_UPDATE_DB_SETTINGS, {'name': options.db_name})
    if options.stat_consolidate > 0:
      svr.send(CMD_STAT_CONSOLIDATE, {'client_count': options.stat_consolidate})
    else:
      if options.recreate_db:
        svr.send(CMD_RECREATE_DB)
      # EXAMPLE:
      # The following line sends any paths from the CLI to the server to get ready
      # for processing as soon as clients connect. This could be done instead from
      # a UI or even via a TCP message to the server. For the purpose of this
      # example, this is the simplest method.
      svr.send(hydra.Server.EVENT_SUBMIT_WORK, {'paths': proc_paths})
    svr.start()
    
    while True:
      try:
        readable, _, _ = select.select([svr], [], [], options.stat_poll_interval)
        if len(readable) > 0:
          data = svr.recv()
          
          cmd = data.get('cmd')
          if cmd == hydra.Server.CMD_SVR_STATE:
            state = data['msg'].get('state')
            pstate = data['msg'].get('prev_state')
            log.info("Server state transition: %s -> %s"%(pstate, state))
            if state == hydra.Server.STATE_PROCESSING:
              if pstate == hydra.Server.STATE_IDLE:
                start_time = time.time()
                log.info("Time start: %s"%start_time)
            elif state == hydra.Server.STATE_IDLE:
              if startup:
                startup = False
              else:
                svr.send(hydra.Server.EVENT_QUERY_STATS, {'type': 'individual'})
                end_time = time.time()
                log.info("Time end: %s"%end_time)
                log.info("Total time: %s"%(end_time - start_time))
                # EXAMPLE:
                # The following line will shutdown the server when all
                # clients go idle. If you comment out the line the server
                # will continue to run and require some sort of interrupt of
                # this example program or a message to be sent to the server to
                # shutdown
                svr.send(hydra.Server.EVENT_SHUTDOWN)
            elif state == hydra.Server.STATE_SHUTDOWN:
              break
          elif cmd == hydra.Server.CMD_SVR_STATS:
            log.info(
              'UI received stats update (%s):\n%s'%(
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                json.dumps(
                    data['msg'].get('stats'),
                    ensure_ascii=False,
                    indent=4,
                    sort_keys=True,
                )
              )
            )
          else:
            log.info("UI received: %s"%cmd)
        else:
          log.info("Server wait timeout. Asking for stats update.")
          svr.send(hydra.Server.EVENT_QUERY_STATS)
      except KeyboardInterrupt as ke:
        log.info("Terminate signal received, shutting down")
        break
      except Exception as e:
        log.exception(e)
        break
    log.debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
    svr.terminate()
  else:
    log.info("Starting up client")
    client_args = dict(DEFAULT_CONFIG)
    client_args.update(DEFAULT_STATS_CONFIG)
    client_args.update({
        # Basic arguments
        'svr': options.connect,
        'port': options.port,
        'handler': ClientProcessor,
        'file_handler': WorkerHandler,
        # Options
        'logger_cfg': LOGGER_CONFIG,
        'dirs_per_idle_worker': options.dirs_per_worker,
        'select_poll_interval': options.select_poll_interval,
        'source_addr': options.src_addr,
        'source_port': options.src_port,
        # EXAMPLE:
        # Application specific variables
        'default_stat_array_len': options.default_stat_array_len,
        'db': {
          'db_type': options.db_type,
          'db_name': options.db_name,
          'db_host': options.db_host,
          'db_port': options.db_port,
          'db_containers': DEFAULT_CONFIG['db_containers'],
          'db_svr_name': options.db_svr_name,
          'recreate_db': options.recreate_db,
        }
    })
    client = hydra.ClientProcess(client_args)
    client.set_workers(options.num_workers)
    client.start()
    log.info("Waiting until client exits")
    client.join()
    log.info("Client exiting")

if __name__ == "__main__" or __file__ == None:
    multiprocessing.freeze_support()                  # Support scripts built into executable on Windows
    if hasattr(multiprocessing, 'set_start_method'):  # Python 3.4+
      multiprocessing.set_start_method('spawn')       # Force all OS to behave the same when spawning new process
    main()
