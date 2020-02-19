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

FILE_AGE_HISTOGRAM = [# File size histogram table to see how many files fall within each date range (in seconds)
  0,                  # Sometime in the future
  60,                 # Within last minute
  3600,               # Within 1 hour
  86400,              # Within 1 day
  604800,             # Within 1 week
  2592000,            # Within 30 days
  5184000,            # Within 60 days
  7776000,            # Within 90 days
  15552000,           # Within 180 days
  31536000,           # Within 1 year (365 days)
  63072000,           # Within 2 years (365*2 days)
  940608000,          # Within 3 years (365*3 days)
  126144000,          # Within 4 years (365*4 days)
  157680000,          # Within 5 years (365*5 days)
  315360000,          # Within 10 years (365*10 days)
  #'other',           # > 10 years
]

DEFAULT_CONFIG = {
  'top_n_count': 100,
  'max_user_stats': 0,
  'file_size_historgram': FILE_SIZE_HISTOGRAM,
  'file_age_histogram': FILE_AGE_HISTOGRAM,
  'cache_size': 10000,
  'db_name': 'fs_audit',
}

'''
Generic server processor
'''
class ServerProcessor(HydraServer):
  def consolidate_stats(self):
    super(ServerProcessor, self).consolidate_stats()

  def handle_extended_client_cmd(self, cmd):
    print("Got extended client cmd in client processor: %s"%cmd)
    return True
    
  def handle_extended_server_cmd(self, cmd):
    print("Got extended server cmd in server processor")
    return True
   
'''
An example of how to extend Hydra to check file dates
'''
class FSAudit(HydraWorker):
  def __init__(self, args={}):
    super(FSAudit, self).__init__(args)
    self.args = dict(DEFAULT_CONFIG)
    self.args.update(args)
    self.global_stats = {}
    self.per_user_stats = {}
    self.stats_init(self.global_stats)
    self.cache = [None]*self.args.get('cache_size')
    self.cache_idx = 0
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
    
  def stats_init(self, stat_obj):
    stat_obj['hist_file_size_config'] = self.args.get('hist_file_size_config')
    stat_obj['hist_file_atime_config'] = self.args.get('hist_file_atime_config')
    stat_obj['hist_file_mtime_config'] = self.args.get('hist_file_mtime_config')
    stat_obj['hist_file_ctime_config'] = self.args.get('hist_file_ctime_config')
    stat_obj['topn_file_size_by_uid_count'] = self.args.get('topn_file_size_by_uid_count')
    stat_obj['topn_file_size_by_gid_count'] = self.args.get('topn_file_size_by_gid_count')
    stat_obj['topn_file_size_count'] = self.args.get('topn_file_size')
    #stat_obj['hist_file_count_by_size'] = HistogramStatCountAndValue(self.hist_file_size_config)
    #stat_obj['hist_file_count_by_atime'] = HistogramStat2D(self.hist_file_atime_config, self.hist_file_size_config)
    #stat_obj['hist_file_count_by_mtime'] = HistogramStat2D(self.hist_file_mtime_config, self.hist_file_size_config)
    #stat_obj['hist_file_count_by_ctime'] = HistogramStat2D(self.hist_file_ctime_config, self.hist_file_size_config)
    #stat_obj['topn_file_size'] = []
    stat_obj['extensions'] = HashBinCountAndValue()
    stat_obj['category'] = HashBinCountAndValue()
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
    # Number of files in current directory = len(files)
    # Can use this to find average number of files/directory or avg width of directories
    #self.audit.info(root)
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

      file_data = {
        'filename': full_path_file,
        'ext': os.path.splitext(full_path_file)[1].replace('.', ''),
        'filesize': file_lstats.st_size,
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
    #  self.hist_file_count_by_size.insert_data(file_lstats.st_size)
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
    return True
    
  def handle_stats_collection(self):
    self.flush_cache()
    
class FSAuditProcessor(HydraClient):
  def __init__(self, worker_class, args={}):
    super(FSAuditProcessor, self).__init__(worker_class, args)
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
    
  #def consolidate_stats(self):
  #  print("Client consolidating stats")
  #  super(FSAuditProcessor, self).consolidate_stats()
    
  def handle_extended_server_cmd(self, raw_data):
    print("Got extended server cmd in client processor")
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
                      action="append",
                      help="Path to scan. Multiple --path options will queue up paths for processing. "
                           "Use of full paths is recommended as clients will interpret this path according to their "
                           "own current working directory."
                           "This option is additive with the --path_file option with exact duplicate paths removed.")
    op_group.add_option("--path_file", "-f",
                      default=None,
                      help="File name with a CR/LF separated list of paths to process. Any leading or trailing "
                           "whitespace is preserved.")
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
  parser = optparse.OptionParser(version="%prog "+__version__)
  # Create main CLI parser
  AddParserOptions(parser, cli_options)
  (options, args) = parser.parse_args(cli_options)
  if options.server is False and options.connect is None:
    parser.print_help()
    print("You must specify running as a server or client")
    sys.exit(1)

  # Setup logging and use the --debug CLI option to set the logging level
  if options.debug > 3:
    log_level = 5
  elif options.debug > 2:
    log_level = 9
  elif options.debug > 1:
    log_level = logging.DEBUG
  elif options.debug > 0:
    log_level = logging.INFO
  else:
    log_level = logging.WARNING
  logger_config = dict(HydraUtils.LOGGING_CONFIG)
  HydraUtils.config_logger(logger_config, '', log_level=log_level, file=options.log)
  # Setup auditing logger. Use this to output results to a separate file than
  # the normal logger
  #DEL: HydraUtils.config_logger(logger_config, 'audit', log_level=0, file=options.audit)
  if options.audit:
    logger_config['loggers']['audit']['handlers'] = ['audit']
    logger_config['handlers']['audit']['file'] = options.audit
  logging.config.dictConfig(logger_config)
  log = logging.getLogger('')
  audit = logging.getLogger('audit')
  
  
  # Get paths to process from parsed CLI options
  proc_paths = HydraUtils.get_processing_paths(options.path, options.path_file)

  svr_handler = ServerProcessor
  handler = FSAuditProcessor
  handler_args = {}
  file_handler = FSAudit
  if options.server:
    log.info("Starting up the server")
    if len(proc_paths) < 1:
      log.critical('A path via command line or a path file must be specified.')
      sys.exit(1)

    start_time = 0
    end_time = 0
    startup = True
    svr = HydraServerProcess(
        addr=options.listen,
        port=options.port,
        handler=svr_handler,
        args={
          'logger_cfg': logger_config,
          'dirs_per_idle_client': options.dirs_per_client,
          'select_poll_interval': options.select_poll_interval,
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
          cmd = svr.recv()
          log.info("UI received: %s"%cmd)
          if cmd.get('cmd') == 'state':
            state = cmd.get('state')
            pstate = cmd.get('prev_state')
            if state == 'processing':
              log.info("Server state transition: %s -> %s"%(pstate, state))
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
                # EXAMPLE:
                # The following line will shutdown the server when all
                # clients go idle. If you comment out the line the server
                # will continue to run and require some sort of interrupt of
                # this example program or a message to be sent to the server to
                # shutdown
                print("DEBUG: Asking for full histogram")
                svr.send({'cmd': 'generate_full_histogram'})
                time.sleep(5)
                svr.send({'cmd': 'shutdown'})
            elif state == 'shutdown':
              break
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
        'handler': handler,
        'file_handler': file_handler,
        # Options
        'logger_cfg': logger_config,
        'dirs_per_idle_worker': options.dirs_per_worker,
        'select_poll_interval': options.select_poll_interval,
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
