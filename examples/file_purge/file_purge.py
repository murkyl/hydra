# -*- coding: utf8 -*-
__title__ = "file_purge"
__version__ = "1.0.0"
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
__description__="""Requirements:
  python 2.7+

Description:
  Server example usage:
  python {p} -s -p /some/path --date 30 --mtime --purge

  Search all files under /some/path directory and check if the file mtime (file
  last modified time) is older than 30 days in the past. If it is then delete
  the file.
  If the current date is July 20, 1969 (1969-07-20) at 08:00:05, then a setting
  of --date 30 means that any file before 1969-06-20 at 08:00:05 would be
  deleted.
  -----
  python {p} -s -p /some/path --date 30 --mtime

  In this instance, without the --purge option, files will not be deleted.
  This mode is like a simulation of what files would be deleted.
  -----
  python {p} -s -p /a/path -p /b/path --date 365 --mtime --log server.log

  This instance will process 2 directories, /some/path and /some/other_path as
  well as deleting files that have mtimes older than 1 year ago.
  All program output will be redirected to a file called server.log. This will
  will be automatically rotated if it exists with up to 10 copies retained. It
  is therefore safe to re-use the same log file name.

  Once a server instance is running. You need to connect one or more clients
  to start file processing. The clients can be located on the same machines as
  the server or it can be located on other machines. There is an assumption
  that the paths that are passed to the server instance will be reachable by
  all the clients via the same path.

  ====================
  Client example usage:
  python {p} -c 127.0.0.1

  This is the simplest invocation of the client. The client will connect to a
  server running on the local machine.
  -----
  python {p} -c 192.168.42.42

  This invocation will connect to a server running at IP 192.168.42.42
  -----
  python {p} -c 192.168.42.42 --audit audit.log

  Connect to a server running on 192.168.42.42 and write any audit events to
  the file named audit.log. This file will be created on the machine that is
  running the client and in the current working directory.""".format(p=__title__)


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
# On Windows systems check for WindowsError for platform specific exception handling
try:
  dir(WindowsError)
except:
  class WindowsError(OSError): pass
# EXAMPLE:
# Add any additional imports
import json
import datetime

# EXAMPLE:
# You can add arguments that worker processes should have by default here
DEFAULT_CONFIG = {
  'compare_atime': False,
  'compare_ctime': False,
  'compare_mtime': False,
  'time_delta_sec': 0,
  'current_time': 0,
  'purge': False,
  'block_size': 8192,
}

# EXAMPLE:
# Additional simple counting stats. Adding stats here requires the code below to
# increment the counters somewhere.
EXTRA_BASIC_STATS = [
  'file_size_logical_scanned',
  'file_size_logical_512byte_block_scanned',
  'file_size_logical_8kbyte_block_scanned',
  'deleted_files',
  'deleted_files_size_logical_total',
  'deleted_files_size_512byte_block_total',
  'deleted_files_size_8kbyte_block_total',
]

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
  parser.add_option_group(op_group)

  op_group = optparse.OptionGroup(parser, "[Server] Processing options",
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

  # EXAMPLE: Add or alter options specific for your application here
  op_group = optparse.OptionGroup(parser, "[Server] Delete file criteria")
  op_group.add_option("--date", "-d",
                    help="String the describes at what point in time should a file be considered eligible for deletion."
                        "Currently it only supports the number of days in the past to compare. e.g. 20 or 30 or 365.")
  op_group.add_option("--purge",
                    action="store_true",
                    default=False,
                    help="Flag to purge files that match date and timestamp criteria.")
  op_group.add_option("--atime",
                    action="store_true",
                    default=False,
                    help="Compare atime (last access time) of file to determine if a file should be deleted.")
  op_group.add_option("--ctime",
                    action="store_true",
                    default=False,
                    help="Compare ctime (last file metadata change time) of file to determine if a file should be deleted. The ctime value normally changes when file metadata or file contents change.")
  op_group.add_option("--mtime",
                    action="store_true",
                    default=False,
                    help="Compare mtime (last file modified time) of file to determine if a file should be deleted. The mtime value changes when the contents of the file change.")
  parser.add_option_group(op_group)

  op_group = optparse.OptionGroup(parser, "[Client] Tuning parameters")
  op_group.add_option("--num_workers", "-n",
                    type="int",
                    default=0,
                    help="For clients, specifies the number of worker processes to launch. A value of 0 will have"
                         " the system set this to the number of CPU cores available. [Default: %default]")
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
  parser.add_option_group(op_group)

  op_group = optparse.OptionGroup(parser, "Logging, auditing and debug")
  op_group.add_option("--log", "-l",
                    default=None,
                    help="If specified, we will log to this file instead of the console. This is "
                         "required for logging on Windows platforms.")
  op_group.add_option("--audit", "-a",
                    default=None,
                    help="If specified, we will log audit events to this file instead of the console.")
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

def parse_date_input(date_str):
  try:
    num_days = int(date_str)
  except:
    print("Could not parse the --date option. Please only provide a number in days.")
    sys.exit(1)
  return datetime.timedelta(days=num_days).total_seconds()


class WorkerHandler(hydra.WorkerClass):
  def __init__(self, args={}):
    super(WorkerHandler, self).__init__(args)
    self.args = dict(DEFAULT_CONFIG)
    self.args.update(args)
    # EXAMPLE:
    # You can configure additional loggers by adding new variables and using
    # the correct logger name
    self.audit = logging.getLogger('audit')
    
  def init_process(self):
    # EXAMPLE:
    # Add any initialization that is required after worker starts

    # Set the audit log level to INFO, otherwise only WARNING and above get logged
    self.audit.setLevel(logging.INFO)
    
  def init_stats(self):
    super(WorkerHandler, self).init_stats()
    # EXAMPLE:
    # Extra simple counting stats need to be initialized properly
    for s in EXTRA_BASIC_STATS:
      self.stats[s] = 0
      
  def filter_subdirectories(self, root, dirs, files):
    return dirs, files
    
  def handle_directory_pre(self, dir):
    return False
    
  def handle_file(self, dir, file):
    """
    Delete files older than a certain time determined by the
    self.args['time_delta_sec'] argument
    """
    # EXAMPLE:
    # Add your application code to handle each file
    delete_file = False
    time_delta = self.args['time_delta_sec']
    current_time = self.args['current_time']
    full_path_file = os.path.join(dir, file)
    try:
      file_lstats = os.lstat(full_path_file)
    except WindowsError as e:
      if e.winerror == 3 and len(full_path_file) > 255:
        self.log.error('Unable to stat file due to path length > 255 characters. Try setting HKLM\System\CurrentControlSet\Control\FileSystem\LongPathsEnabled to 1')
        return False
    file_lstats = os.lstat(os.path.join(dir, file))
    
    '''Compare the access time to find a file that is older than X'''
    if self.args['compare_atime']:
      if (file_lstats.st_atime + time_delta) < current_time:
        delete_file = True
    '''Compare the creation time to find a file that is older than X'''
    if self.args['compare_ctime']:
      if (file_lstats.st_ctime + time_delta) < current_time:
        delete_file = True
    '''Compare the modified time to find a file that is older than X'''
    if self.args['compare_mtime']:
      if (file_lstats.st_mtime + time_delta) < current_time:
        delete_file = True
    
    fsize = file_lstats.st_size
    bs = self.args['block_size']
    block_fsize = (fsize//bs + (not not fsize%bs))*bs
    block_512_fsize = (fsize//512 + (not not fsize%512))*512
    self.stats['file_size_logical_scanned'] += fsize
    self.stats['file_size_logical_512byte_block_scanned'] += block_512_fsize
    self.stats['file_size_logical_8kbyte_block_scanned'] += block_fsize
    if delete_file:
      if self.args.get('purge'):
        try:
          os.unlink(full_path_file)
          self.audit.info(full_path_file)
        except:
          self.log.warn('Error deleting file: %s'%full_path_file)
      else:
        self.audit.info('Simulate delete: %s'%full_path_file)
      self.stats['deleted_files'] += 1
      self.stats['deleted_files_size_logical_total'] += fsize
      self.stats['deleted_files_size_512byte_block_total'] += block_512_fsize
      self.stats['deleted_files_size_8kbyte_block_total'] += block_fsize
    return True
    
  def handle_update_settings(self, cmd):
    self.args.update(cmd['settings'])
    return True
    
'''
EXAMPLE:
The client and server processors need to be modified if you have custom
statistics or if you want to add additional functionality
'''
class ClientProcessor(hydra.ClientClass):
  def init_stats(self, stat_state):
    super(ClientProcessor, self).init_stats(stat_state)
    for s in EXTRA_BASIC_STATS:
      stat_state[s] = 0
  
  def consolidate_stats(self):
    super(ClientProcessor, self).consolidate_stats()
    for set in [self.workers, self.shutdown_pending, self.shutdown_workers]:
      for w in set:
        if not set[w]['stats']:
          continue
        for s in EXTRA_BASIC_STATS:
          self.stats[s] += set[w]['stats'][s]
  
  def handle_update_settings(self, cmd):
    super(ClientProcessor, self).handle_update_settings(cmd)
    self.args.update(cmd['settings'])
    return True
    
class ServerProcessor(hydra.ServerClass):
  def init_stats(self, stat_state):
    super(ServerProcessor, self).init_stats(stat_state)
    for s in EXTRA_BASIC_STATS:
      stat_state[s] = 0
  
  def consolidate_stats(self):
    super(ServerProcessor, self).consolidate_stats()
    for set in [self.clients, self.shutdown_clients]:
      for c in set:
        if not set[c]['stats']:
          continue
        for s in EXTRA_BASIC_STATS:
          self.stats[s] += set[c]['stats'][s]
    return self.stats
          
  def handle_client_connected(self, client):
    self.send_client_command(
        client,
        hydra.Client.EVENT_UPDATE_SETTINGS,
        {
          'settings': self.args,
        }
    )
   

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
    print("===========\nYou must specify running as a server or client")
    sys.exit(1)
  # EXAMPLE: Add option validation code
  if options.server and not options.date:
    parser.print_help()
    print("===========\nYou must specify the --date argument")
    sys.exit(1)
  if options.server and not (options.mtime or options.atime or options.ctime):
    parser.print_help()
    print("===========\nYou must specify one or more of --mtime, --atime or --ctime")
    sys.exit(1)

  log = ConfigureLogging(options)
  stats = logging.getLogger('stats')
  
  if options.server:
    log.info("Starting up the server")
    # Get paths to process from parsed CLI options
    proc_paths = hydra.get_processing_paths(options.path, options.path_file)
    if not proc_paths or len(proc_paths) < 1:
      log.critical('A path via command line or a path file must be specified.')
      sys.exit(1)
    # EXAMPLE:
    # There is an assumption that all clients have their clocks synchronized.
    current_time = time.time()
    # Parse date field to get number of seconds in the past to use for comparison
    time_delta_sec = parse_date_input(options.date)
    log.debug("Time delta in seconds: %s"%time_delta_sec)
    log.debug("Current time: %s"%current_time)
    svr_args = {
        'logger_cfg': LOGGER_CONFIG,
        'dirs_per_idle_client': options.dirs_per_client,
        'select_poll_interval': options.select_poll_interval,
        # EXAMPLE:
        # Application specific variables
        'current_time': current_time,
        'time_delta_sec': time_delta_sec,
        'compare_atime': options.atime,
        'compare_ctime': options.ctime,
        'compare_mtime': options.mtime,
        'block_size': DEFAULT_CONFIG['block_size'],
        'purge': options.purge,
    }

    start_time = 0
    end_time = 0
    svr = hydra.ServerProcess(
        addr=options.listen,
        port=options.port,
        handler=ServerProcessor,
        args=svr_args,
    )
    svr.start()
    # EXAMPLE:
    # The following line sends any paths from the CLI to the server to get ready
    # for processing as soon as clients connect. This could be done instead from
    # a UI or even via a TCP message to the server. For the purpose of this
    # example, this is the simplest method.
    svr.send(hydra.Server.EVENT_SUBMIT_WORK, {'paths': proc_paths})
    
    ui_state = 'process'
    while True:
      try:
        readable, _, _ = select.select([svr], [], [])
        if len(readable) > 0:
          cmd = svr.recv()
          log.debug("UI received: %s"%cmd)
          if ui_state == 'process':
            if cmd.get('cmd') == hydra.Server.CMD_SVR_STATE:
              state = cmd['msg'].get('state')
              pstate = cmd['msg'].get('prev_state')
              log.debug("Server state transition: %s -> %s"%(pstate, state))
              if state == hydra.Server.STATE_IDLE:
                if pstate == hydra.Server.STATE_PROCESSING:
                  end_time = time.time()
                  stats.info("Time end: %s"%end_time)
                  stats.info("Total time: %s"%(end_time - start_time))
                  svr.send(hydra.Server.EVENT_QUERY_STATS, {'data': 'individual_clients'})
                  ui_state = 'stats'
                elif pstate == hydra.Server.STATE_INIT:
                  start_time = time.time()
                  stats.info("Time start: %s"%start_time)
            elif cmd.get('cmd') == hydra.Server.CMD_SVR_STATS:
              stats.info(json.dumps(cmd['msg'].get('stats'), indent=4, sort_keys=True))
          elif ui_state == 'stats':
            if cmd.get('cmd') == hydra.Server.CMD_SVR_STATS:
              stats.info('========== Final stats output ==========')
              stats.info(json.dumps(cmd['msg'].get('stats'), indent=4, sort_keys=True))
              svr.send(hydra.Server.EVENT_SHUTDOWN)
              break
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
    client_args = {
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
    }
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
