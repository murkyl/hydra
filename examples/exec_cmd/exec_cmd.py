# -*- coding: utf8 -*-
__title__ = "exec_cmd"
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
__usage__="""
  {p} [options] -s <-p|--path_file start_path> [options] [commands]
  {p} [options] -c <ip_or_fqdn_of_server> [options]""".format(p=__title__)
__description__="""Requirements:
  python 2.7+

Description:
  {p} will recursively run [commands] on every subdirectory starting from
  <start_path>. The string '{{}}' will be replaced with the current directory
  anywhere it is found in the [commands].
  The [commands] will be split among multiple threads and possibly multiple
  nodes to increase the speed of processing.
  
  The script uses a full shell to execute the command. If you require a file
  glob to process all files in a directory, include it into the command.

  Example commands:
    chown someusr {{}}/*
    chmod +a user someuser allow dir_gen_all {{}}/*""".format(p=__title__)

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
import subprocess
import re

try:
  bytes('A', encoding='utf-8')
  TO_BYTES = lambda x: bytes(x, encoding='utf-8')
except:
  TO_BYTES = lambda x: bytes(x)
# EXAMPLE:
# You can add arguments that worker processes should have by default here
DEFAULT_CONFIG = {
}

# EXAMPLE:
# Additional simple counting stats. Adding stats here requires the code below to
# increment the counters somewhere.
EXTRA_BASIC_STATS = [
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
  op_group = optparse.OptionGroup(parser, "Server settings")
  op_group.add_option("--server", "-s",
                    action="store_true",
                    default=False,
                    help="Act as the Hydra server.")
  op_group.add_option("--listen",
                    default=None,
                    help="IP address to bind to when run as a server. The default will listen to all interfaces.")
  op_group.add_option("--port",
                    default=hydra.Utils.DEFAULT_LISTEN_PORT,
                    help="Port to listen when running as a server and port to connect to as a client.")
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
  op_group.add_option("--log_stderr",
                    action="store_true",
                    default=False,
                    help="Enable to log stderr from command execution.")
  parser.add_option_group(op_group)

  # EXAMPLE: Add or alter options specific for your application here

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
                    help="Log to this file instead of the console.")
  op_group.add_option("--audit", "-a",
                    default=None,
                    help="Log audit events to this file instead of the console.")
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
    if self.args['logger_cfg']['handlers']['audit']['filename']:
      match = re.match(r'.*\:(?P<id>[0-9]+)', self.name)
      if match:
        match_dict = match.groupdict()
      else:
        match_dict = {}
      base, ext = os.path.splitext(self.args['logger_cfg']['handlers']['audit']['filename'])
      worker_audit_file = "%s-%s%s"%(base, match_dict.get('id', '1'), ext)
      self.audit.handlers = [logging.handlers.RotatingFileHandler(
        worker_audit_file,
        backupCount=5,
      )]
    self.audit.propagate = False
    
  def init_stats(self):
    super(WorkerHandler, self).init_stats()
    # EXAMPLE:
    # Extra simple counting stats need to be initialized properly
    for s in EXTRA_BASIC_STATS:
      self.stats[s] = 0
      
  def filter_subdirectories(self, root, dirs, files):
    """
    We will always return no files to process here because we are calling the exec function with a glob for all files
    in the directory so processing individual files is not necessary.
    """
    self.stats['processed_files'] += len(files)
    self.stats['filtered_files'] += len(files)
    return dirs, []
    
  def handle_directory_pre(self, dir):
    with open(os.devnull, 'w') as devnull:
      try:
        raw_cmd = ' '.join(self.args['exec_cmd'])
        cmd = raw_cmd.replace('{}', dir)
        self.log.debug("Command to run: %s"%cmd)
        proc = subprocess.Popen(
          cmd,
          shell=True,
          stdout=devnull,
          stderr=subprocess.PIPE)
        (proc_out, proc_err) = proc.communicate()
        if proc.returncode != 0:
          self.log.error("Non-zero (%d) return code on directory: %s"%(proc.returncode, dir))
          self.log.error("CMD run: %s"%cmd)
          if self.args.get('log_stderr'):
            self.log.error("Command STDERR: %s"%proc_err)
      except Exception as e:
        self.log.error("Command error: %s"%e)
    return False
    
  #def handle_file(self, dir, file):
  #  return True
    
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
  if options.server and not args:
    parser.print_help()
    print("===========\nYou must specify a command to execute in each directory")
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
    svr_args = {
        'logger_cfg': LOGGER_CONFIG,
        'dirs_per_idle_client': options.dirs_per_client,
        'select_poll_interval': options.select_poll_interval,
        # EXAMPLE:
        # Application specific variables
        'exec_cmd': args,
        'log_stderr': options.log_stderr,
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
