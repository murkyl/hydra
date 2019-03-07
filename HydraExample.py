# -*- coding: utf8 -*-
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
from HydraWorker import HydraWorker
from HydraClient import HydraClient
from HydraClient import HydraClientProcess
from HydraServer import HydraServer
from HydraServer import HydraServerProcess


__title__ = "HydraExample"
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


MODULES = [
  "DateCheck",
  "DeleteOldFiles",
  "FileNameCheck",
]

'''
Generic server processor
'''
class ServerProcessor(HydraServer):
  def consolidate_stats(self):
    super(ServerProcessor, self).consolidate_stats()
   
'''
An example of how to extend Hydra to check file dates
'''
class DateCheck(HydraWorker):
  def __init__(self, args={}):
    super(DateCheck, self).__init__(args)
    self.reinit(args)
    
  def reinit(self, args={}):
    # Setup the time difference in seconds from now
    self.time_delta_sec = args['time_delta']
    self.now = time.time()
    # You can configure additional loggers by adding new variables and using
    # the correct logger name
    self.audit = logging.getLogger('audit')
    
  def handle_file(self, dir, file):
    """
    Fill in docstring
    """
    #self.audit.info("File: %s"%file)
    file_lstats = os.lstat(os.path.join(dir, file))
    # Compare the modified time to find a file that is older than X
    if (file_lstats.st_mtime + self.time_delta_sec) < self.now:
      self.audit.info("Modified time older: %s/%s"%(dir, file))
      # EXAMPLE:
      # You can remove a file that has a modification time older than X
      #os.unlink(os.path.join(dir, file))
    # Compare the creation time to find a file that is older than X
    if (file_lstats.st_ctime + self.time_delta_sec) < self.now:
      self.audit.info("Create time older: %s/%s"%(dir, file))
    # Compare the access time to find a file that is older than X
    if (file_lstats.st_atime + self.time_delta_sec) < self.now:
      self.audit.info("Access time older: %s/%s"%(dir, file))
    return True
    
class DateCheckProcessor(HydraClient):
  def consolidate_stats(self):
    super(DateCheckProcessor, self).consolidate_stats()
    
'''
Check if a file name is in UTF8 format
'''
class FileNameCheck(HydraWorker):
  def __init__(self, args={}):
    super(FileNameCheck, self).__init__(args)
    self.max_file_name_bytes = 255
    self.max_smb_file_path = 260
    self.check_utf8 = True
    self.detect_encoding = True
    self.output_name_in_hex = False
    # You can configure additional loggers by adding new variables and using
    # the correct logger name
    self.audit = logging.getLogger('audit')
    
  def handle_file(self, dir, file):
    """
    Fill in docstring
    """
    if len(file) > self.max_file_name_bytes:
      self.audit.info("Exceeded file name length (%d): %s"%(len(file), os.path.join(dir, file)))
    
    # This UTF8 file name check works on Linux and over NFS. It does not work
    # on a Windows machine as Windows translates the invalid characters into
    # some UTF8 code point by default
    if self.check_utf8:
      try:
        file.decode('utf-8')
        if self.output_name_in_hex:
          ba = bytearray()
          for c in file:
            ba.append(ord(c))
          self.audit.info("File name (hex) %s"%":".join("{:02x}".format(c) for c in ba))
      except Exception as e:
        found_enc = None
        if self.detect_encoding:
          for enc in ['shift-jis', 'big5', 'euc_kr']:
            try:
              decoded = file.decode(enc)
              found_enc = enc
              break
            except:
              pass
        if not found_enc:
          found_enc = 'Unknown'
        self.audit.info("Non UTF-8 file (Guess encoding: %s): %s"%(found_enc, os.path.join(dir, file)))
    return True

class FileNameCheckProcessor(HydraClient):
  def consolidate_stats(self):
    super(FileNameCheckProcessor, self).consolidate_stats()
    
'''
Extend Hydra to delete old files
'''
class DeleteOldFiles(HydraWorker):
  def __init__(self, args={}):
    super(DeleteOldFiles, self).__init__(args)
    self.reinit(args)
    
  def reinit(self, args={}):
    # Setup the time difference in seconds from now
    self.time_delta_sec = args['time_delta']
    self.now = time.time()
    # You can configure additional loggers by adding new variables and using
    # the correct logger name
    self.audit = logging.getLogger('audit')
  
  def init_stats(self):
    super(DeleteOldFiles, self).init_stats()
    for s in ['deleted_files']:
      self.stats[s] = 0
  
  def handle_file(self, dir, file):
    """
    Delete files older than a certain time.
    The default uses the last modified time.
    Uncomment the desired section
    """
    delete_file = False
    file_lstats = os.lstat(os.path.join(dir, file))
    
    '''Compare the modified time to find a file that is older than X'''
    if (file_lstats.st_mtime + self.time_delta_sec) < self.now:
      delete_file = True
      
    '''Compare the creation time to find a file that is older than X'''
    #if (file_lstats.st_ctime + self.time_delta_sec) < self.now:
    #  delete_file = True
    
    '''Compare the access time to find a file that is older than X'''
    #if (file_lstats.st_atime + self.time_delta_sec) < self.now:
    #  delete_file = True
    
    if delete_file:
      os.unlink(os.path.join(dir, file))
      self.audit.info("%s/%s"%(dir, file))
      self.stats['deleted_files'] += 1
    return True
    
class DeleteOldFilesServerProcessor(HydraServer):
  def init_stats(self, stat_state):
    super(DeleteOldFilesServerProcessor, self).init_stats(stat_state)
    for s in ['deleted_files']:
      stat_state[s] = 0
  
  def consolidate_stats(self):
    super(DeleteOldFilesServerProcessor, self).consolidate_stats()
    try:
      for set in [self.clients, self.shutdown_clients]:
        for c in set:
          if not set[c]['stats']:
            continue
          for s in ['deleted_files']:
            self.stats[s] += set[c]['stats'][s]
    except Exception as e:
      self.log.exception(e)
   
class DeleteOldFilesProcessor(HydraClient):
  def init_stats(self, stat_state):
    super(DeleteOldFilesProcessor, self).init_stats(stat_state)
    for s in ['deleted_files']:
      stat_state[s] = 0
  
  def consolidate_stats(self):
    super(DeleteOldFilesProcessor, self).consolidate_stats()
    try:
      for set in [self.workers, self.shutdown_pending, self.shutdown_workers]:
        for w in set:
          if not set[w]['stats']:
            continue
          for s in HydraUtils.BASIC_STATS + ['deleted_files']:
            self.stats[s] += set[w]['stats'][s]
    except Exception as e:
      self.log.exception(e)

'''
Add command line options
'''
def AddParserOptions(parser, raw_cli):
    parser.add_option("--module", "-m",
                      action="store",
                      type="string",
                      default=None,
                      help="Select one of the modules to run: %s"%",".join(MODULES))
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
    op_group = optparse.OptionGroup(parser, "Logging, auditing and debug",
                           "File names support some variable replacement. {pid} will be replaced "
                           "with the PID of the process. {host} will be replaced by the host name of "
                           "the machine running the script. All variable substitutions for strftime "
                           "are also available for use.")
    op_group.add_option("--log", "-l",
                      default=None,
                      help="If specified, we will log to this file instead of the console. This is "
                           "required for logging on Windows platforms.")
    op_group.add_option("--log_format",
                      default="%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s",
                      help="Format for log output. Follows Python standard logging library. [Default: %default]")
    op_group.add_option("--audit", "-a",
                      default=None,
                      help="If specified, we will log audit events to this file instead of the console.")
    op_group.add_option("--audit_format",
                      default="%(message)s",
                      help="Format for audit output. Follows Python standard logging library. [Default: %default]")
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

  # Setup logging and use the --debug CLI option to set the logging level
  log = HydraUtils.setup_logger(None, options.log_format, options.debug, options.log)
  # Setup auditing logger. Use this to output results to a separate file than
  # the normal logger
  audit = HydraUtils.setup_logger('audit', options.audit_format, 0, options.audit)
  # EXAMPLE:
  # You can add additional loggers to customize your output here. In the file
  # handler module init routine, just call getLogger('<name>') with the logger
  # name you create here.
  
  # Get paths to process from parsed CLI options
  proc_paths = HydraUtils.get_processing_paths(options.path, options.path_file)
  
  if options.module == 'DateCheck':
    svr_handler = ServerProcessor
    handler = DateCheckProcessor
    handler_args = {}
    file_handler = DateCheck
    file_handler_args = {
        'time_delta': datetime.timedelta(days=20).total_seconds(),
    }
  elif options.module == 'DeleteOldFiles':
    svr_handler = DeleteOldFilesServerProcessor
    handler = DeleteOldFilesProcessor
    handler_args = {}
    file_handler = DeleteOldFiles
    file_handler_args = {
        'time_delta': datetime.timedelta(days=20).total_seconds(),
    }
  elif options.module == 'FileNameCheck':
    svr_handler = ServerProcessor
    handler = FileNameCheckProcessor
    handler_args = {}
    file_handler = FileNameCheck
    file_handler_args = {}
  else:
    log.critical("A module must be selected")
    parser.print_help()
    sys.exit(1)
  if options.server:
    log.info("Starting up the server")
    start_time = 0
    end_time = 0
    startup = True
    svr = HydraServerProcess(
        addr=options.listen,
        port=options.port,
        handler=svr_handler,
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
                svr.send({'cmd': 'shutdown'})
            elif state == 'shutdown':
              break
      except KeyboardInterrupt as ke:
        logging.getLogger().info("Terminate signal received, shutting down")
        break
      except Exception as e:
        logging.getLogger().exception(e)
        break
    logging.getLogger().debug("Waiting for shutdown up to 10 seconds")
    svr.join(10)
  else:
    log.info("Starting up client")    
    client = HydraClientProcess({
        'svr': options.connect,
        'port': options.port,
        'handler': handler,
        'handler_args': handler_args,
        'file_handler': file_handler,
        'file_handler_args': file_handler_args,
    })
    client.start()
    log.info("Waiting until client exits")
    client.join()
    log.info("Client exiting")
  
if __name__ == "__main__" or __file__ == None:
    multiprocessing.freeze_support()
    main()
