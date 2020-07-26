# hydra
Parallel file system walker framework

# Overview

Hydra has a client/server model for operation. The server does not do any actual processing but instead coordinates
 directories to process between one or more clients. The client and server communications are performed over raw TCP
 sockets using a simple internal messaging system. Each client starts up in a listening mode for a connection from the
 server. Once the server completes connecting to all configured clients, processing begins.
 
The server takes the configured path and sends it to one client for processing. Clients that do not have any work or
 have finished all their directories will request the server to send them additional directories to process. When the
 server receives a work request from an idle client it will turn around and send a request to all non-idle clients
 that are processing data to return back a subset of the unprocessed directories. If the server receives any 
 unprocessed directories back, it will distribute them to all the idle clients. If all clients are requesting work
 then all processing for the configured path will have been completed.
 
The client itself does no work and relies on one or more worker processes to perform the actual processing. Each worker
 is run in its own process in order to avoid the Python GIL locking issue. Workers will behave in the same manner as the
 client and server in terms of communications and work distribution.

When a worker receives a directory or directories to process, it will start processing the directory by getting a list
 of all files and subdirectories for any given directory. Any subdirectories are put at the front of a processing
 queue for further work. By inserting newly found directories at the beginning of the queue, the worker will bias the
 directory processing in a depth first manner. When a server requests work from a client the returned directories will
 be taken from the end of the queue. This will bias the returned directories to be closer to the root of the starting
 path. A single directory currently can be only processed by a single worker and cannot be distributed among multiple
 workers. This means that a directory with a large number of files cannot have work split among multiple workers or
 clients and such large directories may cause the file walk to wait on a single worker or client.

The script attempts to reduce interprocess communications by sending multiple directories to a client or worker
 whenever possible. Biasing returned directories to be closer to the root also helps with this goal. Depending on the
 actual work that the framework is doing, this IPC could cause IPC across multiple machines to actually take longer
 than the actual work itself.

The script supports running the clients on a separate host or the same host as the server. A host should only run a
 single instance of the client script. If more parallelism is desired for the client, adjusting the number of sub
 processes is the recommended method. Multiple clients can be used to decrease total run time however there are cases
 where using more clients will be slower due to more IPC.

An example of a situation where the IPC can slow down total processing is in a situation where the time required to
 finish processing a single directory is very short compared to the IPC latency. If the actual processing is a simple
 file count for example, the IPC call between the workers, clients and server can take longer than just having a single
 client do all the work.
 
Conversely, operations that require long processing time per file will benefit from more concurrency in both number of
 clients and number of workers per client.


# Logging

The default Python logger is used to handle logging from the different modules. The different loggers can be
 accessed by the name of the module: HydraWorker, HydraClient or HydraServer. e.g. logging.getLogger('HydraClient').

The workers use a socket handler to send log messages to the client process. When starting the worker processes, the
 log level at that time will be inherited by the worker processes. There is currently no built in method to adjust
 the log level during run time.

The TCP stream between the worker and client does need to be configured and a very basic level of security is used by
 sending a fixed length random secret over the TCP connection when the TCP stream is setup. This only prevents the
 most basic of attacks. The log information between is not encrypted and is sent using the standard logging library's
 pickling routine.
 
By default, the local loopback address of 127.0.0.1 and a random listening port will be used for
 the TCP connection. Both the address and port can be configured if required. To do this you can either override the
 init method of HydraClient and set the 'log_addr' and 'log_port' of the class to your values before calling the base
  class __init__ or you can override the _init_logger(addr, port) method to pass in your own values.

The client will use its configured logger to emit the messages from the workers along with its own messages.

The server uses a standard Python logger.


# Licenses

The main code is licensed under the MIT License. Please see the LICENSE file for details on the license.

## 3rd party libraries

### Core script

scandir - lib/scandir_src/LICENSE.txt

### Examples

XlsxWriter - examples/fs_audit/lib/xlsxwriter/LICENSE.txt - https://xlsxwriter.readthedocs.io/
