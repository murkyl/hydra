# file_purge
This example script walks a file structure and deletes file according to the different timestamps on a file. The script relies on the hydra module to execute on multiple machines simultaneously.

The script makes an assumption that the path provided exists on all the clients that are performing the processing.

## Starting the server
The server process determines the timestamp criteria as well as the path to process. The minimal execution of the script is as follows:
> python file_purge.py -s -p <path> -d <time_in_days> [--atime|--mtime|--ctime] [--purge] 

|Argument        |Description                                                                    |
|----------------|-------------------------------------------------------------------------------|
|-s or --server  |Run the script as a server                                                     |
|-d or --date    |Specify the number of days in the past for a file to be considered for deletion|
|--atime         |Use the file last access time                                                  |
|--mtime         |Use the file last modified time                                                |
|--ctime         |Use the file last change time                                                  |
|--purge         |When the flag is present, the script will perform the actual file deletion     |

The --atime, --mtime and --ctime options specify which of the timestamps to use for comparison. It is supported to use more than 1 timestamp option and if any of the timestamps meet the criteria for deletion then the file will be deleted.

The most important option is the --purge option. When this option is present the script will actually perform the file deletion. When this option is not present, the script will output which file it would have deleted. Be **very careful** when specifying the --purge option.

## Starting the clients
At least one client needs to be started to perform any processing. A single machine should run only a single instance of the client. Increase the number of workers instead of using more clients per machine for more parallelism.

The client process has minimal arguments. A minimal execution of the script is as follows:
> python file_purge.py -c <ip_or_fqdn_of_server>

|Argument        |Description                                                                    |
|----------------|-------------------------------------------------------------------------------|
|-c or --client  |Run the script as a client and connect to the server specified                 |

While the above command is sufficient, there are some additional recommended arguments to the script. There are two arguments of importance. The --audit or -a argument and the --num_workers or -n argument.

When using the -a argument, you can specify a file name base to output the name of files that would be deleted or are deleted to a log file. This is useful to understand which files have been deleted by the script. The name is a base for the actual audit log filename. The script will automatically append a suffix for the worker number to the filename. If the filename contains an extension, then the suffix will be inserted before the extension. e.g. audit.log will output audit-1.log, audit-2.log, etc.

The -n argument specifies the number of worker processes to launch for this client. By default the script will create MAX(CEIL(NUM_CPUS/2), 8) workers. On a 4 CPU core client, this will start 2 workers. On a 7 CPU core client, this will start 4 clients. On a 20 CPU core client, this will start 8 clients.

An example command would look like the following:
> python file_purge.py -c <ip_or_fqdn_of_server> -a <audit_file_base> -n 4

## Using the wrapper script
A wrapper script named exec_wrapper.sh is provided to simplify running the script across multiple machines. It leverages SSH to automatically start the client scripts on the various clients. The requirements for running this wrapper script using SSH include having the *screen* binary available on the clients and that the machine where the script is run has SSH public-private key-pair login to all the clients.

Some modification of script variables is required before running the script. This can also be accomplished through use of environment variables on the server and all the clients.

The following script parameters should be modified.
|Shell variables |Description                                                                    |
|----------------|-------------------------------------------------------------------------------|
|PROCESS_PATH    |The path the script will start processing                                      |
|BASE_PATH       |Path to the script file that is accessible to all clients. This can be local to each client or it can be on shared storage|
|LOG_PATH        |Path where the clients and server can store log files. This can be local to each client or it can be on shared storage|
|CLIENTS         |A space separated list of clients that will be used for processing             |

The following are optional variables that can be modified.
|Shell variables |Description                                                                    |
|----------------|-------------------------------------------------------------------------------|
|CLIENT_OPTIONS  |Additional options to pass to the script running as a client                   |
|SERVER_OPTIONS  |Additional options to pass to the script running as a server                   |
|SVR_IP          |This defaults to the host name of the server machine. Specify a value here if the host name is not accurate or is not resolvable by the clients|
|NUM_WORKERS     |The default value is 4 for the script                                          |

By default the script will take the server's host name via the *hostname* command and use this as the destination for all the clients. 

On a machine that will be the server the simplest execution of the wrapper script looks like the following:
> bash exec_wrapper.sh -d <time_in_days> [--atime|--mtime|--ctime] [--purge]

The shell script accepts an arbitrary number of additional parameters which are passed directly to the underlying script. The example given above includes the minimal set required for the script. 

There is an additional option which is the TYPE of execution. The default value is SSH but it can be specified as ONEFS. When set to ONEFS the script will assume it is running from the CLI of a clustered file system running the OneFS operating system.