#!/bin/bash
# The MIT License (MIT)
#
# Copyright (c) 2020 Andrew Chung <acchung@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

# Modify the options below to match your environment
PROCESS_PATH="/ifs/some/path"
BASE_PATH=/ifs/scripts/exec_cmd
LOG_PATH=/ifs/scripts/exec_cmd_logs
CLIENT_OPTIONS="-v"
SERVER_OPTIONS="-v"

# Standard options below. Should not need to modify these normally
NUM_WORKERS=4
SVR_NODE=`isi_nodes -L "%{lnn}"`
SVR_IP=`isi_nodes -L "%{internal}"`
SCRIPT="$BASE_PATH/exec_cmd.py"
CLIENT_LOG=" -l ${LOG_PATH}/client_$(hostname).log"
SERVER_LOG=" -l ${LOG_PATH}/server.log"


function cleanup() {
  echo "Cleaning up old 'screen' sessions"
  isi_for_array -X \
      screen -d -r exec_cmd -X quit &> /dev/null
}

function check_dirs() {
  if [ ! -d "${BASE_PATH}" ]; then
    echo "Base path does not exist. Check script variables."
    exit 2
  fi
  if [ ! -d "${LOG_PATH}" ]; then
    echo "Log path does not exist. Check script variables."
    exit 2
  fi
}

check_dirs
if [ "$1" == "client" ]; then
  python ${SCRIPT} -c $2 -n ${NUM_WORKERS} ${CLIENT_LOG} ${CLIENT_OPTIONS}
elif [ "$1" == "cleanup" ]; then
  cleanup
  exit 1
elif [ "$1" == "start" ]; then
  cleanup
  echo "Starting server on current node"
  shift
  screen -d -m -S exec_cmd \
      python ${SCRIPT} -s -p ${PROCESS_PATH} ${SERVER_LOG} ${SERVER_OPTIONS} $@
  echo "Starting clients on all other nodes"
  isi_for_array -x ${SVR_NODE} -X \
      screen -d -m -S exec_cmd \
      bash "${BASE_PATH}/$0 client ${SVR_IP}"
else
  cat <<- EOF
Usage:
  $0 [cleanup|start] [options]

Description:
Please supply as the first argument one of the following options: cleanup|start
Specifying 'cleanup' will terminate any running instances. This is done
  automatically when running 'start'

For start the syntax is:
  $0 start <cmd_to_run>

The root path is coded into the script itself.
You can monitor the progress by 'tail'ing the server log file, connecting
to the detatched screen session or by monitoring when the screen session
terminates. When running 'screen -ls' to monitor progress, look for the
session named <pid>.exec_cmd
EOF
  exit 1
fi
