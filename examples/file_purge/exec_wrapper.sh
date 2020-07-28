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

# ========================================
# Modify the options below to match your environment or override via shell
# environment variables of the same name. Defaults that you can modify are
# after the '-' character. e.g. ${VAR_NAME-DEFAULT_VALUE}
# ========================================
# TYPE = [SSH|ONEFS]
# Use ONEFS when running on cluster and SSH on Linux/UNIX clients
SCRIPT_NAME="file_purge"
TYPE=${TYPE-"SSH"}
PROCESS_PATH=${PROCESS_PATH-"/some/path"}
BASE_PATH=${BASE_PATH-"/scripts/path/${SCRIPT_NAME}"}
LOG_PATH=${LOG_PATH-"/scripts/path/${SCRIPT_NAME}_logs"}
CLIENT_OPTIONS=${CLIENT_OPTIONS-"-v -a ${LOG_PATH}/audit_$(hostname).log"}
SERVER_OPTIONS=${SERVER_OPTIONS-"-v"}
# The CLIENTS variable below is used when the TYPE is SSH. This is a
#   space delimited list of clients that can be reached via SSH using SSH
#   public/private key pairs. You can specify IP address or FQDN
# Use localhost or 127.0.0.1 to include the machine running as the server
#   as one of the clients.
CLIENTS=${CLIENTS-"localhost client1.fqdn client2.ip client3.fqdn"}
if [ "${TYPE}" == "SSH" ]; then
  # It is difficult to properly provide a routine to get the correct IP address
  #   that a client should use to communicate with the server. The default is
  #   to use the server's hostname. Change this parameter to fit your
  #   environment if you are using SSH.
  SVR_IP=${SVR_IP-`hostname`}
elif [ "${TYPE}" == "ONEFS" ]; then
  SVR_IP=${SVR_IP-`isi_nodes -L "%{internal}"`}
fi

# ========================================
# Standard options below. Should not need to modify these normally
# ========================================
NUM_WORKERS=${NUM_WORKERS-4}
SCRIPT=${SCRIPT-"${BASE_PATH}/${SCRIPT_NAME}.py"}
CLIENT_LOG=${CLIENT_LOG-"-l ${LOG_PATH}/client_$(hostname).log"}
CLIENT_ERR_LOG=${CLIENT_ERR_LOG-"${LOG_PATH}/client_err_$(hostname).log"}
SERVER_LOG=${SERVER_LOG-"-l ${LOG_PATH}/server.log"}


read -r -d '' DESCRIPTION <<- EOF
Usage:
  $0 [start|cleanup] [options]

Description:
Please supply as the first argument one of the following options: cleanup|start
Specifying 'cleanup' will terminate any running instances. This is done
  automatically when running 'start'

For start the syntax is:
  $0 start <--date #> [--mtime|--atime|--ctime] [--purge]

The root path is coded into the script itself.
You can monitor the progress by 'tail'ing the server log file, connecting
to the detatched screen session or by monitoring when the screen session
terminates. When running 'screen -ls' to monitor progress, look for the
session named <pid>.exec_cmd_s
EOF


function cleanup() {
  echo "Cleaning up old 'screen' sessions"
  if [[ "${TYPE}" == "ONEFS" ]]; then
    isi_for_array -X screen -d -r exec_cmd_s -X quit &> /dev/null
    isi_for_array -X screen -d -r exec_cmd -X quit &> /dev/null
  elif [[ "${TYPE}" == "SSH" ]]; then
    for c in ${CLIENTS}; do
      if [[ "${c}" == '127.0.0.1' ]] || [[ "${c}" == 'localhost' ]]; then
        screen -d -r exec_cmd_s -X quit &> /dev/null
        screen -d -r exec_cmd -X quit &> /dev/null
      else
        ssh ${c} screen -d -r exec_cmd -X quit &> /dev/null
        ssh ${c} screen -d -r exec_cmd -X quit &> /dev/null
      fi
    done
  fi
}

function check_params() {
  err=0
  # Check to see if the hydra library is installed as a module, installed as a
  #   directory under the current directory, or is available 2 levels up
  hydra=`python -c "import hydra" &> /dev/null`
  hydra_missing=$?
  if [[ ${hydra_missing} != 0 ]]; then
    hydra=`python -c "import sys; from os import path as p; \
        sys.path.insert(0, p.dirname(p.dirname(\"${BASE_PATH}\"))); import hydra"`
    hydra_missing=$?
    if [[ ${hydra_missing} != 0 ]]; then
      echo "Hydra Python module not found. Check installation."
      err=1
    fi
  fi
  screen_exe=`which screen`
  if [ "${screen_exe}" == "" ]; then
    echo "This script depends on the 'screen' binary being available. Install 'screen' and re-run."
    err=1
  fi
  if [ ! -f "${SCRIPT}" ]; then
    echo "Script to execute not found. Check the SCRIPT variable."
    err=1
  fi
  if [ ! -d "${BASE_PATH}" ]; then
    echo "Base path does not exist. Check BASE_PATH variable."
    err=1
  fi
  if [ ! -d "${LOG_PATH}" ]; then
    echo "Log path does not exist. Check LOG_PATH variable."
    err=1
  fi
  if [[ "${TYPE}" != "SSH" ]] && [[ "${TYPE}" != "ONEFS" ]]; then
    echo "Connect TYPE incorrect. Please change to one of: SSH | ONEFS"
    err=1
  fi
  if [ "${err}" == 1 ]; then
    exit 2
  fi
}

function start_clients() {
  echo "Starting clients with command: bash ${BASE_PATH}/$0 client ${SVR_IP}"
  if [[ "${TYPE}" == "ONEFS" ]]; then
    isi_for_array -X \
        screen -d -m -S exec_cmd bash "${BASE_PATH}/$0 client ${SVR_IP}"
  elif [[ "${TYPE}" == "SSH" ]]; then
    for c in ${CLIENTS}; do
      if [[ "${c}" == '127.0.0.1' ]] || [[ "${c}" == 'localhost' ]]; then
        echo "Starting local client"
        # The extra 'exec bash' is a workaround for a permission denied error
        #   when trying to run on the local machine and the script file does
        #   not have the execute bit enabled
        screen -d -m -S exec_cmd bash -c "exec bash ${BASE_PATH}/$0 client ${SVR_IP}" &
      else
        echo "Starting remote client: ${c}"
        ssh ${c} screen -d -m -S exec_cmd bash "${BASE_PATH}/$0 client ${SVR_IP}" &
      fi
    done
  fi
}

check_params
if [ "$1" == "client" ]; then
  python ${SCRIPT} -c ${2} -n ${NUM_WORKERS} ${CLIENT_LOG} ${CLIENT_OPTIONS} &> ${CLIENT_ERR_LOG}
elif [ "$1" == "cleanup" ]; then
  cleanup
  exit 0
elif [ "$1" == "start" ]; then
  cleanup
  shift
  echo "Starting server with command: python ${SCRIPT} -s -p ${PROCESS_PATH} ${SERVER_LOG} ${SERVER_OPTIONS} $@"
  screen -d -m -S exec_cmd_s python ${SCRIPT} -s -p ${PROCESS_PATH} ${SERVER_LOG} ${SERVER_OPTIONS} $@
  # Give some time for the server to start up
  sleep 2
  start_clients
else
  echo "${DESCRIPTION}"
  exit 1
fi
