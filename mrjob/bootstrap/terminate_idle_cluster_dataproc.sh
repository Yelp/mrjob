#!/bin/sh

# Copyright 2013 Lyft
# Copyright 2014 Alex Konradi
# Copyright 2015 Yelp and Contributors
# Copyright 2016 Google
# Copyright 2017 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Matthew Tai <mtai84@gmail.com>

# This script is part of mrjob, but can be run as an initializationAction on
# ANY GCP Dataproc cluster.  Because initializationAction scripts cannot take args
# this script reads MAX_SECS_IDLE from metadata attribute "mrjob-max-secs-idle"

# This script runs `yarn application -list` in a loop and considers the cluster
# idle if no jobs are currently running.  If the cluster stays idle long
# enough we explicitly delete ourselves (as this runs on a master node).

# By default, we allow an idle time of 10 minutes.

# Caveats:

# Race conditions: this script can only see currently running jobs, not ones
# pending in Dataproc, or ones that you're about to submit, or jobs that started
# running since the last time we called `yarn application -list`.

# full usage:
#
# ./terminate_idle_cluster_dataproc.sh

MAX_SECS_IDLE=$(/usr/share/google/get_metadata_value attributes/mrjob-max-secs-idle)
if [ -z "${MAX_SECS_IDLE}" ]; then MAX_SECS_IDLE=300; fi

(
while true  # the only way out is to SHUT DOWN THE MACHINE
do
    # get the uptime as an integer (expr can't handle decimals)
    UPTIME=$(cat /proc/uptime | cut -f 1 -d .)

    if [ -z "${LAST_ACTIVE}" ] || \
        (which yarn > /dev/null && \
            nice yarn application -list 2> /dev/null | \
            grep -v 'Total number' | grep -q RUNNING)
    then
        LAST_ACTIVE=${UPTIME}
    else
	# the cluster is idle! how long has this been going on?
        SECS_IDLE=$(expr ${UPTIME} - ${LAST_ACTIVE})
        if expr ${SECS_IDLE} '>' ${MAX_SECS_IDLE} > /dev/null
        then
            yes | gcloud dataproc clusters delete $(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name) --async
            exit
        fi
    fi

    # sleep so we don't peg the CPU
    sleep 5
done
# close file handles to daemonize the script; otherwise bootstrapping
# never finishes
) 0<&- &> /dev/null &
