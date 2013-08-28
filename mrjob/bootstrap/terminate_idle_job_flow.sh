#!/bin/sh

# Copyright 2013 Lyft
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

# Author: David Marin <dm@davidmarin.org>

# This script is part of mrjob, but can be run as a bootstrap action on
# ANY Elastic MapReduce job flow. Arguments are totally optional.

# This script runs `hadoop job -list` in a loop and considers the job flow
# idle if no jobs are currently running. If the job flow stays idle long
# enough AND we're close enough to the end of an EC2 billing hour, we
# shut down the master node, which kills the job flow.

# By default, we allow an idle time of 15 minutes, and shut down within
# the last 5 minutes of the hour.

# Caveats:

# Race conditions: this script can only see currently running jobs, not ones
# pending in EMR, or ones that you're about to submit, or jobs that started
# running since the last time we called `hadoop job -list`.

# This script will leave the job flow in the FAILED (not TERMINATED) state,
# with LastStateChangeReason "The master node was terminated. ". It can
# take EMR a minute or so to realize that master node has been shut down.

# full usage:
#
# ./terminate_idle_job_flow.sh [ max_hours_idle [ min_secs_to_end_of_hour ] ]
#
# Both arguments must be integers

MAX_SECS_IDLE=$1
if [ -z "$MAX_SECS_IDLE" ]; then MAX_SECS_IDLE=1800; fi

MIN_SECS_TO_END_OF_HOUR=$2
if [ -z "$MIN_SECS_TO_END_OF_HOUR" ]; then MIN_SECS_TO_END_OF_HOUR=300; fi


(
while true  # the only way out is to SHUT DOWN THE MACHINE
do
    # get the uptime as an integer (expr can't handle decimals)
    UPTIME=$(cat /proc/uptime | cut -f 1 -d .)
    SECS_TO_END_OF_HOUR=$(expr 3600 - $UPTIME % 3600)

    # first time through this loop, we just initialize LAST_ACTIVE
    # might as well nice hadoop; if there's other activity, it's Hadoop jobs
    if [ -z "$LAST_ACTIVE" ] || \
        nice hadoop job -list 2> /dev/null | grep -q '^job_'
    then
        LAST_ACTIVE=$UPTIME

        # echo 0 $SECS_TO_END_OF_HOUR
    else
        SECS_IDLE=$(expr $UPTIME - $LAST_ACTIVE)

        # echo $SECS_IDLE $SECS_TO_END_OF_HOUR

        if expr $SECS_IDLE '>' $MAX_SECS_IDLE '&' \
            $SECS_TO_END_OF_HOUR '<' $MIN_SECS_TO_END_OF_HOUR > /dev/null
        then
            sudo shutdown -h now
            exit
        fi
    fi
done
# close file handles to daemonize the script; otherwise bootstrapping
# never finishes
) 0<&- &> /dev/null &
