#!/bin/sh

# by default, terminate any job that's been idle at least a 15 minutes
# AND has 5 minutes or less to the end of the hour (i.e. when get billed
# for ec2 again

# This script polls hadoop continuously (hadoop job -list) and considers the
# job flow idle if no jobs are currently running; it can't see pending jobs.

# usage:
#
# ./terminate_idle_job_flow.sh [ max_hours_idle [ min_secs_to_end_of_hour ] ]
#
# both arguments must be integers

MAX_SECS_IDLE=$1
if [ -z "$MAX_HOURS_IDLE" ]; then MAX_SECS_IDLE=1800; fi

MIN_SECS_TO_END_OF_HOUR=$2
if [ -z "$MIN_SECS_TO_END_OF_HOUR" ]; then MIN_SECS_TO_END_OF_HOUR=300; fi


(
# we're a bootstrap script, so no jobs could have run before us
LAST_ACTIVE=0

while true  # the only way out is to SHUT DOWN THE MACHINE
do
    # get the uptime as an integer (expr can't handle decimals)
    UPTIME=$(cat /proc/uptime | cut -f 1 -d .)
    SECS_TO_END_OF_HOUR=$(expr 3600 - $UPTIME % 3600)

    # might as well nice this; if there's other activity, it's Hadoop jobs
    if nice hadoop job -list 2> /dev/null | grep -q '^job_'
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
)&
