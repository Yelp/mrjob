#!/bin/sh
INPUT_URI=$(cut -f 2)
INPUT_PATH=$(mktemp ./input-XXXXXXXXXX)
CP=cp

set -e
case $INPUT_URI in
    *.gz)
        $CP $INPUT_URI $INPUT_PATH.gz
        gunzip -f $INPUT_PATH.gz
        ;;
    *.bz2)
        $CP $INPUT_URI $INPUT_PATH.bz2
        bunzip2 -f $INPUT_PATH.bz2
        ;;
    *)
        $CP $INPUT_URI $INPUT_PATH
        ;;
esac

set +e
"$@" < $INPUT_PATH
RETURNCODE=$?
rm $INPUT_PATH
exit $RETURNCODE
