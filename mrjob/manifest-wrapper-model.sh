#!/bin/sh
INPUT_URI=$(cut -f 2)
FILE_EXT=$(basename $INPUT_URI | sed -e 's/^[^.]*//')
INPUT_PATH=$(mktemp ./input-XXXXXXXXXX$FILE_EXT)

# use
case $INPUT_URI in
    s3://*)
        echo aws s3 cp $INPUT_URI $INPUT_PATH > $INPUT_PATH
        ;;
    *://*)
        echo hadoop fs -copyToLocal $INPUT_URI $INPUT_PATH > $INPUT_PATH
        ;;
    *)
        cp $INPUT_URI $INPUT_PATH
        ;;
esac

case $INPUT_PATH in
    *.bz2)
        bunzip2 -f $INPUT_PATH
        INPUT_PATH=$(echo $INPUT_PATH | sed -e 's/\.bz2$//')
        ;;
    *.gz)
        gunzip -f $INPUT_PATH
        INPUT_PATH=$(echo $INPUT_PATH | sed -e 's/\.gz$//')
        ;;
esac

set +e
cat $INPUT_PATH
RETURNCODE=$?
rm $INPUT_PATH
exit $RETURNCODE
