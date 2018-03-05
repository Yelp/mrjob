#!/bin/sh -ex
INPUT_URI=$(cut -f 2)

FILE_EXT=$(basename $INPUT_URI | sed -e 's/^[^.]*//')

INPUT_PATH=$(mktemp ./input-XXXXXXXXXX$FILE_EXT)
rm $INPUT_PATH

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
        bunzip2 $INPUT_PATH
        INPUT_PATH=$(echo $INPUT_PATH | sed -e 's/\.bz2$//')
        ;;
    *.gz)
        gunzip $INPUT_PATH
        INPUT_PATH=$(echo $INPUT_PATH | sed -e 's/\.gz$//')
        ;;
esac

"$@" $INPUT_PATH $INPUT_URI
{ RETURNCODE=$?; set +x; } 2> /dev/null

if [ $RETURNCODE -ne 0 ]
then
    echo 2>&1
    echo "while reading input from $INPUT_URI" 2>&1
fi

rm $INPUT_PATH
exit $RETURNCODE
