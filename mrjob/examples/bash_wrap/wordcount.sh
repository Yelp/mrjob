#!/bin/bash

# Copyright 2013 Andrew Price
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

# Author: Andrew Price <andrew.price@ensighten.com>

# Info:
# Mapper input is raw lines from the input files.
# Reducer input is what the mapper emits, i.e.: word\t123
# In Hadoop streaming, a reducer receives separate lines for each value in a key group,
# as opposed to a single line with the key and an array of values.

if [ "$1" == "mapper" ]
then
  # || [ -n "$line" ] ensures the last line is always processed
  while read line || [ -n "$line" ]
  do
    for word in $line
    do
      echo -e "$word\t1"
    done
  done
elif [ "$1" == "reducer" ]
then
  # set internal field separator to split line on tab to get key and value
  IFS=$'\t'
  key=''
  value=0
  while read line  || [ -n "$line" ]
  do
    arr=($line);
    linekey=${arr[0]}
    linevalue=${arr[1]}

    if [ "$key" == "$linekey" ]
    then
      value=$(($value+$linevalue));
    else
      if [ -n "$key" ]
      then
        echo -e "$key\t$value"
      fi
      key=$linekey
      value=$linevalue
    fi
  done

  echo -e "$key\t$value"
fi
