#!/bin/bash
cd "$(dirname "$0")"

if [ $# -ne 1 ] || [ ! -f "$1"/ignis/rpc/*.thrift ]; 
    then echo "usage thrift.sh <rpc-folder>"
    exit
fi


out="ignis/src/main/java/"
for file in `find $1/ignis -name "*thrift"`; do
    thrift --gen java:private-members,handle_runtime_exceptions,generated_annotations=suppress -out $out $file &
done
wait
