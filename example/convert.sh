#!/bin/bash

cd $(dirname $0)

data=data.jsonl

echo "Input data:"
cat $data
echo

cmd="typed-data convert --schema schema.avsc $data"
echo "Execute: $cmd"
eval $cmd
echo

cmd="cat $data | typed-data convert --schema schema.avsc"
echo "Execute: $cmd"
eval $cmd
