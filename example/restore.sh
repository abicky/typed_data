#!/bin/bash

cd $(dirname $0)

data=converted_data.jsonl

echo "Input data:"
cat $data
echo

cmd="typed-data restore --schema schema.avsc $data"
echo "Execute: $cmd"
eval $cmd
echo

cmd="cat $data | typed-data restore --schema schema.avsc"
echo "Execute: $cmd"
eval $cmd
