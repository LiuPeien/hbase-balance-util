#!/bin/sh

usage="Usage: get_group_servers <group_name>"
if [ $# != 1 ]; then
    echo "${usage}"
    exit 1
fi

group=$1

PROJECT_HOME=$(cd $(dirname $0)/..;pwd)
output=${PROJECT_HOME}/data/${group}.servers

echo "get_group '${group}'" | hbase shell | grep -o "ip.*internal" > ${output}
echo "output path ${output}"