#!/bin/sh

usage="
Usage: balance_table.sh <table_list> <action> [-|--options] [args...]
    table_list:
            required. specify the tables you want to balance;
            if it is a file, it consists of an table list, one per line;
            if it is a string, it is a comma-separated ip list, e.g. t1,t2,...
    action:
            balance
            save
            recover

where options for action balance:
    -s|--server:
            required currently. specify the dest servers;
            a file contains the region server list one per line and with format: hostname capacity, e.g. ip-10-23-12-156.ec2.internal 1;
            if you don't want move any region to a server, specify its capacity to 0;
            in future, default value is servers that contain at least one region of the table;
    -p|--policy:
            specify the balance policy you want to use;
            0: balance regions to dest servers according to the region count for each table, it will distribute regions evenly;
            1: balance regions to dest servers according to the data locality randomly for each table, which means the distribution of regions may not be even;
            2: balance regions to dest servers according to the region count and data locality both for each table, it will distribute regions evenly;
            default value is 0;
    -r|--realrun:
            true|false;
            default is false;
    -i|--interval:
            interval
            millisecond between each move action;
            default value is 10;
    -h|--threshold:
            threshold
            used when the policy is 1, we will keep region in place to hold cache locality if (dest_host_locality - cur_host_locality < threshold);
            default value is 0;
"

PROJECT_HOME=$(cd $(dirname $0)/..;pwd)
BIN_HOME=${PROJECT_HOME}/bin
source ${BIN_HOME}/env.sh

if [ $# -lt 2 ]; then
    echo "$usage"
    exit 1
fi

if [ -f $1 ]; then
    tables=$(cat $1)
else
    tables=$(echo $1 | sed 's/,/ /g')
fi
action=$2
shift 2

policy=0
real_run=false
interval=10
threshold=0

while [ "$1" != "" ]; do
    case "$1" in
        -p | --policy)
            policy=$2
            shift 2
        ;;
        -s | --server)
            if [[ "$2" =~ ^[/~].* ]]; then
                server_file=$2
            else
                server_file=${BIN_HOME}/$2
            fi
            if [ ! -f ${server_file} ];then
                echo "the file: ${server_file} is not exist!"
                exit 1
            fi
            shift 2
        ;;
        -r | --realrun)
            real_run=$2
            shift 2
        ;;
        -i | --interval)
            interval=$2
            shift 2
        ;;
        -t | --threshold)
            threshold=$2
            shift 2
        ;;
        *)
            echo "invalid parameter!"
            echo "$usage"
            exit 1
    esac
done

function do_balance() {
    for table in ${tables}; do
        echo "--------------------- ${table} ----------------------"
        plan_file=${PROJECT_DATA_PATH}/${table}.plan
        java -cp `hbase classpath`:${PROJECT_JAR} com.jinhongliu.hbase.balancer.TableBalancer \
            "table=${table}" \
            "policy=${policy}" \
            "server_file=${server_file}" \
            "real_run=${real_run}" \
            "zk=${HBASE_ZK}" \
            "zk_path=${HBASE_ZK_PATH}" \
            "plan_file=${plan_file}" \
            "threshold=${threshold}" \
            "interval=${interval}"
        sleep 2
    done
}

function do_save() {
    for table in ${tables}; do
        echo "--------------------- ${table} ----------------------"
        location_file=${PROJECT_DATA_PATH}/${table}.location
        java -cp `hbase classpath`:${PROJECT_JAR} com.jinhongliu.hbase.keeper.HbaseLocationKeeper \
            "table=${table}" \
            "action=save" \
            "zk=${HBASE_ZK}" \
            "zk_path=${HBASE_ZK_PATH}" \
            "location_file=${location_file}"
    done
}

function do_recover() {
    for table in ${tables}; do
        echo "--------------------- ${table} ----------------------"
        location_file=${PROJECT_DATA_PATH}/${table}.location
        java -cp `hbase classpath`:${PROJECT_JAR} com.jinhongliu.hbase.keeper.HbaseLocationKeeper \
            "action=recover" \
            "zk=${HBASE_ZK}" \
            "zk_path=${HBASE_ZK_PATH}" \
            "location_file=${location_file}"
    done
}

case "${action}" in
    balance)
        do_balance
    ;;
    save)
        do_save
    ;;
    recover)
        do_recover
    ;;
    *)
        echo "${usage}"
        exit 1
esac

