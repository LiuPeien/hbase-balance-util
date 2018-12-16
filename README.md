# HBase Ops Utils
HBase ops utils for HBase operation.

# Install
```
git clone https://github.com/LiuPeien/hbase-balance-util.git
cd hbase-balance-util/bin
./build.sh
```

# Usage
```
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
```


# How to balance tables?
1. `HBASE_ZK` and `HBASE_ZK_PATH` in bin/env.sh must be set.
2. prepare the server_list file, the file includes the hostname of servers, one per line.
3. use hbase_op.sh script in `hbase-ops/bin` to blance tables.
4. the move plan will be output to `hbase-ops/data/${table}.plan`.

# Example
```
# blance hbase table m1
bin/balance_table.sh m1 balance --server /tmp/server.list --policy 0 --realrun false
```

```
# content in /tmp/server.list
10.23.12.129 4
10.23.12.130 4
10.23.12.131 4
10.23.12.132 1
10.23.12.133 1
10.23.12.134 1
10.23.12.135 1
```

# How to save and recover region locations?
1. `HBASE_ZK` and `HBASE_ZK_PATH` in bin/env.sh must be set.
2. use hbase_op.sh script in `hbase-ops/bin` to save/recover regions.
3. save -- the location info will be output to `hbase-ops/data/${table}.location` if you don't specify the output file.
4. recover -- it will load location info from `hbase-ops/data/${table}.location` if you don't specify the input file.

# Example
```
# save table regions location info
bin/hbase_op.sh m1 save

# recover regions location info
bin/hbase_op.sh m1 recover
```