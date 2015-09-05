#!/bin/bash
# set -x
set -e
set -o pipefail

scp collect_info.py root@172.16.52.119:/tmp >/dev/null
ssh root@172.16.52.119 scp /tmp/collect_info.py 10.20.0.5:/tmp >/dev/null
file=$(ssh root@172.16.52.119 ssh 10.20.0.5 python /tmp/collect_info.py -u 10 2>&1 | grep 'Result saved into' | awk '{print $8}' | tr -d "'")
echo $file 
ssh root@172.16.52.119 scp 10.20.0.5:$file /tmp >/dev/null
bfile=$(basename $file)
scp root@172.16.52.119:/tmp/$bfile /tmp/$bfile >/dev/null

echo "Results stored into /tmp/$bfile"

if [ ! -z "$1" ] ; then
	python visualize_cluster.py "test" /tmp/$bfile > $1
fi
