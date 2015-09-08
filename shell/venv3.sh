#!/bin/bash
set -x
set -e
set -o pipefail

# venv 3.3
# FUEL_IP=172.16.52.119

# venv 3.1
# FUEL_IP=172.16.52.107

if [ ! -z "$1" ] ; then
	FUEL_IP="$1"
else
	echo "Usage $0 FIEL_MASTER_IP"
	exit 1 
fi

NODE_IP=$(ssh root@$FUEL_IP fuel node 2>/dev/null | awk '-F|' '/controller/{print $5}' | head -n 1)
NODE_IP=$(echo $NODE_IP | tr -d '[[:space:]]')

scp collect_info.py root@$FUEL_IP:/tmp >/dev/null
ssh root@$FUEL_IP scp /tmp/collect_info.py $NODE_IP:/tmp >/dev/null

file=$(ssh root@$FUEL_IP ssh $NODE_IP python /tmp/collect_info.py --collectors  2>&1 | grep 'Result saved into' | awk '{print $8}' | tr -d "'")

echo $file 
ssh root@$FUEL_IP scp $NODE_IP:$file /tmp >/dev/null
bfile=$(basename $file)
scp root@$FUEL_IP:/tmp/$bfile /tmp/$bfile >/dev/null

echo "Results stored into /tmp/$bfile"

if [ ! -z "$2" ] ; then
	python visualize_cluster.py "test" /tmp/$bfile > $2
fi


