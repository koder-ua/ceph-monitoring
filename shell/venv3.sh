#!/bin/bash
#set -x
set -e
set -o pipefail
REP_STORAGE=/var/ceph_reports

# venv 3.3
# FUEL_IP=172.16.52.119

# venv 3.1
# FUEL_IP=172.16.52.107

if [ ! -z "$1" ] ; then
	FUEL_IP="$1"
	shift
else
	echo "Usage $0 FIEL_MASTER_IP"
	exit 1 
fi

echo "Discovering controlles node"
NODE_IP=$(ssh root@$FUEL_IP fuel node 2>/dev/null | awk '-F|' '/controller/{print $5}' | head -n 1)
NODE_IP=$(echo $NODE_IP | tr -d '[[:space:]]')
echo "Ok controller ip $NODE_IP"

scp collect_info.py root@$FUEL_IP:/tmp >/dev/null
echo "Copy collect_info.py to fuel master"

ssh root@$FUEL_IP scp /tmp/collect_info.py $NODE_IP:/tmp >/dev/null
echo "Copy collect_info.py to $NODE_IP"

log_file=$(tempfile)

echo "Collecting data..."
# ssh root@$FUEL_IP ssh $NODE_IP python /tmp/collect_info.py --collectors ceph,node,performance --performance-collect-seconds 600 2>&1 | tee $log_file
ssh root@$FUEL_IP ssh $NODE_IP python /tmp/collect_info.py $@ 2>&1 | tee $log_file
file=$(grep 'Result saved into' $log_file | awk '{print $8}' | tr -d "'")
rm $log_file

echo "Copying results back"
ssh root@$FUEL_IP scp $NODE_IP:$file /tmp >/dev/null
bfile=$(basename $file)
scp root@$FUEL_IP:/tmp/$bfile $REP_STORAGE/$bfile >/dev/null

echo "Archive stored in $REP_STORAGE/$bfile"

dirname="${bfile#file}"
dirname="${dirname/.tar.gz/}"
mkdir "$REP_STORAGE/$dirname"
tar -zxvf "$REP_STORAGE/$bfile" -C "$REP_STORAGE/$dirname" >/dev/null 2>&1
echo "Unpacked data stored into $REP_STORAGE/$dirname"
