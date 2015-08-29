#!/bin/bash
#set -x
set -e
set -o pipefail

source common.sh

function test_node_ready() {
    node="$1"
    io_file="$2"

    for name in iostat screen ceph ; do
        set +e
        which_util=$(ssh $SSH_OPTS "$node" which "$name")
        found_file=$(ssh $SSH_OPTS ls "$io_file")
        set -e

        if [ -n "$found_file" ] ; then
            echo "File $io_file already exists on $node. Loop found. Exiting"
            exit 1
        fi

        if [ -z "$which_util" ] ; then
            echo "No $name utility found on node $node. Exiting"
            exit 1
        fi
    done
}

function monitor_ceph_io() {
    runtime="$1"
    devs=$(get_osd_devices)
    uniq_dev=$(uniq "$devs")
    grp=$(grep_rr $uniq_dev)

    echo $devs
    echo $uniq_dev
    iostat -x $uniq_dev 1 $runtime | grep -E "$grp"
}

function monitor_ceph_io_sum() {
    runtime="$1"
    devs=$(get_osd_devices)
    uniq_dev=$(uniq "$devs")
    grp=$(grep_rr $uniq_dev)

    #echo $uniq_dev

    for i in $(seq "$runtime") ; do
        iostat -g testgroup { $uniq_dev } # | grep testgroup | awk '{print $5, $6}'
        sleep 1
    done
    #iostat -g testgroup { $uniq_dev } 1 $runtime #| grep testgroup
    #iostat -g testgroup { $uniq_dev } #| grep testgroup
}

function monitor_ceph_cpu() {
    pids=$(get_osd_pids)
    runtime="$1"

    echo $pids
    for i in $(seq "$runtime") ; do
        get_cpu_usage $(get_osd_pids)
        echo 
        sleep 1
    done
}

function monitor_ceph_net() {
    runtime="$1"

    nic=$(get_nic)
    #echo $nic
    for i in $(seq "$runtime") ; do
        ifconfig $nic | grep bytes | awk -F":| +" '{print $4, $9}'
        #ifconfig $nic | grep "bytes"
        #echo 
        sleep 1
    done
}

if [ "$1" == "--monitor" ] ; then
    # 
    # THIS EXECUTED ON MONITORED NODES
    #
    execution_id="$2"
    runtime="$3"
else
    execution_id=$(uuidgen)
    runtime="$1"
fi


MONITOR_DIR="/tmp"
RESULT_DIR="/tmp"
MYDIR=$(dirname "$(readlink -f "$0")")

io_file="${MONITOR_DIR}/ceph_stats_io_${execution_id}.txt"
io_sum_file="${MONITOR_DIR}/ceph_stats_io2_${execution_id}.txt"
cpu_file="${MONITOR_DIR}/ceph_stats_cpu_${execution_id}.txt"
net_file="${MONITOR_DIR}/ceph_stats_net_${execution_id}.txt"
all_files="$io_file $cpu_file $net_file $io_sum_file"


if [ "$1" == "--monitor" ] ; then
    monitor_ceph_io "$runtime" > "$io_file" &
    monitor_ceph_io_sum "$runtime" > "$io_sum_file" &
    monitor_ceph_cpu "$runtime" > "$cpu_file" &
    monitor_ceph_net "$runtime" > "$net_file"
else
    # 
    # THIS EXECUTED ON MASTER NODE
    #
    hosts=$(get_osd_hosts)

    if [ -z "$hosts" ] ; then
        echo "No ceph hosts found!"
        exit 1
    fi

    echo -n "Find ceph nodes: "
    echo $hosts | tr '\n' ' '
    echo
    echo "Start monitoring"

    # test nodes ok
    for host in $hosts ; do
        test_node_ready "$host" "$io_file"
    done

    # Start monitoting in BG
    for host in $hosts ; do
        run_me_in_screen "$0" "$host" "$execution_id" "$runtime"
    done

    # Wait
    (( stime=runtime+1 ))
    echo -n "Will sleep for $stime seconds till "
    date -d "+${stime} seconds" "+%H:%M:%S"
    sleep $stime
    echo "Done, collecting results"

    files=""
    # Collect data
    for host in $hosts ; do
        for file in $all_files ; do
            base_fname=$(basename ${file})
            target="$RESULT_DIR/${host}_${base_fname}"
            scp $SSH_OPTS "$host:$file" "$target" >/dev/null
            files="$files $target"
        done
        ssh $SSH_OPTS "$host" rm "$all_files " >/dev/null
    done
    res_file="$RESULT_DIR/ceph_stats_${execution_id}.tar.gz"
    tar cvzf "$res_file" $files >/dev/null

    files_io=$(ls $RESULT_DIR/*ceph_stats_io2_${execution_id}.txt)
    #echo $files
    echo "IO: Read MB/ Write MB"
    for i in $files_io; do
       cat $i | grep testgroup | awk '{print $5, $6}' | awk -f $MYDIR/sum.awk -v divisor=1024 -v name_offs=6
       #awk -f sum.awk -v divisor=1024 -v name_offs=6 $i
    done

    files_net=$(ls $RESULT_DIR/*ceph_stats_net_${execution_id}.txt)
    #echo $files
    echo "Net: RX MB/ TX MB"
    for i in $files_net; do
        awk -f $MYDIR/sum.awk -v divisor=1048576 -v name_offs=6 $i 
    done

    rm $files
    echo "Done, results are stored in $res_file"
fi

