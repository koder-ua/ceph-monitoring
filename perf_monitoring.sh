#!/bin/bash
#set -x
set -e
set -o pipefail

SSH_OPTS="-o LogLevel=quiet -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

function get_osd_pids() {
    ps aux | grep -v grep | awk '/ceph-osd/{print $2}'    
}

function get_nic() {
    ifconfig | awk '/Ethernet/{print $1}'
}

function join() {
    jstr=$1
    shift 1
    res=""
    for part in $@ ; do
        if [ -z "$res" ] ; then
            res="$part"
        else
            res="${res}${jstr}${part}"
        fi
    done
    echo $res
}

function get_cpu_usage() {
    pids=$(join ',' $@)
    ps -p $pids -o cputime,etime | grep -v ELAPSED
}

NODE_CODE="import json,sys; data = json.loads(sys.stdin.read());"
NODE_CODE="${NODE_CODE}print ' '.join(i['name'] for i in data['nodes'] if i['type'] == 'host')"

function get_osd_nodes() {
    ceph osd tree -f json | python -c "$NODE_CODE"
}


function get_dev() {
    if [ -b "$1" ] ; then
        echo $1
    else
        echo $(df "$1" | tail -1 | awk '{print $1}')
    fi
}

DEV_CODE="import socket, json, sys;"
DEV_CODE="${DEV_CODE}hostdata = socket.gethostbyaddr(socket.gethostname());"
DEV_CODE="${DEV_CODE}hostname = hostdata[1][0] if len(hostdata[1]) > 0 else hostdata[0];"
DEV_CODE="${DEV_CODE}data = json.loads(sys.stdin.read());"
DEV_CODE="${DEV_CODE}res = [' '.join(map(str, i['children'])) for i in data['nodes']"
DEV_CODE="${DEV_CODE}                                         if i['name'] == hostname];"
DEV_CODE="${DEV_CODE}print ''.join(res)"

function get_osd_devices() {
    for osd_num in $(ceph osd tree -f json | python -c "$DEV_CODE") ; do
        get_osd_devices_by_num $osd_num
    done
}

function get_root_dev() {
    echo $1 | tr -d '0123456789'
}

function get_osd_devices_by_num() {
    osd_num="$1"
    jpath=$(sudo ceph --admin-daemon "/var/run/ceph/ceph-osd.${osd_num}.asok" config show | grep -E '\bosd_journal\b' | awk '{print $2}' | tr -d '\\",')
    data=$(sudo ceph --admin-daemon /var/run/ceph/ceph-osd.${osd_num}.asok config show | grep -E '\bosd_data\b'  | awk '{print $2}' | tr -d '\\",')

    get_root_dev $(get_dev $(follow_slink $jpath))
    get_root_dev $(get_dev $(follow_slink $data))
}


function follow_slink() {
    path="$1"
    while [ -h "$path" ] ; do
        path=$(readlink "$path")
        path=$(readlink -f "$path")
    done
    echo $path
}

function grep_rr() {
    res=""
    for dev in $@ ; do
        ndev=$(basename "$dev")
        if [ -z "$res" ] ; then
            res="\b$ndev\b"
        else
            res="$res|\b$ndev\b"
        fi
    done
    echo $res
}

function uniq() {
    echo $1 | tr ' ' '\n' | sort -u
}

function get_osd_hosts() {
    ceph osd tree | grep host | awk '{print $4}'
}

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

    for i in $(seq "$runtime") ; do
        iostat -g testgroup { $uniq_dev }
        sleep 1
    done
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
    for i in $(seq "$runtime") ; do
        ifconfig $nic | awk -F":| +" '/bytes/{print $4, $9}'
        sleep 1
    done
}

function run_me_in_screen() {
    script_path="$1"
    host="$2"
    execution_id="$3"
    mtime="$4"

    script_basename=$(basename "$script_path")
    target="/tmp/$script_basename"
    scp $SSH_OPTS "$script_path" "$host:$target" >/dev/null
    ssh $SSH_OPTS "$host" screen -S ceph_monitor -d -m bash "$target" --monitor "$execution_id" "$mtime"
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
            local_fname="${host}_${base_fname}"
            target="$RESULT_DIR/${local_fname}"
            scp $SSH_OPTS "$host:$file" "$target" >/dev/null
            files="$files $local_fname"
        done
        ssh $SSH_OPTS "$host" rm "$all_files " >/dev/null
    done
    res_file="$RESULT_DIR/ceph_stats_${execution_id}.tar.gz"
    pushd "$RESULT_DIR"
    tar cvzf "$res_file" $files >/dev/null
    popd
    echo "Done, results are stored in $res_file"

    # files_io=$(ls $RESULT_DIR/*ceph_stats_io2_${execution_id}.txt)
    # echo "IO: Read MB/ Write MB"
    # for i in $files_io; do
    #    cat "$i" | grep testgroup | awk '{print $5, $6}' | awk -f "$MYDIR/sum.awk" -v divisor=1024 -v name_offs=6
    # done

    # files_net=$(ls $RESULT_DIR/*ceph_stats_net_${execution_id}.txt)
    # echo "Net: RX MB/ TX MB"
    # for i in $files_net; do
    #     awk -f "$MYDIR/sum.awk" -v divisor=1048576 -v name_offs=6 $i 
    # done

    pushd "$RESULT_DIR"
    rm $files
    popd
fi

