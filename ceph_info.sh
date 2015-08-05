#!/bin/bash
set -x
set -e
set -o pipefail

SSH_OPTS="-o LogLevel=quiet -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

function get_osd_pids() {
    ps aux | grep ceph-osd | grep -v grep | awk '{print $2}'    
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

function test_node_ready() {
    node="$1"
    for name in iostat screen ceph ; do
        set +e
        which_util=$(ssh $SSH_OPTS "$node" which "$name")
        set -e
        if [ -z "$which_util" ] ; then
            echo "No $name utility found on node $node. Exiting"
            exit 1
        fi
    done
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


function get_osd_nodes_old() {
    ceph osd tree | grep -E '\bhost\b' | awk '{print $4}'   
}

function get_dev() {
    if [ -b "$1" ] ; then
        echo $1
    else
        echo $(df "$1" | tail -1 | awk '{print $1}')
    fi
}

function get_osd_devices_old() {
    for dirr in /var/lib/ceph/osd/ceph-* ; do 
        df $dirr | tail -1 | awk '{print $1}'
        get_dev $(follow_slink $dirr/journal)
    done
}

DEV_CODE="import socket, json, sys;"
DEV_CODE="${DEV_CODE}hostname = socket.gethostbyaddr(socket.gethostname())[1][0];"
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
    jpath=$(ceph --admin-daemon "/var/run/ceph/ceph-osd.${osd_num}.asok" config show | grep -E '\bosd_journal\b' | awk '{print $2}' | tr -d '\\",')
    data=$(ceph --admin-daemon /var/run/ceph/ceph-osd.${osd_num}.asok config show | grep -E '\bosd_data\b'  | awk '{print $2}' | tr -d '\\",')

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

function monitor_ceph_io() {
    runtime="$1"
    devs=$(get_osd_devices)
    uniq_dev=$(uniq "$devs")
    grp=$(grep_rr $uniq_dev)

    echo $devs
    echo $uniq_dev
    iostat -x $uniq_dev 1 $runtime | grep -E "$grp"
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

function get_osd_hosts() {
    ceph osd tree | grep host | awk '{print $4}'
}

function run_me_in_screen() {
    script_path="$1"
    host="$2"
    mtime="$3"

    script_basename=$(basename "$script_path")
    target="/tmp/$script_basename"
    scp $SSH_OPTS "$script_path" "$host:$target" >/dev/null
    ssh $SSH_OPTS "$host" screen -S ceph_monitor -d -m bash "$target" --monitor $mtime
}

MONITOR_FILE="/tmp/osd_mon"

if [ "$1" == "--monitor" ] ; then
    # 
    # THIS EXECUTED ON MONITORED NODES
    #
    monitor_ceph_io "$2" > "${MONITOR_FILE}.io" &
    monitor_ceph_cpu "$2" > "${MONITOR_FILE}.cpu"
else
    # 
    # THIS EXECUTED ON MASTER NODE
    #

    hosts=$(get_osd_hosts)

    if [ -z "$hosts" ] ; then
        echo "No ceph hosts found!"
        exit 1
    fi

    # runtime - 3s
    mtime=$1

    echo -n "Find ceph nodes: "
    echo $hosts | tr '\n' ' '
    echo
    echo "Start monitoring"
    # Start monitoting in BG
    for host in $hosts ; do
        test_node_ready "$host"
        run_me_in_screen "$0" "$host" $mtime
    done

    # Wait
    (( stime=mtime+1 ))
    echo -n "Will sleep for $stime seconds till "
    date -d "+${stime} seconds" "+%H:%M:%S"
    sleep $stime
    echo "Done, collecting results"

    files=""
    # Collect data
    for host in $hosts ; do
        scp $SSH_OPTS "$host:${MONITOR_FILE}.io" "${MONITOR_FILE}.${host}.io" >/dev/null
        scp $SSH_OPTS "$host:${MONITOR_FILE}.cpu" "${MONITOR_FILE}.${host}.cpu" >/dev/null
        ssh $SSH_OPTS "$host" rm "${MONITOR_FILE}.io" "${MONITOR_FILE}.cpu" >/dev/null
        files="$files ${MONITOR_FILE}.${host}.io ${MONITOR_FILE}.${host}.cpu"
    done
    echo -n "Done, results are stored in "
    echo $files
fi
