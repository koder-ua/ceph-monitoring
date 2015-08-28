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

