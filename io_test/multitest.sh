#!/bin/bash

#$1 - osd list
#$2 - Ceph rebalance completion pattern, e.g. "12288 active+clean"
osd=$1
c_pattern=$2

function wait_more() {
   startT=$1
   endT=$(date +%s)
   #echo $endT $startT
   duration=$(expr $endT - $startT)
   echo "waiting stat and load modules to complete"
   while [[ $duration -le $2 ]]; do
     sleep 1
     endT=$(date +%s)
     duration=$(expr $endT - $startT)
   done
   echo "proceeding"
}

function do_test(){
   B=$1
   R=$2
   N=$3
   W=$4
   monitorT=$5

   testN=$(expr $testN + 1 )
   printf ">>>>>>> Test %d (%d %d %d %f)\n" $testN $B $R $N $W
   ./setup.sh $R $B >/dev/nul 2>/dev/nul

   startTime=$(date +%s)

   printf ">>>>>>> Test %d (%d %d %d %f)" $testN $B $R $N $W >>net.log
   ../ceph_info.sh $monitorT >>net.log &
   sleep 5 # wait for some time for monitor to start
   printf ">>>>>>> Test %d (%d %d %d %f)" $testN $B $R $N $W >>fio.log 
   #NB!!!!: FIO Version needs to be >= fio-2.2.9, regular Ubuntu repositories provides less one at the moment
   sudo /usr/local/bin/fio randwrite.cfg >>fio.log & 

   ./in.sh "$osd" "$c_pattern" $N $W $W 1
   monitorT=$(expr $monitorT + 20)
   wait_more $startTime $monitorT
   ./reset.sh "$osd" "$c_pattern" >/dev/nul 2>/dev/nul
}

printf "Preparing....\n"
./reset.sh "$osd" "$c_pattern" >/dev/nul 2>/dev/nul

testN=0
monitorDuration=1550 #this should be large enough to cover FIO generation, see fio config for corresponding value

#NB!!! one should pprovide test cases below
do_test 5 8 8 0.25 
do_test 10 15 1 1
do_test 10 15 3 1
