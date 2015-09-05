#!/bin/bash

start_t=$(date +%s)
s=$1
repeat=$2
repeat0=$3

if [ -z "$repeat" ] 
then
  repeat=3
fi
if [ -z "$repeat0" ] 
then
  repeat0=8
fi
echo "wait for start(" $repeat0 $repeat ")..."

cnt=$repeat0
out=$(ceph -s | grep "$s")
while [ $cnt -gt 0 ]; do
#while [ -n "$out" ] 
$do
   if [ -n "$out" ]; then
     cnt=$(expr $cnt - 1)
     sleep 1
     out=$(ceph -s | grep "$s")
   else
     cnt=0
   fi
done

echo "wait for complete..."
cnt=$repeat
cnt2=1
while [ $cnt -gt 0 ]; do
   sleep 1
   if [ -n "$out" ]; then
     cnt=$(expr $cnt - 1)
     #echo "...." $cnt
   else
     cnt=$repeat
   fi
   out=$(ceph -s | grep "$s")
   
   cnt2=$(expr $cnt2 - 1)
   
   if [ $cnt2 -eq 0 ]; then
      cnt2=4
      out2=$(ceph -s | grep ".*active+clean")
      printf "\r %s" "$out2"
   fi
done
end_t=$(date +%s)
echo 
echo "Wait completed in" $(expr $end_t - $start_t) s.
