#!/bin/bash

#$1 - osd list
#$2 - completion pattern
#$3 - osd portion size
#$4-$6 - start weight, delta, max weight
osd=$1
c_pattern=$2
portion=$3
sW=$4
dW=$5
eW=$6

start_t=$(date +%s)
cnt=$portion
for w in $(seq $sW $dW $eW); do
    #echo $i
    for i in $osd; do
       if [ $cnt -eq 0 ]; then
          ./w.sh "$c_pattern"
          cnt=$portion
       fi
       ceph osd reweight $i $w
       cnt=$(expr $cnt - 1)
    done
done
./w.sh "$c_pattern"
end_t=$(date +%s)
echo Reweighted in $(expr $end_t - $start_t) s.
