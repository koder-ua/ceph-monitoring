#!/bin/bash

start_t=$(date +%s)

./setup.sh 15 10
for i in $1; do
  ceph osd reweight $i 0
done

./w.sh $2 8 3

end_t=$(date +%s)
echo elapsed $(expr $end_t - $start_t) s.
