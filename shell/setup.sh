#!/bin/bash

ceph tell osd.* injectargs --osd_recovery_max_active $1
ceph tell osd.* injectargs --osd_max_backfills $2
