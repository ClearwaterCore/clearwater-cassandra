#!/bin/bash

# @file poll_cqlsh.sh
#
# Copyright (C) Metaswitch Networks 2017
# If license terms are provided to you in a COPYING file in the root directory
# of the source code repository by which you are accessing this code, then
# the license outlined in that COPYING file applies to your use.
# Otherwise no rights are granted except for those provided to you by
# Metaswitch Networks in a separate written agreement.

. /usr/share/clearwater/utils/check-root-permissions 1

[ $# -le 1 ] || { echo "Usage: poll_cqlsh [--no-grace-period] (defaults to a two minute grace period)" >&2 ; exit 2 ; }

# Get how long the cassandra process has been running. If this is less than
# 2 minutes, then don't poll cqlsh (as it may not be up yet), unless
# we're specifically asked to.
# Getting the uptime can fail if the cassandra process fails - this is caught
# by the monit script.
if [ -z "$1" ]; then
  value=$( ps -p $( cat /var/run/cassandra/cassandra.pid ) -o etimes=)
  if [ $? == 0 ] && [ "$value" -lt 120 ]; then
    exit 0
  fi
fi

# This script attempts to connect to cassandra over cqlsh. This script should be
# run in the signaling namespace if set.
/usr/share/clearwater/bin/run-in-signaling-namespace cqlsh -e quit
rc=$?
exit $rc
