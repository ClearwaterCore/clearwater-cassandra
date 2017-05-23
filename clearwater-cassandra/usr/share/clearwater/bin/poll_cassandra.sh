#!/bin/bash

# @file poll_cassandra.sh
#
# Copyright (C) Metaswitch Networks 2017
# If license terms are provided to you in a COPYING file in the root directory
# of the source code repository by which you are accessing this code, then
# the license outlined in that COPYING file applies to your use.
# Otherwise no rights are granted except for those provided to you by
# Metaswitch Networks in a separate written agreement.

# Get how long the cassandra process has been running. If this is less than
# 2 minutes, then don't poll cassandra (as it may not be up yet), unless
# we're specifically asked to.
# Getting the uptime can fail if the cassandra process fails - this is caught
# by the monit script.

. /usr/share/clearwater/utils/check-root-permissions 1

[ $# -le 1 ] || { echo "Usage: poll_cassandra [--no-grace-period] (defaults to a two minute grace period)" >&2 ; exit 2 ; }

if [ -z "$1" ]; then
  value=$( ps -p $( cat /var/run/cassandra/cassandra.pid ) -o etimes=)
  if [ $? == 0 ] && [ "$value" -lt 120 ]; then
    exit 0
  fi
fi

# This script polls a cassandra process and check whether it is healthy by checking
# that the 9160 port is open at cassandra_hostname. This script should be
# run in the signaling namespace if set.

. /etc/clearwater/config

[ ! -z "$cassandra_hostname" ] || cassandra_hostname="127.0.0.1"
/usr/share/clearwater/bin/run-in-signaling-namespace /usr/share/clearwater/bin/poll-tcp 9160 $cassandra_hostname
rc=$?
if [[ $rc != 0 ]]
then
  /usr/share/clearwater/bin/run-in-signaling-namespace nodetool enablethrift
fi
exit $rc
