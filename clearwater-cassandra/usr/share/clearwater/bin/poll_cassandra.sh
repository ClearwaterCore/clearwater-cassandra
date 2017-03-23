#!/bin/bash

# @file poll_cassandra.sh
#
# Project Clearwater - IMS in the Cloud
# Copyright (C) 2015  Metaswitch Networks Ltd
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License, or (at your
# option) any later version, along with the "Special Exception" for use of
# the program along with SSL, set forth below. This program is distributed
# in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details. You should have received a copy of the GNU General Public
# License along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
#
# The author can be reached by email at clearwater@metaswitch.com or by
# post at Metaswitch Networks Ltd, 100 Church St, Enfield EN2 6BQ, UK
#
# Special Exception
# Metaswitch Networks Ltd  grants you permission to copy, modify,
# propagate, and distribute a work formed by combining OpenSSL with The
# Software, or a work derivative of such a combination, even if such
# copying, modification, propagation, or distribution would otherwise
# violate the terms of the GPL. You must comply with the GPL in all
# respects for all of the code used other than OpenSSL.
# "OpenSSL" means OpenSSL toolkit software distributed by the OpenSSL
# Project and licensed under the OpenSSL Licenses, or a work based on such
# software and licensed under the OpenSSL Licenses.
# "OpenSSL Licenses" means the OpenSSL License and Original SSLeay License
# under which the OpenSSL Project distributes the OpenSSL toolkit software,
# as those licenses appear in the file LICENSE-OPENSSL.

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
