#!/bin/bash

# @file poll_cassandra_ring.sh
#
# Project Clearwater - IMS in the Cloud
# Copyright (C) 2014  Metaswitch Networks Ltd
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

# This script is used by Monit to check connectivity to remote Cassandra
# instances. If connectivity is lost to any instances in the ring an
# alarm will be issued (major severity for a single instance, critical
# severity for more than one instance). When connectivity is restored to
# all instances, the alarm is cleared. Alarms are issued here vs. the Monit
# DSL to avoid retransmissions.
#
# If the local instance is not running, nodetool will fail. In that case
# we do not issue an alarm and return a good status. The Clearwater pro-
# cess using Cassandra is responsible for alarming communication issues
# with the local instance. 
#
# Execution of nodetool is "niced" to minimize the impact of its JVM use.


alarm_state_file="/tmp/.cassandra_ring_alarm_issued"

. /etc/clearwater/config
[ -z "$signaling_namespace" ] || namespace_prefix="ip netns exec $signaling_namespace"

# Return state of Cassandra ring, 0 if all nodes are up, otherwise the 
# count of nodes that are down. 
ring_state()
{
    local state=0
    # Run nodetool to get the status of nodes in the ring, if successful
    # continue to check node status, otherwise return 0 (local Cassandra
    # failure is not considered a ring error). 
    local out=`nice -n 19 $namespace_prefix nodetool status 2> /dev/null`
    if [ "$?" = 0 ] ; then
        # Look through nodetool output for status lines. These begin
        # with two uppercase characters, indicating Status and State,
        # followed by whitespace. The first character is what we are
        # interested in: U = Up status, D = Down status. 
        local state_regex="^([UD])[NLJM]\s+.*"
        IFS=$'\n'
        for line in $out
        do
            if [[ $line =~ $state_regex ]] ; then
                if [ "${BASH_REMATCH[1]}" == "D" ] ; then
                    # Accumulate the count of down nodes for return.
                    let state+=1
                fi
            fi
        done
    fi
    return $state
}


check_clear_alarm()
{
    if [ -f $alarm_state_file ] ; then
        rm -f $alarm_state_file
        /usr/share/clearwater/bin/issue_alarm.py "monit" "4001.1"
    fi
}


check_issue_major_alarm()
{
    if [ ! -f $alarm_state_file ] || [ `cat $alarm_state_file` != "major" ] ; then
        echo "major" > $alarm_state_file
        /usr/share/clearwater/bin/issue_alarm.py "monit" "4001.4"
    fi
}


check_issue_critial_alarm()
{
    if [ ! -f $alarm_state_file ] || [ `cat $alarm_state_file` != "critical" ] ; then
        echo "critical" > $alarm_state_file
        /usr/share/clearwater/bin/issue_alarm.py "monit" "4001.3"
    fi
}


ring_state
state=$? 

echo $state

if [ "$state" = 0 ] ; then
    check_clear_alarm
elif [ "$state" = 1 ] ; then
    check_issue_major_alarm
else
    check_issue_critial_alarm
fi

exit $state

