#!/bin/bash

# @file poll_cassandra_ring.sh
#
# Copyright (C) Metaswitch Networks 2016
# If license terms are provided to you in a COPYING file in the root directory
# of the source code repository by which you are accessing this code, then
# the license outlined in that COPYING file applies to your use.
# Otherwise no rights are granted except for those provided to you by
# Metaswitch Networks in a separate written agreement.

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


# Return state of Cassandra ring, 0 if all nodes are up, otherwise the 
# count of nodes that are down. 
ring_state()
{
    local state=0
    # Run nodetool to get the status of nodes in the ring, if successful
    # continue to check node status, otherwise return 0 (local Cassandra
    # failure is not considered a ring error). 
    local out=`nice -n 19 /usr/share/clearwater/bin/run-in-signaling-namespace nodetool status 2> /dev/null`
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
        /usr/share/clearwater/bin/issue-alarm "monit" "4001.1"
    fi
}


check_issue_major_alarm()
{
    if [ ! -f $alarm_state_file ] || [ `cat $alarm_state_file` != "major" ] ; then
        echo "major" > $alarm_state_file
        /usr/share/clearwater/bin/issue-alarm "monit" "4001.4"
    fi
}


check_issue_critial_alarm()
{
    if [ ! -f $alarm_state_file ] || [ `cat $alarm_state_file` != "critical" ] ; then
        echo "critical" > $alarm_state_file
        /usr/share/clearwater/bin/issue-alarm "monit" "4001.3"
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

