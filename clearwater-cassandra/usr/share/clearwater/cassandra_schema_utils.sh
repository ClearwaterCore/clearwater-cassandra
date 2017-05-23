#! /bin/bash

# Copyright (C) Metaswitch Networks 2017
# If license terms are provided to you in a COPYING file in the root directory
# of the source code repository by which you are accessing this code, then
# the license outlined in that COPYING file applies to your use.
# Otherwise no rights are granted except for those provided to you by
# Metaswitch Networks in a separate written agreement.

. /etc/clearwater/config

# The default replication factor is 2, but the calling script may have other
# ideas.
replication_factor=${replication_factor:-2}

# Create the common REPLICATION string to be used when creating Clearwater
# Cassandra schemas. This file gets dotted in by the schema creation scripts.
replication_str="{'class': 'SimpleStrategy', 'replication_factor': $replication_factor}"

if [ -n "$remote_site_name" ] && [ -z "$remote_site_names" ]
then
  remote_site_names=$remote_site_name
fi

# If local_site_name and remote_site_names are set then this is a GR
# deployment. Set the replication strategy to NetworkTopologyStrategy and
# define the sites.
if [ -n "$local_site_name" ] && [ -n "$remote_site_names" ]
then
  IFS=',' read -a remote_site_names_array <<< "$remote_site_names"
  replication_str="{'class': 'NetworkTopologyStrategy', '$local_site_name': $replication_factor"
  for remote_site in "${remote_site_names_array[@]}"
  do
    replication_str+=", '$remote_site': $replication_factor"
  done
  replication_str+="}"
fi

function quit_if_no_cassandra() {

  dpkg-query -W -f='${Status}\n' clearwater-cassandra 2> /dev/null | grep -q "install ok installed"
  cassandra_installed_rc=$?

  if [ $cassandra_installed_rc -ne 0 ]
  then
    echo "Cassandra is not installed yet, skipping schema addition for now"
    exit 0
  fi

  if [ ! -e /etc/cassandra/cassandra.yaml ]
  then
    echo "Cassandra is not configured yet, skipping schema addition for now"
    exit 0
  fi
}
