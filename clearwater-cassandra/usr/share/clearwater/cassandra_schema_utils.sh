#! /bin/bash

# Project Clearwater - IMS in the Cloud
# Copyright (C) 2016 Metaswitch Networks Ltd
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
