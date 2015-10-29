#!/bin/bash

# @file restore_backup.sh
#
# Project Clearwater - IMS in the Cloud
# Copyright (C) 2013  Metaswitch Networks Ltd
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

readonly ERROR_USER=1
readonly ERROR_SYSTEM=2

die () {
  # Set the error code to the value of the second argument if it exists,
  # otherwise default it to a system error.
  rc=${2:-$ERROR_SYSTEM}
  echo >&2 "$1"
  exit $rc
}

[ "$#" -ge 1 ] || die "Usage: restore_backup.sh <keyspace> [backup] (will default to latest backup if none specified) [backup directory]" $ERROR_USER
KEYSPACE=$1
COMPONENT=$(cut -d_ -f1 <<< $KEYSPACE)
DATABASE=$(cut -d_ -f2 <<< $KEYSPACE)
BACKUP=$2
BACKUP_DIR=$3
DATA_DIR=/var/lib/cassandra/data
COMMITLOG_DIR=/var/lib/cassandra/commitlog

if [[ -z "$BACKUP" ]]
then
  echo "No backup specified, will attempt to restore from latest"
else
  echo "Will attempt to restore from backup $BACKUP"
fi

if [[ -z "$BACKUP_DIR" ]]
then
  if [ -n "$DATABASE" ]
  then
    BACKUP_DIR="/usr/share/clearwater/$COMPONENT/backup/backups/$DATABASE"
  else
    BACKUP_DIR="/usr/share/clearwater/$COMPONENT/backup/backups"
  fi
  echo "No backup directory specified, will attempt to restore from $BACKUP_DIR"
else
  echo "Will attempt to restore from directory $BACKUP_DIR"
fi

if [[ "$(ls -A $BACKUP_DIR)" ]]
then
  if [[ -z $BACKUP ]]
  then
    echo "No valid backup specified, will attempt to restore from latest"
    BACKUP=$(ls -t $BACKUP_DIR | head -1)
    mkdir -p $DATA_DIR
  elif [ -d "$BACKUP_DIR/$BACKUP" ]
  then
    echo "Found backup directory $BACKUP_DIR/$BACKUP"
  else
    die "Could not find specified backup directory $BACKUP_DIR/$BACKUP, use list_backups to see available backups" $ERROR_USER
  fi
else
  echo "No backups exist in $BACKUP_DIR"
fi

# We've made sure all the necessary backups exist, proceed with backup
[ -d "$DATA_DIR/$KEYSPACE" ] || die "Keyspace $KEYSPACE does not exist" $ERROR_USER
echo "Restoring backup for keyspace $KEYSPACE..."

# Stop monit from restarting Cassandra while we restore
monit unmonitor -g cassandra

# Stop Cassandra.  We remove any xss=.., as this can be printed out by
# cassandra-env.sh
service cassandra stop | grep -v "^xss = "

echo "Clearing commitlog..."
rm -rf $COMMITLOG_DIR/*

for t in $BACKUP_DIR/$BACKUP/*
do
  TABLE=`basename $t`
  TARGET_DIR=$DATA_DIR/$KEYSPACE/$TABLE
  mkdir -p $TARGET_DIR
  echo "$TABLE: Deleting old .db files..."
  find $TARGET_DIR -maxdepth 1 -type f -exec rm -f {} \;
  echo "$TABLE: Restoring from backup: $BACKUP"
  cp $BACKUP_DIR/$BACKUP/$TABLE/* $TARGET_DIR
  chown cassandra:cassandra $TARGET_DIR/*
done

# Start Cassandra.  We remove any xss=.., as this can be printed out by
# cassandra-env.sh
service cassandra start | grep -v "^xss = "
monit monitor -g cassandra
