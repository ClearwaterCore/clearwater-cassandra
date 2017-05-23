#!/bin/bash

# @file restore_backup.sh
#
# Copyright (C) Metaswitch Networks 2017
# If license terms are provided to you in a COPYING file in the root directory
# of the source code repository by which you are accessing this code, then
# the license outlined in that COPYING file applies to your use.
# Otherwise no rights are granted except for those provided to you by
# Metaswitch Networks in a separate written agreement.

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

# The data is stored in /var/lib/cassandra/data/<KEYSPACE>/<table>-<UUID>.
# The <UUID> is matched to the particular Cassandra cluster - you must keep the
# same <UUID> for the data folder in order for Cassandra to pick up the updated
# db files
for t in $BACKUP_DIR/$BACKUP/*
do
  TABLE=`basename $t`
  PREFIX=`echo $TABLE | cut -d - -f 1`
  TARGET=`ls $DATA_DIR/$KEYSPACE/ | grep $PREFIX-`

  if [ -n "$TARGET" ]; then
    TARGET_DIR=$DATA_DIR/$KEYSPACE/$TARGET

    # Delete all the .db files for the old tables
    echo "$TABLE: Deleting old .db files..."
    rm -rf $TARGET_DIR/*

    # Now copy across the .db files for the table (making sure to keep the
    # existing folder names)
    echo "$TABLE: Restoring from backup: $BACKUP"
    cp $BACKUP_DIR/$BACKUP/$TABLE/* $TARGET_DIR
    chown cassandra:cassandra $TARGET_DIR/*
  fi
done

# Start Cassandra.  We remove any xss=.., as this can be printed out by
# cassandra-env.sh
service cassandra start | grep -v "^xss = "
monit monitor -g cassandra
