#!/bin/bash

# @file list_backups.sh
#
# Copyright (C) Metaswitch Networks 2017
# If license terms are provided to you in a COPYING file in the root directory
# of the source code repository by which you are accessing this code, then
# the license outlined in that COPYING file applies to your use.
# Otherwise no rights are granted except for those provided to you by
# Metaswitch Networks in a separate written agreement.

readonly ERROR_USER=1

die () {
  # Set the error code to the value of the second argument if it exists,
  # otherwise default it to a system error.
  rc=${2:-$ERROR_SYSTEM}
  echo >&2 "$1"
  exit $rc
}

[ "$#" -ge 1 ] || die "Usage: list_backup.sh <keyspace> [backup directory]" $ERROR_USER
KEYSPACE=$1
COMPONENT=$(cut -d_ -f1 <<< $KEYSPACE)
DATABASE=$(cut -d_ -f2 <<< $KEYSPACE)
BACKUP_DIR=$2
DATA_DIR=/var/lib/cassandra/data
[ -d "$DATA_DIR/$KEYSPACE" ] || die "Keyspace $KEYSPACE does not exist" $ERROR_USER

if [[ -z "$BACKUP_DIR" ]]
then
  if [ -n "$DATABASE" ]
  then
    BACKUP_DIR="/usr/share/clearwater/$COMPONENT/backup/backups/$DATABASE"
  else
    BACKUP_DIR="/usr/share/clearwater/$COMPONENT/backup/backups"
  fi
  echo "No backup directory specified, defaulting to $BACKUP_DIR"
else
  echo "Will look for backups in $BACKUP_DIR"
fi

if [[ "$(ls -A $BACKUP_DIR 2> /dev/null)" ]]
then
  for b in $BACKUP_DIR/*
  do
    SNAPSHOT=`basename $b`
    echo "$SNAPSHOT"
  done
else
  echo "No backups exist in $BACKUP_DIR"
fi
