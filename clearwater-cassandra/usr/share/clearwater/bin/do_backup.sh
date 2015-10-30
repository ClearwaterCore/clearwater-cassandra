#!/bin/bash

# @file do_backup.sh
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

# Read in the configured signalling namespace prefix as '$namespace_prefix'
. /etc/clearwater/config

[ "$#" -eq 1 ] || die "Usage: do_backup.sh <keyspace>" $ERROR_USER

KEYSPACE=$1
COMPONENT=$(cut -d_ -f1 <<< $KEYSPACE)
DATABASE=$(cut -d_ -f2 <<< $KEYSPACE)
DATA_DIR=/var/lib/cassandra/data

[ -d "$DATA_DIR/$KEYSPACE" ] || die "Keyspace $KEYSPACE does not exist" $ERROR_USER

if [ -n "$DATABASE" ]
then
  BACKUP_DIR="/usr/share/clearwater/$COMPONENT/backup/backups/$DATABASE"
else
  BACKUP_DIR="/usr/share/clearwater/$COMPONENT/backup/backups"
fi

if [[ ! -d "$BACKUP_DIR" ]]
then
  mkdir -p $BACKUP_DIR
  echo "Created backup directory $BACKUP_DIR"
fi

# Remove old backups from the backup directory (keeping last 3)
for f in $(ls -t $BACKUP_DIR | tail -n +4)
do
  echo "Deleting old backup: $BACKUP_DIR/$f"
  rm -r $BACKUP_DIR/$f
done

# Create new backup.  We remove any xss=.., as this can be printed out by
# cassandra-env.sh.
echo "Creating backup for keyspace $KEYSPACE..."
$namespace_prefix nodetool -h localhost -p 7199 snapshot "$KEYSPACE" | grep -v "xss = "

# Check we successfully took the snapshot by looking at the return code
# for the nodetool command
[ $? -eq 0 ] || die "Failed to take snapshot of the database" $ERROR_SYSTEM

# Find all the non-empty snapshot directories in the keyspace we're taking a
# snapshot of. Cassandra keeps snapshots per column family, so we have to find
# them individually.
SNAPSHOT_DIRS=$(find $DATA_DIR/$KEYSPACE -type d -not -empty | grep 'snapshots$')

# Check we have enough disk space to copy over to the backup directory
SNAPSHOT_SIZE=0
for d in $SNAPSHOT_DIRS
do
  # Find the most recent snapshot
  SNAPSHOT=$(ls -t $d | head -1)
  SNAPSHOT_SIZE=$(($SNAPSHOT_SIZE + $(du -sk $d/$SNAPSHOT | cut -f 1)))
done

AVAILABLE_SPACE=$(df -k $BACKUP_DIR | grep -v "Filesystem" | awk '{ print $4 }')

# Only continue if the available space is at least as big as our snapshot
(($AVAILABLE_SPACE >= $SNAPSHOT_SIZE)) || die "Not enough available disk space to take backup" $ERROR_SYSTEM

# Now copy over the shapshot.
for d in $SNAPSHOT_DIRS
do
  # Work out the table name from the path of the snapshots directory
  TABLE=$(basename $(dirname $d))

  # Find the most recent snapshot
  SNAPSHOT=$(ls -t $d | head -1)
  mkdir -p $BACKUP_DIR/$SNAPSHOT/$TABLE
  cp -al $d/$SNAPSHOT/* $BACKUP_DIR/$SNAPSHOT/$TABLE
done

# Finally remove the snapshots from the Cassandra data directory, leaving only
# the backups in the backup directory.  We remove any xss=.., as this can be
# printed out by cassandra-env.sh.
$namespace_prefix nodetool clearsnapshot "$KEYSPACE" | grep -v "^xss = "

echo "Backups can be found at: $BACKUP_DIR"
