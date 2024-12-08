#!/bin/bash

DB="STELLAR"
DIR="/opt/db_backups/"

KEEP_DAILY=7

date=`date +%Y-%m-%d`

# Do the backup
pg_dump -U railgun --clean railgun_internal > /opt/db_backups/${DB}_$date.sql
ls $DIR | sort -rn | awk " NR > $KEEP_DAILY" | while read f; do rm $DIR/$f; done
