#!/bin/bash

DATA_DIR=/Users/user/de-2024/data

TARGET_DATE=`date -v-1d "+%Y-%m-%d-%H"`
TARGET_FILE=${TARGET_DATE}.json.gz
TARGET_URL=https://data.githubarchive.org/${TARGET_FILE}

wget ${TARGET_URL} -P ${DATA_DIR} --backups=0
gunzip -f ${DATA_DIR}/${TARGET_FILE}

