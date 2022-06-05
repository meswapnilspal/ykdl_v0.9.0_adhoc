#!/bin/bash

set -e

kinit -k -t /home/`whoami`/keytab.file `whoami`

INBOUND=/project/ykdl/ykdl
ETL_FOLDER=/kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_db
TODAY=`date '+%Y-%m-%d'`

fls=`ls ${INBOUND} | grep '.csv'`
for fl in ${fls}
do
  rmvExt=${fl::${#fl}-4}
  dt=${rmvExt:${#rmvExt}-10:10}
  dt1=${dt/-/}
  dt2=${dt1/-/}
  rmvDt=${rmvExt::${#rmvExt}-11}
  svr=${rmvDt##*_}
  tblRaw=${rmvDt/${svr}/}
  tbl=${tblRaw::${#tblRaw}-1}

  echo Doing ${fl}
  if [ "${TODAY}" != "${dt}" ]
  then
    {
      hdfs dfs -test -d ${ETL_FOLDER}/${tbl}_raw/ingestion_date=${dt2}
      echo ${fl} $?
      if [ $? -eq 0 ]
      then
        hdfs dfs -mkdir -p ${ETL_FOLDER}/${tbl}_raw/ingestion_date=${dt2}
      fi
    } || :

    hdfs dfs -put -f ${INBOUND}/${fl} ${ETL_FOLDER}/${tbl}_raw/ingestion_date=${dt2}/${tbl}_${svr}.csv
    rm -f ${INBOUND}/${fl}
  fi
done
