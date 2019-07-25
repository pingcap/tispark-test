#!/bin/bash
set -e
postfix=scripts
DIR=$PWD
PAR_DIR=${PWD%/$postfix}
DBGEN=$PAR_DIR/dbgen
prefix=$DBGEN/
write_to_tidb()
{
    rm -rf $DBGEN/*.sql
    for tbl in `ls $DBGEN/*.tbl`; do
        table=$(echo "${tbl%.*}")
        name=${table#$prefix}
	      sql_file="$table.sql"
	      if [ ! -f "$sql_file" ] ; then
        	  touch "$sql_file"
	      fi
        echo $sql_file
        echo "LOAD DATA LOCAL INFILE '$tbl' INTO TABLE $name" >> $sql_file
        echo "FIELDS TERMINATED BY '|';" >> $sql_file
	      # mysql -h 127.0.0.1 -P 4000 -u root --local-infile=1 -D tpch < $sql_file&
    done
    echo "starting waiting mysql..."
    FAIL=0
    for job in `jobs -p`;
    do
        wait $job || let "FAIL+=1"
    done
    if [ "$FAIL" == "0" ];
    then
        echo "finished"
    else
        echo "FAIL! ($FAIL)"
    fi
    echo "finish mysql..."
}

write_to_tidb
