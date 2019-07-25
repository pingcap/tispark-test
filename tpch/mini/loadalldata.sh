#!/bin/bash
set -e
postfix=scripts
prefix=dbgen/
write_to_tidb()
{
    rm -rf *.sql
    for tbl in `ls *.tbl`; do
        table=$(echo "${tbl%.*}")
	sql_file="$PWD/$table.sql"
	if [ ! -f "$sql_file" ] ; then
        	touch "$sql_file"
	fi
        echo "LOAD DATA LOCAL INFILE '$PWD/$tbl' INTO TABLE $table" >> $sql_file
        echo "FIELDS TERMINATED BY '|';" >> $sql_file
	mysql -h 127.0.0.1 -P 4000 -u root --local-infile=1 -D tpch < $sql_file&
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
    
