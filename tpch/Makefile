tbl: dbgen
	cd dbgen && ./dbgen -s 0.05
dbgen:
	cd dbgen; make;
load:
	mysql -h 127.0.0.1 -P 4000 -u root -D test < dss.sql
	sh loadalldata.sh
cleansql:
	mv dss.sql dss.sqt
	rm -rf *.sql
	rm -rf dbgen/*.sql
	mv dss.sqt dss.sql
cleantbl:
	cd dbgen; make clean; rm -f *.tbl

.PHONY: dbgen load clean tbl
