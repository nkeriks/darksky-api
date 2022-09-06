VENV=ds


dataset:
	. ${VENV}/bin/activate && python build_df.py --place $(place)

update_some:
	. ${VENV}/bin/activate && python drive.py --place $(place) --noskip_today

update:
	. ${VENV}/bin/activate && python drive.py --place $(place) --skip_today

backfill: db
	#. ${VENV}/bin/activate && python backfill.py --place givatayim 
	#. ${VENV}/bin/activate && python backfill.py --place missoula
	. ${VENV}/bin/activate && python backfill.py --place sharon
	#. ${VENV}/bin/activate && python backfill.py --place san_carlos
	#. ${VENV}/bin/activate && python backfill.py --place minneapolis
	#. ${VENV}/bin/activate && python backfill.py --place foglo

db: darksky.sqlite

darksky.sqlite:
	sqlite3 darksky.sqlite < db_tables.sql

venv: ${VENV}/bin/activate

${VENV}/bin/activate: requirements.txt
	test -d ${VENV} || virtualenv -p python3 ${VENV}
	. ${VENV}/bin/activate && pip3 install -r requirements.txt
	touch ${VENV}/bin/activate

clean:
	rm -rf ${VENV}
