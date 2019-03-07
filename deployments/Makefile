stop:
	docker-compose stop

build:
	docker-compose build

start:
	docker-compose up -d

destroy:
	make stop
	docker-compose rm -f

rebuild:
	make destroy
	make build
	make start


godump:
	docker-compose run --rm  go-dump

goimport:
	docker-compose run --rm  go-import

# Run below to populate the ENVs in dev (DO NOT RUN IT IN PROD!!) 
# set -a && . ./.env && set +a && echo $(cut -d= -f1 ./.env) && echo ${MYSQL_USER}