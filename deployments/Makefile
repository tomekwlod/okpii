stop:
	docker-compose stop

build:
	make destroy
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


goimport:
	docker-compose run --rm go-import
godump:
	docker-compose run --rm go-dump
gomatching:
	# USAGE: make gomatching -did=1,2,3 [-onekey=WEM0123456789]
	docker-compose run --rm go-matching ./matching $(filter-out $@,$(MAKECMDGOALS))

%:
	@:
## %: <-- does nothing to silently ignore the other goals

# Run below to populate the ENVs in dev (DO NOT RUN IT IN PROD!!) 
# set -a && . ./.env && set +a && echo $(cut -d= -f1 ./.env) && echo ${MYSQL_USER}