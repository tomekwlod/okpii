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
	# mkdir -p ../cmd/restful/log && chmod -R 777 ../cmd/restful/log/
	make destroy
	make build
	make start


goimport:
	# USAGE: make goimport
	docker-compose run --rm go-import    ./import   $(filter-out $@,$(MAKECMDGOALS))
godump:
	# USAGE: make godump -did=1,2,3 -countries=germany,poland
	docker-compose run --rm go-dump      ./dump     $(filter-out $@,$(MAKECMDGOALS))
gomatching:
	# USAGE: make gomatching -did=1,2,3 -onekey=WEM0123456789
	docker-compose run --rm go-matching  ./matching $(filter-out $@,$(MAKECMDGOALS))

%:
	@:
## %: - rule which match any task name;  
## @: - empty recipe = do nothing

# Run below to populate the ENVs in dev (DO NOT RUN IT IN PROD!!) 
# set -a && . ./.env && set +a && echo $(cut -d= -f1 ./.env) && echo ${MYSQL_USER}