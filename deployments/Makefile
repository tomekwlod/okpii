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