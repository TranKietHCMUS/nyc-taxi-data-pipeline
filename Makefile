.PHONY: run_all

run_all:
	docker-compose -f docker-compose-kafka.yaml up -d
	docker-compose -f docker-compose-storage.yaml up -d
	docker-compose -f docker-compose-airflow.yaml up -d
	docker-compose -f docker-compose-app.yaml up -d

.PHONY: down_all

down_all:
	docker-compose -f docker-compose-kafka.yaml down -v
	docker-compose -f docker-compose-storage.yaml down -v
	docker-compose -f docker-compose-airflow.yaml down -v
	docker-compose -f docker-compose-app.yaml down -v

.PHONY: run_storage_kafka

run_storage_kafka:
	docker-compose -f docker-compose-kafka.yaml up -d
	docker-compose -f docker-compose-storage.yaml up -d
	docker-compose -f docker-compose-app.yaml up -d

.PHONY: down_storage_kafka

down_storage_kafka:
	docker-compose -f docker-compose-kafka.yaml down -v
	docker-compose -f docker-compose-storage.yaml down -v
	docker-compose -f docker-compose-app.yaml down -v

.PHONY: run_kafka 

run_kafka:
	docker-compose -f docker-compose-kafka.yaml up -d
	docker-compose -f docker-compose-app.yaml up -d

.PHONY: down_kafka

down_kafka:
	docker-compose -f docker-compose-kafka.yaml down -v
	docker-compose -f docker-compose-app.yaml down -v
