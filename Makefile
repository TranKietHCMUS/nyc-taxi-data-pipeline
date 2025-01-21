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


.PHONY: run_storage
run_storage:
	docker-compose -f docker-compose-storage.yaml up -d


.PHONY: down_storage
down_storage:
	docker-compose -f docker-compose-storage.yaml down -v


.PHONY: run_airflow
run_airflow:
	docker-compose -f docker-compose-airflow.yaml up -d


.PHONY: down_airflow
down_airflow:
	docker-compose -f docker-compose-airflow.yaml down -v


.PHONY: run_kafka 
run_kafka:
	docker-compose -f docker-compose-kafka.yaml up -d


.PHONY: down_kafka
down_kafka:
	docker-compose -f docker-compose-kafka.yaml down -v


.PHONY: run_app
run_app:
	docker-compose -f docker-compose-app.yaml up -d


.PHONY: down_app
down_app:
	docker-compose -f docker-compose-app.yaml down -v