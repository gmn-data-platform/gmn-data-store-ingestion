.ONESHELL:
SHELL=/bin/bash

build_all_services: build_kafka build_kafka_database_sink build_trajectory_summary_batch_ingestion
init_all_services: init_kafka
run_all_services: run_kafka run_kafka_database_sink run_trajectory_summary_batch_ingestion
stop_all_services: stop_kafka stop_kafka_database_sink stop_trajectory_summary_batch_ingestion
stop_and_clean_all_services: stop_and_clean_kafka stop_and_clean_kafka_database_sink

build_kafka:
	echo "Building kafka"
	docker-compose -f ./services/kafka/docker-compose.yaml build
	docker-compose -f ./services/kafka/docker-compose.yaml --profile=init build

init_kafka:
	echo "Initializing kafka"
	docker-compose -f ./services/kafka/docker-compose.yaml --profile=init up --abort-on-container-exit --force-recreate

run_kafka:
	echo "Running kafka"
	docker-compose -f ./services/kafka/docker-compose.yaml up -d

stop_kafka:
	echo "Stopping kafka"
	docker-compose -f ./services/kafka/docker-compose.yaml down --remove-orphans

logs_kafka:
	echo "Logs kafka"
	docker-compose -f ./services/kafka/docker-compose.yaml logs -f

stop_and_clean_kafka:
	read -r -p "Are you sure you want to stop and clean all services (this will remove the database volume)? [y/N] " response
	case "$response" in
		[yY][eE][sS]|[yY])
			do_something
			;;
		*)
			do_something_else
			;;
	esac
	echo "Stopping and cleaning kafka"
	docker-compose -f ./services/kafka/docker-compose.yaml down -v --rmi all


build_kafka_database_sink:
	docker-compose -f ./services/kafka_database_sink/docker-compose.yaml build

run_kafka_database_sink:
	docker-compose -f ./services/kafka_database_sink/docker-compose.yaml up -d

stop_kafka_database_sink:
	docker-compose -f ./services/kafka_database_sink/docker-compose.yaml down --remove-orphans

logs_kafka_database_sink:
	docker-compose -f ./services/kafka_database_sink/docker-compose.yaml logs -f

stop_and_clean_kafka_database_sink:
	read -r -p "Are you sure you want to stop and clean all services (this will remove the database volume)? [y/N] " response
	case "$response" in
		[yY][eE][sS]|[yY])
			do_something
			;;
		*)
			do_something_else
			;;
	esac
	echo "Stopping and cleaning kafka_database_sink"
	docker-compose -f ./services/kafka_database_sink/docker-compose.yaml down -v --rmi all


build_trajectory_summary_batch_ingestion:
	docker-compose -f ./services/trajectory_summary_batch_ingestion/docker-compose.yaml build

run_trajectory_summary_batch_ingestion:
	docker-compose -f ./services/trajectory_summary_batch_ingestion/docker-compose.yaml up -d

stop_trajectory_summary_batch_ingestion:
	docker-compose -f ./services/trajectory_summary_batch_ingestion/docker-compose.yaml down --remove-orphans

stop_and_clean_trajectory_summary_batch_ingestion:
	read -r -p "Are you sure you want to stop and clean all services (this will remove the database volume)? [y/N] " response
	case "$response" in
		[yY][eE][sS]|[yY])
			do_something
			;;
		*)
			do_something_else
			;;
	esac
	echo "Stopping and cleaning trajectory_summary_batch_ingestion"
	docker-compose -f ./services/trajectory_summary_batch_ingestion/docker-compose.yaml down -v --rmi all