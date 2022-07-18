.PHONY: dagster_start
dagster_start:
	@docker compose --profile dagster up --build

.PHONY: dagster_start_detached
dagster_start_detached:
	@docker compose --profile dagster up -d --build

.PHONY: dagster_restart
dagster_restart:
	@docker container restart $(docker ps -aqf "name=ucr")

.PHONY: dagster_down
dagster_down:
	@docker compose --profile dagster down --remove-orphans