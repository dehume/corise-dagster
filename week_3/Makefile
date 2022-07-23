.PHONY: dagster_start
dagster_start:
	@docker compose --profile dagster up --build

.PHONY: dagster_start_detached
dagster_start_detached:
	@docker compose --profile dagster up -d --build

.PHONY: dagster_restart_content
dagster_restart_content:
	@docker container restart $(docker ps -aqf "name=content")

.PHONY: dagster_restart_project
dagster_restart_project:
	@docker container restart $(docker ps -aqf "name=project")

.PHONY: dagster_down
dagster_down:
	@docker compose --profile dagster down --remove-orphans