# Tests
.PHONY: week_1_tests
week_1_tests:
	@pytest week_1 -vv -m "not challenge"

.PHONY: week_1_challenge_tests
week_1_challenge_tests:
	@pytest week_1 -vv

.PHONY: week_2_tests
week_2_tests:
	@pytest week_2 -vv

.PHONY: week_3_tests
week_3_tests:
	@pytest week_3 -vv

.PHONY: week_4_tests
week_4_tests:
	@pytest week_4 -vv


# Week 2
.PHONY: week_2_start
week_2_start:
	@docker compose --env-file=week_2/.course_week --profile dagster --profile week_2 up -d --build

.PHONY: week_2_down
week_2_down:
	@docker compose --env-file=week_2/.course_week --profile dagster --profile week_2 down --remove-orphans

.PHONY: week_2_restart_workspace
week_2_restart_workspace:
	@docker container restart $$(docker ps -aqf "name=ucr")


# Week 3
.PHONY: week_3_start
week_3_start:
	@docker compose --env-file=week_3/.course_week --profile dagster --profile week_3_4 up -d --build

.PHONY: week_3_down
week_3_down:
	@docker compose --env-file=week_3/.course_week --profile dagster --profile week_3_4 down --remove-orphans

.PHONY: week_3_restart_workspace_content
week_3_restart_workspace_content:
	@docker container restart $$(docker ps -aqf "name=content")

.PHONY: week_3_restart_workspace_project
week_3_restart_workspace_project:
	@docker container restart $$(docker ps -aqf "name=project")


# Week 4
.PHONY: week_4_start
week_4_start:
	@docker compose --env-file=week_4/.course_week --profile dagster --profile week_3_4 up -d --build

.PHONY: week_4_down
week_4_down:
	@docker compose --env-file=week_4/.course_week --profile dagster --profile week_3_4 down --remove-orphans

.PHONY: week_4_restart_workspace_content
week_4_restart_workspace_content:
	@docker container restart $$(docker ps -aqf "name=content")

.PHONY: week_4_restart_workspace_project
week_4_restart_workspace_project:
	@docker container restart $$(docker ps -aqf "name=project")