# Tests
.PHONY: week_1_tests
week_1_tests:
	@pytest week_1 -vv -m "not challenge" --disable-warnings

.PHONY: week_1_challenge_tests
week_1_challenge_tests:
	@pytest week_1 -vv --disable-warnings

.PHONY: week_2_tests
week_2_tests:
	@pytest week_2 -vv --disable-warnings

.PHONY: week_3_tests
week_3_tests:
	@pytest week_3 -vv --disable-warnings

.PHONY: week_4_tests
week_4_tests:
	@pytest week_4 -vv --disable-warnings


# Week 2
.PHONY: week_2_start
week_2_start:
	@docker compose --env-file=week_2/.course_week --profile dagster up -d --build

.PHONY: week_2_down
week_2_down:
	@docker compose --env-file=week_2/.course_week --profile dagster down --remove-orphans


# Week 3
.PHONY: week_3_start
week_3_start:
	@docker compose --env-file=week_3/.course_week --profile dagster up -d --build

.PHONY: week_3_down
week_3_down:
	@docker compose --env-file=week_3/.course_week --profile dagster down --remove-orphans


# Week 4
.PHONY: week_4_start
week_4_start:
	@docker compose --env-file=week_4/.course_week --profile dagster up -d --build

.PHONY: week_4_down
week_4_down:
	@docker compose --env-file=week_4/.course_week --profile dagster down --remove-orphans


# Restart UCRs
.PHONY: restart_content
restart_content:
	@docker container restart $$(docker ps -aqf "name=content")

.PHONY: restart_project
restart_project:
	@docker container restart $$(docker ps -aqf "name=project")

.PHONY: restart_challenge
restart_challenge:
	@docker container restart $$(docker ps -aqf "name=challenge")