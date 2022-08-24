# ----------------------------------------- #
#                 Base
# ----------------------------------------- #
FROM python:3.8.5-slim AS base

ARG COURSE_WEEK
ENV VIRTUAL_ENV=/opt/venv
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV POETRY_VERSION=1.1.12
ENV POETRY_HOME=/opt/poetry

# ----------------------------------------- #
#                 Builder
# ----------------------------------------- #
FROM base AS builder

RUN python -m venv "$VIRTUAL_ENV" && \
    mkdir -p "$DAGSTER_HOME"

ENV PATH="$VIRTUAL_ENV/bin:$POETRY_HOME/bin:$PATH"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y --no-install-recommends \
        curl \
        build-essential \
        libpq-dev && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/* && \
    curl -sSL https://install.python-poetry.org | python -

COPY poetry.lock pyproject.toml /
RUN pip install --no-cache-dir --upgrade pip==21.3.1 setuptools==60.2.0 wheel==0.37.1 && \
    poetry config virtualenvs.path "$VIRTUAL_ENV" && \
    poetry install --no-root --no-interaction --no-ansi --no-dev

# ----------------------------------------- #
#                  Runner
# ----------------------------------------- #
FROM base AS runner

ARG COURSE_WEEK
ENV PATH="$VIRTUAL_ENV/bin:$POETRY_HOME/bin:$PATH"

RUN groupadd -r dagster && useradd -m -r -g dagster dagster

COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
COPY --from=builder --chown=dagster $DAGSTER_HOME $DAGSTER_HOME

WORKDIR $DAGSTER_HOME

# ----------------------------------------- #
#                  Dagit
# ----------------------------------------- #
FROM runner AS dagit
# USER dagster:dagster
EXPOSE 3000
CMD ["dagit", "-h", "0.0.0.0", "--port", "3000", "-w", "workspace.yaml"]

# ----------------------------------------- #
#                  Daemon
# ----------------------------------------- #
FROM runner AS daemon
# USER dagster:dagster
CMD ["dagster-daemon", "run"]

# ----------------------------------------- #
#       User Code Repository Week 2
# ----------------------------------------- #
FROM runner AS week_2
COPY week_2/dagster_ucr/ ./dagster_ucr
USER dagster:dagster
EXPOSE 4000
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "dagster_ucr/repo.py"]

# ----------------------------------------- #
#       User Code Repository Week 3/4
# ----------------------------------------- #
FROM runner AS week_3_4_content
ENV DAGSTER_CURRENT_IMAGE=corise-dagster-answer-key_content
ARG COURSE_WEEK
COPY ${COURSE_WEEK}/content/ ./content
USER dagster:dagster
EXPOSE 4000
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "content/repo.py"]

FROM runner AS week_3_4_project
ENV DAGSTER_CURRENT_IMAGE=corise-dagster-answer-key_project
ARG COURSE_WEEK
COPY ${COURSE_WEEK}/project/ ./project
USER dagster:dagster
EXPOSE 4001
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4001", "-f", "project/repo.py"]
