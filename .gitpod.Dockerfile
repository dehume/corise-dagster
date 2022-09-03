ARG PY_VERSION=3.8

FROM gitpod/workspace-python-${PY_VERSION}:latest

RUN pip install "poetry==1.1.12"

COPY poetry.lock pyproject.toml /

RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi