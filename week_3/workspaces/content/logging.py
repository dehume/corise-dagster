import json
import logging

from dagster import Field, InitLoggerContext, graph, logger, op


def message_slack(api_key: str, message: str):
    # Pretend API call
    print(message)


class JsonFormatter(logging.Formatter):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def format(self, record):
        message_slack(self.api_key, record.__dict__["msg"])
        return json.dumps(record.__dict__)


@logger(
    {
        "api_key": Field(str, is_required=True),
        "log_level": Field(str, is_required=False, default_value="INFO"),
        "name": Field(str, is_required=False, default_value="CoRise"),
    },
    description="Our custom CoRise logger",
)
def corise_logger(init_context: InitLoggerContext):
    api_key = init_context.logger_config["api_key"]
    level = init_context.logger_config["log_level"]
    name = init_context.logger_config["name"]

    klass = logging.getLoggerClass()
    logger_ = klass(name, level=level)

    handler = logging.StreamHandler()

    handler.setFormatter(JsonFormatter(api_key=api_key))
    logger_.addHandler(handler)

    return logger_


@op
def print_logging(context):
    print("Using print()")


@op
def basic_logging(context, start_after):
    context.log.info("Logging via context: info")
    context.log.warning("Logging via context: warning")
    context.log.error("Logging via context: error")


@graph
def logging_graph():
    basic_logging(print_logging())


logging_dev_job = logging_graph.to_job(name="logging_dev_job")

logging_prod_job = logging_graph.to_job(
    name="logging_prod_job",
    config={"loggers": {"corise_logger": {"config": {"api_key": "XXX", "log_level": "ERROR"}}}},
    logger_defs={"corise_logger": corise_logger},
)