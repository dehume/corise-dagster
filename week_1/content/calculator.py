from dagster import Float, graph, op


class ConvertTemp:
    def fahrenheit_celsius(self, fahrenheit: float) -> float:
        return (fahrenheit - 32) * (5 / 9)


@op(config_schema={"fahrenheit": Float})
def convert(context):
    fahrenheit = context.op_config["fahrenheit"]
    celsius = (float(fahrenheit) - 32) * (5 / 9)
    context.log.info(celsius)


# @op(config_schema={"fahrenheit": Float})
# def convert(context):
#     fahrenheit = context.op_config["fahrenheit"]
#     celsius = ConvertTemp().fahrenheit_celsius(fahrenheit)
#     context.log.info(celsius)


@graph
def convert_temp():
    convert()


job = convert_temp.to_job()
