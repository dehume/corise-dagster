import sqlalchemy
from dagster import IOManager, io_manager


def write_dataframe_to_table():
    pass


def read_dataframe_from_table():
    pass


class PostgresIOManager(IOManager):
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        schema: str,
        table: str,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.table = table
        self._engine = sqlalchemy.create_engine(self.uri)

    @property
    def uri(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}/{self.database}"

    def _execute_query(self, query: str):
        return self._engine.execute(query)

    def handle_output(self, context, obj):
        self._execute_query("INSERT INTO {self.schema}.{self.table} ")

    def load_input(self, context):
        return self._execute_query("SELECT * FROM ")


@io_manager(
    output_config_schema={
        "host": str,
        "user": str,
        "password": str,
        "database": str,
        "table": str,
        "schema": str,
    }
)
def postgres_io_manager(init_context):
    return PostgresIOManager()


class MyIOManager(IOManager):
    def handle_output(self, context, obj):
        table_name = context.config["table"]
        write_dataframe_to_table(name=table_name, dataframe=obj)

    def load_input(self, context):
        table_name = context.upstream_output.config["table"]
        return read_dataframe_from_table(name=table_name)


@io_manager(output_config_schema={"table": str})
def my_io_manager(_):
    return MyIOManager()
