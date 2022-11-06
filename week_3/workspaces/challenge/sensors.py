from workspaces.resources import Postgres


def db_new_rows(conn: Postgres, table_name: str, order_column: str, since_key: str = None):
    """Get S3 keys"""
    cursor = ""
    contents = []

    response = conn.execute_query(f"SELECT {order_column} FROM {table_name} ORDER BY {order_column} DESC LIMIT 1")

    if response and response[0] > since_key:
        return response[0]

    return []
