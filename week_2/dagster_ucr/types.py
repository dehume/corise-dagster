from dagster import usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: str
    close: float
    volume: int
    open: float
    high: float
    low: float


@usable_as_dagster_type(description="Aggregation")
class Aggregation(BaseModel):
    day: str
    high: float
