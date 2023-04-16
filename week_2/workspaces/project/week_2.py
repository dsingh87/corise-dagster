from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(required_resource_keys={"s3"},
    config_schema={"s3_key": String})
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    stocks = context.resources.s3.get_data(context.op_config["s3_key"])
    return [Stock.from_list(i) for i in stocks]


@op
def process_data(context, stock_list: List[Stock]) -> Aggregation:
    stock_dict = {}
    for stock in stock_list:
        stock_dict[stock.date] = stock.high
    return(Aggregation(date = max(stock_dict, key=stock_dict.get), high = max(stock_dict.values())))


@op(required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation)},)
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))


@op(required_resource_keys={'s3'},
    ins={"aggregation": In(dagster_type=Aggregation)})
def put_s3_data(context, aggregation):
    context.resources.s3.put_data(str(aggregation.date), aggregation.high)


@graph
def machine_learning_graph():
    aggregation = process_data(get_s3_data())
    put_redis_data(aggregation)
    put_s3_data(aggregation)    


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config = local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config = docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
