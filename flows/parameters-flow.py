from prefect import task, Flow, Parameter
from prefect.environments.storage import GitHub
import time

@task
def sleep(time):
    time.sleep(time)

@task
def hello(name):
    print("hello there ", y)

with Flow(name="parameters flow") as flow:
    x = Parameter("time", default = 5)
    y = Parameter("name", default = "allyson")
    sleep(time=time)

flow.storage = GitHub(
    repo="whimsicallyson/example-flows",
    path="flows/parameters-flow.py",
    secrets=["ALLYSON_GITHUB_ACCESS_TOKEN"]
)