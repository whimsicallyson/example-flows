from prefect import task, Flow
from prefect.environments.storage import GitHub
from prefect.triggers import some_successful
import random

@task
def choose_array_length():
    return random.randint(1, 10)

@task
def create_array_of_randoms(length):
    array = []
    for x in range(0,length):
        array.append(random.randint(0,10))
    return array

@task
def check_even(num):
    if num % 2 != 0:
        raise ValueError("number is odd!")

@task(trigger=some_successful(at_least=0.5))
def mostly_ok():
    return True

with Flow("random list") as flow:
    n  = choose_array_length()
    arr = create_array_of_randoms(n)
    ok = mostly_ok(upstream_tasks=[check_even.map(arr)])
    
flow.set_reference_tasks([ok])

flow.storage = GitHub(
    repo="whimsicallyson/example-flows",
    path="flows/random-flow.py",
    secrets=["GITHUB_ACCESS_TOKEN"]
)