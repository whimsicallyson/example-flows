from prefect import task, Flow, Parameter


@task(log_stdout=True)
def say_hello(name):
    print("Hello, {}!".format(name))


with Flow("My First Flow") as flow:
    name = Parameter('name')
    say_hello(name)


# flow.run(name='world') # "Hello, world!" 
# flow.run(name='Marvin') # "Hello, Marvin!" 
flow.register(project_name="first-flow-test")