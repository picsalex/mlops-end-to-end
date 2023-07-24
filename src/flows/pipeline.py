import os

from prefect import flow, task

os.environ["PREFECT_API_URL"] = "http://prefect-api:4200/api"


@task
def say_hello(name):
    print(f"hello 2 {name}")


@task
def say_goodbye(name):
    print(f"goodbye 2 {name}")


@flow(name="test flow")
def greetings(names):
    for name in names:
        say_hello(name)
        say_goodbye(name)


if __name__ == "__main__":
    greetings(["arthur", "trillian", "ford", "marvin"])
