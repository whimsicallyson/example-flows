import requests
import json

from prefect import task, Flow
from prefect.client import Secret
from prefect.environments.storage import GitHub

@task
def get_yesterdays_metrics():
  auth_token = Secret("airtable_token")
  data = requests.get("https://api.airtable.com/v0/appYXvuNEb0jN7LyX/Metrics?sort%5B0%5D%5Bfield%5D=Name&sort%5B0%5D%5Bdirection%5D=desc", headers=auth_token).json()

  # Airtable reference
  # 0: ID recenhEgeiQY83FrL, Name ui_stars
  # 1: ID recWlYBY5Bx0RNBr1, Name server_stars
  # 2: ID recrhgFxXbHn7BNDv, Name prefect_slack
  # 3: ID rechq8AKCsgu5dj1b, Name dagster_slack
  # 4: ID recnOekrsOmEr2n0g, Name core_stars

  return data

@task 
def update_metrics(todays_metrics):
  data = json.dumps({"records": todays_metrics})
  auth_token = Secret("airtable_token")
  response = requests.patch("https://api.airtable.com/v0/appYXvuNEb0jN7LyX/Metrics", headers=auth_token, data=data)
  return response

@task
def get_todays_slack_stats(instance_name):
  data = requests.get(instance_name).json()
  return data["total"]

@task
def get_todays_star_count(repo):
    data = requests.get(repo).json()
    return data["stargazers_count"]

@task
def print_response(prefect_slack, dagster_slack, core_stars, server_stars, ui_stars, yesterdays_metrics):
  print(f"""
  Today, the Prefect slack has {prefect_slack} users, an increase of {prefect_slack - yesterdays_metrics["records"][2]["fields"]["Count"]} from the previous business day. Dagster has {dagster_slack} in their Slack, an increase of {dagster_slack - yesterdays_metrics["records"][3]["fields"]["Count"]}. 
  The core repo has {core_stars} stars ({core_stars - yesterdays_metrics["records"][4]["fields"]["Count"]} increase), Server has {server_stars} ({server_stars - yesterdays_metrics["records"][1]["fields"]["Count"]} increase), and UI has {ui_stars} ({ui_stars - yesterdays_metrics["records"][0]["fields"]["Count"]} increase). 
  
  Have a great day!
  """)


with Flow("Metrics Reporting Flow") as flow:
  yesterdays_metrics = get_yesterdays_metrics()
  prefect_slack = get_todays_slack_stats('https://prefect-slackin.herokuapp.com/data')
  dagster_slack = get_todays_slack_stats('https://dagster-slackin.herokuapp.com/data')
  core_stars = get_todays_star_count('https://api.github.com/repos/prefecthq/prefect')
  server_stars = get_todays_star_count('https://api.github.com/repos/prefecthq/server')
  ui_stars = get_todays_star_count('https://api.github.com/repos/prefecthq/ui')
  todays_metrics = [{"id": "recWlYBY5Bx0RNBr1", "fields": {"Count": server_stars}}, {"id": 'recenhEgeiQY83FrL', "fields": {"Count": ui_stars}}, {"id": 'rechq8AKCsgu5dj1b', "fields": {"Count": dagster_slack}}, {"id": 'recnOekrsOmEr2n0g', "fields": {"Count": core_stars}}, {"id": 'recrhgFxXbHn7BNDv', "fields": {"Count": prefect_slack}}]
  update_metrics(todays_metrics)
  print_response(prefect_slack, dagster_slack, core_stars, server_stars, ui_stars, yesterdays_metrics)

flow.storage = GitHub(
    repo="whimsicallyson/example-flows",
    path="flows/marketing_daily_metrics.py",
    secrets=["ALLYSON_GITHUB_ACCESS_TOKEN"]
)