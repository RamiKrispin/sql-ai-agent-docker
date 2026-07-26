from sql_ai_agent2.api_handler import SqlAgent2
import ibis

base_url="http://model-runner.docker.internal/engines/v1"
api_key="docker"
temperature=0
model = "ai/llama3.2:latest"
# model = "ai/devstral-small:24B"
model = "ai/granite-4.0-h-micro"
# model = "ai/gemma3n"
fallback_model = "ai/gemma3n"
fallback_model = "ai/devstral-small:24B"
tbl_name = "air_traffic"
max_token = 10000

con_ibis = ibis.postgres.connect(
    user="postgres",
    password="password",
    host="postgres",
    port=5432,
    database="my_db",
)

con_ibis.get_schema("air_traffic")

# air_traffic = con.sql("SELECT * FROM air_traffic LIMIT 10").execute()

# print(air_traffic.head())


import duckdb
import duckdb as db
import pandas as pd
import ibis
from dataclasses import dataclass
import sys
sys.path.append("../")


from sql_ai_agent2.db_handler import get_tbl_attr
from sql_ai_agent2.api_handler import SqlAgent2


air_traffic = con_ibis.sql("SELECT * FROM air_traffic LIMIT 100").execute()

air_traffic.head()

schema_ibis =  get_tbl_attr(con = con_ibis, tbl_name = tbl_name)
print("#" * 60)
print("Schema")
print(schema_ibis.schema)
print("#" * 60)
print("Schema Table")
print(schema_ibis.table) 

print("#" * 60)
print("Setting the prompt")
# prompt = set_prompt(tbl_name = tbl_name, 
#                     schema = schema_ibis.schema, 
#                     additional_context = "", 
#                     question =  "test" )

# print(prompt.messages[0].content)

agent = SqlAgent2(api_key= api_key, 
                  base_url=base_url,
                  model = model,
                  con = con_ibis,
                  fallback= True,
                  fallback_model= fallback_model,
                  tbl_name = tbl_name)

question = "How many rows are in the dataset?"



agent.ask_question(question = question, verbose=False)


agent.ask_question(question = "What are the unique values of the Activity Type Code field?")

# agent.ask_question(question = "How many landing where in 2020?")




con_db = ibis.duckdb.connect()
con_db.create_table(tbl_name, air_traffic)


agent_db = SqlAgent2(api_key= api_key, 
                  base_url=base_url,
                  model = model,
                  con = con_db,
                  fallback= True,
                  fallback_model= fallback_model,
                  tbl_name = tbl_name)

additional_context = "I am using a DuckDB database. Please ensure that the field names in the query are enclosed in double quotes."
question = "What are the unique values of the Activity Type Code field?"
agent_db.ask_question(question = question, additional_context= additional_context)
agent.ask_question(question = question, additional_context= additional_context)


additional_context = ""
question = "What are the unique values of the Activity Type Code field?"
agent_db.ask_question(question = question, additional_context= additional_context, trial= 3)


# additional_context = ""
# question ="What are the unique values of the Activity Type Code field?"
# agent_db.ask_question(question = question, additional_context= additional_context)
# agent.ask_question(question = question, additional_context= additional_context)


