from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_openai import ChatOpenAI
from sql_ai_agent2.db_handler import get_tbl_attr, query_execute

from langchain_core.prompts import (
  SystemMessagePromptTemplate,
  HumanMessagePromptTemplate,
  ChatPromptTemplate
)

def set_prompt_template_test():
    """
    Build a LangChain ChatPromptTemplate for SQL generation.

    Parameters
    ----------
    question : str
        Natural language question describing the SQL request.
    tbl_name : str
        Name of the SQL table.
    schema : str
        Table schema (column definitions).
    additional_context : str, optional
        Extra system context. Defaults to empty string.

    Returns
    -------
    ChatPromptTemplate
        A LangChain prompt template ready to be invoked.
    """

    system_template = """
Given the following SQL table, your job is to write queries given a user’s request.
Return just the SQL query as plain text, without additional text, and don't use markdown format.
I am quering the data against a {database} database, please make sure you are using the {database} SQL dialects. 
Please ensure that the field names in the query are enclosed in double quotes.
{additional_context}
CREATE TABLE {tbl_name} ({schema})
""".strip()

    user_template = "Write a SQL query that returns: {question}"

    messages = [
        ("system", system_template),
        ("user", user_template)
    ]

    prompt_template = ChatPromptTemplate.from_messages(messages)

    # prompt = prompt_template.invoke({"tbl_name": tbl_name, 
    #                                 "schema": schema, 
    #                                 "additional_context": additional_context, 
    #                                 "question": question })

    return prompt_template

from sql_ai_agent2.api_handler import SqlAgent2
import ibis

base_url="http://model-runner.docker.internal/engines/v1"
api_key="docker"
temperature=0
model = "ai/llama3.2:latest"
# model = "ai/gemma3n"
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

schema =  get_tbl_attr(con = con_ibis, tbl_name = tbl_name)
question = "How many rows are in the dataset?"
prompt_template = set_prompt_template_test()
prompt = prompt_template.invoke({"tbl_name": tbl_name, 
                 "schema": schema.schema,
                 "question":question,
                 "additional_context": "",
                 "database": schema.db_type})


llm = ChatOpenAI(
            base_url = base_url,
            api_key = api_key,
            temperature = temperature,
            model = model
        )

chain = prompt_template | llm


q = chain.invoke({"tbl_name": tbl_name, 
                 "schema": schema.schema,
                 "question":question,
                 "additional_context": "",
                 "database": schema.db_type})


chat_history = []

