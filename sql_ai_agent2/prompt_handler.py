from langchain_core.prompts import ChatPromptTemplate

def set_prompt(question: str, tbl_name: str, schema: str, additional_context: str = ""):
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
{additional_context}
CREATE TABLE {tbl_name} ({schema})
""".strip()

    user_template = "Write a SQL query that returns: {question}"

    messages = [
        ("system", system_template),
        ("user", user_template)
    ]

    prompt_template = ChatPromptTemplate.from_messages(messages)

    prompt = prompt_template.invoke({"tbl_name": tbl_name, "schema": schema, "additional_context": additional_context, "question": question })

    return prompt
