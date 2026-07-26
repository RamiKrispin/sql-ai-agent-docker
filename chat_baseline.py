import openai

base_url= "http://model-runner.docker.internal/engines/v1"
api_key = "docker"
model = "ai/llama3.2:latest"
client = openai.OpenAI(
  base_url = base_url,
  api_key = api_key
)

def prompt_template(question: str):
    return [
        {"role": "system", "content": "You are a helpful AI assistant."},
        {"role": "user", "content": question},
    ]


while True:
    question = input("Question: ")
    if question == 'quit':
      break

    completion = client.chat.completions.create(
      model= model, 
      messages = prompt_template(question),
      temperature=0
    )
    print(f"AI: {completion.choices[0].message.content} ")

