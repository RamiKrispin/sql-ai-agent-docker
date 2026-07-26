import openai

base_url = "http://model-runner.docker.internal/engines/v1"
api_key = "docker"
model = "ai/llama3.2:latest"

client = openai.OpenAI(
    base_url=base_url,
    api_key=api_key
)

# Conversation memory
conversation = []

# ---- Token estimation ----
def estimate_tokens(text: str) -> int:
    # Rough but effective approximation
    return max(1, len(text) // 4)

def count_message_tokens(messages):
    return sum(estimate_tokens(m["content"]) for m in messages)

def build_messages(question: str):
    return (
        [{"role": "system", "content": "You are a helpful AI assistant."}]
        + conversation
        + [{"role": "user", "content": question}]
    )

while True:
    question = input("Question: ")
    if question == "quit":
        break

    messages = build_messages(question)
    input_tokens = count_message_tokens(messages)

    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0
    )

    answer = completion.choices[0].message.content
    output_tokens = estimate_tokens(answer)

    print(f"\nAI: {answer}")
    print(f"🧮 Tokens — input: {input_tokens}, output: {output_tokens}, total: {input_tokens + output_tokens}\n")

    # Update memory
    conversation.append({"role": "user", "content": question})
    conversation.append({"role": "assistant", "content": answer})
