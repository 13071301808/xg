from openai import OpenAI


class ChatModel:
    @staticmethod
    def chat(message):
        client = OpenAI(api_key="sk-0487cf8ff3154e1da6e33344051418f0", base_url="https://api.deepseek.com")
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "请始终以一个修仙世界的百事通角色回答问题"},
                {"role": "user", "content": f"{message}"}
            ],
            stream=False
        )
        response_text = response.choices[0].message.content
        return response_text


# message = "你好"
# chat_api = ChatModel()
# print(chat_api.chat(message))
