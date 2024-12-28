from database import Database
from model import LLM
from typing import Dict


class ChatBot:
    def __init__(self, database_config: Dict, api_key: str):
        self.database = Database(**database_config)
        self.llm = LLM(api_key, self.database)

    def process_query(self, user_query: str):
        if not user_query.strip():
            return f"Error: Invalid input. Please enter a valid query."

        # process the query
        response = self.llm.process_user_query(user_query)

        # Use LLM to make the results human-readable
        response = self.model.generate_content(
            f"Format the following results in a human-readable format:\n{response}",
            stream=True,
        )

        result = ""
        print("Bot: ")
        for chunk in response:
            result += chunk.text
            print(result)

        return result
