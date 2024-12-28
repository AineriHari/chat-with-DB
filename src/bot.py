from .database import Database
from .model import LLM
from typing import Dict


class ChatBot:
    def __init__(self, database_config: Dict, api_key: str):
        self.database = Database(database_config)
        self.llm = LLM(api_key, self.database)

    def process_query(self, user_query: str):
        if not user_query.strip():
            return f"Error: Invalid input. Please enter a valid query."

        # process the query
        return self.llm.process_user_query(user_query)
