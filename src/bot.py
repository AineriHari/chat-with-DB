from utils import validate_input, handle_error


class ChatBot:
    def __init__(self, db, llm):
        self.db = db
        self.llm = llm

    def process_query(self, user_query):
        if not validate_input(user_query):
            return handle_error("Invalid input. Please enter a valid query.")

        sql_query, clarification_needed = self.llm.convert_to_sql(user_query)
        while clarification_needed:
            clarification_question = sql_query
            user_query = input(f"Bot: {clarification_question}\nYou: ")
            if not validate_input(user_query):
                return handle_error("Invalid input. Please enter a valid query.")
            sql_query, clarification_needed = self.llm.convert_to_sql(user_query)

        results = self.db.execute_query(sql_query)
        if results is None:
            return handle_error("Error executing query.")

        return self.format_response(results)
