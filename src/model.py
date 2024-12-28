import google.generativeai as genai


class LLM:
    def __init__(self, api_key, database_connection):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel("gemini-1.5-flash")
        self.database_connection = database_connection
        self.context = {}
        self.history = []

    def process_user_query(self, user_query):
        """Main method to handle user query, clarify, and generate response."""
        # Add user query to history to keep track of the conversation
        self.history.append({"type": "user", "value": user_query})

        # Step 1: Determine if a new table is mentioned
        available_tables = self.get_available_tables()
        table_name = self.match_table_name(user_query, available_tables)

        if table_name:
            if self.context.get("table") != table_name:
                # Reset columns and conditions if the table changes
                self.context = {"table": table_name}
                # reset the history if the table found
                self.history.clear()
            else:
                self.context["table"] = table_name
                # reset the history if the table found
                self.history.clear()
        else:
            # Ask for clarification and pass the history to maintain the conversation flow
            return self.ask_for_clarification("table", available_tables)

        # Step 2: Handle columns (optional)
        if "columns" not in self.context:
            available_columns = self.get_columns_for_table(self.context["table"])
            columns = self.match_columns(user_query, available_columns)
            if columns is None:
                # Ask for clarification and update the history
                return self.ask_for_clarification("columns", available_columns)

            self.context["columns"] = columns
            # reset the history if the table found
            self.history.clear()

        # Step 3: Handle conditions
        if "conditions" not in self.context:
            conditions = self.extract_conditions(user_query)
            if conditions is None:
                # Ask for clarification and update the history
                return self.ask_for_clarification("conditions")

            self.context["conditions"] = conditions
            # reset the history if the table found
            self.history.clear()

        print("Context:", self.context)
        # Generate SQL query
        sql_query = self.generate_sql_query()
        if sql_query is None:
            # reset the context and history if the sql query is not valid
            self.context.clear()
            self.history.clear()
            return "The SQL query could not be generated. Please start your query with more information."

        # Validate the SQL query
        if not self.is_valid_sql(sql_query):
            # reset the context and history if the sql query is not valid
            self.context.clear()
            self.history.clear()
            return "The generated SQL query is not valid. Please provide more information to generate a valid SQL query."

        # Execute SQL query
        results = self.execute_sql_query(sql_query)
        if results is None:
            return "No results found."

        # Format and return results
        response = self.format_response(results)

        # Clear history after response is given
        self.context.clear()
        self.history.clear()  # Clear the conversation history after the response

        return response

    def get_available_tables(self):
        """Retrieve the list of table names from the database."""
        try:
            result = self.database_connection.execute_query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
            )
            return [table for row in result for table in row]
        except Exception:
            return []

    def match_table_name(self, user_query, available_tables):
        """Match table name in the user query with available tables."""
        # Join the conversation history with the user query to provide full context
        history_text = "\n".join(
            [
                f"{entry['type'].capitalize()}: {entry['value']}"
                for entry in self.history
            ]
        )

        response = self.model.generate_content(
            f"""Conversation history:\n{history_text}\n
            Analyze the conversation history (if available) and incorporate it into your understanding. Then analyze the current user query: '{user_query}'.
            Determine which table the user is referring to from the available tables: {available_tables}. 
            - If the query matches one of the available tables, return only the table name.
            - If the query does not match any available table, return the string 'None'. 
            Do not include any extra explanation or additional text."""
        )

        return response.text.strip() if response.text.strip() != "None" else None

    def get_columns_for_table(self, table_name):
        """Retrieve the column names for the specified table."""
        try:
            result = self.database_connection.execute_query(
                f"""SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name   = '{table_name}';
                """
            )
            return [column for row in result for column in row]
        except Exception:
            return []

    def match_columns(self, user_query, available_columns):
        """Match columns in the user query with the available columns."""
        # Join the conversation history with the user query to provide full context
        history_text = "\n".join(
            [
                f"{entry['type'].capitalize()}: {entry['value']}"
                for entry in self.history
            ]
        )

        response = self.model.generate_content(
            f"""Conversation history:\n{history_text}\n
            If the conversation history is available, analyze it first to understand the user's intent. Then analyze the current user query: '{user_query}'. 
            Determine which columns the user is referring to from the available columns: {available_columns}. 
            - If the user wants data for all columns or if the columns cannot be determined from the query, return all 'column1, column2', etc. 
            - Otherwise, return a comma-separated list of columns like 'column1, column2', etc.
            Do not include any extra explanation or text."""
        )

        return response.text.strip().split(",")

    def extract_conditions(self, query):
        """Extract conditions (e.g., WHERE clause) from user query."""
        # Join the conversation history with the user query to provide full context
        history_text = "\n".join(
            [
                f"{entry['type'].capitalize()}: {entry['value']}"
                for entry in self.history
            ]
        )

        response = self.model.generate_content(
            f"""Conversation history:\n{history_text}\n
            If the conversation history is available, analyze it first to understand the user's intent. Then analyze the current user query: '{query}', 
            where the table name is {self.context['table']} and the columns are {self.context['columns']}. 
            Based on this data, identify the WHERE conditions the user is referring to. 
            - If there are conditions, return them as a valid SQL WHERE clause, formatted like 'column_name = value' or other valid conditions (e.g., 'column_name > value').
            - If no conditions are specified or applicable, return '1=1'. 
            Provide only the SQL condition or '1=1' with no additional explanation or text."""
        )

        return response.text.strip()

    def ask_for_clarification(
        self,
        field,
        available_data=None,
    ):
        """Ask for clarification from the user and update history, passing history to the LLM."""

        if field == "table":
            clarification = f"I couldn't understand the table you are referring to. Here are the available tables: {available_data}. Could you please specify the table?"
            self.history.append(
                {"type": "clarification", "field": "table", "value": clarification}
            )
            return clarification
        elif field == "columns":
            clarification = f"I couldn't identify the columns you are referring to. Available columns: {available_data}. Please specify."
            self.history.append(
                {"type": "clarification", "field": "columns", "value": clarification}
            )
            return clarification
        elif field == "conditions":
            clarification = (
                "I couldn't identify the conditions for your query. Could you clarify?"
            )
            self.history.append(
                {"type": "clarification", "field": "conditions", "value": clarification}
            )
            return clarification

    def generate_sql_query(self):
        # Assuming you generate the SQL based on context
        table = self.context["table"]
        columns = self.context.get("columns", ["*"])  # "*" if columns not specified
        conditions = self.context.get("conditions", "1=1")  # Default condition

        # Construct the basic SQL query
        columns_str = ", ".join(columns) if isinstance(columns, list) else columns
        sql_query = f"SELECT {columns_str} FROM {table} WHERE {conditions};"

        print("Generated SQL Query:", sql_query)

        # check the sql is valid or not
        response = self.model.generate_content(
            f"""Generated a SQL query based on the following context: 
            - Table name: {table}
            - Columns: {columns}
            - Conditions: {conditions}
            
            The generated SQL query is: {sql_query}. 
            Validate whether this SQL query adheres to standard SQL syntax. If the query is valid, return it as is. 
            If the query is not valid, generate a corrected SQL query using the above information with standard SQL syntax. 
            Ensure the column and table names are not enclosed in backticks (`); use standard SQL quoting or no quoting if unnecessary. 
            If a valid query cannot be generated, return 'None'. 
            Provide only the valid query or 'None' without any additional explanation."""
        )
        print("Generated SQL Query Response:", response.text.strip())
        return response.text.strip() if response.text.strip() != "None" else None

    def is_valid_sql(self, sql_query):
        """Basic SQL validation."""
        return "SELECT" in sql_query.upper() and "FROM" in sql_query.upper()

    def execute_sql_query(self, sql_query):
        """Execute the SQL query on the database."""
        try:
            results = self.database_connection.execute_query(sql_query)
            if results is None:
                return None
            elif isinstance(results, list):
                return self.format_response(results)
            else:
                return f"Query executed successfully, {results} rows affected."
        except Exception:
            return None

    def format_response(self, results):
        """Format the results for display."""
        if not results:
            return "No results found."

        # Use LLM to make the results human-readable
        response = self.model.generate_content(
            f"Format the following results in a human-readable format:\n{results}"
        )

        return response.text.strip()