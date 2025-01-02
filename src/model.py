import google.generativeai as genai
from typing import List, Union
from .database import Database


class LLM:
    def __init__(self, api_key: str, database: Database):
        """
        Initialize the LLM class with API key and database instance.

        Args:
            api_key (str): The API key for the generative AI model.
            database (Database): An instance of the Database class.
        """
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel("gemini-1.5-flash")
        self.database = database
        self.is_format_response = False
        self.context = {}
        self.history = []

    def genai_config(
        self, temperature: float = 0.7, max_output_tokens: int = 100, top_p: float = 0.9
    ) -> genai.types.GenerationConfig:
        """
        Generate the configuration for the generative model.

        Args:
            temperature (float, optional): Sampling temperature. Defaults to 0.7.
            max_output_tokens (int, optional): Maximum number of tokens to generate. Defaults to 250.
            top_p (float, optional): Nucleus sampling probability. Defaults to 0.9.

        Returns:
            genai.types.GenerationConfig: Configuration for the generative model.
        """
        # create a generative config
        return genai.types.GenerationConfig(
            temperature=temperature,
            max_output_tokens=max_output_tokens,
            top_p=top_p,
        )

    def process_user_query(self, user_query: str) -> str:
        """
        Main method to handle user query, clarify, and generate response.

        Args:
            user_query (str): The user's query.

        Returns:
            str: The response generated based on the user's query.
        """
        status = self.check_user_query(user_query)
        if status == "Table_Names":
            available_tables = self.get_available_tables()
            self.clear_history_context()
            return f"Available tables: {available_tables}"
        elif status == "Invalid":
            return "I'm sorry, I couldn't generate the response for your query. Please try later. thank you."
        elif status == "Valid":
            return self.handle_database_query(user_query)
        elif status == "Generic":
            response = self.model.generate_content(
                user_query, generation_config=self.genai_config()
            ).text.strip()
            return response

    def handle_database_query(self, user_query: str) -> str:
        """
        Processes a user's natural language query to interact with a database, extracting table, columns,
        and conditions to generate and execute a SQL query.

        Steps:
        1. Check if a table is mentioned in the user's query. If a new table is identified:
            - Reset the query context and history.
            - Set the identified table as the context.
            - If no table is found, ask the user for clarification.
        2. Identify the columns to query. If not already in context:
            - Match columns in the query to available columns for the table.
            - If no match is found, ask the user for clarification.
        3. Extract query conditions. If not already in context:
            - Parse conditions from the query.
            - If conditions are missing or unclear, ask the user for clarification.
        4. Generate a SQL query based on the extracted table, columns, and conditions.
            - If SQL query generation fails, reset the context and history, and notify the user.
            - Validate the generated SQL query. If invalid, reset the context and history, and notify the user.
        5. Execute the SQL query:
            - If execution succeeds, return the results.
            - If execution fails or no results are found, reset the context and history, and notify the user.

        Args:
            user_query (str): The natural language query input by the user.

        Returns:
            str: The results of the SQL query execution or a clarification prompt.
        """

        # Add user query to history to keep track of the conversation
        self.history.append({"type": "user", "value": user_query})

        # Step 1: Determine if a new table is mentioned
        available_tables = self.get_available_tables()
        table_name = self.match_table_name(user_query, available_tables)

        if table_name:
            if self.context.get("table") != table_name:
                # reset the history if the table found
                self.clear_history_context()
                # Reset columns and conditions if the table changes
                self.context["table"] = table_name
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

        # Step 3: Handle conditions
        if "conditions" not in self.context:
            conditions = self.extract_conditions(user_query)
            if conditions is None:
                # Ask for clarification and update the history
                return self.ask_for_clarification("conditions")

            self.context["conditions"] = conditions

        # Generate SQL query
        sql_query = self.generate_sql_query()
        if sql_query is None:
            # reset the context and history if the sql query is not valid
            self.clear_history_context()
            return "The SQL query could not be generated. Please start your query with more information."

        # Validate the SQL query
        if not self.is_valid_sql(sql_query):
            # reset the context and history if the sql query is not valid
            self.clear_history_context()
            return "The generated SQL query is not valid. Please provide more information to generate a valid SQL query."

        # Execute SQL query
        results = self.execute_sql_query(sql_query)
        if results is None:
            # reset the context and history if the sql query is not valid
            self.clear_history_context()
            return "No results found."

        # set the flag to format the response
        self.is_format_response = True
        return results

    def clear_history_context(self):
        """
        Clears the current context and conversation history.

        This function resets the state by clearing the `context` dictionary, which holds
        details about the ongoing query (e.g., table, columns, conditions), and the `history`
        list, which tracks the conversation flow.
        """
        self.context.clear()
        self.history.clear()

    def check_user_query(self, user_query: str) -> str:
        """
        Analyzes a user's query to determine its type and relevance to database operations.

        The query is categorized into one of the following:
        - 'Table_Names': If the query asks for a list of database tables.
        - 'Valid': If the query pertains to managing or interacting with database entries.
        - 'Generic': If the query is unrelated to the database.

        This function uses a content generation model to process the query and classify it accordingly.

        Args:
            user_query (str): The user's natural language query.

        Returns:
            str:
                - 'Table_Names' for queries requesting database table names.
                - 'Valid' for database-related queries (e.g., retrieving, updating, or managing data).
                - 'Generic' for general questions unrelated to the database.
                - 'Invalid' in case of an error during processing.
        """
        try:
            response = self.model.generate_content(
                f"""
                context: You are working with a database. Your thinking process should be as follows:
                Analyze the user query: '{user_query}'.
                Determine the type of the query:
                - If the query is specifically asking for the list of tables in the database, return 'Table_Names'.
                - If the query is related to database entries (e.g., retrieving, updating, or managing data), return 'Valid'.
                - If the query is a generic or general question (not related to the database), return 'Generic'.
                Do not include any extra explanation or text beyond the requested output.
                """,
                generation_config=self.genai_config(),
            )

            result = response.text.strip()

            # Return "Valid" or "Table_Names" for database-related queries, otherwise return the generic response
            if result in ["Valid", "Table_Names"]:
                return result
            else:
                return result  # General response for generic questions
        except Exception:
            return "Invalid"

    def get_available_tables(self) -> List:
        """
        Retrieves the list of table names from the database.

        This function queries the `information_schema` to fetch the names of all tables
        within the `public` schema of the database.

        Returns:
            List: A list of table names available in the database. Returns an empty list if
                an error occurs or no tables are found.
        """

        try:
            result = self.database.execute_query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
            )
            return [table for row in result for table in row]
        except Exception:
            return []

    def match_table_name(self, user_query: str, available_tables: List) -> str:
        """
        Matches a table name from the user's query against a list of available database tables.

        This function considers the user's current query and the conversation history to determine
        the table being referenced. If a match is found, it returns the table name; otherwise, it
        returns `None`.

        Args:
            user_query (str): The user's natural language query.
            available_tables (List): A list of table names available in the database.

        Returns:
            str:
                - The matched table name if a match is found.
                - `None` if no table matches the user's query.
        """
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
            Do not include any extra explanation or additional text.""",
            generation_config=self.genai_config(),
        )

        return response.text.strip() if response.text.strip() != "None" else None

    def get_columns_for_table(self, table_name: str) -> List:
        """
        Retrieves the column names for a specified database table.

        This function queries the `information_schema` to fetch column names for the given table
        within the `public` schema.

        Args:
            table_name (str): The name of the table for which to retrieve column names.

        Returns:
            List: A list of column names for the specified table. Returns an empty list if
                an error occurs or no columns are found.
        """
        try:
            result = self.database.execute_query(
                f"""SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name   = '{table_name}';
                """
            )
            return [column for row in result for column in row]
        except Exception:
            return []

    def match_columns(self, user_query: str, available_columns: List) -> List:
        """
        Matches columns in the user's query against a list of available columns in the database.

        The function uses the conversation history and the current query to infer the user's intent
        and determine the columns they are referencing. If no specific columns are identified, it
        defaults to all available columns.

        Args:
            user_query (str): The user's natural language query.
            available_columns (List): A list of column names available for the table.

        Returns:
            List: A list of matched column names, or all available columns if no match is found.
        """
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
            Do not include any extra explanation or text.""",
            generation_config=self.genai_config(),
        )

        return response.text.strip().split(",")

    def extract_conditions(self, query: str) -> str:
        """
        Extracts conditions (e.g., WHERE clause) from the user's query for SQL query generation.

        The function analyzes the query and uses the table and columns context to identify relevant
        conditions. If no conditions are found, it defaults to `1=1`.

        Args:
            query (str): The user's natural language query.

        Returns:
            str: A valid SQL WHERE clause or '1=1' if no conditions are specified.
        """

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
            Provide only the SQL condition or '1=1' with no additional explanation or text.""",
            generation_config=self.genai_config(),
        )

        return response.text.strip()

    def ask_for_clarification(
        self,
        field: str,
        available_data: List = None,
    ) -> str:
        """
        Requests clarification from the user for ambiguous or incomplete parts of the query.

        Args:
            field (str): The query field requiring clarification ('table', 'columns', or 'conditions').
            available_data (Optional): Additional information to guide the user's clarification.

        Returns:
            str: A clarification prompt for the user.
        """

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

    def generate_sql_query(self) -> Union[str, None]:
        """
        Generates an SQL query based on the current context, including table, columns, and conditions.

        The query is validated for syntax correctness, and if invalid, attempts to correct or returns `None`.

        Returns:
            str: A valid SQL query or `None` if the query could not be generated.
        """

        # Assuming you generate the SQL based on context
        table = self.context["table"]
        columns = self.context.get("columns", ["*"])  # "*" if columns not specified
        conditions = self.context.get("conditions", "1=1")  # Default condition

        # Construct the basic SQL query
        columns_str = ", ".join(columns) if isinstance(columns, list) else columns
        sql_query = f"SELECT {columns_str} FROM {table} WHERE {conditions};"

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
            Provide only the valid query or 'None' without any additional explanation.""",
            generation_config=self.genai_config(),
        )

        return response.text.strip() if response.text.strip() != "None" else None

    def is_valid_sql(self, sql_query: str) -> bool:
        """
        Validates the basic structure of an SQL query.

        Args:
            sql_query (str): The SQL query to validate.

        Returns:
            bool: `True` if the query contains 'SELECT' and 'FROM'; otherwise, `False`.
        """
        return "SELECT" in sql_query.upper() and "FROM" in sql_query.upper()

    def execute_sql_query(self, sql_query: str) -> Union[str, None]:
        """
        Executes an SQL query on the database.

        Args:
            sql_query (str): The SQL query to execute.

        Returns:
            str or None: Query results, number of rows affected, or `None` if execution fails.
        """
        try:
            results = self.database.execute_query(sql_query)
            if results is None:
                return None
            elif isinstance(results, list):
                return results
            else:
                return f"Query executed successfully, {results} rows affected."
        except Exception:
            return None

    def format_response(self, results: str) -> str:
        """
        Formats the query results for user-friendly display.

        Args:
            results (str or list): The results of the query execution.

        Returns:
            str: A formatted response or the original error/message.
        """
        get_schema = self.get_columns_for_table(self.context["table"])
        response = self.model.generate_content(
            f"""
            Based on the response data, dynamically fetch the relevant schema and use it to create a clear and user-friendly explanation. 
            - Include only the fields from the schema that are present in the response, seamlessly integrating them into a natural explanation.
            - If the response contains a list of results, present the information with clear descriptions and proper structure without using "|" symbols for tables.
            - If the response is a single value, provide a concise and natural sentence conveying the information.
            - Avoid explicitly mentioning any schema fields that are not included in the response.
            - If the response is an error or message, present it as is without additional formatting.
            schema: {get_schema if get_schema else "No schema found."}

            Response:
            {results if results else "Not able to generate the response for your query. Please provide more information."}
            """,
            generation_config=self.genai_config(max_output_tokens=500),
            stream=True,
        )

        result = ""
        print("AI: ", end="")
        for chunk in response:
            result += chunk.text
            print(chunk.text, end="")
        return result
