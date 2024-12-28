import google.generativeai as genai


class LLM:
    def __init__(self, api_key, database_connection):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel("gemini-1.5-flash")
        self.database_connection = database_connection
        self.context = {}  # To maintain conversation history

    def process_user_query(self, user_query):
        """Main method to handle user query, clarify, and generate response."""
        # Step 1: Determine if a new table is mentioned
        available_tables = self.get_available_tables()
        table_name = self.match_table_name(user_query, available_tables)

        if table_name:
            if self.context.get("table") != table_name:
                # Reset columns and conditions if the table changes
                self.context = {"table": table_name}
            else:
                self.context["table"] = table_name
        elif not self.context.get("table"):
            return self.ask_for_clarification("table", available_tables)

        # Step 2: Handle columns (optional)
        if "columns" not in self.context:
            available_columns = self.get_columns_for_table(self.context["table"])
            columns = self.match_columns(user_query, available_columns)
            if columns is None:
                return self.ask_for_clarification(
                    "columns", available_columns, optional=True
                )
            self.context["columns"] = columns

        # Step 3: Handle conditions (optional)
        if "conditions" not in self.context:
            conditions = self.extract_conditions(user_query)
            if conditions is None:
                return self.ask_for_clarification("conditions", optional=True)
            self.context["conditions"] = conditions

        # Generate SQL query
        sql_query = self.generate_sql_query()
        if not self.is_valid_sql(sql_query):
            return "The SQL query could not be generated. Please clarify further."

        # Execute SQL query
        results = self.execute_sql_query(sql_query)
        if results is None:
            return "No results found."

        # Format and return results
        response = self.format_response(results)
        self.clear_context_partially()  # Retain table context for subsequent queries
        return response

    def get_available_tables(self):
        """Retrieve the list of table names from the database."""
        try:
            cursor = self.database_connection.cursor()
            cursor.execute("SHOW TABLES;")
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            return []

    def match_table_name(self, user_query, available_tables):
        """Match table name in the user query with available tables."""
        response = self.model.generate_content(
            f"Match the user query to one of the following table names: {available_tables}. Query: {user_query}"
        )
        return response.text.strip()

    def get_columns_for_table(self, table_name):
        """Retrieve the column names for the specified table."""
        try:
            cursor = self.database_connection.cursor()
            cursor.execute(f"DESCRIBE {table_name};")
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            return []

    def match_columns(self, user_query, available_columns):
        """Match columns in the user query with the available columns."""
        response = self.model.generate_content(
            f"Match the user query to one or more of the following columns (if applicable): {available_columns}. "
            f"Query: {user_query}"
        )
        if response.text.strip().lower() == "all":
            return None  # Indicates generic data for the whole table
        return response.text.strip().split(",") if response.text.strip() else None

    def extract_conditions(self, query):
        """Extract conditions (e.g., WHERE clause) from user query."""
        response = self.model.generate_content(
            f"Extract conditions for the WHERE clause from the following query (if applicable): {query}"
        )
        return response.text.strip() if response.text.strip() else None

    def ask_for_clarification(self, missing_part, options=None, optional=False):
        """Ask for clarification based on missing information."""
        if missing_part == "table":
            return f"I couldn't identify the table from your query. Available tables: {options}. Which one should I use?"
        elif missing_part == "columns":
            if optional:
                return "Do you want data for specific columns, or the entire table?"
            return f"I couldn't identify the columns you are referring to. Available columns: {options}. Please specify."
        elif missing_part == "conditions":
            if optional:
                return "What conditions should apply to the query, if any?"
            return (
                "I couldn't identify the conditions for your query. Could you clarify?"
            )

    def generate_sql_query(self):
        """Generate SQL query based on the current context."""
        table = self.context.get("table")
        columns = ", ".join(
            self.context.get("columns", ["*"])
        )  # Use * if columns are None
        conditions = self.context.get(
            "conditions", "1=1"
        )  # Default condition if none provided
        return f"SELECT {columns} FROM {table} WHERE {conditions};"

    def is_valid_sql(self, sql_query):
        """Basic SQL validation."""
        return "SELECT" in sql_query.upper() and "FROM" in sql_query.upper()

    def execute_sql_query(self, sql_query):
        """Execute the SQL query on the database."""
        try:
            cursor = self.database_connection.cursor()
            cursor.execute(sql_query)
            return cursor.fetchall()
        except Exception as e:
            return None

    def format_response(self, results):
        """Format database results into human-readable form."""
        if not results:
            return "No results found."
        return "\n".join([", ".join(map(str, row)) for row in results])

    def clear_context_partially(self):
        """Partially clear the context to retain table information."""
        if self.context.get("table"):
            self.context = {
                "table": self.context["table"]
            }  # Retain only the table context
        else:
            self.clear_context()

    def clear_context(self):
        """Clear the entire context."""
        self.context = {}
