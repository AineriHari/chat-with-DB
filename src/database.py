class Database:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None

    def connect(self):
        import psycopg2

        try:
            self.connection = psycopg2.connect(**self.db_config)
            print("Database connection established.")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def execute_query(self, query):
        if self.connection is None:
            print("Database connection is not established.")
            return None
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            return cursor
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def fetch_results(self, cursor):
        try:
            return cursor.fetchall()
        except Exception as e:
            print(f"Error fetching results: {e}")
            return None

    def close(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed.")
