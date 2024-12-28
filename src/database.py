from tenacity import retry, stop_after_attempt, wait_fixed


class Database:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = self.connect()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def connect(self):
        import psycopg2

        try:
            self.connection = psycopg2.connect(**self.db_config)
            print("Database connection established.")
        except Exception as e:
            raise Exception(f"Error connecting to database: {e}")

    def is_connection_alive(self):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            print(f"Database connection is not alive: {e}")
            print("retrying to connect to database")
            try:
                import psycopg2

                self.connection = psycopg2.connect(**self.db_config)
                print("Database connection established.")
                return True
            except Exception as e:
                print(f"Error connecting to database: {e}")
                return False

    def execute_query(self, query):
        if not self.is_connection_alive():
            print("Database connection is not established.")
            return None
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            if query.strip().upper().startswith("SELECT"):
                return self.fetch_results(cursor)
            return (
                cursor.rowcount
            )  # Return the number of affected rows for non-SELECT queries
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
