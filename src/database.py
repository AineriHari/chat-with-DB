import psycopg2
from typing import Any, Dict, List, Optional, Union
from tenacity import retry, stop_after_attempt, wait_fixed


class Database:
    """
    A class to handle database connections and operations.

    This class provides methods to establish a connection to a PostgreSQL database,
    execute queries, fetch results, and manage the connection lifecycle.
    """

    def __init__(self, db_config):
        """
        Initialize the Database instance.

        Args:
            db_config (Dict[str, Union[str, int]]): A dictionary containing the database configuration.
                Example keys include `dbname`, `user`, `password`, `host`, and `port`.
        """
        self.db_config = db_config
        self.connection = None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def connect(self) -> psycopg2.extensions.connection:
        """
        Establish a connection to the database with retry logic.

        Retries the connection up to 3 times, waiting 2 seconds between attempts.

        Returns:
            psycopg2.extensions.connection: The established database connection.

        Raises:
            Exception: If the connection cannot be established after retries.
        """
        try:
            self.connection = psycopg2.connect(**self.db_config)
            print("Database connection established.")
            return self.connection
        except Exception as e:
            raise Exception(f"Error connecting to database: {e}")

    def is_connection_alive(self) -> bool:
        """
        Check if the database connection is alive.

        If the connection is not alive, it attempts to reconnect.

        Returns:
            bool: True if the connection is alive, False otherwise.
        """
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

    def execute_query(self, query: str) -> Optional[Union[List[Any], int]]:
        """
        Execute a SQL query on the database.

        Args:
            query (str): The SQL query to execute.

        Returns:
            Optional[Union[List[Any], int]]:
                - A list of results for SELECT queries.
                - The number of affected rows for non-SELECT queries.
                - None if the query fails or the connection is not alive.
        """
        if not self.is_connection_alive():
            print("Database connection is not established.")
            return None
        try:
            with self.connection.cursor() as cursor:
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

    def fetch_results(self, cursor: psycopg2.extensions.cursor) -> Optional[List[Any]]:
        """
        Fetch results from the executed query.

        Args:
            cursor (psycopg2.extensions.cursor): The database cursor.

        Returns:
            Optional[List[Any]]: The results of the query, or None if an error occurs.
        """
        try:
            return cursor.fetchall()
        except Exception as e:
            print(f"Error fetching results: {e}")
            return None

    def close(self) -> None:
        """
        Close the database connection.

        This method safely closes the database connection if it is established.
        """
        if self.connection:
            self.connection.close()
            print("Database connection closed.")
