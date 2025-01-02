import sys
from src.database import Database
from config import DATABASE_URL, GEMINI_API_KEY
from src.model import LLM
from pathlib import Path

BASE_PATH = Path(__file__).resolve().parent.parent
sys.path.append(BASE_PATH)


conversation_log = []


def main():
    """
    Main function to run the Database Chatbot.

    - Initializes the database and LLM instances.
    - Interacts with the user in a conversational manner.
    - Handles user queries and provides responses based on the database content.
    - Logs the conversation and provides hints for follow-up queries.
    - Allows the user to exit the chatbot gracefully.

    Returns:
        None
    """
    global conversation_log

    # Initialize the database
    db_config = {"dsn": DATABASE_URL}
    db = Database(db_config)
    db.connect()

    # Initialize the LLM
    llm = LLM(api_key=GEMINI_API_KEY, database=db)

    print("Welcome to the Database Chatbot! Type 'exit' to quit.")

    while True:
        user_query = input("You: ")

        if user_query.lower() == "exit":
            print("Goodbye!")
            llm.clear_history_context()
            db.close()
            break

        response = llm.process_user_query(user_query)
        if llm.is_format_response:
            llm.format_response(response)
        else:
            print(f"AI: {response}")

        # Log the conversation
        conversation_log.append({"user_query": user_query, "bot_response": response})

        # Provide a hint for follow-up queries
        if "table" in llm.context:
            print()
            print(
                f"(Hint: Hey, I am currently working with the table: {llm.context['table']}). if this is not the table you want to work with, please let me know."
            )
        print()

    # Optionally, print the conversation log when exiting
    print("\nConversation Log:")
    for log in conversation_log:
        print(f"You: {log['user_query']}\nAI: {log['bot_response']}\n")


if __name__ == "__main__":
    main()
