import sys
from src.database import Database
from config import DATABASE_URL, GEMINI_API_KEY
from src.model import LLM
from pathlib import Path

BASE_PATH = Path(__file__).resolve().parent
sys.path.append(BASE_PATH)


conversation_log = []


def main():
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
            db.close()
            break

        response = llm.process_user_query(user_query)

        # Log the conversation
        conversation_log.append({"user_query": user_query, "bot_response": response})

        # Print the formatted response
        print("Bot:", response)

        # Provide a hint for follow-up queries
        if "table" in llm.context:
            print(
                f"(Hint: You are currently working with the table: {llm.context['table']})"
            )
        print()

    # Optionally, print the conversation log when exiting
    print("\nConversation Log:")
    for log in conversation_log:
        print(f"You: {log['user_query']}\nBot: {log['bot_response']}\n")


if __name__ == "__main__":
    main()
