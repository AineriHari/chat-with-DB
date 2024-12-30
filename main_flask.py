import sys
from src.database import Database
from config import DATABASE_URL, GEMINI_API_KEY
from src.model import LLM
from pathlib import Path
from flask import Flask, request, jsonify

BASE_PATH = Path(__file__).resolve().parent
sys.path.append(BASE_PATH)


conversation_log = []

app = Flask(__name__)

# Initialize the database and LLM
db_config = {"dsn": DATABASE_URL}
db = Database(db_config)
db.connect()
llm = LLM(api_key=GEMINI_API_KEY, database=db)


@app.route("/chat", methods=["GET", "POST"])
def chat():
    global conversation_log

    if request.method == "POST":
        user_query = request.json.get("query")

        if not user_query:
            return jsonify({"error": "No query provided"}), 400

        if user_query.lower() == "exit":
            llm.clear_history_context()
            db.close()
            return jsonify({"response": "Goodbye!"})

        response = llm.process_user_query(user_query)
        response_text = (
            response if not llm.is_format_response else llm.format_response(response)
        )

        # Log the conversation
        conversation_log.append(
            {"user_query": user_query, "bot_response": response_text}
        )

        # Provide a hint for follow-up queries
        hint = ""
        if "table" in llm.context:
            hint = f"(Hint: Hey, I am currently working with the table: {llm.context['table']}). If this is not the table you want to work with, please let me know."

        return jsonify({"response": response_text, "hint": hint})

    elif request.method == "GET":
        return jsonify({"error": "Method not allowed"}), 405


if __name__ == "__main__":
    app.run(debug=True)
