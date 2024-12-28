def validate_input(user_input):
    # Basic validation to check if the input is empty
    if not user_input.strip():
        return False
    return True

def format_output(data):
    # Format the output data into a readable format
    if isinstance(data, list):
        formatted_data = "\n".join([str(record) for record in data])
    else:
        formatted_data = str(data)
    return formatted_data

def handle_error(error_message):
    # Format error messages for user-friendly output
    return f"Error: {error_message}"