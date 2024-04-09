from m_database import User_token

def print_tokens_in_db():
    tokens_from_mongo = list(User_token.find())
    return [token['token'] for token in tokens_from_mongo]

tokens = print_tokens_in_db()
