from sqlalchemy import text
from create_db_engine import create_db_engine
from db_conn_settings import DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

engine = create_db_engine(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)


def terminal_query(query: str):
    """
    Function to send SQL query to the database.

    Args:
        query (str): SQL query string.

    Returns:
        None
    """
    try:
        # Connect to the database
        with engine.begin() as connection:
            # Check if the query is a SELECT statement
            is_select = query.strip().upper().startswith('SELECT')

            # Execute the query
            result = connection.execute(text(query))

            # If it's a SELECT query, fetch and print the rows
            if is_select:
                rows = result.fetchall()
                for row in rows:
                    print(row)
            else:
                # For other types of queries, just print query executed successfully
                print("Query executed successfully.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

# EXAMPLE OF USAGE IN TERMINAL/BASH
# python -c "from sequal_queries import terminal_query; terminal_query('SELECT * FROM best_movies')"
