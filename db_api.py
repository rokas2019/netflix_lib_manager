from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from create_db_engine import create_db_engine
from db_conn_settings import DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

# Create FastAPI instance
app = FastAPI()

# Create database engine
engine = create_db_engine(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)


def send_query(query: str):
    """
    Function to send SQL query to the database.

    Args:
        query (str): SQL query string.

    Returns:
        List[Dict] or str: List of dictionaries containing query result if query is SELECT, or message for other queries.
    """
    try:
        # Connect to the database
        with engine.begin() as connection:
            result = connection.execute(text(query))
            # Fetch all rows from the result proxy if query is SELECT
            if query.strip().upper().startswith("SELECT"):
                rows = result.fetchall()
                # Get column names
                columns = result.keys()
                # Convert rows to list of dictionaries
                result_list = [dict(zip(columns, row)) for row in rows]
                return result_list
            else:
                # For non-SELECT queries, return a message
                return f"Query executed successfully."
    except Exception as e:
        connection.rollback()
        raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")
    finally:
        engine.dispose()


@app.post("/query/")
async def run_query(query: str):
    """
    Run SQL query on the database.

    Args:
        query (str): SQL query string.

    Returns:
        List[Dict] or str: List of dictionaries containing query result if query is SELECT, or message for other queries.
    """
    return send_query(query)
