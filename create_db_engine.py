from sqlalchemy import create_engine


def create_db_engine(username: str, password: str, host: str, port: int, db_name: str) -> create_engine:
    """
    Function to create a SQLAlchemy engine for connecting to a PostgreSQL database.

    Args:
        username (str): Username for the database.
        password (str): Password for the database.
        host (str): Hostname of the database server.
        port (int): Port number of the database server.
        db_name (str): Name of the database.

    Returns:
        SQLAlchemy Engine: Engine object for connecting to the database.
    """
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db_name}')
    return engine
