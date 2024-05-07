import pandas as pd
from sqlalchemy import text
from typing import Optional


def create_tables(
        engine,
        table_name: str,
        data_frame: pd.DataFrame,
        columns: list[str],
        constraint_column: Optional[str] = None,
        sql_constraint: bool = True,
        main_table: Optional[str] = None,
        indexing: bool = False
) -> None:
    """
    Create a table with an optional specified constraint.

    Parameters:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine to connect to the database.
        table_name (str): Name for the table.
        data_frame (pd.DataFrame): DataFrame containing the data to be inserted into the table.
        columns (list[str]): List of column names for the table.
        constraint_column (str, optional): Name of the column for the constraint. Default is None.
        sql_constraint (bool): Input (True) for primary key constraint or (False) for foreign key constraint. Default is True.
        main_table (str, optional): Name of the main table. Required if sql_constraint is False. Default is None.
        indexing (bool): Input (True) for indexing. Default is None.

    Returns:
        None
    """
    # Connect to the database
    connection = engine.connect()

    try:
        # Create DataFrame for the table
        table_df = data_frame[columns]

        # Convert column names to lowercase
        table_df.columns = map(str.lower, table_df.columns)

        # Write DataFrame to SQL table
        table_df.to_sql(
            table_name,
            con=engine,
            if_exists='replace',
            index=indexing,
        )

        # Add constraint to the table
        if constraint_column is not None and sql_constraint:
            # Add primary key constraint
            sql_add_pk = f"""
                ALTER TABLE {table_name}
                ADD PRIMARY KEY ({constraint_column});
            """
            with engine.begin() as connection:
                connection.execute(text(sql_add_pk))

            print(f"Table {table_name} created successfully with primary key as {constraint_column}.")
        elif constraint_column is not None and not sql_constraint and main_table is not None:
            # Add foreign key constraint
            sql_add_fk = f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT fk_{constraint_column}
                FOREIGN KEY ({constraint_column}) REFERENCES {main_table}({constraint_column});
            """
            with engine.begin() as connection:
                connection.execute(text(sql_add_fk))

            print(f"Table {table_name} created successfully with foreign key referring to {main_table} PK.")
        else:
            print(f"Table {table_name} created successfully without constraint.")

    except Exception as e:
        connection.rollback()
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the database connection
        connection.close()
