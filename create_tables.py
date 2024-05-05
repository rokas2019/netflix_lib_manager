import pandas as pd
from sqlalchemy import text


def create_table_with_constraint(
        engine,
        table_name: str,
        constraint_column: str,
        data_frame: pd.DataFrame,
        columns: list[str],
        sql_constraint: bool,
        main_table: str
) -> None:
    """
    Create a table with a specified constraint.

    Parameters:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine to connect to the database.
        table_name (str): Name for the table.
        constraint_column (str): Name of the column for the constraint.
        data_frame (pd.DataFrame): DataFrame containing the data to be inserted into the table.
        columns (list[str]): List of column names for the table.
        sql_constraint (bool): Input (True) for primary key constraint or (False) for foreign key constraint.
        main_table (str): Name of the main table

    Returns:
        None
    """
    # Connect to the database
    connection = engine.connect()

    try:
        # Create DataFrame for the table
        table_df = data_frame[columns]

        # Convert constraint column to integer
        table_df.loc[:, constraint_column] = table_df[constraint_column].astype('int')

        # Write DataFrame to SQL table
        table_df.to_sql(
            table_name,
            con=engine,
            if_exists='replace',
            index=False,
        )

        # Add constraint to the table
        if sql_constraint:
            # Add primary key constraint
            sql_add_pk = f"""
                ALTER TABLE {table_name}
                ADD PRIMARY KEY ({constraint_column});
            """
            with engine.begin() as connection:
                connection.execute(text(sql_add_pk))

            print(f"Table {table_name} created successfully with primary key as {constraint_column}.")
        else:
            # Add foreign key constraint
            sql_add_fk = f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT fk_{constraint_column}
                FOREIGN KEY ({constraint_column}) REFERENCES {main_table}({constraint_column});
            """
            with engine.begin() as connection:
                connection.execute(text(sql_add_fk))

            print(f"Table {table_name} created successfully with foreign key referring to {main_table} PK.")

    except Exception as e:
        connection.rollback()
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the database connection
        connection.close()


def create_table(
        engine,
        table_name: str,
        data_frame: pd.DataFrame,
        columns: list[str],
        indexing: bool
) -> None:
    """
    Create a table from DataFrame.

    Parameters:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine to connect to the database.
        table_name (str): Name for the table.
        data_frame (pd.DataFrame): DataFrame containing the data to be inserted into the table.
        columns (list[str]): List of column names for the table.
        indexing (bool): True to include DataFrame index as a column in the table.

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

        print(f"Table {table_name} created successfully.")
    except Exception as e:
        connection.rollback()
        print(f"An error occurred: {str(e)}")
    finally:
        # Close the database connection
        connection.close()


def add_constrains(
        engine,
        table_name: str,
        constraint_column: str,
        sql_constraint: bool,
        main_table: str
) -> None:
    """
    Add constraints to a table.

    Parameters:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine to connect to the database.
        table_name (str): Name of the table.
        constraint_column (str): Name of the column for the constraint.
        sql_constraint (bool): True for primary key constraint, False for foreign key constraint.
        main_table (str): Name of the main table

    Returns:
        None
    """
    try:

        if sql_constraint:
            # Add primary key constraint
            sql_add_pk = f"""
                    ALTER TABLE {table_name}
                    ADD PRIMARY KEY ({constraint_column});
                """
            with engine.begin() as connection:
                connection.execute(text(sql_add_pk))

            print(f"Table {table_name} created successfully with primary key as {constraint_column}.")
        else:
            # Add foreign key constraint
            sql_add_fk = f"""
                    ALTER TABLE {table_name}
                    ADD CONSTRAINT fk_{constraint_column}
                    FOREIGN KEY ({constraint_column}) REFERENCES {main_table}({constraint_column});
                """
            with engine.begin() as connection:
                connection.execute(text(sql_add_fk))

            print(f"Table {table_name} created successfully with foreign key referring to {main_table} PK.")

    except Exception as e:
        connection.rollback()
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the database connection
        connection.close()
