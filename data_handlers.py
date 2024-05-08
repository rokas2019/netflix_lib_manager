import ast
import pandas as pd


def explode_list(data: pd.DataFrame, *columns) -> pd.DataFrame:
    """
    Explodes specified columns containing comma-separated values into separate rows.

    Args:
        data (pd.DataFrame): DataFrame containing the data.
        *columns (str): Variable-length argument list of column names to explode.

    Returns:
        pd.DataFrame: DataFrame with specified columns exploded.
    """
    for column_name in columns:
        try:
            data[column_name] = data[column_name].apply(ast.literal_eval)
        except (SyntaxError, ValueError):  # Handle cases where literal_eval fails
            data[column_name] = data[column_name].astype(str)
            data[column_name] = data[column_name].str.split('/ ')
        finally:
            data = data.explode(column_name)
    return data


def get_distinct_values_from_column(*data_frames, column_name: str) -> pd.DataFrame:
    """
    Get distinct values from a specified column in multiple DataFrames.

    Parameters:
    data_frames (list of pandas.DataFrame): List of DataFrames.
    column_name (str): Name of the column to extract distinct values from.

    Returns:
    pandas.DataFrame: DataFrame containing only the distinct values from the specified column
                      with a new index.
    """
    distinct_values = set()  # Initialize an empty set to store distinct values
    for df in data_frames:
        # Add the unique values of the column to the set
        distinct_values.update(df[column_name].unique())

    # Create a DataFrame from the set of distinct values
    distinct_values_df = pd.DataFrame({column_name: list(distinct_values)})

    return distinct_values_df
