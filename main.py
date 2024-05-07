import pandas as pd
from create_db_engine import create_db_engine
from create_tables import create_tables
from data_handlers import explode_list, get_distinct_values_from_column
from db_conn_settings import DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT

if __name__ == "__main__":
    # Read CSV files
    titles_raw = pd.read_csv('raw_titles.csv')
    credits_raw = pd.read_csv('raw_credits.csv')
    best_movies_raw = pd.read_csv('best_movies_netflix.csv')
    best_shows_raw = pd.read_csv('best_shows_netflix.csv')
    best_movies_by_year_raw = pd.read_csv('best_movie_by_year_netflix.csv')
    best_shows_by_year_raw = pd.read_csv('best_show_by_year_netflix.csv')

    # Select columns for tables
    titles_table = ['index', 'id', 'title', 'type', 'release_year', 'age_certification', 'runtime',
                    'seasons', 'imdb_id', 'imdb_score', 'imdb_votes']
    titles_genres_table = ['index', 'id', 'genres']
    titles_production_countries = ['index', 'id', 'production_countries']

    credits_table = ['index', 'id', 'person_id', 'name', 'role']
    characters_table = ['index', 'character']

    best_shows = ['index', 'TITLE', 'RELEASE_YEAR', 'SCORE', 'NUMBER_OF_VOTES', 'DURATION', 'NUMBER_OF_SEASONS',
                  'MAIN_GENRE', 'MAIN_PRODUCTION']
    best_shows_by_year = ['index', 'TITLE', 'RELEASE_YEAR', 'SCORE', 'NUMBER_OF_SEASONS', 'MAIN_GENRE',
                          'MAIN_PRODUCTION']
    best_movies = ['index', 'TITLE', 'RELEASE_YEAR', 'SCORE', 'NUMBER_OF_VOTES', 'DURATION', 'MAIN_GENRE',
                   'MAIN_PRODUCTION']
    best_movies_by_year = ['index', 'TITLE', 'RELEASE_YEAR', 'SCORE', 'MAIN_GENRE', 'MAIN_PRODUCTION']

    # Get Distinct values from multiple tables
    list_to_distinct_values = [best_movies_raw, best_movies_by_year_raw, best_shows_by_year_raw, best_shows_raw]
    main_prod = 'MAIN_PRODUCTION'
    main_prod_col = ['main_production']

    main_production = get_distinct_values_from_column(*list_to_distinct_values, column_name=main_prod)

    main_gen = 'MAIN_GENRE'
    main_gen_col = ['main_genre']
    main_genres = get_distinct_values_from_column(*list_to_distinct_values, column_name='MAIN_GENRE')

    # Explode the columns
    modified_genres_df = explode_list(titles_raw, 'genres')
    modified_character_df = explode_list(credits_raw, 'character')
    modified_production_countries = explode_list(titles_raw, 'production_countries')

    # Create engine
    engine = create_db_engine(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

    # Create tables and relationships
    id_column_name = 'index'

    create_tables(engine, 'titles_table', titles_raw, titles_table, id_column_name, True)

    create_tables(engine, 'titles_genres_table', modified_genres_df, titles_genres_table, id_column_name, False,
                  'titles_table')

    create_tables(engine, 'titles_production_countries', modified_production_countries, titles_production_countries,
                  id_column_name, False, 'titles_table')

    create_tables(engine, 'credits_table', credits_raw, credits_table, id_column_name, True)
    create_tables(engine, 'characters_table', modified_character_df, characters_table, id_column_name, False,
                  'credits_table')
    create_tables(engine, 'best_movies', best_movies_raw, best_movies, id_column_name, True)
    create_tables(engine, 'best_shows', best_shows_raw, best_shows, id_column_name, True)
    create_tables(engine, 'best_shows_by_year', best_shows_by_year_raw, best_shows_by_year, id_column_name, True)
    create_tables(engine, 'best_movies_by_year', best_movies_by_year_raw, best_movies_by_year, id_column_name, True)
    create_tables(engine, 'main_genres', main_genres, main_gen_col, id_column_name, True, None, True)
    create_tables(engine, 'main_productions', main_production, main_prod_col, id_column_name, True, None, True)

    # Insert raw data into database tables
    titles_raw.to_sql('raw_titles', engine, if_exists='replace', index=False)
    credits_raw.to_sql('raw_credits', engine, if_exists='replace', index=False)
    best_movies_raw.to_sql('best_movies_raw', engine, if_exists='replace', index=False)
    best_shows_raw.to_sql('best_shows_raw', engine, if_exists='replace', index=False)
    best_movies_by_year_raw.to_sql('best_movies_by_year_raw', engine, if_exists='replace', index=False)
    best_shows_by_year_raw.to_sql('best_shows_by_year_raw', engine, if_exists='replace', index=False)

    # Close connection with DB
    engine.dispose()
    print("Connection to the PostgreSQL database closed!")
