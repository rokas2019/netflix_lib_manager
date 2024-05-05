-- best_movies TABLE
-- Step 1: Set PK
ALTER TABLE best_movies
ADD PRIMARY KEY (index);

-- Step 2: Add the new columns to best_movies table
ALTER TABLE best_movies
ADD COLUMN main_production_id INT,
ADD COLUMN main_genre_id INT;

-- Step 3: Update the new columns with the corresponding index values from main_productions and main_genres
UPDATE best_movies bm
SET main_production_id = mp.index
FROM main_productions mp
WHERE bm.main_production = mp.main_production;

UPDATE best_movies bm
SET main_genre_id = mg.index
FROM main_genres mg
WHERE bm.main_genre = mg.main_genre;

-- Step 4: Add foreign key constraints
ALTER TABLE best_movies
ADD CONSTRAINT fk_main_production_id
FOREIGN KEY (main_production_id)
REFERENCES main_productions(index);

ALTER TABLE best_movies
ADD CONSTRAINT fk_main_genre_id
FOREIGN KEY (main_genre_id)
REFERENCES main_genres(index);

-- Step 5: Drop the unnecessary columns
ALTER TABLE best_movies
DROP COLUMN main_genre,
DROP COLUMN main_production;


-- best_movies_by_year TABLE
-- Step 1: Set PK
ALTER TABLE best_movies_by_year
ADD PRIMARY KEY (index);

-- Step 2: Add the new columns to best_movies table
ALTER TABLE best_movies_by_year
ADD COLUMN main_production_id INT,
ADD COLUMN main_genre_id INT;

-- Step 3: Update the new columns with the corresponding index values from main_productions and main_genres
UPDATE best_movies_by_year bmy
SET main_production_id = mp.index
FROM main_productions mp
WHERE bmy.main_production = mp.main_production;

UPDATE best_movies_by_year bmy
SET main_genre_id = mg.index
FROM main_genres mg
WHERE bmy.main_genre = mg.main_genre;

-- Step 4: Add foreign key constraints
ALTER TABLE best_movies_by_year
ADD CONSTRAINT fk_main_production_id
FOREIGN KEY (main_production_id)
REFERENCES main_productions(index);

ALTER TABLE best_movies_by_year
ADD CONSTRAINT fk_main_genre_id
FOREIGN KEY (main_genre_id)
REFERENCES main_genres(index);

-- Step 5: Drop the unnecessary columns
ALTER TABLE best_movies_by_year
DROP COLUMN main_genre,
DROP COLUMN main_production;


-- best_shows TABLE
-- Step 1: Set PK
ALTER TABLE best_shows
ADD PRIMARY KEY (index);

-- Step 2: Add the new columns to best_movies table
ALTER TABLE best_shows
ADD COLUMN main_production_id INT,
ADD COLUMN main_genre_id INT;

-- Step 3: Update the new columns with the corresponding index values from main_productions and main_genres
UPDATE best_shows bs
SET main_production_id = mp.index
FROM main_productions mp
WHERE bs.main_production = mp.main_production;

UPDATE best_shows bs
SET main_genre_id = mg.index
FROM main_genres mg
WHERE bs.main_genre = mg.main_genre;

-- Step 4: Add foreign key constraints
ALTER TABLE best_shows
ADD CONSTRAINT fk_main_production_id
FOREIGN KEY (main_production_id)
REFERENCES main_productions(index);

ALTER TABLE best_shows
ADD CONSTRAINT fk_main_genre_id
FOREIGN KEY (main_genre_id)
REFERENCES main_genres(index);

-- Step 5: Drop the unnecessary columns
ALTER TABLE best_shows
DROP COLUMN main_genre,
DROP COLUMN main_production;


-- best_shows_by_year TABLE
-- Step 1: Set PK
ALTER TABLE best_shows_by_year
ADD PRIMARY KEY (index);

-- Step 2: Add the new columns to best_movies table
ALTER TABLE best_shows_by_year
ADD COLUMN main_production_id INT,
ADD COLUMN main_genre_id INT;

-- Step 3: Update the new columns with the corresponding index values from main_productions and main_genres
UPDATE best_shows_by_year bsy
SET main_production_id = mp.index
FROM main_productions mp
WHERE bsy.main_production = mp.main_production;

UPDATE best_shows_by_year bsy
SET main_genre_id = mg.index
FROM main_genres mg
WHERE bsy.main_genre = mg.main_genre;

-- Step 4: Add foreign key constraints
ALTER TABLE best_shows_by_year
ADD CONSTRAINT fk_main_production_id
FOREIGN KEY (main_production_id)
REFERENCES main_productions(index);

ALTER TABLE best_shows_by_year
ADD CONSTRAINT fk_main_genre_id
FOREIGN KEY (main_genre_id)
REFERENCES main_genres(index);

-- Step 5: Drop the unnecessary columns
ALTER TABLE best_shows_by_year
DROP COLUMN main_genre,
DROP COLUMN main_production;