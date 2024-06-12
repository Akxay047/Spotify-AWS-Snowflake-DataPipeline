-- create database spotify_db;

create or replace storage integration s3_init
    TYPE = EXTERNAL_STAGE 
    STORAGE_PROVIDER = S3
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::654654289696:role/spotify-snowflake-role-akxay'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-akxay')


DESC integration s3_init


create or replace file format csv_fileformat
    type = csv 
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = TRUE;


create or replace stage spotify_stage
    URL = 's3://spotify-etl-project-akxay/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat


list @spotify_stage



create or replace table tbl_album(
    album_id STRING,
    name STRING,
    release_date DATE,
    total_tracks INT,
    url STRING
)


create or replace table tbl_artists(
    artist_id STRING,
    name STRING,
    url STRING
)


create or replace table tbl_songs(
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING
)


-- create snowpipe 
create or replace schema pipe 


create or replace pipe spotify_db.pipe.tbl_songs_pipe
auto_ingest = TRUE 
AS 
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_stage/songs;


create or replace pipe spotify_db.pipe.tbl_album_pipe
auto_ingest = TRUE 
AS 
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/album;


create or replace pipe spotify_db.pipe.tbl_artists_pipe
auto_ingest = TRUE 
AS 
COPY INTO spotify_db.public.tbl_artists
FROM @spotify_db.public.spotify_stage/artist;

desc pipe pipe.tbl_songs_pipe

select count(*) from spotify_db.public.tbl_songs

select count(*) from spotify_db.public.tbl_artists

select count(*) from spotify_db.public.tbl_album
