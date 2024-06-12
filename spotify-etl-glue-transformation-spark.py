import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame 



sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_path = "s3://spotify-etl-project-akxay/raw_data/to_processed/"
source_df = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="json"
)
spotify_df = source_df.toDF()
def process_albums(df):
    df = df.withColumn("items", explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("url")
    ).drop_duplicates(['album_id'])
    return df



def process_artists(df):
    df_artist_exploded = df.select(explode(col("items")).alias("item")).select(
    explode(col("item.track.artists")).alias("artists")
    )
    df_artists = df_artist_exploded.select(
        col("artists.id").alias("artist_id"),
        col("artists.name").alias("artist_name"),
        col("artists.external_urls.spotify").alias("artist_url")
    ).drop_duplicates(['artist_id'])
    return df_artists



def process_songs(df):
    df_explode = df.select(explode(col("items")).alias("item"))
    
    df_songs = df_explode.select(
        col("item.track.id").alias("song_id"),
        col("item.track.name").alias("song_name"),
        col("item.track.duration_ms").alias("duration_ms"),
        col("item.track.external_urls.spotify").alias("url"),
        col("item.track.popularity").alias("popularity"),
        col("item.added_at").alias("song_added"),
        col("item.track.album.id").alias("album_id"),
        col("item.track.artists")[0]["id"].alias("artist_id")
    ).drop_duplicates(['song_id'])
    
    df_songs = df_songs.withColumn("song_added", to_date(col("song_added")))
    
    return df_songs

    
    
    
album_df = process_albums(spotify_df)

artist_df = process_artists(spotify_df)

songs_df = process_songs(spotify_df)


def write_to_s3(df, path_suffix, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": "s3://spotify-etl-project-akxay/transformed_data/" + path_suffix + "/"
        },
        format=format_type
    )
    
    
write_to_s3(album_df, "album/album_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(artist_df, "artist/artist_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(songs_df, "songs/song_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
job.commit()