import argparse
import boto3
from internet_scholar import read_dict_from_s3_url, AthenaLogger, AthenaDatabase, compress
import logging
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import csv
from pathlib import Path
import json
from datetime import datetime


SELECT_YOUTUBE_VIDEOS = """
select distinct
  url_extract_parameter(validated_url, 'v') as video_id
from
  validated_url
where
  url_extract_host(validated_url) = 'www.youtube.com'
"""

SELECT_COUNT_YOUTUBE_VIDEOS = """
select count(distinct url_extract_parameter(validated_url, 'v')) as video_count
from
  validated_url
where
  url_extract_host(validated_url) = 'www.youtube.com'
"""

TABLE_YOUTUBE_VIDEO_SNIPPET_EXISTS = """
  and url_extract_parameter(validated_url, 'v') not in (select id from youtube_video_snippet)
"""

CREATE_VIDEO_SNIPPET_JSON = """
create external table if not exists youtube_video_snippet
(
    kind string,
    etag string,
    id   string,
    retrieved_at timestamp,
    snippet struct<
        publishedAt:  timestamp,
        title:        string,
        description:  string,
        channelId:    string,
        channelTitle: string,
        categoryId:   string,
        tags:         array<string>,
        liveBroadcastContent: string,
        defaultlanguage:      string,
        defaultAudioLanguage: string,
        localized:  struct <title: string, description: string>,
        thumbnails: struct<
            default:  struct <url: string, width: int, height: int>,
            medium:   struct <url: string, width: int, height: int>,
            high:     struct <url: string, width: int, height: int>,
            standard: struct <url: string, width: int, height: int>,
            maxres:   struct <url: string, width: int, height: int>
        >
    >
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'ignore.malformed.json' = 'true'
)
LOCATION 's3://{s3_bucket}/youtube_video_snippet/'
TBLPROPERTIES ('has_encrypted_data'='false')
"""

SELECT_DISTINCT_CHANNEL = """
select distinct
  snippet.channelId as channel_id
from
  youtube_video_snippet
order by
  channel_id;
"""

SELECT_COUNT_DISTINCT_CHANNEL = """
select count(distinct snippet.channelId) as channel_count
from
  youtube_video_snippet;
"""

CREATE_CHANNEL_STATS_JSON = """
create external table if not exists youtube_channel_stats
(
    kind string,
    etag string,
    id   string,
    statistics struct<
        viewCount: bigint,
        commentCount: bigint,
        subscriberCount: bigint,
        hiddenSubscriberCount: boolean,
        videoCount: bigint
    >,
    retrieved_at timestamp
)
PARTITIONED BY (creation_date String)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'ignore.malformed.json' = 'true'
)
LOCATION 's3://{s3_bucket}/youtube_channel_stats/'
TBLPROPERTIES ('has_encrypted_data'='false')
"""


class Youtube:
    def __init__(self, credentials, athena_data, s3_admin, s3_data):
        self.credentials = credentials
        self.athena_data = athena_data
        self.s3_admin = s3_admin
        self.s3_data = s3_data

    LOGGING_INTERVAL = 100

    def collect_video_snippets(self):
        logging.info("Start collecting video snippets")
        athena = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)
        query = SELECT_YOUTUBE_VIDEOS
        query_count = SELECT_COUNT_YOUTUBE_VIDEOS
        if athena.table_exists("youtube_video_snippet"):
            logging.info("Table youtube_video_snippet exists")
            query = query + TABLE_YOUTUBE_VIDEO_SNIPPET_EXISTS
            query_count = query_count + TABLE_YOUTUBE_VIDEO_SNIPPET_EXISTS
        logging.info("Download IDs for all Youtube videos that have not been processed yet")
        video_count = int(athena.query_athena_and_get_result(query_string=query_count)['video_count'])
        logging.info("There are %d links to be processed: download them", video_count)
        video_ids_csv = athena.query_athena_and_download(query_string=query, filename="video_ids.csv")

        output_json = Path(Path(__file__).parent, 'tmp', 'youtube_video.json')
        Path(output_json).parent.mkdir(parents=True, exist_ok=True)
        current_key = 0
        youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                  version="v3",
                                                  developerKey=
                                                  self.credentials[current_key]['developer_key'],
                                                  cache_discovery=False)
        with open(video_ids_csv, newline='') as csv_reader:
            with open(output_json, 'w') as json_writer:
                reader = csv.DictReader(csv_reader)
                num_videos = 0
                for video_id in reader:
                    if num_videos % self.LOGGING_INTERVAL == 0:
                        logging.info("%d out of %d videos processed", num_videos, video_count)
                    num_videos = num_videos + 1

                    no_response = True
                    while no_response:
                        try:
                            response = youtube.videos().list(part="snippet",id=video_id['video_id']).execute()
                            no_response = False
                        except HttpError as e:
                            if "403" in str(e):
                                logging.info("Invalid {} developer key: {}".format(
                                    current_key,
                                    self.credentials[current_key]['developer_key']))
                                current_key = current_key + 1
                                if current_key >= len(self.credentials):
                                    raise
                                else:
                                    youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                                              version="v3",
                                                                              developerKey=
                                                                              self.credentials[current_key][
                                                                                  'developer_key'],
                                                                              cache_discovery=False)
                            else:
                                raise
                    for item in response.get('items', []):
                        item['snippet']['publishedAt'] = item['snippet']['publishedAt'].rstrip('Z').replace('T', ' ')
                        item['retrieved_at'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        json_writer.write("{}\n".format(json.dumps(item)))

        logging.info("Compress file %s", output_json)
        compressed_file = compress(filename=output_json, delete_original=True)

        s3 = boto3.resource('s3')
        s3_filename = "youtube_video_snippet/{}-{}.json.bz2".format(datetime.utcnow().strftime("%Y-%m-%d"), num_videos)
        logging.info("Upload file %s to bucket %s at %s", compressed_file, self.s3_data, s3_filename)
        s3.Bucket(self.s3_data).upload_file(str(compressed_file), s3_filename)

        logging.info("Concluded collecting video snippets")
        athena.query_athena_and_wait(query_string=CREATE_VIDEO_SNIPPET_JSON.format(s3_bucket=self.s3_data))

    def collect_channel_stats(self):
        logging.info("Start collecting Youtube channel stats")
        channel_ids = Path(Path(__file__).parent, 'tmp', 'channel_ids.csv')
        athena = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)
        athena.query_athena_and_download(query_string=SELECT_DISTINCT_CHANNEL, filename=channel_ids)
        channel_count = int(
            athena.query_athena_and_get_result(
                query_string=SELECT_COUNT_DISTINCT_CHANNEL
            )['channel_count']
        )
        logging.info("There are %d channels to be processed: download them", channel_count)

        current_key = 0
        youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                  version="v3",
                                                  developerKey=
                                                  self.credentials[current_key]['developer_key'],
                                                  cache_discovery=False)
        with open(channel_ids, newline='') as csv_reader:
            output_json = Path(Path(__file__).parent, 'tmp', 'youtube_channel_stats.json')
            with open(output_json, 'w') as json_writer:
                reader = csv.DictReader(csv_reader)
                num_channels = 0
                for channel_id in reader:
                    if num_channels % self.LOGGING_INTERVAL == 0:
                        logging.info("%d out of %d channels processed", num_channels, channel_count)
                    num_channels = num_channels + 1

                    no_response = True
                    while no_response:
                        try:
                            response = youtube.channels().list(part="statistics",id=channel_id['channel_id']).execute()
                            no_response = False
                        except HttpError as e:
                            if "403" in str(e):
                                logging.info("Invalid {} developer key: {}".format(
                                    current_key,
                                    self.credentials[current_key]['developer_key']))
                                current_key = current_key + 1
                                if current_key >= len(self.credentials['youtube']):
                                    raise
                                else:
                                    youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                                              version="v3",
                                                                              developerKey=
                                                                              self.credentials[current_key][
                                                                                  'developer_key'],
                                                                              cache_discovery=False)
                            else:
                                raise
                    for item in response.get('items', []):
                        item['retrieved_at'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        json_writer.write("{}\n".format(json.dumps(item)))

        logging.info("Compress file %s", output_json)
        compressed_file = compress(filename=output_json, delete_original=True)

        s3 = boto3.resource('s3')
        s3_filename = "youtube_channel_stats/creation_date={}/{}.json.bz2".format(datetime.utcnow().strftime("%Y-%m-%d"),
                                                                               num_channels)
        logging.info("Upload file %s to bucket %s at %s", compressed_file, self.s3_data, s3_filename)
        s3.Bucket(self.s3_data).upload_file(str(compressed_file), s3_filename)

        logging.info("Recreate table for Youtube channel stats")
        athena.query_athena_and_wait(query_string="DROP TABLE IF EXISTS youtube_channel_stats")
        athena.query_athena_and_wait(query_string=CREATE_CHANNEL_STATS_JSON.format(s3_bucket=self.s3_data))
        athena.query_athena_and_wait(query_string="MSCK REPAIR TABLE youtube_channel_stats")

        logging.info("Concluded collecting channel stats")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='S3 Bucket with configuration', required=True)
    args = parser.parse_args()

    config = read_dict_from_s3_url(url=args.config)
    logger = AthenaLogger(app_name="youtube",
                          s3_bucket=config['aws']['s3-admin'],
                          athena_db=config['aws']['athena-admin'])
    try:
        youtube = Youtube(credentials=config['youtube'],
                          athena_data=config['aws']['athena-data'],
                          s3_admin=config['aws']['s3-admin'],
                          s3_data=config['aws']['s3-data'])
        youtube.collect_video_snippets()
        youtube.collect_channel_stats()
    finally:
        logger.save_to_s3()


if __name__ == '__main__':
    main()
