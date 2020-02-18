"""
Transformation functions for use in conjunction with s3 manager operations.
"""
import json
from pandas import DataFrame, read_csv, read_json
from s3_manager import Session, parse_path

s3mgr = Session()


def csv_to_df(path: str) -> DataFrame:
    """
    Reads a csv-like file from the given S3 path and converts it into a Pandas dataframe
    """
    parsed = parse_path(path)

    s3mgr._load_into_buffer(
        (
            s3mgr.client.get_object(Bucket=parsed["bucket"], Key=parsed["key"])["Body"]
            .read()
            .decode("utf-8")
        )
    )
    return read_csv(s3mgr.buffer)


def df_to_csv(frame: DataFrame, path: str, **kwargs):
    """
    Reads a dataframe-like file from the given S3 path and converts it into a CSV object
    """
    parsed = parse_path(path)
    s3mgr._clear_buffer()
    frame.to_csv(s3mgr.buffer, index=False, **kwargs)
    s3mgr.client.put_object(
        Bucket=parsed["bucket"], Key=parsed["key"], Body=s3mgr.buffer.getvalue()
    )


def json_to_df(path: str) -> DataFrame:
    """
    Reads a json-like file from the given S3 path and converts it into a Pandas dataframe
    """
    parsed = parse_path(path)
    data = json.loads(
        (
            s3mgr.client.get_object(Bucket=parsed["bucket"], Key=parsed["key"])["Body"]
            .read()
            .decode("utf-8")
        )
    )
    s3mgr._load_into_buffer(json.dumps(data))
    return read_json(s3mgr.buffer)


def df_to_json(frame: DataFrame, path: str, **kwargs):
    """
    Reads a dataframe-like file from the given S3 path and converts it into a JSON object
    """
    parsed = parse_path(path)
    s3mgr._clear_buffer()
    frame.to_json(s3mgr.buffer, **kwargs)
    s3mgr.client.put_object(
        Bucket=parsed["bucket"], Key=parsed["key"], Body=s3mgr.buffer.getvalue()
    )
