"""
High-level wrapper around S3 functions, based on fully qualified paths instead of bucket/key pairs.
This module differs from s3_rw in that it is less concerned with dataframe <-> s3 path
transformations and is more similar in design to the functionality found in backend x/aws.
"""
import re
import io
from logger import logging as log
from typing import List, AnyStr
from botocore.exceptions import ClientError
import boto3


class Session:
    """
    High-level, path based wrapper around boto3 S3 operations
    """

    def __init__(self):
        self.client = boto3.client("s3")
        self.buffer = io.StringIO()
        self.error = None

    def clear_buffer(self):
        """ Clear the internal string buffer """
        self.buffer = io.StringIO()

    def get(self, path: str) -> str:
        """
        Downloads a file and return its contents as a string. Objects that are larger than available
        memory cannot be loaded via `get`.
        """
        parsed_path = parse_path(path)
        try:
            data = self.client.get_object(
                Bucket=parsed_path["bucket"], Key=parsed_path["key"]
            )["Body"]
            return data.read().decode("unicode-escape")
        except ClientError as err:
            self.error = err
            if err.response["Error"]["Code"] == "NoSuchKey":
                log.error("S3 file %s does not exist", path)

    def list(self, path: str) -> List[str]:
        """
        Lists all objects at the given S3 path and returns them in a list. Each elemet of the
        returned list is a fully-qualified S3 path (i.e. it could be passed to other s3_manager
        functions).
        """
        objects = []
        parsed_path = parse_path(path)
        try:
            keys = self.client.list_objects_v2(
                Bucket=parsed_path["bucket"], Prefix=parsed_path["key"]
            )["Contents"]
            objects.extend(
                ["s3://" + parsed_path["bucket"] + "/" + k["Key"] for k in keys]
            )
        except ClientError as err:
            self.error = err
            if err.response["Error"]["Code"] == "NoSuchKey":
                log.error("S3 file %s does not exist", path)
        return objects

    def delete(self, path: str):
        """
        Deletes a file at the given S3 path. If a 'directory-like' path is provided, a single http
        connection is used to recursively delete all keys in the ensuing paths.
        """
        parsed_path = parse_path(path)
        del_list = list(path)
        del_dict = {
            "Objects": list(map(lambda p: {"Key": parse_path(p)["key"]}, del_list))
        }
        if not del_list:
            _ = self.client.delete_object(
                Bucket=parsed_path["bucket"], Key=parsed_path["key"]
            )
        else:
            _ = self.client.delete_objects(
                Bucket=parsed_path["bucket"], Delete=del_dict
            )

    def move(self, origin: str, destination: str):
        """
        Moves the given file from the origin path to the destination path. If a 'directory-like'
        path is provided, a single http connection is used to recursively delete all keys in the
        ensuing paths.
        """
        input_path = parse_path(origin)
        input_list = list(origin)
        output_path = parse_path(destination)
        output_list = list(destination)

        copy_dict = {
            "Objects": list(map(lambda p: {"Key": parse_path(p)["key"]}, input_list))
        }
        if not input_list:
            _ = self.client.put_object(
                Bucket=output_path["bucket"], Key=input_path["key"]
            )
            _ = self.client.delete_object(
                Bucket=input_path["bucket"], Key=input_path["key"]
            )
        else:
            copy_list = list(map(lambda p: parse_path(p)["key"], input_list))
            for k in copy_list:
                _ = self.client.put_object(Bucket=output_path["bucket"], Key=k)
            _ = self.client.delete_objects(
                Bucket=input_path["bucket"], Delete=copy_dict
            )

    def copy(self, origin: str, destination: str):
        """
        Copies the given file from the origin path to the destination path. If a 'directory-like'
        path is provided, a single http connection is used to recursively delete all keys in the
        ensuing paths.
        """
        input_path = parse_path(origin)
        input_list = list(origin)
        output_path = parse_path(destination)
        output_list = list(destination)

        copy_dict = {
            "Objects": list(map(lambda p: {"Key": parse_path(p)["key"]}, input_list))
        }
        if not input_list:
            _ = self.client.put_object(
                Bucket=output_path["bucket"], Key=input_path["key"]
            )
        else:
            copy_list = list(map(lambda p: parse_path(p)["key"], input_list))
            for k in copy_list:
                _ = self.client.put_object(Bucket=output_path["bucket"], Key=k)

    def path_exists(self, path: str) -> bool:
        """ Returns true if the specified path exists in S3, otherwise false """
        parsed = parse_path(path)
        try:
            self.client.get_object(Bucket=parsed["bucket"], Key=["key"])["Body"]
        except ClientError as err:
            self.error = err
            if err.response["Error"]["Code"] == "NoSuchKey":
                return False
        return True


def parse_path(path: str) -> dict:
    """ Parses a given fully-qualified S3 path and returns a dictionary of S3-relevant items """
    regex = re.match(r"s3:\/\/.*?\/", path).group()
    bucket = regex.split("s3://")[1].strip("/")
    key = path.split(regex)[1]
    return {
        "path": path,
        "bucket": bucket,
        "key": key,
    }

