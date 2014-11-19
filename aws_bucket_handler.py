#coding: utf-8

import os

from boto.s3.key import Key
from boto.s3.connection import S3Connection, Bucket
from flask import current_app

from my_exceptions import (
    BucketStoreFileError,
    BucketRetrieveFileError)


class BucketHandler(object):

    def __init__(self,
                 bucket_name=None,
                 access_id=None,
                 secret_key=None):
        self.bucket_name = bucket_name
        self.access_id = access_id
        self.secret_key = secret_key

        self.conn = S3Connection(self.access_id, self.secret_key)

    def list_bucket_files(self, bucket_name=None):
        if not bucket_name:
            bucket_name = self.bucket_name

        bucket = self.conn.get_bucket(bucket_name)

        return [key.name.encode('utf-8') for key in bucket.list()]

    def store_file(self, kw, filename, filepath=None, bucket_name=None):

        try:
            if not bucket_name:
                bucket_name = self.bucket_name

            if not filepath:
                base_path = os.path.dirname(os.path.abspath(__file__))
                filepath = os.path.join(base_path, '../tmp/')

            keyword = Key(self.conn.get_bucket(bucket_name))
            keyword.key = kw

            keyword.set_contents_from_filename(os.path.join(filepath, filename))

        except Exception as error:
            current_app.logger.error(error)
            raise BucketStoreFileError('Store Fail')

    def retrieve_file(self, kw, filename, filepath=None, bucket_name=None):

        try:
            if not bucket_name:
                bucket_name = self.bucket_name

            if not filepath:
                base_path = os.path.dirname(os.path.abspath(__file__))
                filepath = os.path.join(base_path, '../tmp/')

            keyword = Key(self.conn.get_bucket(bucket_name))
            keyword.key = kw

            fullpath_file = os.path.join(filepath, filename)
            # filepath and filename to restore the file down on the app side
            keyword.get_contents_to_filename(fullpath_file)

            return fullpath_file

        except Exception as error:
            current_app.logger.error(error)
            raise BucketRetrieveFileError('Bucket Retrieve File Error')

    def remove_file_from_bucket(self, kw, bucket_name=None):
        if not bucket_name:
            bucket_name = self.bucket_name

        bucket = Bucket(self.conn, self.bucket_name)
        keyword = Key(bucket)
        keyword.key = kw
        try:
            bucket.delete_key(keyword)
        except Exception as error:
            current_app.logger.error(error)
            return False

        return True
