#!python2.7

# Standard libs
from datetime import datetime
from datetime import timedelta
import os
import re

# Third party libs
import boto

def main():
    # Retrieve parameters from environment variables
    access_key_id     = os.environ['AWS_ACCESS_KEY_ID']
    secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    bucket_name       = os.environ['S3_BUCKET']
    source_dir        = os.environ['SOURCE_DIR'].rstrip('/') + '/'
    dest_dir          = os.environ['DEST_DIR'].rstrip('/') + '/'

    # Connect to S3 bucket
    s3 = boto.connect_s3(access_key_id, secret_access_key)
    bucket = s3.get_bucket(bucket_name)

    # See if we have a backup file from this month in the destination directory
    backup_file = get_backup_from_this_month(bucket, dest_dir)

    # If we already have a backup from this month, we can bail here
    if backup_file:
        return

    # Otherwise, try to find a backup from this month in the source directory
    backup_file = get_backup_from_this_month(bucket, source_dir)

    # If we didn't find a backup from this month, give up
    if not backup_file:
        return

    # Otherwise, copy the backup from the source dir to the destination dir
    dest_filename = re.sub(source_dir, dest_dir, backup_file.name)
    backup_file.copy(bucket_name, dest_filename)

def get_backup_from_this_month(bucket, dir_name):
    now = datetime.utcnow()

    for backup_file in bucket.list(prefix=dir_name):
        pattern = r'%s%s-%s-\d{2}_\d{2}-\d{2}-\d{2}.gz' % (dir_name,
                                                           now.strftime('%Y'),
                                                           now.strftime('%m'))
        if re.match(pattern, backup_file.name):
            return backup_file

    return None

if __name__ == '__main__':
    main()
