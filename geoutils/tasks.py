from geoutils import app
import math
import requests
import os
import logging
import boto
from geoutils.utils import get_master_hostname
from geoutils.hdf2tiff import hdf2tif
from filechunkio import FileChunkIO

DIRECTORY = "/tmp/etl/"

try:
    os.makedirs(DIRECTORY)
except OSError:
    pass


MASTER_HOSTNAME = get_master_hostname()

# Set up Logging
def _logging():
    logger = logging.getLogger('geoutils.etl')
    logger.setLevel(logging.INFO)

    if not len(logger.handlers):
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s')

        fh = logging.FileHandler(
            os.path.join(DIRECTORY, "{}.log".format(MASTER_HOSTNAME)))
        fh.setFormatter(formatter)

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        ch.setLevel(logging.DEBUG)

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger

def _extract(url, local_file):
    logger = _logging()
    logger.info("Requesting {}".format(url))

    r = requests.get(url, stream=True)

    with open(local_file, 'wb') as fh:
        for chunk in r.iter_content(chunk_size=1024 * 1024 * 10):
            if chunk:
                fh.write(chunk)

    logger.info("Finished Downloading {}".format(url))

    return local_file


def _transform(local_file, **kwargs):
    logger = _logging()
    filename, _ = os.path.splitext(os.path.basename(local_file))
    transformed_file = os.path.join(DIRECTORY, "{}.tiff".format(filename))

    logger.info("Transforming {}".format(local_file))

    hdf2tif(local_file, transformed_file, **kwargs)

    logger.info("Finished transforming {}".format(transformed_file))

    return transformed_file


def _load(transformed_file, s3_bucket_name):
    logger = _logging()
    # Upload to S3
    # See: http://boto.cloudhackers.com/en/latest/s3_tut.html
    transformed_size = os.stat(transformed_file).st_size
    logger.info("Uploading {} to '{}' s3 bucket.".format(
        os.path.basename(transformed_file), s3_bucket_name))

    # For extreme boto level debugging
    # boto.set_stream_logger('boto')

    conn = boto.connect_s3()
    try:
        bucket = conn.get_bucket(s3_bucket_name)
    except boto.exception.S3ResponseError:
        bucket = conn.create_bucket(s3_bucket_name)

    mp = bucket.initiate_multipart_upload(
        os.path.basename(transformed_file))

    try:
        chunk_size = 1024 * 1024 * 10
        chunk_count = int(math.ceil(transformed_size / float(chunk_size)))

        for i in range(chunk_count):
            offset = chunk_size * i
            bytes = min(chunk_size, transformed_size - offset)
            with FileChunkIO(transformed_file, 'r', offset=offset,
                             bytes=bytes) as fp:
                mp.upload_part_from_file(fp, part_num=i + 1)
            logger.debug("Uploaded chunk {}".format(i + 1))
        mp.complete_upload()

        logger.info("Finished uploading {}".format(transformed_file))

        # Cleanup
        try:
            os.remove(transformed_file)
        except OSError:
            pass

    except Exception as e:
        mp.cancel_upload()
        raise e


@app.task(bind=True, default_retry_delay=10,
          max_retries=3, acks_late=True)
def etl(task, url, s3_bucket_name,
        extract=True, transform=True, load=True, **kwargs):
    try:
        os.makedirs(DIRECTORY)
    except OSError:
        pass


    logger = _logging()
    logger.info("Starting new ETL task for {}".format(os.path.basename(url)))


    try:
        # Download the file to local_file
        local_file = os.path.join(DIRECTORY, os.path.basename(url))

        if extract:
            _extract(url, local_file)

        if transform:
            transformed_file = _transform(local_file, **kwargs)
        else:
            transformed_file = local_file

        if load:
            _load(transformed_file, s3_bucket_name)

        # Cleanup
        try:
            os.remove(local_file)
        except OSError:
            pass


    except Exception as exc:
        raise task.retry(exc=exc)


if __name__ == "__main__":
    etl("http://gweld-download.cr.usgs.gov/collections/weld.global.annual.2011.v3.0/L57.Globe.annual.2011.hh18vv02.h2v3.doy097to281.NBAR.v3.0.hdf",
        "kitware-weld-tiff-etl", clobber=True, bands=(9,))
