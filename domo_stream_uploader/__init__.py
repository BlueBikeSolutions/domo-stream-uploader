import argparse as ap
import csv
import datetime
import functools
import gzip
import io
import itertools
import logging
import logging.config
import multiprocessing as mp

import humanfriendly as hf
import requests

from requests.exceptions import RequestException


# TODO cache HTTP connections per-worker
# mp.current_process().name

class BytesIOWrapper(io.BytesIO):
    def write(self, value):
        if isinstance(value, str):
            value = value.encode()
        return super().write(value)


def new_chunk():
    stream = BytesIOWrapper()
    return stream, csv.writer(stream)


def chunk_csv(handle, chunk_bytes):
    reader = csv.reader(handle)
    stream, writer = new_chunk()
    for row in reader:
        writer.writerow(row)
        if len(stream.getbuffer()) >= chunk_bytes:
            yield stream
            stream, writer = new_chunk()
    yield stream


def is_http_nxx(resp, code_class):
    lower = code_class
    upper = code_class + 100

    return resp.status_code >= lower and resp.status_code < upper


def process_chunk(args):
    token, stream_id, exec_id, part_id, chunk = args

    starttime = datetime.datetime.now()

    logger = logging.getLogger('part.%s' % part_id)

    data = chunk.getvalue()
    full_size = len(data)
    #data = gzip.compress(data)
    comp_size = len(data)

    logger.info('Starting to upload %s (%s raw size)',
                hf.format_size(comp_size), hf.format_size(full_size),
               )
    logging.debug(data)

    retries = 10
    for retry_idx in range(retries):  # TODO configurable retries
        try:
            resp = requests.put(
                'https://api.domo.com/v1/streams/%s/executions/%s/part/%s' % (
                    stream_id, exec_id, part_id,
                ),
                headers = {'Authorization': 'Bearer ' + token,
                           'Content-Type': 'text/csv',
                           #'Content-Encoding': 'gzip',
                          },
            )

        except RequestException:
            logger.exception('Error uploading chunk (retry %s/%s)',
                             retry_idx + 1,
                             retries,
                            )
        else:
            if is_http_nxx(resp, 200):
                success = True
                break
            else:
                logger.error('Error uploading chunk (retry %s/%s): %s',
                             retry_idx + 1,
                             retries,
                             resp.text
                            )

    else:
        logger.error("Couldn't upload stream part after %s retries!", retries)
        success = False

    return part_id, datetime.datetime.now() - starttime, success


def import_cmd(logger, args, auth_data):
    starttime = datetime.datetime.now()

    with open(args.filename, 'rU') as handle:
        logger.debug('Creating stream execution')
        resp = requests.post(
            'https://api.domo.com/v1/streams/%s/executions' % args.stream_id,
            headers = {'Authorization': 'Bearer ' + auth_data['access_token']},
        )
        if not is_http_nxx(resp, 200):
            raise Exception("Couldn't create stream execution: " + resp.text)

        exec_data = resp.json()

        chunks = chunk_csv(handle, chunk_bytes = args.size)

        jobs = zip(
            itertools.repeat(auth_data['access_token']),
            itertools.repeat(args.stream_id),
            itertools.repeat(exec_data['id']),
            itertools.count(1),
            chunks,
        )
        avgtime = None
        with mp.Pool(args.jobs) as pool:
            for part_id, time, success in pool.imap_unordered(process_chunk, jobs):
                if not success:
                    logger.info('Cancelling stream execution')
                    resp = requests.put(
                        'https://api.domo.com/v1/streams/%s/executions/%s/abort' % (
                            args.stream_id,
                            exec_data['id'],
                        ),
                        headers = {'Authorization': 'Bearer ' + auth_data['access_token']},
                    )
                    if not is_http_nxx(resp, 200):
                        raise Exception("Failed to complete, AND couldn't abort stream execution: " + resp.text)

                    raise Exception('Failed to complete')

                if avgtime is None:
                    avgtime = time
                else:
                    avgtime = (avgtime + time) / 2
                logger.info('Chunk %s completed in %s (%s average)',
                            part_id, time, avgtime,
                           )
    logger.info('All chunks completed in %s', datetime.datetime.now() - starttime)
    logger.debug('Comming stream execution')
    resp = requests.put(
        'https://api.domo.com/v1/streams/%s/executions/%s/commit' % (
            args.stream_id,
            exec_data['id'],
        ),
        headers = {'Authorization': 'Bearer ' + auth_data['access_token']},
    )
    if not is_http_nxx(resp, 200):
        raise Exception("Failed to commmit: " + resp.text)


def create_cmd(logger, args, auth_data):
    logger.debug('Getting dataset definition')
    resp = requests.get(
        'https://api.domo.com/v1/datasets/%s' % args.dataset_id,
        headers = {'Authorization': 'Bearer ' + auth_data['access_token']},
    )
    if not is_http_nxx(resp, 200):
        raise Exception("Couldn't get dataset: " + resp.text)

    ds_data = resp.json()

    DOMO_COLUMNS = (
        '_BATCH_ID_',
        '_BATCH_LAST_RUN_',
    )
    ds_data['schema']['columns'] = [
        column for column in ds_data['schema']['columns']
        if column['name'] not in DOMO_COLUMNS
    ]

    resp = requests.post(
        'https://api.domo.com/v1/streams',
        headers = {'Authorization': 'Bearer ' + auth_data['access_token']},
        json = {
            'dataSet': {
                'name': ds_data['name'],
                'schema': ds_data['schema'],
            },
            'updateMethod': args.update_method,
        },
    )
    if not is_http_nxx(resp, 200):
        raise Exception("Couldn't create dataset: " + resp.text)

    stream_data = resp.json()

    logger.info('Created %s', stream_data['dataSet']['name'])
    logger.info('Data set ID: %s', stream_data['dataSet']['id'])
    logger.info('Stream ID: %s', stream_data['id'])


def cancel_cmd(logger, args, auth_data):
    limit = 500
    offset = 0
    while True:
        logger.debug('Getting executions page')
        resp = requests.get(
            'https://api.domo.com/v1/streams/%s/executions?limit=%s&offset=%s' % (
                args.stream_id,
                limit,
                offset,
            ),
            headers = {'Authorization': 'Bearer ' + auth_data['access_token']},
        )
        if not is_http_nxx(resp, 200):
            raise Exception("Couldn't get executions: " + resp.text)

        list_data = resp.json()

        for exec_data in list_data:
            if exec_data['currentState'].lower() == 'active':
                logger.info('Aborting execution %s', exec_data['id'])
                resp = requests.put(
                    'https://api.domo.com/v1/streams/%s/executions/%s/abort' % (
                        args.stream_id,
                        exec_data['id'],
                    ),
                    headers = {'Authorization': 'Bearer ' + auth_data['access_token']},
                )
                if not is_http_nxx(resp, 200):
                    raise Exception("Couldn't abort execution: " + resp.text)

        if len(list_data) < limit:
            break
        else:
            offset = list_data[-1]['id']


def main():
    args = parser.parse_args()

    if args.logging_config:
        logging.config.fileConfig(args.logging_config)
    else:
        logging.basicConfig(level = logging.DEBUG)

    logger = logging.getLogger('master')

    logger.debug('Getting Domo access token')
    resp = requests.get(
        'https://api.domo.com/oauth/token?grant_type=client_credentials&scope=data',
        auth = (args.client_id, args.client_secret),
    )
    if not is_http_nxx(resp, 200):
        raise Exception("Couldn't login: " + resp.text)

    auth_data = resp.json()
    logger.info('Logged into %s as %s',
                auth_data['domain'], auth_data['role'],
               )

    args.func(logger, args, auth_data)


JOBS_DEFAULT = mp.cpu_count() * 2
SIZE_DEFAULT = hf.parse_size('100MB')


### GLOBAL ARGS

parser = ap.ArgumentParser(description='Upload a CSV file to Domo in parallel chunks')
parser.add_argument('--logging-config',
                    help = "Logging configuration file",
                   )

domo_group = parser.add_argument_group('domo', 'Domo configuration options')
domo_group.add_argument('-u', '--client-id',
                        help = 'Domo Client ID',
                        required = True,
                       )
domo_group.add_argument('-p', '--client-secret',
                        help = 'Domo Client secret',
                        required = True,
                       )

subparsers = parser.add_subparsers()

### CREATE COMMAND
create_cmd_parser = subparsers.add_parser(
    'create',
    help = 'create a new API dataset using a previously created dataset as a '
           'template',
)
create_cmd_parser.set_defaults(func = create_cmd)

create_cmd_parser.add_argument('-i', '--dataset-id',
                               help = 'Domo dataset ID to clone',
                               required = True,
                              )

UPDATE_METHOD_DEFAULT = 'REPLACE'
update_method_group = create_cmd_parser.add_argument_group(
    'update method',
    'How to update the dataset',
).add_mutually_exclusive_group()
update_method_group.add_argument('--append',
                                 help = 'Append the data to the dataset',
                                 dest = 'update_method',
                                 action = 'store_const',
                                 const = 'APPEND',
                                 default = UPDATE_METHOD_DEFAULT,
                                )
update_method_group.add_argument('--replace',
                                 help = 'Replace all data in the dataset',
                                 dest = 'update_method',
                                 action = 'store_const',
                                 const = 'REPLACE',
                                 default = UPDATE_METHOD_DEFAULT,
                                )

### IMPORT COMMAND
import_cmd_parser = subparsers.add_parser('import',
                                          help = 'import data into a previously created dataset',
                                         )
import_cmd_parser.set_defaults(func = import_cmd)

import_cmd_parser.add_argument('-i', '--stream-id',
                               help = 'Domo stream ID to upload to',
                               required = True,
                              )

jobs_group = import_cmd_parser.add_argument_group('concurrency', 'Job control')
jobs_group.add_argument('-j', '--jobs',
                        help = "Allow N jobs at once (default: %s)" % JOBS_DEFAULT,
                        type = int,
                        default = JOBS_DEFAULT,
                       )
jobs_group.add_argument('-s', '--size',
                        help = "Size, in bytes of each chunk (default: %s [%s])" % (
                            SIZE_DEFAULT,
                            hf.format_size(SIZE_DEFAULT),
                        ),
                        type = int,
                        default = SIZE_DEFAULT,
                       )

import_cmd_parser.add_argument('filename',
                               help = "The CSV file to process and upload",
                              )

### CANCEL COMMAND
cancel_cmd_parser = subparsers.add_parser('cancel',
                                          help = 'cancel all stream executions',
                                         )
cancel_cmd_parser.set_defaults(func = cancel_cmd)

cancel_cmd_parser.add_argument('-i', '--stream-id',
                               help = 'Domo stream ID to cancel executions for',
                               type = int,
                               required = True,
                              )



if __name__ == '__main__':
    main()
