# Domo Stream Uploader

```
usage: domo-stream-uploader [-h] [--logging-config LOGGING_CONFIG] -u
                            CLIENT_ID -p CLIENT_SECRET
                            {create,import,cancel} ...

Upload a CSV file to Domo in parallel chunks

positional arguments:
  {create,import,cancel}
    create              create a new API dataset using a previously created
                        dataset as a template
    import              import data into a previously created dataset
    cancel              cancel all stream executions

optional arguments:
  -h, --help            show this help message and exit
  --logging-config LOGGING_CONFIG
                        Logging configuration file

domo:
  Domo configuration options

  -u CLIENT_ID, --client-id CLIENT_ID
                        Domo Client ID
  -p CLIENT_SECRET, --client-secret CLIENT_SECRET
                        Domo Client secret
```

## General usage

1. Create a domo client ID and secret at https://developer.domo.com
1. Create a dataset with the schema you want (possibly import the first 100
   lines of your CSV). Record the dataset ID (from the browser address bar)
1. "Clone" the dataset with the stream uploader:
   `domo-stream-uploader -u <client id> -p <client secret> create -i <dataset id>`.
   Record the stream ID
1. Import your data:
   `domo-stream-uploader -u <client id> -p <client secret> import -i <stream id> <csv filename>`

## `create`
```
usage: domo-stream-uploader create [-h] -i DATASET_ID [--append | --replace]

optional arguments:
  -h, --help            show this help message and exit
  -i DATASET_ID, --dataset-id DATASET_ID
                        Domo dataset ID to clone

update method:
  How to update the dataset

  --append              Append the data to the dataset
  --replace             Replace all data in the dataset
```

## `import`
```
usage: domo-stream-uploader import [-h] -i STREAM_ID [-j JOBS] [-s SIZE]
                                   filename

positional arguments:
  filename              The CSV file to process and upload

optional arguments:
  -h, --help            show this help message and exit
  -i STREAM_ID, --stream-id STREAM_ID
                        Domo stream ID to upload to

concurrency:
  Job control

  -j JOBS, --jobs JOBS  Allow N jobs at once (default: 16)
  -s SIZE, --size SIZE  Size, in bytes of each chunk (default: 100000000 [100
                        MB])
```

## `cancel`
```
usage: domo-stream-uploader cancel [-h] -i STREAM_ID

optional arguments:
  -h, --help            show this help message and exit
  -i STREAM_ID, --stream-id STREAM_ID
                        Domo stream ID to cancel executions for
```
