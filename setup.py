""" Upload a CSV file to Domo in parallel chunks """

from setuptools import setup


setup(
    name = 'domo_stream_uploader',
    version = '0.1.0',
    author = 'Ricky Cook <ricky.cook@bluebike.com.au>',
    description = 'Upload a CSV file to Domo in parallel chunks',
    py_modules = 'domo_stream_uploader',
    entry_points = {
        'console_scripts': [
            'domo-stream-uploader=domo_stream_uploader:main'
        ]
    },
    install_requires = (
        'humanfriendly',
        'requests',
    ),
)
