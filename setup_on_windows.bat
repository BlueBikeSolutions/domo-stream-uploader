### once off environment setup
pip install virtualenv
virtualenv python_env
python_env\scripts\pip install .

### run command 
python_env\scripts\python -m domo_stream_uploader --help