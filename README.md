# How to run in locally:
1. Create and activate a virtual envinronment:
```
python3 -m venv venv
source venv/bin/activate
```

2. Install requirements:
```
pip install -r requirements.txt
```

3. Run the command:
```
python run.py --fields date,campaign,clicks
```

4. Run the tests:
```
pytest -W ignore tests.py
```

# How to run the app in Docker:
1. Build the docker image:
```
docker build -t cli_app .
```
    
2. Run the docker container. This command will launch the tests and fetch 3 columns from the file:
```
docker run --rm -it cli_app
```
