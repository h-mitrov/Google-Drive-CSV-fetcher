FROM python:3.10

RUN mkdir -p /wd

WORKDIR /wd

COPY requirements.txt ./requirements.txt

RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . ./

CMD pytest tests.py && python3 run.py --fields date,campaign,clicks
