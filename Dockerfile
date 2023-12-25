FROM python:bullseye
WORKDIR /usr/src/app
COPY . .

RUN pip install pipenv && pipenv install  --deploy --ignore-pipfile && mkdir data
CMD ["pipenv", "run", "python", "./geolfond.py"]
