FROM python:3.12

RUN useradd -m appuser
USER appuser

VOLUME /home/appuser/app .
WORKDIR /home/appuser/app

RUN pip install pipenv
RUN pipenv install --deploy --ignore-pipfile
CMD ["pipenv", "run", "python", "./geolfond.py", "oil-reestr"]
