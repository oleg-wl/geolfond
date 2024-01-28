FROM python:bullseye

RUN useradd -m appuser
USER appuser

VOLUME /home/appuser/app .
WORKDIR /home/appuser/app

RUN pip install -r requirements.txt
CMD ["python", "./geolfond.py"]
