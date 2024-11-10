FROM python:3.12

WORKDIR /opt/railgun_app
COPY ./requirements.txt /opt/railgun_app/
RUN pip install -r /opt/railgun_app/requirements.txt
EXPOSE 8888

COPY ./app /opt/railgun_app/

CMD ["uvicorn", "main:railgun_app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8888"]
