FROM python:3.11.5
WORKDIR /usr/src/api_2
COPY . /usr/src/api_2
RUN pip install -r requirements.txt
EXPOSE 8001
CMD ["uvicorn", "api2:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]


