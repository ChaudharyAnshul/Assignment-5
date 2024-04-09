FROM python:3.11.5
WORKDIR /usr/src/ui
COPY . /usr/src/ui
RUN pip install -r requirements.txt
EXPOSE 8501
CMD ["streamlit", "run", "src/main.py"]


