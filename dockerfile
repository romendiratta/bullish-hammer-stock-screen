FROM python:3.8-slim
USER root
WORKDIR /app
COPY ./screener.py /app
COPY ./tickers.csv /app
COPY ./requirements.txt /app
RUN pip install -r requirements.txt
CMD ["python", "screener.py"]
