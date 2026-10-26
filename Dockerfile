FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir "google-cloud-bigquery[pandas]" pyarrow
RUN pip list
COPY extract_load.py .
CMD ["python", "extract_load.py"]