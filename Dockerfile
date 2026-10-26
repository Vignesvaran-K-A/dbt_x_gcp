FROM python:3.12
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingestion script
COPY extract_load.py .

# Copy the dbt project and profiles.yml into the expected directory
COPY nyc_transform /app/dbt
COPY profiles.yml /app/dbt/

CMD ["python", "extract_load.py"]