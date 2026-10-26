FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
# Force install pyarrow directly to bypass caching issues
RUN pip install --no-cache-dir pyarrow
COPY extract_load.py .
CMD ["python", "extract_load.py"]