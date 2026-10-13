FROM python:3.11.11-slim
WORKDIR /project
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app app
COPY tests tests
USER 10001
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "app.worker"]
