FROM public.ecr.aws/docker/library/python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd --create-home app
USER app
CMD ["python", "-m", "stockstream.producer.main"]
