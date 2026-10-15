FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY pipeline_tools/ pipeline_tools/
COPY generator/ generator/
COPY api/ api/
COPY contracts/ contracts/
RUN pip install --no-cache-dir .

USER nobody
