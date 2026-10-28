FROM python:3.13-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/dataquery-cnpj

RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --create-home app

COPY query/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install -r /tmp/requirements.txt

COPY src ./src
COPY schema.py utils.py read_schema.py conversor.py ./

USER app

EXPOSE 8000

CMD ["uvicorn", "src.query.web.app:app", "--host", "0.0.0.0", "--port", "8000"]


FROM base AS test

USER root
COPY query/requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install -r /tmp/requirements-dev.txt

COPY tests ./tests
COPY pytest.ini ./

USER app

CMD ["pytest", "tests/query", "-q"]
