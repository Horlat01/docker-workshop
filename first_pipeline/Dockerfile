FROM python:3.13.11


# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/


WORKDIR /code
COPY pyproject.toml .python-version uv.lock ./


RUN uv sync --locked

#  RUN pip install pandas pyarrow



COPY main.py .

ENTRYPOINT ["uv", "run","python", "main.py"]
