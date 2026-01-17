FROM python:3.13.11


# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/


WORKDIR /code
COPY pyproject.toml .python-version uv.lock ./


RUN uv sync --locked
ENV PATH="/code/.venv/bin:$PATH"
#  RUN pip install pandas pyarrow



COPY nyc_taxi_with_click.py .

ENTRYPOINT ["python", "nyc_taxi_with_click.py"]






#used it to tun psql in docker container

# docker run -d   --name pgdatabase   --network pg-network   -e POSTGRES_PASSWORD=root   -e POSTGRES_USER=root   -e POSTGRES_DB=ny_taxi   -p 5432:5432   -v nyc_taxi_data:/var/lib/postgresql   postgres:18

#after that i used this to run d container i built containinng script that is meant to connect to psql and load data the data.
# docker run -it \                                    
# --network pg-network \                                                                                            new:v005 \                        
#   --host pgdatabase \
#   --port 5432 \
#   --user root \
#   --password root \
#   --db ny_taxi \
#   --table yellow_taxi_trips