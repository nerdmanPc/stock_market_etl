FROM python

WORKDIR /usr/app

COPY ./src ./src
COPY ./migration ./migration
RUN mkdir ./data

ENV ETL_WAREHOUSE_PATH=./data/warehouse.db

RUN python3 ./migration/migrate.py
RUN pip install requests

CMD [ "python3", "./src/av_update.py" ]