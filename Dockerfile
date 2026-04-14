FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/Script1__fetch_items_from_source_id.py scripts/Script2__fetch_uuid_from_v1_and_v2.py scripts/Script3__patching_persitant_Ids.py ./

RUN mkdir /data

CMD ["sh", "-c", \
  "python Script1__fetch_items_from_source_id.py && \
   python Script2__fetch_uuid_from_v1_and_v2.py && \
   python Script3__patching_persitant_Ids.py"]