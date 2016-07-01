FROM ubuntu:16.04
MAINTAINER Tamaki Nishino <otamachan@gmail.com>

RUN apt-get update && apt-get install -y python python-boto3 python-yaml \
    && rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/*

COPY s3volume.py /s3volume.py

EXPOSE 8000

ENTRYPOINT ["python", "s3volume.py"]

