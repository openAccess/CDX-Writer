FROM ubuntu:latest

MAINTAINER Vinay Goel <vinay@archive.org>

RUN apt-get update && apt-get install -y \
    python \
    python-pip \
    python-lxml \
    python-dev \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    build-essential \
    gcc \
    git

ADD requirements.txt /
ADD cdx_writer.py /

RUN pip install -r /requirements.txt

CMD ["bash"]

