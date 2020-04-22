FROM python:3.8-buster

RUN apt-get update && \
    apt-get install -y \
    vim-tiny
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --user \
    setuptools wheel twine 

WORKDIR /app

CMD [ "tail", "-f", "/dev/null" ]
