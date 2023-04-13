FROM python:3.10-slim-bullseye

RUN apt -qqy update \
  && apt install --no-install-recommends -qqy \ 
    fuse3 \
    davfs2 \
    libfuse2 \
    libcurl4 \
    libjansson4 \
    tini \
    sudo \
  && apt install --no-install-recommends -qqy \
    libjansson-dev \
    libfuse3-dev \
    libcurl4-openssl-dev \
    git \
    pkg-config \
    build-essential \
  && git clone https://github.com/data-exp-lab/girderfs_passthrough.git /tmp/passthrough \
  && cd /tmp/passthrough \
  && make \
  && make install \
  && cd / \
  && rm -rf /tmp/passthrough \
  && apt remove -qqy \
    libjansson-dev \
    libcurl4-openssl-dev \
    git \
    pkg-config \
    build-essential \
  && apt autoremove -qqy \
  && apt -qqy clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
  && echo "user_allow_other" >> /etc/fuse.conf

COPY . /app
WORKDIR /app

RUN pip install --no-cache-dir . && pip install tornado jsonschema --no-cache-dir
ENTRYPOINT ["/usr/bin/tini", "--", "girderfs-server"]
