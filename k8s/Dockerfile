FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive

ENV TZ=UTC
RUN apt-get update && \
    BUILD_DEPS="autoconf bison build-essential libssl-dev libreadline6-dev libncurses5-dev libffi-dev libgdbm-dev libdb-dev git" && \
    DEBIAN_FRONTEND="noninteractive" apt-get install --yes --no-install-recommends \
        $BUILD_DEPS \
        libyaml-dev \
        python3-setuptools \
        python3-pip \
        python3-idna \
        python3-aiohttp \
        python3-wrapt \
        supervisor

# installs Python 3.10
RUN apt upgrade -y --no-install-recommends
RUN apt-get remove python3-pip python3-setuptools -y
RUN apt install software-properties-common make curl vim -y
RUN add-apt-repository ppa:deadsnakes/ppa
RUN TZ=UTC apt install --no-install-recommends python3.10 python3.10-dev -y
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

RUN pip install --upgrade pip
# RUN pip install supervisor

# COPY . /app
# RUN mv /app/k8s/stackpack/config/ /app/config
# RUN mkdir /app/share
# RUN mkdir /app/logs

RUN pip install --upgrade aiohttp
RUN pip install git+https://github.com/elastic/connectors-python.git@8.6



#  CMD ["supervisord", "-c", "/app/config/supervisord.conf"]

# RUN apt-get install \
#     ca-certificates \
#     curl \
#     gnupg \
#     lsb-release
# RUN mkdir -p /etc/apt/keyrings
# RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
# RUN echo \
#   "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
#   $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

# RUN apt-get update

# RUN apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin -y
# RUN cur -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
# RUN chmod +x /usr/local/bin/docker-compose


# RUN rm -rf /app/bin; rm -rf /app/lib; rm -rf /app/include
# RUN apt-get install python3.10-venv -y
#RUN cd app; bash /usr/local/bin/python3.10 -m venv .; bin/pip install -e .
