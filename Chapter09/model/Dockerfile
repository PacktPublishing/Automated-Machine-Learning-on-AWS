FROM ubuntu:18.04

# prevent stopping by user interaction
ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true
ENV KMP_AFFINITY=granularity=fine,compact,1,0
ENV KMP_BLOCKTIME=1
ENV KMP_SETTINGS=0

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=UTF-8
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

ARG PYTHON=python3.7
ARG PYTHON_PIP=python3-pip
ARG PIP=pip3
ARG PYTHON_VERSION=3.7.10

ARG TF_URL=https://aws-tensorflow-binaries.s3-us-west-2.amazonaws.com/tensorflow/r2.3_aws/20210219-025624/cpu/py37/tensorflow_cpu-2.3.2-cp37-cp37m-manylinux2010_x86_64.whl

ARG ESTIMATOR_URL=https://aws-tensorflow-binaries.s3-us-west-2.amazonaws.com/estimator/r2.3_aws/20201211-182958/tensorflow_estimator-2.3.0-py2.py3-none-any.whl

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    openssh-client \
    openssh-server \
    ca-certificates \
    curl \
    emacs \
    git \
    libssl1.1 \
    libtemplate-perl \
    openssl \
    wget \
    vim \
    zlib1g-dev \
    # Install dependent library for OpenCV
    libgtk2.0-dev \
 && rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=/usr/local/openmpi/lib:$LD_LIBRARY_PATH
ENV PATH /usr/local/openmpi/bin/:$PATH

WORKDIR /

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    libbz2-dev \
    libc6-dev \
    libffi-dev \
    libgdbm-dev \
    liblzma-dev \
    libncursesw5-dev \
    libreadline-gplv2-dev \
    libsqlite3-dev \
    libssl-dev \
    tk-dev \
    nginx \
 && rm -rf /var/lib/apt/lists/* \
 && apt-get clean

RUN wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz \
 && tar -xvf Python-$PYTHON_VERSION.tgz \
 && cd Python-$PYTHON_VERSION \
 && ./configure && make && make install \
 && make && make install && rm -rf ../Python-$PYTHON_VERSION*

RUN ${PIP} --no-cache-dir install --upgrade \
    pip \
    setuptools

# Some TF tools expect a "python" binary
RUN ln -s $(which ${PYTHON}) /usr/local/bin/python \
 && ln -s $(which ${PIP}) /usr/bin/pip

RUN ${PIP} install --no-cache-dir -U \
    numpy==1.19.1 \
    scipy==1.5.2 \
    scikit-learn==0.23 \
    pandas==1.1 \
    Pillow==7.2.0 \
    python-dateutil==2.8.1 \
    # install PyYAML>=5.4.1 to avoid conflict with latest awscli
    "pyYAML>=5.4.1,<5.5" \
    requests==2.24.0 \
    "awscli<2" \
    opencv-python==4.3.0.36 \
    # Let's install TensorFlow separately in the end to avoid
    # the library version to be overwritten
    && ${PIP} install --no-cache-dir -U \
    ${TF_URL} \
    ${ESTIMATOR_URL} \
    h5py==2.10.0 \
    "absl-py>=0.9,<0.11" \
    werkzeug==1.0.1 \
    psutil==5.7.2 \
    flask \
    gevent \
    gunicorn \
    pyarrow==2 \
    awswrangler==2.7.0

RUN curl https://aws-dlc-licenses.s3.amazonaws.com/tensorflow-2.3/license.txt -o /license.txt
RUN mkdir -p /opt/program
RUN mkdir -p /opt/ml

COPY app.py /opt/program
COPY model.py /opt/program
COPY nginx.conf /opt/program
COPY wsgi.py /opt/program
WORKDIR /opt/program

EXPOSE 8080

ENTRYPOINT ["python", "app.py"]