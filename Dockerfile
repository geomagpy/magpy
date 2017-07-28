FROM debian:jessie

MAINTAINER Roman Leonhardt <roman.leonhardt@zamg.ac.at>
LABEL geomagpy.magpy.version=0.3.95

# update os
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
        bzip2 \
        ca-certificates \
        curl \
        wget \
        gcc \
        gfortran \
        libcurl4-gnutls-dev \
        libglib2.0-0 \
        libgnutls28-dev \
        libncurses5 \
        libncurses5-dev \
        libsm6 \
        libxext6 \
        libxrender1 \
        make && \
    apt-get clean


# install conda
ENV PATH /conda/bin:$PATH
RUN echo 'export PATH=/conda/bin:$PATH' > /etc/profile.d/conda.sh && \
    curl https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh \
        -o ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /conda && \
    rm ~/miniconda.sh


# install obspy and dependencies via conda
RUN conda config --add channels obspy && \
    conda install --yes jupyter mysql-python obspy && \
    conda clean -i -l -t -y && \
    useradd \
        -c 'Docker image user' \
        -m \
        -r \
        -s /sbin/nologin \
         magpy_user && \
    mkdir -p /home/magpy_user/notebooks && \
    chown -R magpy_user:magpy_user /home/magpy_user


# copy library (ignores set in .dockerignore)
COPY . /magpy


# install cdf, spacepy, and magpy
RUN cd /tmp && \ 
    wget http://cdaweb.gsfc.nasa.gov/pub/software/cdf/dist/cdf36_4/linux/cdf36_4-dist-all.tar.gz && \
    tar zxvf cdf36_4-dist-all.tar.gz && \
    cd cdf36* && \
    make OS=linux ENV=gnu CURSES=yes FORTRAN=no UCOPTIONS=-O2 SHARED=yes all && \
    make INSTALLDIR=/usr/local/cdf install && \
    cd /tmp && \
    pip install spacepy && \
    pip install geomagpy && \
    cd /tmp && \
    rm -rf /tmp/cdf36*


USER magpy_user

WORKDIR /home/magpy_user
EXPOSE 80
# entrypoint needs double quotes
ENTRYPOINT [ "/magpy/docker-entrypoint.sh" ]
