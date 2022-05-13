FROM debian:buster

MAINTAINER Roman Leonhardt <roman.leonhardt@zamg.ac.at>
LABEL geomagpy.magpy.version=1.0.4

# update os
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        wget \
        make && \
    apt-get clean


# install conda
ENV PATH="/conda/bin":$PATH
ARG PATH="/conda/bin":$PATH

RUN echo 'export PATH=/conda/bin:$PATH' > /etc/profile.d/conda.sh && \
    wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && bash Miniconda3-latest-Linux-x86_64.sh -b -p /conda \
    && rm -f Miniconda3-latest-Linux-x86_64.sh 

# install packages and dependencies via conda
RUN conda --version  && \
    conda install --yes jupyter scipy matplotlib  && \
    conda clean -i -t -y

RUN useradd \
        -c 'Docker image user' \
        -m \
        -r \
        -s /sbin/nologin \
         magpyuser && \
    mkdir -p /home/magpyuser/notebooks && \
    mkdir -p /home/magpyuser/.conda && \
    chown -R magpyuser:magpyuser /home/magpyuser


# copy library (ignores set in .dockerignore)
COPY . /magpy


# install magpy
RUN cd /tmp && \ 
    pip install geomagpy && \
    cd /tmp

USER magpyuser

WORKDIR /home/magpyuser
EXPOSE 80
# entrypoint needs double quotes
ENTRYPOINT [ "/magpy/docker-entrypoint.sh" ]
