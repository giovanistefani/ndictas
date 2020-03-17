ARG BASE_CONTAINER=jupyter/pyspark-notebook
FROM $BASE_CONTAINER

LABEL maintainer="Softplan <dictas@softplan.com.br>"

USER root
RUN apt-get -qq update && apt-get install -y --no-install-recommends apt-utils openssh-client

USER $NB_UID

WORKDIR /home/giovani/Documentos/jupyter/

ARG SSH_PRIVATE_KEY
RUN mkdir -p ~/.ssh/
RUN echo "$SSH_PRIVATE_KEY" > ~/.ssh/id_rsa
RUN chmod 600 ~/.ssh/id_rsa
RUN touch ~/.ssh/known_hosts
RUN ssh-keyscan github.com >> ~/.ssh/known_hosts

ADD requirements.txt .
RUN pip install  -r requirements.txt
RUN pip install  pytest pytest-cov

RUN rm ~/.ssh/id_rsa
