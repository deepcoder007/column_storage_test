
FROM ubuntu:noble

RUN apt update
RUN apt -y install -V ca-certificates lsb-release wget
RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt update
RUN apt install -yV libarrow-dev libarrow-dataset-dev libarrow-acero-dev libarrow-flight-dev libgandiva-dev libparquet-dev
