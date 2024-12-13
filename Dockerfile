# FROM python:latest
FROM ubuntu:latest
WORKDIR /project
COPY app /project
# COPY  output /project/output
#COPY .env /project


COPY requirements.txt /project/

RUN export DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && apt-get install -y tzdata
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
RUN dpkg-reconfigure --frontend noninteractive tzdata
RUN apt-get install -y curl
RUN apt install -y python3
RUN apt install -y --fix-missing pip

RUN apt-get update && apt-get install -y ca-certificates

RUN pip install --break-system-packages --no-cache-dir --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org -r /project/requirements.txt

#Install SQLServer Driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update -y
RUN apt install ca-certificates
RUN ACCEPT_EULA=Y apt-get install -y --fix-missing msodbcsql17


CMD ["python3", "main.py"]
