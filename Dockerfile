# Use an official Ubuntu runtime as a parent image
FROM ubuntu:latest

# Set the working directory in the container
WORKDIR /project

# Copy the current directory contents into the container
COPY . /project

# Copy the requirements.txt file into the container
COPY requirements.txt /project/

# Set the environment variable to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update the package list and install tzdata
RUN apt-get update -y && apt-get install -y tzdata

# Set the timezone to America/New_York
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime && \
    dpkg-reconfigure --frontend noninteractive tzdata

# Install curl
RUN apt-get install -y curl

# Install Python 3 and pip
RUN apt-get install -y python3 && \
    apt-get install -y --fix-missing python3-pip

# Update the package list and install ca-certificates
RUN apt-get update && apt-get install -y ca-certificates

# Install Python dependencies
RUN pip3 install --break-system-packages --no-cache-dir --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org -r /project/requirements.txt

# Install SQL Server ODBC driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update -y && \
    ACCEPT_EULA=Y apt-get install -y --fix-missing msodbcsql17

# Run main.py when the container launches
CMD ["python3", "main.py"]