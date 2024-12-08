FROM bde2020/spark-submit:3.3.0-hadoop3.3

LABEL maintainer="Your Name <your.email@example.com>"

RUN mkdir /app

RUN #rm /submit.sh
COPY ./submit.sh /

# Copy only the requirements file to /app
#COPY /big_data/src/runspark.sh  /

COPY /big_data/src/requirements.txt  /app/

# Install dependencies (if needed)
RUN pip3 install -r /app/requirements.txt

COPY ./runspark.sh /


# Copy all files from ./app in local to /app in container
COPY ./big_data/src /app

# Set the working directory to /app
WORKDIR /app

# Specify the command to run when the container starts
CMD ["/bin/bash", "/runspark.sh"]