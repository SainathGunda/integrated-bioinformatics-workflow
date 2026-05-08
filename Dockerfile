# Uses a Python 3.13 Debian-based development image as the base image
FROM dhi.io/python:3.13-debian13-dev
# Sets the Airflow home directory inside the container
ENV AIRFLOW_HOME=/root/airflow
# Tells Airflow where to look for DAG files
ENV AIRFLOW__CORE__DAGS_FOLDER=/root/airflow/dags 
# Disables Airflow example DAGs so only custom DAGs are shown
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
# Tells Airflow to use SMTP for sending emails
ENV AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp
# Sets the sender email address for Airflow email notifications
ENV AIRFLOW__EMAIL__FROM_EMAIL="sainathgunda99@gmail.com"
# Configures the default SMTP connection for Airflow
# This is used by EmailOperator when conn_id="smtp_default"
ENV AIRFLOW_CONN_SMTP_DEFAULT="smtp://sainathgunda99%40gmail.com:riukpvqvihjlkpqx@smtp.gmail.com:587?disable_ssl=true&from_email=sainathgunda99%40gmail.com"

# Updates the package list so apt knows the latest available packages
RUN apt update
# Installs required Linux tools and libraries
RUN apt install -y \
    samtools bcftools gnuplot \
    build-essential zlib1g-dev \
    texlive-font-utils git
# Downloads and builds PSMC from GitHub
RUN git clone https://github.com/lh3/psmc.git /root/psmc && \
    cd /root/psmc && \
    make && \
    cd utils && \
    make
# Downloads and builds Dsuite from GitHub
RUN git clone https://github.com/millanek/Dsuite.git /root/Dsuite && \
    cd  /root/Dsuite && \
    make
# Sets /app as the working directory inside the container
WORKDIR /app
# Copies the requirements.txt file into the current working directory
COPY requirements.txt .
# Installs Python packages listed in requirements.txt
RUN pip install -r requirements.txt
# Creates the Airflow home folder and DAGs folder
RUN mkdir /root/airflow && mkdir /root/airflow/dags
# Copies the local dags folder into the Airflow DAGs folder inside the container
COPY dags /root/airflow/dags
# Starts Airflow in standalone mode when the container runs
CMD ["airflow", "standalone"]
