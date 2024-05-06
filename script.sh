#!/bin/bash

# get on the right directory
cd home/ec2-user

# install some dependencies
wget https://bootstrap.pypa.io/get-pip.py
python3 get-pip.py
pip install boto3

# install git
sudo yum install git -y

# install crontab
sudo yum install cronie -y
sudo systemctl enable crond.service
sudo systemctl start crond.service

# cloning repository
git clone https://github.com/vbombarda-aic/test_git_pull_ec2

## Create Crontab
# write out current crontab
sudo crontab -l > mycron
# echo new cron into cron file
echo "* * * * * cd /home/ec2-user/test_git_pull_ec2/ && sudo git pull" >> mycron
# install new cron file
crontab mycron
rm mycron


### Install Docker & Docker-Compose
sudo yum install docker

sudo yum install python3 python3-pip

sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

sudo chmod +x /usr/local/bin/docker-compose

sudo service docker start

# Configure AIRFLOW enviromental variables
ENV_CONTENT="AIRFLOW_UID=50000\nAIRFLOW_GID=0"

# Write content to .env file
sudo echo -e "$ENV_CONTENT" > .env
