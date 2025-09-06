#!/bin/bash
export NGROK_AUTHTOKEN=2vls36KLZYno3gHbaO7yr82hTl6_6TBmA8hFU3bsPK79gAupy
# install and configure docker on the ec2 instance
sudo yum update -y

sudo amazon-linux-extras install epel -y
sudo yum install docker -y
sudo amazon-linux-extras install docker -y
sudo service docker start
sudo systemctl enable docker
sudo yum install htop


# install docker-compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

sudo chmod +x /usr/local/bin/docker-compose


# create a dockerfile
sudo chown $USER /var/run/docker.sock

# create directories
mkdir /home/ec2-user/n8n_data

sudo chown -R 1000:1000 /home/ec2-user/n8n_data

# create directories
mkdir /home/ec2-user/db-data

sudo chown -R 999:999 /home/ec2-user/db-data

cd /home/ec2-user

docker-compose up -d