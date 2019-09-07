#!/bin/bash
sudo timedatectl set-timezone UTC
sudo apt update -y
sudo apt install -y python3-pip
cd /home/ubuntu
wget https://raw.githubusercontent.com/internet-scholar/youtube/master/requirements.txt
wget https://raw.githubusercontent.com/internet-scholar/youtube/master/youtube.py
wget https://raw.githubusercontent.com/internet-scholar/internet_scholar/master/requirements.txt -O requirements2.txt
wget https://raw.githubusercontent.com/internet-scholar/internet_scholar/master/internet_scholar.py
pip3 install --trusted-host pypi.python.org -r /home/ubuntu/requirements.txt
pip3 install --trusted-host pypi.python.org -r /home/ubuntu/requirements2.txt
python3 /home/ubuntu/youtube.py -c $1 && sudo shutdown -h now