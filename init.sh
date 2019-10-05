#! /bin/bash

# setup miniconda for conda environments
curl -fL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o miniconda.sh
chmod u+x miniconda.sh 
./miniconda.sh -bfu
echo 'export PATH=$PATH:/root/miniconda3/bin/' >> ~/.bashrc
source ~/.bashrc

# install the basics
sudo apt-get update -y
sudo apt-get install gcc -y

# create conda environment for example
conda create -n ssh-example python=3.7 -y
git clone https://github.com/cicdw/ssh-etl-monitoring.git
cd ssh-etl-monitoring
source activate ssh-example
pip install -r requirements.txt
