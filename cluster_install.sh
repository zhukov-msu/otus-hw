sudo apt-get update -y && sudo apt-get upgrade -y
sudo apt-get install git
git clone https://github.com/zhukov-msu/otus-hw.git && cd otus-hw
python -m venv .venv
source ./.venv/bin/activate
pip install -r requirements.txt
nohup jupyter notebook --no-browser --port=9999 > jup.log &
hadoop distcp s3a://otus-hw-bucket/* hdfs:///user/data
sudo apt-get install s3cmd
#sudo apt-get install python3.10