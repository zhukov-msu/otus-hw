sudo apt-get -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" dist-upgrade
sudo apt-get update -y && sudo apt-get upgrade -y
sudo apt-get install -y git
python -m venv .venv
source ./.venv/bin/activate
# pip install pyspark==3.0.2
python -m pip install --upgrade pip
pip install -r requirements.txt
nohup jupyter notebook --no-browser --port=9999 > jup.log &
hdfs dfs -mkdir /user/data
hadoop distcp s3a://otus-hw-bucket/* hdfs:///user/data
sudo apt-get install -y s3cmd
pip install -U requests mlflow urllib3 botocore boto3