sudo apt-get update -y && sudo apt-get upgrade -y
sudo apt-get install -y git
sudo apt install -y python3.8-venv

git clone https://github.com/zhukov-msu/otus-hw.git && cd otus-hw
python3 -m venv .venv
source ./.venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt

sudo apt-get update -y
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update -y

sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

mkdir -p ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

#sudo docker compose up airflow-init

curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash
source .bashrc
yc init
#yc config profile create airflow
#yc config set folder-id b1gmi1gs1575jti9jgnl
