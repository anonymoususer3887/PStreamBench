# Install python 3.10, java 11 openjdk
sudo mkdir /home/playground
sudo /usr/local/etc/emulab/mkextrafs.pl -f /home/playground
sudo mkdir /home/playground/kafka-logs
sudo mkdir /home/playground/zip
sudo mkdir -p ~/.kaggle
cp /local/repository/kaggle.json ~/.kaggle
chmod 600 ~/.kaggle/kaggle.json
# When this script is run as `sudo bash pre_conditioning.sh`, everything above
# (the freshly-mounted /home/playground tree and ~/.kaggle, even though `~`
# still expands to the invoking user's home) ends up owned by root -- later,
# unprivileged steps (e.g. download_datasets.sh's `kaggle datasets download`)
# can't write/read them. Hand ownership back to whoever invoked sudo. Services
# that need root (Kafka, Spark, ...) are started with their own explicit sudo
# in the Ansible playbooks regardless of file ownership, so this is safe.
if [ -n "$SUDO_USER" ]; then
    sudo chown -R "$SUDO_USER" ~/.kaggle /home/playground
fi
# EXPORT variables for custom producer
sudo update-locale LC_ALL=C.UTF-8
sudo update-locale LANG=C.UTF-8
# Install python3.10 and pip3
sudo apt update && sudo apt upgrade --yes
sudo apt autoremove --yes
sudo dpkg --configure -a --force-depends --yes
sudo apt-get -f install --yes
sudo apt install software-properties-common --yes
sudo add-apt-repository ppa:deadsnakes/ppa --yes
sudo apt install  python3.10 --yes
sudo apt-get install python3-pip python3-venv --yes
sudo apt install unzip --yes
# Install java
sudo apt-get install openjdk-11-jdk --yes
# Kafka-producer + Kaggle dependencies always go into a dedicated venv at
# /home/playground/venv, never system/global pip (see
# feedback_use_venv_for_python project memory) -- referenced by its full
# path wherever custom_producer.py or kaggle is invoked on this node (see
# tasks/producer/producer-run.yaml and download_datasets.sh).
python3 -m venv /home/playground/venv
/home/playground/venv/bin/pip install --quiet --upgrade pip setuptools
/home/playground/venv/bin/pip install --quiet click openpyxl confluent-kafka pandas kaggle
# This whole script runs under one `sudo`, so the venv just created above is
# root-owned like everything else was before the earlier chown -- hand it
# back too, same reasoning as that step.
if [ -n "$SUDO_USER" ]; then
    sudo chown -R "$SUDO_USER" /home/playground/venv
fi
