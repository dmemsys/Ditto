#!/bin/bash

mode="$1"
ubuntu_version=$(lsb_release -r -s)

if [ $ubuntu_version == "18.04" ]; then
  wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-5.1.0.0/MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz
  mv MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz ofed.tgz
elif [ $ubuntu_version == "20.04" ]; then
  wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-5.1.0.0/MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu20.04-x86_64.tgz
  mv MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu20.04-x86_64.tgz ofed.tgz
else
  echo "Wrong ubuntu distribution for $mode!"
  exit 0
fi
echo $mode $ubuntu_version $ofed_fid

sudo apt update -y

# install anaconda
if [ ! -d "./install" ]; then
  mkdir install
fi
mkdir install
mv ofed.tgz install

cd install
if [ ! -f "./anaconda-install.sh" ]; then
  wget https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-x86_64.sh -O anaconda-install.sh
fi
if [ ! -d "$HOME/anaconda3" ]; then
  chmod +x anaconda-install.sh
  ./anaconda-install.sh -b
  export PATH=$PATH:$HOME/anaconda3/bin
  # add conda to path
  # activate base
fi
echo PATH=$PATH:$HOME/anaconda3/bin >> $HOME/.bashrc
source ~/.bashrc
conda init bash
source ~/.bashrc
conda activate base
cd ..

pip install gdown python-memcached fabric
sudo apt install libmemcached-dev memcached libboost-all-dev -y

# install ofed
cd install
if [ ! -d "./ofed" ]; then
  tar zxf ofed.tgz
  mv MLNX* ofed
fi
cd ofed
sudo ./mlnxofedinstall --force
if [ $mode == "scalestore" ]; then
  sudo /etc/init.d/openibd restart
fi
cd ..

# install cmake
cd install
if [ ! -f cmake-3.16.8.tar.gz ]; then
  wget https://cmake.org/files/v3.16/cmake-3.16.8.tar.gz
fi
if [ ! -d "./cmake-3.16.8" ]; then
  tar zxf cmake-3.16.8.tar.gz
  cd cmake-3.16.8 && ./configure && make -j 4 && sudo make install
fi
cd ..

# install redis
sudo apt update -y
sudo apt install lsb-release -y
curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list
sudo apt-get update -y
sudo apt-get install redis -y

# install hiredis
sudo apt install libhiredis-dev -y

# install redis++
if [ ! -d "third_party" ]; then
  mkdir third_party
fi
cd third_party
git clone https://github.com/sewenew/redis-plus-plus.git
cd redis-plus-plus
if [ ! -d "build" ]; then
  mkdir build
fi
cd build
cmake ..
make -j 8
sudo make install
cd .. # -> redis-plus-plus
cd .. # -> third_party
cd .. # -> install

# install gtest
if [ ! -d "/usr/src/gtest" ]; then
  sudo apt install -y libgtest-dev
fi
cd /usr/src/gtest
sudo cmake .
sudo make
sudo make install

# install oh-my-zsh
if [ ! -d '~/.oh-my-zsh' ]; then
  sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended
fi
echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/usr/local/lib" >> ~/.zshrc
echo "ulimit -n unlimited" >> ~/.zshrc
conda init zsh

# setup-disk-part
# use `sudo fdisk /dev/sda` to first delete all the partition
# then carefully recreate it with a larger size at the same position
echo "p
d

d

d

d

d

n
p
1



No
p
w
" | sudo fdisk /dev/sda

# chsh
sudo chsh $USER -s /bin/zsh

# mkdir build
cd ~/Ditto
if [ ! -d "build" ]; then
  mkdir build
fi