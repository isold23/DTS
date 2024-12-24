# DTS
  DTS基于mysql binlog的实时数据传输系统，支持数据获取、数据加工、数据发布。类似腾讯云和阿里云的DTS服务。
单进程单线程架构设计，在大流量的广告系统中，可以跑满千兆网卡， 由于在万兆网卡下测试的是历史数据， 瓶颈就归到了MySQL SSD磁盘的IO读写上。

# 编译
## 依赖库
### protobuf

```bash
git clone -b v3.20.0 https://github.com/protocolbuffers/protobuf.git
cd protobuf
git submodule update --init --recursive
./autogen.sh
./configure
make -j$(nproc) # $(nproc) ensures it uses all cores for compilation
make check
sudo make install
sudo ldconfig # refresh shared library cache.

[root@VM-0-11-centos protobuf]# protoc --version  
libprotoc 3.20.0-rc2  
```

### glog
```bash
git clone https://github.com/google/glog.git 
mkdir build 
cd build 
cmake .. 
make
make install  
```
### mysql
mysql

## cmake编译

```bash
cd ./src 
mkdir build  
cd ./build 
cmake .. 
make 
```
# 运行 

```bash  
./dts -f ./dts.xml
```  
