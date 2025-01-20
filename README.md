# DTS
&ensp;&ensp;&ensp;&ensp;DTS基于mysql binlog的实时数据传输系统，支持数据获取、数据加工、数据发布。类似腾讯云和阿里云的DTS服务。
单进程单线程架构设计，在大流量的广告系统中，可以跑满千兆网卡， 由于在万兆网卡下测试的是历史数据， 瓶颈就归到了MySQL SSD磁盘的IO读写上。  
&ensp;&ensp;&ensp;&ensp;MySQL主从复制有两种方式：   
&ensp;&ensp;&ensp;&ensp;1、index+offset   
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;代码目前支持这种模式， offset模式在故障恢复时有一定概率出现数据丢失的情况。  
&ensp;&ensp;&ensp;&ensp;2、GTID（MySQL5.6+支持）  
&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;&ensp;代码升级后支持, GTID在MySQL出现异常，切换到其他数据库，可以进行精确的数据对齐，这个是offset方式不具备的。 

## MySQL主从复制原理  

<img width="651" alt="image" src="https://github.com/user-attachments/assets/f6ba04a6-c533-440d-8596-f7016ef40537" />  

<img width="1150" alt="image" src="https://github.com/user-attachments/assets/ade84927-928c-4c2a-869f-ad187fe2e0a0" />

## MySQL并行复制

<img width="1177" alt="image" src="https://github.com/user-attachments/assets/e16deaf6-b7bb-48a4-be63-02ad39a3399e" />

## MySQL Replication Format

<img width="760" alt="image" src="https://github.com/user-attachments/assets/d861c27b-a4a4-4cde-854f-41ac23f41cf1" />

## MySQL命令  

<img width="1089" alt="image" src="https://github.com/user-attachments/assets/b09da7f7-0db7-4f0a-aa0d-a6cf9f3e672e" />  

## MySQL Replication Protocol

<img width="1145" alt="image" src="https://github.com/user-attachments/assets/a642a79d-2f83-4756-8fad-339b116196a7" />

## MySQL Binlog Event  

<img width="1161" alt="image" src="https://github.com/user-attachments/assets/3708a50b-2a7d-4a79-91bd-b51b99d1dd1f" />

<img width="1062" alt="image" src="https://github.com/user-attachments/assets/910a547f-c23a-4d75-ab40-544deca7f443" />

## MySQL Switch Master During Failover 

<img width="1142" alt="image" src="https://github.com/user-attachments/assets/287e008c-2546-4693-b440-793290addf96" />

# 编译
## 编译工具cmake

```bash
git clone https://gitlab.kitware.com/cmake/cmake.git
./configure
make
make install  
```

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
### MySQL  
#### MySQL install  
[MySQL5.7.20 下载](https://downloads.mysql.com/archives/community/)  

#### MySQL config  
修改MySQL配置文件/etc/my.cnf   
```bash
[mysqld]  
binlog_format=ROW  
```
#### MySQL账号权限  
MySQL账号获取复制权限  
```bash
grant replication slave on *.* to 'dbuser'@'%';
#查看binlog index&offset  
show master status;  
```

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
# Star History

[![Star History Chart](https://api.star-history.com/svg?repos=isold23/DTS&type=Date)](https://star-history.com/#isold23/DTS&Date)

# Contributors  
<a href="https://github.com/isold23/DTS/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=isold23/DTS" />
</a>
