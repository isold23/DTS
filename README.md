# DTS
  DTS基于mysql binlog的实时数据传输系统，支持数据获取、数据加工、数据发布。类似腾讯云和阿里云的DTS服务。
单进程单线程架构设计，在大流量的广告系统中，可以跑满千兆网卡， 由于在万兆网卡下测试的是历史数据， 瓶颈就归到了MySQL SSD磁盘的IO读写上。

# 编译
## 依赖库
libprotoc 3.20.0-rc2  
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
