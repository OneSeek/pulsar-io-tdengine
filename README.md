1. 启动tdengine服务
```shell script
sudo service taosd start
```
创建表

```sql
create database db;
create table db.test_jdbc(ts timestamp,message binary(255));
```
2. 启动单机模式 Pulsar
```shell script
./bin/pulsar standalone
```
3. 执行
```shell script
~/${apache-pulsar-2.6.1安装目录}/bin/pulsar-admin sinks localrun \
--archive ${Sink connector目录}/pulsar-io-tdengine-0.2-SNAPSHOT.nar \
--tenant public --namespace default \
--inputs test-tdengine \
--name pulsar-io-tdengine \
--sink-config-file ${sink配置文件目录}/tdengine-sink-config.yaml \
--parallelism 1
```
