1. 启动tdengine服务
```shell script
sudo service taosd start
```
2. 启动单机模式 Pulsar
```shell script
./bin/pulsar standalone
```
3. 执行
```shell script
~/apache-pulsar-2.6.1/bin/pulsar-admin sinks localrun \
--archive ${}/pulsar-io-tdengine-0.2-SNAPSHOT.nar \
--tenant public --namespace default \
--inputs test-tdengine \
--name pulsar-io-tdengine \
--sink-config-file ${}/tdengine-sink-config.yaml \
--parallelism 1
```
