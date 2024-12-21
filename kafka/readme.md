# 解压下载的文件
tar -xvzf apache-zookeeper-3.8.4-bin.tar.gz

# 将解压后的文件夹移动到 /opt/zookeeper
mv apache-zookeeper-3.8.4-bin /opt/zookeeper

cd /opt/zookeeper/conf
cp zoo_sample.cfg zoo.cfg

vim zoo.cfg

# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial synchronization phase can take
initLimit=10
# The number of ticks that can pass between sending a request and getting an acknowledgment
syncLimit=5
# The directory where the snapshot is stored.
dataDir=/opt/zookeeper/data
# The port at which the clients will connect
clientPort=2181

//启动zookeeper
/opt/zookeeper/bin/zkServer.sh start

/opt/zookeeper/bin/zkServer.sh status

//启动kafka
sh kafka-server-start.sh ../config/server.properties


//删除主题
./kafka-topics.sh --delete --topic test_topic --zookeeper 192.168.186.138:2181

//创建主题
./kafka-topics.sh --create --topic test_topic --zookeeper 192.168.186.138:2181 --replication-factor 1 --partitions 2
//查主题
./kafka-topics.sh --list --zookeeper 192.168.186.138:2181

//查看分区
./kafka-topics.sh --describe --topic test_topic --zookeeper 192.168.186.138:2181
