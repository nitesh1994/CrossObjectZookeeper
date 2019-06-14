# CrossObjectZookeeper
Coordinate cross object transaction in Ceph using Apache Zookeeper

To start zookeeper server run.

cd bin/
./zkServer.sh start

# zookeeper will be started

cd src/c
./cli 127.0.0.1:2181 cmd:"help"  ==> prints all the commands which are supported by zookeeper client
./cli 127.0.0.1:2181 cmd:"create /TableT.busy" => creates a new znode
