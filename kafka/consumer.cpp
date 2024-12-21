// g++ -o kafka_consumer kafka_consumer.cpp -lrdkafka++ -lrdkafka

#include <iostream>
#include <string>
#include <vector>
#include <librdkafka/rdkafkacpp.h>
#include <librdkafka/rdkafka.h>  // 添加这一行用于分区分配策略相关定义

//定义了一个名为ConsumerEventCb的类，它继承自RdKafka::EventCb。这个类用于处理 Kafka 相关的事件回调
class ConsumerEventCb : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event &event) override {
        std::cerr << "Kafka event: " << event.str() << std::endl;
    }
};

//处理消费者的重平衡回调
class ConsumerRebalanceCb : public RdKafka::RebalanceCb {
public:
    //rebalance_cb函数重写了基类的虚函数，在消费者组内的分区分配或回收等重平衡情况发生时被调用
    void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                      RdKafka::ErrorCode err,
                      std::vector<RdKafka::TopicPartition*> &partitions) override {
        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
            std::cout << "Assigning partitions" << std::endl;
            //consumer->assign(partitions)来让消费者开始处理这些新分配的分区
            consumer->assign(partitions);  // 使用指针类型传递
        } else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
            std::cout << "Revoking partitions" << std::endl;
            consumer->unassign();
        } else {
            std::cerr << "Rebalance error: " << RdKafka::err2str(err) << std::endl;
        }
    }
};

//消费者消息处理函数
void msg_consume(RdKafka::Message *msg,void *opaque){
    switch(msg->err()){
        case RdKafka::ERR__TIMED_OUT:
            // 超时错误处理，避免每次超时都输出信息
            // 如果没有消息且没有新消息，打印一次超时信息而不是每次都打印
            static  bool firstTimeout = true;
            if (firstTimeout) {
                std::cerr << "Consumer error: " << msg->errstr() << std::endl;
                firstTimeout = false; // 只打印一次
            }
            break;
        
        case RdKafka::ERR_NO_ERROR:
            // 打印消息信息
            std::cout << "Message in -> topic: " << msg->topic_name()
                      << ", partition: [" << msg->partition() << "] at offset "
                      << msg->offset() << " key: ";
            if (msg->key()) {
                std::cout << msg->key();
            } else {
                std::cout << "NULL";
            }
            std::cout << " payload: " << std::string((char*)msg->payload(), msg->len()) << std::endl;
            break;

        default:
            std::cerr << "Consumer error: " << msg->errstr() << std::endl;
            break;
    }
}

int main(){
    std::string brokers = "192.168.186.138:9092"; // Kafka broker地址
    std::string groupId = "my_consumer_group"; // Kafka消费者组ID
    std::vector<std::string> topics = {"test_topic"}; // 订阅的主题
    std::string errorStr;

    // 创建配置对象
    RdKafka::Conf *config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *topicConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    if (!config ||!topicConfig) {
        std::cerr << "Failed to create Kafka configuration objects!" << std::endl;
        return -1;
    }

    std::string partitionAssignStrategy = "round_robin";  // 定义分区分配策略为round-robin
    // 设置分区分配策略配置项
    if (config->set("partition.assignment.strategy", partitionAssignStrategy, errorStr)!= RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set partition assignment strategy: " << errorStr << std::endl;
        return -1;
    }

    // 设置事件回调
    ConsumerEventCb *eventCb = new ConsumerEventCb();
    if (config->set("event_cb", eventCb, errorStr)!= RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set event callback: " << errorStr << std::endl;
        return -1;
    }

    // 设置重平衡回调
    ConsumerRebalanceCb *rebalanceCb = new ConsumerRebalanceCb();
    if (config->set("rebalance_cb", rebalanceCb, errorStr)!= RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set rebalance callback: " << errorStr << std::endl;
        return -1;
    }

    // 设置消费者组ID
    if (config->set("group.id", groupId, errorStr)!= RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set group.id: " << errorStr << std::endl;
        return -1;
    }

    // 设置broker地址
    if (config->set("bootstrap.servers", brokers, errorStr)!= RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set bootstrap.servers: " << errorStr << std::endl;
        return -1;
    }

    // 设置自动偏移重置（从最新的消息开始消费）
    if (topicConfig->set("auto.offset.reset", "latest", errorStr)!= RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set topic configuration: " << errorStr << std::endl;
        return -1;
    }

    // 设置默认的Topic配置
    if (config->set("default_topic_conf", topicConfig, errorStr)!= RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set default_topic_conf: " << errorStr << std::endl;
        return -1;
    }

    //创建消费者实例
    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(config,errorStr);
    if (!consumer) {
        std::cerr << "Failed to create KafkaConsumer: " << errorStr << std::endl;
        return -1;
    }

    std::cout << "Created consumer " << consumer->name() << std::endl;

    //订阅主题
    RdKafka::ErrorCode errorCode = consumer->subscribe(topics);
    if (errorCode!= RdKafka::ERR_NO_ERROR) {
        std::cerr << "Failed to subscribe to topics: " << RdKafka::err2str(errorCode) << std::endl;
        return -1;
    }

    //消费消息循环
    while(true){
        RdKafka::Message *msg = consumer->consume(1000);  //1000ms超时
        msg_consume(msg,nullptr);
        delete msg;
    }

    // 关闭消费者
    consumer->close();
    delete consumer;
    return 0;
}