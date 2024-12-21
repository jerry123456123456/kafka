//g++ -o kafka_producer kafka_producer.cpp -lrdkafka++ -lrdkafka

#include <iostream>
#include <string>
#include <cstdlib>
#include <librdkafka/rdkafkacpp.h>

//这个类用于处理消息的传递报告回调
class ProducerDeliveryReportCb : public RdKafka::DeliveryReportCb{
public:
    //重写了基类中的纯虚函数dr_cb，它接收一个RdKafka::Message类型的非 const 引用作为参数
    void dr_cb(RdKafka::Message &message) override {
        if (message.err()) {
            std::cerr << "Message delivery failed: " << message.errstr() << std::endl;
        } else {
            std::cout << "Message delivered to topic " << message.topic_name()
                      << " [" << message.partition() << "] at offset " << message.offset() << std::endl;
        }
    }
};

//用于处理kafka相关事件的回调
class ProducerEventCb : public RdKafka::EventCb{
public:
    //：重写了基类的纯虚函数event_cb，当有 Kafka 事件发生时
    //，会传入一个RdKafka::Event类型的参数，在这个函数里，直接通过std::cerr输出事件的相关字符串描述（通过event.str()获取）
    //，用于提示一些 Kafka 运行过程中的事件情况，比如连接建立、断开等相关事件
    void event_cb(RdKafka::Event &event) override {
        std::cerr << "Kafka event: " <<event.str() << std::endl;
    }
};

int main(){
    std::string brokers = "192.168.186.138:9092";   
    std::string topicStr = "test_topic";
    std::string key = "key1";  //key: 消息的键，通常用于分区的计算。这里简单地设定为 "key1"

    //config全局的kafka配置对象，用于设置kafka生产者的配置
    RdKafka::Conf *config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    //topicConfig: 针对特定主题的配置对象，用于设置与 Kafka 主题相关的配置
    RdKafka::Conf *topicConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    if (!config || !topicConfig) {
        std::cerr << "Failed to create Kafka configuration objects!" << std::endl;
        return -1;
    }

    //设置回调函数
    //drCb: 消息发送的回调函数，负责报告生产消息的状态（成功或失败）
    ProducerDeliveryReportCb *drCb = new ProducerDeliveryReportCb();
    std::string errStr;
    //config->set("dr_cb", drCb, errStr): 将发送报告回调函数设置到配置中。如果设置失败，打印错误并退出
    if(config->set("dr_cb",drCb,errStr) != RdKafka::Conf::CONF_OK){
        std::cerr << "Failed to set delivery report callback: " << errStr << std::endl;
        return -1;
    }

    //eventCb: 用于处理 Kafka 事件的回调函数（例如连接错误等）
    ProducerEventCb *eventCb = new ProducerEventCb();
    if (config->set("event_cb", eventCb, errStr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set event callback: " << errStr << std::endl;
        return -1;
    }

    //bootstrap.servers: 指定 Kafka Broker 的地址。这个配置用于 Kafka 生产者连接到集群
    if (config->set("bootstrap.servers", brokers, errStr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set broker list: " << errStr << std::endl;
        return -1;
    }

    //使用配置创建kafka生产者实例
    RdKafka::Producer *producer = RdKafka::Producer::create(config,errStr);
    if (!producer) {
        std::cerr << "Failed to create Kafka producer: " << errStr << std::endl;
        return -1;
    }

    //创建 Kafka 主题对象。这是向 Kafka 生产消息时必须指定的目标主题
    RdKafka::Topic *topic = RdKafka::Topic::create(producer,topicStr,topicConfig,errStr);
    if (!topic) {
        std::cerr << "Failed to create Kafka topic: " << errStr << std::endl;
        return -1;
    }

    //消息生产循环
    std::string input;
    while(true){
        std::cout << "请输入要发送到Kafka的消息内容(输入'exit'退出) :";
        std::getline(std::cin,input);

        if(input == "exit"){
            break;
        }

        //调用procude函数将消息发送到Kafka
        RdKafka::ErrorCode resp = producer->produce(
            topic,                          //目标主题
            RdKafka::Topic::PARTITION_UA,   // Kafka 会根据消息键自动选择分区（PARTITION_UA 表示自动分配分区）
            RdKafka::Producer::RK_MSG_COPY, //指定消息复制类型，RK_MSG_COPY 表示消息的内容会被复制到 Kafka 内部
            input.data(),           //消息内容  
            input.size(),                   //消息长度
            &key,                           //消息的键，这里传入了一个字符串 key1，用于消息的分区计算
            nullptr                         //可选的指针，可以用于传递额外的回调数据
        );
        
        //如果消息生产失败，打印错误并继续循环
        if (resp != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Failed to produce message: " << RdKafka::err2str(resp) << std::endl;
            continue;
        }

        //如果生产成功，输出消息成功发送到指定主题的提示。
        std::cout << "Message produced to topic " << topicStr << std::endl;

        //flush(0): 请求 Kafka 生产者尽快发送消息，0 表示不等待太长时间，避免阻塞
        producer->flush(0);  // 尽快尝试刷新，让消息发送出去，这里参数0表示不等待太长时间，避免阻塞太久
    }    

    // Clean up
    delete topic;
    delete producer;

    return 0;
}