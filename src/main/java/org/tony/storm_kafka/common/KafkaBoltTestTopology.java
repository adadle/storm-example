package org.tony.storm_kafka.common;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.kafka.trident.TridentKafkaState;

import java.util.Properties;

/**
 * Created by TonyLee on 2015/1/23.
 * By IDEA
 */
public class KafkaBoltTestTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        //1. KafkaBolt的前置组件emit出来的(可以是spout也可以是bolt)
        Spout spout = new Spout(new Fields("key", "message"));
        builder.setSpout("spout", spout);

        //2. 给KafkaBolt配置topic及前置tuple消息到kafka的mapping关系
        KafkaBolt bolt = new KafkaBolt();
        bolt.withTopicSelector(new DefaultTopicSelector("tony-S2K"))
            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping("spout");

        Config conf = new Config();
        //3. 设置kafka producer的配置
        Properties props = new Properties();
        props.put("metadata.broker.list", "10.100.90.203:9092");
        props.put("producer.type","async");
        props.put("request.required.acks", "0"); // 0 ,-1 ,1
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        conf.put("topic","tony-S2K");

        if(args.length > 0){
            // cluster submit.
            try {
                StormSubmitter.submitTopology("kafkaboltTest", conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else{
            new LocalCluster().submitTopology("kafkaboltTest", conf, builder.createTopology());
        }

    }
}
