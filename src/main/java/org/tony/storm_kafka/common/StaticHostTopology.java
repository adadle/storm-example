package org.tony.storm_kafka.common;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by TonyLee on 2015/1/22.
 * By IDEA
 */
public class StaticHostTopology {

    public static void main(String[] args) {

        String kafkaHost = "10.100.90.201";
        Broker brokerForPartition0 = new Broker(kafkaHost);//localhost:9092
        Broker brokerForPartition1 = new Broker(kafkaHost, 9092);//localhost:9092 but we specified the port explicitly
        GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
        partitionInfo.addPartition(0, brokerForPartition0);//mapping form partition 0 to brokerForPartition0
        partitionInfo.addPartition(1, brokerForPartition1);//mapping form partition 1 to brokerForPartition1
        StaticHosts hosts = new StaticHosts(partitionInfo);

        String topic="mars-wap";
        String offsetZkRoot ="/stormExample";
        String offsetZkId="staticHost";
        String offsetZkServers = "10.100.90.201";
        String offsetZkPort = "2181";
        List<String> zkServersList = new ArrayList<String>();
        zkServersList.add(offsetZkServers);



        SpoutConfig kafkaConfig = new SpoutConfig(hosts,topic,offsetZkRoot,offsetZkId);

        kafkaConfig.zkPort = Integer.parseInt(offsetZkPort);
        kafkaConfig.zkServers = zkServersList;
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout spout = new KafkaSpout(kafkaConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout, 1);
        builder.setBolt("bolt", new Bolt(), 1).shuffleGrouping("spout");

        Config config = new Config();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());

    }
}
