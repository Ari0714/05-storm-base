package com.itbys.storm_demo.demo02_wc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class _99_topology {

    public static void main(String[] args) {

        Config config = new Config();

        _01_spout spout = new _01_spout();
        _02_splitBolt bolt = new _02_splitBolt();
        _03_countBolt countBolt = new _03_countBolt();
        _04_printBolt printBolt = new _04_printBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("1_spout",spout);
        topologyBuilder.setBolt("2_bolt",bolt).globalGrouping("1_spout");
        topologyBuilder.setBolt("3_bolt",countBolt).globalGrouping("2_bolt");
        topologyBuilder.setBolt("4_bolt",printBolt).globalGrouping("3_bolt");

        StormTopology topology = topologyBuilder.createTopology();

        //本地模式测试
        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("aa",config,topology);

    }
}
