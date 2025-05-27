package com.itbys.storm_demo.demo01;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class _03_topology {

    public static void main(String[] args) {

        Config config = new Config();

        _01_spout spout = new _01_spout();
        _02_bolt bolt = new _02_bolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("1_spout", spout);
        topologyBuilder.setBolt("1_bolt", bolt).globalGrouping("1_spout");

        StormTopology topology = topologyBuilder.createTopology();

        //本地模式测试
        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("aa", config, topology);

    }
}
