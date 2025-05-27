package com.itbys.storm_demo.work.demo01_ETLmysql;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class _99_topology {

    public static void main(String[] args) {

        Config config = new Config();

        _01_spout spout = new _01_spout();
        _02_spout spout1 = new _02_spout();

        _03_bolt bolt = new _03_bolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("1_spout",spout);
        topologyBuilder.setSpout("2_spout",spout1);

//        topologyBuilder.setBolt("1_bolt",bolt).globalGrouping("1_spout");
        topologyBuilder.setBolt("1_bolt",bolt).shuffleGrouping("1_spout").shuffleGrouping("2_spout");


        StormTopology topology = topologyBuilder.createTopology();

        //本地模式测试
        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("test",config,topology);

    }
}
