package com.itbys.storm_demo.demo03_work;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class _99_topology {

    public static void main(String[] args) {

        Config config = new Config();

        _01_spout spout = new _01_spout();
        _02_spout spout1 = new _02_spout();
        _03_spout spout2 = new _03_spout();

        _04_bolt bolt = new _04_bolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("spout01",spout);
        topologyBuilder.setSpout("spout02",spout1);
        topologyBuilder.setSpout("spout03",spout2);

//        topologyBuilder.setBolt("1_bolt",bolt).globalGrouping("spout03");
        topologyBuilder.setBolt("1_bolt",bolt).shuffleGrouping("spout01").shuffleGrouping("spout02").shuffleGrouping("spout03");


        StormTopology topology = topologyBuilder.createTopology();

        //本地模式测试
        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("ddfdfsdffdgg",config,topology);

    }
}
