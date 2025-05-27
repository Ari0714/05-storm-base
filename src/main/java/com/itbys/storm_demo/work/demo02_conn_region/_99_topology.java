package com.itbys.storm_demo.work.demo02_conn_region;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class _99_topology {

    public static void main(String[] args) {

        Config config = new Config();

        _01_spout spout = new _01_spout();
        _02_bolt bolt = new _02_bolt();
        _03_bolt bolt1 = new _03_bolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("1_spout",spout);

        topologyBuilder.setBolt("1_bolt",bolt).globalGrouping("1_spout");
        topologyBuilder.setBolt("2_bolt",bolt1).globalGrouping("1_bolt");


        StormTopology topology = topologyBuilder.createTopology();

        //本地模式测试
        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("test",config,topology);

    }
}
