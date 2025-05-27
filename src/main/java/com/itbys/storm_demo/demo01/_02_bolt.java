package com.itbys.storm_demo.demo01;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class _02_bolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        this.collector = collector;

    }

    @Override
    public void execute(Tuple tuple) {

        System.out.println(tuple.getIntegerByField("number"));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


    }

}
