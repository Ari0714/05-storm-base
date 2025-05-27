package com.itbys.storm_demo.demo01;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class _01_spout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {

        int num = new Random().nextInt(100);
        collector.emit(new Values(num));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("number"));

    }


}
