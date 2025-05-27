package com.itbys.storm_demo.demo02_wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class _02_splitBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        this.collector = collector;

    }

    @Override
    public void execute(Tuple tuple) {

        String line = tuple.getStringByField("line");
        String[] strings = line.split(" ");
        for (String string : strings) {
            collector.emit(new Values(string));
            System.out.println(string);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("word"));

    }

}
