package com.itbys.storm_demo.demo02_wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class _04_printBolt extends BaseRichBolt {

//    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        this.collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {

        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");

        System.out.println(word + ":" + count);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


    }
}
