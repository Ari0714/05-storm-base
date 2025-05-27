package com.itbys.storm_demo.demo02_wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class _03_countBolt extends BaseRichBolt {

    private OutputCollector collector;

    private HashMap<String,Integer> hashMap =  new HashMap();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String word = tuple.getStringByField("word");

        if (hashMap.containsKey(word))
            hashMap.put(word,(hashMap.get(word)+1));
        else
            hashMap.put(word,1);

        collector.emit(new Values(word,hashMap.get(word)));
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("word","count"));

    }


}
