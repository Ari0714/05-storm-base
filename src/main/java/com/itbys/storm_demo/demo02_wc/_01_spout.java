package com.itbys.storm_demo.demo02_wc;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import javax.xml.soap.SAAJResult;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Random;

public class _01_spout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private String[] line = new String[]{"ff aa ff"};

    private BufferedReader bufferedReader;
    private FileReader fileReader;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.bufferedReader = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\02storm\\原数据\\app.log"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {

        collector.emit(new Values(line));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("line"));

    }


}
