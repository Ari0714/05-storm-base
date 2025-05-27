package com.itbys.storm_demo.work.demo01_ETLmysql;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class _02_spout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader bufferedReader;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.bufferedReader = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\02storm\\原数据\\lng_lat.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    String aa;

    @Override
    public void nextTuple() {

        try {
            while ((aa = this.bufferedReader.readLine()) != null) {
//                System.out.println(aa);
                collector.emit(new Values(aa));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        //发送Fields设置好
        outputFieldsDeclarer.declare(new Fields("mapP"));

    }


}
