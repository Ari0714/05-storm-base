package com.itbys.storm_demo.work.demo01_ETLmysql;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.sql.*;
import java.util.Map;

public class _03_bolt extends BaseRichBolt {

//    private OutputCollector collector;
    Connection conn;
    private Statement state;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

//        this.collector = collector;

        try {
            String driver = "com.mysql.jdbc.Driver";
            Class.forName(driver);
            conn = DriverManager.getConnection("jdbc:mysql://hdp103:3306/test", "root", "111111");
            state = conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceComponent().equalsIgnoreCase("1_spout")) {

            String ipArea = tuple.getStringByField("ipArea");
            String[] strings = ipArea.split("\t");

            try {
                state.execute("insert into ip_area_isp values('" + strings[0] + "','" + strings[1] + "','" + strings[2] + "'" +
                        ",'" + strings[3] + "','" + strings[4] + "','" + strings[5] + "')");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        if (tuple.getSourceComponent().equalsIgnoreCase("2_spout")) {

            String lngMap = tuple.getStringByField("mapP");
            String[] strings02 = lngMap.split(",");

            try {
                state.execute("insert into lng_lat_mapping values('" + strings02[0] + "','" + strings02[1] + "','" + strings02[2] + "','" + strings02[3]+ "')");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


    }

}
