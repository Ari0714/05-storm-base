package com.itbys.storm_demo.work.demo02_conn_region;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class _02_bolt extends BaseRichBolt {

    private OutputCollector collector;

    Connection conn;
    private Statement state;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        this.collector = collector;

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

        //卢森堡				1476288512	1476296703
        //中国	北京市		长城宽带	2081308672	2081325055
        //中国	山东省	烟台市	铁通	2052169728	2052173823
        //outPut（山东省，2081308672_2081325055）

        //1413276664	18223408375	7d-78-7a-29-70-21:CMCC-EASY	182.146.113.219	sina.com	门户	18	7	3685	3538	200
        String appLog = tuple.getStringByField("appLog");
        String[] strings = appLog.split("\t");
        String ipNUm = strings[3].replaceAll("\\.", "");

        try {
            ResultSet resultSet = state.executeQuery("SELECT province,city from ip_area_isp where '" + ipNUm + "' >= min_ip and '" + ipNUm + "' <= max_ip");
            while (resultSet.next()){
                String pro = resultSet.getString(1);
                String ct = resultSet.getString(2);

                //13470564658_207.2.40.234_黑龙江省_黑河市
//                System.out.println(strings[1] + "_" + strings[3] + "_" + pro + "_" + ct);
                collector.emit(new Values(strings[1] + "_" + strings[3] + "_" + pro + "_" + ct));

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("region_info"));
    }

}
