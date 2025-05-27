package com.itbys.storm_demo.work.demo02_conn_region;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.sql.*;
import java.util.Map;

public class _03_bolt extends BaseRichBolt {

    private OutputCollector collector;

    Connection conn;
    private Statement state;
    private Statement state02;

    int a = 1;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = collector;

        try {
            String driver = "com.mysql.jdbc.Driver";
            Class.forName(driver);
            conn = DriverManager.getConnection("jdbc:mysql://hdp103:3306/test", "root", "111111");
            state = conn.createStatement();
            state02 = conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {

        String regionInfo = tuple.getStringByField("region_info");
        System.out.println(regionInfo);
        System.out.println(a);

        String[] strings = regionInfo.split("_");

        String city = null;
        String province = null;
        if (strings.length > 3 && strings[3] != null)
             city = strings[3].replaceAll("省", "").replaceAll("市", "");
        if (strings.length > 2 && strings[2] != null)
            province = strings[2].replaceAll("省", "").replaceAll("市", "");

        try {
            if (city != null && city.length() > 0) {
                ResultSet resultSet = state.executeQuery("SELECT lng,lat from lng_lat_mapping where city = \"" + city + "\" ");
                while (resultSet.next()) {
                    String lng = resultSet.getString(1);
                    String lat = resultSet.getString(2);

                    state02.execute("insert into phone_ip_lng_lat values('" + strings[0] + "','" + strings[1] + "','" + lng + "'" +
                            ",'" + lat + "')");
                }
                resultSet.close();
            } else if (province != null && province.length() > 0) {
                ResultSet resultSet = state.executeQuery("SELECT lng,lat from lng_lat_mapping where province = \"" + province + "\" and city is null ");
                while (resultSet.next()) {
                    String lng = resultSet.getString(1);
                    String lat = resultSet.getString(2);

                    state02.execute("insert into phone_ip_lng_lat values('" + strings[0] + "','" + strings[1] + "','" + lng + "'" +
                            ",'" + lat + "')");
                }
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        a++;

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
