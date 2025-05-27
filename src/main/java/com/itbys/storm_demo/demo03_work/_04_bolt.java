package com.itbys.storm_demo.demo03_work;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class _04_bolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        this.collector = collector;

    }


//    HashMap<String,HashMap<String,String>> lngProvinceMap = new HashMap<>();
//    HashMap<String,HashMap<String,String>> lngCityMap = new HashMap<>();


    HashMap<String,String> ipProvinceMap = new HashMap<>();
    HashMap<String,String> ipCityMap = new HashMap<>();


    HashMap<String,String> log2ipMap = new HashMap<>();

    ArrayList<String> lenLatArrayList = new ArrayList<>();


    @Override
    public void execute(Tuple tuple) {


        //卢森堡				1476288512	1476296703
        //中国	北京市		长城宽带	2081308672	2081325055
        //中国	山东省	烟台市	铁通	2052169728	2052173823
        //outPut（山东省，2081308672_2081325055）
        if (tuple.getSourceComponent().equalsIgnoreCase("spout02")) {

            String mapp = tuple.getStringByField("ipArea");
            String[] strings = mapp.split("\t");

            if (strings[1] != null) {
                if (!strings[2].equals("")){
//                    System.out.println("aa");
                    ipCityMap.put(strings[2], strings[4] + "_" + strings[5]);
                }
                else{
//                    System.out.println("bb");
                    ipProvinceMap.put(strings[1], strings[4] + "_" + strings[5]);
                }

            }

//            System.out.println(mapp);
        }


        for (Map.Entry<String, String> stringStringEntry : ipCityMap.entrySet()) {
            System.out.println(stringStringEntry);
        }


        //1413276664	18223408375	7d-78-7a-29-70-21:CMCC-EASY	182.146.113.219	sina.com	门户	18	7	3685	3538	200
        //outPut（18223408375_182.146.113.219,山东省）
//        if (tuple.getSourceComponent().equalsIgnoreCase("spout01")) {
//
//            String appLog = tuple.getStringByField("appLog");
//            String[] strings = appLog.split("\t");
//
//            String ipNum = strings[3].replaceAll(".", "");
//            for (Map.Entry<String, String> stringStringEntry : ipProvinceMap.entrySet()) {
//                String[] strings1 = stringStringEntry.getValue().split("_");
//                if (Integer.parseInt(ipNum) >= Integer.parseInt(strings1[0]) && Integer.parseInt(ipNum) <= Integer.parseInt(strings1[1]))
//                    log2ipMap.put(strings[1] + "_" + strings[3], stringStringEntry.getKey());
//                else {
//                    for (Map.Entry<String, String> stringStringEntry1 : ipProvinceMap.entrySet()) {
//                        String[] strings2 = stringStringEntry1.getValue().split("_");
//                        if (Integer.parseInt(ipNum) >= Integer.parseInt(strings2[0]) && Integer.parseInt(ipNum) <= Integer.parseInt(strings2[1]))
//                            log2ipMap.put(strings2[1] + "_" + strings2[3], stringStringEntry.getKey());
//                    }
//                }
//
//                System.out.println(appLog);
//
//            }
//        }


        //重庆,南桐,107.04,29.86
//        if (tuple.getSourceComponent().equalsIgnoreCase("spout03")){
//
//            String mapp = tuple.getStringByField("mapP");
//            String[] strings = mapp.split(",");
//
//            HashMap<String, String> stringStringHashMap = new HashMap<>();
//            stringStringHashMap.put(strings[2],strings[3]);
//
//            lngProvinceMap.put(strings[0],stringStringHashMap);
//            lngCityMap.put(strings[0],stringStringHashMap);
//
//            System.out.println(mapp);
//
//        }


//        if (tuple.getSourceComponent().equalsIgnoreCase("spout02")){
//
//            String ipArea = tuple.getStringByField("ipArea");
//            System.out.println(ipArea);
//            String[] strings = ipArea.split("\t");
//            if (strings[1] != null){
//
//            }
//        }


    }




    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


    }

}
