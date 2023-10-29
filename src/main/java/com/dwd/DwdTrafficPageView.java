package com.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTrafficPageView {
    public static void main(String[] args) throws Exception {
//TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数

        // 开启CheckPoint
        //env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//
        //// 设置状态后端
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/real-timeDW/ck");
        //System.setProperty("HADOOP_USER_NAME", "pxt");

        //TODO 2.消费Kafka topic_log 主题的数据创建流
        String topic = "topic_log";
        String groupId = "dwd_traffic_page_view";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.过滤掉非JSON格式的数据&将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        //获取侧输出流脏数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>>>>>");

        //TODO 4.获取页面访问数据
        SingleOutputStreamOperator<String> pageDS = jsonObjDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //尝试获取错误信息
                String err = value.getString("err");
                // 尝试获取启动信息
                String start = value.getString("start");
                // 没有错误，而且不是启动日志，那么是页面日志
                if (err == null && start == null) {
                    // 移除错误、动作和曝光，只保留common、page和ts
                    value.remove("err");
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }
        });
        //TODO 5.打印并写进对应主题
        pageDS.print("Page>>>>>>>>>>");
        String page_topic = "dwd_traffic_page_view";
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));

        env.execute("BaseLogApp");
    }
}
