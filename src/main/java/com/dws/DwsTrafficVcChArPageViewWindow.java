package com.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bean.TrafficPageViewBean;
import com.utils.DateFormatUtil;
import com.utils.MyClickHouseUtil;
import com.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficVcChArPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "pxt");

        //TODO 2.读取三个主题的数据创建流
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String ujdTopic = "dwd_traffic_user_jump_detail";
        String topic = "dwd_traffic_page_log";
        String groupId = "vc-ch-ar_page_view_window";
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uvTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(ujdTopic, groupId));
        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.统一数据格式
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPageDS = pageDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");
            long sv = 0L;
            if (lastPageId == null) {
                sv = 1L;
            }
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    0L, sv, 1L, page.getLong("during_time"), 0L,
                    jsonObject.getLong("ts"));
        });

        //TODO 4.将三个流进行Union
        DataStream<TrafficPageViewBean> unionDS = trafficPageViewWithUvDS.union(
                trafficPageViewWithUjDS,
                trafficPageViewWithPageDS);

        //TODO 5.提取事件时间生成WaterMark
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithWmDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(14)).
                        withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        //TODO 6.分组开窗聚合
        WindowedStream<TrafficPageViewBean, Tuple3<String, String, String>, TimeWindow> windowedStream = trafficPageViewWithWmDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return new Tuple3<>(value.getAr(),
                        value.getCh(),
                        value.getVc());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple3<String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple3<String, String, String> key, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {

                //获取数据
                TrafficPageViewBean next = input.iterator().next();

                //补充信息
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                //修改TS
                next.setTs(System.currentTimeMillis());

                //输出数据
                out.collect(next);
            }
        });

        //TODO 7.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("DwsTrafficVcChArPageViewWindow");

    }

}
