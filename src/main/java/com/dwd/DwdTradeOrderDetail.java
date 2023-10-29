package com.dwd;

import com.utils.MyKafkaUtil;
import com.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class DwdTradeOrderDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.1 开启CheckPoint
        //env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        //1.2 设置状态后端
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        //System.setProperty("HADOOP_USER_NAME", "pxt");

        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 为表关联时状态中存储的数据设置过期时间,支付可能比下单慢15分钟，再加5s的乱序
        configuration.setString("table.exec.state.ttl", "5 s");


        //TODO 2.创建 topic_db 表
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("order_detail"));

        //TODO 3.过滤出订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['sku_name'] sku_name, " +
                "    data['order_price'] order_price, " +
                "    data['sku_num'] sku_num, " +
                "    data['create_time'] create_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    data['split_total_amount'] split_total_amount, " +
                "    data['split_activity_amount'] split_activity_amount, " +
                "    data['split_coupon_amount'] split_coupon_amount, " +
                "    pt  " + // 额外定义的字段作为处理事件，lookup join 时需要使用
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_table", orderDetailTable);

        //转换为流并打印测试
        tableEnv.toAppendStream(orderDetailTable, Row.class).print(">>>>");

        //TODO 4.过滤出订单数据
        Table orderInfoTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['consignee'] consignee, " +
                "    data['consignee_tel'] consignee_tel, " +
                "    data['total_amount'] total_amount, " +
                "    data['order_status'] order_status, " +
                "    data['user_id'] user_id, " +
                "    data['payment_way'] payment_way, " +
                "    data['delivery_address'] delivery_address, " +
                "    data['order_comment'] order_comment, " +
                "    data['out_trade_no'] out_trade_no, " +
                "    data['trade_body'] trade_body, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['expire_time'] expire_time, " +
                "    data['process_status'] process_status, " +
                "    data['tracking_no'] tracking_no, " +
                "    data['parent_order_id'] parent_order_id, " +
                "    data['province_id'] province_id, " +
                "    data['activity_reduce_amount'] activity_reduce_amount, " +
                "    data['coupon_reduce_amount'] coupon_reduce_amount, " +
                "    data['original_total_amount'] original_total_amount, " +
                "    data['feight_fee'] feight_fee, " +
                "    data['feight_fee_reduce'] feight_fee_reduce, " +
                "    data['refundable_time'] refundable_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_info_table", orderInfoTable);

        //转换为流并打印测试
        //tableEnv.toAppendStream(orderInfoTable, Row.class).print(">>>>");

        //TODO 5.过滤出订单明细活动关联数据
        Table orderActivityTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['order_detail_id'] order_detail_id, " +
                "    data['activity_id'] activity_id, " +
                "    data['activity_rule_id'] activity_rule_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_activity' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_activity_table", orderActivityTable);
        //转换为流并打印测试
        //tableEnv.toAppendStream(orderActivityTable, Row.class).print(">>>>");

        //TODO 6.过滤出订单明细购物券关联数据
        Table orderCouponTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['order_detail_id'] order_detail_id, " +
                "    data['coupon_id'] coupon_id, " +
                "    data['coupon_use_id'] coupon_use_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_coupon' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_coupon_table", orderCouponTable);

        //转换为流并打印测试
        //tableEnv.toAppendStream(orderCouponTable, Row.class).print(">>>>");

        //TODO 7.创建 base_dic LookUp表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //TODO 8.关联5张表
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    od.id, " +
                "    od.order_id, " +
                "    oi.user_id, " +
                "    od.sku_id, " +
                "    od.sku_name, " +
                "    od.sku_num, " +
                "    od.order_price, " +
                "    oi.province_id, " +
                "    oa.activity_id, " +
                "    oa.activity_rule_id, " +
                "    oc.coupon_id, " +
                "    od.create_time, " +
                "    od.source_id, " +
                "    od.source_type source_type_id, " +
                "    dic.dic_name source_type_name, " +
                "    od.split_activity_amount, " +
                "    od.split_coupon_amount, " +
                "    od.split_total_amount, " +
                "    current_row_timestamp() row_op_ts "+ // 处理时间字段，用于 dws 层的去重
                "from order_detail_table od " +
                "join order_info_table oi " +
                "on od.order_id = oi.id " +
                "left join order_activity_table oa " +
                "on od.id = oa.order_detail_id " +
                "left join order_coupon_table oc " +
                "on od.id = oc.order_detail_id " +
                "join base_dic FOR SYSTEM_TIME AS OF od.pt as dic " +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        //转换为流并打印测试
        //tableEnv.toRetractStream(resultTable, Row.class).print(">>>>");

        //TODO 9.创建 upsert-kafka 表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "sku_num string, " +
                "order_price string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "create_time string, " +
                "source_id string, " +
                "source_type_id string, " +
                "source_type_name string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(id) not enforced " + // 使用upsert-kafka-connector建表语句必须有主键
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_detail"));

        //{"id":"13067","order_id":"4881","user_id":"83","sku_id":"11","sku_name":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机","sku_num":"2",
        // "order_price":"8197.0","province_id":"1","activity_id":"2","activity_rule_id":"4","coupon_id":null,"create_time":"2020-06-15 20:37:41",
        // "source_id":null,"source_type_id":"2401","source_type_name":"用户查询","split_activity_amount":"1200.0","split_coupon_amount":null,"split_total_amount":"15194.0","row_op_ts":"2023-10-27 12:37:42.502Z"}

        //TODO 10.将数据写出
        tableEnv.executeSql("insert into dwd_trade_order_detail select * from result_table");

        //TODO 11.启动任务
        env.execute("DwdTradeOrderPreProcess");
    }
}
