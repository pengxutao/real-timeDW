import com.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestKafkaConnector {
    public static void main(String[] args) {
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("" +
                "create table test_result(" +
                "id_a string," +
                "id_b string," +
                "name_a string," +
                "name_b string" +
                ")" + MyKafkaUtil.getKafkaDDL("test_result", "test_kafka_connector")
        );


        tableEnv.executeSql("" +
                "create table kafka_connector(" +
                "id_a string," +
                "id_b string," +
                "name_a string," +
                "name_b string" +
                ")" + MyKafkaUtil.getKafkaSinkDDL("result_kafka_connector")
        );

        tableEnv.executeSql("insert into kafka_connector " +
                "select * from test_result");

    }
}
