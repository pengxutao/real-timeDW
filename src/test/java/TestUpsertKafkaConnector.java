import com.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestUpsertKafkaConnector {
    public static void main(String[] args) {
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("" +
                "create table a(" +
                "id string," +
                "name_a string" +
                ")" + MyKafkaUtil.getKafkaDDL("testa", "testa")
        );

        tableEnv.executeSql("" +
                "create table b(" +
                "id string," +
                "name_b string" +
                ")" + MyKafkaUtil.getKafkaDDL("testb", "testb")
        );

        Table tableLeftJoin = tableEnv.sqlQuery("select a.id, b.id, a.name_a, b.name_b" +
                " from a left join b on a.id = b.id");
        tableEnv.createTemporaryView("table_left_join", tableLeftJoin);

        tableEnv.executeSql("" +
                "create table test_result(" +
                "id_a string," +
                "id_b string," +
                "name_a string," +
                "name_b string," +
                "primary key(id_a) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("test_result")
        );

        tableEnv.executeSql("insert into test_result " +
                "select * from table_left_join");

        // 输1-a，1-b，2-b，2-a
        //{"id":"1","name_a":"a"},{"id":"2","name_a":"aa"}
        //{"id":"1","name_b":"b"},{"id":"2","name_b":"bb"}

    }
}
