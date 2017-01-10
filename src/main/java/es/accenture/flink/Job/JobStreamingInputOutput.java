package es.accenture.flink.Job;

import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Sources.KuduInputSplit;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

/**
 * Created by dani on 14/12/16.
 * un job con datastream que lee de kafka, hace un minimo cambio con un map y va escribiendo en una tabla kudu
 */
public class JobStreamingInputOutput {

    public static final String KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.getProperty("tableName", "sample");

    public static void main(String[] args) throws Exception {

        KuduInputFormat prueba = new KuduInputFormat(TABLE_NAME, KUDU_MASTER);

        String [] columnNames = new String[2];
        columnNames[0] = "key";
        columnNames[1] = "value";

        UUID id = UUID.randomUUID();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");
        prop.setProperty("group.id", String.valueOf(id));
        prop.setProperty("auto.offset.reset", "latest");
        prop.setProperty("zookeeper.connect", "localhost:2181");
        prop.setProperty("topic", "test");

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer09<>(
                prop.getProperty("topic"),
                new SimpleStringSchema(),
                prop));

        DataStream<RowSerializable> stream2 = stream.map(new MapFunction<String, RowSerializable>() {
            @Override
            public RowSerializable map(String input) throws Exception {

                RowSerializable res = new RowSerializable(input.split("\\n").length);
                Integer i = 0;
                for (String s : input.split(" ")) {
                    res.setField(i, s);
                    i++;
                }
                return res;
            }
        });

        //stream2.addSink(new KuduSink(KUDU_MASTER, TABLE_NAME, columnNames)));

        env.execute();
    }
}