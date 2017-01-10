package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduSink;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by dani on 14/12/16.
 * Un job con datastream que lee de Kafka, hace un minimo cambio con un map y va escribiendo en una tabla kudu
 */
public class JobStreamingInputOutput {

    public static void main(String[] args) throws Exception {

        String tableName = args[0];
        String topic = args[1];
        String host = args[2];

        String [] columnNames = new String[4];
        columnNames[0] = "key";
        columnNames[1] = "value";
        columnNames[2] = "descripcion";
        columnNames[3] = "cuarto";

        UUID id = UUID.randomUUID();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");
        prop.setProperty("group.id", String.valueOf(id));
        prop.setProperty("auto.offset.reset", "latest");
        prop.setProperty("zookeeper.connect", "localhost:2181");
        prop.setProperty("topic", topic);

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer09<>(
                prop.getProperty("topic"),
                new SimpleStringSchema(),
                prop));

        DataStream<RowSerializable> stream2 = stream.map(new MapFunction<String, RowSerializable>() {
            @Override
            public RowSerializable map(String input) throws Exception {

                RowSerializable res = new RowSerializable(4);
                Integer i = 0;
                for (String s : input.split(" ")) {
                    res.setField(i, s);
                    i++;
                }
                return res;
            }
        });

        stream2.addSink(new KuduSink(host, tableName, columnNames));

        env.execute();
    }
}