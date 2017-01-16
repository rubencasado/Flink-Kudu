package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduSink;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Logger;

/**
 * Created by dani on 14/12/16.
 */
public class JobStreamingSink {

    // LOG4J
    final static Logger logger = Logger.getLogger(JobStreamingSink.class);

    // Args[0] = sample
    // Args[1] = localhost
    public static void main(String[] args) throws Exception {


        String [] columnNames = new String[3];
        columnNames[0] = "key";
        columnNames[1] = "value";
        columnNames[2] = "description";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.fromElements("field1 field2 field3");

        DataStream<RowSerializable> stream2 = stream.map(new MapFunction<String, RowSerializable>() {
            @Override
            public RowSerializable map(String inputs) throws Exception {
                RowSerializable r = new RowSerializable(3);
                Integer i = 0;
                for (String s : inputs.split(" ")) {
                    r.setField(i, s);
                    i++;
                }
                return r;
            }
        });

        stream2.addSink(new KuduSink("localhost", "Table_5", columnNames));

        env.execute();
    }
}
