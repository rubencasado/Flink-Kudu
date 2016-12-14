package es.accenture.flink.Job;

import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Sources.KuduInputSplit;
import es.accenture.flink.Utils.RowSerializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by dani on 14/12/16.
 */
public class JobStreamingSink {

    public static final String KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.getProperty("tableName", "sample");

    public static void main(String[] args) throws Exception {

        KuduInputFormat prueba = new KuduInputFormat("Table_1", "localhost");
        KuduInputSplit a = null;
        prueba.configure(new Configuration());
        prueba.open(a);

        String [] columnNames = new String[3];
        columnNames[0] = "key";
        columnNames[1] = "value";
        columnNames[2] = "descripcion";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.fromElements("fila100 value100 descripcion1000");

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

        //stream2.addSink(new KuduSink(KUDU_MASTER, TABLE_NAME, columnNames)));

        env.execute();
    }
}
