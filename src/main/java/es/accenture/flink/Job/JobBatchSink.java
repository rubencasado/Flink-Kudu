package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduOutputFormat;

import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by dani on 9/12/16.
 */
public class JobBatchSink {

    // Args[0] = mytable
    // Args[1] = CREATE
    // Args[2] = localhost
    public static void main(String[] args) throws Exception {

        String tableName = args[0];
        String tableMode = args[1];
        String host = args[2];

        String [] columnNames = new String[3];
        columnNames[0] = "key";
        columnNames[1] = "value";
        columnNames[2] = "description";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> input = env.fromElements("fila100 value100 description100");

        DataSet<RowSerializable> out = input.map(new MapFunction<String, RowSerializable>() {
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

        out.output(new KuduOutputFormat(host, tableName, columnNames, tableMode));

        env.execute();

    }
}
