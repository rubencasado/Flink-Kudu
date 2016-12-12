package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduSinkFunction;

import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.table.Row;

/**
 * Created by dani on 9/12/16.
 */
public class JobSink {

    public static final String KUDU_MASTER = System.setProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.setProperty("tableName", "sample");
    public static final String TABLE_NAME2 = System.setProperty("tableName2", "sample2");

    public static void main(String[] args) throws Exception {

        String [] columnNames = new String[3];
        columnNames[0] = "key";
        columnNames[1] = "value";
        columnNames[2] = "descripcion";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> input = env.fromElements("fila100 value100 descripcion1000");

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

        out.output(new KuduSinkFunction(KUDU_MASTER, TABLE_NAME, columnNames,"APPEND"));
        env.execute();

    }
}
