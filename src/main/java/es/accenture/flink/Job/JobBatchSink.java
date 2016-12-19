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

    public static final String KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.getProperty("tableName", "sample");

    public static void main(String[] args) throws Exception {

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

        out.output(new KuduOutputFormat(KUDU_MASTER, TABLE_NAME, columnNames, "CREATE"));
        env.execute();

    }
}
