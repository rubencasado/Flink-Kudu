package es.accenture.flink.Job;

import es.accenture.flink.Sources.KuduInputFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.table.Row;

/**
 * Created by dani on 9/12/16.
 */
public class JobSource {

    public static final String KUDU_MASTER = System.setProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.setProperty("tableName", "sample");
    public static final String TABLE_NAME2 = System.setProperty("tableName2", "sample2");

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Row> source = env.createInput(new KuduInputFormat());

        source.map(new MapFunction<Row, String>() {

                    @Override
                    public String map(Row row) throws Exception {

                        return row.toString();
                    }
                });

        source.print();

        env.execute();
    }
}
