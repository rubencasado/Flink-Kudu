package es.accenture.flink.Job;

import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by dani on 9/12/16.
 */
public class JobSource {

    public static final String KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.setProperty("tableName", "sample");
    public static final String TABLE_NAME2 = System.setProperty("tableName2", "sample2");

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<RowSerializable> source = env.createInput(new KuduInputFormat(TABLE_NAME, KUDU_MASTER));

        source.map(new MapFunction<RowSerializable, String>() {

                    @Override
                    public String map(RowSerializable row) throws Exception {

                        return row.toString();
                    }
                });

        source.print();

        env.execute();
    }
}
