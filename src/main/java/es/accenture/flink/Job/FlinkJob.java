package es.accenture.flink.Job;

/**
 * Created by dani on 23/11/16.
 */

import es.accenture.flink.Sources.KuduInputFormat;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.table.Row;

public class FlinkJob {

    public static final String KUDU_MASTER = System.setProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.setProperty("tableName", "sample");
    public static final String TABLE_NAME2 = System.setProperty("tableName2", "sample2");

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Row> source = env.createInput(new KuduInputFormat());

            /*
            @Override
            public String getTableName() {
                return System.getProperty("tableName");
            }

            public String getMasterAddress() {
                return System.getProperty("kuduMaster");
            }

            Tuple2<String, String> myTuple = new Tuple2<String, String>();

            public Tuple2<String, String> rowToTuple(Row r) {
                String key = r.productElement(1).toString();
                String value = r.productElement(2).toString();
                myTuple.setField(key, 0);
                myTuple.setField(value, 1);
                return myTuple;
            }*/

        DataSet<Row> source2 = source

                .map(new MapFunction<Row, Row>() {

                    @Override
                    public Row map(Row row) throws Exception {
                        String value = row.productElement(1).toString();
                        Row row2 = row;
                        if(value.startsWith("value")) {
                            row2.setField(1, "VALUE");
                            return row2;
                        }
                        return row;
                    }
                });

                source2.print();

        //source2.output(new SinkFunction(""));
        env.execute();
    }
}
