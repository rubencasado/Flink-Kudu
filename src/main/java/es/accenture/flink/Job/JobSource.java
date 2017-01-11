package es.accenture.flink.Job;

import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.File;

/**
 * Created by dani on 9/12/16.
 */
public class JobSource {

    public static final String KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.getProperty("tableName", "Table_2");

    public static void main(String[] args) throws Exception {

        KuduInputFormat prueba = new KuduInputFormat(TABLE_NAME, KUDU_MASTER);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<RowSerializable> typeInformation = TypeInformation.of(RowSerializable.class);
        DataSet<RowSerializable> source = env.createInput(prueba, typeInformation);

        DataSet<String> str = source.map(new MyMapFunction());

        File dir = new File("tmp/test");
        File[] files = dir.listFiles();
        if (files!=null) {
            for (int i = 0; i < files.length; i++) {
                files[i].delete();
            }
        }
        dir.delete();

        str.writeAsText("tmp/test");
        env.execute();
    }

    private static class MyMapFunction implements MapFunction<RowSerializable, String> {

        @Override
        public String map(RowSerializable row) throws Exception {
            return row.toString();
        }
    }
}
