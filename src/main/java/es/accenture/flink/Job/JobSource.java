package es.accenture.flink.Job;

import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Sources.KuduInputSplit;
import es.accenture.flink.Utils.KuduTypeInformation;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;

/**
 * Created by dani on 9/12/16.
 */
public class JobSource {

    public static final String KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.getProperty("tableName", "sample");
    public static final String TABLE_NAME2 = System.getProperty("tableName2", "sample2");

    public static void main(String[] args) throws Exception {

        KuduInputFormat prueba = new KuduInputFormat("Table_1", "localhost");
        //KuduInputSplit a = null;
        //prueba.configure(new Configuration());
        //prueba.open(prueba.createInputSplits(1)[0]);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //KuduTypeInformation typeInformation = new KuduTypeInformation(prueba.getRows().get(0));
        TypeInformation<RowSerializable> typeInformation2 = TypeInformation.of(new TypeHint<RowSerializable>() { });
        TypeInformation<RowSerializable> typeInformation3 = TypeInformation.of(RowSerializable.class);
        DataSet<RowSerializable> source = env.createInput(prueba, typeInformation2);

        source.map(new MapFunction<RowSerializable, String>() {

                    @Override
                    public String map(RowSerializable row) throws Exception {
                        System.out.println("HOAL");
                        return row.toString();
                    }
                });

        source.writeAsText("test.txt");

        env.execute();
    }
}
