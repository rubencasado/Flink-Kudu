package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduOutputFormat;
import es.accenture.flink.Sink.KuduSink;
import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Sources.KuduInputSplit;
import es.accenture.flink.Utils.KuduTypeInformation;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.table.Row;
import org.apache.flink.configuration.Configuration;

import java.util.Scanner;

/**
 * Created by dani on 14/12/16.
 * Un job de dataset que lea de una clase kudu, haga un minimo cambio sobre los datos
 * (por ejemplo un map que pase a mayusuculas los string y los float los multiplique por 2)
 * y escriba el resultado en una clase kudu
 */
public class JobBatchInputOutput {

    public static final String KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.getProperty("tableName", "Table_1");
    public static final String TABLE_NAME2 = System.getProperty("tableName", "Table_2");

    public static void main(String[] args) throws Exception {

        KuduInputFormat KuduInputTest = new KuduInputFormat(TABLE_NAME, KUDU_MASTER);

        String [] columnNames = new String[2];
        columnNames[0] = "key";
        columnNames[1] = "value";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<RowSerializable> typeInformation = TypeInformation.of(RowSerializable.class);

        DataSet<RowSerializable> source = env.createInput(KuduInputTest, typeInformation);

        DataSet<RowSerializable> input = source.map(new MyMapFunction());

        input.output(new KuduOutputFormat(KUDU_MASTER, TABLE_NAME2, columnNames, "APPEND"));

        env.execute();
    }

    private static class MyMapFunction implements MapFunction<RowSerializable, RowSerializable> {

        @Override
        public RowSerializable map(RowSerializable row) throws Exception {

            RowSerializable res = row;

            for (int i = 0; i < row.productArity(); i++) {
                if (row.productElement(i).getClass().equals(String.class))
                    res.setField(1, row.productElement(1).toString().toUpperCase());
                else if (row.productElement(i).getClass().equals(Integer.class))
                    res.setField(0, (Integer)row.productElement(0)*2);
            else continue;
            }
            return res;
        }
    }
}
