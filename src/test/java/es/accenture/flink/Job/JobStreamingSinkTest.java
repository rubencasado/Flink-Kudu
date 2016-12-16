package es.accenture.flink.Job;

import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Sources.KuduInputSplit;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.junit.Assert.*;

/**
 * Created by jenni on 14/12/16.
 */
public class JobStreamingSinkTest {
    public String KUDU_MASTER;
    public String TABLE_NAME;
    public String [] columnNames;
    public KuduInputFormat prueba;
    public  DataStream<String> stream;
    public KuduInputSplit a;


    @org.junit.Before
    public void setUp() throws Exception {
        KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
        TABLE_NAME = System.getProperty("tableName", "sample");
        columnNames= new String[3];
        columnNames[0] = "key";
        columnNames[1] = "value";
        columnNames[2] = "description";
        prueba = new KuduInputFormat("Table_1", "localhost");
        a = null;
    }

    @org.junit.After
    public void tearDown() throws Exception {
     //Assert.assertEquals();
    }

    @org.junit.Test
    public void main() throws Exception {

            prueba.configure(new Configuration());
            prueba.open(a);

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

             stream = env.fromElements("fila100 value100 descripcion1000");

           /* DataStream<RowSerializable> stream2 = stream.map(new MapFunction<String, RowSerializable>() {
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
            });*/
    }

}