package es.accenture.flink.Job;

import es.accenture.flink.Sink.*;
import es.accenture.flink.Utils.RowSerializable;
import es.accenture.flink.Utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.*;

/**
 * Created by jenni on 14/12/16.
 */
public class JobBatchSinkTest{

    public String KUDU_MASTER;
    public String TABLE_NAME;
    public String[] columnNames;
    public DataSet<String> input_test;
    public DataSet<RowSerializable> out_test;
    public  KuduOutputFormat kuduformat;
    public Utils utils;



    @Before
    public void setUp() throws Exception {
        KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
        TABLE_NAME = System.getProperty("tableName", "sample");
        columnNames= new String[3];
        columnNames[0] = "key";
        columnNames[1] = "value";
        columnNames[2] = "description";
        utils=new Utils("localhost");
    }

    @After
    public void tearDown() throws Exception {
     //Assert.assertEquals();  Al final del test comprobar que se han introducido correctamente lso datos en la tabla
    }

    @Test
    public void main() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        input_test = env.fromElements("fila100 value100 description1000");

         out_test = input_test.map(new MapFunction<String, RowSerializable>() {
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
        kuduformat=  new KuduOutputFormat(KUDU_MASTER, TABLE_NAME, columnNames, KuduOutputFormat.CREATE);
        out_test.output(kuduformat);
         
         //kuduformat.writeRecord(r);
       // Assert.assertTrue(utils.toString(r)=="fila100 value100 description1000");*/
        //env.execute();




    }

}