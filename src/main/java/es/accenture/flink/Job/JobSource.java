package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduOutputFormat;
import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.log4j.Logger;
import scala.Int;

import java.io.File;

/**
 * Created by dani on 9/12/16.
 */
public class JobSource {

    private static final Logger LOG = Logger.getLogger(KuduInputFormat.class);
    public static final String KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
    public static final String TABLE_NAME = System.getProperty("tableName", "Table_1");

    public static void main(String[] args) throws Exception {

        /********Only for test, delete once finished*******/
        args[0]="Table_1";
        args[1]="localhost";
        /**************************************************/

        System.out.println("-----------------------------------------------");
        System.out.println("1. Read data from a Kudu DB (" + args[0] + ").\n" +
                           "2. Can change rows' information using a Map Function (Not necessary)\n" +
                            "3. Write data as text file.");
        System.out.println("-----------------------------------------------");

        if(args.length!=2){
            System.out.println( "JobSource params: [TableRead] [Master Adress]\n");
            return;
        }

        final String TABLE_NAME = args[0];
        final Integer MODE;
        if (args[1].equalsIgnoreCase("create")){
            MODE = KuduOutputFormat.CREATE;
        }else if (args[1].equalsIgnoreCase("append")){
            MODE = KuduOutputFormat.APPEND;
        } else if (args[1].equalsIgnoreCase("override")){
            MODE = KuduOutputFormat.OVERRIDE;
        } else{
            System.out.println("Error in param [Mode]. Only create, append or override allowed.");
            return;
        }
        final String KUDU_MASTER = args[2];


        KuduInputFormat prueba = new KuduInputFormat(TABLE_NAME, KUDU_MASTER);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<RowSerializable> typeInformation = TypeInformation.of(RowSerializable.class);
        DataSet<RowSerializable> source = env.createInput(prueba, typeInformation);


        /*Uncomment to modify dataset using a map function*/
        //DataSet<RowSerializable> str = source.map(new MyMapFunction());

        File dir = new File("tmp/test");
        File[] files = dir.listFiles();
        if (files!=null) {
            for (int i = 0; i < files.length; i++) {
                files[i].delete();
            }
        }
        dir.delete();

        source.writeAsText("tmp/test");
        env.execute();
        LOG.info("Created files at: " + System.getProperty("user.dir") + "/tmp/test");
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
