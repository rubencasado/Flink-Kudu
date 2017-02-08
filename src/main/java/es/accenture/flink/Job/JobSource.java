package es.accenture.flink.Job;

import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.log4j.Logger;
import java.io.File;


/**
 * A job which reads from a Kudu database, creates a dataset, makes some changes over the dataset,
 * and writes the result on a text field. Number os files generated depends on number of cores on the cpu
 * Path where the files will be created: tmp/test
 */
public class JobSource {

    private static final Logger LOG = Logger.getLogger(KuduInputFormat.class);

    public static void main(String[] args) throws Exception {

        //********Only for test, delete once finished*******
        args[0]="Table_1";
        args[1]="localhost";
        //**************************************************

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
        final String KUDU_MASTER = args[1];


        KuduInputFormat prueba = new KuduInputFormat(TABLE_NAME, KUDU_MASTER);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<RowSerializable> typeInformation = TypeInformation.of(RowSerializable.class);
        DataSet<RowSerializable> source = env.createInput(prueba, typeInformation);


        /*Comment or uncomment to modify dataset using a map function*/
        DataSet<RowSerializable> sourceaux = source.map(new MyMapFunction());


        if(!deleteFiles()){
            LOG.error("Error deleting files, exiting.");
        }
        sourceaux.writeAsText("tmp/test");
        env.execute();
        LOG.info("Created files at: " + System.getProperty("user.dir") + "/tmp/test");
    }


    /**
     * Deletes all files existing on tmp/test
     *
     * @return True if files were deleted, False if not
     */
    private static boolean deleteFiles(){
        File dir = new File("tmp/test");
        File[] files = dir.listFiles();
        if (files!=null) {
            for (File file : files) {
                if(!file.delete()){
                    return false;
                }
            }
        }
        return dir.delete();
    }



    /**
     * Map function which receives a row and makes some changes. For example, multiplies the key field by 2
     * and changes value field to upper case
     *
     * @return the RowSerializable generated
     */
    private static class MyMapFunction implements MapFunction<RowSerializable, RowSerializable> {

        @Override
        public RowSerializable map(RowSerializable row) throws Exception {

            for (int i = 0; i < row.productArity(); i++) {
                if (row.productElement(i).getClass().equals(String.class))
                    row.setField(1, row.productElement(1).toString().toUpperCase());
                else if (row.productElement(i).getClass().equals(Integer.class))
                    row.setField(0, (Integer)row.productElement(0)*2);
            }
            return row;
        }
    }
}
