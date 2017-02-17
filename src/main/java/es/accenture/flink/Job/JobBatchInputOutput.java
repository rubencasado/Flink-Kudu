package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduOutputFormat;
import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Utils.RowSerializable;
import es.accenture.flink.Utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.kudu.client.KuduClient;

/**
 * A job which reads from a Kudu database, creates a dataset, makes some changes over the dataset,
 * and writes the result on another Kudu database.
 * (This example multiply the field 'key' by 2 and transform the field 'value' to uppercase)
 */
public class JobBatchInputOutput {

    public static void main(String[] args) throws Exception {

        /********Only for test, delete once finished*******/
        args[0] = "TableBatchSource";
        args[1] = "TableBatchSink";
        args[2] = "create";
        args[3] = "localhost";
        /**************************************************/

        if(args.length != 4){
            System.out.println( "JobBatchInputOutput params: [TableReadName] [TableWriteName] [Mode] [Kudu Master Adress]\n");
            return;
        }

        // Params of program
        String tableSourceName = args[0];
        String tableSinkName = args[1];
        String mode = args[2];
        String host = args[3];

        System.out.println("-----------------------------------------------");
        System.out.println("1. Read data from a Kudu DB (" + tableSourceName + ").\n" +
                "2. Change field 'value' to uppercase.\n" +
                "3. Write back in a new Kudu DB (" + tableSinkName + ").");
        System.out.println("-----------------------------------------------\n");

        String [] columnNames = new String[2];
        columnNames[0] = "col1";
        columnNames[1] = "col2";

        final Integer MODE;
        if (mode.equalsIgnoreCase("create")){
            MODE = KuduOutputFormat.CREATE;
        }else if (mode.equalsIgnoreCase("append")){
            MODE = KuduOutputFormat.APPEND;
        } else if (mode.equalsIgnoreCase("override")){
            MODE = KuduOutputFormat.OVERRIDE;
        } else {
            System.out.println("Error in param [Mode]. Only create, append or override allowed.");
            return;
        }

        long startTime = System.currentTimeMillis();

        KuduInputFormat KuduInputTest = new KuduInputFormat(tableSourceName, host);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<RowSerializable> typeInformation = TypeInformation.of(RowSerializable.class);

        DataSet<RowSerializable> source = env.createInput(KuduInputTest, typeInformation);

        DataSet<RowSerializable> input = source.map(new MyMapFunction());

        input.output(new KuduOutputFormat(host, tableSinkName, columnNames, MODE));

        env.execute();

        long endTime = System.currentTimeMillis();

        System.out.println("Program executed in " + (endTime - startTime)/1000 + " seconds");  //divide by 1000000 to get milliseconds.
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
                if (row.productElement(i).getClass().equals(String.class)) {
                    row.setField(1, row.productElement(1).toString().toUpperCase());
                } else if (row.productElement(i).getClass().equals(Integer.class))
                    row.setField(0, (Integer)row.productElement(0)*2);
            }
            return row;
        }
    }
}