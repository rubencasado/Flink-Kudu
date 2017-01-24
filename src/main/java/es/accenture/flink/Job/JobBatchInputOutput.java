package es.accenture.flink.Job;


import es.accenture.flink.Sink.KuduOutputFormat;
import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by dani on 14/12/16.
 * Un job de dataset que lea de una clase kudu, haga un minimo cambio sobre los datos
 * (por ejemplo un map que pase a mayusuculas los string y los float los multiplique por 2)
 * y escriba el resultado en una clase kudu
 */
public class JobBatchInputOutput {


    public static void main(String[] args) throws Exception {

        /********Only for test, delete once finished*******/
        args[0]="Table_1";
        args[1]="Table_2";
        args[2]="append";
        args[3]="localhost";
        /**************************************************/

        System.out.println("-----------------------------------------------");
        System.out.println("1. Read data from a Kudu DB (" + args[0] + ").\n" +
                            "2. Change field 'value' to uppercase.\n" +
                            "3. Write back in a new Kudu DB (" + args[1] + ").");
        System.out.println("-----------------------------------------------\n");

        if(args.length!=4){
            System.out.println( "JobBatchInputOutput params: [TableReadName] [TableWriteName] [Mode] [Kudu Master Adress]\n");
            return;
        }

        final String TABLE_NAME = args[0];
        final String TABLE_NAME2 = args[1];
        final Integer MODE;
        if (args[2].equalsIgnoreCase("create")){
            MODE = KuduOutputFormat.CREATE;
        }else if (args[2].equalsIgnoreCase("append")){
            MODE = KuduOutputFormat.APPEND;
        } else if (args[2].equalsIgnoreCase("override")){
            MODE = KuduOutputFormat.OVERRIDE;
        } else{
            System.out.println("Error in param [Mode]. Only create, append or override allowed.");
            return;
        }
        final String KUDU_MASTER = args[3];
        long startTime = System.currentTimeMillis();

        KuduInputFormat KuduInputTest = new KuduInputFormat(TABLE_NAME, KUDU_MASTER);

        String [] columnNames = new String[2];
        columnNames[0] = "key";
        columnNames[1] = "value";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<RowSerializable> typeInformation = TypeInformation.of(RowSerializable.class);

        DataSet<RowSerializable> source = env.createInput(KuduInputTest, typeInformation);

        DataSet<RowSerializable> input = source.map(new MyMapFunction());



        input.output(new KuduOutputFormat(KUDU_MASTER, TABLE_NAME2, columnNames, MODE));


        env.execute();

        long endTime = System.currentTimeMillis();

        System.out.println("Program executed in " + (endTime - startTime)/1000 + " seconds");  //divide by 1000000 to get milliseconds.
    }

    private static class MyMapFunction implements MapFunction<RowSerializable, RowSerializable> {

        @Override
        public RowSerializable map(RowSerializable row) throws Exception {

            RowSerializable res = row;

            for (int i = 0; i < row.productArity(); i++) {
                if (row.productElement(i).getClass().equals(String.class))
                    res.setField(1, row.productElement(1).toString().toUpperCase());
                else if (row.productElement(i).getClass().equals(Integer.class))
                    res.setField(0, (Integer)row.productElement(0)*1);
            else continue;
            }
            return res;
        }
    }
}
