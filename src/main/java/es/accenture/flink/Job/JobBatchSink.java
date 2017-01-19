package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduOutputFormat;
import es.accenture.flink.Utils.ModeType;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import static es.accenture.flink.Utils.ModeType.CREATE;

/**
 * Created by dani on 9/12/16.
 */
public class JobBatchSink {


    public static void main(String[] args) throws Exception {

        /********Only for test, delete once finished*******/
        args[0]="Table_20";
        args[1]="Create";
        args[2]="localhost";
        /**************************************************/



        System.out.println("-----------------------------------------------");
        System.out.println("1. Creates a new data set from elements.\n" +
                "2. Concat string 'NEW' to each field.\n" +
                "3. Write back in a new Kudu DB (" + args[1] + ").");
        System.out.println("-----------------------------------------------");

        if(args.length!=3){
            System.out.println("PARAM ERROR: 3 params required but " + args.length + " given");
            System.out.println( "Run program with arguments: [TableWrite] [Mode] [Master Adress]\n" +
                    "- TableToWrite: Name of the table to write.\n" +
                    "- Mode:  'Create'      If 'TableToWrite' does not exist. It is created and filled with the information.\n" +
                    "         'Append'      Adds rows to the existing Kudu Database 'TableToWrite'.\n" +
                    "         'Override'    Clear the given table 'TableToWrite' and appends it rows.\n" +
                    "- Master Address: RPC Address for Master consensus-configuration (For example: localhost)\n");

            return;
        }

        final String TABLE_NAME = args[0];
        final ModeType MODE;
        if (args[1].equalsIgnoreCase("create")){
            MODE = ModeType.CREATE;
        }else if (args[1].equalsIgnoreCase("append")){
            MODE = ModeType.APPEND;
        } else if (args[1].equalsIgnoreCase("override")){
            MODE = ModeType.OVERRIDE;
        } else{
            System.out.println("Error in param [Mode]. Only create, append or override allowed.");
            return;
        }
        final String KUDU_MASTER = args[2];


        String[] columnNames = new String[3];
        columnNames[0] = "key";
        columnNames[1] = "value";
        columnNames[2] = "description";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> input = env.fromElements("fila100 value100 description100");

        DataSet<RowSerializable> out = input.map(new MyMapFunction());

        out.output(new KuduOutputFormat(KUDU_MASTER, TABLE_NAME, columnNames, MODE));

        env.execute();

    }

    private static class MyMapFunction implements MapFunction<String, RowSerializable> {
        @Override
        public RowSerializable map(String inputs) throws Exception {
            RowSerializable r = new RowSerializable(3);
            Integer i = 0;
            for (String s : inputs.split(" ")) {

                r.setField(i, s.concat("NEW"));
                i++;
            }
            return r;
        }
    }

}



