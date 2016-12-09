package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduSinkFunction;
import es.accenture.flink.Sink.utils.Utils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.java.DataSet;

import java.util.List;

/**
 * Created by sergiy on 9/12/16.
 */
public class JobSink {

    public static final String KUDU_MASTER = System.getProperty("kuduMaster", "localhost");

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Utils u = new Utils(KUDU_MASTER);

        String [] fields = {"nick", "age", "male"};
        String tableName = "users";

        Row row1 = new Row(3);
        row1.setField(0, "sergiy");
        row1.setField(1, 21);
        row1.setField(2, true);
        Row row2 = new Row(3);
        row2.setField(0, "virginia");
        row2.setField(1, 22);
        row2.setField(2, false);

        DataSet<Row> data = env.fromElements(row1, row2);

//        u.createTable(tableName, fields, row1);
//        List<Row> lista = data.collect();
//
//        System.out.println("EL DATASET TIENE:");
//        for(Row r : lista){
//            System.out.println(u.printRow(r));
//        }


        data.output(new KuduSinkFunction(KUDU_MASTER, tableName, fields, "APPEND"));

        System.out.println("LA TABLA TIENE:");
        List<Row> tableResult = u.readTable(tableName);
        for(Row row : tableResult){
            System.out.println(u.printRow(row));
        }

    }
}
