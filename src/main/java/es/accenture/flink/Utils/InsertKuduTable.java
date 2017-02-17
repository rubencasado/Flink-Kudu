package es.accenture.flink.Utils;

import org.apache.kudu.client.*;

/**
 * Created by sergiy on 17/02/17.
 */
public class InsertKuduTable {

    public static void main(String[] args) {

        String tableName = ""; // TODO insert table name
        String host = "localhost";


        KuduClient client = new KuduClient.KuduClientBuilder(host).build();
        try {
            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            for (int i = 0; i < 50000; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, i);
                row.addString(1, "This is the row number: "+ i);
                session.apply(insert);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
