package es.accenture.flink.Utils;

import org.apache.kudu.client.KuduClient;

/**
 * Created by sergiy on 17/02/17.
 */
public class DeleteKuduTable {

    public static void main(String[] args) {
        String tableName = ""; // TODO insert table name
        String host = "localhost";


        KuduClient client = new KuduClient.KuduClientBuilder(host).build();
        try {
            client.deleteTable(tableName);
            System.out.println("Table \"" + tableName + "\" deleted succesfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
