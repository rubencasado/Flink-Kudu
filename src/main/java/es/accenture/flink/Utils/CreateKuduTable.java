package es.accenture.flink.Utils;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;

import java.util.ArrayList;
import java.util.List;


public class CreateKuduTable {
    public static void main(String[] args) {

        String tableName = ""; // TODO insert table name
        String host = "localhost";

        KuduClient client = new KuduClient.KuduClientBuilder(host).build();
        try {
            List<ColumnSchema> columns = new ArrayList(2);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("valueInt", Type.INT32)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("valueString", Type.STRING)
                    .build());
            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("valueInt");
            Schema schema = new Schema(columns);
            client.createTable(tableName, schema,
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys).addHashPartitions(rangeKeys, 4));
            System.out.println("Table \"" + tableName + "\" created succesfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}