package es.accenture.flink.Sink;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by sshvayka on 21/11/16.
 */
public class Utils {

    // Atributos de Kudu
    private KuduClient client;
    private KuduSession session;

    public Utils(String host) throws KuduException {
        this.client = createClient(host);
        this.session = createSession();
    }

    private KuduClient createClient (String host){
        return new KuduClientBuilder(host).build();
    }

    private KuduSession createSession (){
        return client.newSession();
    }

    public KuduTable createTable(String tableName, Map<String,Type> fields, String primary) throws KuduException  {
        KuduTable table = null;

        if (client.tableExists(tableName)){
            System.out.println("Ya existe una tabla con el nombre \"" + tableName + "\"");
            table = client.openTable(tableName);
        } else {

            List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
            List<String> rangeKeys = new ArrayList<String>();
            rangeKeys.add(primary);

            System.out.println("Creando la tabla \"" + tableName + "\"...");
            for (Map.Entry<String, Type> entry : fields.entrySet()) {
                ColumnSchema col;
                String colName = entry.getKey();
                Type colType = entry.getValue();

                if (colName.equals(primary)) {
                    col = new ColumnSchemaBuilder(colName, colType).key(true).build();
                    columns.add(0, col); // Para crear la tabla, la clave tiene que ir la primera en la lista de columnas, sino da fallo!!!
                } else {
                    col = new ColumnSchemaBuilder(colName, colType).build();
                    columns.add(col);
                }
            }

            Schema schema = new Schema(columns);

            if ( ! client.tableExists(tableName)) {
                table = client.createTable(tableName, schema, new CreateTableOptions().setRangePartitionColumns(rangeKeys));
                System.out.println("La tabla ha sido creada con exito");
            } else {
                System.out.println("Error al crear la tabla");
            }
        }
        return table;
    }

    public void deleteTable (String table){

        System.out.println("Borrando la tabla \"" + table + "\"... ");
        try {
            client.deleteTable(table);
            System.out.println("Tabla borrada con exito");
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void insertToTable(Insert insert) throws KuduException {
        session.apply(insert);
    }

    public KuduClient getClient() {
        return client;
    }

}