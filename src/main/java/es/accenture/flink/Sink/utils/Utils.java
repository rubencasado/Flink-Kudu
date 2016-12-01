package es.accenture.flink.Sink.utils;

import org.apache.flink.api.table.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;

import java.util.ArrayList;
import java.util.List;

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

    public KuduTable useTable(String tableName, String [] fieldsNames, Row row, String tableMode) throws KuduException  {
        KuduTable table = null;

        switch(tableMode){
            case "CREATE":
                System.out.println("Modo CREATE");
                if (client.tableExists(tableName)){
                    System.err.println("Error, la tabla ya existe");
                    System.exit(-1);
                } else{
                    if(fieldsNames == null || fieldsNames[0].isEmpty()){
                        // No ha de darse ya que hay que proporcionar los parametros "fields" y "primary" con el modo CREATE
                        System.err.println("Parametros incorrectos, por favor, revise el constructor usado ya que no concuerda el modo de la tabla con los datos proporcionados");
                        System.exit(-1);
                    } else {
                        table = createTable(tableName, fieldsNames, row);
                    }
                }

                break;

            case "APPEND":
                System.out.println("Modo APPEND");
                if (client.tableExists(tableName)){
                    System.out.println("Existe una tabla con el nombre \"" + tableName + "\"");
                    table = client.openTable(tableName);
                } else{
                    System.err.println("ERROR: la tabla no existe asi que no se puede hacer APPEND");
                    System.exit(-1);
                }

                break;

            case "OVERRIDE":
                System.out.println("Modo OVERRIDE");
                if (client.tableExists(tableName)){
                    clearTable(tableName);
                    table = client.openTable(tableName);
                } else{
                    System.err.println("ERROR: la tabla no existe asi que no se puede hacer OVERRIDE");
                    System.exit(-1);
                }

                break;

        }

        return table;
    }

    public KuduTable createTable (String tableName, String [] fieldsNames, Row row) throws KuduException{
        KuduTable table = null;
        List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
        List<String> rangeKeys = new ArrayList<String>(); // Clave primaria
        rangeKeys.add(fieldsNames[0]);

        System.out.println("Creando la tabla \"" + tableName + "\"...");
        for (int i = 0; i < fieldsNames.length; i++){
            ColumnSchema col;
            String colName = fieldsNames[i];
            Type colType = getRowsPositionType(i, row);

            if (colName.equals(fieldsNames[0])) {
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
            System.err.println("Error al crear la tabla (ya existe)");
        }

        return table;
    }

    public void deleteTable (String tableName){

        System.out.println("Borrando la tabla \"" + tableName + "\"... ");
        try {
            client.deleteTable(tableName);
            System.out.println("Tabla borrada con exito");
        } catch (KuduException e) {
            System.err.println("La tabla " + tableName + " no existe, por lo que no puede ser borrada");
        }
    }

    /**
     * Dado un Row, devuelve el tipo del valor en la posicion "pos", como objeto de la clase "Type", para su posterior manejo en Kudu
     */
    public Type getRowsPositionType (int pos, Row row){
        Type colType = null;
        switch(row.productElement(pos).getClass().getName()){
            case "java.lang.String":
                colType = Type.STRING;
                break;
            case "java.lang.Integer":
                colType = Type.INT32;
                break;
            case "java.lang.Boolean":
                colType = Type.BOOL;
                break;
            default:
                break;
        }
        return colType;
    }

    public List<Row> readTable (String tableName) throws KuduException {

        KuduTable table = client.openTable(tableName);
        KuduScanner scanner = client.newScannerBuilder(table).build();
        // Obtenemos la lista de los nombres de las columnas
        String[] columnsNames = getNamesOfColumns(table);
        // La lista que se devolvera, con todos los Rows
        List<Row> rowsList = new ArrayList<>();

        System.out.println();

        int number = 1, posRow = 0;
        System.out.println("La tabla contiene:");
        while (scanner.hasMoreRows()) {
            for (RowResult row : scanner.nextRows()) { //Se sacan las Rows
                Row rowToInsert = new Row(columnsNames.length);

                System.out.print("Row " + number + ": ");
                for (String col : columnsNames) { // Por cada columna, se determina su tipo, y asi se sabe como leerlo

                    String colType = row.getColumnType(col).getName();
                    switch (colType) {
                        case "string":
                            System.out.print(row.getString(col) + "|");
                            rowToInsert.setField(posRow, row.getString(col));
                            posRow++;
                            break;
                        case "int32":
                            System.out.print(row.getInt(col) + "|");
                            rowToInsert.setField(posRow, row.getInt(col));
                            posRow++;
                            break;
                        case "bool":
                            System.out.print(row.getBoolean(col) + "|");
                            rowToInsert.setField(posRow, row.getBoolean(col));
                            posRow++;
                            break;
                        default:

                    }
                }
                System.out.println();
                rowsList.add(rowToInsert);
                number++;
                posRow = 0;
            }
        }
        return rowsList;
    }

    public String printRow (Row row){
        String res = "";
        for(int i = 0; i< row.productArity(); i++){
            res += (row.productElement(i) + " | ");
        }
        return res;
    }

    /**
     * Borra todas las Rows de la tabla, hasta dejarla vacia
     */
    public void clearTable (String tableName) throws KuduException {
        KuduTable table = client.openTable(tableName);
        List<Row> rowsList = readTable(tableName);

        String primaryKey = table.getSchema().getPrimaryKeyColumns().get(0).getName();
        List<Delete> deletes = new ArrayList<>();
        for(Row row : rowsList){
            Delete d = table.newDelete();
            switch(getRowsPositionType(0, row).getName()){
                case "string":
                    d.getRow().addString(primaryKey, (String) row.productElement(0));
                    break;

                case "int32":
                    d.getRow().addInt(primaryKey, (Integer) row.productElement(0));
                    break;

                case "bool":
                    d.getRow().addBoolean(primaryKey, (Boolean) row.productElement(0));
                    break;

                default:
                    break;
            }
            deletes.add(d);
        }
        for(Delete d : deletes){
            deleteFromTable(d);
        }
        System.out.println("Borrado completo de la tabla");
    }

    public String [] getNamesOfColumns(KuduTable table){
        List<ColumnSchema> columns = table.getSchema().getColumns();
        List<String> columnsNames = new ArrayList<>(); // Lista de nombres de las columnas
//        System.out.println("Lista de columnas:");
        for (ColumnSchema schema : columns) {
//            System.out.println(schema.getName());
            columnsNames.add(schema.getName());
        }
        String [] array = new String[columnsNames.size()];
        array = columnsNames.toArray(array);
        return array;
    }

    public void insertToTable(Insert insert) throws KuduException {
        session.apply(insert);
    }

    public void deleteFromTable(Delete delete) throws KuduException {
        session.apply(delete);
    }

    public KuduClient getClient() {
        return client;
    }

}