package es.accenture.flink.Sink.utils;

import org.apache.flink.api.table.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sshvayka on 21/11/16.
 */
public class Utils {

    // Atributos de Kudu
    private KuduClient client;
    private KuduSession session;

    // LOG4J
    final static Logger logger = Logger.getLogger(Utils.class);

    /**
     * Constructor de la clase Utils, que crea un cliente de Kudu e inicia una sesion para poder realizar operaciones posteriormente
     *
     * @param host host de Kudu
     * @throws KuduException
     */
    public Utils(String host) throws KuduException {
        this.client = createClient(host);
        this.session = createSession();
    }

    /**
     * Devuelve un objeto de la clase KuduClient, que se usara para realizar operaciones posteriormente
     *
     * @param host  host de Kudu
     * @return      cliente de Kudu
     */
    private KuduClient createClient (String host){
        return new KuduClientBuilder(host).build();
    }

    /**
     * Devuelve un objeto de la clase KuduSession, que se usara para realizar operaciones posteriormente
     *
     * @return sesion de Kudu
     */
    private KuduSession createSession (){
        return client.newSession();
    }

    /**
     * Devuelve una instancia de la tabla indicada en los parametros.
     * <li> - En caso de existir, devuelve una instancia de la tabla para ser usada posteriormente </li>
     * <li> - En caso de no existir, se crea una nueva tabla con los datos proporcionados y se devuelve la instancia </li>
     * <li> - En ambos casos, se tiene en cuenta el modo de la tabla para realizar unas operaciones u otras: </li>
     * <ul>
     *  <li> Si el modo es CREATE: </li>
     *  <ul>
     *      <li> Si la tabla existe -> devuelve error ( ya que no se puede crear una tabla que ya existe ) </li>
     *      <li> Si la tabla no existe y no se ha proporcionado la lista de nombres de columnas -> devuelve error </li>
     *      <li> Si la tabla no existe y se ha proporcionado la lista de nombres de columnas -> se crea la tabla con los parametros dados y se devuelve la instancia de dicha tabla </li>
     *  </ul>
     *
     *  <li> Si el modo es APPEND: </li>
     *  <ul>
     *      <li> Si la tabla existe -> se devuelve la instancia de la tabla </li>
     *      <li> Si la tabla no existe -> devuelve error </li>
     *  </ul>
     *  <li> Si el modo es OVERRIDE: </li>
     *  <ul>
     *      <li> Si la tabla existe -> se borran todas las filas de dicha tabla y se devuelve una instancia de ella </li>
     *      <li> Si la tabla no existe -> devuelve error </li>
     *  </ul>
     * </ul>
     * @param tableName     nombre de la tabla a usar
     * @param fieldsNames   lista de nombres de columnas de la tabla (para crear la tabla)
     * @param row           lista de valores a insertar en una fila de la tabla (para saber tipos de las columnas)
     * @param tableMode     modo de operacion para operar con la tabla (CREATE, APPEND u OVERRIDE)
     * @return              instancia de la tabla indicada
     * @throws KuduException
     */
    public KuduTable useTable(String tableName, String [] fieldsNames, Row row, String tableMode) throws KuduException  {
        KuduTable table = null;

        switch(tableMode){
            case "CREATE":
                System.out.println("Modo CREATE");
                if (client.tableExists(tableName)){
                    logger.error("ERROR: The table already exists.");
                    System.exit(-1);
                } else{
                    if(fieldsNames == null || fieldsNames[0].isEmpty()){
                        // No ha de darse ya que hay que proporcionar los parametros "fields" y "primary" con el modo CREATE
                        logger.error("ERROR: Incorrect parameters, please check the constructor method. Missing \"fields\" parameter.");
                        System.exit(-1);
                    } else {
                        table = createTable(tableName, fieldsNames, row);
                    }
                }

                break;

            case "APPEND":
                System.out.println("Modo APPEND");
                if (client.tableExists(tableName)){
                    logger.info("SUCCESS: There is the table with the name \"" + tableName + "\"");
                    table = client.openTable(tableName);
                } else{
                    logger.error("ERROR: The table doesn't exist, so can't do APPEND operation");
                    System.exit(-1);
                }

                break;

            case "OVERRIDE":
                System.out.println("Modo OVERRIDE");
                if (client.tableExists(tableName)){
                    logger.info("SUCCESS: There is the table with the name \"" + tableName + "\". Emptying the table");
                    clearTable(tableName);
                    table = client.openTable(tableName);
                } else{
                    logger.error("ERROR: The table doesn't exist, so can't do OVERRIDE operation");
                    System.exit(-1);
                }

                break;
        }
        return table;
    }

    /**
     * Crea una tabla en Kudu y devuelve la instancia de esta tabla
     *
     * @param tableName     nombre de la tabla a crear
     * @param fieldsNames   lista de nombres de columnas de la tabla
     * @param row           lista de valores a insertar en una fila de la tabla (para saber tipos de las columnas)
     * @return              instancia de la tabla indicada
     * @throws KuduException
     */
    public KuduTable createTable (String tableName, String [] fieldsNames, Row row) throws KuduException{
        KuduTable table = null;
        List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
        List<String> rangeKeys = new ArrayList<String>(); // Clave primaria
        rangeKeys.add(fieldsNames[0]);

        logger.info("Creating the table \"" + tableName + "\"...");
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
            logger.info("SUCCESS: The table has been created successfully");
        } else {
            logger.error("ERROR: The table already exists");
        }

        return table;
    }

    /**
     * Borra la tabla indicada
     *
     * @param tableName nombre de la tabla a borrar
     */
    public void deleteTable (String tableName){

        logger.info("Deleting the table \"" + tableName + "\"...");
        try {
            client.deleteTable(tableName);
            logger.info("SUCCESS: Table deleted successfully");
        } catch (KuduException e) {
            logger.error("The table \"" + tableName  +"\" doesn't exist, so can't be deleted.", e);
        }
    }

    /**
     * Devuelve el tipo del valor en la posicion "pos", como objeto de la clase "Type"
     *
     * @param pos   posicion del Row
     * @param row   lista de valores de la fila de una tabla
     * @return      tipo del elemento "pos"-esimo de "row"
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

    /**
     * Devuelve una lista con todas las filas de la tabla indicada
     *
     * @param tableName nombre de la tabla a leer
     * @return          lista de filas de la tabla (objetos Row)
     * @throws KuduException
     */
    public List<Row> readTable (String tableName) throws KuduException {

        KuduTable table = client.openTable(tableName);
        KuduScanner scanner = client.newScannerBuilder(table).build();
        // Obtenemos la lista de los nombres de las columnas
        String[] columnsNames = getNamesOfColumns(table);
        // La lista que se devolvera, con todos los Rows
        List<Row> rowsList = new ArrayList<>();
        String content = "The table contains:";

        int number = 1, posRow = 0;
        while (scanner.hasMoreRows()) {
            for (RowResult row : scanner.nextRows()) { //Se sacan las Rows
                Row rowToInsert = new Row(columnsNames.length);
                content += "\nRow " + number + ": \n";
                for (String col : columnsNames) { // Por cada columna, se determina su tipo, y asi se sabe como leerlo

                    String colType = row.getColumnType(col).getName();
                    switch (colType) {
                        case "string":
                            content += row.getString(col) + "|";
                            rowToInsert.setField(posRow, row.getString(col));
                            posRow++;
                            break;
                        case "int32":
                            content += row.getInt(col) + "|";
                            rowToInsert.setField(posRow, row.getInt(col));
                            posRow++;
                            break;
                        case "bool":
                            content += row.getBoolean(col) + "|";
                            rowToInsert.setField(posRow, row.getBoolean(col));
                            posRow++;
                            break;
                        default:
                            break;
                    }
                }
                rowsList.add(rowToInsert);
                number++;
                posRow = 0;
            }
        }
        logger.info(content);
        return rowsList;
    }

    /**
     * Devuelve una representacion en pantalla de la fila de una tabla
     *
     * @param row   fila a mostrar
     * @return      una cadena que contiene los datos de la fila indicada en el parametro
     */
    public String printRow (Row row){
        String res = "";
        for(int i = 0; i< row.productArity(); i++){
            res += (row.productElement(i) + " | ");
        }
        return res;
    }

    /**
     * Borra todas las filas de la tabla, hasta dejarla vacia
     *
     * @param tableName nombre de la tabla a vaciar
     * @throws KuduException
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
        logger.info("SUCCESS: The table has been emptied successfully");
    }

    /**
     * Devuelve una lista de nombres de columnas de una tabla
     *
     * @param table instancia de la tabla
     * @return      lista de nombres de columnas de la tabla indicada en el parametro
     */
    public String [] getNamesOfColumns(KuduTable table){
        List<ColumnSchema> columns = table.getSchema().getColumns();
        List<String> columnsNames = new ArrayList<>(); // Lista de nombres de las columnas
        for (ColumnSchema schema : columns) {
            columnsNames.add(schema.getName());
        }
        String [] array = new String[columnsNames.size()];
        array = columnsNames.toArray(array);
        return array;
    }

    /**
     * Inserta en una tabla de Kudu los datos proporcionados
     *
     * @param insert  objeto insert que contiene los datos a insertar en la tabla
     * @throws KuduException
     */
    public void insertToTable(Insert insert) throws KuduException {
        session.apply(insert);
    }

    /**
     * Borra de una tabla los datos proporcinados
     *
     * @param delete    objeto delete que contiene los datos a borrar de la tabla
     * @throws KuduException
     */
    public void deleteFromTable(Delete delete) throws KuduException {
        session.apply(delete);
    }

    /**
     * Devuelve una instancia del cliente de Kudu
     *
     * @return cliente de Kudu
     */
    public KuduClient getClient() {
        return client;
    }

}