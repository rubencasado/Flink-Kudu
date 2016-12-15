package es.accenture.flink.Sink;

import es.accenture.flink.Utils.RowSerializable;
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

    //Kudu variables
    private KuduClient client;
    private KuduSession session;

    // LOG4J
    final static Logger logger = Logger.getLogger(Utils.class);

    /**
     * Builder Util Class which creates a Kudu client and log in to be able to perform operations later
     * @param host Kudu host
     * @throws KuduException
     */

    public Utils(String host) throws KuduException {
        this.client = new KuduClient.KuduClientBuilder(host).build();
        this.session = this.client.newSession();
    }

    /**
     * Return an instance of the table indicated in the settings
     *
     * <li> In case that the table exists, return an instance of the table </li>
     * <li> In case that the table doesn't exist, create a new table with the data provided and return an instance </li>
     * <li> In both cases,takes into account the way of the table to perfom some operations or others </li>
     * <ul>
     *     <li> If the mode is CREATE: </li>
     *     <ul>
     *         <li> If the table exists -> return error (Can not create table that already exists)</li>
     *         <li> If the table doesn't exist and  the list of column names has not been provided-> return error </li>
     *         <li> If the table doesn't exist and  the list of column names has been provided->create a new table with data provided and return an instance  </li>
     *    </ul>
     *    <li> If the mode is APPEND: </li>
     *    <ul>
     *        <li> If the table exists-> return the instance in the table </li>
     *        <li> If the table doesn't exist -> return error </li>
     *    </ul>
     *    <li> If the mode is OVERRIDE: </li>
     *    <ul>
     *        <li> If the table exist-> delete all rows of this table and return an instance of it </li>
     *        <li> If the table doesn't exist-< return error </li>
     *
     *     </ul>
     * </ul>
     *
     * @param tableName     Table name to use
     * @param fieldsNames   List of names of columns of the table (to create table)
     * @param row           List of values to insert a row in the table (to know the types of columns)
     * @param tableMode     Operations mode for operate with the table (CREATE, APPEND, OVERRIDE)
     * @return              Instance of the table indicated
     * @throws Exception    In case of wrong parameters or wrong combination (for example, APPEND mode to a non-existent table)
     */
    public KuduTable useTable(String tableName, String [] fieldsNames, RowSerializable row, String tableMode) throws Exception {
        KuduTable table;

        switch(tableMode){
            case "CREATE":
                System.out.println("Modo CREATE");
                if (client.tableExists(tableName)){
                    logger.error("ERROR: The table already exists.");
                    throw new Exception("ERROR: The table already exists.");
                } else{
                    if (tableName == null || tableName.equals("")){
                      throw new Exception("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"tableName\" parameter.");

                    } else if(fieldsNames == null || fieldsNames[0].isEmpty()){
                        throw new Exception("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"fields\" parameter.");

                    } else if (row == null){
                        throw new Exception("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"row\" parameter.");

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
                    throw new Exception("ERROR: The table doesn't exist, so can't do APPEND operation");
                }
                break;

            case "OVERRIDE":
                System.out.println("Modo OVERRIDE");
                if (client.tableExists(tableName)){
                    logger.info("SUCCESS: There is the table with the name \"" + tableName + "\". Emptying the table");
                    clearTable(tableName);
                    table = client.openTable(tableName);
                } else{
                    throw new Exception("ERROR: The table doesn't exist, so can't do OVERRIDE operation");
                }
                break;
            default:
                throw new Exception("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"tableMode\" parameter.");
        }
        return table;
    }

    /**
     * Returns an instance of the table requested in parameters
     * <li> If the table exists, returns an instance of the table </li>
     * <li> If the table doesn't exist, creates a new table with the data provided and returns an instance </li>
     *
     * @param tableName     Table name to use
     * @param fieldsNames   List of names of columns of the table (to create table)
     * @param row           List of values to insert a row in the table (to know the types of columns)
     * @return              Instance of the table indicated
     * @throws Exception    In case of wrong parameters
     */
    public KuduTable useTable(String tableName, String [] fieldsNames, RowSerializable row) throws Exception {
        KuduTable table;

        if (client.tableExists(tableName)){
            table = client.openTable(tableName);
        } else {
            if (tableName == null || tableName.equals("")) {
                throw new Exception("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"tableName\" parameter.");

            } else if (fieldsNames == null || fieldsNames[0].isEmpty()) {
                throw new Exception("ERROR: Incorrect parameters, please check the constructor method. Missing \"fields\" parameter.");

            } else if (row == null){
                throw new Exception("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"row\" parameter.");

            } else {
                table = createTable(tableName, fieldsNames, row);
            }
        }
        return table;
    }

    /**
     * Create a new Kudu table and return the instance of this table
     *
     * @param tableName     name of the table to create
     * @param fieldsNames   list name columns of the table
     * @param row           list of values to insert a row in the table( to know the types of columns)
     * @return              instance of the table indicated
     * @throws KuduException
     */
    public KuduTable createTable (String tableName, String [] fieldsNames, RowSerializable row) throws KuduException{
        KuduTable table = null;
        List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
        List<String> rangeKeys = new ArrayList<String>(); // Primary key
        rangeKeys.add(fieldsNames[0]);

        logger.info("Creating the table \"" + tableName + "\"...");
        for (int i = 0; i < fieldsNames.length; i++){
            ColumnSchema col;
            String colName = fieldsNames[i];
            Type colType = getRowsPositionType(i, row);

            if (colName.equals(fieldsNames[0])) {
                col = new ColumnSchemaBuilder(colName, colType).key(true).build();
                columns.add(0, col);//To create the table, the key must be the first in the column list otherwise it will give a failure
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
     * Delete the indicated table
     *
     * @param tableName name table to delete
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
     * Return the type of the value of the position "pos", like the class object "Type"
     *
     * @param pos   Row position
     * @param row   list of values to insert a row in the table
     * @return      element type "pos"-esimo of "row"
     */
    public Type getRowsPositionType (int pos, RowSerializable row){
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
     * Return a list with all rows of the indicated table
     *
     * @param tableName Table name to read
     * @return          List of rows in the table(object Row)
     * @throws KuduException
     */
    public List<RowSerializable> readTable (String tableName) throws KuduException {

        KuduTable table = client.openTable(tableName);
        KuduScanner scanner = client.newScannerBuilder(table).build();
        //Obtain the column name list
        String[] columnsNames = getNamesOfColumns(table);
        //The list return all rows
        List<RowSerializable> rowsList = new ArrayList<>();
        String content = "The table contains:";

        int number = 1, posRow = 0;
        while (scanner.hasMoreRows()) {
            for (RowResult row : scanner.nextRows()) { //Get the rows
                RowSerializable rowToInsert = new RowSerializable(columnsNames.length);
                content += "\nRow " + number + ": \n";
                for (String col : columnsNames) { //For each column, it's type determined and this is how to read it

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
     * Returns a representation on the table screen of a table
     *
     * @param row   row to show
     * @return      a string containing the data of the row indicated in the parameter
     */
    public String printRow (RowSerializable row){
        String res = "";
        for(int i = 0; i< row.productArity(); i++){
            res += (row.productElement(i) + " | ");
        }
        return res;
    }

    /**
     * Deelte all rows of the table until empty
     *
     * @param tableName  table name to empty
     * @throws KuduException
     */
    public void clearTable (String tableName) throws KuduException {
        KuduTable table = client.openTable(tableName);
        List<RowSerializable> rowsList = readTable(tableName);

        String primaryKey = table.getSchema().getPrimaryKeyColumns().get(0).getName();
        List<Delete> deletes = new ArrayList<>();
        for(RowSerializable row : rowsList){
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
            session.apply(d);
        }
        logger.info("SUCCESS: The table has been emptied successfully");
    }

    /**
     * Return a list of columns names in a table
     *
     * @param table table instance
     * @return      List of column names in the table indicated in the parameter
     */
    public String [] getNamesOfColumns (KuduTable table){
        List<ColumnSchema> columns = table.getSchema().getColumns();
        List<String> columnsNames = new ArrayList<>(); //  List of column names
        for (ColumnSchema schema : columns) {
            columnsNames.add(schema.getName());
        }
        String [] array = new String[columnsNames.size()];
        array = columnsNames.toArray(array);
        return array;
    }

    public void insert (KuduTable table, RowSerializable row, String [] fieldsNames) throws KuduException {

        Insert insert = table.newInsert();

        for (int index = 0; index < row.productArity(); index++){

            //Create the insert with the previous data in function of the type ,a different "add"
            switch(getRowsPositionType(index, row).getName()){
                case "string":
                    insert.getRow().addString(fieldsNames[index], (String)(row.productElement(index)));
                    break;
                case "int32":
                    insert.getRow().addInt(fieldsNames[index], (Integer) row.productElement(index));
                    break;
                case "bool":
                    insert.getRow().addBoolean(fieldsNames[index], (Boolean) row.productElement(index));
                    break;
                default:
                    break;
            }
        }
        //When the Insert is complete, write it in the table
        session.apply(insert);
    }

    /**
     * Returns an instance of the kudu client
     *
     * @return  Kudu client
     */
    public KuduClient getClient() {
        return client;
    }

}