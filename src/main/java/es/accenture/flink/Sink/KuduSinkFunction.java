package es.accenture.flink.Sink;

import es.accenture.flink.Sink.utils.Utils;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by sshvayka on 21/11/16.
 */
public class KuduSinkFunction extends RichOutputFormat<RowSerializable> {

    private String host, tableName, tableMode;
    private Utils utils = null;
    private String [] fieldsNames;
    private KuduTable table;
    final static Logger logger = Logger.getLogger(KuduSinkFunction.class);

    /**
     * Builder to use when you want to create a new table as it contains schema data
     * Sink class builder that it will be used when you want to create a new table as it contains schema data
     * @param host         Kudu host
     * @param  tableName   Kudu table name
     * @param fieldsNames  List of column names in the table to be created
     * @param tableMode    Way to operate with table(CREATE,APPEND,OVERRIDE)
     * @throws KuduException
     */
    public KuduSinkFunction(String host, String tableName, String [] fieldsNames, String tableMode) throws KuduException {
        if(!(tableMode.equals("CREATE") || tableMode.equals("APPEND") || tableMode.equals("OVERRIDE"))){
            logger.error("ERROR: missing table mode parameter at the constructor method");
            System.exit(-1);
        }
        this.host = host;
        this.utils = new Utils(host);
        this.tableName = tableName;
        this.fieldsNames = fieldsNames;
        this.tableMode = tableMode;
    }

    /**
     * Builder to be used when using an existing table
     * Sink class builder that it will be used when you want to create a new table as it contains schema data
     * @param host         Kudu host
     * @param  tableName   Kudu table name to be used
     * @param tableMode    Way to operate with table(CREATE,APPEND,OVERRIDE)
     * @throws KuduException
     */

    public KuduSinkFunction(String host, String tableName, String tableMode) throws KuduException {
        if(!(tableMode.equals("CREATE") || tableMode.equals("APPEND") || tableMode.equals("OVERRIDE"))){
            logger.error("ERROR: missing table mode parameter at the constructor method");
            System.exit(-1);
        }
        if(tableMode.equals("CREATE")){
            logger.error("ERROR: missing fields parameter at the constructor method");
            System.exit(-1);
        }
        this.host = host;
        this.utils = new Utils(host);
        this.tableName = tableName;
        this.tableMode = tableMode;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    /**
     * It's responsible to insert  a row into the indicated table by the builder
     * @param row  Data of a row to insert
     * @throws IOException
     */
    
    @Override
    public void writeRecord(RowSerializable row) throws IOException {

        //Look at the situation of the table(exist or not) depending of the mode,the table is create or not
        this.table = utils.useTable(tableName, fieldsNames, row, tableMode);
        //Case APPEND, with builder without column names , because otherwise it exits NullPointerException
        if(fieldsNames == null){
            fieldsNames = utils.getNamesOfColumns(table);
        }
        //Initialization of the insert object
        Insert insert = table.newInsert();

        for (int index = 0; index < row.productArity(); index++){

            //Create the insert with the previous data in function of the type ,a different "add"
            switch(utils.getRowsPositionType(index, row).getName()){
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
        utils.insertToTable(insert);
        logger.info("Inserted the Row: " + utils.printRow(row));
    }
}
