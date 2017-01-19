package es.accenture.flink.Sink;

import es.accenture.flink.Utils.RowSerializable;
import es.accenture.flink.Utils.Utils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.client.KuduTable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by sergiy on 14/12/16.
 */
public class KuduSink extends RichSinkFunction<RowSerializable>{

    private String host, tableName;
    private String [] fieldsNames;
    private transient Utils utils;

    //Kudu variables
    private transient KuduTable table;

    // LOG4J
    final static Logger logger = Logger.getLogger(KuduSink.class);

    /**
     * Builder to use when you want to create a new table
     *
     * @param host          Kudu host
     * @param tableName     Kudu table name
     * @param fieldsNames   List of column names in the table to be created
     */
    public KuduSink (String host, String tableName, String [] fieldsNames){

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"host\" not valid (null or empty)");

        } else if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"tableName\" not valid (null or empty)");

        }
        this.host = host;
        this.tableName = tableName;
        this.fieldsNames = fieldsNames;
    }

    /**
     * Builder to be used when using an existing table
     *
     * @param host          Kudu host
     * @param tableName     Kudu table name
     */
    public KuduSink (String host, String tableName){

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"host\" not valid (null or empty)");

        } else if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"tableName\" not valid (null or empty)");
        }
        this.host = host;
        this.tableName = tableName;
    }

    /**
     * It's responsible to insert a row into the indicated table by the builder (Streaming)
     *
     * @param row   Data of a row to insert
     * @throws IOException
     */
    @Override
    public void invoke(RowSerializable row) throws IOException {

        // Establish connection with Kudu
        this.utils = new Utils(host);

        // Look at the situation of the table (exist or not). Depending of the mode, the table is created or opened
        this.table = utils.useTable(tableName, fieldsNames, row);

        if(fieldsNames == null || fieldsNames.length == 0){
            fieldsNames = utils.getNamesOfColumns(table);
        } else {
            // When column names provided, and table exists, must check if column names match
            utils.checkNamesOfColumns(utils.getNamesOfColumns(table), fieldsNames);
        }

        // Make the insert into the table
        utils.insert(table, row, fieldsNames);

        utils.getClient().close();
        logger.info("Inserted the Row: | " + utils.printRow(row) + "at the table \"" + this.tableName + "\"");
    }
}
