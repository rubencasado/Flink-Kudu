package es.accenture.flink.Sink;

import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.client.KuduTable;
import org.apache.log4j.Logger;

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
    final static Logger logger = Logger.getLogger(KuduOutputFormat.class);

    /**
     * Builder to use when you want to create a new table
     *
     * @param host          Kudu host
     * @param tableName     Kudu table name
     * @param fieldsNames   List of column names in the table to be created
     */
    public KuduSink (String host, String tableName, String [] fieldsNames){
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
        this.host = host;
        this.tableName = tableName;
    }

    /**
     * It's responsible to insert a row into the indicated table by the builder (Streaming)
     *
     * @param row   Data of a row to insert
     * @throws Exception
     */
    @Override
    public void invoke(RowSerializable row) throws Exception {
        this.utils = new Utils(host);
        try {
            this.table = utils.useTable(tableName, fieldsNames, row);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(fieldsNames == null){
            fieldsNames = utils.getNamesOfColumns(table);
        } else {
            // When column names provided, and table exists, must check if column names match
            try {
                utils.checkNamesOfColumns(utils.getNamesOfColumns(table), fieldsNames);
            } catch (Exception e){
                logger.error(e.getMessage());
                System.exit(1);
            }
        }

        // Make the insert into the table
        utils.insert(table, row, fieldsNames);

        logger.info("Inserted the Row: " + utils.printRow(row));
    }
}
