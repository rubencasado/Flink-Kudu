<<<<<<< HEAD
package es.accenture.flink.Sources;
=======

>>>>>>> ruben/master

/**
 * Created by lballestin, danielcoto & alvarovadillo on 23/11/16.
 */

package es.accenture.flink.Sources;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.Result;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link InputFormat} subclass that wraps the access for KuduTable.
 */
<<<<<<< HEAD
public abstract class KuduTableInputFormat<T extends Tuple> extends RichInputFormat<T, KuduInputSplit> {
=======
public class KuduTableInputFormat implements InputFormat<RowResult, KuduInputSplit> {
>>>>>>> ruben/master

    private static final String KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
    private static final String TABLE_NAME = System.getProperty("tableName", "sample");

    protected transient KuduTable table = null;
    protected transient KuduScanner scanner = null;
    protected transient KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

    private RowResultIterator results = null;
    private boolean endReached = false;
    private int scannedRows = 0;

    private static final Logger LOG = LoggerFactory.getLogger(KuduTableInputFormat.class);

    /**
     * Returns an instance of Scan that retrieves the required subset of records from the Kudu table.
     * @return The appropriate instance of Scan for this usecase.
     */
    public KuduScanner getScanner(){
        return this.scanner;
    }

    /**
     * What table is to be read.
     * Per instance of a TableInputFormat derivative only a single tableName is possible.
     * @return The name of the table
     */
     public String getTableName(){
         return TABLE_NAME;
     }

    /**
     * The output from Kudu is always an instance of {@link Result}.
     * This method is to copy the data in the Result instance into the required {@link Tuple}
     * @param r The Result instance from Kudu that needs to be converted
     * @return The approriate instance of {@link Tuple} that contains the needed information.
     */
    public void mapResultToTuple(Result r){

        System.out.println("Function call: mapResultToTuple");

    }


    public void createTableTest() throws KuduException {

        System.out.println("Creando tabla de prueba para testear");

        List<ColumnSchema> columns = new ArrayList(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
                .build());
        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("key");

        Schema schema = new Schema(columns);
        client.createTable(this.getTableName(), schema,
                new CreateTableOptions().setRangePartitionColumns(rangeKeys));

        KuduTable table = client.openTable(this.getTableName());
        KuduSession session = client.newSession();
        for (int i = 0; i < 3; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addInt(0, i);
            row.addString(1, "value " + i);
            session.apply(insert);
        }
    System.out.println("Tabla " + this.getTableName()+ " creada");

    }



    /**
<<<<<<< HEAD
     * Creates a {@link KuduScanner} object and opens the {@link KuduTable} connection.
=======
     * Creates a object and opens the {@link KuduTable} connection.
>>>>>>> ruben/master
     * These are opened here because they are needed in the createInputSplits
     * which is called before the openInputFormat method.
     * So the connection is opened in {@link #configure(Configuration)}.
     *
     * @param parameters The configuration that is to be used
     * @see Configuration
     */

    @Override
    public void configure(Configuration parameters) {
        LOG.info("Initializing KUDUConfiguration");
        try {
            if (client.tableExists(TABLE_NAME)) {
                table = client.openTable(TABLE_NAME);
            } else {
                this.createTableTest();
            }
        }catch (Exception e){
            throw new RuntimeException("Could not obtain table");
        }
        if (table != null) {
            scanner = getScanner();
        }
    }

    /**
     * Create an {@link KuduTable} instance and set it into this format
     */
//    private KuduTable createTable() {
//        LOG.info("Initializing HBaseConfiguration");
//        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
//        try{
//            if(client.tableExists(tableName)){
//                table = client.openTable(tablename);
//            }
//            else { table = client.createTable(tableName); }
//            return table;
//
//        } catch (Exception e) {
//            throw new RuntimeException("could not obtain table")
//        }
//    }

    @Override
    public void open(KuduInputSplit split) throws IOException {
<<<<<<< HEAD
=======
        System.out.println("Open");
>>>>>>> ruben/master
        try {
            table = client.openTable(TABLE_NAME);
            KuduSession session = client.newSession();
            System.out.println("Sesion creada");

            System.out.println("Llamando a createInputSplits");
            //TODO Quitar si se ejecuta desde el JOB
            KuduInputSplit[] splits = createInputSplits(3);
            System.out.println("Salido de createInputSplits");
            System.out.println("Se han generado " + splits.length+ " splits");

        } catch (IOException e) {
            LOG.error("Could not open Kudu Table named: " + TABLE_NAME, e);
        }

        endReached = false;
        scannedRows = 0;

        scanner = client.newScannerBuilder(table)
                .build();
        results = scanner.nextRows();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return endReached;
    }

    @Override
    public RowResult nextRecord(RowResult reuse) throws IOException {
        if (scanner == null) {
            throw new IOException("No table scanner provided!");
        }
        try {
            RowResult res = results.next();

            if (res != null) {
                scannedRows++;
                System.out.println("Reading row: " + scannedRows);
                mapResultToTuple((Result) res); // GUARDAR LA FILA EN ALGUN LADO O DEVOLVERLA
                return null;
            }
        } catch (Exception e) {
            endReached = true;
            scanner.close();
            //workaround for timeout on scan
            LOG.warn("Error after scan of " + scannedRows + " rows. Retry with a new scanner...", e);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing split (scanned {} rows)", scannedRows);
        try {
            if (scanner != null) {
                scanner.close();
            }
        } finally {
            scanner = null;
        }
    }

    @Override
    public KuduInputSplit[] createInputSplits(final int minNumSplits) throws IOException {

        int cont = 0;


        try {

            KuduScanToken.KuduScanTokenBuilder builder = client.newScanTokenBuilder(this.table);
            List<KuduScanToken> tokens = builder.build();
            KuduInputSplit[] inputs = new KuduInputSplit[tokens.size()];



            String[] strArray = new String[] {KUDU_MASTER};

            for (KuduScanToken token : tokens){


                byte[] serializadedToken = token.serialize();
                KuduScanner scanner = KuduScanToken.deserializeIntoScanner(serializadedToken, this.client);


                //++++++++++++++++++++++++++++++++++++++++++++++++++++
                byte[] start = token.getTablet().getPartition().getPartitionKeyStart();
                byte[] end = token.getTablet().getPartition().getPartitionKeyEnd();


                KuduInputSplit inputSplit = new KuduInputSplit(cont,strArray,this.table.getName().getBytes()
                        ,start, end);

                //++++++++++++++++++++++++++++++++++++++++++++++++++++
                inputs[cont] = inputSplit;
                cont++;
                System.out.println("CONTADOR:" + cont);
            }

<<<<<<< HEAD
    @Override
    public KuduInputSplit[] createInputSplits(final int minNumSplits) throws IOException {
=======
        return inputs;

        } catch (Exception e) {
            System.out.println("Fallo");
            //e.printStackTrace();
        }
>>>>>>> ruben/master

/*
        try {
            if (table == null) {
                throw new IOException("Table was not provided/opened");
            }
            if (scanner == null) {
                throw new IOException("getScanner returned null");
            }

            KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);

            List<KuduScanToken> tokens = tokenBuilder.build();

            List<KuduInputSplit> splits = new ArrayList<>(tokens.size());
            for (KuduScanToken token : tokens) {
                List<String> locations = new ArrayList<>(token.getTablet().getReplicas().size());
                for (LocatedTablet.Replica replica : token.getTablet().getReplicas()) {
                    locations.add(reverseDNS(replica.getRpcHost(), replica.getRpcPort()));
                }
                splits.add(new TableSplit(token, locations.toArray(new String[locations.size()])));
            }
            return splits.toArray(new KuduInputSplit[0]);
        } finally {
            try {
                client.close();
            } catch (IOException g) {
                throw new IOException(g);
            }
        }
        */

return null;
    }

<<<<<<< HEAD
//    private void logSplitInfo(String action, KuduInputSplit split) {
//        int splitId = split.getSplitNumber();
//        String splitStart = Bytes.toString(split.getStartRow());
//        String splitEnd = Bytes.toString(split.getEndRow());
//        String splitStartKey = splitStart.isEmpty() ? "-" : splitStart;
//        String splitStopKey = splitEnd.isEmpty() ? "-" : splitEnd;
//        String[] hostnames = split.getHostnames();
//        LOG.info("{} split (this={})[{}|{}|{}|{}]", action, this, splitId, hostnames, splitStartKey, splitStopKey);
//    }
=======
    private void logSplitInfo(String action, LocatableInputSplit split) {
    /*
        int splitId = split.getSplitNumber();
        String splitStart = Bytes.toString(split.getStartRow());
        String splitEnd = Bytes.toString(split.getEndRow());
        String splitStartKey = splitStart.isEmpty() ? "-" : splitStart;
        String splitStopKey = splitEnd.isEmpty() ? "-" : splitEnd;
        String[] hostnames = split.getHostnames();
        LOG.info("{} split (this={})[{}|{}|{}|{}]", action, this, splitId, hostnames, splitStartKey, splitStopKey);
    */
    }

    /**
     * Test if the given region is to be included in the InputSplit while splitting the regions of a table.
     * <p>
     * This optimization is effective when there is a specific reasoning to exclude an entire region from the M-R job,
     * (and hence, not contributing to the InputSplit), given the start and end keys of the same. <br>
     * Useful when we need to remember the last-processed top record and revisit the [last, current) interval for M-R
     * processing, continuously. In addition to reducing InputSplits, reduces the load on the region server as well, due
     * to the ordering of the keys. <br>
     * <br>
     * Note: It is possible that <code>endKey.length() == 0 </code> , for the last (recent) region. <br>
     * Override this method, if you want to bulk exclude regions altogether from M-R. By default, no region is excluded(
     * i.e. all regions are included).
     *
     * @param startKey Start key of the region
     * @param endKey   End key of the region
     * @return true, if this region needs to be included as part of the input (default).
     */
    protected boolean includeRegionInSplit(final byte[] startKey, final byte[] endKey) {
        return true;
    }
>>>>>>> ruben/master







    @Override
    public InputSplitAssigner getInputSplitAssigner(KuduInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }


    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return null;
    }

}
