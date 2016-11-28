package org.kududb.examples.sample;

/**
 *
 * Created by luis on 23/11/16.
 */


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
 * {@link InputFormat} subclass that wraps the access for HTables.
 */
public abstract class KuduTableInputFormat<T extends Tuple> extends RichInputFormat<T, InputSplit> {

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
     * Returns an instance of Scan that retrieves the required subset of records from the HBase table.
     * @return The appropriate instance of Scan for this usecase.
     */
    protected abstract KuduScanner getScanner();

    /**
     * What table is to be read.
     * Per instance of a TableInputFormat derivative only a single tablename is possible.
     * @return The name of the table
     */
    protected abstract String getTableName();

    /**
     * The output from HBase is always an instance of {@link Result}.
     * This method is to copy the data in the Result instance into the required {@link Tuple}
     * @param r The Result instance from HBase that needs to be converted
     * @return The approriate instance of {@link Tuple} that contains the needed information.
     */
    protected abstract T mapResultToTuple(Result r);

    /**
     * Creates a {@link KuduScan} object and opens the {@link KuduTable} connection.
     * These are opened here because they are needed in the createInputSplits
     * which is called before the openInputFormat method.
     * So the connection is opened in {@link #configure(Configuration)} and closed in {@link #closeInputFormat()}.
     *
     * @param parameters The configuration that is to be used
     * @see Configuration
     */
    @Override
    public void configure(Configuration parameters) {
        LOG.info("Initializing KUDUConfiguration");
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        try {
            if (client.tableExists(TABLE_NAME)) {
                table = client.openTable(TABLE_NAME);
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
    public void open(InputSplit split) throws IOException {
        try {
            table = client.openTable(TABLE_NAME);
            KuduSession session = client.newSession();

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
    public T nextRecord(T reuse) throws IOException {
        if (scanner == null) {
            throw new IOException("No table scanner provided!");
        }
        try {
            RowResult res = results.next();
            if (res != null) {
                scannedRows++;
                return mapResultToTuple((Result) res); // GUARDAR LA FILA EN ALGUN LADO O DEVOLVERLA
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
    public void closeInputFormat() {
        try {
            if (table != null) {
                client.close();
            }
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            table = null;
        }
    }

    @Override
    public InputSplit[] createInputSplits(final int minNumSplits) throws IOException {

        try {
            if (table == null) {
                throw new IOException("Table was not provided/opened");
            }
            if (scanner == null) {
                throw new IOException("getScanner returned null");
            }

            KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);

            List<KuduScanToken> tokens = tokenBuilder.build();

            List<InputSplit> splits = new ArrayList<InputSplit>(tokens.size());
            for (KuduScanToken token : tokens) {
                List<String> locations = new ArrayList<>(token.getTablet().getReplicas().size());
                for (LocatedTablet.Replica replica : token.getTablet().getReplicas()) {
                    locations.add(reverseDNS(replica.getRpcHost(), replica.getRpcPort()));
                }
                splits.add(new TableSplit(token, locations.toArray(new String[locations.size()])));
            }
            return splits.toArray(new InputSplit[0]);
        } finally {
            try {
                client.close();
            } catch (IOException g) {
                throw new IOException(g);
            }
        }
    }

    private void logSplitInfo(String action, LocatableInputSplit split) {
        int splitId = split.getSplitNumber();
        String splitStart = Bytes.toString(split.getStartRow());
        String splitEnd = Bytes.toString(split.getEndRow());
        String splitStartKey = splitStart.isEmpty() ? "-" : splitStart;
        String splitStopKey = splitEnd.isEmpty() ? "-" : splitEnd;
        String[] hostnames = split.getHostnames();
        LOG.info("{} split (this={})[{}|{}|{}|{}]", action, this, splitId, hostnames, splitStartKey, splitStopKey);
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

    @Override
    public InputSplitAssigner getInputSplitAssigner(TableInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return null;
    }

}
