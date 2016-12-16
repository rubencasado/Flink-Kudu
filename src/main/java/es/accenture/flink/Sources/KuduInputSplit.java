package es.accenture.flink.Sources;

import org.apache.flink.core.io.InputSplit;
import org.apache.hadoop.io.Writable;
import org.apache.kudu.client.Bytes;
import org.apache.kudu.client.KuduScanToken;
import org.apache.flink.hadoop.shaded.com.google.common.base.Objects;
import org.apache.kudu.client.shaded.com.google.common.primitives.UnsignedBytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by lballestin on 28/11/16.
 */
public class KuduInputSplit implements InputSplit, Writable, Comparable<KuduInputSplit> {

    /** The scan token that the split will use to scan the Kudu table. */
    private byte[] scanToken;

    /** The start partition key of the scan. Used for sorting splits. */
    private byte[] partitionKey;

    /** Tablet server locations which host the tablet to be scanned. */
    private String[] locations;

    public KuduInputSplit() { } // Writable

    public KuduInputSplit(KuduScanToken token, String[] locations) throws IOException {
        this.scanToken = token.serialize();
        this.partitionKey = token.getTablet().getPartition().getPartitionKeyStart();
        this.locations = locations;
    }

    public byte[] getScanToken() {
        return scanToken;
    }

    public byte[] getPartitionKey() {
        return partitionKey;
    }

    public String[] getLocations() throws IOException, InterruptedException {
        return locations;
    }

    @Override
    public int compareTo(KuduInputSplit other) {
        return UnsignedBytes.lexicographicalComparator().compare(partitionKey, other.partitionKey);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Bytes.writeByteArray(dataOutput, scanToken);
        Bytes.writeByteArray(dataOutput, partitionKey);
        dataOutput.writeInt(locations.length);
        for (String location : locations) {
            byte[] str = Bytes.fromString(location);
            Bytes.writeByteArray(dataOutput, str);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        scanToken = Bytes.readByteArray(dataInput);
        partitionKey = Bytes.readByteArray(dataInput);
        locations = new String[dataInput.readInt()];
        for (int i = 0; i < locations.length; i++) {
            byte[] str = Bytes.readByteArray(dataInput);
            locations[i] = Bytes.getString(str);
        }
    }

    @Override
    public int hashCode() {
        // We currently just care about the partition key since we're within the same table.
        return Arrays.hashCode(partitionKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KuduInputSplit that = (KuduInputSplit) o;

        return this.compareTo(that) == 0;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("partitionKey", Bytes.pretty(partitionKey))
                .add("locations", Arrays.toString(locations))
                .toString();
    }

    @Override
    public int getSplitNumber() {
        return 0;
    }
}




    /*private static final long serialVersionUID = 1L;

    *//** The name of the table to retrieve data from *//*
    private final byte[] tableName;

//    *//** The start row of the split. *//*
//    private final byte[] startRow;
//
//    *//** The end row of the split. *//*
//    private final byte[] endRow;

    *//**
 * Creates a new kudu input split
 *
 * @param splitNumber
 *        the number of the input split
 * @param hostnames
 *        the names of the hosts storing the data the input split refers to
 * @param tableName
 *        the name of the table to retrieve data from
//     * @param startRow
//     *        the start row of the split
//     * @param endRow
//     *        the end row of the split
//     *//*
    KuduInputSplit(final int splitNumber, final String[] hostnames, final byte[] tableName) {

        this.tableName = tableName;
    }

    *//**
 * Returns the table name.
 *
 * @return The table name.
 *//*
    public byte[] getTableName() {
        return this.tableName;
    }

    @Override
    public int getSplitNumber() {
        return 0;
    }

    *//**
 * Returns the start row.
 *
 * @return The start row.
 *//*
//    public byte[] getStartRow() {
//        return this.startRow;
//    }
//
//    *//**
 //     * Returns the end row.
 //     *
 //     * @return The end row.
 //     *//*
//    public byte[] getEndRow() {
//        return this.endRow;
//    }*/
