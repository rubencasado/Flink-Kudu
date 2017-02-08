package es.accenture.flink.Sources;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.LocatableInputSplit;

public class KuduInputSplit extends LocatableInputSplit implements InputSplit  {

    private static final long serialVersionUID = 1L;

    /** The name of the table to retrieve data from */
    private final String tableName;

    /** The start row of the split. */
    private final byte[] startKey;

    /** The end row of the split. */
    private final byte[] endKey;

    /* The number of this input split. */
    private final Integer splitNumber;

    /**
     * Creates a new kudu input split
     * @param splitNumber the number of the input split
     * @param hostnames the names of the hosts storing the data the input split refers to
     * @param tableName the name of the table to retrieve data from
     * @param startKey the start row of the split
     * @param endKey the end row of the split
     */

    KuduInputSplit(final int splitNumber, final String[] hostnames, final String tableName,
                   final byte[] startKey, final byte[] endKey) {
        super(splitNumber, hostnames);
        this.tableName = tableName;
        this.startKey = startKey;
        this.endKey = endKey;
        this.splitNumber = splitNumber;

    }

    @Override
    public int getSplitNumber() { return this.splitNumber; }

    /**
     * Returns the table name.
     * @return The table name.
     */
    public String getTableName() { return this.tableName; }

    /**
     * Returns the start row.
     * @return The start row.
     */

    public byte[] getStartKey() { return this.startKey; }

    /**
     * Returns the end row.
     * @return The end row.
     */

    public byte[] getEndKey() { return this.endKey; }
}
