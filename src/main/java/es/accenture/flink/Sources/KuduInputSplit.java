package es.accenture.flink.Sources;

import org.apache.flink.core.io.InputSplit;

/**
 * Created by lballestin on 28/11/16.
 */
public class KuduInputSplit implements InputSplit {

    private static final long serialVersionUID = 1L;

    /** The name of the table to retrieve data from */
    private final byte[] tableName;

//    /** The start row of the split. */
//    private final byte[] startRow;
//
//    /** The end row of the split. */
//    private final byte[] endRow;

    /**
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
//     */
    KuduInputSplit(final int splitNumber, final String[] hostnames, final byte[] tableName) {

        this.tableName = tableName;
    }

    /**
     * Returns the table name.
     *
     * @return The table name.
     */
    public byte[] getTableName() {
        return this.tableName;
    }

    @Override
    public int getSplitNumber() {
        return 0;
    }

    /**
     * Returns the start row.
     *
     * @return The start row.
     */
//    public byte[] getStartRow() {
//        return this.startRow;
//    }
//
//    /**
//     * Returns the end row.
//     *
//     * @return The end row.
//     */
//    public byte[] getEndRow() {
//        return this.endRow;
//    }
}