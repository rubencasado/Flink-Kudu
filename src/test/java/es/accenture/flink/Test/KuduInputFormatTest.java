package es.accenture.flink.Test;

import es.accenture.flink.Sources.KuduInputFormat;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by dani on 10/01/17.
 */
public class KuduInputFormatTest {

    private KuduInputFormat kuduInputFormat;

    @After
    public void tearDown() throws IOException {
        if (kuduInputFormat != null) {
            kuduInputFormat.close();
        }
        kuduInputFormat = null;
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidTable() throws IOException {
        kuduInputFormat = new KuduInputFormat("invalid.table.name", "localhost");
        kuduInputFormat.createTable("invalid.table.name");
    }

    //@Test(expected = RuntimeException.class)
    //public void testInvalidMasterAddress() throws IOException {
    //    kuduInputFormat = new KuduInputFormat("valid.table.name", "invalid.master.address");
    //}
}
