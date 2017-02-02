package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduOutputFormat;
import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Utils.*;
import es.accenture.flink.Utils.Exceptions.KuduClientException;
import es.accenture.flink.Utils.Exceptions.KuduTableException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class JobBatchSinkTest {

    /*class vars*/
    private String KUDU_MASTER;
    private String TABLE_NAME;
    private String TABLE_NAME2;
    private String[] columnNames;
    private Utils utils;
    private KuduClient client;
    private ExecutionEnvironment env;
    private boolean singleton = true;
    private DataSet<RowSerializable> input = null;
    private Integer MODE;



    @Before
    public void setUp() throws Exception {
        KUDU_MASTER = System.getProperty("kuduMaster", "localhost");
        TABLE_NAME = System.getProperty("tableName", "Table_1");
        TABLE_NAME2 = System.getProperty("tableName", "Table_2");
        MODE = KuduOutputFormat.APPEND;
        columnNames= new String[2];
        columnNames[0] = "key";
        columnNames[1] = "value";
        utils=new Utils("localhost");
        client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();


    }

    @After
    public void tearDown() throws Exception {
        for(int i = 1; i<=10; i++)
            if(client.tableExists("Table_"+i))
                client.deleteTable("Table_"+i);
        client.shutdown();
        client.close();
    }

    @Test
    public void hola(){
        assert true;
    }

    @Test
    public void JobBatchSinkTest() throws Exception {
        hola();
        //Test: Create a table
        assert !client.tableExists(TABLE_NAME) : "JUnit error: Table already exists";
        assert createTable(TABLE_NAME) : "JUnit test failed creating the table";
        assert !createTable(TABLE_NAME) : "JUnit test failed creating the table";
        assert scanRows(TABLE_NAME).size() == 0: "JUnit error: " + TABLE_NAME + " is not empty" ;
        //Test: Adding rows to the table
        assert insertData(TABLE_NAME, 0) : "JUnit test failed inserting data";
        assert scanRows(TABLE_NAME).isEmpty(): "JUnit error: " + TABLE_NAME + " is not empty" ;
        assert insertData(TABLE_NAME, 10) : "JUnit error inserting rows" ;
        assert !scanRows(TABLE_NAME).isEmpty(): "JUnit error: " + TABLE_NAME + " is empty" ;
        assert scanRows(TABLE_NAME).size() == 10: "JUnit error: " + TABLE_NAME + " must have 10 rows" ;
        /*If the data already exists in the db, it won't change*/
        assert insertData(TABLE_NAME, 5) : "JUnit test failed inserting data";
        assert scanRows(TABLE_NAME).size() == 10: "JUnit error: " + TABLE_NAME + " must have 10 rows";
        /*Test: Searching rows on the db*/
        assert scanRows(TABLE_NAME).contains("INT32 key=0, STRING value=This is the row number: 0"): "JUnit test failed scanning data from the table, expected row not read";
        assert scanRows(TABLE_NAME).contains("INT32 key=3, STRING value=This is the row number: 3"): "JUnit test failed scanning data from the table, expected row not read";
        assert !scanRows(TABLE_NAME).contains("INT32 key=6, STRING value=This is the row number: 3"): "JUnit test failed scanning data from the table, expected row not read";

        /*Executed with mode: CREATE*/
        assert setup2(KuduOutputFormat.CREATE): "JUnit failed while setting up the mode";
        env.execute();
        assert client.tableExists(TABLE_NAME2):"JUnit error: Table does not exists";
        assert scanRows(TABLE_NAME2).contains("INT32 key=0, STRING value=THIS IS THE ROW NUMBER: 0"): "JUnit failed: not expected record";
        assert scanRows(TABLE_NAME2).contains("INT32 key=6, STRING value=THIS IS THE ROW NUMBER: 3"): "JUnit failed: not expected record";
        assert numRows(TABLE_NAME2) == 10: "JUnit error: " + TABLE_NAME2 + " must have 10 rows";

        client.deleteTable(TABLE_NAME2);
        assert !client.tableExists(TABLE_NAME2);
        assert createTable(TABLE_NAME2);



        /*Changed mode to: APPEND*/
        assert setup2(KuduOutputFormat.APPEND);
        env.execute();
        assert client.tableExists(TABLE_NAME2);
        assert scanRows(TABLE_NAME2).contains("INT32 key=0, STRING value=THIS IS THE ROW NUMBER: 0"): "Junit failed: not expected record";
        assert scanRows(TABLE_NAME2).contains("INT32 key=18, STRING value=THIS IS THE ROW NUMBER: 9"): "Junit failed: not expected record";
        assert !scanRows(TABLE_NAME2).contains("INT32 key=2, STRING value=THIS IS THE ROW NUMBER: 2"): "Junit failed: not expected record";
        assert numRows(TABLE_NAME2) == 10;


        /*Changed mode to: OVERRIDE
        * This mode has the same tests than append mode
        */
        assert setup2(KuduOutputFormat.OVERRIDE);
        assert client.tableExists(TABLE_NAME2):"JUnit error: " + TABLE_NAME2 + " does not exists";
        env.execute();
        assert client.tableExists(TABLE_NAME2):"JUnit error: " + TABLE_NAME2 + " does not exists";
        assert scanRows(TABLE_NAME2).contains("INT32 key=0, STRING value=THIS IS THE ROW NUMBER: 0"): "Junit failed: not expected record";
        assert scanRows(TABLE_NAME2).contains("INT32 key=18, STRING value=THIS IS THE ROW NUMBER: 9"): "Junit failed: not expected record";
        assert !scanRows(TABLE_NAME2).contains("INT32 key=2, STRING value=THIS IS THE ROW NUMBER: 2"): "Junit failed: not expected record";
        assert numRows(TABLE_NAME2) == 10;

    }

    private boolean setup2(Integer mode) {


        if (singleton == true) {
            KuduInputFormat KuduInputTest = new KuduInputFormat(TABLE_NAME, KUDU_MASTER);

            env = ExecutionEnvironment.getExecutionEnvironment();

            TypeInformation<RowSerializable> typeInformation = TypeInformation.of(RowSerializable.class);

            DataSet<RowSerializable> source = env.createInput(KuduInputTest, typeInformation);

            input = source.map(new MyMapFunction());
            singleton = false;
        }
        try {
            input.output(new KuduOutputFormat(KUDU_MASTER, TABLE_NAME2, columnNames, mode));
            return true;
        } catch (KuduException | KuduTableException | KuduClientException e) {
            return false;
        }


    }

    private static class MyMapFunction implements MapFunction<RowSerializable, RowSerializable> {

        @Override
        public RowSerializable map(RowSerializable row) throws Exception {

            for (int i = 0; i < row.productArity(); i++) {
                if (row.productElement(i).getClass().equals(String.class))
                    row.setField(1, row.productElement(1).toString().toUpperCase());
                else if (row.productElement(i).getClass().equals(Integer.class))
                    row.setField(0, (Integer)row.productElement(0)*2);
            }
            return row;
        }
    }


    public boolean createTable(String tableName) {

        try{
            List<ColumnSchema> columns = new ArrayList(2);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
                    .build());
            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("key");
            Schema schema = new Schema(columns);
            client.createTable(tableName, schema,
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys).addHashPartitions(rangeKeys, 4));
            return true;
        } catch(Exception e){
            /*Print stack disable for JUnit*/
            return false;
        }
    }



    public boolean insertData (String tableName, int numRows){

        try {

            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            for (int i = 0; i < numRows; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, i);
                row.addString(1, "This is the row number: "+ i);
                session.apply(insert);
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public ArrayList<String> scanRows(String tableName){

        int cont= 0;
        ArrayList<String> array = new ArrayList<>();
        try {
            KuduTable table = client.openTable(tableName);
            table.getSchema();
            List<String> projectColumns = new ArrayList<>(2);
            projectColumns.add("key");
            projectColumns.add("value");

            KuduScanner scanner = client.newScannerBuilder(table)
                    .setProjectedColumnNames(projectColumns)
                    .build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    cont++;
                    array.add(result.rowToString());
                }
            }
            return array;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public int numRows(String tableName){

        Integer cont = 0;
        try {
            KuduTable table = client.openTable(tableName);
            table.getSchema();
            List<String> projectColumns = new ArrayList<>(2);
            projectColumns.add("key");
            projectColumns.add("value");

            KuduScanner scanner = client.newScannerBuilder(table)
                    .setProjectedColumnNames(projectColumns)
                    .build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    cont++;
                }
            }
            return cont;
        } catch (Exception e) {
            return -1;
        }
    }

}
