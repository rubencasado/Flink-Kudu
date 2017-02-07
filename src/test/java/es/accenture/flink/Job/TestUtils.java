package es.accenture.flink.Job;


import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

class TestUtils {

    public static class MyMapFunction implements MapFunction<RowSerializable, RowSerializable> {

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
    public static class MyMapFunction2 implements MapFunction<String, RowSerializable>{

        @Override
        public RowSerializable map(String input) throws Exception {

            RowSerializable res = new RowSerializable(2);
            Integer i = 0;
            for (String s : input.split(" ")) {
                /*Needed to prevent exception on map function if phrase has more than 4 words*/
                if(i<3)
                    res.setField(i, s.toUpperCase());
                i++;
            }
            return res;
        }
    }

    public static boolean createTable(KuduClient client, String tableName, String mode) {

        try{
            List<ColumnSchema> columns = new ArrayList(2);
            if(mode.equals("INT")){
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                    .key(true)
                    .build());
            }else if (mode.equals("STRING")){
                columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
                        .key(true)
                        .build());
            }else{
                return false;
            }

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



    public static boolean insertData (KuduClient client,String tableName, int numRows){

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

    public static ArrayList<String> scanRows(KuduClient client,String tableName){

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

    public static int numRows(KuduClient client,String tableName){

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
