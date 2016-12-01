package es.accenture.flink.Sources;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.table.Row;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by vadi on 28/11/16.
 *
 */
public class main_source {



    public static void main (String[] args) throws Exception {

        List<Row> rows = new ArrayList<>();
        RowResult rowRes;
        Row row;

        KuduInputFormat prueba = new KuduInputFormat<Row>();
        KuduInputSplit a = null;
        prueba.configure(new Configuration());
        System.out.println("Salido de configure");
        prueba.open(a);
        System.out.println("Salido de Open");
        RowResultIterator results=prueba.getResults();
        System.out.println(rows.toString());
        try {
            rowRes=results.next();
            row=prueba.RowResultToRow(rowRes);
        } catch (Exception e){
            rowRes=null;
            row=prueba.RowResultToRow(rowRes);
            System.out.println("TABLA VACIA");
        }
        while(results.hasNext()) {
            rows.add(row);
            row=prueba.nextRecord(row);
        }
        rows.add(row);

        for (Row r : rows){
            System.out.println(r);
        }

        System.out.println("Generando dataset");
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Row> data = env
                .createInput(prueba);

        data.print();


        System.out.println("dataset creado");

        DataSet<Row> data2 = data
                .map(new MapFunction<Row, Row>() {

                    @Override
                    public Row map(Row row) throws Exception {
                        Row row2 = row;
                        System.out.println("aaaaaaaaa");
                        String value = row.productElement(1).toString();
                        if (value.startsWith("value")) {
                            row2.setField(1,"AAAAA");
                            return row2;
                        }
                        return row;
                    }
                });

    }



}
