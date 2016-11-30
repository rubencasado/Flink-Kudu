package es.accenture.flink.Sources;

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



    public static void main (String[] args) throws IOException {

        List<Row> rows = new ArrayList<>();
        RowResult rowRes;
        Row row;

        KuduTableInputFormat prueba = new KuduTableInputFormat();
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
            System.out.println(row);
            row=prueba.nextRecord(row);
        }
        rows.add(row);
        System.out.println("Fin de bucle de lectura");





    }



}
