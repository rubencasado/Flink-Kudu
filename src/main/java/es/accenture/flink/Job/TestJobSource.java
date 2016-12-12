package es.accenture.flink.Job;

import es.accenture.flink.Sources.KuduInputFormat;
import es.accenture.flink.Sources.KuduInputSplit;
import es.accenture.flink.utils.KuduTypeInformation;
import es.accenture.flink.utils.RowSerializable;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.configuration.Configuration;

import org.apache.kudu.client.RowResult;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by vadi on 28/11/16.
 *
 */
public class TestJobSource {



    public static void main (String[] args) throws Exception {

        List<RowSerializable> rows = new ArrayList<>();
        RowResult rowRes;
        RowSerializable row;

        KuduInputFormat prueba = new KuduInputFormat("Table_1", "localhost");
        KuduInputSplit a = null;
        prueba.configure(new Configuration());
        prueba.open(a);



        for (RowSerializable r : prueba.getRows()){
            System.out.println(r);
        }




        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<RowSerializable> data = env.createInput(prueba, new KuduTypeInformation(prueba.getRows().get(0)));





        /*DataSet<RowSerializable> data2 = data
                .map(new MapFunction<RowSerializable, RowSerializable>()  {

                    @Override
                    public RowSerializable map(RowSerializable row) throws Exception {
                        RowSerializable row2 = row;
                        System.out.println("aaaaaaaaa");
                        String value = row.productElement(1).toString();
                        if (value.startsWith("value")) {
                            row2.setField(1,"AAAAA");
                            return row2;
                        }
                        return row;
                    }
                });*/


    }




}
