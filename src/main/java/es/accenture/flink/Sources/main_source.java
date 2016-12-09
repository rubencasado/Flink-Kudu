package es.accenture.flink.Sources;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;

import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;



import java.util.ArrayList;
import java.util.List;

/**
 * Created by vadi on 28/11/16.
 *
 */
public class main_source {



    public static void main (String[] args) throws Exception {

        List<RowSerializable> rows = new ArrayList<>();
        RowResult rowRes;
        RowSerializable row;

        KuduInputFormat prueba = new KuduInputFormat();
        KuduInputSplit a = null;
        prueba.configure(new Configuration());
        System.out.println("Salido de configure");
        prueba.open(a);
        System.out.println("Salido de Open");


        for (RowSerializable r : prueba.getRows()){
            System.out.println(r);
        }

        System.out.println("Generando dataset");



        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<RowSerializable> data = env.createInput(prueba, new TypeInformation<RowSerializable>() {
            @Override
            public boolean isBasicType() {
                return false;
            }

            @Override
            public boolean isTupleType() {
                return false;
            }

            @Override
            public int getArity() {
                return 0;
            }

            @Override
            public int getTotalFields() {
                return 0;
            }

            @Override
            public Class<RowSerializable> getTypeClass() {
                return null;
            }

            @Override
            public boolean isKeyType() {
                return false;
            }

            @Override
            public TypeSerializer<RowSerializable> createSerializer(ExecutionConfig executionConfig) {
                return null;
            }

            @Override
            public String toString() {
                return null;
            }

            @Override
            public boolean equals(Object o) {
                return false;
            }

            @Override
            public int hashCode() {
                return 0;
            }

            @Override
            public boolean canEqual(Object o) {
                return false;
            }
        });




        System.out.println("dataset creado");
/*
        DataSet<RowSerializable> data2 = data
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
                });
        System.out.println("Fuera map");
*/
    }




}
