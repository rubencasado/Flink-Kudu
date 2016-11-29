package es.accenture.flink.Sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;

import java.io.IOException;

/**
 * Created by vadi on 28/11/16.
 */
public class main_source {



    public static void main (String[] args) throws IOException {


        KuduTableInputFormat prueba = new KuduTableInputFormat();
        KuduInputSplit a = null;
        prueba.configure(new Configuration());
        System.out.println("Salido de configure");
        prueba.open(a);




    }



}
