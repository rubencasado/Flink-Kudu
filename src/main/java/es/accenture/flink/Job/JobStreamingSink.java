package es.accenture.flink.Job;

import es.accenture.flink.Sink.KuduSink;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A job which reads a line of elements, split the line by spaces to generate the rows,
 * and writes the result on another Kudu database using streaming functions.
 * (This example split the line by spaces)
 */
public class JobStreamingSink {

    public static void main(String[] args) throws Exception {

        //********Only for test, delete once finished*******
        args[0] = "Table_5";
        args[1] = "localhost";
        //**************************************************

        String tableName = args[0];
        String KUDU_MASTER = args[1];


        String [] columnNames = new String[3];
        columnNames[0] = "key";
        columnNames[1] = "value";
        columnNames[2] = "description";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.fromElements("field1 field2 field3");

        DataStream<RowSerializable> stream2 = stream.map(new MyMapFunction());

        stream2.addSink(new KuduSink(KUDU_MASTER, tableName, columnNames));

        env.execute();
    }


    private static class MyMapFunction implements MapFunction<String, RowSerializable>{

        @Override
        public RowSerializable map(String input) throws Exception {

            RowSerializable res = new RowSerializable(2);
            Integer i = 0;
            for (String s : input.split(" ")) {
                res.setField(i, s);
                i++;
            }
            return res;
        }
    }



}
