package es.accenture.flink.Sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduTable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by sshvayka on 21/11/16.
 */
public class SinkFunction extends RichSinkFunction<Tuple2<String, Integer>>{

    private String host;
    private Utils utils = null;

    public SinkFunction(String host) {
        this.host = host;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.utils = new Utils(host);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(Tuple2<String, Integer> in) throws Exception {

        // Creacion de la tabla en la que insertaremos los datos
        Map<String,Type> fields = new LinkedHashMap<String, Type>();
        fields.put("username", Type.STRING);
        fields.put("age", Type.INT32);

        KuduTable table = utils.createTable("users", fields, "username");
        String username = in.getField(0);
        int age = in.getField(1);

        Insert insert = table.newInsert();
        insert.getRow().addString("username", username);
        insert.getRow().addInt("age", age);

        utils.insertToTable(insert);

        System.out.println("Se ha insertado la fila: " + username + "-" + age + " en la tabla \"" + table.getName() + "\"");
    }
}
