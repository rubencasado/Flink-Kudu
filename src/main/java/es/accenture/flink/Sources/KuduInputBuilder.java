package es.accenture.flink.Sources;

import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class KuduInputBuilder {


    public static ExecutionEnvironment env = null;

    public static DataSet<RowSerializable> build(String table_name, String master_add){
        KuduInputFormat prueba = new KuduInputFormat(table_name, master_add);

        env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<RowSerializable> typeInformation = TypeInformation.of(RowSerializable.class);
        DataSet<RowSerializable> source = env.createInput(prueba, typeInformation);
        return source;
    }
}
