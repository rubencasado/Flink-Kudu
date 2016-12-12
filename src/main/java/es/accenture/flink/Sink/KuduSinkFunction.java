package es.accenture.flink.Sink;

import es.accenture.flink.Sink.utils.Utils;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.table.Row;
import org.apache.flink.configuration.Configuration;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by sshvayka on 21/11/16.
 */
public class KuduSinkFunction extends RichOutputFormat<Row> {

    private String host, tableName, tableMode;
    private Utils utils = null;
    private String [] fieldsNames;
    private KuduTable table;
    final static Logger logger = Logger.getLogger(KuduSinkFunction.class);

    /**
     * Constructor de la clase Sink que se empleara cuando se quiera crear una tabla nueva, ya que contiene los datos del esquema
     *
     * @param host          host de Kudu
     * @param tableName     nombre de la tabla de Kudu
     * @param fieldsNames   lista de nombres de columnas de la tabla a crear
     * @param tableMode     modo de operar con la tabla (CREATE, APPEND u OVERRIDE)
     * @throws KuduException
     */
    public KuduSinkFunction(String host, String tableName, String [] fieldsNames, String tableMode) throws KuduException {
        if(!(tableMode.equals("CREATE") || tableMode.equals("APPEND") || tableMode.equals("OVERRIDE"))){
            System.err.println("ERROR: No se ha proporcionado el modo de tabla en el constructor");
            System.exit(-1);
        }
        this.host = host;
        this.utils = new Utils(host);
        this.tableName = tableName;
        this.fieldsNames = fieldsNames;
        this.tableMode = tableMode;
    }

    /**
     * Constructor de la clase Sink que se empleara cuando se quiera usar una tabla ya existente. Esquema no requerido
     *
     * @param host          host de Kudu
     * @param tableName     nombre de la tabla a usar
     * @param tableMode     modo de operar con la tabla (CREATE, APPEND u OVERRIDE)
     * @throws KuduException
     */
    public KuduSinkFunction(String host, String tableName, String tableMode) throws KuduException {
        if(!(tableMode.equals("CREATE") || tableMode.equals("APPEND") || tableMode.equals("OVERRIDE"))){
            System.err.println("ERROR: No se ha proporcionado el modo de tabla en el constructor");
            System.exit(-1);
        }
        if(tableMode.equals("CREATE")){
            System.err.println("ERROR: Para este modo han de proporcionarse los nombres de las columnas de la tabla en el constructor");
            System.exit(-1);
        }
        this.host = host;
        this.utils = new Utils(host);
        this.tableName = tableName;
        this.tableMode = tableMode;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    /**
     * Se encarga de insertar un Row dentro de la tabla indicada en el constructor
     *
     * @param row   los datos de una fila a insertar
     * @throws IOException
     */
    @Override
    public void writeRecord(Row row) throws IOException {

        // Miramos la situacion de la tabla (si existe o no) en funcion del modo, la creamos o no
        this.table = utils.useTable(tableName, fieldsNames, row, tableMode);
        // Caso APPEND, con constructor sin nombres de columnas (porque sino luego da NullPointerException)
        if(fieldsNames == null){
            fieldsNames = utils.getNamesOfColumns(table);
        }
        // Inicializacion del objeto insert
        Insert insert = table.newInsert();

        for (int index = 0; index < row.productArity(); index++){

            // Se crea el insert con los anteriores datos, en funcion del tipo, un "add" distinto
            switch(utils.getRowsPositionType(index, row).getName()){
                case "string":
                    insert.getRow().addString(fieldsNames[index], (String)(row.productElement(index)));
                    break;
                case "int32":
                    insert.getRow().addInt(fieldsNames[index], (Integer) row.productElement(index));
                    break;
                case "bool":
                    insert.getRow().addBoolean(fieldsNames[index], (Boolean) row.productElement(index));
                    break;
                default:
                    break;
            }
        }
        // Una vez completado el Insert, se escribe en la tabla
        utils.insertToTable(insert);
        logger.info("Insertado el Row: " + utils.printRow(row));
    }
}
