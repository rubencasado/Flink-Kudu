package es.accenture.flink.Utils.Exceptions;

import java.io.IOException;

/**
 * Created by sergiy on 23/12/16.
 */
public class KuduTableException extends IOException {

    public KuduTableException (String msg){
        super(msg);
    }
}
