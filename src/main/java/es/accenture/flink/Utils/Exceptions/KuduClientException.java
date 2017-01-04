package es.accenture.flink.Utils.Exceptions;

import java.io.IOException;

/**
 * Created by sergiy on 23/12/16.
 */
public class KuduClientException extends IOException {

    public KuduClientException (String msg) {
        super(msg);
    }
}
