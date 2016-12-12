package es.accenture.flink.utils;

import org.apache.flink.api.table.Row;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * Created by vadi on 9/12/16.
 */
public class RowSerializable extends Row implements Serializable {


    private Object[] fields2;

    public RowSerializable(int arity) {
        super(arity);

    }
    protected RowSerializable() {
        super(0);

    }

    public int getArity(){
        return this.fields2.length;
    }

    public void serialize(Row row) throws IllegalAccessException {
        Field[] fs = row.getClass().getSuperclass().getDeclaredFields();
        fs[0].setAccessible(true);
        this.fields2 = (Object[]) fs[0].get(row);
    }

}
