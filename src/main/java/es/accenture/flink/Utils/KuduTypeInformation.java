package es.accenture.flink.Utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Created by vadi on 12/12/16.
 */
public class KuduTypeInformation extends TypeInformation{

    int arity;
    Class c;

    public KuduTypeInformation(RowSerializable r) {
        this.arity = r.getArity();
        this.c = r.getClass();
    }

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
        return this.arity;
    }

    @Override
    public int getTotalFields() {
        return this.arity;
    }

    @Override
    public Class getTypeClass() {
        return this.c;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer createSerializer(ExecutionConfig executionConfig) {
        return null;
    }

    @Override
    public String toString() {
        return "Arity: " + this.arity + "   Class: " + this.c;
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
}
