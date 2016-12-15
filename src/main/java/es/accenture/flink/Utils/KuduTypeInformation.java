package es.accenture.flink.Utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Created by vadi on 12/12/16.
 */
public class KuduTypeInformation extends TypeInformation{

    private int arity;
    private Class c;
    private RowSerializable row;
    private TypeInformation[] fieldTypes;

    public KuduTypeInformation(RowSerializable r) {
        this.arity = r.productArity();
        this.c = r.getClass();
        this.row = r;
        this.fieldTypes = new TypeInformation[r.productArity()];
        for(int i=0;i<this.arity;i++){
            r.productElement(i).getClass();
        }
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
    public TypeSerializer<RowSerializable> createSerializer(ExecutionConfig executionConfig) {
        if (this.arity > 0) {
            // create serializer for kudu with schema
            TypeSerializer[] fieldSers;
            fieldSers = new TypeSerializer[this.arity];
            for (int i=0; i<this.arity; i++) {
                System.out.println(i);
                fieldSers[i] = this.fieldTypes[i].createSerializer(executionConfig);
            }
            return new KuduSerializer(fieldSers);
        } else
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
        return o.getClass()==RowSerializable.class;
    }
}
