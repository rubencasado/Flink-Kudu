package es.accenture.flink.Utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.table.typeutils.NullMaskUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import java.io.IOException;

public class KuduSerializer extends TypeSerializer<RowSerializable> {

    private boolean[] nullMask;
    private TypeSerializer[] fieldSerializers;

    KuduSerializer(TypeSerializer[] fieldSerializers) {
        this.nullMask=new boolean[fieldSerializers.length];
        this.fieldSerializers = fieldSerializers;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<RowSerializable> duplicate() {
        return null;
    }

    @Override
    public RowSerializable createInstance() {
        return new RowSerializable(fieldSerializers.length);
    }

    @Override
    public RowSerializable copy(RowSerializable rowSerializable) {
        int len = rowSerializable.productArity();
        RowSerializable rowCopy = new RowSerializable(len);
        Object field;
        for (int i=0;i<len;i++){
            field=rowSerializable.productElement(i);
            rowCopy.setField(i, field);
        }
        return rowCopy;
    }

    @Override
    public RowSerializable copy(RowSerializable from, RowSerializable reuse) {
        int len = fieldSerializers.length;
        if (reuse==null){
            return copy(from);
        }

        if (reuse.productArity() != len){
            throw new RuntimeException("Row arity of reuse or from is incompatible with this " +
                    "KuduSerializer.");
        }

        Object field;
        for (int i=0;i<len;i++){
            field=from.productElement(i);
            reuse.setField(i, field);
        }

        return reuse;
    }

    @Override
    public int getLength() {
        return fieldSerializers.length;
    }

    @Override
    public void serialize(RowSerializable row, DataOutputView dataOutputView) throws IOException {
        int len = fieldSerializers.length;
        if (len != row.productArity()){
          throw new RuntimeException("rowSerializable's arity does not match this KuduSerializer ");
        }

        //NullMaskUtils.writeNullMask(len, row, dataOutputView);

        Object field;
        for(int i=0; i<len; i++){
            field=row.productElement(i);
            if (field!=null){
                TypeSerializer serializer = fieldSerializers[i];
                serializer.serialize(row.productElement(i), dataOutputView);
            }
        }

    }

    @Override
    public RowSerializable deserialize(DataInputView dataInputView) throws IOException {
        int len = fieldSerializers.length;
        RowSerializable result = new RowSerializable(len);

        NullMaskUtils.readIntoNullMask(len, dataInputView, nullMask);

        for (int i=0; i<len ; i++){
            if(nullMask[i]){
                result.setField(i, null);
            }
            else {
                result.setField(i, fieldSerializers[i].deserialize(dataInputView));
            }
        }
        return result;
    }

    @Override
    public RowSerializable deserialize(RowSerializable reuse, DataInputView dataInputView) throws IOException {
        int len = fieldSerializers.length;

        if(reuse.productArity() != len){
            throw new RuntimeException("Row arity of reuse does not match serializers.");
        }

        NullMaskUtils.readIntoNullMask(len, dataInputView, nullMask);

        for (int i=0; i<len; i++){
            if (nullMask[i]){
                reuse.setField(i, null);
            }
            else {
                reuse.setField(i, fieldSerializers[i].deserialize(dataInputView));
            }
        }

        return reuse;
    }

    @Override
    public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
        int len = fieldSerializers.length;

        NullMaskUtils.readIntoAndCopyNullMask(len, dataInputView, dataOutputView, nullMask);

        for (int i=0; i<len; i++){
            if(!nullMask[i]){
                fieldSerializers[i].copy(dataInputView, dataOutputView);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if(this.canEqual(o)){
            KuduSerializer eq = (KuduSerializer)o;
            return this.fieldSerializers.equals(eq.fieldSerializers);
        }
        return false;
    }

    @Override
    public boolean canEqual(Object o) {
        return o.getClass()==KuduSerializer.class;
    }

    @Override
    public int hashCode() {
        return java.util.Arrays.hashCode(fieldSerializers);
    }
}
