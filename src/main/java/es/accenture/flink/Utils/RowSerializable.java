package es.accenture.flink.Utils;


import java.io.Serializable;


public class RowSerializable implements Serializable {
    private static final long serialVersionUID = 1L;
    private Object[] fields2;

    /**
     * Creates an instance of RowSerializable
     * @param arity Size of the row
     */
    public RowSerializable(int arity){

        this.fields2 = new Object[arity];
    }
    public RowSerializable(){

    }

    /**
     * returns number of fields contained in a Row
     * @return int arity
     */
    public int productArity(){
        return this.fields2.length;
    }

    /**
     * Inserts the "field" Object in the position "i".
     * @param i index value
     * @param field Object to write
     */
    public void setField(int i, Object field){
        this.fields2[i]=field;
    }

    /**
     * returns the Object contained in the position "i" from the RowSerializable.
     * @param i index value
     * @return Object
     */
    public Object productElement(int i){
        return this.fields2[i];
    }

    /**
     * returns a String element with the fields of the RowSerializable
     * @return String
     */
    public String toString(){
        String str=fields2[0].toString();
        for (int i=1; i<fields2.length; i++){
            str=str+", " + fields2[i].toString();
        }
        return str;
    }

    public boolean equals(Object object){
        return false;
    }
}
