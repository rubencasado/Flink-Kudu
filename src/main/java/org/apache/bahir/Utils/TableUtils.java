/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bahir.Utils;

import org.apache.flink.hadoop.shaded.com.google.common.collect.ImmutableBiMap;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

import java.nio.ByteBuffer;
import java.util.Date;

public class TableUtils {

    private final static ImmutableBiMap<Type,Class> TYPES =
            new ImmutableBiMap.Builder<Type,Class>()
                .put(Type.STRING, String.class)
                .put(Type.BOOL, Boolean.class)
                .put(Type.DOUBLE, Double.class)
                .put(Type.FLOAT, Float.class)
                .put(Type.BINARY, ByteBuffer.class)
                .put(Type.INT8, Byte.class)
                .put(Type.INT16, Short.class)
                .put(Type.INT32, Integer.class)
                .put(Type.INT64, Long.class)
                .put(Type.UNIXTIME_MICROS, Date.class)
                .build();


    public final static Type mapToType(Class clazz){
        return TYPES.inverse().get(clazz);
    }

    public final static Class mapFromType(Type type){
        return TYPES.get(type);
    }

    public final static Object valueFromRow(RowResult row, String col){
        Object value = new Object();
        Type colType = row.getColumnType(col);
        switch (colType) {
            case STRING:
                value = row.getString(col);
                break;
            case BOOL:
                value = row.getBoolean(col);
                break;
            case DOUBLE:
                value = row.getDouble(col);
                break;
            case FLOAT:
                value = row.getFloat(col);
                break;
            case BINARY:
                value = row.getBinary(col);
                break;
            case INT8:
                value = row.getByte(col);
                break;
            case INT16:
                value = row.getShort(col);
                break;
            case INT32:
                value = row.getInt(col);
                break;
            case INT64:
                value = row.getLong(col);
                break;
            case UNIXTIME_MICROS:
                value = new Date(row.getLong(col)/1000);
                break;
        }
        return value;
    }

    private final static <T> T mapValue(Object value, Class clazz){
        return (T)value;
    }

    public final static void valueToRow(PartialRow row, Type colType, String col, Object value){
        switch (colType) {
            case STRING:
                row.addString(col, mapValue(value, mapFromType(colType)));
                break;
            case BOOL:
                row.addBoolean(col, mapValue(value, mapFromType(colType)));
                break;
            case DOUBLE:
                row.addDouble(col, mapValue(value, mapFromType(colType)));
                break;
            case FLOAT:
                row.addFloat(col, mapValue(value, mapFromType(colType)));
                break;
            case BINARY:
                //row.addBinary(col, mapValue(value, mapFromType(colType)));
                break;
            case INT8:
                row.addByte(col, mapValue(value, mapFromType(colType)));
                break;
            case INT16:
                row.addShort(col, mapValue(value, mapFromType(colType)));
                break;
            case INT32:
                row.addInt(col, mapValue(value, mapFromType(colType)));
                break;
            case INT64:
                row.addLong(col, mapValue(value, mapFromType(colType)));
                break;
            case UNIXTIME_MICROS:
                row.addLong(col, mapValue(value, mapFromType(colType)));
                break;
        }
    }
}
