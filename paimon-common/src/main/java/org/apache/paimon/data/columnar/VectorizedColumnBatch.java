/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.BytesColumnVector.Bytes;

import java.io.Serializable;

/**
 * VectorizedColumnBatch 是一组行，每个列作为一个向量组织。它是查询执行的单位，组织起来以最小化每行的成本。
 * <p>VectorizedColumnBatch 受 Apache Hive VectorizedRowBatch 的影响。
 */
public class VectorizedColumnBatch implements Serializable {

    private static final long serialVersionUID = 8180323238728166155L;

    /**
     * 这个数字经过精心选择，以最小化开销，通常允许一个 VectorizedColumnBatch 适应缓存。
     */
    public static final int DEFAULT_SIZE = 2048;

    private int numRows;                       // 当前数据行数
    public final ColumnVector[] columns;       // 用于保存数据

    public VectorizedColumnBatch(ColumnVector[] vectors) {
        this.columns = vectors;
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public int getNumRows() {
        return numRows;
    }

    public int getArity() {
        return columns.length;
    }

    public boolean isNullAt(int rowId, int colId) {
        return columns[colId].isNullAt(rowId);
    }

    public boolean getBoolean(int rowId, int colId) {
        return ((BooleanColumnVector) columns[colId]).getBoolean(rowId);
    }

    public byte getByte(int rowId, int colId) {
        return ((ByteColumnVector) columns[colId]).getByte(rowId);
    }

    public short getShort(int rowId, int colId) {
        return ((ShortColumnVector) columns[colId]).getShort(rowId);
    }

    public int getInt(int rowId, int colId) {
        return ((IntColumnVector) columns[colId]).getInt(rowId);
    }

    public long getLong(int rowId, int colId) {
        return ((LongColumnVector) columns[colId]).getLong(rowId);
    }

    public float getFloat(int rowId, int colId) {
        return ((FloatColumnVector) columns[colId]).getFloat(rowId);
    }

    public double getDouble(int rowId, int colId) {
        return ((DoubleColumnVector) columns[colId]).getDouble(rowId);
    }

    public BinaryString getString(int rowId, int pos) {
        Bytes byteArray = getByteArray(rowId, pos);
        return BinaryString.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
    }

    public byte[] getBinary(int rowId, int pos) {
        Bytes byteArray = getByteArray(rowId, pos);
        if (byteArray.len == byteArray.data.length) {
            return byteArray.data;
        } else {
            byte[] ret = new byte[byteArray.len];
            System.arraycopy(byteArray.data, byteArray.offset, ret, 0, byteArray.len);
            return ret;
        }
    }

    public Bytes getByteArray(int rowId, int colId) {
        return ((BytesColumnVector) columns[colId]).getBytes(rowId);
    }

    public Decimal getDecimal(int rowId, int colId, int precision, int scale) {
        return ((DecimalColumnVector) (columns[colId])).getDecimal(rowId, precision, scale);
    }

    public Timestamp getTimestamp(int rowId, int colId, int precision) {
        return ((TimestampColumnVector) (columns[colId])).getTimestamp(rowId, precision);
    }

    public InternalArray getArray(int rowId, int colId) {
        return ((ArrayColumnVector) columns[colId]).getArray(rowId);
    }

    public InternalRow getRow(int rowId, int colId) {
        return ((RowColumnVector) columns[colId]).getRow(rowId);
    }

    public InternalMap getMap(int rowId, int colId) {
        return ((MapColumnVector) columns[colId]).getMap(rowId);
    }

    public VectorizedColumnBatch copy(ColumnVector[] vectors) {
        VectorizedColumnBatch vectorizedColumnBatch = new VectorizedColumnBatch(vectors);
        vectorizedColumnBatch.setNumRows(numRows);
        return vectorizedColumnBatch;
    }
}
