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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapBooleanVector;
import org.apache.paimon.data.columnar.heap.HeapByteVector;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapDoubleVector;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.data.columnar.heap.HeapShortVector;
import org.apache.paimon.data.columnar.heap.HeapTimestampVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.types.DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;

/** Array {@link ColumnReader}. TODO Currently ARRAY type only support non nested case. */
public class ArrayColumnReader extends BaseVectorizedColumnReader {

    // The value read in last time
    private Object lastValue;

    // flag to indicate if there is no data in parquet data page
    private boolean eof = false;

    // flag to indicate if it's the first time to read parquet data page with this instance
    boolean isFirstRow = true;

    public ArrayColumnReader(
            ColumnDescriptor descriptor,
            PageReadStore pageReadStore,
            boolean isUtcTimestamp,
            Type type,
            DataType dataType)
            throws IOException {
        super(descriptor, pageReadStore, isUtcTimestamp, type, dataType);
    }

    @Override
    public void readToVector(int readNumber, WritableColumnVector vector) {
        HeapArrayVector lcv = (HeapArrayVector) vector;
        // before readBatch, initial the size of offsets & lengths as the default value,
        // the actual size will be assigned in setChildrenInfo() after reading complete.
        lcv.setOffsets(new long[VectorizedColumnBatch.DEFAULT_SIZE]);
        lcv.setLengths(new long[VectorizedColumnBatch.DEFAULT_SIZE]);

        DataType elementType = ((ArrayType) dataType).getElementType();

        // read the first row in parquet data page, this will be only happened once for this
        // instance
        if (isFirstRow) {
            if (!fetchNextValue(elementType)) {
                return;
            }
            isFirstRow = false;
        }

        // Because the length of ListColumnVector.child can't be known now,
        // the valueList will save all data for ListColumnVector temporary.
        List<Object> valueList = new ArrayList<>();

        int index = collectDataFromParquetPage(readNumber, lcv, valueList, elementType);
        // Convert valueList to array for the ListColumnVector.child
        fillColumnVector(elementType, lcv, valueList, index);
    }

    /**
     * Reads a single value from parquet page, puts it into lastValue. Returns a boolean indicating
     * if there is more values to read (true).
     *
     * @param type the element type of array
     * @return boolean
     */
    private boolean fetchNextValue(DataType type) {
        boolean ready = false;

        while (!ready) {

            if (readState.isFinish()){
                eof = true;
                break;
            }

            readPageIfNeed();  //  如果需要将读取一个新的 page, 返回可读数据量

            if (readState.valuesToReadInPage <= 0){  // 没有数据
                eof = true;
                break;
            }

            long pageRowId = readState.rowId;
            long rangeStart = readState.currentRangeStart();
            long rangeEnd = readState.currentRangeEnd();

            int leftInPage = readState.valuesToReadInPage;  // 当前Page的可读数据量

            if (pageRowId < rangeStart) {  // 有无效数据
                long toSkip = (rangeStart - pageRowId);

                if (toSkip >= leftInPage) {  // 所有数据都抛弃
                    pageRowId += leftInPage;
                    leftInPage = 0;
                } else {
                    skipDatas(toSkip, type);
                    pageRowId += toSkip;
                    leftInPage -= toSkip;
                }
            }else if (pageRowId > rangeEnd){
                readState.nextRange();
            }else{
                // get the values of repetition and definitionLevel
                readRepetitionAndDefinitionLevels();
                // read the data if it isn't null
                if (definitionLevel == maxDefLevel) {
                    if (isCurrentPageDictionaryEncoded) {
                        lastValue = dataColumn.readValueDictionaryId();
                    } else {
                        lastValue = readPrimitiveTypedRow(type);
                    }
                } else {
                    lastValue = null;
                }
                pageRowId += 1;
                leftInPage -= 1;
                ready = true;
            }

            if (pageRowId == -1){
                System.out.println("=========");
            }
            readState.valuesToReadInPage = leftInPage;
            readState.rowId = pageRowId;
        }

        return ready;
    }


    private boolean fetchNextValueNEW(DataType type) {
        boolean ready = false;

        while (!ready) {

            if (readState.isFinish()){
                eof = true;
                break;
            }

            readPageIfNeed();  //  如果需要将读取一个新的 page, 返回可读数据量

            if (readState.valuesToReadInPage <= 0){  // 没有数据
                eof = true;
                break;
            }

            long pageRowId = readState.rowId;
            long rangeStart = readState.currentRangeStart();
            long rangeEnd = readState.currentRangeEnd();

            int leftInPage = readState.valuesToReadInPage;  // 当前Page的可读数据量

            if (pageRowId < rangeStart) {  // 有无效数据
                long toSkip = (rangeStart - pageRowId);

                if (toSkip >= leftInPage) {  // 所有数据都抛弃
                    pageRowId += leftInPage;
                    leftInPage = 0;
                } else {
                    skipDatas(toSkip, type);
                    pageRowId += toSkip;
                    leftInPage -= toSkip;
                }
            }else if (pageRowId > rangeEnd){
                readState.nextRange();
            }else{
                // get the values of repetition and definitionLevel
                readRepetitionAndDefinitionLevels();
                // read the data if it isn't null
                if (definitionLevel == maxDefLevel) {
                    if (isCurrentPageDictionaryEncoded) {
                        lastValue = dataColumn.readValueDictionaryId();
                    } else {
                        lastValue = readPrimitiveTypedRow(type);
                    }
                } else {
                    lastValue = null;
                }
                pageRowId += 1;
                leftInPage -= 1;
                ready = true;
            }

            if (pageRowId == -1){
                System.out.println("=========");
            }
            readState.valuesToReadInPage = leftInPage;
            readState.rowId = pageRowId;
        }

        return ready;
    }




    private void skipDatas(long num, DataType type){

        int index = 0;
        int repetitionLevelSkip;
        int definitionLevelSkip;

        while (index < num) {

            do {
                repetitionLevelSkip = repetitionLevelColumn.nextInt();

                definitionLevelSkip = definitionLevelColumn.nextInt();

                // read the data if it isn't null
                if (repetitionLevelSkip == maxDefLevel) {
                    if (isCurrentPageDictionaryEncoded) {
                        dataColumn.readValueDictionaryId();
                    } else {
                         readPrimitiveTypedRow(type);
                    }
                }

            } while ((repetitionLevelSkip != 0));
            index++;
        }

    }

    private void readPageIfNeed() {
//        // Compute the number of values we want to read in this page.
//        int leftInPage = (int) (endOfPageValueCount - valuesRead);
//        if (leftInPage == 0) {
//            // no data left in current page, load data from new page
//            readPage();
//            leftInPage = (int) (endOfPageValueCount - valuesRead);
//        }
//        return leftInPage;

        if (readState.valuesToReadInPage == 0) {
            int pageValueCount = readPage();
            // 返回当前 page 的数据量
            if (pageValueCount < 0) {
                // we've read all the pages; this could happen when we're reading a repeated list and we
                // don't know where the list will end until we've seen all the pages.
                return;
            }
        }

    }

    // Need to be in consistent with that VectorizedPrimitiveColumnReader#readBatchHelper
    // TODO Reduce the duplicated code
    private Object readPrimitiveTypedRow(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return dataColumn.readBytes();
            case BOOLEAN:
                return dataColumn.readBoolean();
            case TIME_WITHOUT_TIME_ZONE:
            case DATE:
            case INTEGER:
                return dataColumn.readInteger();
            case TINYINT:
                return dataColumn.readTinyInt();
            case SMALLINT:
                return dataColumn.readSmallInt();
            case BIGINT:
                return dataColumn.readLong();
            case FLOAT:
                return dataColumn.readFloat();
            case DOUBLE:
                return dataColumn.readDouble();
            case DECIMAL:
                switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return dataColumn.readInteger();
                    case INT64:
                        return dataColumn.readLong();
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        return dataColumn.readBytes();
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int precision;
                if (type.getTypeRoot() == TIMESTAMP_WITHOUT_TIME_ZONE) {
                    precision = ((TimestampType) type).getPrecision();
                } else {
                    precision = ((LocalZonedTimestampType) type).getPrecision();
                }

                if (precision <= 3) {
                    return dataColumn.readMillsTimestamp();
                } else if (precision <= 6) {
                    return dataColumn.readMicrosTimestamp();
                } else {
                    throw new RuntimeException(
                            "Unsupported precision of time type in the list: " + precision);
                }
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }

    private Object dictionaryDecodeValue(DataType type, Integer dictionaryValue) {
        if (dictionaryValue == null) {
            return null;
        }

        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return dictionary.readBytes(dictionaryValue);
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTEGER:
                return dictionary.readInteger(dictionaryValue);
            case BOOLEAN:
                return dictionary.readBoolean(dictionaryValue) ? 1 : 0;
            case DOUBLE:
                return dictionary.readDouble(dictionaryValue);
            case FLOAT:
                return dictionary.readFloat(dictionaryValue);
            case TINYINT:
                return dictionary.readTinyInt(dictionaryValue);
            case SMALLINT:
                return dictionary.readSmallInt(dictionaryValue);
            case BIGINT:
                return dictionary.readLong(dictionaryValue);
            case DECIMAL:
                switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return dictionary.readInteger(dictionaryValue);
                    case INT64:
                        return dictionary.readLong(dictionaryValue);
                    case FIXED_LEN_BYTE_ARRAY:
                    case BINARY:
                        return dictionary.readBytes(dictionaryValue);
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return dictionary.readTimestamp(dictionaryValue);
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }

    /**
     * Collects data from a parquet page and returns the final row index where it stopped. The
     * returned index can be equal to or less than total.
     * 从 Parquet 页中收集数据，并返回停止收集数据的最终行索引。
     * 返回的索引值可以等于或小于总行数。
     *
     * @param total  要收集的最大行数
     * @param lcv  用于在数据收集期间进行初始设置的列向量
     * @param valueList 将稍后馈送到向量中的值集合
     * @param type 数组的元素类型
     * @return int
     */
    private int collectDataFromParquetPage(
            int total, HeapArrayVector lcv, List<Object> valueList, DataType type) {
        int index = 0;
        /*
         * Here is a nested loop for collecting all values from a parquet page.
         * 这是一个用于从 Parquet 页中收集所有值的嵌套循环。
         * A column of array type can be considered as a list of lists, so the two loops are as below:
         * 一列数组类型可以被视为一个列表的列表，因此有两个循环如下:
         * 1. The outer loop iterates on rows (index is a row index, so points to a row in the batch), e.g.:
         * 1. 外层循环迭代行（索引是行索引，因此指向批处理中的一行），例如：
         * [0, 2, 3]    <- index: 0
         * [NULL, 3, 4] <- index: 1
         *
         * 2. 内层循环迭代一行内的值（为 ListColumnVector 中的一个元素设置来自 Parquet 数据页的所有数据），
         * 因此 fetchNextValue 函数一次返回一个值：
         * 0, 2, 3, NULL, 3, 4
         *
         * As described below, the repetition level (repetitionLevel != 0)
         * can be used to decide when we'll start to read values for the next list.
         *
         * 如下所述，重复级别（repetitionLevel != 0） 可以用来决定何时开始读取下一个列表的值。
         */
        while (!eof && index < total) {
            // add element to ListColumnVector one by one
            lcv.getOffsets()[index] = valueList.size();
            /*
             * Let's collect all values for a single list.
             * Repetition level = 0 means that a new list started there in the parquet page,
             * in that case, let's exit from the loop, and start to collect value for a new list.
             */

            do {
                /*
                 * Definition level = 0 when a NULL value was returned instead of a list
                 * (this is not the same as a NULL value in of a list).
                 */
                if (definitionLevel == 0) {
                    lcv.setNullAt(index);
                }
                valueList.add(
                        isCurrentPageDictionaryEncoded
                                ? dictionaryDecodeValue(type, (Integer) lastValue)
                                : lastValue);
            } while (fetchNextValue(type) && (repetitionLevel != 0));

            lcv.getLengths()[index] = valueList.size() - lcv.getOffsets()[index];
            index++;
        }
        return index;
    }

    /**
     * The lengths & offsets will be initialized as default size (1024), it should be set to the
     * actual size according to the element number.
     */
    private void setChildrenInfo(HeapArrayVector lcv, int itemNum, int elementNum) {
        lcv.setSize(itemNum);
        long[] lcvLength = new long[elementNum];
        long[] lcvOffset = new long[elementNum];
        System.arraycopy(lcv.getLengths(), 0, lcvLength, 0, elementNum);
        System.arraycopy(lcv.getOffsets(), 0, lcvOffset, 0, elementNum);
        lcv.setLengths(lcvLength);
        lcv.setOffsets(lcvOffset);
    }

    private void fillColumnVector(
            DataType type, HeapArrayVector lcv, List valueList, int elementNum) {
        int total = valueList.size();
        setChildrenInfo(lcv, total, elementNum);
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                HeapBytesVector bytesVector = new HeapBytesVector(total);
                bytesVector.reset();
                lcv.setChild(bytesVector);
                for (int i = 0; i < valueList.size(); i++) {
                    byte[] src = (byte[]) valueList.get(i);
                    if (src == null) {
                        ((HeapBytesVector) lcv.getChild()).setNullAt(i);
                    } else {
                        ((HeapBytesVector) lcv.getChild()).appendBytes(i, src, 0, src.length);
                    }
                }
                break;
            case BOOLEAN:
                HeapBooleanVector booleanVector = new HeapBooleanVector(total);
                booleanVector.reset();
                lcv.setChild(booleanVector);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapBooleanVector) lcv.getChild()).setNullAt(i);
                    } else {
                        ((HeapBooleanVector) lcv.getChild()).vector[i] = (boolean) valueList.get(i);
                    }
                }
                break;
            case TINYINT:
                HeapByteVector byteVector = new HeapByteVector(total);
                byteVector.reset();
                lcv.setChild(byteVector);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapByteVector) lcv.getChild()).setNullAt(i);
                    } else {
                        ((HeapByteVector) lcv.getChild()).vector[i] =
                                ((List<Integer>) valueList).get(i).byteValue();
                    }
                }
                break;
            case SMALLINT:
                HeapShortVector shortVector = new HeapShortVector(total);
                shortVector.reset();
                lcv.setChild(shortVector);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapShortVector) lcv.getChild()).setNullAt(i);
                    } else {
                        ((HeapShortVector) lcv.getChild()).vector[i] =
                                ((List<Integer>) valueList).get(i).shortValue();
                    }
                }
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                HeapIntVector intVector = new HeapIntVector(total);
                intVector.reset();
                lcv.setChild(intVector);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapIntVector) lcv.getChild()).setNullAt(i);
                    } else {
                        ((HeapIntVector) lcv.getChild()).vector[i] =
                                ((List<Integer>) valueList).get(i);
                    }
                }
                break;
            case FLOAT:
                HeapFloatVector floatVector = new HeapFloatVector(total);
                floatVector.reset();
                lcv.setChild(floatVector);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapFloatVector) lcv.getChild()).setNullAt(i);
                    } else {
                        ((HeapFloatVector) lcv.getChild()).vector[i] =
                                ((List<Float>) valueList).get(i);
                    }
                }
                break;
            case BIGINT:
                HeapLongVector longVector = new HeapLongVector(total);
                longVector.reset();
                lcv.setChild(longVector);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapLongVector) lcv.getChild()).setNullAt(i);
                    } else {
                        ((HeapLongVector) lcv.getChild()).vector[i] =
                                ((List<Long>) valueList).get(i);
                    }
                }
                break;
            case DOUBLE:
                HeapDoubleVector doubleVector = new HeapDoubleVector(total);
                doubleVector.reset();
                lcv.setChild(doubleVector);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapDoubleVector) lcv.getChild()).setNullAt(i);
                    } else {
                        ((HeapDoubleVector) lcv.getChild()).vector[i] =
                                ((List<Double>) valueList).get(i);
                    }
                }
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (descriptor.getPrimitiveType().getPrimitiveTypeName()
                        == PrimitiveType.PrimitiveTypeName.INT64) {
                    HeapTimestampVector heapTimestampVector = new HeapTimestampVector(total);
                    heapTimestampVector.reset();
                    lcv.setChild(new ParquetTimestampVector(heapTimestampVector));
                    for (int i = 0; i < valueList.size(); i++) {
                        if (valueList.get(i) == null) {
                            ((HeapTimestampVector)
                                            ((ParquetTimestampVector) lcv.getChild()).getVector())
                                    .setNullAt(i);
                        } else {
                            ((HeapTimestampVector)
                                            ((ParquetTimestampVector) lcv.getChild()).getVector())
                                    .fill(((List<Timestamp>) valueList).get(i));
                        }
                    }
                    break;
                } else {
                    HeapTimestampVector timestampVector = new HeapTimestampVector(total);
                    timestampVector.reset();
                    lcv.setChild(timestampVector);
                    for (int i = 0; i < valueList.size(); i++) {
                        if (valueList.get(i) == null) {
                            ((HeapTimestampVector) lcv.getChild()).setNullAt(i);
                        } else {
                            ((HeapTimestampVector) lcv.getChild())
                                    .setTimestamp(i, ((List<Timestamp>) valueList).get(i));
                        }
                    }
                    break;
                }
            case DECIMAL:
                switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        HeapIntVector heapIntVector = new HeapIntVector(total);
                        heapIntVector.reset();
                        lcv.setChild(new ParquetDecimalVector(heapIntVector));
                        for (int i = 0; i < valueList.size(); i++) {
                            if (valueList.get(i) == null) {
                                ((HeapIntVector)
                                                ((ParquetDecimalVector) lcv.getChild()).getVector())
                                        .setNullAt(i);
                            } else {
                                ((HeapIntVector)
                                                        ((ParquetDecimalVector) lcv.getChild())
                                                                .getVector())
                                                .vector[i] =
                                        ((List<Integer>) valueList).get(i);
                            }
                        }
                        break;
                    case INT64:
                        HeapLongVector heapLongVector = new HeapLongVector(total);
                        heapLongVector.reset();
                        lcv.setChild(new ParquetDecimalVector(heapLongVector));
                        for (int i = 0; i < valueList.size(); i++) {
                            if (valueList.get(i) == null) {
                                ((HeapLongVector)
                                                ((ParquetDecimalVector) lcv.getChild()).getVector())
                                        .setNullAt(i);
                            } else {
                                ((HeapLongVector)
                                                        ((ParquetDecimalVector) lcv.getChild())
                                                                .getVector())
                                                .vector[i] =
                                        ((List<Long>) valueList).get(i);
                            }
                        }
                        break;
                    default:
                        HeapBytesVector heapBytesVector = new HeapBytesVector(total);
                        heapBytesVector.reset();
                        lcv.setChild(new ParquetDecimalVector(heapBytesVector));
                        for (int i = 0; i < valueList.size(); i++) {
                            byte[] src = (byte[]) valueList.get(i);
                            if (valueList.get(i) == null) {
                                ((HeapBytesVector)
                                                ((ParquetDecimalVector) lcv.getChild()).getVector())
                                        .setNullAt(i);
                            } else {
                                ((HeapBytesVector)
                                                ((ParquetDecimalVector) lcv.getChild()).getVector())
                                        .appendBytes(i, src, 0, src.length);
                            }
                        }
                        break;
                }
                break;
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }
}
