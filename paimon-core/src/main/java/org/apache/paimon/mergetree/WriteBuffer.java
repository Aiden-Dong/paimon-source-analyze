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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

/**
 * 仅用于追加的写入缓冲区，用于存储键值对。当缓冲区满时，它将被刷新到磁盘并形成一个数据文件。
 */
public interface WriteBuffer {

    /**
     * 写入一个带有序列号和值类型的记录。
     * @return 如果记录成功写入则返回 true；如果内存表已满则返回 false。
     */
    boolean put(long sequenceNumber, RowKind valueKind, InternalRow key, InternalRow value) throws IOException;

    // 此表的记录大小
    int size();

    // 此表的内存占用大小。
    long memoryOccupancy();

    // 刷新内存；如果不支持则返回 false。
    boolean flushMemory() throws IOException;

    /**
     * 对缓冲区中每个剩余元素执行指定操作，直到所有元素都已处理完毕或操作抛出异常为止。
     *
     * @param rawConsumer 消费未合并记录的消费者。
     * @param mergedConsumer 消费合并后记录的消费者。
     */
    void forEach(
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable KvConsumer rawConsumer,
            KvConsumer mergedConsumer)
            throws IOException;

    // 删除此表中的所有记录。调用返回后，表将为空。
    void clear();

    // 一个接受 KeyValue 并可能抛出异常的消费者。
    @FunctionalInterface
    interface KvConsumer {
        void accept(KeyValue kv) throws IOException;
    }
}
