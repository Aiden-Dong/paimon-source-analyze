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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FieldsComparator;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;

/**
 * 该读取器用于读取已按键和序列号排序的 {@link RecordReader} 列表，并执行归并排序算法。
 *   具有相同键的 {@link KeyValue} 在归并排序期间也将合并。
 *
 * <p>注意：来自同一 {@link RecordReader} 的 {@link KeyValue} 不能包含相同的键。
 */
public interface SortMergeReader<T> extends RecordReader<T> {

    static <T> SortMergeReader<T> createSortMergeReader(
            List<RecordReader<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper,
            SortEngine sortEngine) {
        switch (sortEngine) {
            case MIN_HEAP:
                return new SortMergeReaderWithMinHeap<>(
                        readers, userKeyComparator, userDefinedSeqComparator, mergeFunctionWrapper);
            case LOSER_TREE:
                return new SortMergeReaderWithLoserTree<>(
                        readers, userKeyComparator, userDefinedSeqComparator, mergeFunctionWrapper);
            default:
                throw new UnsupportedOperationException("Unsupported sort engine: " + sortEngine);
        }
    }
}
