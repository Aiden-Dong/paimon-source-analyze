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

package org.apache.paimon.table.source;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.splitread.IncrementalChangelogReadProvider;
import org.apache.paimon.table.source.splitread.IncrementalDiffReadProvider;
import org.apache.paimon.table.source.splitread.MergeFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.RawFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.SplitReadProvider;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * 抽象层，在 {@link MergeFileSplitRead} 之上提供 {@link InternalRow} 的读取。
 */
public final class KeyValueTableRead extends AbstractDataTableRead<KeyValue> {

    private final List<SplitReadProvider> readProviders;   // 读取工具类

    private int[][] projection = null;              // 查询映射关系
    private boolean forceKeepDelete = false;
    private Predicate predicate = null;             // 过滤条件
    private IOManager ioManager = null;

    public KeyValueTableRead(
            Supplier<MergeFileSplitRead> mergeReadSupplier,
            Supplier<RawFileSplitRead> batchRawReadSupplier,
            TableSchema schema) {
        super(schema);

        this.readProviders = Arrays.asList(
                        new RawFileSplitReadProvider(batchRawReadSupplier, this::assignValues),
                        new MergeFileSplitReadProvider(mergeReadSupplier, this::assignValues),
                        new IncrementalChangelogReadProvider(mergeReadSupplier, this::assignValues),
                        new IncrementalDiffReadProvider(mergeReadSupplier, this::assignValues)
        );

    }

    private List<SplitRead<InternalRow>> initialized() {
        List<SplitRead<InternalRow>> readers = new ArrayList<>();
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.initialized()) {
                readers.add(readProvider.getOrCreate());
            }
        }
        return readers;
    }

    private void assignValues(SplitRead<InternalRow> read) {
        if (forceKeepDelete) {
            read = read.forceKeepDelete();
        }
        read.withProjection(projection).withFilter(predicate).withIOManager(ioManager);
    }

    @Override
    public void projection(int[][] projection) {
        initialized().forEach(r -> r.withProjection(projection));
        this.projection = projection;
    }

    @Override
    public InnerTableRead forceKeepDelete() {
        initialized().forEach(SplitRead::forceKeepDelete);
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    protected InnerTableRead innerWithFilter(Predicate predicate) {
        initialized().forEach(r -> r.withFilter(predicate));
        this.predicate = predicate;
        return this;
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        initialized().forEach(r -> r.withIOManager(ioManager));
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public RecordReader<InternalRow> reader(Split split) throws IOException {
        DataSplit dataSplit = (DataSplit) split;
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.match(dataSplit, forceKeepDelete)) {
                return readProvider.getOrCreate().createReader(dataSplit);
            }
        }

        throw new RuntimeException("Should not happen.");
    }

    public static RecordReader<InternalRow> unwrap(RecordReader<KeyValue> reader) {
        return new RecordReader<InternalRow>() {

            @Nullable
            @Override
            public RecordIterator<InternalRow> readBatch() throws IOException {
                RecordIterator<KeyValue> batch = reader.readBatch();
                return batch == null ? null : new ValueContentRowDataRecordIterator(batch);
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
