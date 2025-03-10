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

package org.apache.paimon.table;

import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.BatchWriteBuilderImpl;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilderImpl;
import org.apache.paimon.table.source.InnerStreamTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ReadBuilderImpl;

/**
 * Inner table for implementation, provide newScan, newRead ... directly.
 *
 * 主要提供两种实现 :
 *
 *   - {@link org.apache.paimon.table.PrimaryKeyFileStoreTable}
 *   - {@link org.apache.paimon.table.AppendOnlyFileStoreTable}
 *
 **/
public interface InnerTable extends Table {

    // 主要用来获取当前要读取的文件信息
    InnerTableScan newScan();

    InnerStreamTableScan newStreamScan();

    InnerTableRead newRead();

    InnerTableWrite newWrite(String commitUser);

    InnerTableCommit newCommit(String commitUser);

    @Override
    default ReadBuilder newReadBuilder() {
        return new ReadBuilderImpl(this);
    }

    @Override
    default BatchWriteBuilder newBatchWriteBuilder() {
        return new BatchWriteBuilderImpl(this);
    }

    @Override
    default StreamWriteBuilder newStreamWriteBuilder() {
        return new StreamWriteBuilderImpl(this);
    }
}
