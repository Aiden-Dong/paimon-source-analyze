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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * 用于构建 {@link BatchTableWrite} 和 {@link BatchTableCommit} 的接口。
 * <p>分布式批处理写入示例：
 * <pre>
 *   {@code
 *      // 1. 创建一个 WriteBuilder（可序列化）
 *
 *      Table table = catalog.getTable(...);
 *      WriteBuilder builder = table.newWriteBuilder();
 *
 *      // 2. 在分布式任务中写入记录
 *      BatchTableWrite write = builder.newWrite();
 *      write.write(...);
 *      write.write(...);
 *      write.write(...);
 *      List<CommitMessage> messages = write.prepareCommit();
 *
 *      // 3. 收集所有 CommitMessage 到全局节点并提交
 *
 *      BatchTableCommit commit = builder.newCommit();
 *      commit.commit(allCommitMessages());
 *    }
 *  </pre>
 * @since 0.4.0
 * */
@Public
public interface BatchWriteBuilder extends WriteBuilder {

    long COMMIT_IDENTIFIER = Long.MAX_VALUE;

    /** Overwrite writing, same as the 'INSERT OVERWRITE' semantics of SQL. */
    default BatchWriteBuilder withOverwrite() {
        withOverwrite(Collections.emptyMap());
        return this;
    }

    /** Overwrite writing, same as the 'INSERT OVERWRITE T PARTITION (...)' semantics of SQL. */
    BatchWriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition);

    /** Create a {@link TableWrite} to write {@link InternalRow}s. */
    @Override
    BatchTableWrite newWrite();

    /** Create a {@link TableCommit} to commit {@link CommitMessage}s. */
    @Override
    BatchTableCommit newCommit();
}
