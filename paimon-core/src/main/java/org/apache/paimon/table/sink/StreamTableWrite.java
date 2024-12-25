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

import java.util.List;

/**
 * 用于流处理的 {@link TableWrite}。您可以使用此类多次提交。
 *
 * @since 0.4.0
 * @see StreamWriteBuilder
 */
@Public
public interface StreamTableWrite extends TableWrite {

    /**
     * 为 {@link TableCommit} 准备提交。收集此写入的增量文件。
     *
     * @param waitCompaction 是否等待后台压缩结束。
     * @param commitIdentifier 已提交的事务 ID，可以从 0 开始。如果有多个提交，请递增此 ID。
     * @see StreamTableCommit#commit
     */
    List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception;
}
