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
 * 用于批处理的 {@link TableWrite}。建议用于一次性提交。
 *
 * @since 0.4.0
 */
@Public
public interface BatchTableWrite extends TableWrite {

    /**
     * 为 {@link TableCommit} 准备提交。收集此写入的增量文件。
     *
     * @see BatchTableCommit#commit
     */
    List<CommitMessage> prepareCommit() throws Exception;
}
