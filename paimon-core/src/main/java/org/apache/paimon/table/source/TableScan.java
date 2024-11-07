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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.Table;

import java.util.List;

/**
 * 扫描 {@link Table} 以生成 {@link Split} 切片。
 */
@Public
public interface TableScan {

    /** Plan splits, throws {@link EndOfScanException} if the scan is ended. */
    Plan plan();

    // 列出所有分区
    List<BinaryRow> listPartitions();

    // 读取计划
    @Public
    interface Plan {
        List<Split> splits();
    }
}
