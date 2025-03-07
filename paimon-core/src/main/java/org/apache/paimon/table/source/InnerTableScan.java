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

import org.apache.paimon.manifest.ManifestFileScanner;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.utils.Filter;

import java.util.Map;

/**
 * Inner {@link TableScan} 包含谓词下推功能.
 **/
public interface InnerTableScan extends TableScan {

    // 谓词下推功能
    InnerTableScan withFilter(Predicate predicate);

    // 限制输出行数
    default InnerTableScan withLimit(int limit) {
        return this;
    }

    // 分区过滤器
    default InnerTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        return this;
    }

    // bucket 过滤器
    default InnerTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        return this;
    }

    // 层级过滤器
    default InnerTableScan withLevelFilter(Filter<Integer> levelFilter) {
        return this;
    }

    //
    default InnerTableScan withMetricsRegistry(MetricRegistry metricRegistry) {
        // do nothing, should implement this if need
        return this;
    }

    default InnerTableScan withManifestFileScanner(ManifestFileScanner manifestFileScanner){
        return this;
    }
}
