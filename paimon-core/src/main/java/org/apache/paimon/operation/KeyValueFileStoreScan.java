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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.stats.FieldStatsConverters;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;

/** {@link FileStoreScan} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreScan extends AbstractFileStoreScan {

    private final FieldStatsConverters fieldKeyStatsConverters;
    private final FieldStatsConverters fieldValueStatsConverters;

    private Predicate keyFilter;
    private Predicate valueFilter;
    private final boolean deletionVectorsEnabled;
    private final MergeEngine mergeEngine;

    public KeyValueFileStoreScan(
            RowType partitionType,
            ScanBucketFilter bucketFilter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets,
            Integer scanManifestParallelism,
            String branchName,
            boolean deletionVectorsEnabled,
            MergeEngine mergeEngine) {
        super(
                partitionType,
                bucketFilter,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                manifestListFactory,
                numOfBuckets,
                checkNumOfBuckets,
                scanManifestParallelism,
                branchName);
        this.fieldKeyStatsConverters =
                new FieldStatsConverters(
                        sid -> keyValueFieldsExtractor.keyFields(scanTableSchema(sid)),
                        schema.id());
        this.fieldValueStatsConverters =
                new FieldStatsConverters(
                        sid -> keyValueFieldsExtractor.valueFields(scanTableSchema(sid)),
                        schema.id());
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.mergeEngine = mergeEngine;
    }

    public KeyValueFileStoreScan withKeyFilter(Predicate predicate) {
        this.keyFilter = predicate;
        this.bucketKeyFilter.pushdown(predicate);
        return this;
    }

    public KeyValueFileStoreScan withValueFilter(Predicate predicate) {
        this.valueFilter = predicate;
        return this;
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        Predicate filter = null;
        FieldStatsArraySerializer serializer = null;
        BinaryTableStats stats = null;
        if ((deletionVectorsEnabled || mergeEngine == FIRST_ROW)
                && entry.level() > 0
                && valueFilter != null) {
            filter = valueFilter;
            serializer = fieldValueStatsConverters.getOrCreate(entry.file().schemaId());
            stats = entry.file().valueStats();
        }

        if (filter == null && keyFilter != null) {
            filter = keyFilter;
            serializer = fieldKeyStatsConverters.getOrCreate(entry.file().schemaId());
            stats = entry.file().keyStats();
        }

        if (filter == null) {
            return true;
        }

        return filter.test(
                entry.file().rowCount(),
                serializer.evolution(stats.minValues()),
                serializer.evolution(stats.maxValues()),
                serializer.evolution(stats.nullCounts(), entry.file().rowCount()));
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected List<ManifestEntry> filterWholeBucketByStats(List<ManifestEntry> entries) {
        if (valueFilter == null) {
            return entries;
        }

        return noOverlapping(entries)
                ? filterWholeBucketPerFile(entries)   // 如果只有一个文件，或者是这个文件没有 level0 层文件，则 都可以进行过滤
                : filterWholeBucketAllFiles(entries); // 如果存在 level0 层文件, 则需要看整个 bucket 是否满足
    }

    private List<ManifestEntry> filterWholeBucketPerFile(List<ManifestEntry> entries) {
        List<ManifestEntry> filtered = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            if (filterByValueFilter(entry)) {
                filtered.add(entry);
            }
        }
        return filtered;
    }

    private List<ManifestEntry> filterWholeBucketAllFiles(List<ManifestEntry> entries) {
        // 条目来自同一个 bucket，如果其中任何一个不满足请求，我们可以过滤掉整个 bucket。
        for (ManifestEntry entry : entries) {
            if (filterByValueFilter(entry)) {
                return entries;
            }
        }
        return Collections.emptyList();
    }

    private boolean filterByValueFilter(ManifestEntry entry) {
        FieldStatsArraySerializer serializer =
                fieldValueStatsConverters.getOrCreate(entry.file().schemaId());
        BinaryTableStats stats = entry.file().valueStats();
        return valueFilter.test(
                entry.file().rowCount(),
                serializer.evolution(stats.minValues()),
                serializer.evolution(stats.maxValues()),
                serializer.evolution(stats.nullCounts(), entry.file().rowCount()));
    }

    private static boolean noOverlapping(List<ManifestEntry> entries) {
        if (entries.size() <= 1) {
            return true;
        }

        Integer previousLevel = null;
        for (ManifestEntry entry : entries) {
            int level = entry.file().level();
            // level 0 files have overlapping
            if (level == 0) {
                return false;
            }

            if (previousLevel == null) {
                previousLevel = level;
            } else {
                // different level, have overlapping
                if (previousLevel != level) {
                    return false;
                }
            }
        }

        return true;
    }
}
