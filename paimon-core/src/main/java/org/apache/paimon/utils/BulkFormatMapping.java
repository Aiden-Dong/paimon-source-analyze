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

package org.apache.paimon.utils;

import org.apache.paimon.KeyValue;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * 具有索引映射和批量格式的类
 */
public class BulkFormatMapping {

    @Nullable private final int[] indexMapping;                  // 数据索引映射
    @Nullable private final CastFieldGetter[] castMapping;       // 数据转换映射
    @Nullable private final Pair<int[], RowType> partitionPair;  // 分区字段
    private final FormatReaderFactory bulkFormat;

    public BulkFormatMapping(
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable Pair<int[], RowType> partitionPair,
            FormatReaderFactory bulkFormat) {
        this.indexMapping = indexMapping;
        this.castMapping = castMapping;
        this.bulkFormat = bulkFormat;
        this.partitionPair = partitionPair;
    }

    @Nullable
    public int[] getIndexMapping() {
        return indexMapping;
    }

    @Nullable
    public CastFieldGetter[] getCastMapping() {
        return castMapping;
    }

    @Nullable
    public Pair<int[], RowType> getPartitionPair() {
        return partitionPair;
    }

    public FormatReaderFactory getReaderFactory() {
        return bulkFormat;
    }

    public static BulkFormatMappingBuilder newBuilder(
            FileFormatDiscover formatDiscover,
            KeyValueFieldsExtractor extractor,
            int[][] keyProjection,
            int[][] valueProjection,
            @Nullable List<Predicate> filters) {
        return new BulkFormatMappingBuilder(
                formatDiscover, extractor, keyProjection, valueProjection, filters);
    }

    /** Builder to build {@link BulkFormatMapping}. */
    public static class BulkFormatMappingBuilder {

        private final FileFormatDiscover formatDiscover;
        private final KeyValueFieldsExtractor extractor;
        private final int[][] keyProjection;               // key 投影关系
        private final int[][] valueProjection;             // value 投影关系
        @Nullable private final List<Predicate> filters;   // 谓词下推的数据过滤条件

        private BulkFormatMappingBuilder(
                FileFormatDiscover formatDiscover,
                KeyValueFieldsExtractor extractor,
                int[][] keyProjection,
                int[][] valueProjection,
                @Nullable List<Predicate> filters) {
            this.formatDiscover = formatDiscover;
            this.extractor = extractor;
            this.keyProjection = keyProjection;
            this.valueProjection = valueProjection;
            this.filters = filters;
        }

        public BulkFormatMapping build(String formatIdentifier, TableSchema tableSchema, TableSchema dataSchema) {
            List<DataField> tableKeyFields = extractor.keyFields(tableSchema);      // 获取 key 的字段信息
            List<DataField> tableValueFields = extractor.valueFields(tableSchema);  // 获取 value 的字段信息

            // 返回表的完整视图 :
            // RecordKey, sequenceField, recordKind, RecordValue
            int[][] tableProjection = KeyValue.project(keyProjection, valueProjection, tableKeyFields.size());

            List<DataField> dataKeyFields = extractor.keyFields(dataSchema);
            List<DataField> dataValueFields = extractor.valueFields(dataSchema);

            RowType keyType = new RowType(dataKeyFields);            // key 数据类型
            RowType valueType = new RowType(dataValueFields);        // value 数据类型
            RowType dataRecordType = KeyValue.schema(keyType, valueType);  // 完整的数据类型  RecordKey, sequenceField, recordKind, RecordValue


            int[][] dataKeyProjection = SchemaEvolutionUtil.createDataProjection(tableKeyFields, dataKeyFields, keyProjection);           // 正确处理 key 的投影关系
            int[][] dataValueProjection = SchemaEvolutionUtil.createDataProjection(tableValueFields, dataValueFields, valueProjection);   // 正确处理 value 的投影关系

            // 模式演化后的数据投影关系
            int[][] dataProjection = KeyValue.project(dataKeyProjection, dataValueProjection, dataKeyFields.size());

            /**
             * 我们需要在投影上创建索引映射，而不是分别在键和值上创建索引映射
             * 这里，例如
             *
             * <ul>
             *   <li>表键字段：1->d, 3->a, 4->b, 5->c
             *   <li>数据键字段：1->a, 2->b, 3->c
             * </ul>
             *
             * <p>表和数据的数值字段为 0->value_count，键和值的投影如下
             *
             * <ul>
             *   <li>表键投影：[0, 1, 2, 3]，值投影：[0]，数据投影：[0, 1, 2, 3, 4, 5, 6]，其中 4/5 是 seq/kind，6 是值
             *   <li>数据键投影：[0, 1, 2]，值投影：[0]，数据投影：[0, 1, 2, 3, 4, 5]，其中 3/4 是 seq/kind，5 是值
             * </ul>
             *
             * <p>我们将从上述中获得空的值索引映射，我们不能再基于键和值索引映射创建投影索引映射。
             */
            // 模式演化的映射关系
            IndexCastMapping indexCastMapping = SchemaEvolutionUtil.createIndexCastMapping(
                            Projection.of(tableProjection).toTopLevelIndexes(),
                            tableKeyFields,
                            tableValueFields,
                            Projection.of(dataProjection).toTopLevelIndexes(),
                            dataKeyFields,
                            dataValueFields);

            // 基于模式演化的数据过滤条件
            List<Predicate> dataFilters = tableSchema.id() == dataSchema.id()
                            ? filters
                            : SchemaEvolutionUtil.createDataFilters(tableSchema.fields(), dataSchema.fields(), filters);

            Pair<int[], RowType> partitionPair = null;   // 提取分区字段

            if (!dataSchema.partitionKeys().isEmpty()) {
                Pair<int[], int[][]> partitionMapping = PartitionUtils.constructPartitionMapping(
                        dataRecordType, dataSchema.partitionKeys(), dataProjection);

                // 如果分区字段未被选择，我们什么都不做。
                if (partitionMapping != null) {
                    dataProjection = partitionMapping.getRight();         // 拆分 paritition
                    partitionPair = Pair.of(                              // 提取分区
                            partitionMapping.getLeft(),
                            dataSchema.projectedLogicalRowType(dataSchema.partitionKeys()));
                }
            }

            // 生成数据查询 schema
            RowType projectedRowType = Projection.of(dataProjection).project(dataRecordType);

            return new BulkFormatMapping(
                    indexCastMapping.getIndexMapping(),
                    indexCastMapping.getCastMapping(),
                    partitionPair,
                    formatDiscover
                            .discover(formatIdentifier)
                            .createReaderFactory(projectedRowType, dataFilters));
        }
    }
}
