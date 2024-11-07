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

package org.apache.paimon.schema;

import org.apache.paimon.KeyValue;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateReplaceVisitor;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * 模式演化工具
 **/
public class SchemaEvolutionUtil {

    private static final int NULL_FIELD_INDEX = -1;

    /**
     * 从表字段到底层数据字段创建索引映射。例如，表和数据字段如下：
     *
     * <ul>
     *   <li>表字段：1->c, 6->b, 3->a
     *   <li>数据字段：1->a, 3->c
     * </ul>
     *
     * <p>我们可以得到索引映射 [0, -1, 1]，其中 0 是表字段 1->c 在数据字段中的索引，1 是 6->b 在数据字段中的索引，1 是 3->a 在数据字段中的索引。
     *
     * <p>/// TODO 当支持嵌套模式演化时，应该支持嵌套索引映射。
     *
     * @param tableFields 表的字段
     * @param dataFields 底层数据的字段
     * @return 索引映射
     */
    @Nullable
    public static int[] createIndexMapping(
            List<DataField> tableFields, List<DataField> dataFields) {
        int[] indexMapping = new int[tableFields.size()];
        Map<Integer, Integer> fieldIdToIndex = new HashMap<>();
        for (int i = 0; i < dataFields.size(); i++) {
            fieldIdToIndex.put(dataFields.get(i).id(), i);
        }

        for (int i = 0; i < tableFields.size(); i++) {
            int fieldId = tableFields.get(i).id();
            Integer dataFieldIndex = fieldIdToIndex.get(fieldId);
            if (dataFieldIndex != null) {
                indexMapping[i] = dataFieldIndex;
            } else {
                indexMapping[i] = NULL_FIELD_INDEX;
            }
        }

        for (int i = 0; i < indexMapping.length; i++) {
            if (indexMapping[i] != i) {
                return indexMapping;
            }
        }
        return null;
    }

    /**
     * 从表投影到底层数据投影创建索引映射。例如，表和数据字段如下：
     *
     * <ul>
     *   <li>表字段：1->c, 3->a, 4->e, 5->d, 6->b
     *   <li>数据字段：1->a, 2->b, 3->c, 4->d
     * </ul>
     *
     * <p>表和数据顶部投影如下：
     *
     * <ul>
     *   <li>表投影：[0, 4, 1]
     *   <li>数据投影：[0, 2]
     * </ul>
     *
     * <p>我们可以首先从它们的字段获取表和数据投影的字段列表，如下所示：
     *
     * <ul>
     *   <li>表投影字段列表：[1->c, 6->b, 3->a]
     *   <li>数据投影字段列表：[1->a, 3->c]
     * </ul>
     *
     * <p>然后基于字段列表创建索引映射，并基于索引映射创建转换映射。
     *
     * <p>/// TODO 当支持嵌套模式演化时，应该支持嵌套索引映射。
     *
     * @param tableProjection 表投影
     * @param tableFields 表中的字段
     * @param dataProjection 底层数据投影
     * @param dataFields 底层数据中的字段
     * @return 索引映射
     */
    public static IndexCastMapping createIndexCastMapping(
            int[] tableProjection,
            List<DataField> tableFields,
            int[] dataProjection,
            List<DataField> dataFields) {
        return createIndexCastMapping(
                projectDataFields(tableProjection, tableFields),
                projectDataFields(dataProjection, dataFields));
    }

    /** 从表字段到底层数据字段创建索引映射 */
    public static IndexCastMapping createIndexCastMapping(
            List<DataField> tableFields, List<DataField> dataFields) {
        int[] indexMapping = createIndexMapping(tableFields, dataFields);
        CastFieldGetter[] castMapping =
                createCastFieldGetterMapping(tableFields, dataFields, indexMapping);
        return new IndexCastMapping() {
            @Nullable
            @Override
            public int[] getIndexMapping() {
                return indexMapping;
            }

            @Nullable
            @Override
            public CastFieldGetter[] getCastMapping() {
                return castMapping;
            }
        };
    }

    private static List<DataField> projectDataFields(int[] projection, List<DataField> dataFields) {
        List<DataField> projectFields = new ArrayList<>(projection.length);
        for (int index : projection) {
            projectFields.add(dataFields.get(index));
        }

        return projectFields;
    }

    /**
     * 从表投影到具有键和值字段的数据创建索引映射。我们应该首先创建表和数据字段及其键/值字段，然后使用它们的投影和字段创建索引映射。例如，表和数据投影和字段如下：
     *
     * <ul>
     *   <li>表键字段：1->ka, 3->kb, 5->kc, 6->kd；值字段：0->a, 2->d, 4->b；
     *       投影：[0, 2, 3, 4, 5, 7]，其中 0 是 1->ka，2 是 5->kc，3 是 5->kc，4/5 是 seq 和 kind，7 是 2->d
     *   <li>数据键字段：1->kb, 5->ka；值字段：2->aa, 4->f；投影：[0, 1, 2, 3, 4]，
     *       其中 0 是 1->kb，1 是 5->ka，2/3 是 seq 和 kind，4 是 2->aa
     * </ul>
     *
     * <p>首先，我们将从表和数据字段中获取最大键 ID，即 6，然后在其上创建表和数据字段：
     *
     * <ul>
     *   <li>表字段：1->ka, 3->kb, 5->kc, 6->kd, 7->seq, 8->kind, 9->a, 11->d, 13->b
     *   <li>数据字段：1->kb, 5->ka, 7->seq, 8->kind, 11->aa, 13->f
     * </ul>
     *
     * <p>最后，我们可以使用表/数据投影和字段创建索引映射，并基于索引映射创建转换映射。
     *
     * <p>/// TODO 当支持嵌套模式演化时，应该支持嵌套索引映射。
     *
     * @param tableProjection 表投影
     * @param tableKeyFields 表键字段
     * @param tableValueFields 表值字段
     * @param dataProjection 数据投影
     * @param dataKeyFields 数据键字段
     * @param dataValueFields 数据值字段
     * @return 结果索引和转换映射
     */
    public static IndexCastMapping createIndexCastMapping(
            int[] tableProjection,
            List<DataField> tableKeyFields,
            List<DataField> tableValueFields,
            int[] dataProjection,
            List<DataField> dataKeyFields,
            List<DataField> dataValueFields) {
        int maxKeyId =
                Math.max(
                        tableKeyFields.stream().mapToInt(DataField::id).max().orElse(0),
                        dataKeyFields.stream().mapToInt(DataField::id).max().orElse(0));
        List<DataField> tableFields =
                KeyValue.createKeyValueFields(tableKeyFields, tableValueFields, maxKeyId);
        List<DataField> dataFields =
                KeyValue.createKeyValueFields(dataKeyFields, dataValueFields, maxKeyId);
        return createIndexCastMapping(tableProjection, tableFields, dataProjection, dataFields);
    }
    /**
     * 从表投影创建数据投影。例如，表和数据字段如下：
     *
     * <ul>
     *   <li>表字段:   1->c, 3->a, 4->e, 5->d, 6->b
     *   <li>数据字段： 1->a, 2->b, 3->c, 4->d
     * </ul>
     *
     * <p>当我们从表字段投影 1->c, 6->b, 3->a 时，
     *    表投影为 [[0], [4], [1]]，其中 0 是字段 1->c 的索引，4 是字段 6->b 的索引，1 是字段 3->a 在表字段中的索引。
     *    我们需要从 [[0], [4], [1]] 创建数据投影，如下所示：
     *
     *   <ul>
     *     <li>从表字段获取表投影中每个索引的字段 ID
     *     <li>从数据字段获取每个字段的索引
     *   </ul>
     *
     * <p>然后我们可以创建表投影如下： [[0], [-1], [2]]，其中 0、-1 和 2 是字段 [1->c, 6->b, 3->a] 在数据字段中的索引。
     *     当我们从底层数据投影列时，我们需要指定字段索引和名称。
     *     很难为数据投影中的 6->b 分配适当的字段 ID 和名称，并将其添加到数据字段中，我们不能直接使用 6->b，因为 b 在底层中的字段索引为 2。
     *     我们可以删除数据投影中的 -1 字段索引，然后结果数据投影为：[[0], [2]]。
     *
     * <p>我们从底层数据投影 1->a, 3->c 后创建 {@link InternalRow}，然后创建带有索引映射的 {@link ProjectedRow}，并为表字段中的 6->b 返回 null。
     *
     * @param tableFields 表的字段
     * @param dataFields 底层数据的字段
     * @param tableProjection 表的投影
     * @return 数据的投影
     */
    public static int[][] createDataProjection(
            List<DataField> tableFields, List<DataField> dataFields, int[][] tableProjection) {

        // 获取数据字段的 ID
        List<Integer> dataFieldIdList = dataFields.stream().map(DataField::id).collect(Collectors.toList());

        return Arrays.stream(tableProjection)  // 取出表映射关系
                .map(p -> Arrays.copyOf(p, p.length))
                .peek(
                        p -> {
                            int fieldId = tableFields.get(p[0]).id();   // 从表中找到这个投影对应的索引位置
                            p[0] = dataFieldIdList.indexOf(fieldId);    // 从数据字段找到这个字段的索引位置
                        })
                .filter(p -> p[0] >= 0)
                .toArray(int[][]::new);
    }

    /**
     * 从数据字段创建谓词列表。我们将访问过滤器中的所有谓词，重置其字段索引、名称和类型，如果字段不存在则忽略。
     *
     * @param tableFields 表字段
     * @param dataFields 底层数据字段
     * @param filters 过滤器
     * @return 数据过滤器
     */
    @Nullable
    public static List<Predicate> createDataFilters(
            List<DataField> tableFields, List<DataField> dataFields, List<Predicate> filters) {
        if (filters == null) {
            return null;
        }

        Map<String, DataField> nameToTableFields =
                tableFields.stream().collect(Collectors.toMap(DataField::name, f -> f));
        LinkedHashMap<Integer, DataField> idToDataFields = new LinkedHashMap<>();
        dataFields.forEach(f -> idToDataFields.put(f.id(), f));
        List<Predicate> dataFilters = new ArrayList<>(filters.size());

        PredicateReplaceVisitor visitor =
                predicate -> {
                    DataField tableField =
                            checkNotNull(
                                    nameToTableFields.get(predicate.fieldName()),
                                    String.format("Find no field %s", predicate.fieldName()));
                    DataField dataField = idToDataFields.get(tableField.id());
                    if (dataField == null) {
                        return Optional.empty();
                    }

                    DataType dataValueType = dataField.type().copy(true);
                    DataType predicateType = predicate.type().copy(true);
                    CastExecutor<Object, Object> castExecutor =
                            dataValueType.equals(predicateType)
                                    ? null : (CastExecutor<Object, Object>) CastExecutors.resolve(predicate.type(), dataField.type());
                    // 将值从谓词类型转换为底层数据类型，可能会丢失信息，例如将 double 值转换为 int。
                    // 但这没关系，因为它只是为了谓词下推，数据在读取后将被正确过滤。
                    List<Object> literals =
                            predicate.literals().stream()
                                    .map(v -> castExecutor == null ? v : castExecutor.cast(v))
                                    .collect(Collectors.toList());
                    return Optional.of(
                            new LeafPredicate(
                                    predicate.function(),
                                    dataField.type(),
                                    indexOf(dataField, idToDataFields),
                                    dataField.name(),
                                    literals));
                };

        for (Predicate predicate : filters) {
            predicate.visit(visitor).ifPresent(dataFilters::add);
        }
        return dataFilters;
    }

    private static int indexOf(DataField dataField, LinkedHashMap<Integer, DataField> dataFields) {
        int index = 0;
        for (Map.Entry<Integer, DataField> entry : dataFields.entrySet()) {
            if (dataField.id() == entry.getKey()) {
                return index;
            }
            index++;
        }

        throw new IllegalArgumentException(
                String.format("Can't find data field %s", dataField.name()));
    }

    /**
     * 从表字段到底层数据字段创建转换器映射。例如，表和数据字段如下：
     *
     * <ul>
     *   <li>表字段：1->c INT, 6->b STRING, 3->a BIGINT
     *   <li>数据字段：1->a BIGINT, 3->c DOUBLE
     * </ul>
     *
     * <p>我们可以通过索引映射 [0, -1, 1] 从数据字段获取表字段 (1->c INT) 和 (3->a BIGINT) 的列类型 (1->a BIGINT)、(3->c DOUBLE)，然后比较数据类型并创建转换器映射。
     *
     * <p>/// TODO 当支持嵌套模式演化时，应该支持嵌套索引映射。
     *
     * @param tableFields 表的字段
     * @param dataFields 底层数据的字段
     * @param indexMapping 表字段到数据字段的索引映射
     * @return 索引映射
     */
    @Nullable
    public static CastExecutor<?, ?>[] createConvertMapping(
            List<DataField> tableFields, List<DataField> dataFields, int[] indexMapping) {
        CastExecutor<?, ?>[] converterMapping = new CastExecutor<?, ?>[tableFields.size()];
        boolean castExist = false;
        for (int i = 0; i < tableFields.size(); i++) {
            int dataIndex = indexMapping == null ? i : indexMapping[i];
            if (dataIndex < 0) {
                converterMapping[i] = CastExecutors.identityCastExecutor();
            } else {
                DataField tableField = tableFields.get(i);
                DataField dataField = dataFields.get(dataIndex);
                if (dataField.type().equalsIgnoreNullable(tableField.type())) {
                    converterMapping[i] = CastExecutors.identityCastExecutor();
                } else {
                    // TODO support column type evolution in nested type
                    checkState(
                            !tableField.type().is(DataTypeFamily.CONSTRUCTED),
                            "Only support column type evolution in atomic data type.");
                    converterMapping[i] =
                            checkNotNull(
                                    CastExecutors.resolve(dataField.type(), tableField.type()));
                    castExist = true;
                }
            }
        }

        return castExist ? converterMapping : null;
    }

    /**
     * 从表字段到底层数据字段创建 getter 和转换映射，并给出索引映射。例如，表和数据字段如下：
     *
     * <ul>
     *   <li>表字段：1->c INT, 6->b STRING, 3->a BIGINT
     *   <li>数据字段：1->a BIGINT, 3->c DOUBLE
     * </ul>
     *
     * <p>我们可以通过索引映射 [0, -1, 1] 从数据字段获取表字段 (1->c INT) 和 (3->a BIGINT) 的列类型 (1->a BIGINT)、(3->c DOUBLE)，然后比较数据类型并创建 getter 和转换映射。
     *
     * <p>/// TODO 当支持嵌套模式演化时，应该支持嵌套索引映射。
     *
     * @param tableFields 表的字段
     * @param dataFields 底层数据的字段
     * @param indexMapping 表字段到数据字段的索引映射
     * @return getter 和转换映射
     */
    private static CastFieldGetter[] createCastFieldGetterMapping(
            List<DataField> tableFields, List<DataField> dataFields, int[] indexMapping) {
        CastFieldGetter[] converterMapping = new CastFieldGetter[tableFields.size()];
        boolean castExist = false;
        for (int i = 0; i < tableFields.size(); i++) {
            int dataIndex = indexMapping == null ? i : indexMapping[i];
            if (dataIndex < 0) {
                converterMapping[i] =
                        new CastFieldGetter(row -> null, CastExecutors.identityCastExecutor());
            } else {
                DataField tableField = tableFields.get(i);
                DataField dataField = dataFields.get(dataIndex);
                if (dataField.type().equalsIgnoreNullable(tableField.type())) {
                    // 创建索引为 i 的 getter，投影行数据将转换为底层数据
                    converterMapping[i] =
                            new CastFieldGetter(
                                    InternalRowUtils.createNullCheckingFieldGetter(
                                            dataField.type(), i),
                                    CastExecutors.identityCastExecutor());
                } else {
                    // TODO support column type evolution in nested type
                    checkState(
                            !(tableField.type() instanceof MapType
                                    || dataField.type() instanceof ArrayType
                                    || dataField.type() instanceof MultisetType
                                    || dataField.type() instanceof RowType),
                            "Only support column type evolution in atomic data type.");
                    // 创建 getter，索引为 i，投影行数据将转换为底层数据
                    converterMapping[i] =
                            new CastFieldGetter(
                                    InternalRowUtils.createNullCheckingFieldGetter(
                                            dataField.type(), i),
                                    checkNotNull(
                                            CastExecutors.resolve(
                                                    dataField.type(), tableField.type())));
                    castExist = true;
                }
            }
        }

        return castExist ? converterMapping : null;
    }
}
