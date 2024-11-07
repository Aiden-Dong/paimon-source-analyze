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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.types.DataTypeRoot.ROW;

/**
 * {@link Projection} 表示一个（可能是嵌套的）索引列表，可以用来投影数据类型。
 *                    行投影包括减少可访问字段并重新排序它们。
 */
public abstract class Projection {

    // sealed class
    private Projection() {}

    public abstract RowType project(RowType rowType);

    /** Project array. */
    public <T> T[] project(T[] array) {
        int[] project = toTopLevelIndexes();
        @SuppressWarnings("unchecked")
        T[] ret = (T[]) Array.newInstance(array.getClass().getComponentType(), project.length);
        for (int i = 0; i < project.length; i++) {
            ret[i] = array[project[i]];
        }
        return ret;
    }

    /** Project list. */
    public <T> List<T> project(List<T> list) {
        int[] project = toTopLevelIndexes();
        List<T> ret = new ArrayList<>();
        for (int i : project) {
            ret.add(list.get(i));
        }
        return ret;
    }

    /**
     * @return {@code true} 如果这个投影是嵌套的，否则返回 {@code false}。
     */
    public abstract boolean isNested();

    /**
     * 执行此 {@link Projection} 与另一个 {@link Projection} 的差集操作。该操作的结果是一个新的 {@link Projection}，
     * 保留当前实例的相同顺序，但移除 {@code other} 中的索引。例如：
     *
     * <pre>
     *   <code>[4, 1, 0, 3, 2] - [4, 2] = [1, 0, 3]</code>
     * </pre>
     *
     * <p>注意被减数中的索引 {@code 3} 会变为 {@code 2}，因为它重新缩放以正确地投影一个具有 3 个元素的 {@link InternalRow}。
     *
     * @param other 被减数
     * @throws IllegalArgumentException 如果 {@code other} 是嵌套的，则抛出异常。
     */
    public abstract Projection difference(Projection other);

    /**
     * Complement this projection. The returned projection is an ordered projection of fields from 0
     * to {@code fieldsNumber} except the indexes in this {@link Projection}. For example:
     *
     * <pre>
     * <code>
     * [4, 2].complement(5) = [0, 1, 3]
     * </code>
     * </pre>
     *
     * @param fieldsNumber the size of the universe
     * @throws IllegalStateException if this projection is nested.
     */
    public abstract Projection complement(int fieldsNumber);

    /**
     * 将此实例转换为顶层索引的投影。该数组表示原始 {@link DataType} 字段的映射。
     * 例如，{@code [0, 2, 1]} 指定按以下顺序
     * 包含第 1 个字段、第 3 个字段和第 2 个字段的行。
     *
     * @throws IllegalStateException 如果此投影是嵌套的，则抛出异常。
     */
    public abstract int[] toTopLevelIndexes();

    /**
     * Convert this instance to a nested projection index paths. The array represents the mapping of
     * the fields of the original {@link DataType}, including nested rows. For example, {@code [[0,
     * 2, 1], ...]} specifies to include the 2nd field of the 3rd field of the 1st field in the
     * top-level row.
     */
    public abstract int[][] toNestedIndexes();

    /**
     * Create an empty {@link Projection}, that is a projection that projects no fields, returning
     * an empty {@link DataType}.
     */
    public static Projection empty() {
        return EmptyProjection.INSTANCE;
    }

    /**
     * Create a {@link Projection} of the provided {@code indexes}.
     *
     * @see #toTopLevelIndexes()
     */
    public static Projection of(int[] indexes) {
        if (indexes.length == 0) {
            return empty();
        }
        return new TopLevelProjection(indexes);
    }

    /**
     * Create a {@link Projection} of the provided {@code indexes}.
     *
     * @see #toNestedIndexes()
     */
    public static Projection of(int[][] indexes) {
        if (indexes.length == 0) {
            return empty();
        }
        return new NestedProjection(indexes);
    }

    /** Create a {@link Projection} of a field range. */
    public static Projection range(int startInclusive, int endExclusive) {
        return new TopLevelProjection(IntStream.range(startInclusive, endExclusive).toArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Projection)) {
            return false;
        }
        Projection other = (Projection) o;
        if (!this.isNested() && !other.isNested()) {
            return Arrays.equals(this.toTopLevelIndexes(), other.toTopLevelIndexes());
        }
        return Arrays.deepEquals(this.toNestedIndexes(), other.toNestedIndexes());
    }

    @Override
    public int hashCode() {
        if (isNested()) {
            return Arrays.deepHashCode(toNestedIndexes());
        }
        return Arrays.hashCode(toTopLevelIndexes());
    }

    @Override
    public String toString() {
        if (isNested()) {
            return "Nested projection = " + Arrays.deepToString(toNestedIndexes());
        }
        return "Top level projection = " + Arrays.toString(toTopLevelIndexes());
    }

    private static class EmptyProjection extends Projection {

        static final EmptyProjection INSTANCE = new EmptyProjection();

        private EmptyProjection() {}

        @Override
        public RowType project(RowType rowType) {
            return new NestedProjection(toNestedIndexes()).project(rowType);
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public Projection difference(Projection projection) {
            return this;
        }

        @Override
        public Projection complement(int fieldsNumber) {
            return new TopLevelProjection(IntStream.range(0, fieldsNumber).toArray());
        }

        @Override
        public int[] toTopLevelIndexes() {
            return new int[0];
        }

        @Override
        public int[][] toNestedIndexes() {
            return new int[0][];
        }
    }

    private static class NestedProjection extends Projection {

        final int[][] projection;
        final boolean nested;

        NestedProjection(int[][] projection) {
            this.projection = projection;
            this.nested = Arrays.stream(projection).anyMatch(arr -> arr.length > 1);
        }

        @Override
        public RowType project(RowType rowType) {
            final List<DataField> updatedFields = new ArrayList<>();
            Set<String> nameDomain = new HashSet<>();
            int duplicateCount = 0;
            for (int[] indexPath : this.projection) {
                DataField field = rowType.getFields().get(indexPath[0]);
                StringBuilder builder =
                        new StringBuilder(rowType.getFieldNames().get(indexPath[0]));
                for (int index = 1; index < indexPath.length; index++) {
                    Preconditions.checkArgument(
                            field.type().getTypeRoot() == ROW, "Row data type expected.");
                    RowType rowtype = ((RowType) field.type());
                    builder.append("_").append(rowtype.getFieldNames().get(indexPath[index]));
                    field = rowtype.getFields().get(indexPath[index]);
                }
                String path = builder.toString();
                while (nameDomain.contains(path)) {
                    path = builder.append("_$").append(duplicateCount++).toString();
                }
                updatedFields.add(field.newName(path));
                nameDomain.add(path);
            }
            return new RowType(rowType.isNullable(), updatedFields);
        }

        @Override
        public boolean isNested() {
            return nested;
        }

        @Override
        public Projection difference(Projection other) {
            if (other.isNested()) {
                throw new IllegalArgumentException(
                        "Cannot perform difference between nested projection and nested projection");
            }
            if (other instanceof EmptyProjection) {
                return this;
            }
            if (!this.isNested()) {
                return new TopLevelProjection(toTopLevelIndexes()).difference(other);
            }

            // Extract the indexes to exclude and sort them
            int[] indexesToExclude = other.toTopLevelIndexes();
            indexesToExclude = Arrays.copyOf(indexesToExclude, indexesToExclude.length);
            Arrays.sort(indexesToExclude);

            List<int[]> resultProjection =
                    Arrays.stream(projection).collect(Collectors.toCollection(ArrayList::new));

            ListIterator<int[]> resultProjectionIterator = resultProjection.listIterator();
            while (resultProjectionIterator.hasNext()) {
                int[] indexArr = resultProjectionIterator.next();

                // Let's check if the index is inside the indexesToExclude array
                int searchResult = Arrays.binarySearch(indexesToExclude, indexArr[0]);
                if (searchResult >= 0) {
                    // Found, we need to remove it
                    resultProjectionIterator.remove();
                } else {
                    // Not found, let's compute the offset.
                    // Offset is the index where the projection index should be inserted in the
                    // indexesToExclude array
                    int offset = (-(searchResult) - 1);
                    if (offset != 0) {
                        indexArr[0] = indexArr[0] - offset;
                    }
                }
            }

            return new NestedProjection(resultProjection.toArray(new int[0][]));
        }

        @Override
        public Projection complement(int fieldsNumber) {
            if (isNested()) {
                throw new IllegalStateException("Cannot perform complement of a nested projection");
            }
            return new TopLevelProjection(toTopLevelIndexes()).complement(fieldsNumber);
        }

        @Override
        public int[] toTopLevelIndexes() {
            if (isNested()) {
                throw new IllegalStateException(
                        "Cannot convert a nested projection to a top level projection");
            }
            return Arrays.stream(projection).mapToInt(arr -> arr[0]).toArray();
        }

        @Override
        public int[][] toNestedIndexes() {
            return projection;
        }
    }

    private static class TopLevelProjection extends Projection {

        final int[] projection;

        TopLevelProjection(int[] projection) {
            this.projection = projection;
        }

        @Override
        public RowType project(RowType rowType) {
            return new NestedProjection(toNestedIndexes()).project(rowType);
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public Projection difference(Projection other) {
            if (other.isNested()) {
                throw new IllegalArgumentException(
                        "Cannot perform difference between top level projection and nested projection");
            }
            if (other instanceof EmptyProjection) {
                return this;
            }

            // Extract the indexes to exclude and sort them
            int[] indexesToExclude = other.toTopLevelIndexes();
            indexesToExclude = Arrays.copyOf(indexesToExclude, indexesToExclude.length);
            Arrays.sort(indexesToExclude);

            List<Integer> resultProjection =
                    Arrays.stream(projection)
                            .boxed()
                            .collect(Collectors.toCollection(ArrayList::new));

            ListIterator<Integer> resultProjectionIterator = resultProjection.listIterator();
            while (resultProjectionIterator.hasNext()) {
                int index = resultProjectionIterator.next();

                // Let's check if the index is inside the indexesToExclude array
                int searchResult = Arrays.binarySearch(indexesToExclude, index);
                if (searchResult >= 0) {
                    // Found, we need to remove it
                    resultProjectionIterator.remove();
                } else {
                    // Not found, let's compute the offset.
                    // Offset is the index where the projection index should be inserted in the
                    // indexesToExclude array
                    int offset = (-(searchResult) - 1);
                    if (offset != 0) {
                        resultProjectionIterator.set(index - offset);
                    }
                }
            }

            return new TopLevelProjection(resultProjection.stream().mapToInt(i -> i).toArray());
        }

        @Override
        public Projection complement(int fieldsNumber) {
            int[] indexesToExclude = Arrays.copyOf(projection, projection.length);
            Arrays.sort(indexesToExclude);

            return new TopLevelProjection(
                    IntStream.range(0, fieldsNumber)
                            .filter(i -> Arrays.binarySearch(indexesToExclude, i) < 0)
                            .toArray());
        }

        @Override
        public int[] toTopLevelIndexes() {
            return projection;
        }

        @Override
        public int[][] toNestedIndexes() {
            return Arrays.stream(projection).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
        }
    }
}
