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

package org.apache.paimon.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * 工厂类，用于创建特定文件格式的读取器和写入器工厂。
 * <p>注意：此类必须是线程安全的。
 */
public abstract class FileFormat {

    protected String formatIdentifier;

    protected FileFormat(String formatIdentifier) {
        this.formatIdentifier = formatIdentifier;
    }

    public String getFormatIdentifier() {
        return formatIdentifier;
    }

    /**
     * 从类型创建 {@link FormatReaderFactory}，并下推投影。

     * @param projectedRowType 带有投影的类型
     * @param filters 用于谓词下推的过滤结果
     */
    public abstract FormatReaderFactory createReaderFactory(RowType projectedRowType, @Nullable List<Predicate> filters);

    /** 从类型创建 {@link FormatWriterFactory}。 */
    public abstract FormatWriterFactory createWriterFactory(RowType type);

    // 验证数据字段类型是否支持。
    public abstract void validateDataFields(RowType rowType);

    public FormatReaderFactory createReaderFactory(RowType rowType) {
        return createReaderFactory(rowType, new ArrayList<>());
    }

    public Optional<TableStatsExtractor> createStatsExtractor(
            RowType type, FieldStatsCollector.Factory[] statsCollectors) {
        return Optional.empty();
    }

    @VisibleForTesting
    public static FileFormat fromIdentifier(String identifier, Options options) {
        return fromIdentifier(identifier, new FormatContext(options, 1024));
    }

    /** 从格式标识符和格式选项创建 {@link FileFormat}。 */
    public static FileFormat fromIdentifier(String identifier, FormatContext context) {
        return fromIdentifier(identifier, context, FileFormat.class.getClassLoader())
                .orElseThrow(
                        () ->
                                new RuntimeException(
                                        String.format(
                                                "Could not find a FileFormatFactory implementation class for %s format",
                                                identifier)));
    }

    private static Optional<FileFormat> fromIdentifier(
            String formatIdentifier, FormatContext context, ClassLoader classLoader) {
        ServiceLoader<FileFormatFactory> serviceLoader = ServiceLoader.load(FileFormatFactory.class, classLoader);
        for (FileFormatFactory factory : serviceLoader) {
            if (factory.identifier().equals(formatIdentifier.toLowerCase())) {
                return Optional.of(factory.create(context));
            }
        }

        return Optional.empty();
    }

    // 从文件后缀中判断文件格式
    public static FileFormat getFileFormat(Options options, String formatIdentifier) {
        int readBatchSize = options.get(CoreOptions.READ_BATCH_SIZE);
        return FileFormat.fromIdentifier(
                formatIdentifier,
                new FormatContext(options.removePrefix(formatIdentifier + "."), readBatchSize));
    }
}
