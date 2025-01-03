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
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 数据文件的删除文件，前 4 个字节是长度，应该，以下是位图内容。
 *
 * <ul>
 *   <li>前 4 个字节是长度，应等于 {@link #length()}。</li>
 *   <li>接下来的 4 个字节是魔数，应等于 1581511376。</li>
 *   <li>其余内容应为 RoaringBitmap。</li>
 * </ul>
 */
@Public
public class DeletionFile {

    private final String path;
    private final long offset;
    private final long length;

    public DeletionFile(String path, long offset, long length) {
        this.path = path;
        this.offset = offset;
        this.length = length;
    }

    /** Path of the file. */
    public String path() {
        return path;
    }

    /** Starting offset of data in the file. */
    public long offset() {
        return offset;
    }

    /** Length of data in the file. */
    public long length() {
        return length;
    }

    public static void serialize(DataOutputView out, @Nullable DeletionFile file)
            throws IOException {
        if (file == null) {
            out.write(0);
        } else {
            out.write(1);
            out.writeUTF(file.path);
            out.writeLong(file.offset);
            out.writeLong(file.length);
        }
    }

    public static void serializeList(DataOutputView out, @Nullable List<DeletionFile> files)
            throws IOException {
        if (files == null) {
            out.write(0);
        } else {
            out.write(1);
            out.writeInt(files.size());
            for (DeletionFile file : files) {
                serialize(out, file);
            }
        }
    }

    @Nullable
    public static DeletionFile deserialize(DataInputView in) throws IOException {
        if (in.readByte() == 0) {
            return null;
        }

        String path = in.readUTF();
        long offset = in.readLong();
        long length = in.readLong();
        return new DeletionFile(path, offset, length);
    }

    @Nullable
    public static List<DeletionFile> deserializeList(DataInputView in) throws IOException {
        List<DeletionFile> files = null;
        if (in.readByte() == 1) {
            int size = in.readInt();
            files = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                files.add(DeletionFile.deserialize(in));
            }
        }
        return files;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DeletionFile)) {
            return false;
        }

        DeletionFile other = (DeletionFile) o;
        return Objects.equals(path, other.path) && offset == other.offset && length == other.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, offset, length);
    }

    @Override
    public String toString() {
        return String.format("{path = %s, offset = %d, length = %d}", path, offset, length);
    }
}
