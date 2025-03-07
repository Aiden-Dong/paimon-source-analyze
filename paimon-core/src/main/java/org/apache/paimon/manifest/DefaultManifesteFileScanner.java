package org.apache.paimon.manifest;

import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ScanParallelExecutor;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                    *
 * @date : 2025/1/15                                                                              *
 * ============================================================================================== */
public class DefaultManifesteFileScanner implements ManifestFileScanner{
  @Override
  public <T extends FileEntry> Iterable<T> readManifestEntrys(List<ManifestFileMeta> manifests,
                                                              Function<ManifestFileMeta, List<T>> manifestReader,
                                                              @Nullable Filter<T> filterUnmergedEntry,
                                                              Predicate<? super ManifestFileMeta> filterManifestFileMeta,
                                                              @Nullable AtomicLong readEntries,
                                                              Integer scanManifestParallelism) {
    Iterable<T> entries =
            ScanParallelExecutor.parallelismBatchIterable(
                    files -> {
                      Stream<T> stream =
                              files.parallelStream()
                                      .filter(filterManifestFileMeta)
                                      .flatMap(m -> manifestReader.apply(m).stream());
                      if (filterUnmergedEntry != null) {
                        stream = stream.filter(filterUnmergedEntry::test);
                      }
                      List<T> entryList = stream.collect(Collectors.toList());
                      if (readEntries != null) {
                        readEntries.getAndAdd(entryList.size());
                      }
                      return entryList;
                    },
                    manifests,
                    scanManifestParallelism);
    return entries;
  }
}
