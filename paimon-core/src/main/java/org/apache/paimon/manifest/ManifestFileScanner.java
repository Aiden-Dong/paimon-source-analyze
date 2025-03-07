package org.apache.paimon.manifest;

import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                  *
 * @date : 2025/1/14                                                                              *
 * ============================================================================================== */
public interface ManifestFileScanner {

  /***
   * 基于 manifest entry 信息
   * @return
   */
  public <T extends FileEntry> Iterable<T> readManifestEntrys(List<ManifestFileMeta> manifests,
                                                          Function<ManifestFileMeta, List<T>> manifestReader,
                                                          @Nullable Filter<T> filterUnmergedEntry,
                                                          Predicate<? super ManifestFileMeta> filterManifestFileMeta,
                                                          @Nullable AtomicLong readEntries,
                                                          Integer scanManifestParallelism);
}
