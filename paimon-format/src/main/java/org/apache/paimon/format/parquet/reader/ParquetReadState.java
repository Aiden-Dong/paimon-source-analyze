package org.apache.paimon.format.parquet.reader;

import org.apache.parquet.column.ColumnDescriptor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;

/**
 * Helper class to store intermediate state while reading a Parquet column chunk.
 * 辅助类，用于在读取 Parquet 列块时存储中间状态。
 */
final class ParquetReadState {
  /**
   *  A special row range used when there is no row indexes (hence all rows must be included)
   *  当没有RowIndex时使用的特殊行范围（因此必须包含所有行）。
   * */
  private static final RowRange MAX_ROW_RANGE = new RowRange(Long.MIN_VALUE, Long.MAX_VALUE);

  /**
   * A special row range used when the row indexes are present AND all the row ranges have been
   * processed. This serves as a sentinel at the end indicating that all rows come after the last
   * row range should be skipped.
   * 当行索引存在且所有行范围都已处理时使用的特殊行范围。
   * 它作为一个哨兵值，表示在最后一个行范围之后的所有行应被跳过。
   */
  private static final RowRange END_ROW_RANGE = new RowRange(Long.MAX_VALUE, Long.MIN_VALUE);

  // 当前 CloumnChunk 过滤后，所有有效的数据范围
  private final Iterator<RowRange> rowRanges;

  // 指向当前在读的数据范围
  private RowRange currentRange;

  long rowId;                      // 当前要读取的 row 的索引号
  int valuesToReadInPage;          // 返回当前 page的剩余可读数据
  int rowsToReadInBatch;           // 当前批次要读取的数据量

  ParquetReadState(PrimitiveIterator.OfLong rowIndexes) {
    this.rowRanges = constructRanges(rowIndexes);
    nextRange();
  }

  /**
   * Construct a list of row ranges from the given `rowIndexes`.
   * For example, suppose the `rowIndexes` are `[0, 1, 2, 4, 5, 7, 8, 9]`,
   * it will be converted into 3 row ranges: `[0-2], [4-5], [7-9]`.
   */
  private Iterator<RowRange> constructRanges(PrimitiveIterator.OfLong rowIndexes) {
    if (rowIndexes == null) {
      return null;
    }

    List<RowRange> rowRanges = new ArrayList<>();
    long currentStart = Long.MIN_VALUE;
    long previous = Long.MIN_VALUE;

    while (rowIndexes.hasNext()) {
      long idx = rowIndexes.nextLong();
      if (currentStart == Long.MIN_VALUE) {
        currentStart = idx;
      } else if (previous + 1 != idx) {
        RowRange range = new RowRange(currentStart, previous);
        rowRanges.add(range);
        currentStart = idx;
      }
      previous = idx;
    }

    if (previous != Long.MIN_VALUE) {
      rowRanges.add(new RowRange(currentStart, previous));
    }

    return rowRanges.iterator();
  }

  /**
   * Must be called at the beginning of reading a new batch.
   */
  void resetForNewBatch(int batchSize) {
    this.rowsToReadInBatch = batchSize;
  }

  /**
   * Must be called at the beginning of reading a new page.
   */
  void resetForNewPage(int totalValuesInPage, long pageFirstRowIndex) {
    this.valuesToReadInPage = totalValuesInPage;  // 总的行数
    this.rowId = pageFirstRowIndex;
  }

  /**
   * Returns the start index of the current row range.
   */
  long currentRangeStart() {
    return currentRange.start;
  }

  /**
   * Returns the end index of the current row range.
   */
  long currentRangeEnd() {
    return currentRange.end;
  }

  /**
   * Advance to the next range.
   */
  void nextRange() {
    if (rowRanges == null) {
      currentRange = MAX_ROW_RANGE;
    } else if (!rowRanges.hasNext()) {
      currentRange = END_ROW_RANGE;
    } else {
      currentRange = rowRanges.next();
    }
  }

  boolean isFinish(){
    return currentRange.start == END_ROW_RANGE.start && currentRange.end == END_ROW_RANGE.end;
  }

  /**
   * Helper struct to represent a range of row indexes `[start, end]`.
   */
  private static class RowRange {
    final long start;
    final long end;

    RowRange(long start, long end) {
      this.start = start;
      this.end = end;
    }
  }
}
