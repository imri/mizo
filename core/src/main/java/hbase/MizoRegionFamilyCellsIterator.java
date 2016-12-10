package hbase;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Created by imrihecht on 12/10/16.
 */
public class MizoRegionFamilyCellsIterator implements Iterator<Cell> {

    /**
     * A customized version of CellComparator.compare() method.
     * We cannot use CellComparator itself, because it is inconsistent -
     * If it compares row, qualifier, column family - it works fine.
     * If it compares timestamps - it returns the opposite.
     * Case A: two cells - first with qualifier 'A' and timestamp 100, second with qualifier 'B' and timestamp 200,
     * then the first ('A') will be returned first and then the second ('B') (Correct)
     * Case B: two cells with qualifier 'A', one with timestamp 100 and the other with timestamp 200,
     * then the one with 200 will be returned before the one with 100. (Wrong)
     * This causes inconsistency, and the following is a fixed one.
     */
    private Comparator<Cell> ASC_CELL_COMPARATOR = (left, right) -> {
        int c = CellComparator.compareRows(left, right);
        if (c != 0) {
            return c;
        } else {
            if (left.getFamilyLength() + left.getQualifierLength() == 0 &&
                    left.getTypeByte() == KeyValue.Type.Minimum.getCode()) {
                return 1;
            } else if (right.getFamilyLength() + right.getQualifierLength() == 0 &&
                    right.getTypeByte() == KeyValue.Type.Minimum.getCode()) {
                return -1;
            } else {
                boolean sameFamilySize = left.getFamilyLength() == right.getFamilyLength();
                if (!sameFamilySize) {
                    return Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
                            right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
                } else {
                    int diff = CellComparator.compareColumns(left, right);
                    if (diff != 0) {
                        return diff;
                    } else {
                        diff = CellComparator.compareTimestamps(right, left); // Different from CellComparator.compare()
                        return diff != 0 ? diff : (255 & right.getTypeByte()) - (255 & left.getTypeByte());
                    }
                }
            }
        }
    };

    /**
     * Ascending sorted cells iterator - the most recent cell is the last
     */
    private final PeekingIterator<Cell> sortedRegionIterator;

    public MizoRegionFamilyCellsIterator(String regionEdgesFamilyPath) throws IOException {
        sortedRegionIterator = createSortedHFilesIterator(regionEdgesFamilyPath);
    }

    /**
     * Creates an ascending-sorted cells iterator, wrapped by a peeking iterator
     * @param regionEdgesFamilyPath Path of the HBase directory that contains Titan's Edges column-family
     * @return Ascending-sorted cells iterator, wrapped by a peeking iterator
     */
    protected PeekingIterator<Cell> createSortedHFilesIterator(String regionEdgesFamilyPath) throws IOException {
        return Iterators.peekingIterator(
                Iterators.mergeSorted(createHFilesIterators(regionEdgesFamilyPath), ASC_CELL_COMPARATOR)
        );
    }

    /**
     * Given a path to HFiles, gets a list of the HFiles residing in the directory,
     * create a Cells iterator per each HFile and return a collection of these iterators,
     * removing iterators that have no items
     * @param regionEdgesFamilyPath Path to HFiles
     * @return Collection of non-empty iterators of the given HFiles
     */
    protected Iterable<Iterator<Cell>> createHFilesIterators(String regionEdgesFamilyPath) throws IOException {
        Path path = new Path(regionEdgesFamilyPath);
        FileSystem fs = path.getFileSystem(new Configuration());

        return Arrays.stream(fs.listStatus(path, new FSUtils.HFileFilter(fs)))
                .map(FileStatus::getPath)
                .map(hfilePath -> MizoHFileIterator.createIterator(fs, hfilePath))
                .filter(Iterator::hasNext)
                .collect(Collectors.toList());
    }

    /**
     * Returns true if any of the HFiles in this region has a cell left to read
     */
    @Override
    public boolean hasNext() {
        return sortedRegionIterator.hasNext();
    }

    /**
     * Popping out cells until reaching the most updated version of a cell, then returns it
     * @return Most updated version of the cell
     */
    @Override
    public Cell next() {
        Cell mostUpdated = sortedRegionIterator.next();

        while (sortedRegionIterator.hasNext() && equalsRowFamilyQualifier(mostUpdated, sortedRegionIterator.peek())) {
            mostUpdated = sortedRegionIterator.next();
        }

        return mostUpdated;
    }

    /**
     * Checks if two cells are equal by comparing their Row, Family and Qualifier (NOT comparing their timestamp
     */
    private boolean equalsRowFamilyQualifier(Cell left, Cell right) {
        return CellComparator.equalsRow(left, right) &&
                CellComparator.equalsFamily(left, right) &&
                CellComparator.equalsQualifier(left, right);
    }
}
