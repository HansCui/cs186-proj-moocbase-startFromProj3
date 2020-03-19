package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            SortOperator leftSortOp = new SortOperator(SortMergeOperator.this.getTransaction(),
                    this.getLeftTableName(), new LeftRecordComparator());
            SortOperator rightSortOp = new SortOperator(SortMergeOperator.this.getTransaction(),
                    this.getRightTableName(), new RightRecordComparator());

            this.leftIterator = SortMergeOperator.this.getRecordIterator(leftSortOp.sort());
            this.rightIterator = SortMergeOperator.this.getRecordIterator(rightSortOp.sort());

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
            this.marked = false;

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        private void advanceLeft() {
            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
        }

        private void advanceRight() {
            if (!rightIterator.hasNext() && leftRecord != null) {
                rightIterator.reset();
                advanceLeft();
                this.marked = false;
            }
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
        }

        private void mark() {
            this.rightIterator.markPrev();
            this.marked = true;
        }

        private void fetchNextRecord() {
            this.nextRecord = null;
            Comparator<Record> c = new leftRightComparator();
            while (!hasNext()) {
                if (this.leftRecord == null) {
                    throw new NoSuchElementException();
                }

                if (!this.marked) {
                    while (c.compare(this.leftRecord, this.rightRecord) < 0) {
                        advanceLeft();
                    }
                    while (c.compare(this.leftRecord, this.rightRecord) > 0) {
                        advanceRight();
                    }
                    this.mark();
                }

                if (c.compare(this.leftRecord, this.rightRecord) == 0) {
                    this.nextRecord = this.joinRecords(this.leftRecord, this.rightRecord);
                    advanceRight();
                } else {
                    rightIterator.reset();
                    advanceRight();
                    advanceLeft();
                    this.marked = false;
                }
            }
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        private class leftRightComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
