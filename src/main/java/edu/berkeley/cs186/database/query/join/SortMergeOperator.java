package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.MaterializeOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             TransactionContext transaction) {
        super(prepareLeft(transaction, leftSource, leftColumnName),
              prepareRight(transaction, rightSource, rightColumnName),
              leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);
        this.stats = this.estimateStats();
    }

    /**
     * If the left source is already sorted on the target column then this
     * returns the leftSource, otherwise it wraps the left source in a sort
     * operator.
     */
    private static QueryOperator prepareLeft(TransactionContext transaction,
                                             QueryOperator leftSource,
                                             String leftColumn) {
        leftColumn = leftSource.getSchema().matchFieldName(leftColumn);
        if (leftSource.sortedBy().contains(leftColumn)) return leftSource;
        return new SortOperator(transaction, leftSource, leftColumn);
    }

    /**
     * If the right source isn't sorted, wraps the right source in a sort
     * operator. Otherwise, if it isn't materialized, wraps the right source in
     * a materialize operator. Otherwise, simply returns the right source. Note
     * that the right source must be materialized since we may need to backtrack
     * over it, unlike the left source.
     */
    private static QueryOperator prepareRight(TransactionContext transaction,
                                              QueryOperator rightSource,
                                              String rightColumn) {
        rightColumn = rightSource.getSchema().matchFieldName(rightColumn);
        if (!rightSource.sortedBy().contains(rightColumn)) {
            return new SortOperator(transaction, rightSource, rightColumn);
        } else if (!rightSource.materialized()) {
            return new MaterializeOperator(rightSource, transaction);
        }
        return rightSource;
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public List<String> sortedBy() {
        return Arrays.asList(getLeftColumnName(), getRightColumnName());
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
    private class SortMergeIterator implements Iterator<Record> {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private Iterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            leftIterator = getLeftSource().iterator();
            rightIterator = getRightSource().backtrackingIterator();
            rightIterator.markNext();

            if (leftIterator.hasNext() && rightIterator.hasNext()) {
                leftRecord = leftIterator.next();
                rightRecord = rightIterator.next();
            }

            this.marked = false;
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            //check if LeftRecord is null and if so, return null
            if(this.leftRecord == null){
                return null;
            }
            //see if a record has been marked, if not mark and advance left and/or right records
            if(!marked){
                this.marked = true;
                //advance rightRecord until it finds record >= leftRecord
                while(compare(this.leftRecord,this.rightRecord) > 0){
                    if(this.rightIterator.hasNext()) {
                        this.rightRecord = this.rightIterator.next();
                    }
                }
                //advance leftRecord until it finds record <= rightRecord
                while(compare(this.leftRecord,this.rightRecord) < 0){
                    if(this.leftIterator.hasNext()) {
                        this.leftRecord = this.leftIterator.next();
                    }
                }
                this.rightIterator.markPrev();
            }
            //
            if(compare(this.leftRecord,this.rightRecord) == 0) {
                //join left and right record in a new record to return
                Record returnRecord = this.leftRecord.concat(this.rightRecord);
                //check if right iterator has next record
                if (!this.rightIterator.hasNext()) {
                    //check if leftIterator has next record
                    if(this.leftIterator.hasNext()){
                        //set leftRecord to next record
                        this.leftRecord = this.leftIterator.next();
                    } else {
                        //set leftRecord to null
                        this.leftRecord = null;
                    }
                    //no next right iterator so reset and move to next record
                    this.rightIterator.reset();
                    this.rightRecord = this.rightIterator.next();
                //right iterator has next so set rightRecord to next
                } else {
                    this.rightRecord = this.rightIterator.next();
                }
                //return joined record
                return returnRecord;
            //records not equal
            } else {
                //unmark
                this.marked = false;
                //check if leftIterator has next record
                if(this.leftIterator.hasNext()){
                    //set leftRecord to next record
                    this.leftRecord = this.leftIterator.next();
                } else {
                    //set leftRecord to null
                    this.leftRecord = null;
                }
                //reset rightIterator and move to next record
                this.rightIterator.reset();
                this.rightRecord = this.rightIterator.next();
                //make recursive call to try again
                return fetchNextRecord();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
