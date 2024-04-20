package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double)numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement

        //create new run to sort and return
        Run sortedRun = new Run(this.transaction,this.getSchema());
        //create a list to hold the records and be sorted
        ArrayList <Record> sortList = new ArrayList<>();

        //add records from iterator to sortList and sort
        while(records.hasNext()){
            sortList.add(records.next());
            sortList.sort(this.comparator);
        }
        //add sorted records to run
        sortedRun.addAll(sortList);
        return sortedRun;

    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement

        //create a list to hold record iterators from the list of runs
        ArrayList <BacktrackingIterator<Record>> runList = new ArrayList<>();
        //create new run to hold result of merging provided runs
        Run mergedRuns = new Run(this.transaction,this.getSchema());
        //create priority queue to hold record pairs and their indices compared with RecordPairComparator
        PriorityQueue<Pair<Record, Integer>> recordQueue = new PriorityQueue<>(new RecordPairComparator());

        //add all the run iterators provided to runList
        for (Run run : runs) {
            runList.add(run.iterator());
        }

        //add first pair from each record iterator to priority queue
        for (int i = 0; i < runList.size(); i++) {
            if (runList.get(i).hasNext()) {
                Pair<Record,Integer> firstPair = new Pair<>(runList.get(i).next(), i);
                recordQueue.add(firstPair);
            }
        }

        //add the highest priority record to runList until recordQueue is empty
        while (!recordQueue.isEmpty()){
            //extract highest priority pair from queue
            Pair <Record, Integer> highestPriority = recordQueue.poll();
            //extract record from highest priority pair
            Record priorityRecord = highestPriority.getFirst();
            //add priorityRecord to mergedRuns
            mergedRuns.add(priorityRecord);
            //extract index from highest priority pair
            int pairIndex = highestPriority.getSecond();
            //check if there is another record in that runs iterator and add to queue if so
            if(runList.get(pairIndex).hasNext()){
                Pair<Record,Integer> addPair = new Pair<>(runList.get(pairIndex).next(), pairIndex);
                recordQueue.add(addPair);
            }
        }

        return mergedRuns;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        //create a list of runs
        ArrayList<Run> runsList = new ArrayList<>();
        //create variable for number of available buffs
        int availableBuffers = numBuffers - 1;

        //merge groups of runs that are of size availableBuffers or smaller and add to runsList
        for(int i = 0; i < runs.size(); i += availableBuffers){
            //create list to hold the subgroup of runs
            ArrayList<Run> subList = new ArrayList<>(runs.subList(i, Math.min((i + availableBuffers),
                    (runs.size()))));
            //sort sublist via mergeSortedRuns() and add to runsList
            runsList.add(mergeSortedRuns(subList));
        }

        return runsList;
        /*List<Run> mergeRun = new ArrayList<>();
        for (int i = 0; numBuffers + i - 1 <= runs.size() + 1; i += numBuffers - 1) {
            List<Run> mergeAdd = runs.subList(i, Math.min(runs.size(), numBuffers + i - 1));
            mergeRun.add(mergeSortedRuns(mergeAdd));
        }
        return mergeRun;*/
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();
        // TODO(proj3_part1): implement

        //create list to keep list of runs that need to be merged
        List<Run> mergeRunList = new ArrayList<>();

        //get blocks of the relation to sort, sort into runs, and add to mergeRunList until no records remain
        while(sourceIterator.hasNext()){
            //create runs
            Run getRun = sortRun(getBlockIterator(sourceIterator,getSchema(),numBuffers));
            //add to list mergeRunList
            mergeRunList.add(getRun);
        }

        //merge runs using mergePass until all records in 1 sorted run
        while(mergeRunList.size() > 1){
            //run mergePass
            mergeRunList = mergePass(mergeRunList);

        }
        //return first/only run
        return mergeRunList.get(0); // TODO(proj3_part1): replace this!
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

