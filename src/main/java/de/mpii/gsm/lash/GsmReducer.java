package de.mpii.gsm.lash;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;







import de.mpii.gsm.localmining.PSMwithIndex;
import de.mpii.gsm.taxonomy.*;
import de.mpii.gsm.writer.DistributedGsmWriter;
import de.mpii.gsm.utils.PrimitiveUtils;
import de.mpii.gsm.utils.IntArrayWritable;


public class GsmReducer extends Reducer<BytesWritable, IntWritable, IntArrayWritable, LongWritable> {
	// an index mapping partitions to their set of items
    Map<Integer, Long> partitionToItems;

    // previously processed key
    BytesWritable prevKey = new BytesWritable();

    // decoded sequence of the previously processed key
    int[] prevSequence = new int[10];

    // size of the decoded sequence of the previously processed key
    int prevSequenceSize = 0;

    // partition ID of the previously processed sequence and its support
    int prevPartitionId = -1;

    int prevSupport = 0;

    // instance of the GSM algorithm initialized with dummy values
	  PSMwithIndex gsm = new PSMwithIndex();

    // a writer object for the mining result sequences
    DistributedGsmWriter writer = new DistributedGsmWriter();

    /** Retrieve cache files from distributed cache */
    @SuppressWarnings("unchecked")
    protected void setup(Context context) throws IOException {
        int sigma = context.getConfiguration().getInt("de.mpii.gsm.partitioning.sigma", -1);
        int gamma = context.getConfiguration().getInt("de.mpii.gsm.partitioning.gamma", -1);
        int lambda = context.getConfiguration().getInt("de.mpii.gsm.partitioning.lambda", -1);
        int[] itemToParent = null;
        gsm.clear();
        
        prevPartitionId = -1;
        prevKey.setSize(0);

        // read map from partitions to item identifiers from distributed
        // cache
        try {
            ObjectInputStream is = new ObjectInputStream(new FileInputStream("partitionToItems"));
            partitionToItems = (HashMap<Integer, Long>) is.readObject();
            is.close();
            
            is = new ObjectInputStream(new FileInputStream("itemToParent"));
            itemToParent = (int[]) is.readObject();
			is.close();
            
            
        } catch (IOException e) {
            GsmJob.LOGGER.severe("Reducer: error reading from dCache: " + e);
        } catch (ClassNotFoundException e) {
            GsmJob.LOGGER.severe("Reducer: deserialization exception: " + e);
        }
        
        gsm.setParameters(sigma, gamma, lambda, new NytTaxonomy(itemToParent));
        
    }

    /**
     * Key is the partition id followed by sequence items and value is the weight of the
     * sequence
     */
    protected void reduce(BytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // if it is the same sequence as before, simply update the support
        // and return
        if (key.equals(prevKey)) {
            for (IntWritable value : values) {
                prevSupport += value.get();
            }
            return;
        }
        // when this line is reached, we found a new sequence

        // we have the support of the previous key; add it to the BFS input
        if (prevPartitionId != -1) {
            gsm.addTransaction(prevSequence, 0, prevSequenceSize, prevSupport);
        }

        // initialize new sequence
        prevKey.set(key);
        prevSupport = 0;
        for (IntWritable value : values) {
            prevSupport += value.get();
        }

        // decode the new sequence
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(key.getBytes(), 0, key.getLength()));
        int partitionId = WritableUtils.readVInt(in); // first byte is partition id
        prevSequenceSize = 0;
        while (in.available() > 0) {

            // resize prevSequence if too short
            if (prevSequence.length == prevSequenceSize) {
                int[] temp = prevSequence;
                prevSequence = new int[2 * prevSequence.length];
                System.arraycopy(temp, 0, prevSequence, 0, temp.length);
            }

            // read item / gap
            prevSequence[prevSequenceSize] = WritableUtils.readVInt(in);
            prevSequenceSize++;
        }
        in.close();

        // if it is also a new partition, run BFS on old partition and
        // reinitialize
        if (partitionId != prevPartitionId) {
            if (prevPartitionId != -1) {
                writer.setContext(context);
                gsm.mine(writer);
            }

            // determine the pivot range [beginItem, endItem] of the
            // partition and initialize
            // BFS
            long pivotRange = partitionToItems.get(partitionId);
            int beginItem = PrimitiveUtils.getLeft(pivotRange);
            int endItem = PrimitiveUtils.getRight(pivotRange);
            gsm.initialize(beginItem, endItem);
        }
        prevPartitionId = partitionId;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // run BFS on last partition
        if (prevPartitionId != -1) {
            gsm.addTransaction(prevSequence, 0, prevSequenceSize, prevSupport);
            writer.setContext(context);
            gsm.mine(writer);
        }
    }
}
