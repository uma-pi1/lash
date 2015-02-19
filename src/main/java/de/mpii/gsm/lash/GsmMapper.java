package de.mpii.gsm.lash;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import de.mpii.gsm.lash.encoders.SimpleGapEncoder;
import de.mpii.gsm.lash.encoders.SplitGapEncoder;
import de.mpii.gsm.taxonomy.NytTaxonomy;
import de.mpii.gsm.taxonomy.Taxonomy;
import de.mpii.gsm.utils.PrimitiveUtils;
import de.mpii.gsm.utils.IntArrayWritable;

public class GsmMapper extends Mapper<LongWritable, IntArrayWritable, BytesWritable, IntWritable> {

	/** an index mapping items to partitions */
	Map<Integer, Integer> itemToPartition;

	/** an index mapping partitions to its pivot range */
	Map<Integer, Long> partitionToItems;

	int[] itemToParent;

	/**
	 * a set containing the partitions for which the current transaction has
	 * already been emitted --used to avoid double emissions
	 */
	HashSet<Integer> partitionsOfTransaction = new HashSet<Integer>();

	/**
	 * Mapping of partition ids to their item id range ({@see
	 * PrimitiveUtils#combine(int, int)} ).
	 */
	long[] partitionToPivotRange;

	/** counter value used for each emitted sequence */
	IntWritable supportWrapper = new IntWritable(1);

	/**
	 * encoder of an output transaction used when we do not split transactions
	 * (see ALLOW_SPLITS)
	 */
	SimpleGapEncoder simpleEncoder;

	/**
	 * encoder of an output transaction used when we split transactions (see
	 * ALLOW_SPLITS)
	 */
	SplitGapEncoder splitEncoder;

	// Taxonomy
	Taxonomy taxonomy;

	/**
	 * if positive, only the respective partition is created -- for debugging
	 * reasons
	 */
	int debugPartId;

	int gamma;

	int lambda;

	int neighborhoodSize;

	boolean COMPRESS_GAPS;

	boolean ALLOW_SPLITS;

	boolean REMOVE_UNREACHABLE;

	@SuppressWarnings("unchecked")
	protected void setup(Context context) throws IOException, InterruptedException {

		ALLOW_SPLITS = context.getConfiguration().getBoolean("de.mpii.gsm.partitioning.allowSplits", false);

		REMOVE_UNREACHABLE = context.getConfiguration().getBoolean("de.mpii.gsm.partitioning.removeUnreachable", true);

		COMPRESS_GAPS = context.getConfiguration().getBoolean("de.mpii.fsm.partitioning.compressGaps", true);

		// read dictionary, map from partitions to item identifiers, and
		// reverse map from distributed cache
		try {
			ObjectInputStream is = new ObjectInputStream(new FileInputStream("itemToPartition"));
			itemToPartition = (Map<Integer, Integer>) is.readObject();
			is.close();

			is = new ObjectInputStream(new FileInputStream("partitionToItems"));
			partitionToItems = (HashMap<Integer, Long>) is.readObject();
			is.close();

			is = new ObjectInputStream(new FileInputStream("itemToParent"));
			itemToParent = (int[]) is.readObject();
			is.close();

		} catch (IOException e) {
			GsmJob.LOGGER.severe("Reducer: error reading from dCache: " + e);
		} catch (ClassNotFoundException e) {
			GsmJob.LOGGER.severe("Mapper error during deserialization: " + e);
		}

		// initialize encoders and other parameters

		taxonomy = new NytTaxonomy(itemToParent);

		// System.out.println("parent size=" + itemToParent.length);
		int totalPartitions = context.getConfiguration().getInt("de.mpii.gsm.partitioning.totalPartitions", 0);
		gamma = context.getConfiguration().getInt("de.mpii.gsm.partitioning.gamma", -1);
		lambda = context.getConfiguration().getInt("de.mpii.gsm.partitioning.lambda", -1);

		debugPartId = context.getConfiguration().getInt("de.mpii.gsm.partitioning.debugPartId", -1);

		simpleEncoder = new SimpleGapEncoder(gamma, lambda, new BytesWritable(), COMPRESS_GAPS, REMOVE_UNREACHABLE);
		splitEncoder = new SplitGapEncoder(gamma, lambda, new ArrayList<BytesWritable>(), COMPRESS_GAPS, REMOVE_UNREACHABLE);
		simpleEncoder.setTaxonomy(taxonomy);
		splitEncoder.setTaxonomy(taxonomy);

		partitionToPivotRange = new long[totalPartitions + 1];

	}

	protected void map(LongWritable key, IntArrayWritable value, Context context) throws IOException,
			InterruptedException {

		// clear partition set for this transaction
		partitionsOfTransaction.clear();

		// for each transaction item, retrieve its partition and encode it
		// using this partition
		// emit the encoded sequence as key and count 1 as value
		int[] transaction = value.getContents();

		// the pivot range [beginItem, endItem] of the partition, packed in
		// a long value
		int beginItem;
		int endItem;
		long pivotRange;

		// iterate through transaction perform encoding at the same time
		for (int offset = 0; offset < transaction.length; offset++) {

			int item = transaction[offset];

			do {

				// System.out.println("processing item = " + item);

				// retrieve the partition of the item, infrequent and stop words
				// have no partition (null)
				if (itemToPartition.get(item) == null) {
					if (taxonomy.hasParent(item)) {
						item = taxonomy.getParent(item);
						continue;
					} else
						break;
				}
				int partitionId = itemToPartition.get(item);

				// TODO: handle hierarchies here ..
				// only output this specific partition -- for debugging reasons
				if (debugPartId > 0 && partitionId != debugPartId) {
					continue;
				}

				// whether this partition has been processed before for this
				// transaction
				boolean isFirstOccurrence = partitionsOfTransaction.add(partitionId);

				// if we have processed this partition before and unless we need
				// to build a partition index
				// we continue with the next item
				if (!isFirstOccurrence) {
					// continue;

					if (taxonomy.hasParent(item)) {
						item = taxonomy.getParent(item);
						continue;
					} else
						break;

					// break;
				}

				// retrieve the pivot range [beginItem, endItem] of this
				// partition
				pivotRange = partitionToItems.get(partitionId);
				beginItem = PrimitiveUtils.getLeft(pivotRange);
				endItem = PrimitiveUtils.getRight(pivotRange);

				// partition)
				if (REMOVE_UNREACHABLE) {
					encode(transaction, partitionId, 0, transaction.length - 1, beginItem, endItem, false, true);
				} else {
					encode(transaction, partitionId, offset, transaction.length - 1, beginItem, endItem, false, true);
				}
				emit(partitionId, context);

				if (taxonomy.hasParent(item)) {
					item = taxonomy.getParent(item);
					// System.out.println("item Parent = " + item);
				} else {
					break;
				}

			} while (true);
		} // end for each transaction item
	}

	/**
	 * Run encoding for (a part of) the transaction {@link BaseGapEncoder}. After
	 * encoding, call
	 * {@link #emit(int, org.apache.hadoop.mapreduce.Mapper.Context)} to output
	 * the result.
	 * 
	 * @param transaction
	 *          the transaction to be encoded
	 * @param partitionId
	 *          the id of the partition for which transaction is encoded
	 * @param minOffset
	 *          the minimum offset of a pivot -- default value (0)
	 * @param maxOffset
	 *          the maximum offset of a pivot -- default value (transaction.length
	 *          - 1)
	 * @param beginItem
	 *          items in [beginItem,endItem] are pivot items
	 * @param endItem
	 *          items > endItem are irrelevant (i.e., will be treated as gaps)
	 * @param append
	 *          append to the current encoded sequence
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void encode(int[] transaction, int partitionId, int minOffset, int maxOffset, int beginItem, int endItem,
			boolean append, boolean finalize) throws IOException {
		// encode the transaction and emit if encoding is non-empty
		if (ALLOW_SPLITS) {

			// encode with splitting (output is a sequence of byte arrays)
			if (!append) {
				splitEncoder.clear();
				splitEncoder.setPartitionId(partitionId);
				splitEncoder.setSequenceLength(transaction.length);
			}
			splitEncoder.encode(transaction, minOffset, maxOffset, beginItem, endItem, append, finalize);
		} else {

			// encode without splitting (output is a byte array)
			if (!append) {
				simpleEncoder.clear();
				simpleEncoder.setPartitionId(partitionId);
			}
			simpleEncoder.encode(transaction, minOffset, maxOffset, beginItem, endItem, append, finalize);
		}
	}

	/**
	 * Emit the current encoded sequence (if non-empty).
	 * 
	 * @param partitionId
	 *          the id of the partition for which transaction is encoded
	 * @param context
	 *          for emitting the result encoded transactions
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void emit(int partitionId, Context context) throws IOException, InterruptedException {
		// compute encoded length of partitionId to check if encoded
		// transaction is non-empty
		int partitionIdVIntSize = WritableUtils.getVIntSize(partitionId);

		if (ALLOW_SPLITS) {

			ArrayList<BytesWritable> targets = splitEncoder.targets();
			for (int i = 0; i < splitEncoder.getTargetsLength(); i++) {
				BytesWritable target = targets.get(i);
				if (target == null) {
					break;
				}
				if (target.getLength() > partitionIdVIntSize) {
					// i.e., there is at least one item
					context.write(target, supportWrapper);
				} else {
					break;
				}
			}
		} else {

			BytesWritable target = simpleEncoder.target();
			if (target.getLength() > partitionIdVIntSize) {
				context.write(target, supportWrapper);
				// int[] decodedSequence = decode(target); // FOR DEBUGGING
			}

		}
	}
}
