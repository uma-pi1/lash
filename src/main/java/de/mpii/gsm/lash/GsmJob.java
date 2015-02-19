package de.mpii.gsm.lash;

import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import de.mpii.gsm.driver.GsmConfig;
import de.mpii.gsm.utils.Dictionary;
import de.mpii.gsm.utils.IntArrayWritable;
import de.mpii.gsm.utils.PrimitiveUtils;

public class GsmJob {

	static final Logger LOGGER = Logger.getLogger(GsmJob.class.getSimpleName());

	public static void runGsmJob(GsmConfig config) throws Exception {

		LOGGER.setLevel(Level.ALL);

		String inputPath = null;
		String dictionaryPath = null;

		String outputPath = config.getEncodedOutputPath();

		if (config.isResume()) {
			inputPath = config.getResumePath() + "/raw/";
			dictionaryPath = config.getResumePath() + "/wc/part-r-00000";
		} else {
			inputPath = config.getEncodedInputPath() + "/raw/";
			dictionaryPath = config.getEncodedInputPath() + "/wc/part-r-00000";
		}

		int sigma = config.getSigma();

		int gamma = config.getGamma();

		int lambda = config.getLambda();

		int partitionSize = 1; // TODO: remove, not used in PSM

		int numberOfReducers = config.getNumReducers();

		int debugPartId = 0; // TODO: not supported

		/*
		 * provide as optional agrument long size= 67108864L; if(args.length > 10){
		 * size = Long.parseLong(args[10]); }
		 */

		// Default options from MG-FSM
		boolean useAggregation = true;
		boolean allowSplits = false;
		boolean removeUnreachable = true;
		boolean compressGaps = true;

		// Job
		Job job = new Job();
		job.setJobName("GSM-" + sigma + "-" + gamma + "-" + lambda);
		job.setJarByClass(GsmJob.class);

		// Job configurations
		// set timeout to 60 minutes
		job.getConfiguration().setInt("mapreduce.task.timeout", 3600000);

		// whether multiple instances of some map/reduce tasks may be executed in
		// parallel.
		job.getConfiguration().setBoolean("mapreduce.map.speculative", false);
		job.getConfiguration().setBoolean("mapreduce.reduce.speculative", false);
		job.getConfiguration().set("dictionary", dictionaryPath);

		job.getConfiguration().setInt("de.mpii.gsm.partitioning.sigma", sigma);
		job.getConfiguration().setInt("de.mpii.gsm.partitioning.gamma", gamma);
		job.getConfiguration().setInt("de.mpii.gsm.partitioning.lambda", lambda);

		job.getConfiguration().setBoolean("de.mpii.gsm.partitioning.allowSplits", allowSplits);
		job.getConfiguration().setBoolean("de.mpii.gsm.partitioning.removeUnreachable", removeUnreachable);
		job.getConfiguration().setBoolean("de.mpii.gsm.partitioning.compressGaps", compressGaps);

		// Index mapping partitions to its range(minId, maxId) packed as long
		Map<Integer, Long> partitionToItems = new HashMap<Integer, Long>();

		// Index mapping items to partition
		Map<Integer, Integer> itemToPartition = new HashMap<Integer, Integer>();

		Configuration conf = job.getConfiguration();

		// allow usage of symbolic links for the distributed cache
		DistributedCache.createSymlink(conf);

		String dictionaryURI = conf.get("dictionary");
		Dictionary dictionary = new Dictionary();
		dictionary.load(conf, dictionaryURI, sigma);

		ArrayList<Long> items = dictionary.getItems();

		int totalPartitions = 0;
		long partitionCurrentSize = 0;

		for (int i = 0; i < items.size(); ++i) {
			int itemId = PrimitiveUtils.getLeft(items.get(i));
			int frequency = PrimitiveUtils.getRight(items.get(i));

			if (frequency < sigma)
				continue;

			long partitionNewSize = partitionCurrentSize + frequency;
			long minMaxId = 0;

			if (partitionNewSize <= partitionSize && totalPartitions > 0) {
				// partition not full yet, add current item
				minMaxId = partitionToItems.get(totalPartitions);
				int minId = Math.min(itemId, PrimitiveUtils.getLeft(minMaxId));
				int maxId = Math.max(itemId, PrimitiveUtils.getRight(minMaxId));

				itemToPartition.put(itemId, totalPartitions);
				partitionToItems.put(totalPartitions, PrimitiveUtils.combine(minId, maxId));
				partitionCurrentSize += frequency;
			} else {
				// Create a new partition
				totalPartitions++;
				partitionCurrentSize = frequency;

				itemToPartition.put(itemId, totalPartitions);
				partitionToItems.put(totalPartitions, PrimitiveUtils.combine(itemId, itemId));
			}
		}

		job.getConfiguration().setInt("de.mpii.gsm.partitioning.totalPartitions", totalPartitions);
		job.getConfiguration().setInt("de.mpii.gsm.partitioning.debugPartId", debugPartId);

		// Taxonomy
		int[] itemToParent = dictionary.getItemToParent();

		// Serialize and add to distributed cache
		try {
			String part2ItemInfoObject = "partitionToItems";
			FileSystem fs = FileSystem.get(URI.create(part2ItemInfoObject), conf);
			ObjectOutputStream os = new ObjectOutputStream(fs.create(new Path(part2ItemInfoObject)));

			os.writeObject(partitionToItems);
			os.close();
			DistributedCache.addCacheFile(new URI(part2ItemInfoObject + "#partitionToItems"), conf);

			// Mapping item identifiers to partitions
			String item2PartInfoObject = "itemToPartition";
			os = new ObjectOutputStream(fs.create(new Path(item2PartInfoObject)));
			os.writeObject(itemToPartition);
			os.close();
			DistributedCache.addCacheFile(new URI(item2PartInfoObject + "#itemToPartition"), conf);

			// taxonomy
			String item2ParentInfoObject = "itemToParent";
			os = new ObjectOutputStream(fs.create(new Path(item2ParentInfoObject)));
			os.writeObject(itemToParent);
			os.close();
			DistributedCache.addCacheFile(new URI(item2ParentInfoObject + "#itemToParent"), conf);

		} catch (Exception e) {
			LOGGER.severe("Exception during serialization: " + e);
		}

		// Delete output if already exists
		Path outputDir = new Path(outputPath);
		outputDir.getFileSystem(conf).delete(outputDir, true);

		// input output paths
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, outputDir);

		// TODO: provide as optional argument
		// FileInputFormat.setMaxInputSplitSize(job, size);

		// Custom comparator and partitioner
		if (useAggregation) {
			job.setSortComparatorClass(GsmRawComparator.class);
		}
		job.setPartitionerClass(GsmPartitioner.class);

		// Input output formats
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Mapper Combiner Reducer
		job.setMapperClass(GsmMapper.class);

		if (useAggregation) {
			job.setCombinerClass(GsmCombiner.class);
		}

		job.setReducerClass(GsmReducer.class);

		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(numberOfReducers);
		job.getConfiguration().set("mapreduce.cluster.reducememory.mb", "7168");
		job.getConfiguration().setLong("mapred.task.timeout", 3600000);

		job.setOutputKeyClass(IntArrayWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.waitForCompletion(true);

		while (!job.isComplete()) {
			// wait
		}
	}

}
