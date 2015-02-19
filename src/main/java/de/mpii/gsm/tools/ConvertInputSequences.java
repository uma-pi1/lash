package de.mpii.gsm.tools;

import de.mpii.gsm.utils.DfsUtils;
import de.mpii.gsm.utils.IntArrayWritable;
import de.mpii.gsm.utils.FPWritable;

import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.list.IntArrayList;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * @author Kaustubh Beedkar
 * @author Klaus Berberich
 *
 */
public class ConvertInputSequences extends Configured implements Tool {

	static final Logger LOGGER = Logger.getLogger(ConvertInputSequences.class.getSimpleName());

	// ////
	// ///
	// // PHASE 1: Perform simple word count
	// /
	//
	public static final class WordCountMapper extends Mapper<LongWritable, Text, Text, FPWritable> {

		// singleton output key -- for efficiency reasons
		private final Text outKey = new Text();

		// singleton output value -- for efficiency reasons
		private final FPWritable outValue = new FPWritable();

		HashMap<String, String> parent;

		String itemSeparator = "\t";

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException, InterruptedException {

			itemSeparator = context.getConfiguration().get("de.mpii.tools.itemSeparator", "\t");

			try {
				ObjectInputStream is = new ObjectInputStream(new FileInputStream("parent"));
				parent = (HashMap<String, String>) is.readObject();
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			OpenObjectIntHashMap<String> wordCounts = new OpenObjectIntHashMap<String>();

			// seqId item_1 item_2 ... item_n
			String[] terms = value.toString().split(itemSeparator);
			for (int i = 1; i < terms.length; ++i) {
				String term = terms[i];

				wordCounts.adjustOrPutValue(term, +1, +1);

				while (parent.get(term) != null) {
					wordCounts.adjustOrPutValue(parent.get(term), +1, +1);
					term = parent.get(term);
				}
			}

			for (String term : wordCounts.keys()) {
				outKey.set(term);
				outValue.setFrequency(wordCounts.get(term));
				outValue.setParent(parent.get(term));
				context.write(outKey, outValue);
			}
		}
	}

	public static final class WordCountReducer extends Reducer<Text, FPWritable, Text, Text> {

		// singleton output key -- for efficiency reasons
		private final Text outKey = new Text();

		// singleton output value -- for efficiency reasons
		private final Text outValue = new Text();

		// collection frequencies
		private final OpenObjectIntHashMap<String> cfs = new OpenObjectIntHashMap<String>();

		// document frequencies
		private final OpenObjectIntHashMap<String> dfs = new OpenObjectIntHashMap<String>();

		// parents
		private final HashMap<String, String> parents = new HashMap<String, String>();

		@Override
		protected void reduce(Text key, Iterable<FPWritable> values, Context context) throws IOException,
				InterruptedException {
			String parent = null;

			int cf = 0;
			int df = 0;
			for (FPWritable value : values) {
				cf += value.getFrequency();
				df++;
				parent = value.getParent();
			}

			cfs.put(key.toString(), cf);
			dfs.put(key.toString(), df);
			parents.put(key.toString(), parent);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			List<String> temp = cfs.keys();
			String[] terms = Arrays.copyOf(temp.toArray(), temp.toArray().length, String[].class);

			// Remove parents with same frequency as children
			for (int i = 0; i < terms.length; ++i) {
				String term = terms[i];
				String parent = parents.get(term);
				if (term == null || parent == null)
					continue;
				while (cfs.get(term) == cfs.get(parent)) {
					parents.put(term, parents.get(parent));
					parent = parents.get(parent);
					if (parent == null)
						break;
				}
			}

			// sort terms in descending order of their collection frequency
			Arrays.sort(terms, new Comparator<String>() {
				@Override
				public int compare(String t, String u) {
					return cfs.get(u) - cfs.get(t);
				}
			});

			// assign term identifiers
			OpenObjectIntHashMap<String> tids = new OpenObjectIntHashMap<String>();
			for (int i = 0; i < terms.length; i++) {
				tids.put(terms[i], (i + 1));
			}

			for (String term : terms) {
				outKey.set(term);

				int parentId = (parents.get(term) == null) ? 0 : tids.get(parents.get(term));

				outValue.set(cfs.get(term) + "\t" + dfs.get(term) + "\t" + tids.get(term) + "\t" + parentId);

				context.write(outKey, outValue);
			}
		}

	}

	// ////
	// ///
	// // PHASE 2: Transform sequences into integer sequences
	// /
	//
	public static final class TransformationMapper extends Mapper<LongWritable, Text, LongWritable, IntArrayWritable> {

		// singleton output key -- for efficiency reasons
		private final LongWritable outKey = new LongWritable(0);

		// singleton output value -- for efficiency reasons
		private final IntArrayWritable outValue = new IntArrayWritable();

		// mapping from terms to their corresponding term identifiers
		private final OpenObjectIntHashMap<String> termTIdMap = new OpenObjectIntHashMap<String>();

		String itemSeparator = "\t";

		@Override
		protected void setup(@SuppressWarnings("rawtypes") Mapper.Context context) throws IOException, InterruptedException {
			itemSeparator = context.getConfiguration().get("de.mpii.tools.itemSeparator", "\t");

			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("dictionary")));
				while (br.ready()) {
					String[] tokens = br.readLine().split("\t");
					termTIdMap.put(tokens[0], Integer.parseInt(tokens[3]));
				}
				br.close();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

		}

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] terms = value.toString().split(itemSeparator);

			IntArrayList tids = new IntArrayList();

			for (int i = 1; i < terms.length; ++i) {

				tids.add(termTIdMap.get(terms[i]));
			}

			if (tids.size() > 0) {

				outValue.setContents(tids.toArray(new int[0]));
				context.write(outKey, outValue);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		LOGGER.setLevel(Level.INFO);
		if (args.length < 4) {
			LOGGER.log(Level.WARNING, "Usage: ConvertSequences <input> <output> <hierarchy> <numReducers> (<itemSeparator>)");
			System.exit(-1);
		}

		// read job parameters from cli arguments
		String input = args[0];
		String output = args[1];
		String taxPath = args[2];
		int numReducers = Integer.parseInt(args[3]);
		String itemSeparator = "\t";

		// item separator can be passed as an optional argument
		if (args.length > 4) {
			itemSeparator = args[4];
		}

		// delete output directory if it exists
		FileSystem.get(getConf()).delete(new Path(args[1]), true);

		// //
		// / PHASE 1: Compute word counts
		//
		Job job1 = new Job(getConf());

		// set job name and options
		job1.setJobName("document collection conversion (phase 1)");
		job1.setJarByClass(this.getClass());

		// set input and output paths
		FileInputFormat.setInputPaths(job1, DfsUtils.traverse(new Path(input), job1.getConfiguration()));
		TextOutputFormat.setOutputPath(job1, new Path(output + "/wc"));

		job1.getConfiguration().setStrings("de.mpii.tools.itemSeparator", itemSeparator);

		job1.getConfiguration().set("taxPath", taxPath);
		Configuration conf = job1.getConfiguration();

		// allow usage of symbolic links for the distributed cache
		DistributedCache.createSymlink(conf);

		String taxURI = conf.get("taxPath");

		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream dis = fs.open(new Path(taxURI));
		BufferedReader br = new BufferedReader(new InputStreamReader(dis));

		HashMap<String, String> parent = new HashMap<String, String>();
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] splits = line.split("\\s+");
			if (splits.length == 2)
				parent.put(splits[0], splits[1]);
		}
		br.close();

		// Serialize and add to distributed cache
		try {
			String tax = "parent";
			fs = FileSystem.get(URI.create(tax), conf);
			ObjectOutputStream os = new ObjectOutputStream(fs.create(new Path(tax)));

			os.writeObject(parent);
			os.close();
			DistributedCache.addCacheFile(new URI(tax + "#parent"), conf);

			parent.clear();

		} catch (Exception e) {
			e.printStackTrace();
		}

		// set input and output format
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		// set mapper and reducer class
		job1.setMapperClass(WordCountMapper.class);
		job1.setReducerClass(WordCountReducer.class);

		// set number of reducers
		job1.setNumReduceTasks(1);

		// map output classes
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(FPWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// start job
		job1.waitForCompletion(true);

		// //
		// / PHASE 2: Transform document collection
		//
		Job job2 = new Job(getConf());

		// set job name and options
		job2.setJobName("document collection conversion (phase 2)");
		job2.setJarByClass(this.getClass());

		job2.getConfiguration().setStrings("de.mpii.tools.itemSeparator", itemSeparator);

		// set input and output paths
		FileInputFormat.setInputPaths(job2, DfsUtils.traverse(new Path(input), job2.getConfiguration()));
		SequenceFileOutputFormat.setOutputPath(job2, new Path(output + "/raw"));
		SequenceFileOutputFormat.setCompressOutput(job2, false);

		// set input and output format
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		// set mapper and reducer class
		job2.setMapperClass(TransformationMapper.class);

		// set number of reducers
		job2.setNumReduceTasks(numReducers);

		// map output classes
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(IntArrayWritable.class);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(IntArrayWritable.class);

		// add files to distributed cache
		for (FileStatus file : FileSystem.get(getConf()).listStatus(new Path(output + "/wc"))) {
			if (file.getPath().toString().contains("part")) {
				DistributedCache.addCacheFile(new URI(file.getPath().toUri() + "#dictionary"), job2.getConfiguration());
			}
		}

		// start job
		job2.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ConvertInputSequences(), args);
		System.exit(exitCode);
	}

}
