package de.mpii.gsm.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import de.mpii.gsm.localmining.Dfs;
import de.mpii.gsm.taxonomy.NytTaxonomy;
import de.mpii.gsm.taxonomy.Taxonomy;
import de.mpii.gsm.utils.Dictionary;
import de.mpii.gsm.writer.SequentialGsmWwriter;

/**
 * @author Kaustubh Beedkar
 * 
 */
public class SequentialMode {

	private GsmConfig config;// = new GsmConfig();

	private OpenObjectIntHashMap<String> tids = new OpenObjectIntHashMap<String>();

	private HashMap<String, String> parents = new HashMap<String, String>();

	private Dfs gsm;

	private SequentialGsmWwriter writer;

	public SequentialMode(GsmConfig config) throws IOException {
		this.config = config;

		if (config.isKeepFiles()) {
			try {
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);

				// create output files
				String inputFile = config.getKeepFilesPath() + "/raw/part-r-00000";
				String dictionaryFile = config.getKeepFilesPath() + "/wc/part-r-00000";

				if (!fs.exists(new Path(inputFile)))
					fs.create(new Path(inputFile));
				else {
					fs.delete(new Path(inputFile), true);
					fs.create(new Path(inputFile));
				}
				if (!fs.exists(new Path(dictionaryFile)))
					fs.create(new Path(dictionaryFile));
				else{
					fs.delete(new Path(dictionaryFile),true);
					fs.create(new Path(dictionaryFile));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public void clear() {
		tids.clear();
		parents.clear();
	}

	public void mine() {

	}

	private void encodeAndMine(String inputPath, String hierarchyPath) throws IOException, InterruptedException {

		// TODO: error checks for hierarchy path

		File hFile = new File(hierarchyPath);
		processHierarchy(hFile);

		File iFile = new File(inputPath);
		processRecursively(iFile, false);
	}

	private void processHierarchy(File hFile) throws IOException {

		FileInputStream fstream = new FileInputStream(hFile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line;

		while ((line = br.readLine()) != null) {
			String[] splits = line.split(config.getItemSeparator());
			if (splits.length == 2)
				parents.put(splits[0].trim(), splits[1].trim()); // TODO: check for DAGs
		}
		br.close();
	}

	private void processRecursively(File file, boolean mine) throws IOException, InterruptedException {

		if (file.isFile()) {
			if (mine)
				encodeAndMineSequences(file);
			else
				encodeSequences(file);
		} else {
			File[] subdirs = file.listFiles();
			for (File subdir : subdirs) {
				if (subdir.isDirectory())
					processRecursively(subdir, mine);
				else if (subdir.isFile())
					if (mine)
						encodeAndMineSequences(subdir);
					else
						encodeSequences(subdir);
			}
		}

	}

	private void encodeSequences(File inputFile) throws IOException, InterruptedException {

		FileInputStream fstream = new FileInputStream(inputFile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line;

		final OpenObjectIntHashMap<String> cfs = new OpenObjectIntHashMap<String>();
		final OpenObjectIntHashMap<String> dfs = new OpenObjectIntHashMap<String>();

		// compute generalized f-list
		while ((line = br.readLine()) != null) {
			OpenObjectIntHashMap<String> wordCounts = new OpenObjectIntHashMap<String>();

			String[] items = line.split(config.getItemSeparator());

			// seqId item_1 item_2 ... item_n
			for (int i = 1; i < items.length; ++i) {
				String item = items[i].trim();

				wordCounts.adjustOrPutValue(item, +1, +1);

				while (parents.get(item) != null) {
					wordCounts.adjustOrPutValue(parents.get(item), +1, +1);
					item = parents.get(item);
				}
			}

			for (String item : wordCounts.keys()) {
				cfs.adjustOrPutValue(item, +wordCounts.get(item), +wordCounts.get(item));
				dfs.adjustOrPutValue(item, +1, +1);
			}
		}
		br.close();

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
		for (int i = 0; i < terms.length; i++) {
			tids.put(terms[i], (i + 1));
		}

		// Write dictionary
		if (config.isKeepFiles()) {
			String outputFileName = config.getKeepFilesPath();

			File outFile = new File(outputFileName.concat("/" + "wc/part-r-00000"));

			try {

				OutputStream fstreamOutput = new FileOutputStream(outFile);

				// Get the object of DataOutputStream
				DataOutputStream out = new DataOutputStream(fstreamOutput);
				BufferedWriter br1 = new BufferedWriter(new OutputStreamWriter(out));

				// Perform the writing to the file
				for (String term : terms) {
					int parentId = (parents.get(term) == null) ? 0 : tids.get(parents.get(term));

					br1.write(term + "\t" + cfs.get(term) + "\t" + dfs.get(term) + "\t" + tids.get(term) + "\t" + parentId + "\n");
				}
				br1.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// create ItemId to ParentId list
		int[] parentIds = new int[terms.length + 1];
		OpenIntObjectHashMap<String> itemIdToItemMap = new OpenIntObjectHashMap<String>();

		for (String term : terms) {
			int parentId = (parents.get(term) == null) ? 0 : tids.get(parents.get(term));
			parentIds[tids.get(term)] = parentId;

			itemIdToItemMap.put(tids.get(term), term);
		}

		// Initialize writer
		writer.setItemIdToItemMap(itemIdToItemMap);
		writer.setOutputPath(config.getOutputPath());

		// Initialize taxonomy
		Taxonomy taxonomy = new NytTaxonomy(parentIds);

		// Mine sequences
		gsm = new Dfs();
		gsm.setParameters(config.getSigma(), config.getGamma(), config.getLambda(), taxonomy);
		gsm.initialize();
		processRecursively(new File(config.getInputPath()), true);

	}

	private void encodeAndMineSequences(File inputFile) throws IOException, InterruptedException {
		FileInputStream fstream = new FileInputStream(inputFile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line;

		while ((line = br.readLine()) != null) {
			String[] items = line.split(config.getItemSeparator());

			int[] sequenceAsInts = new int[items.length - 1];

			// seqId item_1 item_2 ... item_n
			for (int i = 1; i < items.length; ++i) {
				sequenceAsInts[i - 1] = tids.get(items[i].trim());
			}
			gsm.addTransaction(sequenceAsInts, 0, sequenceAsInts.length, 1);

			if (config.isKeepFiles()) {
				BufferedWriter bw = new BufferedWriter(new FileWriter(config.getKeepFilesPath() + "/raw/part-r-00000", true));
				for (int itemId : sequenceAsInts)
					bw.write(itemId + " ");

				bw.write("\n");
				bw.close();
			}
		}
		br.close();

		this.clear();

		gsm.mine(writer);
	}

	public void execute() throws Exception {

		this.writer = new SequentialGsmWwriter();

		if (config.isResume()) {
			String inputFile = config.getResumePath() + "/raw/part-r-00000";
			String dictionaryFile = config.getResumePath() + "/wc/part-r-00000";

			// Load dictionary
			Dictionary dict = new Dictionary();
			dict.load(null, dictionaryFile, config.getSigma());

			// Initialize writer
			writer.setItemIdToItemMap(dict.getItemIdToName());
			writer.setOutputPath(config.getOutputPath());

			// Initialize the taxonomy
			int[] itemToParent = dict.getItemToParent();
			Taxonomy taxonomy = new NytTaxonomy(itemToParent);

			// Mining
			gsm = new Dfs();
			gsm.setParameters(config.getSigma(), config.getGamma(), config.getLambda(), taxonomy);
			gsm.initialize();
			gsm.scanDatabase(inputFile);
			gsm.mine(writer);

		} else {

			encodeAndMine(config.getInputPath(), config.getHierarchyPath());

		}

	}

}
