package de.mpii.gsm.driver;

import java.io.File;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import de.mpii.gsm.lash.GsmJob;
import de.mpii.gsm.tools.ConvertInputSequences;
import de.mpii.gsm.tools.SequenceTranslator;

public class DistributedMode {

	private GsmConfig config;// = new GsmConfig();

	public DistributedMode(GsmConfig config) throws IOException {

		this.config = config;
		
		//prepare i/o for jobs
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		//delete paths if already exists
		Path outputDir = new Path(config.getOutputPath());
		outputDir.getFileSystem(conf).delete(outputDir, true);
		
		//TODO: use tmp dir that is optionally provided by user
		
		File encodedOutputPath = File.createTempFile("LASH_ENCODED_OUTPUT_", "");
		encodedOutputPath.delete();
		encodedOutputPath.mkdir();
		
		config.setEncodedOutputPath(encodedOutputPath.getAbsolutePath().toString());
		
		
		if(config.isKeepFiles()) {
			Path endcodedInputPath = new Path(config.getKeepFilesPath());
			if(fs.exists(endcodedInputPath))
				fs.delete(endcodedInputPath, true);
			
			config.setEncodedInputPath(config.getKeepFilesPath());
		} else {
			File encodedInputPath = File.createTempFile("LASH_ENCODED_INPUT_", "");
			encodedInputPath.delete();
			encodedInputPath.mkdir();
			
			config.setEncodedInputPath(encodedInputPath.getAbsolutePath().toString());
		}

	}

	public void execute() throws Exception {

		if (config.isResume()) {
			// GsmJob
			GsmJob.runGsmJob(config);

			// Sequence Translator
			String[] sequenceTranslatorArgs = { config.getEncodedOutputPath(), config.getOutputPath(),
					config.getResumePath().concat("/wc/part-r-00000") };
			SequenceTranslator.main(sequenceTranslatorArgs);
			
		} else {
			// Convert input sequences job
			String[] conversionArgs = { config.getInputPath(), config.getEncodedInputPath(), config.getHierarchyPath(),
					Integer.toString(config.getNumReducers()), config.getItemSeparator() };
			ToolRunner.run(new ConvertInputSequences(), conversionArgs);

			// GsmJob
			GsmJob.runGsmJob(config);

			// Sequence Translator
			String[] sequenceTranslatorArgs = { config.getEncodedOutputPath(), config.getOutputPath(),
					config.getEncodedInputPath().concat("/wc/part-r-00000") };
			
			SequenceTranslator.main(sequenceTranslatorArgs);
		}

	}

}
