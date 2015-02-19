package de.mpii.gsm.driver;


import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import de.mpii.gsm.driver.GsmConfig.Mode;

/**
 * @author Kausutubh Beedkar
 *
 */
public class GsmDriver {

	private static final Logger LOGGER = Logger.getLogger(GsmDriver.class.getSimpleName());
	
	private String[] args = null;

	private Options options = new Options();

	public GsmConfig config;

	public GsmDriver(String[] args) {
		
		LOGGER.setLevel(Level.ALL);
		
		config = new GsmConfig();

		this.args = args;

		options.addOption("h", "help", false, "show help.");

		options.addOption("i", "input", true, "input path.");
		options.addOption("o", "output", true, "output path.");
		options.addOption("H", "hierarchy", true, "hierarchy path.");

		options.addOption("s", "support", true, "minimum support.");
		options.addOption("g", "gap", true, "maximum gap.");
		options.addOption("l", "length", true, "maximum length.");

		options.addOption("m", "mode", true, "execution mode: (s)equential or (d)istributed.");

		options.addOption("r", "resume", true, "translated input path.");
		options.addOption("k", "keepFiles", true, "translated input path.");

		options.addOption("n", "numReducers", true, "number of reducers.");
	}

	public void parse() throws IOException {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
			if (cmd.hasOption("h"))
				help();

			// input
			if (cmd.hasOption("i")) {
				LOGGER.log(Level.INFO, "Using cli argument -i=" + cmd.getOptionValue("i"));
				config.setInputPath(cmd.getOptionValue("i"));
			} else {
				if(!cmd.hasOption("r")) {
					LOGGER.log(Level.SEVERE, "Missing input");
					help();
				}
			}

			// output
			if (cmd.hasOption("o")) {
				LOGGER.log(Level.INFO, "Using cli argument -o=" + cmd.getOptionValue("o"));
				config.setOutputPath(cmd.getOptionValue("o"));
			} else {
				LOGGER.log(Level.SEVERE, "Missing output");
				help();
			}

			// hierarchy
			if (cmd.hasOption("H")) {
				LOGGER.log(Level.INFO, "Using cli argument -H=" + cmd.getOptionValue("H"));
				config.setHierarchyPath(cmd.getOptionValue("H"));
			} else {
				if(!cmd.hasOption("r")) {
					LOGGER.log(Level.SEVERE, "Missing hierarchy");
					help();
				}
			}

			// support
			if (cmd.hasOption("s")) {
				LOGGER.log(Level.INFO, "Using cli argument -s=" + cmd.getOptionValue("s"));
				config.setSigma(Integer.parseInt(cmd.getOptionValue("s")));
			} else {
				LOGGER.log(Level.INFO, "Missing minimum support, using default value " + config.getSigma());
			}

			// gap
			if (cmd.hasOption("g")) {
				LOGGER.log(Level.INFO, "Using cli argument -g=" + cmd.getOptionValue("g"));
				config.setGamma(Integer.parseInt(cmd.getOptionValue("g")));
			} else {
				LOGGER.log(Level.INFO, "Missing maximum gap, using default value " + config.getGamma());
			}

			// length
			if (cmd.hasOption("l")) {
				LOGGER.log(Level.INFO, "Using cli argument -l=" + cmd.getOptionValue("l"));
				config.setLambda(Integer.parseInt(cmd.getOptionValue("l")));
			} else {
				LOGGER.log(Level.INFO, "Missing maximum length, using default value " + config.getLambda());
			}

			// mode
			if (cmd.hasOption("m")) {
				LOGGER.log(Level.INFO, "Using cli argument -m=" + cmd.getOptionValue("m"));
				// config.setMode(cmd.getOptionValue("m"));
				if (cmd.getOptionValue("m").equals("s")) {
					config.setMode(Mode.SEQUENTIAL);
				} else if (cmd.getOptionValue("m").equals("d")) {
					config.setMode(Mode.DISTRIBUTED);
				} else {
					LOGGER.log(Level.SEVERE, "Incorrect mode");
					help();
				}
			} else {
				LOGGER.log(Level.INFO, "Missing execution mode, using default value " + config.getMode());
			}

			// resume
			if (cmd.hasOption("r")) {
				if (!cmd.hasOption("i")) {
					LOGGER.log(Level.INFO, "Using cli argument -r=" + cmd.getOptionValue("r"));
					config.setResumePath(cmd.getOptionValue("r"));
					config.setResume(true);
				} else {
					LOGGER.log(Level.SEVERE, "input and resume are mutually exclusive");
					help();

				}
			}

			// keepFiles
			if (cmd.hasOption("k")) {
				if (!cmd.hasOption("r")) {
					LOGGER.log(Level.INFO, "Using cli argument -k=" + cmd.getOptionValue("k"));
					config.setKeepFilesPath(cmd.getOptionValue("k"));
					config.setKeepFiles(true);
				} else {
					LOGGER.log(Level.SEVERE, "keepfiles and resume are mutually exclusive");
					help();
				}
			}

			// number of reducers
			if (config.getMode() == Mode.DISTRIBUTED) {
				if (cmd.hasOption("n")) {
					LOGGER.log(Level.INFO, "Using cli argument -n=" + cmd.getOptionValue("n"));
					config.setNumReducers(Integer.parseInt(cmd.getOptionValue("n")));
				} else {
					LOGGER.log(Level.INFO, "Number of reducers, using default value " + config.getNumReducers());
				}
			}

		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, "Failed to parse arguments", e);
			help();
		}
	}

	private void help() {
		HelpFormatter formater = new HelpFormatter();
		formater.printHelp("lash", options);
		System.exit(0);
	}

	private void run() throws Exception {
		if (config.getMode() == Mode.SEQUENTIAL) {
			SequentialMode sMode = new SequentialMode(config);
			sMode.execute();
		} else {
			DistributedMode dMode = new DistributedMode(config);
			dMode.execute();
		}
	}

	public static void main(String[] args) throws Exception {
		GsmDriver driver = new GsmDriver(args);
		driver.parse();
		driver.run();
	}

}
