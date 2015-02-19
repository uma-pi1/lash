package de.mpii.gsm.driver;


/**
 * @author Kaustubh Beedkar
 * 
 */
public class GsmConfig {
	
	public static enum Mode {
		SEQUENTIAL, DISTRIBUTED
	};
	
	// i/o paths
	private String inputPath;
	private String outputPath;
	private String resumePath;
	private String keepFilesPath;
	private String hierarchyPath;
	private String encodedInputPath;
	private String encodedOutputPath;
	
	// Parameters
	private int sigma;
	private int gamma;
	private int lambda;
	
	
	// Execution mode
	private Mode mode;
	
	// Number of reducers when running in distributed mode
	private int numReducers;

	private boolean keepFiles;
	private boolean resume;
	
	private String itemSeparator;
	
	
	public GsmConfig(){
		// Default options
		this.inputPath = null;
		this.outputPath = null;
		this.resumePath = null;
		this.keepFilesPath = null;
		this.hierarchyPath = null;
		this.encodedInputPath = null;
		this.encodedOutputPath = null;
		
		this.sigma = 100;
		this.gamma = 0;
		this.lambda = 5;
		
		this.numReducers = 64;
		
		this.keepFiles = false;
		this.mode = Mode.SEQUENTIAL;
		this.resume = false;
		this.itemSeparator = "\\s+";
		
	}
	
	public GsmConfig(GsmConfig config) {
		this.inputPath = config.getInputPath();
		this.outputPath = config.getOutputPath();
		this.resumePath = config.getResumePath();
		this.keepFilesPath = config.getKeepFilesPath();
		this.hierarchyPath = config.getHierarchyPath();
		this.encodedInputPath = config.getEncodedInputPath();
		this.encodedOutputPath = config.getEncodedOutputPath();
		
		
		this.sigma = config.getSigma();
		this.gamma = config.getGamma();
		this.lambda = config.getLambda();
		
		this.mode = config.getMode();
		this.numReducers = config.getNumReducers();
		this.keepFiles = config.isKeepFiles();
		this.itemSeparator = config.itemSeparator;
	}

	/**
	 * @return the inputPath
	 */
	public String getInputPath() {
		return inputPath;
	}

	/**
	 * @param inputPath the inputPath to set
	 */
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	/**
	 * @return the outputPath
	 */
	public String getOutputPath() {
		return outputPath;
	}

	/**
	 * @param outputPath the outputPath to set
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	/**
	 * @return the resume
	 */
	public String getResumePath() {
		return resumePath;
	}

	/**
	 * @param resume the resume to set
	 */
	public void setResumePath(String resume) {
		this.resumePath = resume;
	}

	/**
	 * @return the keepFilesPath
	 */
	public String getKeepFilesPath() {
		return keepFilesPath;
	}

	/**
	 * @param keepFilesPath the keepFilesPath to set
	 */
	public void setKeepFilesPath(String keepFilesPath) {
		this.keepFilesPath = keepFilesPath;
	}

	/**
	 * @return the hierarchyPath
	 */
	public String getHierarchyPath() {
		return hierarchyPath;
	}

	/**
	 * @param hierarchyPath the hierarchyPath to set
	 */
	public void setHierarchyPath(String hierarchyPath) {
		this.hierarchyPath = hierarchyPath;
	}

	/**
	 * @return the sigma
	 */
	public int getSigma() {
		return sigma;
	}

	/**
	 * @param sigma the sigma to set
	 */
	public void setSigma(int sigma) {
		this.sigma = sigma;
	}

	/**
	 * @return the gamma
	 */
	public int getGamma() {
		return gamma;
	}

	/**
	 * @param gamma the gamma to set
	 */
	public void setGamma(int gamma) {
		this.gamma = gamma;
	}

	/**
	 * @return the lambda
	 */
	public int getLambda() {
		return lambda;
	}

	/**
	 * @param lambda the lambda to set
	 */
	public void setLambda(int lambda) {
		this.lambda = lambda;
	}

	/**
	 * @return the mode
	 */
	public Mode getMode() {
		return mode;
	}

	/**
	 * @param mode the mode to set
	 */
	public void setMode(Mode mode) {
		this.mode = mode;
	}

	/**
	 * @return the numReducers
	 */
	public int getNumReducers() {
		return numReducers;
	}

	/**
	 * @param numReducers the numReducers to set
	 */
	public void setNumReducers(int numReducers) {
		this.numReducers = numReducers;
	}

	/**
	 * @return the keepFiles
	 */
	public boolean isKeepFiles() {
		return keepFiles;
	}

	/**
	 * @param keepFiles the keepFiles to set
	 */
	public void setKeepFiles(boolean keepFiles) {
		this.keepFiles = keepFiles;
	}
	

	/**
	 * @param resume
	 */
	public void setResume(boolean resume) {
		this.resume = resume;
	}
	
	/**
	 * @return
	 */
	public boolean isResume() {
		return resume;
	}

	/**
	 * @return the itemSeparator
	 */
	public String getItemSeparator() {
		return itemSeparator;
	}

	/**
	 * @param itemSeparator the itemSeparator to set
	 */
	public void setItemSeparator(String itemSeparator) {
		this.itemSeparator = itemSeparator;
	}

	/**
	 * @return the encodedInputPath
	 */
	public String getEncodedInputPath() {
		return encodedInputPath;
	}

	/**
	 * @param encodedInputPath the encodedInputPath to set
	 */
	public void setEncodedInputPath(String encodedInputPath) {
		this.encodedInputPath = encodedInputPath;
	}

	/**
	 * @return the encodedOutputPath
	 */
	public String getEncodedOutputPath() {
		return encodedOutputPath;
	}

	/**
	 * @param encodedOutputPath the encodedOutputPath to set
	 */
	public void setEncodedOutputPath(String encodedOutputPath) {
		this.encodedOutputPath = encodedOutputPath;
	}
	
}
