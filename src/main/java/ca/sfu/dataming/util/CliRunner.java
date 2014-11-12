package ca.sfu.dataming.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Interface for command-line map-reduce program
 */
public interface CliRunner {
	
	//Initialize the command line options
	public Options initOptions();
	
	//Validate the input options
	public boolean validateOptions(CommandLine cmdLine);
	
	//Start the runner
	public void start(CommandLine cmdLine);
	
}
