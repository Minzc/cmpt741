package ca.sfu.dataming.util;

import org.apache.commons.cli.*;


/**
 * Class for command line tools
 * @author Arber
 */
public class AdvCli {
	
	//Parameter constants
	public static final String CLI_PARAM_S = "s";	//start row
	public static final String CLI_PARAM_E = "e";	//end row
	public static final String CLI_PARAM_ALL = "all"; //all rows
	public static final String CLI_PARAM_I = "i";	//input file
	public static final String CLI_PARAM_O = "o";	//output file
	public static final String CLI_PARAM_K = "k"; 	//keywords parameters
	public static final String CLI_PARAM_HELP = "help"; //help
	
	private static HelpFormatter formatter = new HelpFormatter();

	/**
	 * @param args	program arguments
	 * @param cmdName	command-line program name
	 * @param runner	command-line runner
	 */
	public static void initRunner(String[] args, String cmdName, CliRunner runner) {
		CommandLineParser parser = new GnuParser();
		Options options = runner.initOptions();
		try {
			CommandLine cmdLine = parser.parse(options, args);
			if (!runner.validateOptions(cmdLine) || cmdLine.hasOption(CLI_PARAM_HELP)) {
				formatter.printHelp(cmdName, options);
				return;
			}
			runner.start(cmdLine);
		} catch (ParseException e) {
			System.out.println("Unexpected exception:" + e.getMessage());
			formatter.printHelp(cmdName, options);
		}
	}
	
	/**
	 * Generate standard job name: 
	 * 	"command: input=<input>, output=<output>"
	 * Recommend to use when standard command line options are used. 
	 * 
	 * @param command
	 * @param cmdLine
	 */
	public static String genStandardJobName( String command, CommandLine cmdLine ){
		String inputString = null;
		if( cmdLine.hasOption(CLI_PARAM_ALL) )
			inputString = CLI_PARAM_ALL;
		else if( cmdLine.hasOption(CLI_PARAM_I) )
			inputString = cmdLine.getOptionValue(CLI_PARAM_I);
		else 
			inputString = cmdLine.getOptionValue(CLI_PARAM_S) + '-' + cmdLine.getOptionValue(CLI_PARAM_E);
			
		return String.format("%s: input=%s, output=%s", command, inputString, 
				cmdLine.hasOption(CLI_PARAM_O)?cmdLine.getOptionValue(CLI_PARAM_O):"null");
	}

}
