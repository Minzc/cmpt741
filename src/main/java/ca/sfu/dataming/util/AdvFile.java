package ca.sfu.dataming.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Advanced file utility functions 
 */
public class AdvFile {
	private static final String COMMENT_SIGN = "#";
	
	/**
	 * Load file in the simple format: 
	 * <value>|<alias1>|<alias2>....
	 * <value>|<alias1>|<alias2>....
	 * @param input stream 
	 * @return a dictionary mapping from <alias> to <value>  
	 * @throws java.io.IOException
	 */
	public static Map<String, List<String>> loadFileInSimpleFormat( InputStream in ) throws IOException{
		Map<String, List<String>> dict = new HashMap<String, List<String>>();

		String line = null;
		BufferedReader br = new BufferedReader( new InputStreamReader(in) );
		while( (line = br.readLine() ) != null ){
			if( line.isEmpty() || line.startsWith(COMMENT_SIGN) ) continue;

			String[] values = line.split(StringUtil.STR_DELIMIT_1ST);
			for( String val: values ){
				List<String> itemList = dict.containsKey(val)? dict.get( val ): new LinkedList<String>();
				itemList.add(values[0]);
				dict.put(val, itemList);
			}
		}
		return dict;
	}

	/**
	 * Load file in line format, with a customized line parser
	 * 1.lines starting with "#" are comments
	 * 2.empty lines are ignored
	 * 3.lines are trim();
	 * @throws java.io.IOException
	 */	
	public static void loadFileInDelimitLine( InputStream in, ILineParser lineParser ) 
		throws IOException{
		String line = null;
		BufferedReader br = new BufferedReader( new InputStreamReader(in) );
		while( (line = br.readLine() ) != null ){
			if( line.trim().isEmpty() || line.startsWith(COMMENT_SIGN) ) continue;
			lineParser.parseLine(line.trim());
		}
		br.close();
		in.close();
	}	
}
