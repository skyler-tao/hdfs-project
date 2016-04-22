package skyler.tao.hadoop.readwrite;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Main {

	private static final Logger logger = Logger.getLogger(Main.class);
	
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		int returnCode = ToolRunner.run(new ReadWriteProcess(), args);
		long endTime = System.currentTimeMillis();
		logger.info("It takes " + (endTime - startTime) / 1000 / 60 / 60 + " hours times!");
		System.exit(returnCode);
	}
}
