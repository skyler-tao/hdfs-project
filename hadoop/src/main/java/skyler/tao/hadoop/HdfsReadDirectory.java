package skyler.tao.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class HdfsReadDirectory extends Configured implements Tool {

	public static final String FS_PARAM_NAME = "fs.defaultFS";

	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("HdfsReader [hdfs input path] [local output path]");
			return 1;
		}

		Path inputPath = new Path(args[0]);
		String localOutputPath = args[1];
		Configuration conf = getConf();
		System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
		Job job = Job.getInstance(conf);
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(inputPath, true);
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			// do stuff with the file like ...
			job.addFileToClassPath(fileStatus.getPath());
		}

		return 0;
	}

}
