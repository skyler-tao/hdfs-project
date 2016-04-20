package skyler.tao.hadoop.readwrite;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.json.JSONObject;

public class ReadWriteProcess extends Configured implements Tool {

	public static final String FS_PARAM_NAME = "fs.defaultFS";
	private static final Logger logger = Logger.getLogger(ReadWriteProcess.class);

	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			logger.info("HdfsReadWrite [hdfs input path] [hdfs output path]");
			return 1;
		}

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = getConf();
		logger.info("configured filesystem = " + conf.get(FS_PARAM_NAME));
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, false)));

		String message = null;
		String messageParsed = null;
		try {
			while ((message = br.readLine()) != null) {

				messageParsed = parseLine(message);
				if (messageParsed == null)
					continue;
				bw.write(messageParsed);
			}
			bw.flush();
		} catch (Exception e) {

		} finally {
			br.close();
			bw.close();
		}
		return 0;
	}

	public String parseLine(String log) {
		int index = log.indexOf('|');
		String message = null;
		if (index > 0) {
			message = log.substring(index + 1);
		}

		if (message == null)
			return null;

		String result = "";
		StatsParser parser = new StatsParser(message);
		StatsHiveTarget target = new StatsHiveTarget();
		JSONObject json = parser.getJSONObject();
		String reqtime = json.optString("reqtime", "null");
		if (reqtime.contains("null") || "".equals(reqtime)) {
			return null;
		}
		target.setReqtime(reqtime);
		target.setReqid(json.optString("reqid", "null"));
		target.setUid(json.optString("uid", "null"));
		target.setFrom(json.optString("from", "null"));
		target.setPlatform(json.optString("platform", "null"));
		target.setVersion(json.optString("version", "null"));
		target.setIp(json.optString("ip", "null"));
		target.setProxy_source(json.optString("proxy_source", "null"));
		target.setWm(json.optString("wm", "null"));
		target.setAvailable_pos(json.optString("available_pos", "null"));
		target.setCategory_r(json.optString("category_r", "null"));
		target.setIdfa(json.optString("idfa", "null"));
		target.setImei(json.optString("imei", "null"));
		target.setLocation(json.optString("location", "null"));
		target.setIs_unread_pool(json.optString("is_unread_pool", "null"));
		target.setLoadmore(json.optString("loadmore", "null"));
		target.setFeedsnum(json.optString("feedsnum", "null"));
		target.setUnread_status(json.optString("unread_status", "null"));
		target.setLast_span(json.optString("last_span", "null"));
		target.setProduct_r(json.optString("product_r", "null"));
		target.setRefresh_type(json.optString("refresh_type", "null"));
		target.setPostostock(json.optString("pos", "null"));
		target.setTmeta(json.optString("tmeta_l2", "null"));
		target.setStat_date(reqtime2date(json.optString("reqtime")));
		target.setService_name(json.optString("service_name", "null"));

		result = target.toString();
		return result;
	}

	private String reqtime2date(String reqtime) {
		String result = "";
		if (reqtime.length() == 10) {
			try {
				long millis = Long.parseLong(reqtime) * 1000;
				Date date = new Date(millis);
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				result = format.format(date).toString();
			} catch (Exception e) {}
		}
		return result;
	}
}
