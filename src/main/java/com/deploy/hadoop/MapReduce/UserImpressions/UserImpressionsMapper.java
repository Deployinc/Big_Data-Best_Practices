package com.deploy.hadoop.MapReduce.UserImpressions;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class UserImpressionsMapper extends
		Mapper<Object, Text, Text, IntWritable> {

	private final IntWritable ONE = new IntWritable(1);
	private Text ip = new Text();

	Logger log = Logger.getLogger(UserImpressionsMapper.class);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// LogFormat
		// "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"" combined
		// 74.193.71.107 - - [14/Jun/2013:12:42:44 -0500]
		// "GET /resources/images/demo_employer/demo.v2.png HTTP/1.1" 200 34518
		// "https://www.terefic.com/" "Mozilla/5.0 (Macintosh;
		// Intel Mac OS X 10_7_5) AppleWebKit/536.29.13 (KHTML, like Gecko)
		// Version/6.0.4 Safari/536.29.13"

		String regex = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|(.+?)) \"([^\"]+|(.+?))\" \"([^\"]+|(.+?))\"";

		Pattern accessLogPattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		Matcher accessLogEntryMatcher;

		accessLogEntryMatcher = accessLogPattern.matcher(value.toString());
		if (!accessLogEntryMatcher.matches()) {
			System.out.println("" + value.toString() + " : couldn't be parsed");
			log.info("" + value.toString() + " : couldn't be parsed");
		} else {
			ip.set(accessLogEntryMatcher.group(1));
			context.write(ip, ONE);
		}
	}
}