import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ContextFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Lemmatization {

	private static HashMap<String, ArrayList<String>> lemmaMap = new HashMap<String, ArrayList<String>>();
	private static String SEPARATOR = ">";

	private static void readLemmas() {
		try {
			String path = "/home/hadoop/inputFiles/lemma/new_lemmatizer.csv";
			Scanner scanner = new Scanner(new File(path));
			while (scanner.hasNext()) {
				String nextStr = scanner.next();
				String strArr[] = nextStr.split(",");
				ArrayList<String> list = new ArrayList<String>();
				for (int i = 1; i < strArr.length; i++) {
					if (strArr[i] != null && strArr[i].length() != 0) {
						list.add(strArr[i]);
					}
				}
				lemmaMap.put(strArr[0], list);
			}
			scanner.close();
		} catch (Exception e) {
			System.out.println("Exception in readLemmas : " + e.getMessage());
		}

	}

	private static String normalizeStrings(String str) {
		str = str.replaceAll("v", "u");
		str = str.replaceAll("j", "i");
		return str;
	}

	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

		private static void parseAndMapData(String nextStr, Context context) {
			try {
				if (nextStr != null && nextStr.trim().length() != 0) {

					int indexOfSep = nextStr.indexOf(SEPARATOR);
					String docIdString = nextStr.substring(1, indexOfSep);
					String docId = docIdString.substring(0, docIdString.lastIndexOf(" "));
					String location[] = docIdString.substring(docIdString.lastIndexOf(" ")).split("\\.");
					String textData = nextStr.substring(indexOfSep + 1).trim();
					textData = normalizeStrings(textData);
					String[] words = textData.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
					for (int i = 0; i < words.length; i++) {
						String locationVal = "<" + docId + " [" + location[location.length - 1] + ", " + i + "]"+">";
						context.write(new Text(words[i]), new Text(locationVal));
						if (lemmaMap.containsKey(words[i])) {
							ArrayList<String> list = lemmaMap.get(words[i]);
							for (String val : list) {
								context.write(new Text(val), new Text(locationVal));
							}
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Exception in parseAndMapData : " + e.getMessage());
			}

		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			parseAndMapData(value.toString(), context);
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String res = "";
			int sum = 0;
			for (Text val : values) {
				res = res + " " + val.toString();
				sum++;
			}
			result.set(res + " count : " + sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		readLemmas();

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Lemmatization.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
