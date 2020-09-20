import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CooccurrencePerformance4b {

	private static HashMap<String, ArrayList<String>> lemmaMap = new HashMap<String, ArrayList<String>>();
	private static String SEPARATOR = ">";

	private static void readLemmas() {
		try {
			String path = "/home/hadoop/inputFiles/lemma/new_lemmatizer.csv";
			Scanner scanner = new Scanner(new File(path));
			while (scanner.hasNext()) {
				String strArr[] = scanner.next().toLowerCase().split(",");
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
			System.out.println("Error Message");
			e.printStackTrace();
		}

	}

	private static String normalizeStrings(String str) {
		str = str.replaceAll("v", "u");
		str = str.replaceAll("j", "i");
		return str;
	}

	public static class CooccurrencePerformance4bMapper extends Mapper<Object, Text, Text, Text> {

		private static void callCombinations(String word1, String word2, String word3, String locationString,
				Context context) throws IOException, InterruptedException {

			ArrayList<String> word1LemmasList = new ArrayList<String>();
			ArrayList<String> word2LemmasList = new ArrayList<String>();
			ArrayList<String> word3LemmasList = new ArrayList<String>();

			if (lemmaMap.containsKey(word1)) {
				word1LemmasList = lemmaMap.get(word1);
			}

			if (lemmaMap.containsKey(word2)) {
				word2LemmasList = lemmaMap.get(word2);
			}

			if (lemmaMap.containsKey(word3)) {
				word3LemmasList = lemmaMap.get(word3);
			}

			word1LemmasList.add(word1);
			word2LemmasList.add(word2);
			word3LemmasList.add(word3);

			for (String lemma1 : word1LemmasList) {
				for (String lemma2 : word2LemmasList) {
					for (String lemma3 : word3LemmasList) {
						Text word = new Text();
						word.set("{" + lemma1 + " " + lemma2 + " " + lemma3 + "}");
						context.write(word, new Text(locationString));
					}
				}
			}
		}

		private static void parseAndMapData(String nextStr, Context context) {
			try {
				if (nextStr != null && nextStr.trim().length() != 0) {

					int indexOfSep = nextStr.indexOf(SEPARATOR);

					String locationString = nextStr.substring(1, indexOfSep);
					locationString = "<" + locationString + ">";

					String textData = nextStr.substring(indexOfSep + 1).trim();
					textData = normalizeStrings(textData);

					String[] words = textData.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");

					int len = words.length;
					for (int i = 0; i < len; i++) {
						for (int j = 0; j < len; j++) {
							for (int k = 0; k < len; k++) {
								boolean isString1 = words[i].matches(".*[a-zA-Z]+.*");
								boolean isString2 = words[j].matches(".*[a-zA-Z]+.*");
								boolean isString3 = words[k].matches(".*[a-zA-Z]+.*");
								if (i != j && i != k && k != j && isString1 && isString2 && isString3) {
									callCombinations(words[i], words[j], words[k], locationString, context);
								}
							}
						}
					}
				}
			} catch (Exception e) {
				//System.out.println("Error Message");
				//e.printStackTrace();
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			parseAndMapData(value.toString(), context);
		}
	}

	public static class CooccurrencePerformance4bReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String res = "";
			for (Text val : values) {
				res = res + " " + val.toString();
			}
			context.write(key, new Text(res));
		}
	}

	public static void main(String[] args) throws Exception {
		readLemmas();

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Cooccurrence Performance");
		job.setJarByClass(CooccurrencePerformance4b.class);
		job.setMapperClass(CooccurrencePerformance4bMapper.class);
		job.setReducerClass(CooccurrencePerformance4bReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
