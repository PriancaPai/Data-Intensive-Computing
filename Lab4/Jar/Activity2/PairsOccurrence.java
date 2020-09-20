import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairsOccurrence {

  public static class PairsOccurrenceMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	System.out.println("Your message " + value.toString());
        String[] tokens = value.toString().split("\\s+");
        int len = tokens.length;
	for (int i = 0; i < len; i++){
		for (int j = 0; j < len; j++){
			boolean isString1 = tokens[i].matches(".*[a-zA-Z]+.*");
			boolean isString2 = tokens[j].matches(".*[a-zA-Z]+.*");
			if(i != j && isString1 && isString2){
				word.set("<" + tokens[i]+ " " + tokens[j] + ">");
				context.write(word, one);			
			}
		}
	}
    }
  }

  public static class PairsOccurrenceReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Pairs Occurrence");
    job.setJarByClass(PairsOccurrence.class);
    job.setMapperClass(PairsOccurrenceMapper.class);
    job.setCombinerClass(PairsOccurrenceReducer.class);
    job.setReducerClass(PairsOccurrenceReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
