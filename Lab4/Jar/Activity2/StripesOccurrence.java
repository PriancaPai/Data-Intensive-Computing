import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;

public class StripesOccurrence {

  public static class StripesOccurrenceMapper extends Mapper<Object, Text, Text, MapWritable>{
    private final static IntWritable one = new IntWritable(1);
    private MapWritable occurrenceMap = new MapWritable();
    private Text word1 = new Text();
    private Text word2 = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	//System.out.println("Your message " + value.toString());
        
	String[] tokens = value.toString().split("\\s+");

        int len = tokens.length;

	for (int i = 0; i < len; i++){
		occurrenceMap.clear();
		for (int j = 0; j < len; j++){
			boolean isString1 = tokens[i].matches(".*[a-zA-Z]+.*");
			boolean isString2 = tokens[j].matches(".*[a-zA-Z]+.*");
			
			if(i != j && isString1 && isString2){
				word2.set(tokens[j]);
				if(occurrenceMap.containsKey(word2)){
				       IntWritable count = (IntWritable)occurrenceMap.get(word2);
				       count.set(count.get()+1);
				}else{
				       occurrenceMap.put(word2,one);
				}			
			}
		}
		if(tokens[i].matches(".*[a-zA-Z]+.*")){
			word1.set(tokens[i]);
			context.write(word1,occurrenceMap);
		}
	}

    }
  }


  public static class StripesOccurrenceReducer extends Reducer<Text, MapWritable, Text, Text> {
    
    private MapWritable result = new MapWritable();
    private Text word = new Text();

    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
	
	result.clear();

	for (MapWritable mapWritable : values) {
		Set<Writable> keys = mapWritable.keySet();
		for (Writable keyVal : keys) {
		    IntWritable fromCount = (IntWritable) mapWritable.get(keyVal);
		    if (result.containsKey(keyVal)) {
		        IntWritable count = (IntWritable) result.get(keyVal);
		        count.set(count.get() + fromCount.get());
		    } else {
		        result.put(keyVal, fromCount);
		    }
		}
        }
	
	StringBuilder resultStr = new StringBuilder();
        Set<Writable> keySet = result.keySet();
	resultStr.append("{");

        for (Object keyVal : keySet) {
            resultStr.append(keyVal.toString() + " = " + result.get(keyVal) + "    ");
        }
	resultStr.append("}");

	word.set(resultStr.toString().trim());
        
	context.write(key, word);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Stripes Occurrence");
    job.setJarByClass(StripesOccurrence.class);
    job.setMapperClass(StripesOccurrenceMapper.class);
    //job.setCombinerClass(StripesOccurrenceReducer.class);
    job.setReducerClass(StripesOccurrenceReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}