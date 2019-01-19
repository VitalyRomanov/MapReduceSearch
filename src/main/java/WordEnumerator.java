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

import org.json.JSONException;

public class WordEnumerator {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      try{
        WikiDoc doc = new WikiDoc(value.toString());

        for(String token : doc.getWords()) {
          word.set(token);
          context.write(word, one);
        }
      } catch (Exception e) {
        System.err.println("Cannot interpret document");
      }

    }
  }

  public static class WordIdEnumerator
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable Id = new IntWritable();
    // private IntWritable currentId = new IntWritable(0);

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(key, Id);
      Id.set(Id.get()+1);
    }
  }

  public static int run(Path inputPath, Path outputPath) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Word ID Enumerator");
    job.setJarByClass(WordEnumerator.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(WordIdEnumerator.class);
    job.setReducerClass(WordIdEnumerator.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(run(new Path(args[0]), new Path(args[1])));
  }
}

// javac -cp $(hadoop classpath):. WordEnumerator.java WikiDoc.java
// jar cvf WordEnumerator.jar WordEnumerator*.class WikiDoc*.class
// hadoop jar WordEnumerator.jar WordEnumerator AA_wiki_00 out-wiki