import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.Mapper.Context;




public class DocumentCount {

  private static String getParamFromContext(Context context, String paramName) {
    Configuration conf = context.getConfiguration();
    String param = conf.get(paramName);
    return param;
  }

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      // WordIds wordIds = new WordIds(getParamFromContext(context, "WordIds"));

      try{
        WikiDoc doc = new WikiDoc(value.toString());

        for(String w : doc.getWords()) {
          word.set(w);
          context.write(word, one);
        }
      } catch (Exception e) {
        System.err.println("Cannot interpret document");
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Integer sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  // public static Integer run(Path pWordIds, Path pDocs, Path pOutput) throws Exception {
  public static Integer run(Path pDocs, Path pOutput) throws Exception {

    Configuration conf = new Configuration();

    // String wordIdsString = HDFSReader.readFile(conf, pWordIds);
    // conf.set("WordIds", wordIdsString);

    Job job = Job.getInstance(conf, "IDF count");
    job.setJarByClass(DocumentCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, pDocs);
    FileOutputFormat.setOutputPath(job, pOutput);

    // return 0;
    return job.waitForCompletion(true) ? 0 : 1;
  }
  

  public static void main(String[] args) throws Exception {
    
    // Path wordIds = new Path(args[0]);
    Path docs = new Path(args[0]);
    Path output = new Path(args[1]);

    // System.exit(run(wordIds, docs, output));
    System.exit(run(docs, output));
  }
}


// javac -cp $(hadoop classpath):. DocumentCount.java WordIds.java
// jar cvf DocumentCount.jar DocumentCount*.class WordIds.class WikiDoc*.class
// hadoop jar DocumentCount.jar DocumentCount AA_wiki_00 out-doc