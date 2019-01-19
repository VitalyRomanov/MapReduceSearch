import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;

class RankerOutput {

    // HashSet<Integer> docs;
    HashMap<Integer, Float> ranks;
    public RankerOutput(String ro_raw) {
        String[] records = ro_raw.trim().split("\n");

        // this.docs = HashSet<Integer>();
        this.ranks = new HashMap<Integer, Float>();

        for(String record : records) {
            String[] split = record.split("\\s+");
            Integer id = Integer.parseInt(split[1]);
            float rank = Float.parseFloat(split[0]);
            // this.docs.add(id);
            this.ranks.put(id, rank);
        }
    }

    public boolean contains(Integer id) {
        return this.ranks.containsKey(id);
    }

    public float getRank(Integer id) {
        return this.ranks.get(id);
    }
}


public class DocumentFilter {

  private static String getParamFromContext(Context context, String paramName) {
    Configuration conf = context.getConfiguration();
    String param = conf.get(paramName);
    return param;
  }

  public static class FilterMapper
       extends Mapper<Object, Text, FloatWritable, Text>{
    
    FloatWritable rank = new FloatWritable();

    RankerOutput rankerOutput = null;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      if(rankerOutput == null) {
        rankerOutput = new RankerOutput(getParamFromContext(context, "Rank"));
      }
      
      try{
        WikiDoc doc = new WikiDoc(value.toString());
        Integer id = doc.getId();
        Text title = new Text(doc.getTitle());
        if(rankerOutput.contains(id)) {
          rank.set(rankerOutput.getRank(id));
          context.write(rank, title);
        }
      } catch (Exception e) {
        System.err.println("Cannot interpret document");
      }
    }
  }

  public static class ReverseComparator extends WritableComparator {
    public ReverseComparator() {
       super(FloatWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(b, a);
    }
  }

  public static class TrivialReducer
       extends Reducer<FloatWritable, Text, FloatWritable, Text> {
    private IntWritable result = new IntWritable();

    public void reduce(FloatWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      for (Text val : values) {
        context.write(key, val);
      }
    }
  }

  // public static Integer run(Path pWordIds, Path pDocs, Path pOutput) throws Exception {
  public static Integer run(Path pDocs, Path pRank, Path pOutput) throws Exception {

    Configuration conf = new Configuration();

    // String wordIdsString = HDFSReader.readFile(conf, pWordIds);
    // conf.set("WordIds", wordIdsString);

    conf.set("Rank", HDFSReader.readFile(conf, pRank));

    Job job = Job.getInstance(conf, "Document Filter");
    job.setJarByClass(DocumentFilter.class);
    job.setMapperClass(FilterMapper.class);
    job.setSortComparatorClass(ReverseComparator.class);
    job.setCombinerClass(TrivialReducer.class);
    job.setReducerClass(TrivialReducer.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, pDocs);
    FileOutputFormat.setOutputPath(job, pOutput);

    // return 0;
    return job.waitForCompletion(true) ? 0 : 1;
  }
  

  public static void main(String[] args) throws Exception {
    
    // Path wordIds = new Path(args[0]);
    Path docs = new Path(args[0]);
    Path rank = new Path(args[1]);
    Path output = new Path(args[2]);

    // System.exit(run(wordIds, docs, output));
    System.exit(run(docs, rank, output));
  }
}


// javac -cp $(hadoop classpath):. DocumentFilter.java WordIds.java
// jar cvf DocumentFilter.jar DocumentFilter*.class WordIds.class WikiDoc*.class
// hadoop jar DocumentFilter.jar DocumentFilter AA_wiki_00 out-doc