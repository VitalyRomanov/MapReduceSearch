import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.gson.Gson;

class DocIndexDeserializer {
  private Integer docId;
  private float[] index;
  public DocIndexDeserializer(String raw_serialized_str) {
    String [] raw_split = raw_serialized_str.trim().split("\t");
    this.docId = Integer.parseInt(raw_split[0]);

    Gson gson = new Gson();
    this.index = gson.fromJson(raw_split[1], float[].class);
  }

  public Integer getId() {
    return this.docId;
  }

  public float[] getIndex() {
    return this.index;
  }
}

public class MapReduceRanker {

  private static String getParamFromContext(Context context, String paramName) {
    Configuration conf = context.getConfiguration();
    String param = conf.get(paramName);
    return param;
  }

  public static class RankerMapper
       extends Mapper<Object, Text, FloatWritable, IntWritable>{

    private FloatWritable relevance = new FloatWritable();
    private IntWritable docId = new IntWritable();

    WordIds wordIds = null;
    WordIDFs wordIDFs = null;
    Query query = null;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      if(wordIds == null || wordIDFs == null || query == null) {
        query = new Query(getParamFromContext(context, "Query"));
        wordIds = new WordIds(getParamFromContext(context, "WordIds"));
        wordIDFs = new WordIDFs(getParamFromContext(context, "WordIDFs"));
      }
      

      DocIndexDeserializer docIndex = new DocIndexDeserializer(value.toString());
      Integer documentId = docIndex.getId();
      float[] index = docIndex.getIndex();

      try {
        float [] queryIndex = DocIndexer.index(wordIds, wordIDFs, query);

        float rank = (float) 0.0;

        for(int i=0; i<index.length; i++) {
          rank += queryIndex[i] * index[i];
        }

        relevance.set(rank);
        docId.set(documentId);

        context.write(relevance, docId);
      } catch (Exception e) {
        System.err.println("Error while processing query: " + e.toString());
      }
    }
  }


  public static class ReverseComparator extends WritableComparator {
    //private static final FloatWritable.Comparator FLOAT_COMPARATOR = new FloatWritable.Comparator();
    public ReverseComparator() {
       super(FloatWritable.class, true);
    }

    // @Override
    // public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    //   return (-1)* TEXT_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
    // }

    //@SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //if (a instanceof FloatWritable && b instanceof FloatWritable) {
        //        return (((FloatWritable) b).compareTo((FloatWritable) a));
        //}
        return super.compare(b, a);
    }
  }



  public static class RankFilter
       extends Reducer<FloatWritable, IntWritable, FloatWritable,IntWritable> {
    private FloatWritable rank = new FloatWritable();
    private IntWritable count = new IntWritable(0);

    public void reduce(FloatWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      rank.set(key.get());
      for (IntWritable val : values) {
        if ((count.get() < 10) && (rank.get() > 0)) {
          count.set(count.get()+1);
          context.write(rank, val);
        }
      }
    }
  }

  public static Integer run(Path pWordIds, Path pWordIDFs, Path pIndex, Path pOutput, String query) throws Exception {

    Configuration conf = new Configuration();

    conf.set("WordIds", HDFSReader.readFile(conf, pWordIds));
    conf.set("WordIDFs", HDFSReader.readFile(conf, pWordIDFs));
    conf.set("Query", query);

    Job job = Job.getInstance(conf, "Ranker");
    job.setJarByClass(MapReduceRanker.class);
    job.setMapperClass(RankerMapper.class);
    job.setSortComparatorClass(ReverseComparator.class);
    job.setCombinerClass(RankFilter.class);
    job.setReducerClass(RankFilter.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, pIndex);
    FileOutputFormat.setOutputPath(job, pOutput);
    job.setNumReduceTasks(1);


    // return 0;
    return job.waitForCompletion(true) ? 0 : 1;
  }
  

  public static void main(String[] args) throws Exception {
    
    // Path wordIds = new Path(args[0]);
    Path pWordIds = new Path(args[0]);
    Path pWordIDFs = new Path(args[1]);
    Path pIndex = new Path(args[2]);
    Path pOutput = new Path(args[3]);
    String query = args[4];

    // System.exit(run(wordIds, docs, output));
    System.exit(run(pWordIds, pWordIDFs, pIndex, pOutput, query));
  }
}
