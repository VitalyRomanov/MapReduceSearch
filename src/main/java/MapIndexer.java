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

import com.google.gson.Gson;

public class MapIndexer {

  private static String getParamFromContext(Context context, String paramName) {
    Configuration conf = context.getConfiguration();
    String param = conf.get(paramName);
    return param;
  }

  public static class IndexerMapper
       extends Mapper<Object, Text, IntWritable, Text>{

    private Text indexStr = new Text();
    private IntWritable docId = new IntWritable();

    WordIds wordIds = null;//new WordIds(getParamFromContext(context, "WordIds"));
    WordIDFs wordIDFs = null;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      if (wordIds == null || wordIDFs == null) {
        wordIds = new WordIds(getParamFromContext(context, "WordIds"));
        wordIDFs = new WordIDFs(getParamFromContext(context, "WordIDFs"));
      }                

      try{
        WikiDoc doc = new WikiDoc(value.toString());

        float [] docIndex = DocIndexer.index(wordIds, wordIDFs, doc);

        docId.set(doc.getId());

        Gson gson = new Gson();
        indexStr.set(gson.toJson(docIndex));
        
        context.write(docId, indexStr);

      } catch (Exception e) {
        System.err.println("Cannot interpret document: "+ e.toString());
      }
    }
  }

//   public static class IntSumReducer
//        extends Reducer<Text,IntWritable,Text,IntWritable> {
//     private IntWritable result = new IntWritable();

//     public void reduce(Text key, Iterable<IntWritable> values,
//                        Context context
//                        ) throws IOException, InterruptedException {
//       Integer sum = 0;
//       for (IntWritable val : values) {
//         sum += val.get();
//       }
//       result.set(sum);
//       context.write(key, result);
//     }
//   }

  public static Integer run(Path pWordIds, Path pWordIDFs, Path pDocs, Path pOutput) throws Exception {

    Configuration conf = new Configuration();

    conf.set("WordIds", HDFSReader.readFile(conf, pWordIds));
    conf.set("WordIDFs", HDFSReader.readFile(conf, pWordIDFs));

    Job job = Job.getInstance(conf, "Indexer");
    job.setJarByClass(MapIndexer.class);
    job.setMapperClass(IndexerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    // job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.addInputPath(job, pDocs);
    FileOutputFormat.setOutputPath(job, pOutput);

    // return 0;
    return job.waitForCompletion(true) ? 0 : 1;
  }
  

  public static void main(String[] args) throws Exception {
    
    // Path wordIds = new Path(args[0]);
    Path pWordIds = new Path(args[0]);
    Path pWordIDFs = new Path(args[1]);
    Path pDocs = new Path(args[2]);
    Path pOutput = new Path(args[3]);

    // System.exit(run(wordIds, docs, output));
    System.exit(run(pWordIds, pWordIDFs, pDocs, pOutput));
  }
}


// javac -cp $(hadoop classpath) MapIndexer.java WordIds.java WordIDFs.java HDFSReader.java WikiDoc.java DocIndexer.java
// jar cvf Indexer.jar MapIndexer*.class WordIds*.class WordIDFs*.class HDFSReader*.class WikiDoc*.class DocIndexer*.class
// hadoop jar MapIndexer.jar MapIndexer out-wiki/part-r-00000 out-doc/part-r-00000 AA_wiki_00 out-index