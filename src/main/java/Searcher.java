import org.apache.hadoop.fs.Path;


public class Searcher {

  public static void main(String[] args) throws Exception {
    Path pWordIds = new Path(args[0]);
    Path pWordIDFs = new Path(args[1]);
    Path pIndex = new Path(args[2]);
    Path docs = new Path(args[3]);
    Path pRankerOutput = new Path(args[4]);
    String query = args[6];

    int res;
    res = MapReduceRanker.run(pWordIds, pWordIDFs, pIndex, pRankerOutput, query);
    if(res != 0) {
        System.exit(res);  
    }
    
    Path output = new Path(args[5]);
    res = DocumentFilter.run(docs, pRankerOutput, output);
    System.exit(res);
  }
}
// hadoop jar Indexer.jar Searcher WordIds WordIDFs Index AA_wiki_00 Rank Result "anarchism"