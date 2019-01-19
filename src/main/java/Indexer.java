import org.apache.hadoop.fs.Path;


public class Indexer {

  public static void main(String[] args) throws Exception {
    Path pDocs = new Path(args[0]);
    Path pWordIdsOut = new Path(args[1]);
    Path pWordIDFsOut = new Path(args[2]);
    Path pWordIdsIn = pWordIdsOut;
    Path pWordIDFsIn = pWordIDFsOut;
    Path pIndex = new Path(args[3]);

    int res;
    res = WordEnumerator.run(pDocs, pWordIdsOut);
    if(res != 0) {
        System.err.println("Word Enumeration failed");
        System.exit(res);  
    }
    res = DocumentCount.run(pDocs, pWordIDFsOut);
    if(res != 0) {
        System.err.println("IDF count failed");
        System.exit(res);  
    }
    res = MapIndexer.run(pWordIdsIn, pWordIDFsIn, pDocs, pIndex);
    System.exit(res);
  }
}
// hadoop jar Indexer.jar Indexer AA_wiki_00 WordIds WordIDFs Index