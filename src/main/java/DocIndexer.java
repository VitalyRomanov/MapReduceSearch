public class DocIndexer {
    public static float[] index(WordIds wordIds, WordIDFs wordIDFs, TextualObject doc) throws Exception {
        Integer vocSize = wordIds.getVocabularySize();
        assert vocSize == wordIDFs.getVocabularySize();
        float [] docIndex = new float[vocSize];

        for(String w : doc.getTokens()) {
          Integer wordId = wordIds.getWordId(w);
          if (wordId == -1) {
            continue;
          }
          docIndex[wordId] += 1.;
        }

        for(String w : doc.getWords()) {
          Integer wordId = wordIds.getWordId(w);
          Integer wordIDF = wordIDFs.getWordId(w);
          if ((wordId == -1) || (wordIDF == -1)) {
            continue;
          }
          
          docIndex[wordId] /= wordIDF;
        }

        return docIndex;
    }
}