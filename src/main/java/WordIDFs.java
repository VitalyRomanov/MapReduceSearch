import java.util.HashMap;
import java.util.Map;

public class WordIDFs {

  Map<String, Integer> wordIDFMap;

  public WordIDFs(String WordIdsString) {

    Map<String, Integer> idmap = new HashMap<String, Integer>();

    for(String word_id : WordIdsString.trim().split("\n")) {
      String [] word_id_split = word_id.split("\\s+");
      if (word_id_split.length != 2) {
        continue;
      }
      idmap.put(word_id_split[0], Integer.parseInt(word_id_split[1]));
    }

    wordIDFMap = idmap;
  }

  public Integer getWordId(String word) {
    return this.wordIDFMap.getOrDefault(word, -1);
  }

  public Integer getVocabularySize() {
    return this.wordIDFMap.size();
  }
}