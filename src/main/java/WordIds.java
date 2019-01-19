import java.util.HashMap;
import java.util.Map;

public class WordIds {

  Map<String, Integer> wordIdMap;

  public WordIds(String WordIdsString) {

    Map<String, Integer> idmap = new HashMap<String, Integer>();

    for(String word_id : WordIdsString.trim().split("\n")) {
      String [] word_id_split = word_id.split("\\s+");
      if (word_id_split.length != 2) {
        continue;
      }
      idmap.put(word_id_split[0], Integer.parseInt(word_id_split[1]));
    }

    wordIdMap = idmap;
  }

  public Integer getWordId(String word) {
    return this.wordIdMap.getOrDefault(word, -1);
  }

  public Integer getVocabularySize() {
    return this.wordIdMap.size();
  }
}