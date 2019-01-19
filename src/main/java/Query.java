import org.json.JSONObject;
import org.json.JSONException;

import java.util.HashSet;

public class Query extends TextualObject{
    private String query;

    public Query(String doc_raw) {
        this.query = doc_raw;
    }

    public String getText() {
        return this.query;
    }

    public HashSet<String> getWords() {
        
        String [] tokens = this.query.replaceAll("[^-a-zA-Z ]", "").toLowerCase().split("\\s+");
        HashSet<String> docSet = new HashSet();

        for (String token : tokens) {
            docSet.add(token);
        }

        return docSet;
    }

    public String [] getTokens() {
        return this.query.replaceAll("[^-a-zA-Z ]", "").toLowerCase().split("\\s+");
    }
}