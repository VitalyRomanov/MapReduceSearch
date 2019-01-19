import org.json.JSONObject;
import org.json.JSONException;

import java.util.HashSet;

public class WikiDoc extends TextualObject{
    private JSONObject doc;

    public WikiDoc(JSONObject docJson){
        this.doc = docJson;
    }

    public WikiDoc(String doc_raw) throws JSONException{
        JSONObject docJson = new JSONObject(doc_raw);
        this.doc = docJson;
    }

    public int getId() throws JSONException{
        return Integer.parseInt((String)doc.get("id"));
    }

    public String getTitle() throws JSONException{
        return (String) doc.get("title");
    }

    public String getText() throws JSONException{
        return (String) doc.get("text");
    }

    public HashSet<String> getWords() throws JSONException{
        
        String [] tokens = this.getText().replaceAll("[^-a-zA-Z ]", "").toLowerCase().split("\\s+");
        HashSet<String> docSet = new HashSet();

        for (String token : tokens) {
            docSet.add(token);
        }

        return docSet;
    }

    public String [] getTokens() throws JSONException {
        return this.getText().replaceAll("[^-a-zA-Z ]", "").toLowerCase().split("\\s+");
    }
}