// package MapReduceSearch;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import org.json.JSONObject;
import org.json.JSONException;
import java.lang.UnsupportedOperationException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.*;
import java.nio.file.Paths;
import java.nio.file.Files;

public class WikiDocsTokenizer {

    WikiDoc [] docs;
    int currentDoc = 0;

    public class WikiDoc {
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
            
            String [] tokens = this.getText().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
            HashSet<String> docSet = new HashSet();

            for (String token : tokens) {
                docSet.add(token);
            }

            return docSet;
        }
    }

    public WikiDocsTokenizer(String WikiDocs) throws JSONException {
        String [] docStrs = WikiDocs.split("\n");
        int numDocs = docStrs.length;
        WikiDoc [] docs = new WikiDoc[numDocs];

        for(int i=0; i<numDocs; i++) {
            docs[i] = new WikiDoc(docStrs[i]);
        }

        this.docs = docs;
    }

    boolean hasNext() {
        return this.currentDoc <= this.docs.length;
    }
    WikiDoc next(){
        return this.docs[currentDoc++];
    }
    void remove(){
        // throw   ();
    } 

    static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    public static void main(String[] args) throws IOException, JSONException{
        String text = WikiDocsTokenizer.readFile("AA_wiki_00", StandardCharsets.UTF_8);
        // String [] docs = text.split("\n");
        // System.out.println(docs[0]);
        WikiDocsTokenizer wikiDocs = new WikiDocsTokenizer(text);
        int counter = 0;

        while (wikiDocs.hasNext()) {
            // for(String word : wikiDocs.next().getWords()) {
            //     System.out.println(word);
            // }
            // System.out.println(wikiDocs.next().getTitle());
            counter++;
            if (counter > 0){
                break;
            }
        }
        
    }
}