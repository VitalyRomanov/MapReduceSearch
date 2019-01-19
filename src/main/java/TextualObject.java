import java.util.HashSet;

public abstract class TextualObject {
    abstract public HashSet<String> getWords() throws Exception;
    abstract public String [] getTokens() throws Exception;
}