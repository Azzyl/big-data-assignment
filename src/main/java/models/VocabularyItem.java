package models;

import org.json.JSONObject;

public class VocabularyItem {
    private String word;
    private long wordId;
    private long inverseDocumentFrequency;

    public VocabularyItem(String word, long wordId, long inverseDocumentFrequency) {
        this.word = word;
        this.wordId = wordId;
        this.inverseDocumentFrequency = inverseDocumentFrequency;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getWordId() {
        return wordId;
    }

    public long getInverseDocumentFrequency() {
        return inverseDocumentFrequency;
    }

    public String getWord() {
        return word;
    }

    public static VocabularyItem parseJSON(String str){
        JSONObject jsonObj = new JSONObject(str);
        return new VocabularyItem(
                jsonObj.getString("word"),
                jsonObj.getLong("wordId"),
                jsonObj.getLong("inverseDocumentFrequency")
        );
    }

    public String createJSON(){
        JSONObject json = new JSONObject();
        json.put("word", this.word);
        json.put("wordId", this.wordId);
        json.put("inverseDocumentFrequency", this.inverseDocumentFrequency);
        return json.toString();
    }
}
