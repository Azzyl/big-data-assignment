package models;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;

public class IndexNormalized {
    private long wordId;
    private double TF_IDF;

    public IndexNormalized(long wordId, double TF_IDF) {
        this.wordId = wordId;
        this.TF_IDF = TF_IDF;
    }

    public double getTF_IDF() {
        return TF_IDF;
    }

    public long getWordId() {
        return wordId;
    }

    public static IndexNormalized parseJSON(JSONObject jsonObj) {
        return new IndexNormalized(
                jsonObj.getLong("wordId"),
                jsonObj.getDouble("TF_IDF")
        );
    }

    public JSONObject createJSON() {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("wordId", this.wordId);
        jsonObj.put("TF_IDF", this.TF_IDF);
        return jsonObj;
    }

    public static ArrayList<IndexNormalized> parseJSONArray(JSONArray jsonArray) {
        ArrayList<IndexNormalized> indexes = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            indexes.add(parseJSON(jsonArray.getJSONObject(i)));
        }
        return indexes;
    }
}
