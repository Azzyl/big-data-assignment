package models;

import org.json.JSONObject;

public class Document {
    private long id;
    private String title;
    private String text;
    private String url;

    public String getText() {
        return text;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void parseJSON(String str){
        JSONObject jsonObj = new JSONObject(str);
        id = jsonObj.getLong("id");
        title = jsonObj.getString("title");
        text = jsonObj.getString("text");
        url = jsonObj.getString("url");
    }
}
