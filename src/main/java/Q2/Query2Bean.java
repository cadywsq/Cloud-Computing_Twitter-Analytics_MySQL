package Q2;
public class Query2Bean {
    // combination of user id and tag
    private String user_tag;
    // formatted returning result including sentiment density, timestamp and tweet
    private String content;
    public String getUser_tag() {
        return user_tag;
    }
    public void setUser_tag(String user_tag) {
        this.user_tag = user_tag;
    }
    public String getContent() {
        return content;
    }
    public void setContent(String content) {
        this.content = content;
    }


}
