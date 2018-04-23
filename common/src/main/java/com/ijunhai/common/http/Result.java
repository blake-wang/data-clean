package com.ijunhai.common.http;

/**
 * Created by Admin on 2017-09-07.
 */
public class Result {
    private int statusCode=0;
    private String content="";
    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }


}
