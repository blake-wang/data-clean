package com.ijunhai.common.http;



import java.io.Serializable;

/**
 * Created by Admin on 2017-09-05.
 */
public class HttpUtil implements Serializable{
    private HttpClientUtil httpClientUtil = null;
    public static void main(String args[]) throws Exception {
        String url = "http://game.data.ijunhai.com/Gamedata/api/getAgentGameChannel";
        String charset = "utf-8";
        HttpClientUtil hcu=new HttpClientUtil();
        Result result=hcu.doGet(url);
        System.out.println(result.getStatusCode());
        System.out.println(result.getContent());
//        HttpUtil main = new HttpUtil();
//        main.getReturn(url,charset);
    }
}
