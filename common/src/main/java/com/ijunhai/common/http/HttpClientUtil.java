package com.ijunhai.common.http;

/**
 * Created by Admin on 2017-09-05.
 */

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;

import java.io.IOException;
import java.io.InputStream;


/*
 * 利用HttpClient进行post请求的工具类
 */
public class HttpClientUtil {
    HttpClient httpClient=null;
    private int connectTimeOut=5000;
    private int socketTimeOut=10000;
    public HttpClientUtil(){
        this.httpClient=new HttpClient();
    }
    public Result doGet(String url){
        Result result=new Result();
        int batchSize=102400;
        HttpMethod method = new GetMethod(url);
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(connectTimeOut);
        httpClient.getHttpConnectionManager().getParams().setSoTimeout(socketTimeOut);

        try {
            int statusCode = httpClient.executeMethod(method);
//        String content=method.getResponseBodyAsString();
//        Header[] header=method.getRequestHeaders();
            InputStream in=method.getResponseBodyAsStream();
            StringBuilder ioBuffer = new StringBuilder("");
            byte[] buf = new byte[batchSize];
            int readLen = in.read(buf);
            while (-1 != readLen) {
//                System.out.println("readLen:"+readLen+" buf:"+new String(buf).length()+"bufContent:"+new String(buf));
                ioBuffer.append(new String(buf),0,readLen);
                readLen = in.read(buf);
            }
            String content = ioBuffer.toString();
            result.setStatusCode(statusCode);
            result.setContent(content);
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public Result doPost(String url) throws IOException {
        int batchSize=2048;
        HttpMethod method = new PostMethod(url);
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(connectTimeOut);
        httpClient.getHttpConnectionManager().getParams().setSoTimeout(socketTimeOut);
        int statusCode = httpClient.executeMethod(method);
//        String content=method.getResponseBodyAsString();

//        Header[] header=method.getRequestHeaders();
        InputStream in=method.getResponseBodyAsStream();
        StringBuilder ioBuffer = new StringBuilder("");
        byte[] buf = new byte[batchSize];
        int readLen = in.read(buf);
        while (-1 != readLen) {
            ioBuffer.append(new String(buf),0,readLen);
            readLen = in.read(buf);
        }
        String content = ioBuffer.toString();

        Result result=new Result();
        result.setStatusCode(statusCode);
        result.setContent(content);
        return result;
    }

    public int getConnectTimeOut() {
        return connectTimeOut;
    }

    public void setConnectTimeOut(int connectTimeOut) {
        this.connectTimeOut = connectTimeOut;
    }

    public int getSocketTimeOut() {
        return socketTimeOut;
    }

    public void setSocketTimeOut(int socketTimeOut) {
        this.socketTimeOut = socketTimeOut;
    }
}
