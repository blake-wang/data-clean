package com.ijunhai.common;

import org.bson.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Admin on 2017-08-16.
 */
public class XMLUtil {
    public static Document xml2bson(String fileName) throws JDOMException, IOException {
        // 定义一个输出流，相当StringBuffer（），会根据读取数据的大小，调整byte的数组长度
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileInputStream fis=new FileInputStream(fileName);
        // 定义一个容量来盛放数据
        byte[] buf = new byte[1024*1024];
        // 获得缓存读取流开始的位置
        int len=0;
        while ((len = fis.read(buf)) != -1) {
            // 如果有数据的话，就把数据添加到输出流
            //这里直接用字符串StringBuffer的append方法也可以接收
            baos.write(buf, 0, len);
        }
        Document json = new Document();
        InputStream is = new ByteArrayInputStream(baos.toByteArray());

        SAXBuilder sb = new SAXBuilder();
        org.jdom2.Document doc = sb.build(is);
        Element root = doc.getRootElement();
        json.put(root.getName(), iterateElement(root));
        return json;
    }
    public static Document xml2bson(ByteArrayOutputStream baos) throws JDOMException, IOException {
        Document json = new Document();
        InputStream is = new ByteArrayInputStream(baos.toByteArray());

        SAXBuilder sb = new SAXBuilder();
        org.jdom2.Document doc = sb.build(is);
        Element root = doc.getRootElement();
        json.put(root.getName(), iterateElement(root));
        return json;
    }
    private static Document iterateElement(Element element) {
        List node = element.getChildren();
        Element et ;
        Document obj = new Document();
        List list ;
        for (int i = 0; i < node.size(); i++) {
            list = new LinkedList();
            et = (Element) node.get(i);
            if (et.getTextTrim().equals("")) {
                if (et.getChildren().size() == 0)
                    continue;
                if (obj.containsKey(et.getName())) {
                    list = (List) obj.get(et.getName());
                }
                list.add(iterateElement(et));
                obj.put(et.getName(), list.get(0));
            } else {
                if (obj.containsKey(et.getName())) {
                    list = (List) obj.get(et.getName());
                }
                list.add(et.getTextTrim());
                obj.put(et.getName(), list.get(0));
            }
        }
        return obj;
    }
}
