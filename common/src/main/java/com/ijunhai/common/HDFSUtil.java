package com.ijunhai.common;

import com.ijunhai.common.ip.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.bson.Document;
import org.jdom2.JDOMException;
import com.ijunhai.common.XMLUtil;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 2016/10/12.
 */
public class HDFSUtil {

    private FileSystem fileSystem = null;

    /**
     * 从HDFS上读取文件
     */
    public static void readFromHdfs(String hdfs, String local) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
        FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));

        OutputStream out = new FileOutputStream(local);
        byte[] ioBuffer = new byte[1024 * 1024];
        int readLen = hdfsInStream.read(ioBuffer);

        while (-1 != readLen) {
            out.write(ioBuffer, 0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        out.close();
        hdfsInStream.close();
        fs.close();
    }

    /**
     * 读数据清洗的配置文件
     *
     * @param hdfs
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws JDOMException
     * @throws IOException
     */
    public static Document readConfigFromHdfs(String hdfs) throws IOException, InterruptedException, JDOMException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");

        FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] ioBuffer = new byte[1024 * 1024];
        int readLen = hdfsInStream.read(ioBuffer);

        while (-1 != readLen) {
            baos.write(ioBuffer, 0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        Document doc = XMLUtil.xml2bson(baos);
        baos.close();
        hdfsInStream.close();
        fs.close();
        return doc;
    }

    /**
     * 读ip库
     *
     * @param hdfs
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws JDOMException
     * @throws IOException
     */
    public static byte[] readFromHdfs(String hdfs) throws InterruptedException, JDOMException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
        FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));

        byte[] ioBuffer = new byte[hdfsInStream.available()];
        byte[] buf = new byte[1024 * 1024];
        int index = 0;
        int readLen = hdfsInStream.read(buf);
        while (-1 != readLen) {
            System.arraycopy(buf, 0, ioBuffer, index, readLen);
            index += readLen;
            readLen = hdfsInStream.read(buf);
        }
        hdfsInStream.close();
        fs.close();
        return ioBuffer;
    }

    /**
     * 读IP库
     *
     * @param hdfs
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws JDOMException
     * @throws IOException
     */
    public static IP readIPFromHdfsIp(String hdfs) throws IOException, InterruptedException, JDOMException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
        FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));
        IP ipClass = new IP();
        ipClass.load(hdfsInStream);
        hdfsInStream.close();
        fs.close();
        return ipClass;
    }

    /**
     * 上传文件到HDFS上去
     */
    public static void uploadToHdfs(String hdfs, String local) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        InputStream in = new FileInputStream(new File(local));
        FileSystem fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
        OutputStream out = fs.create(new Path(hdfs), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     * 上传文件到HDFS上去
     */
    public static void uploadToHdfs(String hdfs, byte[] in) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");  //使用maven打包需要添加这个字段
        int pos = 0;
        int batchSize = 4096;
        int currentSize = 0;
        FileSystem fs;
        OutputStream out;
        try {
            System.out.println("hdfs path :" + hdfs);
            fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
            out = fs.create(new Path(hdfs));
        } catch (Exception e) {
            e.printStackTrace();
            hdfs = "hdfs://uhadoop-1neej2-master2/tmp";
            System.out.println("hdfs path :" + hdfs);
            fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
            out = fs.create(new Path(hdfs));
        }

        while (pos < in.length) {
            currentSize = in.length - pos > batchSize ? batchSize : (in.length - pos);
            out.write(in, pos, currentSize);
            pos += currentSize;
        }
        out.close();
        fs.close();
    }

    /**
     * 从HDFS上删除文件
     */
    public static void deleteFromHdfs(String hdfs) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
        fs.deleteOnExit(new Path(hdfs));//如果存在就删除
        fs.close();
    }

    /**
     * 列举文件列表
     *
     * @param a
     * @param recursively 是否递归子目录
     * @throws IOException
     * @throws InterruptedException
     */
    public static List<String> listFiles(String a, Boolean recursively) throws IOException, InterruptedException {
        List<String> fileList = new ArrayList<String>();
        Configuration conf = new Configuration();
        FileSystem fs;
        String hdfs;
        RemoteIterator<LocatedFileStatus> itr;
        try {
            hdfs = "hdfs://10.13.171.219/tmp"; //uhadoop-1neej2-master1
            fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
            itr = fs.listFiles(new Path(hdfs), recursively);
        } catch (Exception e) {
            System.out.println("master is on m2");
            hdfs = "hdfs://10.13.55.188/tmp"; //uhadoop-1neej2-master2
            fs = FileSystem.get(URI.create(hdfs), conf, "hadoop");
            itr = fs.listFiles(new Path(hdfs), recursively);
        }

        while (itr.hasNext()) {
            LocatedFileStatus lfs = itr.next();
            fileList.add(lfs.getPath().getName());
            if (lfs.getPath().getName().contains("json")) {
                System.out.println(lfs.getPath().getName());
            }
        }
        fs.close();
        return fileList;
    }


    public static void main(String args[]) {
        String hdfs = "hdfs:///tmp";
        String dst = "hdfs://192.168.1.111/data/ip_database/17monipdb.dat";
        String configFile = "hdfs://192.168.1.111/data/config/cleanConfig.xml";
        String local = "E:\\64bit_software\\64bit_software\\17monipdb\\17monipdb.dat";

        String content = "{\"ret\":\"1\",\"msg\":\"\",\"content\":[{\"id\":\"1\",\"game_channel_id\":\"1\",\"game_channel_name\":\"\",\"game_id\":\"1\",\"channel_id\":\"1\",\"pf\":\"0\"},{\"id\":\"2\",\"game_channel_id\":\"2\",\"game_channel_name\":\"\",\"game_id\":\"1\",\"channel_id\":\"2\",\"pf\":\"0\"}]}";
        try {

//            uploadToHdfs(hdfs + "/temp.json", content.getBytes());
            listFiles(hdfs, false);
//            byte[] temp = readFromHdfs(hdfs + "/temp.json");
//            System.out.println(new String(temp));
//            readFromHdfs(hdfs,local);
//            uploadToHdfs(hdfs,local);
//            deleteFromHdfs(dst);
//            listFiles(hdfs,false);
//            Document doc=readConfigFromHdfs(configFile);
//            System.out.println(doc);
//            IPAddress.init(local);
//            IP ip=new IP();
//            ip.load(readFromHdfs(dst));
//            IP ipClass= readIPFromHdfsIp(dst);
//            for (int i = 0; i < 8525962; i++) {
//                if(ip.getDataBuffer().get(i) !=ipClass.getDataBuffer().get(i)){
//                    System.out.println(i);
//                    System.out.println(ip.getDataBuffer().get(i));
//                    System.out.println(ipClass.getDataBuffer().get(i));
//                    System.out.println("error");
//                    break;
//                }
//            }
//
//            System.out.println(readFromHdfs(dst).length);
//            Map<String,String> map1=IPAddress.getIPAddress("14.18.242.247",ipClass);
//            Map<String,String> map=IPAddress.getIPAddress("14.18.242.247",ip);
//            for (String key:map.keySet()) {
//                System.out.println(map.get(key));
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
