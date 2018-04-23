package com.ijunhai.mq2kafka;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class test {
    private static int i;
    Writer writer ;

    public test() throws IOException {
        Writer writer = new FileWriter("C:\\Users\\admin\\Desktop", true);

    }

    public static int getcount() throws Exception {
        return i;
    }

    public static void add(int a) {
        test.i += a;
    }

    public static void set(int b) {
        test.i = b;
    }
}
