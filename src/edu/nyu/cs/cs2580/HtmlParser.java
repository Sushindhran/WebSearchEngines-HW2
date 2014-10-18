package edu.nyu.cs.cs2580;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.util.Iterator;

/**
 * Created by abhisheksanghvi on 10/17/14.
 */
public class HtmlParser {

    public static String parseFile (File file) {

        StringBuffer strippedOutput = new StringBuffer();

        try {
            Document html = Jsoup.parse(file, "UTF-8");
            strippedOutput.append(html.title() + "\t");

            strippedOutput.append(html.body().text());

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return strippedOutput.toString();
    }

    public static void main(String args[]) {
        //File f =  new File("./data/simple/test.html");
        File f =  new File("./data/wiki/1975–76_UEFA_Cup");
        System.out.println(parseFile(f));
    }
}
