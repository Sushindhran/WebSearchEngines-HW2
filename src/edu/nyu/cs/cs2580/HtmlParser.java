package edu.nyu.cs.cs2580;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.*;
import java.util.Iterator;
import java.util.Vector;

/**
 * Created by abhisheksanghvi on 10/17/14.
 */
public class HtmlParser {

    public static String parseFile (File file) {

        StringBuffer strippedOutput = new StringBuffer();

        try {
            Document html = Jsoup.parse(file, "UTF-8");

            String body = html.body().text();
            body = body.replaceAll("\"", " ").trim();
            body=body.replaceAll(",", "").trim();
            body=body.replaceAll("\t", " ").trim();
            body=body.replaceAll("'", "").trim();
            body=body.replaceAll(":", "").trim();
            body=body.replaceAll(";", "").trim();
            body=body.replaceAll("\\)", "").trim();
            body=body.replaceAll("\\(", "").trim();
            body=body.replaceAll("\t", "").trim();
            body=body.replaceAll("\\.", "").trim();

            String title = html.title();
            title=title.replaceAll(",", "").trim();
            title=title.replaceAll("\t", " ").trim();
            title=title.replaceAll("'", "").trim();
            title=title.replaceAll(":", "").trim();
            title=title.replaceAll(";", "").trim();
            title=title.replaceAll("\\)", "").trim();
            title=title.replaceAll("\\(", "").trim();
            title=title.replaceAll("\t", "").trim();
            title=title.replaceAll("\\.", "").trim();


            strippedOutput.append(title + "\t");

            strippedOutput.append(body);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return strippedOutput.toString();
    }
}