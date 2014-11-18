package edu.nyu.cs.cs2580;
import java.util.Vector;

/**
 * Created by sharikri on 10/19/14.
 */
public class StopWords {
    private Vector<String> stopWords = new Vector<String>();

    public Vector<String> getStopWords() {
        stopWords.add("the");
        stopWords.add("and");
        stopWords.add("or");
        stopWords.add("an");
        stopWords.add("if");
        stopWords.add("but");
        stopWords.add("the");
        stopWords.add("is");
        stopWords.add("an");
        stopWords.add("he");
        stopWords.add("she");
        stopWords.add("be");
        stopWords.add("me");
        stopWords.add("has");
        stopWords.add("http");
        stopWords.add("a");
        stopWords.add("able");
        stopWords.add("about");
        stopWords.add("after");
        stopWords.add("ago");
        stopWords.add("an");
        stopWords.add("also");
        stopWords.add("are");
        stopWords.add("by");
        stopWords.add("eg");
        stopWords.add("etc");
        stopWords.add("him");
        stopWords.add("his");
        stopWords.add("in");
        stopWords.add("shall");
        stopWords.add("you");
        stopWords.add("[edit]");
        return stopWords;
    }
}
