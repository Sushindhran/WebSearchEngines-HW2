package edu.nyu.cs.cs2580;

import java.util.HashMap;

/**
 * @CS2580: implement this class for HW2 to incorporate any additional
 * information needed for your favorite ranker.
 */
public class DocumentIndexed extends Document {
    private static final long serialVersionUID = 9184892508124423115L;
    private long _noOfWords = 0;
    private HashMap<String, Integer> wordFrequency = new HashMap<String, Integer>();

    public DocumentIndexed(int docid) {
        super(docid);
    }

    public long getNumberOfWords() {
        return _noOfWords;
    }

    public void setNumberOfWords(long noOfWords) {
        this._noOfWords = noOfWords;
    }

    public void setWordFrequency(String word) {
        if (wordFrequency.containsKey(word)) {
            int i = wordFrequency.get(word);
            i++;
            wordFrequency.put(word, i);
        } else {
            wordFrequency.put(word, 1);
        }
    }

    public HashMap<String, Integer> getWordFrequency() {
        return wordFrequency;
    }
}
