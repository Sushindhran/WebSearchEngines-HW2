package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable{

    private static final long serialVersionUID = 1096251905740085030L;

    private Map<Integer, Integer> skipointers = new HashMap<Integer, Integer>();

    private Map<String, Integer> dictionary =
            new HashMap<String, Integer>();

    private Map<String, Integer> urlDictionary =
            new HashMap<String, Integer>();

    // private Map<Integer,HashSet<Integer>> index =
    //       new HashMap<Integer, HashSet<Integer>>();

    private Map<Integer,ArrayList<Integer>> index =
            new HashMap<Integer, ArrayList<Integer>>();

    private Vector<Document> _documents =
            new Vector<Document>();
    private Map<Integer, Integer> termCorpusFrequency =
            new HashMap<Integer, Integer>();

    private Map<Integer, Map<Integer, Integer>> documentTermFrequency
            = new HashMap<Integer, Map<Integer, Integer>>();

    int uniqueTermNum = 0;



    public IndexerInvertedDoconly(Options options) {
        super(options);
        System.out.println("Using Indexer: " + this.getClass().getSimpleName());
    }

    @Override
    public void constructIndex() throws IOException {
        _numDocs = 0;
        String corpusFile = _options._corpusPrefix;
        File folder = new File(corpusFile);
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile()) {
                String content = HtmlParser.parseFile(file);

                Scanner s = new Scanner(content).useDelimiter("\t");

                String title = "";
                if (s.hasNext()) {
                    title = s.next();
                    updateIndex(title, file);
                }
                if(s.hasNext()) {
                    String body = s.next();
                    updateIndex(body, file);
                }
                DocumentIndexed documentIndexed = new DocumentIndexed(_numDocs);
                documentIndexed.setTitle(title);
                documentIndexed.setUrl(file.getAbsolutePath());
                _documents.add(documentIndexed);
                _numDocs++;
            }
        }
        String indexFile = _options._indexPrefix + "/corpus_docOnly.idx";
        System.out.println("Store index to: " + indexFile);
        ObjectOutputStream writer =
                new ObjectOutputStream(new FileOutputStream(indexFile));
        writer.writeObject(this);
        writer.close();
    }

    private void updateIndex(String document, File file) {
        String[] words = document.split(" ");
        for (int i=0; i<words.length; i++) {
            String lower = words[i].toLowerCase();
            String term = PorterStemming.getStemmedWord(lower);

            if (!dictionary.containsKey(term)) {
                dictionary.put(term, uniqueTermNum);
                ArrayList<Integer> docIds = new ArrayList<Integer>();
                docIds.add(_numDocs);
                index.put(uniqueTermNum, docIds);


                termCorpusFrequency.put(uniqueTermNum,1);

//                String url = file.getAbsolutePath();
//                int urlId;
//                if (urlDictionary.containsKey(url)) {
//                    urlId=urlDictionary.get(url);
//                }
//                else {
//                    urlId = uniqueURLNum;
//                    uniqueURLNum++;
//                }
//                Map<Integer,Integer> map = new HashMap<Integer, Integer>();
//                map.put(urlId, 1);
//                documentTermFrequency.put(uniqueTermNum, map);

                uniqueTermNum++;

            }
            else {
                int termId = dictionary.get(term);
                ArrayList<Integer> docIds = index.get(termId);
                if (!docIds.contains(_numDocs)) {
                    docIds.add(_numDocs);
                    index.put(termId, docIds);
                }
                termCorpusFrequency.put(termId, termCorpusFrequency.get(termId) + 1);

//                Map<Integer,Integer> map = documentTermFrequency.get(termId);
//                String url = file.getAbsolutePath();
//                int urlId;
//                if(urlDictionary.containsKey(url)) {
//                    urlId = urlDictionary.get(url);
//                }
//                else {
//                    urlId=uniqueURLNum;
//                    uniqueURLNum++;
//                }
//                if (map.containsKey(urlId)) {
//                    map.put(urlId, map.get(urlId) + 1);
//                }
//                else {
//                    map.put(urlId, 1);
//                }
//                documentTermFrequency.put(termId, map);
            }
            _totalTermFrequency++;
        }
    }

    @Override
    public void loadIndex() throws IOException, ClassNotFoundException {
        String indexFile = _options._indexPrefix + "/corpus_docOnly.idx";
        System.out.println("Load index from: " + indexFile);

        ObjectInputStream reader =
                new ObjectInputStream(new FileInputStream(indexFile));
        IndexerInvertedDoconly loaded = (IndexerInvertedDoconly) reader.readObject();

        this.index = loaded.index;
        this._documents = loaded._documents;
        this._numDocs = _documents.size();
        this.documentTermFrequency = loaded.documentTermFrequency;

        for (Integer freq : loaded.termCorpusFrequency.values()) {
            this._totalTermFrequency += freq;
        }
        this.termCorpusFrequency = loaded.termCorpusFrequency;
        reader.close();

        System.out.println(Integer.toString(_numDocs) + " documents loaded " +
                "with " + Long.toString(_totalTermFrequency) + " terms!");

    }

    @Override
    public Document getDoc(int docid) {
        return (docid >= _documents.size() || docid < 0) ? null : _documents.get(docid);
    }

    /**
     * In HW2, you should be using {@link DocumentIndexed}
     */
    @Override
    //   public Document nextDoc(Query query, int docid) {
    //       return null;
    //   }

    public Document nextDoc(Query query, int docid) {
        Vector<String> tokens = query._tokens;
        int docidValue = nextDocHelper(tokens, docid);
        if(docidValue == -1){
            skipointers.clear();
            return null;
        }
        skipointers.clear();
        return _documents.get(docidValue);
    }

    public int nextDocHelper(Vector<String> tokens, int docID){
        ArrayList<Integer> docIDs = new ArrayList<Integer>();
        int docidValue =-1;
        int tokennumber = 0;
        for(String eachToken : tokens){
            skipointers.put(tokennumber,-1);
            docidValue = next(eachToken, tokennumber, docID);
            if(docidValue == -1){
                return -1;
            }
            docIDs.add(docidValue);
            tokennumber++;
        }

        if(Collections.frequency(docIDs,docIDs.get(0)) == docIDs.size()){
            return docIDs.get(0);
        }

        docidValue = max(docIDs);
        docidValue--;

        return nextDocHelper(tokens,docidValue);
    }

    public int max(ArrayList<Integer> param_docids){
        int max=0;
        for(int i=0;i<param_docids.size();i++){
            int temp =param_docids.get(i);
            if (temp>max){
                max = temp;
            }
        }
        return max;
    }

    public int next(String word, int tokennumber, int docId){
        // valueList is the list of docIDS for the word.
        List<Integer> keyList = index.get(word);
        int skipptr = skipointers.get(tokennumber);

        if(skipptr == -1){
            if(keyList.get(0)== docId){
                skipptr = (keyList.get(1)+2);
            }
            else{
                //find the next possible docID
                skipptr = (keyList.get(1) + 2);
                while(skipptr<keyList.size()) {
                    if (keyList.get(skipptr) > docId){
                        skipointers.put(tokennumber, skipptr);
                        break;
                    }
                    skipptr = (keyList.get(skipptr+1) + 2);
                }
                if(skipptr == (keyList.get(1) + 2)){
                    return -1;
                }
                return keyList.get(skipptr);
            }
        }
        skipptr = (keyList.get(skipptr+1) + 2);
        skipointers.put(tokennumber,skipptr);
        return keyList.get(skipptr);
    }

    @Override
    public int corpusDocFrequencyByTerm(String term) {
        if (dictionary.containsKey(term)) {
            int termid = dictionary.get(term);
            if (index.containsKey(termid)) {
                return index.get(termid).size();
            }
            return 0;
        }
        return 0;
    }

    @Override
    public int corpusTermFrequency(String term) {
        if (dictionary.containsKey(term)) {
            int termid = dictionary.get(term);
            if (termCorpusFrequency.containsKey(termid)) {
                return termCorpusFrequency.get(termid);
            }
            return 0;
        }
        return  0;
    }

    @Override
    public int documentTermFrequency(String term, String url) {
        if (dictionary.containsKey(term)) {
            int termid = dictionary.get(term);
            if (documentTermFrequency.containsKey(termid) ) {
                Map<Integer,Integer> map = documentTermFrequency.get(termid);
                if (urlDictionary.containsKey(url)) {
                    int urlid = urlDictionary.get(url);
                    if (map.containsKey(urlid)) {
                        return map.get(urlid);
                    }
                    return 0;
                }

                return 0;
            }
            return 0;
        }
        return 0;
    }
}