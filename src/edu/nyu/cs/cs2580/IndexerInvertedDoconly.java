package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer {

    //USE TREEEE MAP

    private HashMap<String, HashSet<Integer>> index =
            new HashMap<String, HashSet<Integer>>();
    private Vector<Document> _documents =
            new Vector<Document>();
    private Map<String, Integer> _termCorpusFrequency =
            new HashMap<String, Integer>();
    private Map<String, Map<String, Integer>> documentTermFrequency
            = new HashMap<String, Map<String, Integer>>();

    public IndexerInvertedDoconly(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
        index = new HashMap<String, HashSet<Integer>>();
  }

  @Override
  public void constructIndex() throws IOException {
      String corpusFile = _options._corpusPrefix;
      File folder = new File(corpusFile);
      File[] listOfFiles = folder.listFiles();

      for (File file : listOfFiles) {
          if (file.isFile()) {
              String content = HtmlParser.parseFile(file);

              Scanner s = new Scanner(content).useDelimiter("\t");
              String title = s.next();
              updateIndex(title, file);
              String body = s.next();
              updateIndex(body, file);

              DocumentIndexed documentIndexed = new DocumentIndexed(_numDocs);
              documentIndexed.setTitle(title);
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
        PorterStemming porterStemming = new PorterStemming();
        for (int i=0; i<words.length; i++) {
            String lower = words[i].toLowerCase();
            porterStemming.add(lower.toCharArray(), lower.length());
            porterStemming.stem();
            String term = porterStemming.toString();
            if (index.containsKey(term)) {
                HashSet<Integer> docIds = index.get(term);
                docIds.add(_numDocs);
                index.put(term, docIds);
                _termCorpusFrequency.put(term, _termCorpusFrequency.get(term) + 1);

                Map<String,Integer> map = documentTermFrequency.get(term);
                String url = file.getAbsolutePath();
                if (map.containsKey(url)) {
                    map.put(url, map.get(url) + 1);
                }
                else {
                    map.put(url, 1);
                }
                documentTermFrequency.put(term, map);
            }
            else {
                HashSet<Integer> docIds = new HashSet<Integer>();
                docIds.add(_numDocs);
                index.put(term, docIds);
                _termCorpusFrequency.put(term, 1);

                Map<String,Integer> map = new HashMap<String, Integer>();
                map.put(file.getAbsolutePath(), 1);
                documentTermFrequency.put(term, map);
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

      for (Integer freq : loaded._termCorpusFrequency.values()) {
          this._totalTermFrequency += freq;
      }
      this._termCorpusFrequency = loaded._termCorpusFrequency;
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
  public Document nextDoc(Query query, int docID) {
      Vector<String> tokens = query._tokens;
    int docidValue = nextDocHelper(tokens, docID);
      if(docidValue == -1){
          return null;
      }
    return _documents.get(docidValue);
  }

   public int nextDocHelper(Vector<String> tokens, int docID){
       Set<Integer> docIDs = new HashSet<Integer>();
       int docidValue =-1;
       for(String eachToken : tokens){
           docidValue = next(eachToken, docID);
           if(docidValue == -1){
               return -1;
           }
           docIDs.add(docidValue);
       }

       if(docIDs.size()==1){
           return docIDs.iterator().next();
       }

       docidValue = max(docIDs);
       docidValue--;

       return nextDocHelper(tokens,docidValue);
   }

   public int max(Set<Integer> param_docids){
       int max=0;
       for(int i=0;i<param_docids.size();i++){
           int temp =param_docids.iterator().next();
           if (temp>max){
               max = temp;
           }
       }
       return max;
   }

   public int next(String word, int docId){
       Set<Integer> valueList = index.get(word);
       List<Integer> keyList = (ArrayList) valueList;
       Collections.sort(keyList);
       int position = keyList.indexOf(docId) + 1;
       return keyList.get(position);
   }


  @Override
  public int corpusDocFrequencyByTerm(String term) {
      if (index.containsKey(term)) {
          return index.get(term).size();
      }
      return 0;
  }

  @Override
  public int corpusTermFrequency(String term) {
      if (_termCorpusFrequency.containsKey(term)) {
          return  _termCorpusFrequency.get(term);
      }
      return  0;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
      if (documentTermFrequency.containsKey(term) ) {
          Map<String,Integer> map = documentTermFrequency.get(term);
          if (map.containsKey(url)) {
              return map.get(url);
          }
          return 0;
      }
    return 0;
  }
}
