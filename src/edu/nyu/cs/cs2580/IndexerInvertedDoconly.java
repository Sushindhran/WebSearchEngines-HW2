package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer {

    private HashMap<String, HashSet<Integer>> index;
    private Vector<Document> _documents = new Vector<Document>();

    int numDocs=0;

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
              updateIndex(title);
              String body = s.next();
              updateIndex(body);

              DocumentIndexed documentIndexed = new DocumentIndexed(numDocs);
              documentIndexed.setTitle(title);
              _documents.add(documentIndexed);
              numDocs++;
          }
      }

      String indexFile = _options._indexPrefix + "/corpus_docOnly.idx";
      System.out.println("Store index to: " + indexFile);
      ObjectOutputStream writer =
              new ObjectOutputStream(new FileOutputStream(indexFile));
      writer.writeObject(this);
      writer.close();

  }

    private void updateIndex(String document) {
        String[] words = document.split(" ");
        PorterStemming porterStemming = new PorterStemming();
        for (int i=0; i<words.length; i++) {
            String lower = words[i].toLowerCase();
            porterStemming.add(lower.toCharArray(), lower.length());
            porterStemming.stem();
            String term = porterStemming.toString();
            if (index.containsKey(term)) {
                HashSet<Integer> docIds = index.get(term);
                docIds.add(numDocs);
                index.put(term, docIds);
            }
            else {
                HashSet<Integer> docIds = new HashSet<Integer>();
                docIds.add(numDocs);
                index.put(term, docIds);
            }
        }
    }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
  }

  @Override
  public Document getDoc(int docid) {
    return null;
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}
   */
  @Override
  public Document nextDoc(Query query, int docid) {
    return null;
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    return 0;
  }

  @Override
  public int corpusTermFrequency(String term) {
    return 0;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }
}
