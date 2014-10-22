package edu.nyu.cs.cs2580;

import java.util.*;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2 based on a refactoring of your favorite
 * Ranker (except RankerPhrase) from HW1. The new Ranker should no longer rely
 * on the instructors' {@link IndexerFullScan}, instead it should use one of
 * your more efficient implementations.
 */
public class RankerFavorite extends Ranker {

  public RankerFavorite(Options options,
      CgiArguments arguments, Indexer indexer) {
    super(options, arguments, indexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

    @Override
    public Vector<ScoredDocument> runQuery(Query query, int numResults) {
        Document doc = null;
        ScoredDocument scoredDoc = null;
        int docId = -1;
        Vector<ScoredDocument> results = new Vector<ScoredDocument>();
        Queue<ScoredDocument> retrieval_results = new PriorityQueue<ScoredDocument>(numResults);
        System.out.println("Inside runQuery");
        while((doc = _indexer.nextDoc(query, docId)) != null) {

          retrieval_results.add(runqueryQL(query, doc._docid));

          if(numResults < retrieval_results.size()) {
              retrieval_results.poll();
          }
          docId = doc._docid;
        }

        while ((scoredDoc = retrieval_results.poll()) != null) {

          results.add(scoredDoc);
        }
        Collections.sort(results, Collections.reverseOrder());
        System.out.println("Results "+results);
        return results;
    }

    /**
     * Method for scoring documents based on QL
     * @param query the query words
     * @param did the document id
     * @return the scored document
     */
    public ScoredDocument runqueryQL(Query query, int did) {
        double lambda = 0.5d, score = 0.0d;
        DocumentIndexed d = (DocumentIndexed) _indexer.getDoc(did);
        Vector<String> qv = new Vector<String>();
        for(String str: query._tokens) {
            //Check the token for spaces and handle them accordingly
            String[] temp = str.split(" ");
            if(temp.length > 1) {
                for (String term : temp) {
                    qv.add(term);
                }
            } else {
                qv.add(str);
            }
        }

        for(String q: qv) {
            int docTerFreq, corpusTerFreq;
            long totWordsInDoc, totWordsInCorp;
            double cumulativeVal = 0.0d;
            docTerFreq = _indexer.documentTermFrequency(q, (d.getUrl()));
            corpusTerFreq = _indexer.corpusTermFrequency(q);
            totWordsInDoc = d.getNumberOfWords();
            totWordsInCorp =  _indexer.totalTermFrequency();

            if(totWordsInDoc != 0) {
                cumulativeVal += ((1-lambda) * ((double)docTerFreq/totWordsInDoc));
            }

            if (totWordsInCorp != 0) {
                cumulativeVal += ((lambda) *  ((double)corpusTerFreq/totWordsInCorp));
            }
            score += (Math.log(cumulativeVal)/Math.log(2));
        }
        return new ScoredDocument(d, Math.pow(2, score));
    }
}
