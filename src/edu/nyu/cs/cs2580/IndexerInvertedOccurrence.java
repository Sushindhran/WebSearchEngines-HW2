package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer implements Serializable{

    private static final long serialVersionUID = 109213905740085030L;
    //Contains all documents
    private Map<Integer, DocumentIndexed> _documents = new HashMap<Integer, DocumentIndexed>();
    //Dictionary that contains all the terms and a termId
    private Map<String, Integer> dictionary = new HashMap<String, Integer>();
    //The index uses the termId as key and an array list of the occurences as value
    private Map<Integer, ArrayList<Integer>> index = new HashMap<Integer, ArrayList<Integer>>();
    //Tracker list that holds the latest position of occurrence inserted in the map.
    private ArrayList<Integer> trackerList = new ArrayList<Integer>();

    private int partialFileCount = 0;
    int uniqueTermNum = 0;
    private int globalIndexCount = 0;
    private boolean loadCache = false;
    private boolean termFreqLoaded = false;

    public IndexerInvertedOccurrence(Options options) {
        super(options);
        System.out.println("Using Indexer: " + this.getClass().getSimpleName());
    }

    @Override
    public void constructIndex() throws IOException {
        int fileCount=0, indexCount=1;
        File corpusFolder = new File(_options._corpusPrefix);
        File[] listOfFiles = corpusFolder.listFiles();
        _numDocs = listOfFiles.length;
        //Ensuring that the map is clear
        index.clear();
        System.out.print(listOfFiles.length);
        for (File file : listOfFiles) {

            if(fileCount==500) {
                System.out.println("Constructing Partial Index" + (int) Math.ceil(indexCount / 500));
                persist((int) Math.ceil(indexCount / 500));
                index.clear();
                fileCount = 0;
            }
            try {
                analyse(file, indexCount);
            }catch (Exception e) {
                e.printStackTrace();
            }
            indexCount++;
            fileCount++;
        }
        persist((int)Math.floor(indexCount/500)+1);
        System.out.println("Constructing Partial Index " + (int)(Math.floor(indexCount/500)+1.0));
        index.clear();

        System.out.println("Merging all partial Indexes");

        //Merging all files into one index
        mergeAllFiles();
    }

    private void analyse(File file, int indexCount) {
        DocumentIndexed documentIndexed = new DocumentIndexed(indexCount);
        if(file.isFile()) {
            documentIndexed.setTitle(file.getName());
            documentIndexed.setUrl(file.getPath());
            String content = HtmlParser.parseFile(file);

            Scanner s = new Scanner(content).useDelimiter("\t");

            String title = "";
            if (s.hasNext()) {
                title = s.next();
            }

            String body="";
            if(s.hasNext()) {
                body = s.next();
            }

            //Sring buffer that contains the document content
            StringBuffer sb = new StringBuffer();
            sb.append(title);
            sb.append(" ");
            sb.append(body);
            int numWords = updateIndex(sb.toString().trim(), indexCount);
            documentIndexed.setNumberOfWords(numWords);
            _documents.put(indexCount, documentIndexed);
        }
    }


    /* This function persists three data structures whenever called and clears them.
     * 1) Document map in tsv format
     * 2) Dictionary
     * 3) Index is tsv format
     */

    private void persist(int fileCount) throws IOException {
        partialFileCount++;
        try {
            //Sort the index before making a partial index
            StringBuilder indexBuilder = new StringBuilder(_options._indexPrefix).append("/").append(fileCount + "tempIndex.tsv");
            BufferedWriter indexWriter = new BufferedWriter(new FileWriter(indexBuilder.toString(), true));

        /* The Index is saved as follows in tsv format
         * Col1 : TermId
         * Col2 : List of Documents and corresponding values-separated by a space.
         */

            Set<Integer> indexKeys = index.keySet();

            //Convert to list to sort
            List<Integer> indexKeysList = new ArrayList<Integer>();
            indexKeysList.addAll(indexKeys);
            Collections.sort(indexKeysList);

            //Iterate over the sorted keyList
            Iterator<Integer> indexIt = indexKeysList.iterator();

            while (indexIt.hasNext()) {


                Integer key = indexIt.next();
                indexWriter.write(key.toString());

                //Value for key
                ArrayList<Integer> indexVal = index.get(key);

                //Iterate over the document details
                // 1) First Value is the number of occurences
                // 2) Subsequent Values are the locations of each occurence in the document.
                int x = 0;
                int skip = 0;

                while (x <= indexVal.size() + 1) {
                    if (x == indexVal.size()) {
                        break;
                    }

                    if (x == skip) {
                        indexWriter.write("\n");
                        //This is a docId
                        skip += indexVal.get(x + 1) + 2;
                    }

                    indexWriter.write(indexVal.get(x).toString() + "\t");
                    x++;
                }
                indexWriter.write("\n");
            }
            indexWriter.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        //Clear index
        index.clear();
    }

    private void mergeAllFiles() throws IOException {
        String indexFile = "/invertedIndexOccurrence.tsv";
        mergeIndexFiles();
        StringBuilder mergebuilder = new StringBuilder(_options._indexPrefix).append(indexFile);
        BufferedWriter mergeWriter = new BufferedWriter(new FileWriter(mergebuilder.toString(), true));



        //Delimiter for next data structure in the index - 10 '#' symbols
        mergeWriter.write("##########\n");
        mergeWriter.write(Long.toString(_totalTermFrequency)+"\n");
        //Document map

        for(int document : _documents.keySet()) {
            DocumentIndexed docIndexed = _documents.get(document);
            mergeWriter.write((document + "\t"));
            mergeWriter.write(docIndexed.getTitle() + "\t" + docIndexed.getUrl() + "\t");
            mergeWriter.write(docIndexed.getNumberOfWords()+"");
            mergeWriter.newLine();
        }
        //Clear all documents from the map after writing.
        _documents.clear();

        //Delimiter for next data structure in the index - 10 '#' symbols
        mergeWriter.write("##########\n");
        mergeWriter.write("##########\n");

        //Dictionary
        Set<Entry<String, Integer>> dictEntrySet = dictionary.entrySet();
        for(Entry<String, Integer> dictEntry : dictEntrySet) {
            mergeWriter.write(dictEntry.getKey()+"\t");
            mergeWriter.write(dictEntry.getValue()+"\n");
        }
        mergeWriter.newLine();
        mergeWriter.close();
        //Clear the dictionary
        dictionary.clear();

        mergeWriter.close();

        deleteTempFiles();
    }

    private void deleteTempFiles() throws  IOException {
        String finalIndexFile = "invertedIndexOccurrence.tsv";
        File indexFolder = new File(_options._indexPrefix);
        File[] listOfFiles = indexFolder.listFiles();

        //Delete ever file except invertedIndexOccurence.tsv

        for(File eachFile : listOfFiles) {
            if(!eachFile.getName().equals(finalIndexFile)) {
                eachFile.delete();
            }
        }
    }

    private void mergeIndexFiles() throws IOException {
        String indexFile = "/invertedIndexOccurrence.tsv";
        mergeTwoFiles("/1tempIndex.tsv", "/2tempIndex.tsv");

        for(int i=3; i<=partialFileCount; i++) {
            File oldFile = new File(_options._indexPrefix+"/temp.tsv");
            File newFile = new File(_options._indexPrefix+"/first.tsv");
            oldFile.renameTo(newFile);
            try {
                mergeTwoFiles("first.tsv", i + "tempIndex.tsv");
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        File oldFile = new File(_options._indexPrefix+"/temp.tsv");
        File newFile = new File(_options._indexPrefix+indexFile);
        oldFile.renameTo(newFile);
    }

    private void mergeTwoFiles(String firstFile, String secondFile) throws IOException {

        try{
            StringBuilder mergebuilder = new StringBuilder(_options._indexPrefix).append("/temp.tsv");
            BufferedWriter mergeWriter = new BufferedWriter(new FileWriter(mergebuilder.toString(), true));

            StringBuilder firstbuilder = new StringBuilder(_options._indexPrefix).append("/"+firstFile);
            BufferedReader firstReader = new BufferedReader(new FileReader(firstbuilder.toString()));

            StringBuilder secondbuilder = new StringBuilder(_options._indexPrefix).append("/"+secondFile);
            BufferedReader secondReader = new BufferedReader(new FileReader(secondbuilder.toString()));

            if(firstFile ==null || firstFile ==".DS_Store" || firstFile == "DocMap.tsv" || firstFile == "Dictionary.tsv") {
                File oldFile = new File(_options._indexPrefix + "/" + secondFile);
                File newFile = new File(_options._indexPrefix+"/temp.tsv");
                oldFile.renameTo(newFile);
                mergeWriter.close();
                firstReader.close();
                secondReader.close();
                return;
            } else if(secondFile == null || secondFile ==".DS_Store" || firstFile == "DocMap.tsv" || firstFile == "Dictionary.tsv") {
                File oldFile = new File(_options._indexPrefix + "/" + firstFile);
                File newFile = new File(_options._indexPrefix+"/temp.tsv");
                oldFile.renameTo(newFile);
                mergeWriter.close();
                firstReader.close();
                secondReader.close();
                return;
            }

            String firstline = firstReader.readLine(), secondline = secondReader.readLine();

            int prevTermId = -1;
            while((secondline != null) && (firstline != null)) {
                List<String> secondlist = null, firstlist=null;
                if(firstline != null) {
                    firstlist = stringTokenizer(firstline);
                } else {
                    mergeWriter.write(secondline);
                    secondline=secondReader.readLine();
                    continue;
                }

                if(secondline != null) {
                    secondlist = stringTokenizer(secondline);
                } else {
                    mergeWriter.write(firstline);
                    firstline=firstReader.readLine();
                    continue;
                }

                if(firstlist.size() == 1 || secondlist.size() ==1 ) {
                    if(Integer.parseInt(firstlist.get(0))>Integer.parseInt(secondlist.get(0))
                            && (Integer.parseInt(secondlist.get(0))>prevTermId)) {

                        mergeWriter.write(secondlist.get(0)+"\n");
                        secondline = secondReader.readLine();
                        secondlist = stringTokenizer(secondline);
                        while(secondlist.size()>1) {
                            mergeWriter.write(secondline+"\n");
                            secondline = secondReader.readLine();
                            secondlist = stringTokenizer(secondline);
                        }
                    } else if (Integer.parseInt(firstlist.get(0)) < Integer.parseInt(secondlist.get(0))) {
                        prevTermId = Integer.parseInt(firstlist.get(0));
                        mergeWriter.write(firstlist.get(0)+"\n");
                        firstline = firstReader.readLine();
                        firstlist = stringTokenizer(firstline);
                        String check = firstline;
                        while(firstlist.size()>1 && (firstline = firstReader.readLine())!=null) {
                            mergeWriter.write(check+"\n");
                            check = firstline;
                            firstlist = stringTokenizer(firstline);
                        }

                    } else {
                        mergeWriter.write(firstlist.get(0)+"\n");
                        firstline = firstReader.readLine();
                        firstlist = stringTokenizer(firstline);
                        while(firstlist.size()>1) {
                            mergeWriter.write(firstline+"\n");
                            firstline = firstReader.readLine();
                            if(firstline==null) {
                                break;
                            }
                            firstlist = stringTokenizer(firstline);
                        }

                        secondline = secondReader.readLine();
                        secondlist = stringTokenizer(secondline);
                        while(secondlist.size()>1) {
                            mergeWriter.write(secondline+"\n");
                            secondline = secondReader.readLine();
                            if(secondline==null) {
                                break;
                            }
                            secondlist = stringTokenizer(secondline);
                        }
                    }
                }
            }

            if(firstline!=null) {
                while(firstline!=null) {
                    mergeWriter.write(firstline+"\n");
                    firstline=firstReader.readLine();
                }
            }

            if(secondline!=null) {
                while(secondline!=null) {
                    mergeWriter.write(secondline+"\n");
                    secondline=secondReader.readLine();
                }
            }

            mergeWriter.close();
            firstReader.close();
            secondReader.close();
        }catch(IOException e)
        {
            e.printStackTrace();
        }
    }

    private List<String> stringTokenizer(String str) {
        List<String> tokenList = new ArrayList<String>();
        try {
            StringTokenizer st = new StringTokenizer(str, "\t");
            while (st.hasMoreElements()) {
                tokenList.add(st.nextElement().toString());
            }
        }catch (Exception e) {
            //IT is null
        }
        return tokenList;
    }

    private int updateIndex(String document, int indexCount) {
        String[] words = document.split(" ");
        for (int i=0; i<words.length; i++) {
            String lower = words[i].toLowerCase();
            lower.replace("\""," ").trim();
            Vector<String> stopWords = new StopWords().getStopWords();
            String term = PorterStemming.getStemmedWord(lower);
            if(!stopWords.contains(term) && term != " " && term.length()>1) {
                //System.out.print("In updateIndex: Dictionary size "+dictionary.size());
                if (!dictionary.containsKey(term)) {
                    dictionary.put(term, uniqueTermNum);

                    ArrayList<Integer> occurrence = new ArrayList<Integer>();
                    occurrence.add(indexCount);
                    occurrence.add(1);
                    occurrence.add(i);
                    index.put(uniqueTermNum, occurrence);

                    trackerList.add(0);
                    uniqueTermNum++;
                } else {
                    //System.out.print("In updateIndex: Dictionary size "+dictionary.size());
                    int termId = dictionary.get(term);
                    int latestPosition = trackerList.get(termId);

                    if(!index.containsKey(termId)) {
                        ArrayList<Integer> occurrence = new ArrayList<Integer>();
                        occurrence.add(indexCount);
                        occurrence.add(1);
                        occurrence.add(i);
                        index.put(termId, occurrence);
                        trackerList.set(termId, 0);
                    } else {
                        ArrayList<Integer> occurrence = index.get(termId);
                        if (occurrence.get(latestPosition) == indexCount) {
                            occurrence.set(latestPosition + 1, occurrence.get(latestPosition + 1) + 1);
                            occurrence.add(i);
                        } else {
                            occurrence.add(indexCount);
                            latestPosition = occurrence.size() - 1;
                            trackerList.set(termId, latestPosition);
                            occurrence.add(1);
                            occurrence.add(i);
                        }
                    }
                }
                _totalTermFrequency++;
            }
        }
        return words.length;
    }

    @Override
    public void loadIndex() throws IOException, ClassNotFoundException {
        int cacheCount = 0;
        int key = 0;
        ArrayList<Integer> value = new ArrayList<Integer>();

        System.out.println("Loading Index ");
        StringBuilder builder = new StringBuilder(_options._indexPrefix).append("/").append("invertedIndexOccurrence.tsv");
        try
        {
            FileInputStream in = new FileInputStream(builder.toString());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            boolean indexDone = false;
            String line = br.readLine();
            int hashcount = 0;
            if(loadCache) {
                int forwardCount = 0;
                while(line!=null && line!="##########" && forwardCount<globalIndexCount) {
                    List<String> stringList = stringTokenizer(line);
                    if(stringList.size() == 1) {
                        forwardCount++;
                    }
                    line=br.readLine();
                }

            }
            //int c=0;
            while (line != null) {
                //if(c==184) break;
                if(line.equals("##########") && !loadCache) {
                    hashcount++;
                    if(hashcount==1) {
                        if(!termFreqLoaded) {
                            line = br.readLine();
                            _totalTermFrequency= Long.parseLong(line);
                            termFreqLoaded = true;
                            System.out.println("Total term frequency "+_totalTermFrequency);
                        }
                        System.out.println("Loading Document map");
                        while(line!=null) {
                            line=br.readLine();
                            if(line.equals("##########")) {
                                break;
                            }
                            Scanner scanner = new Scanner(line).useDelimiter("\t");
                            while (scanner.hasNext()) {
                                int docid = Integer.parseInt(scanner.next());
                                DocumentIndexed documentIndexed = new DocumentIndexed(docid);
                                documentIndexed.setTitle(scanner.next());
                                documentIndexed.setUrl(scanner.next());
                                documentIndexed.setNumberOfWords(Long.parseLong(scanner.next()));
                                _documents.put(docid, documentIndexed);
                            }
                        }
                        System.out.println("Loaded "+_documents.size()+" documents.");
                    } else if(hashcount==2) {
                        loadDictionary(br);
                    }
                } else if(!indexDone) {
                    // System.out.println("Here");
                    if(line.equals("##########")) {
                        System.out.println("Index size at the end is "+index.size());
                        //globalIndexCount = 0;
                        indexDone=true;
                        //loadCache = false;
                        break;
                    }
                    //Always in else to load the index first
                    List<String> stringList = stringTokenizer(line);
                    if(stringList.size() == 1) {
                        if(cacheCount!=0) {
                            //System.out.println("Key is " + key + " value is "+value.get(0));
                            index.put(new Integer(key), value);
                            if(key==1711131) {
                                System.out.println("For web in load index "+key +" "+value);
                            }
                            value = new ArrayList<Integer>();

                        }

                        if(cacheCount==500) {
                            //System.out.println("Key is " + key + " value is "+value.get(0));
                            index.put(new Integer(key), value);
                            value = new ArrayList<Integer>();
                            //System.out.println("Current index size is " + index.size());
                            if(globalIndexCount<500) {
                                loadCache = false;
                            }
                            globalIndexCount--;
                            indexDone = true;
                        }

                        key = Integer.parseInt(stringList.get(0));

                        cacheCount++;
                        globalIndexCount++;
                    } else {

                        for(String s: stringList) {
                            if(key==1711131) {
                                System.out.println("For web in load index "+stringList.size() +" "+s);
                            }
                            value.add(Integer.parseInt(s));
                        }
                    }
                }
                line = br.readLine();
                //c++;
            }
            br.close();
            in.close();
        }catch(Exception e){
            e.printStackTrace();
            System.out.println(e);
        }
    }

    public void loadToCache() throws IOException, ClassNotFoundException {
        loadCache = true;
        //Clear before loading into cache again.
        index.clear();
        loadIndex();
    }

    public void loadDictionary(BufferedReader br) throws IOException, ClassNotFoundException {
        System.out.println("Loading dictionary");
        String dictionaryString = br.readLine();
        int count = 0;
        while (dictionaryString != null) {
            List<String> dictList = stringTokenizer(dictionaryString);
            if(dictList.size()==2) {
                dictionary.put(dictList.get(0), Integer.parseInt(dictList.get(1)));
            }
            dictionaryString = br.readLine();
        }
        System.out.println("Dictionary loaded from index "+ dictionary.size()+ " terms");
    }

    public boolean checkIndexForTerm(int termId) {
        while(globalIndexCount!=0) {

            if (index.containsKey(termId)) {
                return true;
            } else {
                if(globalIndexCount>dictionary.size()){
                    globalIndexCount = 0;
                    System.out.println("Exiting and returning false "+ globalIndexCount);
                    return false;
                }
                try{
                    System.out.println("Before Loading "+ globalIndexCount);
                    loadToCache();
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public List<Integer> getTerm(int termId) {
        if(checkIndexForTerm(termId)) {
            return index.get(termId);
        } else {
            return null;
        }
    }

    @Override
    public Document getDoc(int docid) {
        if (_documents.containsKey(docid)) {
            return _documents.get(docid);
        }
        return null;
    }

    /**
     * In HW2, you should be using {@link DocumentIndexed}.
     */
    @Override
    public Document nextDoc(Query query, int docid) {
        if(query instanceof QueryPhrase){
            return nextDocForPhrase(query, docid);
        }
        else{
            return nextDocForSimple(query, docid);
        }
    }

    public Document nextDocForSimple(Query query, int docid) {
        int maxDocId = 0;
        int start = 1, end = 1;
        for(String q: query._tokens){
            if(docid == -1) {
            }
            int nextDocId = next(q,docid);

            int termId = dictionary.get(q);

            List<Integer> docOccLocList = getTerm(termId);

            if(nextDocId > docOccLocList.get(docOccLocList.size()-2)){
                return null;
            }
            if( nextDocId < 0){
                return null;
            }
            if(start ==0 && maxDocId != nextDocId){
                end = 0;
                if(maxDocId < nextDocId){
                    maxDocId = nextDocId;
                }
            }
            else if(start == 1){
                maxDocId = nextDocId;
                start = 0;
            }
        }
        if( end ==1 ){
            return _documents.get(maxDocId);
        }
        return nextDocForSimple(query, maxDocId - 1);
    }

    public int next(String word, int docId){
        if( (word == null) || (word.trim().length() == 0) ){
            return -1;
        }
        int termId = dictionary.get(word);
        if(termId < 0){
            return -1;
        }
        // valueList is the list of docIDS for the word.
        List<Integer> docOccLocList = getTerm(termId);

        if(docId == -1) {
            docId = docOccLocList.get(0);
        }


        int location = -1;
        int i;
        for(i=0; i < docOccLocList.size(); ){
            if(docOccLocList.get(i) == docId){
                location = i;
                break;
            }
            else{
                i = i + docOccLocList.get(i+1) + 2;
            }
        }
        if(location == -1){
            return -1;
        } else if(location == 0) {
            docOccLocList.get(location);
        } else if(i>=docOccLocList.size()) {
            return -1;
        }
        location = location +  docOccLocList.get(location+1) + 2 ;
        return docOccLocList.get(location);
    }

    private DocumentIndexed nextDocForPhrase(Query query, int docid){
        Set<Integer> checkSet = new HashSet<Integer>();
        DocumentIndexed phraseDoc = (DocumentIndexed)nextDocForSimple(query, docid);
        if(phraseDoc == null){
            return null;
        }
        boolean phraseExists = Boolean.FALSE;
        if(query._tokens.size() > 1) {
            if (!dictionary.containsKey(query._tokens.iterator().next())) {
                return null;
            }
            Iterator it = query._tokens.iterator();
            int maxDocId = 0;
            boolean first = true;
            boolean wholePhrase = false;
            while (it.hasNext()) {
                wholePhrase = false;
                int nextDocId = next(it.next().toString(), docid);
                if (nextDocId < 0) {
                    return null;
                }
                if (first) {
                    maxDocId = nextDocId;
                    first = false;
                }
                if (nextDocId != maxDocId) {
                    if (maxDocId < nextDocId) {
                        maxDocId = nextDocId;
                    }
                    return nextDocForPhrase(query, maxDocId - 1);
                } else {
                    wholePhrase = true;
                }
            }
            if (wholePhrase) {
                Iterator it2 = query._tokens.iterator();
                int termId = dictionary.get(it2.next());
                List<Integer> docOccLocList = getTerm(termId);
                int location = -1;
                for (int i = 0; i < docOccLocList.size(); ) {
                    if (docOccLocList.get(i) == maxDocId) {
                        location = i;
                    } else {
                        i = i + docOccLocList.get(i + 1) + 2;
                    }
                }
                if (location == -1) {
                    return null;
                }

                for (int i = 0; i < docOccLocList.get(location + 1); i++) {
                    checkSet.add(docOccLocList.get(location + 2 + i) + 1);
                }

                int counter = -1;

                boolean tokenpresent = false;
                while (it2.hasNext()) {
                    counter++;
                    int termId2 = dictionary.get(it2.next().toString());
                    List<Integer> docOccLocList2 = getTerm(termId2);
                    int location2 = -1;
                    for (int i = 0; i < docOccLocList2.size(); ) {
                        if (docOccLocList2.get(i) == maxDocId) {
                            location2 = i;
                        } else {
                            i = i + docOccLocList2.get(i + 1) + 2;
                        }
                    }
                    if (location2 == -1) {
                        return null;
                    }

                    for (int i = 0; i < docOccLocList2.get(location2 + 1); i++) {
                        if (checkSet.contains(docOccLocList2.get(location2 + 2 + i) + counter)) {
                            tokenpresent = true;
                            if (i == docOccLocList2.get(location2 + 1) - 1) {
                                break;
                            }
                        }
                    }

                    if (tokenpresent) {
                        tokenpresent = false;
                        continue;
                    } else {
                        return nextDocForPhrase(query, maxDocId - 1);
                    }

                }

                if (tokenpresent) {
                    return _documents.get(maxDocId);
                }
            } else {
                return nextDocForPhrase(query, maxDocId - 1);
            }
        }
        return phraseDoc;
    }

    @Override
    public int corpusDocFrequencyByTerm(String term) {

        if (dictionary.containsKey(term)) {
            int termid = dictionary.get(term);
            if (index.containsKey(termid) || checkIndexForTerm(termid)) {
                List<Integer> occurrence = index.get(termid);
                int documentCount = 0;
                int i=0;
                while (i < occurrence.size()) {
                    i=i+occurrence.get(i+1)+1;
                    documentCount = documentCount + 1;
                }
                return documentCount;
            }
            return 0;
        }
        return 0;
    }


    @Override
    public int corpusTermFrequency(String term) {
        if (dictionary.containsKey(term)) {
            int termid = dictionary.get(term);
            if (index.containsKey(termid) || checkIndexForTerm(termid)) {
                List<Integer> occurrence = index.get(termid);
                int corpusTermFreq = 0;
                int i=0;
                while (i < occurrence.size()) {
                    corpusTermFreq = corpusTermFreq + occurrence.get(i+1);
                    i=i+occurrence.get(i+1)+2;
                }
                return corpusTermFreq;
            }
            return 0;
        }
        return 0;
    }

    @Override
    public int documentTermFrequency(String term, String url) {
        int documentId = -1;
        for (int docid : _documents.keySet()) {
            if (_documents.get(docid).getUrl().equals(url)) {
                documentId = docid;
            }
        }
        if (documentId == -1) {
            return 0;
        }

        if (dictionary.containsKey(term)) {
            int termid = dictionary.get(term);
            if (index.containsKey(termid) || checkIndexForTerm(termid)) {
                List<Integer> occurrence = index.get(termid);
                int docTermFreq = 0;
                int i = 0;
                while (i < occurrence.size()) {
                    if (documentId == occurrence.get(i)) {
                        docTermFreq = occurrence.get(i + 1);
                        break;
                    }
                    i = i + occurrence.get(i + 1) + 2;
                }
                return docTermFreq;
            }
            return 0;
        }
        return 0;
    }

    public static void main(String args[]) {
        try {
            IndexerInvertedOccurrence ind = new IndexerInvertedOccurrence(new Options("conf/engine.conf"));
            ind.loadIndex();
            int termId = ind.dictionary.get("web");
            //System.out.println(ind.index.get(1711131));
            boolean result = ind.checkIndexForTerm(termId);
            System.out.println(result);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}