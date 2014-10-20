package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

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

        for (File file : listOfFiles) {
            if(fileCount==500) {
                System.out.println("Constructing Partial Index " + (int) Math.ceil(indexCount / 500));
                persist((int) Math.ceil(indexCount / 500));
                index.clear();
                fileCount = 0;
            }

            analyse(file, indexCount);

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
        //Document map
        StringBuilder builder = new StringBuilder(_options._indexPrefix).append("/").append("DocMap.tsv");
        BufferedWriter writer = new BufferedWriter(new FileWriter(builder.toString(), true));

        for(int document : _documents.keySet()) {
            DocumentIndexed docIndexed = _documents.get(document);
            writer.write((document + "\t"));
            writer.write(docIndexed.getTitle() + "\t" + docIndexed.getUrl() + "\t");
            writer.write(docIndexed.getNumberOfWords() +"");
            writer.newLine();
        }
        writer.close();
        //Clear all documents from the map after writing.
        _documents.clear();

        //Dictionary
        StringBuilder dictBuilder = new StringBuilder(_options._indexPrefix).append("/").append("Dictionary.tsv");
        BufferedWriter dictWriter = new BufferedWriter(new FileWriter(dictBuilder.toString(), true));

        Set<Entry<String, Integer>> dictEntrySet = dictionary.entrySet();
        for(Entry<String, Integer> dictEntry : dictEntrySet) {
            dictWriter.write(dictEntry.getKey()+"\t");
            dictWriter.write(dictEntry.getValue()+"\n");
        }
        dictWriter.newLine();
        dictWriter.close();
        //Clear the dictionary
        dictionary.clear();

        //Sort the index before making a partial index
        StringBuilder indexBuilder = new StringBuilder(_options._indexPrefix).append("/").append(fileCount+"tempIndex.tsv");
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

        while(indexIt.hasNext()) {

            Integer key = indexIt.next();

            indexWriter.write(key.toString());

            //Value for key
            ArrayList<Integer> indexVal = index.get(key);

            //Iterate over the document details
            // 1) First Value is the number of occurences
            // 2) Subsequent Values are the locations of each occurence in the document.
            int x=0;
            int skip=0;

           while(x <= indexVal.size()+1) {
                if(x == indexVal.size()){
                    break;
                }

                if(x==skip) {
                    indexWriter.write("\n");
                    //This is a docId
                    skip+=indexVal.get(x+1) + 2;
                }

               indexWriter.write(indexVal.get(x).toString()+"\t");
                x++;
            }
            indexWriter.write("\n");
        }
        indexWriter.close();

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

        //Read from DocMap.tsv
        StringBuilder docbuilder = new StringBuilder(_options._indexPrefix).append("/").append("DocMap.tsv");
        BufferedReader docMapReader = new BufferedReader(new FileReader(docbuilder.toString()));

        String line = null;

        while((line=docMapReader.readLine()) != null) {
            mergeWriter.write(line+"\n");
        }

        docMapReader.close();

        //Delimiter for next data structure in the index - 10 '#' symbols
        mergeWriter.write("##########\n");
        mergeWriter.write("##########\n");

        //Read from Dictionary.tsv
        StringBuilder dictbuilder = new StringBuilder(_options._indexPrefix).append("/").append("Dictionary.tsv");
        BufferedReader dictReader = new BufferedReader(new FileReader(dictbuilder.toString()));

        while((line=dictReader.readLine()) != null) {
            mergeWriter.write(line+"\n");
        }

        dictReader.close();

        mergeWriter.close();

        //deleteTempFiles();
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
                secondline=firstReader.readLine();
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
                        firstlist = stringTokenizer(firstline);
                    }

                    secondline = secondReader.readLine();
                    secondlist = stringTokenizer(secondline);
                    while(secondlist.size()>1) {
                        mergeWriter.write(secondline+"\n");
                        secondline = secondReader.readLine();
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
        StringTokenizer st = new StringTokenizer(str, "\t");
        while (st.hasMoreElements()) {
            tokenList.add(st.nextElement().toString());
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
                    int termId = dictionary.get(term);
                    ArrayList<Integer> occurrence = index.get(termId);

                    int latestPosition = trackerList.get(termId);

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
            while (line != null) {
                if(line.equals("##########")) {
                    globalIndexCount=0;
                    hashcount++;
                    if(hashcount==1) {
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
                    } else if(hashcount==2) {
                        loadDictionary(br);
                    }
                } else if(!indexDone) {
                    int forwardCount = 0;
                    while(line!=null && loadCache && forwardCount <= globalIndexCount) {
                        System.out.println("Inside the forward loop "+forwardCount);
                        br.readLine();
                        forwardCount++;
                    }

                    //Always in else to load the index first
                    List<String> stringList = stringTokenizer(line);
                    if(stringList.size() == 1) {
                        if(cacheCount!=0) {
                            //System.out.println("Key is " + key + " value is "+value.get(0));
                            index.put(new Integer(key), value);
                        }
                        if(cacheCount==500) {
                            //System.out.println("Key is " + key + " value is "+value.get(0));
                            index.put(new Integer(key), value);
                            System.out.println("Index size is " + index.size());
                            loadCache = false;
                            indexDone = true;
                        }
                        key = Integer.parseInt(stringList.get(0));

                        cacheCount++;
                        globalIndexCount++;
                    } else {
                        for(String s: stringList) {
                            value.add(Integer.parseInt(s));
                        }
                    }
                }
                line = br.readLine();
            }

        }catch(Exception e){
            e.printStackTrace();
            System.out.println(e);
        }


    }

    public void loadToCache() throws IOException, ClassNotFoundException {
        loadCache = true;
        loadIndex();
    }

    public void loadDictionary(BufferedReader br) throws IOException, ClassNotFoundException {
        String dictionaryString = br.readLine();
        while (dictionaryString != null) {
            List<String> dictList = stringTokenizer(dictionaryString);
            if(dictList.size()==2) {
                dictionary.put(dictList.get(0), Integer.parseInt(dictList.get(1)));
            }
            dictionaryString = br.readLine();
        }

    }

    @Override
    public Document getDoc(int docid) {
        return null;
    }

    /**
     * In HW2, you should be using {@link DocumentIndexed}.
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