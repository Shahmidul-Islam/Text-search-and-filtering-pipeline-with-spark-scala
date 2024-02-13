# Text-search-and-filtering-pipeline-with-spark-scala

**Context:**

The goal of this task is to create a text search and filtering pipeline that can efficiently rank
documents for each query in a list. The documents and queries are both pre-processed to
remove stop-words. A series of transformation functions were performed to convert the
Query and News Article datasets to a Document Ranking list which lists the top 10 documents
for each query. The documents are filtered to remove overly similar documents before
displaying the results.

**Program Logic Summary**

The overall program performs a ranking of documents based on the DPH Scoring function. Its
main parameters include (i) term frequency in current document, (ii) current document
length, (iii) total document length in corpus, (iv) average document length in corpus and (v)
total term frequency in corpus. The initial process is a flatmap function QueryParserMap that
maps a Query to NewsArticle to produce the corresponding term frequencies stored in
QueryParser structure. A map function called QueryFilter is then used to produce a key (query)
on the QueryParser which is used in subsequent mapGroups functions. The
CollectionFrequency mapgroups function is used to calculate the total term frequencies in
corpus. Accumulators are then used to keep track of total document length. Once all the
components for DPH Scoring are available, a DocumentScorer MapGroups Function is used to
calculate the score of documents per query and it returns a list of filtered ranked documents.

**QueryParserMap** : This is a flatmap function used to map NewsArticle objects to query objects.
The queries are passed as broadcast variables (Pointer to the queries) into the flatmap
function which produces a new data type called QueryParser. This structure holds a query,
newsarticle, termFrequency and documentLength. The totalDocument length in corpus is
passed as an accumulator and it keeps track of the total document length of the entire corpus.

The total number of documents is obtained by dividing the size of the QueryParser List by the
size of the Query List. Incidentally, the average document length in corpus can be intuitively
obtained by dividing the total document length in corpus (accumulated by the accumulator).
Now we have 4 components of the DPH Scoring function. The next functions discuss the last
component total document frequency in corpus.

**QueryFilter:** This is a simple map function that returns a query from a QueryParser object
which will serve as a key for grouping.


**CollectionFrequency:** This is a map groups function that takes as input, a Query, a list of
QueryParser objects and returns a tuple2 object combination, a Query and its corresponding
total term frequencies stored in an integer array (CollectionTermFrequency Structure). This
provides the last component for the DPH Scoring.

**DocumentScorer:** This is the final scoring MapGroupsFunction that takes as input a Query,
QueryParser object and returns a list of type DocumentRanking. Its constructor receives the
components for scoring such as averageDocumentLengthInCorpus, totalDocsInCorpus,
CollectionFrequeny (tuple2 contains query and corresponding CollectionTermFrequency) as
broadcast variables. The QueryParser object contains the termFrequency in current document
and current document length. Now, the static DPHScorer function is called and produces a list
of ranked documents for each query. The top ten which are returned to the main program.

But before the final execution, the ranked list is filtered to remove redundant documents. This
is achieved through the TextDistanceCalculator which computes the similarity of documents.
Very similar documents with a distance less than 0.5 are removed. The final list is appended
with the next documents in the ranked list to maintain a total of 10 ranked documents for
each query.

**Efficiency Discussion:**

The main issue that we faced at the start of implementation was how to map queries to news
Articles efficiently. Since the map (or flatmap) function takes only one input, one way to do it
was using a join (Tuple2<Query, NewsArticle>). However, this maps each query to all
documents. It can be seen that, it is a huge processing issue when we have many queries not
to talk of thousands of documents. Thus, a solution to this predicament is passing the queries
into the flatmap as a broadcast variable which acts as a pointer to the data. It means the data
is not copied, but the location instead. This greatly improves the efficiency of the spark
application. The accumulator as well provides an easy way to keep track of the entire
document length without keeping track of local variables all the time. Finally, the document
scorer function as well receives some of the components of the scoring function through
broadcast variables that greatly improve the efficiency overall.

**Challenges:**

**Limitation-1:**

With DPH quey expansion and ranking models, we have seen that these representation scale
with vocabulary size and are therefore extremely sparse.

**Evidence-** We have seen in our BD project that that as it maps each query terms(unigrams) to
the document, it has a multiplier effect in retrieving many matches for the same query.

It takes only the unigrams(individual tokens) of a single query, and returns matching
documents. Thus, scaling and retrieval efficiency is an issue in this type of approach with large
document and query size corpus.


**How to avoid it in future in our project?**

We can take bi-grams or tri-grams (multi words), but then this can also lead to possible future
issue if there are many query relevance terms in a query.

Best approach we think is possibly using semantics-based query similarity without taking
term-based query-document expansion and ranking approach so that it is independent of
individual terms and takes whole document context into account for document ranking.

**Limitation-2:**

Relevancy of the query matching:

We have used document title for similarity comparison, which basically does the matching
based on document summary, which could miss relevant documents as title is a short text.

**How to avoid it in future in our project?**

We can implement the document text-based similarity, but then scaling would be an issue
here in case large document and query corpus.

**Advantages of the DPH model being a non-parametric model:**

```
1 - DPH ranking is a parameter free model and does not require expensive training.
```
```
2 - Being parameter free, DHP model does not require hyper parameter tuning for
length normalization and other features.
So, the hyper-parameter optimization approach is taken care of internally by the
model.
```
```
3 - The hyperparameter tuning approach is a very expensive research process as it can
lead to multiple searches over the parameter grid.
E.g., in case of automatic query expansion, finding the initial right parameter value is
a difficult task. So, DPH being parameter-free takes care of query expansion.
```

