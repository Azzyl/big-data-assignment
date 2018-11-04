# Team 8 MapReduce assignment<br>
#### Prologue
The purprose of the document is to describe the assignment and 
to explain key points of our solution.<br>
#### Team members:
Azat Belgibayev <br>
Ansat Abirov <br>
Ruslan Kim <br>
Petr Nikolaev <br>

#### Assignment idea
The idea of the assignment is to create a search-engine
which is able to quicly look through all documents and 
select the best fitting results. The threshold is 10 results.
They should be sorted in descending order.

#### Implementation
We decided to use 3 jobs in our solution
- Vocabulary creating
- Creating TF/IDF table
- Executing the query

The solving jar should be executed with arguments:
- Indexer: arg[0] – path to input files
- Query: args – contain the "search request"

##### Vocabulary
We read files and parse the documents as models from json.
The json deserializer was taken from stack-overflow. <br>
The text is extracted from our model and then we make it lower cased
and replace all characters with the help of RegEx. <br>
The output is a single word and id of the document, where the word 
was met.

We create IDF by summing up the unique occurrences of word in 
documents and give each word and index

##### Indexer
We reread the files, count amount of each words for a single document
and calculate TF/IDF. Then output each document as list
of tuples {TF_IDF, wordId}

##### Query 
Tokenize the inpit, calculate IDF, print the best matching documents
the limit is 10

#### P.S
Petr Nikolaev and Ruslan Kim were working with mathemcatical 
explanation of the task and the began to implement Indexer.
Azat Belgibayev and Ansat Abirov finished implementation of
Indexer and implemented Query. The only person, who could 
test code on local cluster was Azat Belgibayev, thus all commits
and test were made on his machine and the code was pushed from
his account (we implemented everything in a couple of meetings,
thus the amount of commits is low)