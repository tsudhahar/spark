# Clustering StackOverflow Q&A By Programming Language and Answer Votes

This repo contains my solution for an assignment from the Coursera course [Big Data Analysis with Spark and Scala](https://www.coursera.org/learn/scala-spark-big-data/home/info).

## Results

The k-means clustering took 44 iterations to converge. There are 45 clusters:

  Median Votes (on answers) | Dominant language (%) | #Questions
----------------------------|-----------------------|---------------
0 | MATLAB (100.0%) | 3725
1 | CSS (100.0%) | 113598
1 | Groovy (100.0%) | 2729
1 | C# (100.0%) | 361835
1 | Ruby (100.0%) | 54727
1 | PHP (100.0%) | 315734
1 | Objective-C (100.0%) | 94617
1 | Java (100.0%) | 383473
1 | JavaScript (100.0%) | 365647
2 | Perl (100.0%) | 19229
2 | MATLAB (100.0%) | 10656
2 | C++ (100.0%) | 181268
2 | Scala (100.0%) | 12472
2 | Clojure (100.0%) | 3324
2 | Python (100.0%) | 174573
4 | Haskell (100.0%) | 10362
9 | Perl (100.0%) | 4714
10 | Groovy (100.0%) | 310
12 | Clojure (100.0%) | 712
27 | Scala (100.0%) | 679
34 | MATLAB (100.0%) | 107
53 | Haskell (100.0%) | 202
61 | Groovy (100.0%) | 14
66 | Clojure (100.0%) | 57
77 | Perl (100.0%) | 58
79 | C# (100.0%) | 2585
85 | Ruby (100.0%) | 648
89 | Objective-C (100.0%) | 903
130 | Scala (100.0%) | 47
135 | PHP (100.0%) | 512
172 | CSS (100.0%) | 358
223 | C++ (100.0%)  | 251
223 | Python (100.0%) | 413
249 | Java (100.0%) | 483
375 | JavaScript (100.0%) | 433
443 | C# (100.0%) | 147
473 | Objective-C (100.0%) | 82
546 | Ruby (100.0%) | 34
766 | CSS (100.0%) | 26
887 | PHP (100.0%) | 13
1130 | Haskell (100.0%) | 2
1269 | Python (100.0%) | 19
1290 | C++ (100.0%) | 9
1895 | JavaScript (100.0%) | 33
10271 | Java (100.0%) | 2

Total run time is 193s.

## Questions

Do you think that partitioning your data would help?

> Yes, partitioning by programming language helps because all questions in a cluster are tagged with the same language. This is true for all clusters. Partitioning reduces shuffling when the data is grouped by their clusters to compute their new centroids. After partitioning, run time dropped dramatically from 1,201s to 193s --- a 6x speedup! 

Have you thought about persisting some of your data? Can you think of why persisting your data in memory may be helpful for this algorithm?

> Persisting `vectors` in memory helps because it can be reused in the iterative k-means algorithm.

Of the non-empty clusters, how many clusters have "Java" as their label (based on the majority of questions, see above)? Why?

> 3 clusters are labelled Java.

Only considering the "Java clusters", which clusters stand out and why?

> There is a huge cluster with more than 380K questions but the median votes on the answers is only 1. This suggests beginner or RTFM questions. On the other hand, there is a tiny cluster with only 2 questions but the median votes on the answers is a whopping 10K. 

How are the "C# clusters" different compared to the "Java clusters"?

Java clusters - all questions in these clusters are tagged as Java

<table>
  <tr>
    <td>Median votes (answers)</td>
    <td>#questions</td>
  </tr>
  <tr>
    <td>1</td>
    <td>383,473</td>
  </tr>
  <tr>
    <td>249</td>
    <td>483</td>
  </tr>
  <tr>
    <td>10,271</td>
    <td>2</td>
  </tr>
</table>


C# clusters - all questions in these clusters are tagged as C#

<table>
  <tr>
    <td>Median votes (answers)</td>
    <td>#questions</td>
  </tr>
  <tr>
    <td>1</td>
    <td>361,835</td>
  </tr>
  <tr>
    <td>79</td>
    <td>2,585</td>
  </tr>
  <tr>
    <td>443</td>
    <td>147</td>
  </tr>
</table>


> Java clusters have higher quality answers than the C# clusters, as indicated by the higher #median votes. This suggests that there are more experienced Java programmers than C#, which may not be surprising since Java is slightly older than C#. Or it could also mean that the Java community is larger than C#. Either way, both communities seem to be very healthy as there are a lot of questions and highly voted answers.

## Instructions

To start, first download the assignment: [stackoverflow.zip](http://alaska.epfl.ch/~dockermoocs/bigdata/stackoverflow.zip). For this assignment, you also need to download the data (170 MB):

http://alaska.epfl.ch/~dockermoocs/bigdata/stackoverflow.csv

and place it in the folder: `src/main/resources/stackoverflow` in your project directory.

The overall goal of this assignment is to implement a distributed k-means algorithm which clusters posts on the popular question-answer platform StackOverflow according to their score. Moreover, this clustering should be executed in parallel for different programming languages, and the results should be compared.

The motivation is as follows: StackOverflow is an important source of documentation. However, different user-provided answers may have very different ratings (based on user votes) based on their perceived value. Therefore, we would like to look at the distribution of questions and their answers. For example, how many highly-rated answers do StackOverflow users post, and how high are their scores? Are there big differences between higher-rated answers and lower-rated ones?

Finally, we are interested in comparing these distributions for different programming language communities. Differences in distributions could reflect differences in the availability of documentation. For example, StackOverflow could have better documentation for a certain library than that library's API documentation. However, to avoid invalid conclusions we will focus on the well-defined problem of clustering answers according to their scores.

### The Data

You are given a CSV (comma-separated values) file with information about StackOverflow posts. Each line in the provided text file has the following format:

```

<postTypeId>,<id>,[<acceptedAnswer>],[<parentId>],<score>,[<tag>]

```

A short explanation of the comma-separated fields follows.

```

<postTypeId>:     Type of the post. Type 1 = question, 

                  type 2 = answer.

                  

<id>:             Unique id of the post (regardless of type).

<acceptedAnswer>: Id of the accepted answer post. This

                  information is optional, so maybe be missing 

                  indicated by an empty string.

                  

<parentId>:       For an answer: id of the corresponding 

                  question. For a question:missing, indicated

                  by an empty string.

                  

<score>:          The StackOverflow score (based on user 

                  votes).

                  

<tag>:            The tag indicates the programming language 

                  that the post is about, in case it's a 

                  question, or missing in case it's an answer.

```

You will see the following code in the main class:

```scala

  val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")  

  val raw     = rawPostings(lines)  

  val grouped = groupedPostings(raw)  

  val scored  = scoredPostings(grouped)  

  val vectors = vectorPostings(scored)

```

It corresponds to the following steps:

`lines`: the lines from the csv file as strings

`raw`: the raw Posting entries for each line

`grouped`: questions and answers grouped together

`scored`: questions and scores

`vectors`: pairs of (language, score) for each question

The first two methods are given to you. You will have to implement the rest.

### Data processing

We will now look at how you process the data before applying the kmeans algorithm.

Grouping questions and answers

The first method you will have to implement is groupedPostings:

```scala

val grouped = groupedPostings(raw)

```

In the raw variable we have simple postings, either questions or answers, but in order to use the data we need to assemble them together. Questions are identified using a `postTypeId == 1`. Answers to a question with `id == QID` have (a) `postTypeId == 2` and (b) `parentId == QID`.

Ideally, we want to obtain an RDD with the pairs of `(Question, Iterable[Answer])`. However, grouping on the question directly is expensive (can you imagine why?), so a better alternative is to match on the QID, thus producing an `RDD[(QID, Iterable[(Question, Answer))]`.

To obtain this, in the `groupedPostings` method, first filter the questions and answers separately and then prepare them for a join operation by extracting the QID value in the first element of a tuple. Then, use one of the join operations (which one?) to obtain an `RDD[(QID, (Question, Answer))]`. Then, the last step is to obtain an `RDD[(QID, Iterable[(Question, Answer)])]`. How can you do that, what method do you use to group by the key of a pair RDD?

Finally, in the description we made QID, Question and Answer separate types, but in the implementation QID is an Int and both questions and answers are of type Posting. Therefore, the signature of groupedPostings is:

```scala

def groupedPostings(postings: RDD[/* Question or Answer */ Posting]): 

    RDD[(/*QID*/ Int, Iterable[(/*Question*/ Posting, /*Answer*/ Posting)])]

```

This should allow you to implement the `groupedPostings` method.

### Computing Scores

Second, implement the `scoredPostings` method, which should return an RDD containing pairs of (a) questions and (b) the score of the answer with the highest score (note: this does not have to be the answer marked as "acceptedAnswer"!). The type of this scored RDD is:

```scala

val scored: RDD[(Posting, Int)] = ???

```

For example, the scored RDD should contain the following tuples:

```scala

((1,6,None,None,140,Some(CSS)),67)

((1,42,None,None,155,Some(PHP)),89)

((1,72,None,None,16,Some(Ruby)),3)

((1,126,None,None,33,Some(Java)),30)

((1,174,None,None,38,Some(C#)),20)

```

Hint: use the provided `answerHighScore` given in `scoredPostings`.

Creating vectors for clustering

Next, we prepare the input for the clustering algorithm. For this, we transform the scored RDD into a vectors RDD containing the vectors to be clustered. In our case, the vectors should be pairs with two components (in the listed order!):

Index of the language (in the `langs` list) multiplied by the `langSpread` factor.

The highest answer score (computed above).

The `langSpread` factor is provided (set to 50000). Basically, it makes sure posts about different programming languages have at least distance 50000 using the distance measure provided by the `euclideanDist` function. You will learn later what this distance means and why it is set to this value.

The type of the vectors RDD is as follows:

```scala

val vectors: RDD[(Int, Int)] = ???

```

For example, the vectors RDD should contain the following tuples:

```

(350000,67)

(100000,89)

(300000,3)

(50000,30)

(200000,20)

```

Implement this functionality in method `vectorPostings` and by using the given the `firstLangInTag` helper method.

(Idea for test: `scored` RDD should have 2121822 entries)

### Kmeans Clustering

```scala

 val means = kmeans(sampleVectors(vectors), vectors)

```

Based on these initial means, and the provided variables converged method, implement the K-means algorithm by iteratively:

pairing each vector with the index of the closest mean (its cluster);

computing the new means by averaging the values of each cluster.

To implement these iterative steps, use the provided functions `findClosest`, `averageVectors`, and `euclideanDistance`.

Note 1:

In our tests, convergence is reached after 44 iterations (for `langSpread=50000`) and in 104 iterations (for `langSpread=1`), and for the first iterations the distance kept growing. Although it may look like something is wrong, this is the expected behavior. Having many remote points forces the kernels to shift quite a bit and with each shift the effects ripple to other kernels, which also move around, and so on. Be patient, in 44 iterations the distance will drop from over 100000 to 13, satisfying the convergence condition.

If you want to get the results faster, feel free to downsample the data (each iteration is faster, but it still takes around 40 steps to converge):

```scala

val scored  = scoredPostings(grouped).sample(true, 0.1, 0)

```

However, keep in mind that we will test your assignment on the full data set. So that means you can downsample for experimentation, but make sure your algorithm works on the full data set when you submit for grading.

Note 2:

The variable `langSpread` corresponds to how far away are languages from the clustering algorithm's point of view. For a value of 50000, the languages are too far away to be clustered together at all, resulting in a clustering that only takes scores into account for each language (similarly to partitioning the data across languages and then clustering based on the score). A more interesting (but less scientific) clustering occurs when `langSpread` is set to 1 (we can't set it to 0, as it loses language information completely), where we cluster according to the score. See which language dominates the top questions now?

### Computing Cluster Details

After the call to `kmeans`, we have the following code in method `main`:

```scala

val results = clusterResults(means, vectors)

printResults(results)

```

Implement the `clusterResults` method, which, for each cluster, computes:

(a) the dominant programming language in the cluster;

(b) the percent of answers that belong to the dominant language;

(c) the size of the cluster (the number of questions it contains);

(d) the median of the highest answer scores.

Once this value is returned, it is printed on the screen by the printResults method.

## Run Output

"C:\Program Files\Java\jdk1.8.0_131\bin\java" -Didea.launcher.port=7534 "-Didea.launcher.bin.path=C:\Program Files (x86)\JetBrains\IntelliJ IDEA Community Edition 2016.3.1\bin" -Dfile.encoding=UTF-8 -classpath "C:\Program Files\Java\jdk1.8.0_131\jre\lib\charsets.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\deploy.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\access-bridge-64.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\cldrdata.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\dnsns.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\jaccess.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\jfxrt.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\localedata.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\nashorn.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\sunec.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\sunjce_provider.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\sunmscapi.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\sunpkcs11.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\ext\zipfs.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\javaws.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\jce.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\jfr.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\jfxswt.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\jsse.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\management-agent.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\plugin.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\resources.jar;C:\Program Files\Java\jdk1.8.0_131\jre\lib\rt.jar;C:\_WorkSpace2\1_Career\Scala\coursera3_spark\stackoverflow\target\scala-2.11\classes;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\aopalliance\aopalliance\1.0\aopalliance-1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\xmlenc\xmlenc\0.52\xmlenc-0.52.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\oro\oro\2.0.8\oro-2.0.8.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\xerial\snappy\snappy-java\1.1.2.6\snappy-java-1.1.2.6.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\tukaani\xz\1.0\xz-1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\spark-project\spark\unused\1.0.0\unused-1.0.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\slf4j-log4j12\1.7.16\slf4j-log4j12-1.7.16.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\slf4j-api\1.7.16\slf4j-api-1.7.16.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\jul-to-slf4j\1.7.16\jul-to-slf4j-1.7.16.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\jcl-over-slf4j\1.7.16\jcl-over-slf4j-1.7.16.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\scalatest\scalatest_2.11\2.2.4\scalatest_2.11-2.2.4.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\modules\scala-xml_2.11\1.0.4\scala-xml_2.11-1.0.4.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\modules\scala-parser-combinators_2.11\1.0.4\scala-parser-combinators_2.11-1.0.4.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scalap\2.11.8\scalap-2.11.8.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-reflect\2.11.8\scala-reflect-2.11.8.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\2.11.8\scala-library-2.11.8.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-compiler\2.11.8\scala-compiler-2.11.8.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\roaringbitmap\RoaringBitmap\0.5.11\RoaringBitmap-0.5.11.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\objenesis\objenesis\2.1\objenesis-2.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\mortbay\jetty\jetty-util\6.1.26\jetty-util-6.1.26.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-jackson_2.11\3.2.11\json4s-jackson_2.11-3.2.11.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-core_2.11\3.2.11\json4s-core_2.11-3.2.11.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-ast_2.11\3.2.11\json4s-ast_2.11-3.2.11.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\javassist\javassist\3.18.1-GA\javassist-3.18.1-GA.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\media\jersey-media-jaxb\2.22.2\jersey-media-jaxb-2.22.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-server\2.22.2\jersey-server-2.22.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-common\2.22.2\jersey-common-2.22.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-client\2.22.2\jersey-client-2.22.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\containers\jersey-container-servlet-core\2.22.2\jersey-container-servlet-core-2.22.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\containers\jersey-container-servlet\2.22.2\jersey-container-servlet-2.22.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\bundles\repackaged\jersey-guava\2.22.2\jersey-guava-2.22.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\external\javax.inject\2.4.0-b34\javax.inject-2.4.0-b34.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\external\aopalliance-repackaged\2.4.0-b34\aopalliance-repackaged-2.4.0-b34.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\osgi-resource-locator\1.0.1\osgi-resource-locator-1.0.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-utils\2.4.0-b34\hk2-utils-2.4.0-b34.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-locator\2.4.0-b34\hk2-locator-2.4.0-b34.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-api\2.4.0-b34\hk2-api-2.4.0-b34.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\fusesource\leveldbjni\leveldbjni-all\1.8\leveldbjni-all-1.8.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\janino\janino\3.0.0\janino-3.0.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\janino\commons-compiler\3.0.0\commons-compiler-3.0.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\jackson\jackson-mapper-asl\1.9.13\jackson-mapper-asl-1.9.13.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\jackson\jackson-core-asl\1.9.13\jackson-core-asl-1.9.13.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\zookeeper\zookeeper\3.4.5\zookeeper-3.4.5.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\xbean\xbean-asm5-shaded\4.4\xbean-asm5-shaded-4.4.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-unsafe_2.11\2.1.0\spark-unsafe_2.11-2.1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-tags_2.11\2.1.0\spark-tags_2.11-2.1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-sql_2.11\2.1.0\spark-sql_2.11-2.1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-sketch_2.11\2.1.0\spark-sketch_2.11-2.1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-network-shuffle_2.11\2.1.0\spark-network-shuffle_2.11-2.1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-network-common_2.11\2.1.0\spark-network-common_2.11-2.1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-launcher_2.11\2.1.0\spark-launcher_2.11-2.1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-core_2.11\2.1.0\spark-core_2.11-2.1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-catalyst_2.11\2.1.0\spark-catalyst_2.11-2.1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-jackson\1.8.1\parquet-jackson-1.8.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-hadoop\1.8.1\parquet-hadoop-1.8.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-format\2.3.0-incubating\parquet-format-2.3.0-incubating.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-encoding\1.8.1\parquet-encoding-1.8.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-common\1.8.1\parquet-common-1.8.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-column\1.8.1\parquet-column-1.8.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\ivy\ivy\2.4.0\ivy-2.4.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-yarn-server-nodemanager\2.2.0\hadoop-yarn-server-nodemanager-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-yarn-server-common\2.2.0\hadoop-yarn-server-common-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-yarn-common\2.2.0\hadoop-yarn-common-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-yarn-client\2.2.0\hadoop-yarn-client-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-yarn-api\2.2.0\hadoop-yarn-api-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-mapreduce-client-shuffle\2.2.0\hadoop-mapreduce-client-shuffle-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-mapreduce-client-jobclient\2.2.0\hadoop-mapreduce-client-jobclient-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-mapreduce-client-core\2.2.0\hadoop-mapreduce-client-core-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-mapreduce-client-common\2.2.0\hadoop-mapreduce-client-common-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-mapreduce-client-app\2.2.0\hadoop-mapreduce-client-app-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-hdfs\2.2.0\hadoop-hdfs-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-common\2.2.0\hadoop-common-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-client\2.2.0\hadoop-client-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-auth\2.2.0\hadoop-auth-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-annotations\2.2.0\hadoop-annotations-2.2.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-recipes\2.4.0\curator-recipes-2.4.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-framework\2.4.0\curator-framework-2.4.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-client\2.4.0\curator-client-2.4.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-math3\3.4.1\commons-math3-3.4.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-math\2.1\commons-math-2.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-lang3\3.5\commons-lang3-3.5.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-crypto\1.0.0\commons-crypto-1.0.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-compress\1.4.1\commons-compress-1.4.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro-mapred\1.7.7\avro-mapred-1.7.7-hadoop2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro-ipc\1.7.7\avro-ipc-1.7.7-tests.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro-ipc\1.7.7\avro-ipc-1.7.7.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro\1.7.7\avro-1.7.7.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\org\antlr\antlr4-runtime\4.5.3\antlr4-runtime-4.5.3.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\net\sf\py4j\py4j\0.10.4\py4j-0.10.4.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\net\razorvine\pyrolite\4.13\pyrolite-4.13.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\net\jpountz\lz4\lz4\1.3.0\lz4-1.3.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\net\java\dev\jets3t\jets3t\0.7.1\jets3t-0.7.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\log4j\log4j\1.2.17\log4j-1.2.17.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\jline\jline\0.9.94\jline-0.9.94.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\javax\ws\rs\javax.ws.rs-api\2.0.1\javax.ws.rs-api-2.0.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\javax\validation\validation-api\1.1.0.Final\validation-api-1.1.0.Final.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\javax\servlet\javax.servlet-api\3.1.0\javax.servlet-api-3.1.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\javax\inject\javax.inject\1\javax.inject-1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\javax\annotation\javax.annotation-api\1.2\javax.annotation-api-1.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-all\4.0.42.Final\netty-all-4.0.42.Final.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty\3.8.0.Final\netty-3.8.0.Final.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-jvm\3.1.2\metrics-jvm-3.1.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-json\3.1.2\metrics-json-3.1.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-graphite\3.1.2\metrics-graphite-3.1.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-core\3.1.2\metrics-core-3.1.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-net\commons-net\3.1\commons-net-3.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-lang\commons-lang\2.5\commons-lang-2.5.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-io\commons-io\2.1\commons-io-2.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-httpclient\commons-httpclient\3.1\commons-httpclient-3.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-digester\commons-digester\1.8\commons-digester-1.8.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-configuration\commons-configuration\1.6\commons-configuration-1.6.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-collections\commons-collections\3.2.1\commons-collections-3.2.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-codec\commons-codec\1.10\commons-codec-1.10.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-cli\commons-cli\1.2\commons-cli-1.2.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-beanutils\commons-beanutils-core\1.8.0\commons-beanutils-core-1.8.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\commons-beanutils\commons-beanutils\1.7.0\commons-beanutils-1.7.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\univocity\univocity-parsers\2.2.1\univocity-parsers-2.2.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\twitter\chill_2.11\0.8.0\chill_2.11-0.8.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\twitter\chill-java\0.8.0\chill-java-0.8.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\thoughtworks\paranamer\paranamer\2.6\paranamer-2.6.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\ning\compress-lzf\1.0.3\compress-lzf-1.0.3.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\google\protobuf\protobuf-java\2.5.0\protobuf-java-2.5.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\google\inject\guice\3.0\guice-3.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\google\guava\guava\14.0.1\guava-14.0.1.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\google\code\findbugs\jsr305\1.3.9\jsr305-1.3.9.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\module\jackson-module-scala_2.11\2.6.5\jackson-module-scala_2.11-2.6.5.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\module\jackson-module-paranamer\2.6.5\jackson-module-paranamer-2.6.5.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-databind\2.6.5\jackson-databind-2.6.5.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-core\2.6.5\jackson-core-2.6.5.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-annotations\2.6.5\jackson-annotations-2.6.5.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\esotericsoftware\minlog\1.3.0\minlog-1.3.0.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\esotericsoftware\kryo-shaded\3.0.3\kryo-shaded-3.0.3.jar;C:\Users\sjcsthia\.coursier\cache\v1\https\repo1.maven.org\maven2\com\clearspring\analytics\stream\2.7.0\stream-2.7.0.jar;C:\Program Files (x86)\JetBrains\IntelliJ IDEA Community Edition 2016.3.1\lib\idea_rt.jar" com.intellij.rt.execution.application.AppMain stackoverflow.StackOverflow
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
17/12/23 18:49:59 INFO SparkContext: Running Spark version 2.1.0
17/12/23 18:50:00 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
17/12/23 18:50:00 INFO SecurityManager: Changing view acls to: sjcsthia
17/12/23 18:50:00 INFO SecurityManager: Changing modify acls to: sjcsthia
17/12/23 18:50:00 INFO SecurityManager: Changing view acls groups to: 
17/12/23 18:50:00 INFO SecurityManager: Changing modify acls groups to: 
17/12/23 18:50:00 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(sjcsthia); groups with view permissions: Set(); users  with modify permissions: Set(sjcsthia); groups with modify permissions: Set()
17/12/23 18:50:03 INFO Utils: Successfully started service 'sparkDriver' on port 50852.
17/12/23 18:50:03 INFO SparkEnv: Registering MapOutputTracker
17/12/23 18:50:03 INFO SparkEnv: Registering BlockManagerMaster
17/12/23 18:50:03 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
17/12/23 18:50:03 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
17/12/23 18:50:03 INFO DiskBlockManager: Created local directory at C:\Users\sjcsthia\AppData\Local\Temp\blockmgr-d87dd174-c840-4d34-b631-8c2ccd8c9e04
17/12/23 18:50:03 INFO MemoryStore: MemoryStore started with capacity 1989.6 MB
17/12/23 18:50:03 INFO SparkEnv: Registering OutputCommitCoordinator
17/12/23 18:50:03 INFO Utils: Successfully started service 'SparkUI' on port 4040.
17/12/23 18:50:03 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://10.0.0.115:4040
17/12/23 18:50:04 INFO Executor: Starting executor ID driver on host localhost
17/12/23 18:50:04 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 50865.
17/12/23 18:50:04 INFO NettyBlockTransferService: Server created on 10.0.0.115:50865
17/12/23 18:50:04 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
17/12/23 18:50:04 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 10.0.0.115, 50865, None)
17/12/23 18:50:04 INFO BlockManagerMasterEndpoint: Registering block manager 10.0.0.115:50865 with 1989.6 MB RAM, BlockManagerId(driver, 10.0.0.115, 50865, None)
17/12/23 18:50:04 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 10.0.0.115, 50865, None)
17/12/23 18:50:04 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 10.0.0.115, 50865, None)
17/12/23 18:50:05 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 127.1 KB, free 1989.5 MB)
17/12/23 18:50:05 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 14.3 KB, free 1989.5 MB)
17/12/23 18:50:05 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 10.0.0.115:50865 (size: 14.3 KB, free: 1989.6 MB)
17/12/23 18:50:05 INFO SparkContext: Created broadcast 0 from textFile at StackOverflow.scala:23
17/12/23 18:50:05 ERROR Shell: Failed to locate the winutils binary in the hadoop binary path
java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
	at org.apache.hadoop.util.Shell.getQualifiedBinPath(Shell.java:278)
	at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:300)
	at org.apache.hadoop.util.Shell.<clinit>(Shell.java:293)
	at org.apache.hadoop.util.StringUtils.<clinit>(StringUtils.java:76)
	at org.apache.hadoop.mapred.FileInputFormat.setInputPaths(FileInputFormat.java:362)
	at org.apache.spark.SparkContext$$anonfun$hadoopFile$1$$anonfun$30.apply(SparkContext.scala:1014)
	at org.apache.spark.SparkContext$$anonfun$hadoopFile$1$$anonfun$30.apply(SparkContext.scala:1014)
	at org.apache.spark.rdd.HadoopRDD$$anonfun$getJobConf$6.apply(HadoopRDD.scala:179)
	at org.apache.spark.rdd.HadoopRDD$$anonfun$getJobConf$6.apply(HadoopRDD.scala:179)
	at scala.Option.foreach(Option.scala:257)
	at org.apache.spark.rdd.HadoopRDD.getJobConf(HadoopRDD.scala:179)
	at org.apache.spark.rdd.HadoopRDD.getPartitions(HadoopRDD.scala:198)
	at org.apache.spark.rdd.RDD$$anonfun$partitions$2.apply(RDD.scala:252)
	at org.apache.spark.rdd.RDD$$anonfun$partitions$2.apply(RDD.scala:250)
	at scala.Option.getOrElse(Option.scala:121)
	at org.apache.spark.rdd.RDD.partitions(RDD.scala:250)
	at org.apache.spark.rdd.MapPartitionsRDD.getPartitions(MapPartitionsRDD.scala:35)
	at org.apache.spark.rdd.RDD$$anonfun$partitions$2.apply(RDD.scala:252)
	at org.apache.spark.rdd.RDD$$anonfun$partitions$2.apply(RDD.scala:250)
	at scala.Option.getOrElse(Option.scala:121)
	at org.apache.spark.rdd.RDD.partitions(RDD.scala:250)
	at org.apache.spark.rdd.MapPartitionsRDD.getPartitions(MapPartitionsRDD.scala:35)
	at org.apache.spark.rdd.RDD$$anonfun$partitions$2.apply(RDD.scala:252)
	at org.apache.spark.rdd.RDD$$anonfun$partitions$2.apply(RDD.scala:250)
	at scala.Option.getOrElse(Option.scala:121)
	at org.apache.spark.rdd.RDD.partitions(RDD.scala:250)
	at org.apache.spark.rdd.MapPartitionsRDD.getPartitions(MapPartitionsRDD.scala:35)
	at org.apache.spark.rdd.RDD$$anonfun$partitions$2.apply(RDD.scala:252)
	at org.apache.spark.rdd.RDD$$anonfun$partitions$2.apply(RDD.scala:250)
	at scala.Option.getOrElse(Option.scala:121)
	at org.apache.spark.rdd.RDD.partitions(RDD.scala:250)
	at org.apache.spark.rdd.MapPartitionsRDD.getPartitions(MapPartitionsRDD.scala:35)
	at org.apache.spark.rdd.RDD$$anonfun$partitions$2.apply(RDD.scala:252)
	at org.apache.spark.rdd.RDD$$anonfun$partitions$2.apply(RDD.scala:250)
	at scala.Option.getOrElse(Option.scala:121)
	at org.apache.spark.rdd.RDD.partitions(RDD.scala:250)
	at org.apache.spark.Partitioner$$anonfun$defaultPartitioner$2.apply(Partitioner.scala:66)
	at org.apache.spark.Partitioner$$anonfun$defaultPartitioner$2.apply(Partitioner.scala:66)
	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
	at scala.collection.immutable.List.foreach(List.scala:381)
	at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
	at scala.collection.immutable.List.map(List.scala:285)
	at org.apache.spark.Partitioner$.defaultPartitioner(Partitioner.scala:66)
	at org.apache.spark.rdd.PairRDDFunctions$$anonfun$join$2.apply(PairRDDFunctions.scala:655)
	at org.apache.spark.rdd.PairRDDFunctions$$anonfun$join$2.apply(PairRDDFunctions.scala:655)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
	at org.apache.spark.rdd.RDD.withScope(RDD.scala:362)
	at org.apache.spark.rdd.PairRDDFunctions.join(PairRDDFunctions.scala:654)
	at stackoverflow.StackOverflow.groupedPostings(StackOverflow.scala:94)
	at stackoverflow.StackOverflow$.main(StackOverflow.scala:25)
	at stackoverflow.StackOverflow.main(StackOverflow.scala)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at com.intellij.rt.execution.application.AppMain.main(AppMain.java:147)
17/12/23 18:50:05 INFO FileInputFormat: Total input paths to process : 1
17/12/23 18:50:05 INFO SparkContext: Starting job: collect at StackOverflow.scala:166
17/12/23 18:50:05 INFO DAGScheduler: Registering RDD 4 (map at StackOverflow.scala:87)
17/12/23 18:50:05 INFO DAGScheduler: Registering RDD 6 (map at StackOverflow.scala:91)
17/12/23 18:50:05 INFO DAGScheduler: Registering RDD 11 (flatMap at StackOverflow.scala:105)
17/12/23 18:50:05 INFO DAGScheduler: Registering RDD 15 (flatMap at StackOverflow.scala:120)
17/12/23 18:50:05 INFO DAGScheduler: Got job 0 (collect at StackOverflow.scala:166) with 6 output partitions
17/12/23 18:50:05 INFO DAGScheduler: Final stage: ResultStage 4 (collect at StackOverflow.scala:166)
17/12/23 18:50:05 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 3)
17/12/23 18:50:05 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 3)
17/12/23 18:50:05 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[4] at map at StackOverflow.scala:87), which has no missing parents
17/12/23 18:50:06 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 4.4 KB, free 1989.5 MB)
17/12/23 18:50:06 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 2.6 KB, free 1989.5 MB)
17/12/23 18:50:06 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 10.0.0.115:50865 (size: 2.6 KB, free: 1989.6 MB)
17/12/23 18:50:06 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:996
17/12/23 18:50:06 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[4] at map at StackOverflow.scala:87)
17/12/23 18:50:06 INFO TaskSchedulerImpl: Adding task set 0.0 with 6 tasks
17/12/23 18:50:06 INFO DAGScheduler: Submitting ShuffleMapStage 1 (MapPartitionsRDD[6] at map at StackOverflow.scala:91), which has no missing parents
17/12/23 18:50:06 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 4.4 KB, free 1989.5 MB)
17/12/23 18:50:06 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 2.6 KB, free 1989.4 MB)
17/12/23 18:50:06 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 10.0.0.115:50865 (size: 2.6 KB, free: 1989.6 MB)
17/12/23 18:50:06 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:996
17/12/23 18:50:06 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 1 (MapPartitionsRDD[6] at map at StackOverflow.scala:91)
17/12/23 18:50:06 INFO TaskSchedulerImpl: Adding task set 1.0 with 6 tasks
17/12/23 18:50:06 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:06 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
17/12/23 18:50:06 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:0+33554432
17/12/23 18:50:06 INFO deprecation: mapred.tip.id is deprecated. Instead, use mapreduce.task.id
17/12/23 18:50:06 INFO deprecation: mapred.task.id is deprecated. Instead, use mapreduce.task.attempt.id
17/12/23 18:50:06 INFO deprecation: mapred.task.is.map is deprecated. Instead, use mapreduce.task.ismap
17/12/23 18:50:06 INFO deprecation: mapred.task.partition is deprecated. Instead, use mapreduce.task.partition
17/12/23 18:50:06 INFO deprecation: mapred.job.id is deprecated. Instead, use mapreduce.job.id
17/12/23 18:50:10 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1575 bytes result sent to driver
17/12/23 18:50:10 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, executor driver, partition 1, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:10 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
17/12/23 18:50:10 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 4574 ms on localhost (executor driver) (1/6)
17/12/23 18:50:10 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:33554432+33554432
17/12/23 18:50:14 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 1485 bytes result sent to driver
17/12/23 18:50:14 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2, localhost, executor driver, partition 2, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:14 INFO Executor: Running task 2.0 in stage 0.0 (TID 2)
17/12/23 18:50:14 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 4111 ms on localhost (executor driver) (2/6)
17/12/23 18:50:14 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:67108864+33554432
17/12/23 18:50:18 INFO Executor: Finished task 2.0 in stage 0.0 (TID 2). 1398 bytes result sent to driver
17/12/23 18:50:18 INFO TaskSetManager: Starting task 3.0 in stage 0.0 (TID 3, localhost, executor driver, partition 3, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:18 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 3219 ms on localhost (executor driver) (3/6)
17/12/23 18:50:18 INFO Executor: Running task 3.0 in stage 0.0 (TID 3)
17/12/23 18:50:18 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:100663296+33554432
17/12/23 18:50:21 INFO Executor: Finished task 3.0 in stage 0.0 (TID 3). 1398 bytes result sent to driver
17/12/23 18:50:21 INFO TaskSetManager: Starting task 4.0 in stage 0.0 (TID 4, localhost, executor driver, partition 4, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:21 INFO Executor: Running task 4.0 in stage 0.0 (TID 4)
17/12/23 18:50:21 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:134217728+33554432
17/12/23 18:50:21 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 3068 ms on localhost (executor driver) (4/6)
17/12/23 18:50:24 INFO Executor: Finished task 4.0 in stage 0.0 (TID 4). 1488 bytes result sent to driver
17/12/23 18:50:24 INFO TaskSetManager: Starting task 5.0 in stage 0.0 (TID 5, localhost, executor driver, partition 5, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:24 INFO TaskSetManager: Finished task 4.0 in stage 0.0 (TID 4) in 2950 ms on localhost (executor driver) (5/6)
17/12/23 18:50:24 INFO Executor: Running task 5.0 in stage 0.0 (TID 5)
17/12/23 18:50:24 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:167772160+10866678
17/12/23 18:50:25 INFO Executor: Finished task 5.0 in stage 0.0 (TID 5). 1398 bytes result sent to driver
17/12/23 18:50:25 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 6, localhost, executor driver, partition 0, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:25 INFO Executor: Running task 0.0 in stage 1.0 (TID 6)
17/12/23 18:50:25 INFO TaskSetManager: Finished task 5.0 in stage 0.0 (TID 5) in 1017 ms on localhost (executor driver) (6/6)
17/12/23 18:50:25 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:0+33554432
17/12/23 18:50:25 INFO DAGScheduler: ShuffleMapStage 0 (map at StackOverflow.scala:87) finished in 18.934 s
17/12/23 18:50:25 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:50:25 INFO DAGScheduler: running: Set(ShuffleMapStage 1)
17/12/23 18:50:25 INFO DAGScheduler: waiting: Set(ShuffleMapStage 2, ShuffleMapStage 3, ResultStage 4)
17/12/23 18:50:25 INFO DAGScheduler: failed: Set()
17/12/23 18:50:25 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
17/12/23 18:50:28 INFO Executor: Finished task 0.0 in stage 1.0 (TID 6). 1311 bytes result sent to driver
17/12/23 18:50:28 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 7, localhost, executor driver, partition 1, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:28 INFO Executor: Running task 1.0 in stage 1.0 (TID 7)
17/12/23 18:50:28 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:33554432+33554432
17/12/23 18:50:28 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 6) in 3070 ms on localhost (executor driver) (1/6)
17/12/23 18:50:30 INFO Executor: Finished task 1.0 in stage 1.0 (TID 7). 1398 bytes result sent to driver
17/12/23 18:50:30 INFO TaskSetManager: Starting task 2.0 in stage 1.0 (TID 8, localhost, executor driver, partition 2, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:31 INFO Executor: Running task 2.0 in stage 1.0 (TID 8)
17/12/23 18:50:31 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 7) in 2937 ms on localhost (executor driver) (2/6)
17/12/23 18:50:31 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:67108864+33554432
17/12/23 18:50:33 INFO Executor: Finished task 2.0 in stage 1.0 (TID 8). 1398 bytes result sent to driver
17/12/23 18:50:33 INFO TaskSetManager: Starting task 3.0 in stage 1.0 (TID 9, localhost, executor driver, partition 3, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:33 INFO Executor: Running task 3.0 in stage 1.0 (TID 9)
17/12/23 18:50:33 INFO TaskSetManager: Finished task 2.0 in stage 1.0 (TID 8) in 2900 ms on localhost (executor driver) (3/6)
17/12/23 18:50:33 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:100663296+33554432
17/12/23 18:50:36 INFO Executor: Finished task 3.0 in stage 1.0 (TID 9). 1398 bytes result sent to driver
17/12/23 18:50:36 INFO TaskSetManager: Starting task 4.0 in stage 1.0 (TID 10, localhost, executor driver, partition 4, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:36 INFO Executor: Running task 4.0 in stage 1.0 (TID 10)
17/12/23 18:50:36 INFO TaskSetManager: Finished task 3.0 in stage 1.0 (TID 9) in 2954 ms on localhost (executor driver) (4/6)
17/12/23 18:50:36 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:134217728+33554432
17/12/23 18:50:39 INFO Executor: Finished task 4.0 in stage 1.0 (TID 10). 1398 bytes result sent to driver
17/12/23 18:50:39 INFO TaskSetManager: Starting task 5.0 in stage 1.0 (TID 11, localhost, executor driver, partition 5, PROCESS_LOCAL, 6054 bytes)
17/12/23 18:50:39 INFO TaskSetManager: Finished task 4.0 in stage 1.0 (TID 10) in 2933 ms on localhost (executor driver) (5/6)
17/12/23 18:50:39 INFO Executor: Running task 5.0 in stage 1.0 (TID 11)
17/12/23 18:50:39 INFO HadoopRDD: Input split: file:/C:/_WorkSpace2/1_Career/Scala/coursera3_spark/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv:167772160+10866678
17/12/23 18:50:40 INFO Executor: Finished task 5.0 in stage 1.0 (TID 11). 1398 bytes result sent to driver
17/12/23 18:50:40 INFO TaskSetManager: Finished task 5.0 in stage 1.0 (TID 11) in 1080 ms on localhost (executor driver) (6/6)
17/12/23 18:50:40 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
17/12/23 18:50:40 INFO DAGScheduler: ShuffleMapStage 1 (map at StackOverflow.scala:91) finished in 34.723 s
17/12/23 18:50:40 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:50:40 INFO DAGScheduler: running: Set()
17/12/23 18:50:40 INFO DAGScheduler: waiting: Set(ShuffleMapStage 2, ShuffleMapStage 3, ResultStage 4)
17/12/23 18:50:40 INFO DAGScheduler: failed: Set()
17/12/23 18:50:40 INFO DAGScheduler: Submitting ShuffleMapStage 2 (MapPartitionsRDD[11] at flatMap at StackOverflow.scala:105), which has no missing parents
17/12/23 18:50:40 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 4.7 KB, free 1989.4 MB)
17/12/23 18:50:40 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 2.4 KB, free 1989.4 MB)
17/12/23 18:50:40 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 10.0.0.115:50865 (size: 2.4 KB, free: 1989.6 MB)
17/12/23 18:50:40 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:996
17/12/23 18:50:40 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[11] at flatMap at StackOverflow.scala:105)
17/12/23 18:50:40 INFO TaskSchedulerImpl: Adding task set 2.0 with 6 tasks
17/12/23 18:50:40 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 12, localhost, executor driver, partition 0, PROCESS_LOCAL, 5803 bytes)
17/12/23 18:50:40 INFO Executor: Running task 0.0 in stage 2.0 (TID 12)
17/12/23 18:50:40 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:50:40 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 5 ms
17/12/23 18:50:40 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:50:40 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:50:48 INFO BlockManagerInfo: Removed broadcast_2_piece0 on 10.0.0.115:50865 in memory (size: 2.6 KB, free: 1989.6 MB)
17/12/23 18:50:48 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 10.0.0.115:50865 in memory (size: 2.6 KB, free: 1989.6 MB)
17/12/23 18:50:51 INFO Executor: Finished task 0.0 in stage 2.0 (TID 12). 2035 bytes result sent to driver
17/12/23 18:50:51 INFO TaskSetManager: Starting task 1.0 in stage 2.0 (TID 13, localhost, executor driver, partition 1, PROCESS_LOCAL, 5803 bytes)
17/12/23 18:50:51 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 12) in 10183 ms on localhost (executor driver) (1/6)
17/12/23 18:50:51 INFO Executor: Running task 1.0 in stage 2.0 (TID 13)
17/12/23 18:50:51 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:50:51 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:50:51 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:50:51 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:50:57 INFO Executor: Finished task 1.0 in stage 2.0 (TID 13). 2035 bytes result sent to driver
17/12/23 18:50:57 INFO TaskSetManager: Starting task 2.0 in stage 2.0 (TID 14, localhost, executor driver, partition 2, PROCESS_LOCAL, 5803 bytes)
17/12/23 18:50:57 INFO Executor: Running task 2.0 in stage 2.0 (TID 14)
17/12/23 18:50:57 INFO TaskSetManager: Finished task 1.0 in stage 2.0 (TID 13) in 6525 ms on localhost (executor driver) (2/6)
17/12/23 18:50:57 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:50:57 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:50:57 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:50:57 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
17/12/23 18:51:06 INFO Executor: Finished task 2.0 in stage 2.0 (TID 14). 2125 bytes result sent to driver
17/12/23 18:51:06 INFO TaskSetManager: Starting task 3.0 in stage 2.0 (TID 15, localhost, executor driver, partition 3, PROCESS_LOCAL, 5803 bytes)
17/12/23 18:51:06 INFO TaskSetManager: Finished task 2.0 in stage 2.0 (TID 14) in 9163 ms on localhost (executor driver) (3/6)
17/12/23 18:51:06 INFO Executor: Running task 3.0 in stage 2.0 (TID 15)
17/12/23 18:51:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:13 INFO Executor: Finished task 3.0 in stage 2.0 (TID 15). 2035 bytes result sent to driver
17/12/23 18:51:13 INFO TaskSetManager: Starting task 4.0 in stage 2.0 (TID 16, localhost, executor driver, partition 4, PROCESS_LOCAL, 5803 bytes)
17/12/23 18:51:13 INFO Executor: Running task 4.0 in stage 2.0 (TID 16)
17/12/23 18:51:13 INFO TaskSetManager: Finished task 3.0 in stage 2.0 (TID 15) in 6639 ms on localhost (executor driver) (4/6)
17/12/23 18:51:13 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:13 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:13 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:19 INFO Executor: Finished task 4.0 in stage 2.0 (TID 16). 1948 bytes result sent to driver
17/12/23 18:51:19 INFO TaskSetManager: Starting task 5.0 in stage 2.0 (TID 17, localhost, executor driver, partition 5, PROCESS_LOCAL, 5803 bytes)
17/12/23 18:51:19 INFO Executor: Running task 5.0 in stage 2.0 (TID 17)
17/12/23 18:51:19 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:19 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:19 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:19 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:19 INFO TaskSetManager: Finished task 4.0 in stage 2.0 (TID 16) in 6278 ms on localhost (executor driver) (5/6)
17/12/23 18:51:26 INFO Executor: Finished task 5.0 in stage 2.0 (TID 17). 1948 bytes result sent to driver
17/12/23 18:51:26 INFO DAGScheduler: ShuffleMapStage 2 (flatMap at StackOverflow.scala:105) finished in 45.395 s
17/12/23 18:51:26 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:51:26 INFO DAGScheduler: running: Set()
17/12/23 18:51:26 INFO DAGScheduler: waiting: Set(ShuffleMapStage 3, ResultStage 4)
17/12/23 18:51:26 INFO DAGScheduler: failed: Set()
17/12/23 18:51:26 INFO TaskSetManager: Finished task 5.0 in stage 2.0 (TID 17) in 6625 ms on localhost (executor driver) (6/6)
17/12/23 18:51:26 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
17/12/23 18:51:26 INFO DAGScheduler: Submitting ShuffleMapStage 3 (MapPartitionsRDD[15] at flatMap at StackOverflow.scala:120), which has no missing parents
17/12/23 18:51:26 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 6.7 KB, free 1989.4 MB)
17/12/23 18:51:26 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 3.4 KB, free 1989.4 MB)
17/12/23 18:51:26 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 10.0.0.115:50865 (size: 3.4 KB, free: 1989.6 MB)
17/12/23 18:51:26 INFO SparkContext: Created broadcast 4 from broadcast at DAGScheduler.scala:996
17/12/23 18:51:26 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 3 (MapPartitionsRDD[15] at flatMap at StackOverflow.scala:120)
17/12/23 18:51:26 INFO TaskSchedulerImpl: Adding task set 3.0 with 6 tasks
17/12/23 18:51:26 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 18, localhost, executor driver, partition 0, ANY, 5740 bytes)
17/12/23 18:51:26 INFO Executor: Running task 0.0 in stage 3.0 (TID 18)
17/12/23 18:51:26 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:26 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:29 INFO MemoryStore: Block rdd_15_0 stored as values in memory (estimated size 12.1 MB, free 1977.3 MB)
17/12/23 18:51:29 INFO BlockManagerInfo: Added rdd_15_0 in memory on 10.0.0.115:50865 (size: 12.1 MB, free: 1977.4 MB)
17/12/23 18:51:29 INFO Executor: Finished task 0.0 in stage 3.0 (TID 18). 2757 bytes result sent to driver
17/12/23 18:51:29 INFO TaskSetManager: Starting task 1.0 in stage 3.0 (TID 19, localhost, executor driver, partition 1, ANY, 5740 bytes)
17/12/23 18:51:29 INFO TaskSetManager: Finished task 0.0 in stage 3.0 (TID 18) in 3694 ms on localhost (executor driver) (1/6)
17/12/23 18:51:29 INFO Executor: Running task 1.0 in stage 3.0 (TID 19)
17/12/23 18:51:29 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:32 INFO MemoryStore: Block rdd_15_1 stored as values in memory (estimated size 12.1 MB, free 1965.2 MB)
17/12/23 18:51:32 INFO BlockManagerInfo: Added rdd_15_1 in memory on 10.0.0.115:50865 (size: 12.1 MB, free: 1965.3 MB)
17/12/23 18:51:32 INFO Executor: Finished task 1.0 in stage 3.0 (TID 19). 2757 bytes result sent to driver
17/12/23 18:51:32 INFO TaskSetManager: Starting task 2.0 in stage 3.0 (TID 20, localhost, executor driver, partition 2, ANY, 5740 bytes)
17/12/23 18:51:32 INFO Executor: Running task 2.0 in stage 3.0 (TID 20)
17/12/23 18:51:32 INFO TaskSetManager: Finished task 1.0 in stage 3.0 (TID 19) in 2920 ms on localhost (executor driver) (2/6)
17/12/23 18:51:32 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:32 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:35 INFO MemoryStore: Block rdd_15_2 stored as values in memory (estimated size 12.1 MB, free 1953.0 MB)
17/12/23 18:51:35 INFO BlockManagerInfo: Added rdd_15_2 in memory on 10.0.0.115:50865 (size: 12.1 MB, free: 1953.2 MB)
17/12/23 18:51:35 INFO Executor: Finished task 2.0 in stage 3.0 (TID 20). 2597 bytes result sent to driver
17/12/23 18:51:35 INFO TaskSetManager: Starting task 3.0 in stage 3.0 (TID 21, localhost, executor driver, partition 3, ANY, 5740 bytes)
17/12/23 18:51:35 INFO TaskSetManager: Finished task 2.0 in stage 3.0 (TID 20) in 2788 ms on localhost (executor driver) (3/6)
17/12/23 18:51:35 INFO Executor: Running task 3.0 in stage 3.0 (TID 21)
17/12/23 18:51:35 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:35 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:51:38 INFO MemoryStore: Block rdd_15_3 stored as values in memory (estimated size 12.1 MB, free 1940.9 MB)
17/12/23 18:51:38 INFO BlockManagerInfo: Added rdd_15_3 in memory on 10.0.0.115:50865 (size: 12.1 MB, free: 1941.1 MB)
17/12/23 18:51:38 INFO Executor: Finished task 3.0 in stage 3.0 (TID 21). 2670 bytes result sent to driver
17/12/23 18:51:38 INFO TaskSetManager: Starting task 4.0 in stage 3.0 (TID 22, localhost, executor driver, partition 4, ANY, 5740 bytes)
17/12/23 18:51:38 INFO TaskSetManager: Finished task 3.0 in stage 3.0 (TID 21) in 2875 ms on localhost (executor driver) (4/6)
17/12/23 18:51:38 INFO Executor: Running task 4.0 in stage 3.0 (TID 22)
17/12/23 18:51:38 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:38 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:41 INFO MemoryStore: Block rdd_15_4 stored as values in memory (estimated size 12.2 MB, free 1928.8 MB)
17/12/23 18:51:41 INFO BlockManagerInfo: Added rdd_15_4 in memory on 10.0.0.115:50865 (size: 12.2 MB, free: 1928.9 MB)
17/12/23 18:51:41 INFO Executor: Finished task 4.0 in stage 3.0 (TID 22). 2757 bytes result sent to driver
17/12/23 18:51:41 INFO TaskSetManager: Starting task 5.0 in stage 3.0 (TID 23, localhost, executor driver, partition 5, ANY, 5740 bytes)
17/12/23 18:51:41 INFO Executor: Running task 5.0 in stage 3.0 (TID 23)
17/12/23 18:51:41 INFO TaskSetManager: Finished task 4.0 in stage 3.0 (TID 22) in 3024 ms on localhost (executor driver) (5/6)
17/12/23 18:51:41 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:41 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:44 INFO MemoryStore: Block rdd_15_5 stored as values in memory (estimated size 12.2 MB, free 1916.6 MB)
17/12/23 18:51:44 INFO BlockManagerInfo: Added rdd_15_5 in memory on 10.0.0.115:50865 (size: 12.2 MB, free: 1916.7 MB)
17/12/23 18:51:44 INFO Executor: Finished task 5.0 in stage 3.0 (TID 23). 2757 bytes result sent to driver
17/12/23 18:51:44 INFO TaskSetManager: Finished task 5.0 in stage 3.0 (TID 23) in 3206 ms on localhost (executor driver) (6/6)
17/12/23 18:51:44 INFO TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool 
17/12/23 18:51:44 INFO DAGScheduler: ShuffleMapStage 3 (flatMap at StackOverflow.scala:120) finished in 18.503 s
17/12/23 18:51:44 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:51:44 INFO DAGScheduler: running: Set()
17/12/23 18:51:44 INFO DAGScheduler: waiting: Set(ResultStage 4)
17/12/23 18:51:44 INFO DAGScheduler: failed: Set()
17/12/23 18:51:44 INFO DAGScheduler: Submitting ResultStage 4 (MapPartitionsRDD[17] at flatMap at StackOverflow.scala:164), which has no missing parents
17/12/23 18:51:44 INFO MemoryStore: Block broadcast_5 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:51:44 INFO MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:51:44 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:51:44 INFO SparkContext: Created broadcast 5 from broadcast at DAGScheduler.scala:996
17/12/23 18:51:44 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 4 (MapPartitionsRDD[17] at flatMap at StackOverflow.scala:164)
17/12/23 18:51:44 INFO TaskSchedulerImpl: Adding task set 4.0 with 6 tasks
17/12/23 18:51:44 INFO TaskSetManager: Starting task 1.0 in stage 4.0 (TID 24, localhost, executor driver, partition 1, PROCESS_LOCAL, 5751 bytes)
17/12/23 18:51:44 INFO Executor: Running task 1.0 in stage 4.0 (TID 24)
17/12/23 18:51:44 INFO ShuffleBlockFetcherIterator: Getting 0 non-empty blocks out of 6 blocks
17/12/23 18:51:44 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
17/12/23 18:51:44 INFO Executor: Finished task 1.0 in stage 4.0 (TID 24). 1628 bytes result sent to driver
17/12/23 18:51:44 INFO TaskSetManager: Starting task 3.0 in stage 4.0 (TID 25, localhost, executor driver, partition 3, PROCESS_LOCAL, 5751 bytes)
17/12/23 18:51:44 INFO Executor: Running task 3.0 in stage 4.0 (TID 25)
17/12/23 18:51:44 INFO ShuffleBlockFetcherIterator: Getting 0 non-empty blocks out of 6 blocks
17/12/23 18:51:44 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:44 INFO TaskSetManager: Finished task 1.0 in stage 4.0 (TID 24) in 20 ms on localhost (executor driver) (1/6)
17/12/23 18:51:44 INFO Executor: Finished task 3.0 in stage 4.0 (TID 25). 1549 bytes result sent to driver
17/12/23 18:51:44 INFO TaskSetManager: Starting task 5.0 in stage 4.0 (TID 26, localhost, executor driver, partition 5, PROCESS_LOCAL, 5751 bytes)
17/12/23 18:51:44 INFO TaskSetManager: Finished task 3.0 in stage 4.0 (TID 25) in 22 ms on localhost (executor driver) (2/6)
17/12/23 18:51:44 INFO Executor: Running task 5.0 in stage 4.0 (TID 26)
17/12/23 18:51:44 INFO ShuffleBlockFetcherIterator: Getting 0 non-empty blocks out of 6 blocks
17/12/23 18:51:44 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:44 INFO Executor: Finished task 5.0 in stage 4.0 (TID 26). 1639 bytes result sent to driver
17/12/23 18:51:44 INFO TaskSetManager: Starting task 0.0 in stage 4.0 (TID 27, localhost, executor driver, partition 0, ANY, 5751 bytes)
17/12/23 18:51:44 INFO TaskSetManager: Finished task 5.0 in stage 4.0 (TID 26) in 13 ms on localhost (executor driver) (3/6)
17/12/23 18:51:44 INFO Executor: Running task 0.0 in stage 4.0 (TID 27)
17/12/23 18:51:44 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:44 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:45 INFO Executor: Finished task 0.0 in stage 4.0 (TID 27). 1989 bytes result sent to driver
17/12/23 18:51:45 INFO TaskSetManager: Starting task 2.0 in stage 4.0 (TID 28, localhost, executor driver, partition 2, ANY, 5751 bytes)
17/12/23 18:51:45 INFO TaskSetManager: Finished task 0.0 in stage 4.0 (TID 27) in 511 ms on localhost (executor driver) (4/6)
17/12/23 18:51:45 INFO Executor: Running task 2.0 in stage 4.0 (TID 28)
17/12/23 18:51:45 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:45 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:45 INFO Executor: Finished task 2.0 in stage 4.0 (TID 28). 1989 bytes result sent to driver
17/12/23 18:51:45 INFO TaskSetManager: Starting task 4.0 in stage 4.0 (TID 29, localhost, executor driver, partition 4, ANY, 5751 bytes)
17/12/23 18:51:45 INFO TaskSetManager: Finished task 2.0 in stage 4.0 (TID 28) in 347 ms on localhost (executor driver) (5/6)
17/12/23 18:51:45 INFO Executor: Running task 4.0 in stage 4.0 (TID 29)
17/12/23 18:51:45 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:45 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
17/12/23 18:51:45 INFO Executor: Finished task 4.0 in stage 4.0 (TID 29). 1989 bytes result sent to driver
17/12/23 18:51:45 INFO TaskSetManager: Finished task 4.0 in stage 4.0 (TID 29) in 238 ms on localhost (executor driver) (6/6)
17/12/23 18:51:45 INFO TaskSchedulerImpl: Removed TaskSet 4.0, whose tasks have all completed, from pool 
17/12/23 18:51:45 INFO DAGScheduler: ResultStage 4 (collect at StackOverflow.scala:166) finished in 1.137 s
17/12/23 18:51:45 INFO DAGScheduler: Job 0 finished: collect at StackOverflow.scala:166, took 100.038811 s
17/12/23 18:51:45 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:51:45 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:51:46 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:51:46 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:51:46 INFO DAGScheduler: Registering RDD 18 (map at StackOverflow.scala:186)
17/12/23 18:51:46 INFO DAGScheduler: Got job 1 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:51:46 INFO DAGScheduler: Final stage: ResultStage 9 (collect at StackOverflow.scala:191)
17/12/23 18:51:46 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 8)
17/12/23 18:51:46 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 8)
17/12/23 18:51:46 INFO DAGScheduler: Submitting ShuffleMapStage 8 (MapPartitionsRDD[18] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:51:46 INFO MemoryStore: Block broadcast_6 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:51:46 INFO MemoryStore: Block broadcast_6_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:51:46 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:51:46 INFO SparkContext: Created broadcast 6 from broadcast at DAGScheduler.scala:996
17/12/23 18:51:46 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 8 (MapPartitionsRDD[18] at map at StackOverflow.scala:186)
17/12/23 18:51:46 INFO TaskSchedulerImpl: Adding task set 8.0 with 6 tasks
17/12/23 18:51:46 INFO TaskSetManager: Starting task 0.0 in stage 8.0 (TID 30, localhost, executor driver, partition 0, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:46 INFO Executor: Running task 0.0 in stage 8.0 (TID 30)
17/12/23 18:51:46 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:51:46 INFO Executor: Finished task 0.0 in stage 8.0 (TID 30). 1325 bytes result sent to driver
17/12/23 18:51:46 INFO TaskSetManager: Starting task 1.0 in stage 8.0 (TID 31, localhost, executor driver, partition 1, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:46 INFO Executor: Running task 1.0 in stage 8.0 (TID 31)
17/12/23 18:51:46 INFO TaskSetManager: Finished task 0.0 in stage 8.0 (TID 30) in 495 ms on localhost (executor driver) (1/6)
17/12/23 18:51:46 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:51:47 INFO Executor: Finished task 1.0 in stage 8.0 (TID 31). 1325 bytes result sent to driver
17/12/23 18:51:47 INFO TaskSetManager: Starting task 2.0 in stage 8.0 (TID 32, localhost, executor driver, partition 2, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:47 INFO Executor: Running task 2.0 in stage 8.0 (TID 32)
17/12/23 18:51:47 INFO TaskSetManager: Finished task 1.0 in stage 8.0 (TID 31) in 495 ms on localhost (executor driver) (2/6)
17/12/23 18:51:47 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:51:47 INFO Executor: Finished task 2.0 in stage 8.0 (TID 32). 1325 bytes result sent to driver
17/12/23 18:51:47 INFO TaskSetManager: Starting task 3.0 in stage 8.0 (TID 33, localhost, executor driver, partition 3, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:47 INFO Executor: Running task 3.0 in stage 8.0 (TID 33)
17/12/23 18:51:47 INFO TaskSetManager: Finished task 2.0 in stage 8.0 (TID 32) in 590 ms on localhost (executor driver) (3/6)
17/12/23 18:51:47 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:51:48 INFO Executor: Finished task 3.0 in stage 8.0 (TID 33). 1238 bytes result sent to driver
17/12/23 18:51:48 INFO TaskSetManager: Starting task 4.0 in stage 8.0 (TID 34, localhost, executor driver, partition 4, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:48 INFO Executor: Running task 4.0 in stage 8.0 (TID 34)
17/12/23 18:51:48 INFO TaskSetManager: Finished task 3.0 in stage 8.0 (TID 33) in 615 ms on localhost (executor driver) (4/6)
17/12/23 18:51:48 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:51:48 INFO Executor: Finished task 4.0 in stage 8.0 (TID 34). 1325 bytes result sent to driver
17/12/23 18:51:48 INFO TaskSetManager: Starting task 5.0 in stage 8.0 (TID 35, localhost, executor driver, partition 5, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:48 INFO TaskSetManager: Finished task 4.0 in stage 8.0 (TID 34) in 615 ms on localhost (executor driver) (5/6)
17/12/23 18:51:48 INFO Executor: Running task 5.0 in stage 8.0 (TID 35)
17/12/23 18:51:48 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:51:49 INFO Executor: Finished task 5.0 in stage 8.0 (TID 35). 1238 bytes result sent to driver
17/12/23 18:51:49 INFO TaskSetManager: Finished task 5.0 in stage 8.0 (TID 35) in 577 ms on localhost (executor driver) (6/6)
17/12/23 18:51:49 INFO TaskSchedulerImpl: Removed TaskSet 8.0, whose tasks have all completed, from pool 
17/12/23 18:51:49 INFO DAGScheduler: ShuffleMapStage 8 (map at StackOverflow.scala:186) finished in 3.382 s
17/12/23 18:51:49 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:51:49 INFO DAGScheduler: running: Set()
17/12/23 18:51:49 INFO DAGScheduler: waiting: Set(ResultStage 9)
17/12/23 18:51:49 INFO DAGScheduler: failed: Set()
17/12/23 18:51:49 INFO DAGScheduler: Submitting ResultStage 9 (MapPartitionsRDD[20] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:51:49 INFO MemoryStore: Block broadcast_7 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:51:49 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 3.7 KB, free 1916.6 MB)
17/12/23 18:51:49 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on 10.0.0.115:50865 (size: 3.7 KB, free: 1916.7 MB)
17/12/23 18:51:49 INFO SparkContext: Created broadcast 7 from broadcast at DAGScheduler.scala:996
17/12/23 18:51:49 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 9 (MapPartitionsRDD[20] at mapValues at StackOverflow.scala:190)
17/12/23 18:51:49 INFO TaskSchedulerImpl: Adding task set 9.0 with 6 tasks
17/12/23 18:51:49 INFO TaskSetManager: Starting task 0.0 in stage 9.0 (TID 36, localhost, executor driver, partition 0, ANY, 5751 bytes)
17/12/23 18:51:49 INFO Executor: Running task 0.0 in stage 9.0 (TID 36)
17/12/23 18:51:49 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:49 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:49 INFO Executor: Finished task 0.0 in stage 9.0 (TID 36). 2162 bytes result sent to driver
17/12/23 18:51:49 INFO TaskSetManager: Starting task 1.0 in stage 9.0 (TID 37, localhost, executor driver, partition 1, ANY, 5751 bytes)
17/12/23 18:51:49 INFO Executor: Running task 1.0 in stage 9.0 (TID 37)
17/12/23 18:51:49 INFO TaskSetManager: Finished task 0.0 in stage 9.0 (TID 36) in 577 ms on localhost (executor driver) (1/6)
17/12/23 18:51:49 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:49 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:50 INFO Executor: Finished task 1.0 in stage 9.0 (TID 37). 2008 bytes result sent to driver
17/12/23 18:51:50 INFO TaskSetManager: Starting task 2.0 in stage 9.0 (TID 38, localhost, executor driver, partition 2, ANY, 5751 bytes)
17/12/23 18:51:50 INFO TaskSetManager: Finished task 1.0 in stage 9.0 (TID 37) in 163 ms on localhost (executor driver) (2/6)
17/12/23 18:51:50 INFO Executor: Running task 2.0 in stage 9.0 (TID 38)
17/12/23 18:51:50 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:50 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:50 INFO BlockManagerInfo: Removed broadcast_5_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:51:50 INFO BlockManagerInfo: Removed broadcast_6_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:51:50 INFO Executor: Finished task 2.0 in stage 9.0 (TID 38). 1930 bytes result sent to driver
17/12/23 18:51:50 INFO TaskSetManager: Starting task 3.0 in stage 9.0 (TID 39, localhost, executor driver, partition 3, ANY, 5751 bytes)
17/12/23 18:51:50 INFO Executor: Running task 3.0 in stage 9.0 (TID 39)
17/12/23 18:51:50 INFO TaskSetManager: Finished task 2.0 in stage 9.0 (TID 38) in 307 ms on localhost (executor driver) (3/6)
17/12/23 18:51:50 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:50 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:51:51 INFO Executor: Finished task 3.0 in stage 9.0 (TID 39). 2130 bytes result sent to driver
17/12/23 18:51:51 INFO TaskSetManager: Starting task 4.0 in stage 9.0 (TID 40, localhost, executor driver, partition 4, ANY, 5751 bytes)
17/12/23 18:51:51 INFO TaskSetManager: Finished task 3.0 in stage 9.0 (TID 39) in 960 ms on localhost (executor driver) (4/6)
17/12/23 18:51:51 INFO Executor: Running task 4.0 in stage 9.0 (TID 40)
17/12/23 18:51:51 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:51 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:51 INFO Executor: Finished task 4.0 in stage 9.0 (TID 40). 1889 bytes result sent to driver
17/12/23 18:51:51 INFO TaskSetManager: Starting task 5.0 in stage 9.0 (TID 41, localhost, executor driver, partition 5, ANY, 5751 bytes)
17/12/23 18:51:51 INFO TaskSetManager: Finished task 4.0 in stage 9.0 (TID 40) in 253 ms on localhost (executor driver) (5/6)
17/12/23 18:51:51 INFO Executor: Running task 5.0 in stage 9.0 (TID 41)
17/12/23 18:51:51 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:51 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
17/12/23 18:51:51 INFO Executor: Finished task 5.0 in stage 9.0 (TID 41). 1976 bytes result sent to driver
17/12/23 18:51:51 INFO TaskSetManager: Finished task 5.0 in stage 9.0 (TID 41) in 70 ms on localhost (executor driver) (6/6)
17/12/23 18:51:51 INFO TaskSchedulerImpl: Removed TaskSet 9.0, whose tasks have all completed, from pool 
17/12/23 18:51:51 INFO DAGScheduler: ResultStage 9 (collect at StackOverflow.scala:191) finished in 2.330 s
17/12/23 18:51:51 INFO DAGScheduler: Job 1 finished: collect at StackOverflow.scala:191, took 5.744058 s
Iteration: 1
  * current distance: 888.0
  * desired distance: 20.0
  * means:
             (450000,1) ==>           (450000,2)    distance:        1
             (450000,0) ==>           (450000,0)    distance:        0
             (450000,8) ==>          (450000,10)    distance:        4
                  (0,0) ==>                (0,3)    distance:        9
                  (0,0) ==>                (0,0)    distance:        0
                  (0,0) ==>                (0,0)    distance:        0
             (600000,3) ==>           (600000,4)    distance:        1
             (600000,1) ==>           (600000,0)    distance:        1
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,1) ==>           (150000,0)    distance:        1
             (150000,4) ==>           (150000,4)    distance:        0
            (150000,13) ==>          (150000,32)    distance:      361
             (300000,1) ==>           (300000,6)    distance:       25
             (300000,0) ==>           (300000,0)    distance:        0
             (300000,1) ==>           (300000,1)    distance:        0
              (50000,0) ==>            (50000,3)    distance:        9
              (50000,0) ==>            (50000,0)    distance:        0
              (50000,0) ==>            (50000,0)    distance:        0
             (200000,0) ==>           (200000,0)    distance:        0
             (200000,0) ==>           (200000,0)    distance:        0
             (200000,2) ==>           (200000,7)    distance:       25
             (500000,1) ==>           (500000,0)    distance:        1
             (500000,4) ==>          (500000,11)    distance:       49
             (500000,3) ==>           (500000,3)    distance:        0
             (350000,0) ==>           (350000,0)    distance:        0
             (350000,0) ==>           (350000,0)    distance:        0
             (350000,1) ==>           (350000,5)    distance:       16
            (650000,20) ==>          (650000,28)    distance:       64
             (650000,0) ==>           (650000,1)    distance:        1
             (650000,5) ==>           (650000,5)    distance:        0
             (100000,0) ==>           (100000,0)    distance:        0
             (100000,2) ==>           (100000,5)    distance:        9
             (100000,2) ==>           (100000,2)    distance:        0
             (400000,5) ==>          (400000,16)    distance:      121
             (400000,1) ==>           (400000,0)    distance:        1
             (400000,3) ==>           (400000,3)    distance:        0
             (550000,0) ==>           (550000,1)    distance:        1
             (550000,5) ==>           (550000,7)    distance:        4
            (550000,42) ==>          (550000,53)    distance:      121
             (250000,4) ==>          (250000,11)    distance:       49
             (250000,2) ==>           (250000,0)    distance:        4
             (250000,3) ==>           (250000,3)    distance:        0
             (700000,1) ==>           (700000,0)    distance:        1
             (700000,2) ==>           (700000,5)    distance:        9
             (700000,1) ==>           (700000,1)    distance:        0
17/12/23 18:51:51 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:51:51 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:51:51 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:51:51 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:51:51 INFO DAGScheduler: Registering RDD 21 (map at StackOverflow.scala:186)
17/12/23 18:51:51 INFO DAGScheduler: Got job 2 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:51:51 INFO DAGScheduler: Final stage: ResultStage 14 (collect at StackOverflow.scala:191)
17/12/23 18:51:51 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 13)
17/12/23 18:51:51 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 13)
17/12/23 18:51:51 INFO DAGScheduler: Submitting ShuffleMapStage 13 (MapPartitionsRDD[21] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:51:51 INFO MemoryStore: Block broadcast_8 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:51:51 INFO MemoryStore: Block broadcast_8_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:51:51 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:51:51 INFO SparkContext: Created broadcast 8 from broadcast at DAGScheduler.scala:996
17/12/23 18:51:51 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 13 (MapPartitionsRDD[21] at map at StackOverflow.scala:186)
17/12/23 18:51:51 INFO TaskSchedulerImpl: Adding task set 13.0 with 6 tasks
17/12/23 18:51:51 INFO TaskSetManager: Starting task 0.0 in stage 13.0 (TID 42, localhost, executor driver, partition 0, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:51 INFO Executor: Running task 0.0 in stage 13.0 (TID 42)
17/12/23 18:51:51 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:51:52 INFO Executor: Finished task 0.0 in stage 13.0 (TID 42). 1325 bytes result sent to driver
17/12/23 18:51:52 INFO TaskSetManager: Starting task 1.0 in stage 13.0 (TID 43, localhost, executor driver, partition 1, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:52 INFO Executor: Running task 1.0 in stage 13.0 (TID 43)
17/12/23 18:51:52 INFO TaskSetManager: Finished task 0.0 in stage 13.0 (TID 42) in 512 ms on localhost (executor driver) (1/6)
17/12/23 18:51:52 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:51:52 INFO Executor: Finished task 1.0 in stage 13.0 (TID 43). 1325 bytes result sent to driver
17/12/23 18:51:52 INFO TaskSetManager: Starting task 2.0 in stage 13.0 (TID 44, localhost, executor driver, partition 2, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:52 INFO Executor: Running task 2.0 in stage 13.0 (TID 44)
17/12/23 18:51:52 INFO TaskSetManager: Finished task 1.0 in stage 13.0 (TID 43) in 423 ms on localhost (executor driver) (2/6)
17/12/23 18:51:52 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:51:53 INFO Executor: Finished task 2.0 in stage 13.0 (TID 44). 1238 bytes result sent to driver
17/12/23 18:51:53 INFO TaskSetManager: Starting task 3.0 in stage 13.0 (TID 45, localhost, executor driver, partition 3, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:53 INFO Executor: Running task 3.0 in stage 13.0 (TID 45)
17/12/23 18:51:53 INFO TaskSetManager: Finished task 2.0 in stage 13.0 (TID 44) in 465 ms on localhost (executor driver) (3/6)
17/12/23 18:51:53 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:51:53 INFO Executor: Finished task 3.0 in stage 13.0 (TID 45). 1415 bytes result sent to driver
17/12/23 18:51:53 INFO TaskSetManager: Starting task 4.0 in stage 13.0 (TID 46, localhost, executor driver, partition 4, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:53 INFO Executor: Running task 4.0 in stage 13.0 (TID 46)
17/12/23 18:51:53 INFO TaskSetManager: Finished task 3.0 in stage 13.0 (TID 45) in 428 ms on localhost (executor driver) (4/6)
17/12/23 18:51:53 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:51:54 INFO Executor: Finished task 4.0 in stage 13.0 (TID 46). 1325 bytes result sent to driver
17/12/23 18:51:54 INFO TaskSetManager: Starting task 5.0 in stage 13.0 (TID 47, localhost, executor driver, partition 5, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:54 INFO Executor: Running task 5.0 in stage 13.0 (TID 47)
17/12/23 18:51:54 INFO TaskSetManager: Finished task 4.0 in stage 13.0 (TID 46) in 487 ms on localhost (executor driver) (5/6)
17/12/23 18:51:54 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:51:54 INFO Executor: Finished task 5.0 in stage 13.0 (TID 47). 1238 bytes result sent to driver
17/12/23 18:51:54 INFO TaskSetManager: Finished task 5.0 in stage 13.0 (TID 47) in 427 ms on localhost (executor driver) (6/6)
17/12/23 18:51:54 INFO TaskSchedulerImpl: Removed TaskSet 13.0, whose tasks have all completed, from pool 
17/12/23 18:51:54 INFO DAGScheduler: ShuffleMapStage 13 (map at StackOverflow.scala:186) finished in 2.740 s
17/12/23 18:51:54 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:51:54 INFO DAGScheduler: running: Set()
17/12/23 18:51:54 INFO DAGScheduler: waiting: Set(ResultStage 14)
17/12/23 18:51:54 INFO DAGScheduler: failed: Set()
17/12/23 18:51:54 INFO DAGScheduler: Submitting ResultStage 14 (MapPartitionsRDD[23] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:51:54 INFO MemoryStore: Block broadcast_9 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:51:54 INFO MemoryStore: Block broadcast_9_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:51:54 INFO BlockManagerInfo: Added broadcast_9_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:51:54 INFO SparkContext: Created broadcast 9 from broadcast at DAGScheduler.scala:996
17/12/23 18:51:54 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 14 (MapPartitionsRDD[23] at mapValues at StackOverflow.scala:190)
17/12/23 18:51:54 INFO TaskSchedulerImpl: Adding task set 14.0 with 6 tasks
17/12/23 18:51:54 INFO TaskSetManager: Starting task 0.0 in stage 14.0 (TID 48, localhost, executor driver, partition 0, ANY, 5751 bytes)
17/12/23 18:51:54 INFO Executor: Running task 0.0 in stage 14.0 (TID 48)
17/12/23 18:51:54 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:55 INFO Executor: Finished task 0.0 in stage 14.0 (TID 48). 2072 bytes result sent to driver
17/12/23 18:51:55 INFO TaskSetManager: Starting task 1.0 in stage 14.0 (TID 49, localhost, executor driver, partition 1, ANY, 5751 bytes)
17/12/23 18:51:55 INFO Executor: Running task 1.0 in stage 14.0 (TID 49)
17/12/23 18:51:55 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:55 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:55 INFO TaskSetManager: Finished task 0.0 in stage 14.0 (TID 48) in 727 ms on localhost (executor driver) (1/6)
17/12/23 18:51:55 INFO BlockManagerInfo: Removed broadcast_7_piece0 on 10.0.0.115:50865 in memory (size: 3.7 KB, free: 1916.7 MB)
17/12/23 18:51:55 INFO BlockManagerInfo: Removed broadcast_8_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:51:55 INFO ContextCleaner: Cleaned shuffle 4
17/12/23 18:51:55 INFO Executor: Finished task 1.0 in stage 14.0 (TID 49). 2081 bytes result sent to driver
17/12/23 18:51:55 INFO TaskSetManager: Starting task 2.0 in stage 14.0 (TID 50, localhost, executor driver, partition 2, ANY, 5751 bytes)
17/12/23 18:51:55 INFO Executor: Running task 2.0 in stage 14.0 (TID 50)
17/12/23 18:51:55 INFO TaskSetManager: Finished task 1.0 in stage 14.0 (TID 49) in 140 ms on localhost (executor driver) (2/6)
17/12/23 18:51:55 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:55 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:51:55 INFO Executor: Finished task 2.0 in stage 14.0 (TID 50). 2072 bytes result sent to driver
17/12/23 18:51:55 INFO TaskSetManager: Starting task 3.0 in stage 14.0 (TID 51, localhost, executor driver, partition 3, ANY, 5751 bytes)
17/12/23 18:51:55 INFO Executor: Running task 3.0 in stage 14.0 (TID 51)
17/12/23 18:51:55 INFO TaskSetManager: Finished task 2.0 in stage 14.0 (TID 50) in 258 ms on localhost (executor driver) (3/6)
17/12/23 18:51:55 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:55 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:56 INFO Executor: Finished task 3.0 in stage 14.0 (TID 51). 2040 bytes result sent to driver
17/12/23 18:51:56 INFO TaskSetManager: Starting task 4.0 in stage 14.0 (TID 52, localhost, executor driver, partition 4, ANY, 5751 bytes)
17/12/23 18:51:56 INFO TaskSetManager: Finished task 3.0 in stage 14.0 (TID 51) in 517 ms on localhost (executor driver) (4/6)
17/12/23 18:51:56 INFO Executor: Running task 4.0 in stage 14.0 (TID 52)
17/12/23 18:51:56 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:56 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:56 INFO Executor: Finished task 4.0 in stage 14.0 (TID 52). 1953 bytes result sent to driver
17/12/23 18:51:56 INFO TaskSetManager: Finished task 4.0 in stage 14.0 (TID 52) in 642 ms on localhost (executor driver) (5/6)
17/12/23 18:51:56 INFO TaskSetManager: Starting task 5.0 in stage 14.0 (TID 53, localhost, executor driver, partition 5, ANY, 5751 bytes)
17/12/23 18:51:56 INFO Executor: Running task 5.0 in stage 14.0 (TID 53)
17/12/23 18:51:56 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:56 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:51:56 INFO Executor: Finished task 5.0 in stage 14.0 (TID 53). 1976 bytes result sent to driver
17/12/23 18:51:56 INFO TaskSetManager: Finished task 5.0 in stage 14.0 (TID 53) in 173 ms on localhost (executor driver) (6/6)
17/12/23 18:51:56 INFO TaskSchedulerImpl: Removed TaskSet 14.0, whose tasks have all completed, from pool 
17/12/23 18:51:56 INFO DAGScheduler: ResultStage 14 (collect at StackOverflow.scala:191) finished in 2.450 s
17/12/23 18:51:56 INFO DAGScheduler: Job 2 finished: collect at StackOverflow.scala:191, took 5.227259 s
Iteration: 2
  * current distance: 2123.0
  * desired distance: 20.0
  * means:
             (450000,2) ==>           (450000,2)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,10) ==>          (450000,13)    distance:        9
                  (0,3) ==>                (0,8)    distance:       25
                  (0,0) ==>                (0,0)    distance:        0
                  (0,0) ==>                (0,0)    distance:        0
             (600000,4) ==>           (600000,6)    distance:        4
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,0) ==>           (150000,0)    distance:        0
             (150000,4) ==>           (150000,5)    distance:        1
            (150000,32) ==>          (150000,63)    distance:      961
             (300000,6) ==>          (300000,15)    distance:       81
             (300000,0) ==>           (300000,0)    distance:        0
             (300000,1) ==>           (300000,1)    distance:        0
              (50000,3) ==>            (50000,7)    distance:       16
              (50000,0) ==>            (50000,0)    distance:        0
              (50000,0) ==>            (50000,0)    distance:        0
             (200000,0) ==>           (200000,1)    distance:        1
             (200000,0) ==>           (200000,0)    distance:        0
             (200000,7) ==>          (200000,12)    distance:       25
             (500000,0) ==>           (500000,0)    distance:        0
            (500000,11) ==>          (500000,18)    distance:       49
             (500000,3) ==>           (500000,3)    distance:        0
             (350000,0) ==>           (350000,0)    distance:        0
             (350000,0) ==>           (350000,0)    distance:        0
             (350000,5) ==>          (350000,12)    distance:       49
            (650000,28) ==>          (650000,35)    distance:       49
             (650000,1) ==>           (650000,1)    distance:        0
             (650000,5) ==>           (650000,7)    distance:        4
             (100000,0) ==>           (100000,0)    distance:        0
             (100000,5) ==>          (100000,11)    distance:       36
             (100000,2) ==>           (100000,2)    distance:        0
            (400000,16) ==>          (400000,38)    distance:      484
             (400000,0) ==>           (400000,0)    distance:        0
             (400000,3) ==>           (400000,3)    distance:        0
             (550000,1) ==>           (550000,2)    distance:        1
             (550000,7) ==>           (550000,9)    distance:        4
            (550000,53) ==>          (550000,69)    distance:      256
            (250000,11) ==>          (250000,19)    distance:       64
             (250000,0) ==>           (250000,0)    distance:        0
             (250000,3) ==>           (250000,3)    distance:        0
             (700000,0) ==>           (700000,0)    distance:        0
             (700000,5) ==>           (700000,7)    distance:        4
             (700000,1) ==>           (700000,1)    distance:        0
17/12/23 18:51:57 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:51:57 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:51:57 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:51:57 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:51:57 INFO DAGScheduler: Registering RDD 24 (map at StackOverflow.scala:186)
17/12/23 18:51:57 INFO DAGScheduler: Got job 3 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:51:57 INFO DAGScheduler: Final stage: ResultStage 19 (collect at StackOverflow.scala:191)
17/12/23 18:51:57 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 18)
17/12/23 18:51:57 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 18)
17/12/23 18:51:57 INFO DAGScheduler: Submitting ShuffleMapStage 18 (MapPartitionsRDD[24] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:51:57 INFO MemoryStore: Block broadcast_10 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:51:57 INFO MemoryStore: Block broadcast_10_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:51:57 INFO BlockManagerInfo: Added broadcast_10_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:51:57 INFO SparkContext: Created broadcast 10 from broadcast at DAGScheduler.scala:996
17/12/23 18:51:57 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 18 (MapPartitionsRDD[24] at map at StackOverflow.scala:186)
17/12/23 18:51:57 INFO TaskSchedulerImpl: Adding task set 18.0 with 6 tasks
17/12/23 18:51:57 INFO TaskSetManager: Starting task 0.0 in stage 18.0 (TID 54, localhost, executor driver, partition 0, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:57 INFO Executor: Running task 0.0 in stage 18.0 (TID 54)
17/12/23 18:51:57 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:51:57 INFO Executor: Finished task 0.0 in stage 18.0 (TID 54). 1325 bytes result sent to driver
17/12/23 18:51:57 INFO TaskSetManager: Starting task 1.0 in stage 18.0 (TID 55, localhost, executor driver, partition 1, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:57 INFO TaskSetManager: Finished task 0.0 in stage 18.0 (TID 54) in 446 ms on localhost (executor driver) (1/6)
17/12/23 18:51:57 INFO Executor: Running task 1.0 in stage 18.0 (TID 55)
17/12/23 18:51:57 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:51:57 INFO Executor: Finished task 1.0 in stage 18.0 (TID 55). 1325 bytes result sent to driver
17/12/23 18:51:57 INFO TaskSetManager: Starting task 2.0 in stage 18.0 (TID 56, localhost, executor driver, partition 2, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:57 INFO TaskSetManager: Finished task 1.0 in stage 18.0 (TID 55) in 422 ms on localhost (executor driver) (2/6)
17/12/23 18:51:57 INFO Executor: Running task 2.0 in stage 18.0 (TID 56)
17/12/23 18:51:57 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:51:58 INFO Executor: Finished task 2.0 in stage 18.0 (TID 56). 1238 bytes result sent to driver
17/12/23 18:51:58 INFO TaskSetManager: Starting task 3.0 in stage 18.0 (TID 57, localhost, executor driver, partition 3, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:58 INFO TaskSetManager: Finished task 2.0 in stage 18.0 (TID 56) in 488 ms on localhost (executor driver) (3/6)
17/12/23 18:51:58 INFO Executor: Running task 3.0 in stage 18.0 (TID 57)
17/12/23 18:51:58 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:51:58 INFO Executor: Finished task 3.0 in stage 18.0 (TID 57). 1325 bytes result sent to driver
17/12/23 18:51:58 INFO TaskSetManager: Starting task 4.0 in stage 18.0 (TID 58, localhost, executor driver, partition 4, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:58 INFO Executor: Running task 4.0 in stage 18.0 (TID 58)
17/12/23 18:51:58 INFO TaskSetManager: Finished task 3.0 in stage 18.0 (TID 57) in 435 ms on localhost (executor driver) (4/6)
17/12/23 18:51:58 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:51:59 INFO Executor: Finished task 4.0 in stage 18.0 (TID 58). 1238 bytes result sent to driver
17/12/23 18:51:59 INFO TaskSetManager: Starting task 5.0 in stage 18.0 (TID 59, localhost, executor driver, partition 5, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:51:59 INFO Executor: Running task 5.0 in stage 18.0 (TID 59)
17/12/23 18:51:59 INFO TaskSetManager: Finished task 4.0 in stage 18.0 (TID 58) in 480 ms on localhost (executor driver) (5/6)
17/12/23 18:51:59 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:51:59 INFO Executor: Finished task 5.0 in stage 18.0 (TID 59). 1325 bytes result sent to driver
17/12/23 18:51:59 INFO TaskSetManager: Finished task 5.0 in stage 18.0 (TID 59) in 435 ms on localhost (executor driver) (6/6)
17/12/23 18:51:59 INFO TaskSchedulerImpl: Removed TaskSet 18.0, whose tasks have all completed, from pool 
17/12/23 18:51:59 INFO DAGScheduler: ShuffleMapStage 18 (map at StackOverflow.scala:186) finished in 2.703 s
17/12/23 18:51:59 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:51:59 INFO DAGScheduler: running: Set()
17/12/23 18:51:59 INFO DAGScheduler: waiting: Set(ResultStage 19)
17/12/23 18:51:59 INFO DAGScheduler: failed: Set()
17/12/23 18:51:59 INFO DAGScheduler: Submitting ResultStage 19 (MapPartitionsRDD[26] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:51:59 INFO MemoryStore: Block broadcast_11 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:51:59 INFO MemoryStore: Block broadcast_11_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:51:59 INFO BlockManagerInfo: Added broadcast_11_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:51:59 INFO SparkContext: Created broadcast 11 from broadcast at DAGScheduler.scala:996
17/12/23 18:51:59 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 19 (MapPartitionsRDD[26] at mapValues at StackOverflow.scala:190)
17/12/23 18:51:59 INFO TaskSchedulerImpl: Adding task set 19.0 with 6 tasks
17/12/23 18:51:59 INFO TaskSetManager: Starting task 0.0 in stage 19.0 (TID 60, localhost, executor driver, partition 0, ANY, 5751 bytes)
17/12/23 18:51:59 INFO Executor: Running task 0.0 in stage 19.0 (TID 60)
17/12/23 18:51:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:51:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:00 INFO ContextCleaner: Cleaned shuffle 5
17/12/23 18:52:00 INFO BlockManagerInfo: Removed broadcast_9_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:00 INFO BlockManagerInfo: Removed broadcast_10_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:00 INFO Executor: Finished task 0.0 in stage 19.0 (TID 60). 2145 bytes result sent to driver
17/12/23 18:52:00 INFO TaskSetManager: Starting task 1.0 in stage 19.0 (TID 61, localhost, executor driver, partition 1, ANY, 5751 bytes)
17/12/23 18:52:00 INFO Executor: Running task 1.0 in stage 19.0 (TID 61)
17/12/23 18:52:00 INFO TaskSetManager: Finished task 0.0 in stage 19.0 (TID 60) in 712 ms on localhost (executor driver) (1/6)
17/12/23 18:52:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:00 INFO Executor: Finished task 1.0 in stage 19.0 (TID 61). 2040 bytes result sent to driver
17/12/23 18:52:00 INFO TaskSetManager: Starting task 2.0 in stage 19.0 (TID 62, localhost, executor driver, partition 2, ANY, 5751 bytes)
17/12/23 18:52:00 INFO Executor: Running task 2.0 in stage 19.0 (TID 62)
17/12/23 18:52:00 INFO TaskSetManager: Finished task 1.0 in stage 19.0 (TID 61) in 205 ms on localhost (executor driver) (2/6)
17/12/23 18:52:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:00 INFO Executor: Finished task 2.0 in stage 19.0 (TID 62). 2072 bytes result sent to driver
17/12/23 18:52:00 INFO TaskSetManager: Starting task 3.0 in stage 19.0 (TID 63, localhost, executor driver, partition 3, ANY, 5751 bytes)
17/12/23 18:52:00 INFO Executor: Running task 3.0 in stage 19.0 (TID 63)
17/12/23 18:52:00 INFO TaskSetManager: Finished task 2.0 in stage 19.0 (TID 62) in 257 ms on localhost (executor driver) (3/6)
17/12/23 18:52:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:01 INFO Executor: Finished task 3.0 in stage 19.0 (TID 63). 1953 bytes result sent to driver
17/12/23 18:52:01 INFO TaskSetManager: Starting task 4.0 in stage 19.0 (TID 64, localhost, executor driver, partition 4, ANY, 5751 bytes)
17/12/23 18:52:01 INFO TaskSetManager: Finished task 3.0 in stage 19.0 (TID 63) in 365 ms on localhost (executor driver) (4/6)
17/12/23 18:52:01 INFO Executor: Running task 4.0 in stage 19.0 (TID 64)
17/12/23 18:52:01 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:01 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:02 INFO Executor: Finished task 4.0 in stage 19.0 (TID 64). 1953 bytes result sent to driver
17/12/23 18:52:02 INFO TaskSetManager: Starting task 5.0 in stage 19.0 (TID 65, localhost, executor driver, partition 5, ANY, 5751 bytes)
17/12/23 18:52:02 INFO Executor: Running task 5.0 in stage 19.0 (TID 65)
17/12/23 18:52:02 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:02 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:02 INFO TaskSetManager: Finished task 4.0 in stage 19.0 (TID 64) in 885 ms on localhost (executor driver) (5/6)
17/12/23 18:52:02 INFO Executor: Finished task 5.0 in stage 19.0 (TID 65). 1976 bytes result sent to driver
17/12/23 18:52:02 INFO TaskSetManager: Finished task 5.0 in stage 19.0 (TID 65) in 220 ms on localhost (executor driver) (6/6)
17/12/23 18:52:02 INFO TaskSchedulerImpl: Removed TaskSet 19.0, whose tasks have all completed, from pool 
17/12/23 18:52:02 INFO DAGScheduler: ResultStage 19 (collect at StackOverflow.scala:191) finished in 2.639 s
17/12/23 18:52:02 INFO DAGScheduler: Job 3 finished: collect at StackOverflow.scala:191, took 5.367156 s
Iteration: 3
  * current distance: 4248.0
  * desired distance: 20.0
  * means:
             (450000,2) ==>           (450000,2)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,13) ==>          (450000,15)    distance:        4
                  (0,8) ==>               (0,17)    distance:       81
                  (0,0) ==>                (0,0)    distance:        0
                  (0,0) ==>                (0,0)    distance:        0
             (600000,6) ==>           (600000,7)    distance:        1
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,0) ==>           (150000,0)    distance:        0
             (150000,5) ==>           (150000,6)    distance:        1
            (150000,63) ==>         (150000,106)    distance:     1849
            (300000,15) ==>          (300000,30)    distance:      225
             (300000,0) ==>           (300000,0)    distance:        0
             (300000,1) ==>           (300000,2)    distance:        1
              (50000,7) ==>           (50000,13)    distance:       36
              (50000,0) ==>            (50000,1)    distance:        1
              (50000,0) ==>            (50000,0)    distance:        0
             (200000,1) ==>           (200000,2)    distance:        1
             (200000,0) ==>           (200000,0)    distance:        0
            (200000,12) ==>          (200000,21)    distance:       81
             (500000,0) ==>           (500000,0)    distance:        0
            (500000,18) ==>          (500000,27)    distance:       81
             (500000,3) ==>           (500000,4)    distance:        1
             (350000,0) ==>           (350000,1)    distance:        1
             (350000,0) ==>           (350000,0)    distance:        0
            (350000,12) ==>          (350000,30)    distance:      324
            (650000,35) ==>          (650000,42)    distance:       49
             (650000,1) ==>           (650000,1)    distance:        0
             (650000,7) ==>           (650000,8)    distance:        1
             (100000,0) ==>           (100000,0)    distance:        0
            (100000,11) ==>          (100000,21)    distance:      100
             (100000,2) ==>           (100000,2)    distance:        0
            (400000,38) ==>          (400000,70)    distance:     1024
             (400000,0) ==>           (400000,0)    distance:        0
             (400000,3) ==>           (400000,4)    distance:        1
             (550000,2) ==>           (550000,2)    distance:        0
             (550000,9) ==>          (550000,11)    distance:        4
            (550000,69) ==>          (550000,85)    distance:      256
            (250000,19) ==>          (250000,30)    distance:      121
             (250000,0) ==>           (250000,0)    distance:        0
             (250000,3) ==>           (250000,3)    distance:        0
             (700000,0) ==>           (700000,0)    distance:        0
             (700000,7) ==>           (700000,9)    distance:        4
             (700000,1) ==>           (700000,1)    distance:        0
17/12/23 18:52:02 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:52:02 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:52:02 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:52:02 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:52:02 INFO DAGScheduler: Registering RDD 27 (map at StackOverflow.scala:186)
17/12/23 18:52:02 INFO DAGScheduler: Got job 4 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:52:02 INFO DAGScheduler: Final stage: ResultStage 24 (collect at StackOverflow.scala:191)
17/12/23 18:52:02 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 23)
17/12/23 18:52:02 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 23)
17/12/23 18:52:02 INFO DAGScheduler: Submitting ShuffleMapStage 23 (MapPartitionsRDD[27] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:52:02 INFO MemoryStore: Block broadcast_12 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:52:02 INFO MemoryStore: Block broadcast_12_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:52:02 INFO BlockManagerInfo: Added broadcast_12_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:02 INFO SparkContext: Created broadcast 12 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:02 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 23 (MapPartitionsRDD[27] at map at StackOverflow.scala:186)
17/12/23 18:52:02 INFO TaskSchedulerImpl: Adding task set 23.0 with 6 tasks
17/12/23 18:52:02 INFO TaskSetManager: Starting task 0.0 in stage 23.0 (TID 66, localhost, executor driver, partition 0, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:02 INFO Executor: Running task 0.0 in stage 23.0 (TID 66)
17/12/23 18:52:02 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:52:02 INFO Executor: Finished task 0.0 in stage 23.0 (TID 66). 1325 bytes result sent to driver
17/12/23 18:52:02 INFO TaskSetManager: Starting task 1.0 in stage 23.0 (TID 67, localhost, executor driver, partition 1, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:02 INFO Executor: Running task 1.0 in stage 23.0 (TID 67)
17/12/23 18:52:02 INFO TaskSetManager: Finished task 0.0 in stage 23.0 (TID 66) in 437 ms on localhost (executor driver) (1/6)
17/12/23 18:52:02 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:52:03 INFO Executor: Finished task 1.0 in stage 23.0 (TID 67). 1238 bytes result sent to driver
17/12/23 18:52:03 INFO TaskSetManager: Starting task 2.0 in stage 23.0 (TID 68, localhost, executor driver, partition 2, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:03 INFO Executor: Running task 2.0 in stage 23.0 (TID 68)
17/12/23 18:52:03 INFO TaskSetManager: Finished task 1.0 in stage 23.0 (TID 67) in 468 ms on localhost (executor driver) (2/6)
17/12/23 18:52:03 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:52:03 INFO Executor: Finished task 2.0 in stage 23.0 (TID 68). 1238 bytes result sent to driver
17/12/23 18:52:03 INFO TaskSetManager: Starting task 3.0 in stage 23.0 (TID 69, localhost, executor driver, partition 3, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:03 INFO TaskSetManager: Finished task 2.0 in stage 23.0 (TID 68) in 417 ms on localhost (executor driver) (3/6)
17/12/23 18:52:03 INFO Executor: Running task 3.0 in stage 23.0 (TID 69)
17/12/23 18:52:03 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:52:04 INFO Executor: Finished task 3.0 in stage 23.0 (TID 69). 1325 bytes result sent to driver
17/12/23 18:52:04 INFO TaskSetManager: Starting task 4.0 in stage 23.0 (TID 70, localhost, executor driver, partition 4, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:04 INFO TaskSetManager: Finished task 3.0 in stage 23.0 (TID 69) in 450 ms on localhost (executor driver) (4/6)
17/12/23 18:52:04 INFO Executor: Running task 4.0 in stage 23.0 (TID 70)
17/12/23 18:52:04 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:52:04 INFO Executor: Finished task 4.0 in stage 23.0 (TID 70). 1238 bytes result sent to driver
17/12/23 18:52:04 INFO TaskSetManager: Starting task 5.0 in stage 23.0 (TID 71, localhost, executor driver, partition 5, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:04 INFO TaskSetManager: Finished task 4.0 in stage 23.0 (TID 70) in 438 ms on localhost (executor driver) (5/6)
17/12/23 18:52:04 INFO Executor: Running task 5.0 in stage 23.0 (TID 71)
17/12/23 18:52:04 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:52:05 INFO Executor: Finished task 5.0 in stage 23.0 (TID 71). 1238 bytes result sent to driver
17/12/23 18:52:05 INFO TaskSetManager: Finished task 5.0 in stage 23.0 (TID 71) in 440 ms on localhost (executor driver) (6/6)
17/12/23 18:52:05 INFO TaskSchedulerImpl: Removed TaskSet 23.0, whose tasks have all completed, from pool 
17/12/23 18:52:05 INFO DAGScheduler: ShuffleMapStage 23 (map at StackOverflow.scala:186) finished in 2.650 s
17/12/23 18:52:05 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:52:05 INFO DAGScheduler: running: Set()
17/12/23 18:52:05 INFO DAGScheduler: waiting: Set(ResultStage 24)
17/12/23 18:52:05 INFO DAGScheduler: failed: Set()
17/12/23 18:52:05 INFO DAGScheduler: Submitting ResultStage 24 (MapPartitionsRDD[29] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:52:05 INFO MemoryStore: Block broadcast_13 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:52:05 INFO MemoryStore: Block broadcast_13_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:52:05 INFO BlockManagerInfo: Added broadcast_13_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:05 INFO SparkContext: Created broadcast 13 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:05 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 24 (MapPartitionsRDD[29] at mapValues at StackOverflow.scala:190)
17/12/23 18:52:05 INFO TaskSchedulerImpl: Adding task set 24.0 with 6 tasks
17/12/23 18:52:05 INFO TaskSetManager: Starting task 0.0 in stage 24.0 (TID 72, localhost, executor driver, partition 0, ANY, 5751 bytes)
17/12/23 18:52:05 INFO Executor: Running task 0.0 in stage 24.0 (TID 72)
17/12/23 18:52:05 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:05 INFO ContextCleaner: Cleaned shuffle 6
17/12/23 18:52:05 INFO BlockManagerInfo: Removed broadcast_11_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:05 INFO BlockManagerInfo: Removed broadcast_12_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:05 INFO Executor: Finished task 0.0 in stage 24.0 (TID 72). 2145 bytes result sent to driver
17/12/23 18:52:05 INFO TaskSetManager: Starting task 1.0 in stage 24.0 (TID 73, localhost, executor driver, partition 1, ANY, 5751 bytes)
17/12/23 18:52:05 INFO TaskSetManager: Finished task 0.0 in stage 24.0 (TID 72) in 677 ms on localhost (executor driver) (1/6)
17/12/23 18:52:05 INFO Executor: Running task 1.0 in stage 24.0 (TID 73)
17/12/23 18:52:05 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:05 INFO Executor: Finished task 1.0 in stage 24.0 (TID 73). 2072 bytes result sent to driver
17/12/23 18:52:05 INFO TaskSetManager: Starting task 2.0 in stage 24.0 (TID 74, localhost, executor driver, partition 2, ANY, 5751 bytes)
17/12/23 18:52:05 INFO Executor: Running task 2.0 in stage 24.0 (TID 74)
17/12/23 18:52:05 INFO TaskSetManager: Finished task 1.0 in stage 24.0 (TID 73) in 215 ms on localhost (executor driver) (2/6)
17/12/23 18:52:05 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:06 INFO Executor: Finished task 2.0 in stage 24.0 (TID 74). 1985 bytes result sent to driver
17/12/23 18:52:06 INFO TaskSetManager: Starting task 3.0 in stage 24.0 (TID 75, localhost, executor driver, partition 3, ANY, 5751 bytes)
17/12/23 18:52:06 INFO TaskSetManager: Finished task 2.0 in stage 24.0 (TID 74) in 218 ms on localhost (executor driver) (3/6)
17/12/23 18:52:06 INFO Executor: Running task 3.0 in stage 24.0 (TID 75)
17/12/23 18:52:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:06 INFO Executor: Finished task 3.0 in stage 24.0 (TID 75). 2040 bytes result sent to driver
17/12/23 18:52:06 INFO TaskSetManager: Starting task 4.0 in stage 24.0 (TID 76, localhost, executor driver, partition 4, ANY, 5751 bytes)
17/12/23 18:52:06 INFO TaskSetManager: Finished task 3.0 in stage 24.0 (TID 75) in 282 ms on localhost (executor driver) (4/6)
17/12/23 18:52:06 INFO Executor: Running task 4.0 in stage 24.0 (TID 76)
17/12/23 18:52:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:07 INFO Executor: Finished task 4.0 in stage 24.0 (TID 76). 1953 bytes result sent to driver
17/12/23 18:52:07 INFO TaskSetManager: Starting task 5.0 in stage 24.0 (TID 77, localhost, executor driver, partition 5, ANY, 5751 bytes)
17/12/23 18:52:07 INFO TaskSetManager: Finished task 4.0 in stage 24.0 (TID 76) in 887 ms on localhost (executor driver) (5/6)
17/12/23 18:52:07 INFO Executor: Running task 5.0 in stage 24.0 (TID 77)
17/12/23 18:52:07 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:07 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:07 INFO Executor: Finished task 5.0 in stage 24.0 (TID 77). 2008 bytes result sent to driver
17/12/23 18:52:07 INFO TaskSetManager: Finished task 5.0 in stage 24.0 (TID 77) in 260 ms on localhost (executor driver) (6/6)
17/12/23 18:52:07 INFO TaskSchedulerImpl: Removed TaskSet 24.0, whose tasks have all completed, from pool 
17/12/23 18:52:07 INFO DAGScheduler: ResultStage 24 (collect at StackOverflow.scala:191) finished in 2.529 s
17/12/23 18:52:07 INFO DAGScheduler: Job 4 finished: collect at StackOverflow.scala:191, took 5.201929 s
Iteration: 4
  * current distance: 8074.0
  * desired distance: 20.0
  * means:
             (450000,2) ==>           (450000,3)    distance:        1
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,15) ==>          (450000,17)    distance:        4
                 (0,17) ==>               (0,42)    distance:      625
                  (0,0) ==>                (0,1)    distance:        1
                  (0,0) ==>                (0,0)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,0) ==>           (150000,1)    distance:        1
             (150000,6) ==>           (150000,9)    distance:        9
           (150000,106) ==>         (150000,163)    distance:     3249
            (300000,30) ==>          (300000,52)    distance:      484
             (300000,0) ==>           (300000,0)    distance:        0
             (300000,2) ==>           (300000,4)    distance:        4
             (50000,13) ==>           (50000,23)    distance:      100
              (50000,1) ==>            (50000,2)    distance:        1
              (50000,0) ==>            (50000,0)    distance:        0
             (200000,2) ==>           (200000,2)    distance:        0
             (200000,0) ==>           (200000,0)    distance:        0
            (200000,21) ==>          (200000,35)    distance:      196
             (500000,0) ==>           (500000,0)    distance:        0
            (500000,27) ==>          (500000,38)    distance:      121
             (500000,4) ==>           (500000,5)    distance:        1
             (350000,1) ==>           (350000,2)    distance:        1
             (350000,0) ==>           (350000,0)    distance:        0
            (350000,30) ==>          (350000,61)    distance:      961
            (650000,42) ==>          (650000,49)    distance:       49
             (650000,1) ==>           (650000,1)    distance:        0
             (650000,8) ==>           (650000,9)    distance:        1
             (100000,0) ==>           (100000,0)    distance:        0
            (100000,21) ==>          (100000,36)    distance:      225
             (100000,2) ==>           (100000,3)    distance:        1
            (400000,70) ==>         (400000,108)    distance:     1444
             (400000,0) ==>           (400000,0)    distance:        0
             (400000,4) ==>           (400000,7)    distance:        9
             (550000,2) ==>           (550000,2)    distance:        0
            (550000,11) ==>          (550000,12)    distance:        1
            (550000,85) ==>         (550000,103)    distance:      324
            (250000,30) ==>          (250000,46)    distance:      256
             (250000,0) ==>           (250000,0)    distance:        0
             (250000,3) ==>           (250000,4)    distance:        1
             (700000,0) ==>           (700000,0)    distance:        0
             (700000,9) ==>          (700000,11)    distance:        4
             (700000,1) ==>           (700000,1)    distance:        0
17/12/23 18:52:07 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:52:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:52:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:52:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:52:07 INFO DAGScheduler: Registering RDD 30 (map at StackOverflow.scala:186)
17/12/23 18:52:07 INFO DAGScheduler: Got job 5 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:52:07 INFO DAGScheduler: Final stage: ResultStage 29 (collect at StackOverflow.scala:191)
17/12/23 18:52:07 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 28)
17/12/23 18:52:07 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 28)
17/12/23 18:52:07 INFO DAGScheduler: Submitting ShuffleMapStage 28 (MapPartitionsRDD[30] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:52:07 INFO MemoryStore: Block broadcast_14 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:52:07 INFO MemoryStore: Block broadcast_14_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:52:07 INFO BlockManagerInfo: Added broadcast_14_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:07 INFO SparkContext: Created broadcast 14 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:07 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 28 (MapPartitionsRDD[30] at map at StackOverflow.scala:186)
17/12/23 18:52:07 INFO TaskSchedulerImpl: Adding task set 28.0 with 6 tasks
17/12/23 18:52:07 INFO TaskSetManager: Starting task 0.0 in stage 28.0 (TID 78, localhost, executor driver, partition 0, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:07 INFO Executor: Running task 0.0 in stage 28.0 (TID 78)
17/12/23 18:52:07 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:52:08 INFO Executor: Finished task 0.0 in stage 28.0 (TID 78). 1325 bytes result sent to driver
17/12/23 18:52:08 INFO TaskSetManager: Starting task 1.0 in stage 28.0 (TID 79, localhost, executor driver, partition 1, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:08 INFO TaskSetManager: Finished task 0.0 in stage 28.0 (TID 78) in 445 ms on localhost (executor driver) (1/6)
17/12/23 18:52:08 INFO Executor: Running task 1.0 in stage 28.0 (TID 79)
17/12/23 18:52:08 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:52:08 INFO Executor: Finished task 1.0 in stage 28.0 (TID 79). 1415 bytes result sent to driver
17/12/23 18:52:08 INFO TaskSetManager: Starting task 2.0 in stage 28.0 (TID 80, localhost, executor driver, partition 2, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:08 INFO TaskSetManager: Finished task 1.0 in stage 28.0 (TID 79) in 470 ms on localhost (executor driver) (2/6)
17/12/23 18:52:08 INFO Executor: Running task 2.0 in stage 28.0 (TID 80)
17/12/23 18:52:08 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:52:09 INFO Executor: Finished task 2.0 in stage 28.0 (TID 80). 1325 bytes result sent to driver
17/12/23 18:52:09 INFO TaskSetManager: Starting task 3.0 in stage 28.0 (TID 81, localhost, executor driver, partition 3, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:09 INFO Executor: Running task 3.0 in stage 28.0 (TID 81)
17/12/23 18:52:09 INFO TaskSetManager: Finished task 2.0 in stage 28.0 (TID 80) in 427 ms on localhost (executor driver) (3/6)
17/12/23 18:52:09 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:52:09 INFO Executor: Finished task 3.0 in stage 28.0 (TID 81). 1238 bytes result sent to driver
17/12/23 18:52:09 INFO TaskSetManager: Starting task 4.0 in stage 28.0 (TID 82, localhost, executor driver, partition 4, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:09 INFO TaskSetManager: Finished task 3.0 in stage 28.0 (TID 81) in 483 ms on localhost (executor driver) (4/6)
17/12/23 18:52:09 INFO Executor: Running task 4.0 in stage 28.0 (TID 82)
17/12/23 18:52:09 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:52:09 INFO Executor: Finished task 4.0 in stage 28.0 (TID 82). 1238 bytes result sent to driver
17/12/23 18:52:09 INFO TaskSetManager: Starting task 5.0 in stage 28.0 (TID 83, localhost, executor driver, partition 5, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:09 INFO TaskSetManager: Finished task 4.0 in stage 28.0 (TID 82) in 497 ms on localhost (executor driver) (5/6)
17/12/23 18:52:09 INFO Executor: Running task 5.0 in stage 28.0 (TID 83)
17/12/23 18:52:09 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:52:10 INFO Executor: Finished task 5.0 in stage 28.0 (TID 83). 1238 bytes result sent to driver
17/12/23 18:52:10 INFO TaskSetManager: Finished task 5.0 in stage 28.0 (TID 83) in 498 ms on localhost (executor driver) (6/6)
17/12/23 18:52:10 INFO TaskSchedulerImpl: Removed TaskSet 28.0, whose tasks have all completed, from pool 
17/12/23 18:52:10 INFO DAGScheduler: ShuffleMapStage 28 (map at StackOverflow.scala:186) finished in 2.820 s
17/12/23 18:52:10 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:52:10 INFO DAGScheduler: running: Set()
17/12/23 18:52:10 INFO DAGScheduler: waiting: Set(ResultStage 29)
17/12/23 18:52:10 INFO DAGScheduler: failed: Set()
17/12/23 18:52:10 INFO DAGScheduler: Submitting ResultStage 29 (MapPartitionsRDD[32] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:52:10 INFO MemoryStore: Block broadcast_15 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:52:10 INFO MemoryStore: Block broadcast_15_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:52:10 INFO BlockManagerInfo: Added broadcast_15_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:10 INFO SparkContext: Created broadcast 15 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:10 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 29 (MapPartitionsRDD[32] at mapValues at StackOverflow.scala:190)
17/12/23 18:52:10 INFO TaskSchedulerImpl: Adding task set 29.0 with 6 tasks
17/12/23 18:52:10 INFO TaskSetManager: Starting task 0.0 in stage 29.0 (TID 84, localhost, executor driver, partition 0, ANY, 5751 bytes)
17/12/23 18:52:10 INFO Executor: Running task 0.0 in stage 29.0 (TID 84)
17/12/23 18:52:10 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:10 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:10 INFO ContextCleaner: Cleaned shuffle 7
17/12/23 18:52:10 INFO BlockManagerInfo: Removed broadcast_13_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:10 INFO BlockManagerInfo: Removed broadcast_14_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:11 INFO Executor: Finished task 0.0 in stage 29.0 (TID 84). 2145 bytes result sent to driver
17/12/23 18:52:11 INFO TaskSetManager: Starting task 1.0 in stage 29.0 (TID 85, localhost, executor driver, partition 1, ANY, 5751 bytes)
17/12/23 18:52:11 INFO Executor: Running task 1.0 in stage 29.0 (TID 85)
17/12/23 18:52:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:11 INFO TaskSetManager: Finished task 0.0 in stage 29.0 (TID 84) in 620 ms on localhost (executor driver) (1/6)
17/12/23 18:52:11 INFO Executor: Finished task 1.0 in stage 29.0 (TID 85). 2072 bytes result sent to driver
17/12/23 18:52:11 INFO TaskSetManager: Starting task 2.0 in stage 29.0 (TID 86, localhost, executor driver, partition 2, ANY, 5751 bytes)
17/12/23 18:52:11 INFO Executor: Running task 2.0 in stage 29.0 (TID 86)
17/12/23 18:52:11 INFO TaskSetManager: Finished task 1.0 in stage 29.0 (TID 85) in 324 ms on localhost (executor driver) (2/6)
17/12/23 18:52:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:11 INFO Executor: Finished task 2.0 in stage 29.0 (TID 86). 2162 bytes result sent to driver
17/12/23 18:52:11 INFO TaskSetManager: Starting task 3.0 in stage 29.0 (TID 87, localhost, executor driver, partition 3, ANY, 5751 bytes)
17/12/23 18:52:11 INFO Executor: Running task 3.0 in stage 29.0 (TID 87)
17/12/23 18:52:11 INFO TaskSetManager: Finished task 2.0 in stage 29.0 (TID 86) in 168 ms on localhost (executor driver) (3/6)
17/12/23 18:52:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:52:11 INFO Executor: Finished task 3.0 in stage 29.0 (TID 87). 1953 bytes result sent to driver
17/12/23 18:52:11 INFO TaskSetManager: Starting task 4.0 in stage 29.0 (TID 88, localhost, executor driver, partition 4, ANY, 5751 bytes)
17/12/23 18:52:11 INFO Executor: Running task 4.0 in stage 29.0 (TID 88)
17/12/23 18:52:11 INFO TaskSetManager: Finished task 3.0 in stage 29.0 (TID 87) in 212 ms on localhost (executor driver) (4/6)
17/12/23 18:52:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:12 INFO Executor: Finished task 4.0 in stage 29.0 (TID 88). 2040 bytes result sent to driver
17/12/23 18:52:12 INFO TaskSetManager: Starting task 5.0 in stage 29.0 (TID 89, localhost, executor driver, partition 5, ANY, 5751 bytes)
17/12/23 18:52:12 INFO Executor: Running task 5.0 in stage 29.0 (TID 89)
17/12/23 18:52:12 INFO TaskSetManager: Finished task 4.0 in stage 29.0 (TID 88) in 798 ms on localhost (executor driver) (5/6)
17/12/23 18:52:12 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:13 INFO Executor: Finished task 5.0 in stage 29.0 (TID 89). 2040 bytes result sent to driver
17/12/23 18:52:13 INFO TaskSetManager: Finished task 5.0 in stage 29.0 (TID 89) in 435 ms on localhost (executor driver) (6/6)
17/12/23 18:52:13 INFO TaskSchedulerImpl: Removed TaskSet 29.0, whose tasks have all completed, from pool 
17/12/23 18:52:13 INFO DAGScheduler: ResultStage 29 (collect at StackOverflow.scala:191) finished in 2.555 s
17/12/23 18:52:13 INFO DAGScheduler: Job 5 finished: collect at StackOverflow.scala:191, took 5.387520 s
Iteration: 5
  * current distance: 13991.0
  * desired distance: 20.0
  * means:
             (450000,3) ==>           (450000,4)    distance:        1
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,17) ==>          (450000,20)    distance:        9
                 (0,42) ==>               (0,92)    distance:     2500
                  (0,1) ==>                (0,2)    distance:        1
                  (0,0) ==>                (0,0)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,1) ==>           (150000,1)    distance:        0
             (150000,9) ==>          (150000,14)    distance:       25
           (150000,163) ==>         (150000,233)    distance:     4900
            (300000,52) ==>          (300000,77)    distance:      625
             (300000,0) ==>           (300000,0)    distance:        0
             (300000,4) ==>           (300000,6)    distance:        4
             (50000,23) ==>           (50000,42)    distance:      361
              (50000,2) ==>            (50000,2)    distance:        0
              (50000,0) ==>            (50000,0)    distance:        0
             (200000,2) ==>           (200000,3)    distance:        1
             (200000,0) ==>           (200000,0)    distance:        0
            (200000,35) ==>          (200000,54)    distance:      361
             (500000,0) ==>           (500000,0)    distance:        0
            (500000,38) ==>          (500000,50)    distance:      144
             (500000,5) ==>           (500000,6)    distance:        1
             (350000,2) ==>           (350000,2)    distance:        0
             (350000,0) ==>           (350000,0)    distance:        0
            (350000,61) ==>         (350000,103)    distance:     1764
            (650000,49) ==>          (650000,56)    distance:       49
             (650000,1) ==>           (650000,2)    distance:        1
             (650000,9) ==>          (650000,10)    distance:        1
             (100000,0) ==>           (100000,0)    distance:        0
            (100000,36) ==>          (100000,56)    distance:      400
             (100000,3) ==>           (100000,3)    distance:        0
           (400000,108) ==>         (400000,152)    distance:     1936
             (400000,0) ==>           (400000,0)    distance:        0
             (400000,7) ==>          (400000,10)    distance:        9
             (550000,2) ==>           (550000,3)    distance:        1
            (550000,12) ==>          (550000,14)    distance:        4
           (550000,103) ==>         (550000,123)    distance:      400
            (250000,46) ==>          (250000,68)    distance:      484
             (250000,0) ==>           (250000,0)    distance:        0
             (250000,4) ==>           (250000,6)    distance:        4
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,11) ==>          (700000,13)    distance:        4
             (700000,1) ==>           (700000,2)    distance:        1
17/12/23 18:52:13 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:52:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:52:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:52:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:52:13 INFO DAGScheduler: Registering RDD 33 (map at StackOverflow.scala:186)
17/12/23 18:52:13 INFO DAGScheduler: Got job 6 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:52:13 INFO DAGScheduler: Final stage: ResultStage 34 (collect at StackOverflow.scala:191)
17/12/23 18:52:13 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 33)
17/12/23 18:52:13 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 33)
17/12/23 18:52:13 INFO DAGScheduler: Submitting ShuffleMapStage 33 (MapPartitionsRDD[33] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:52:13 INFO MemoryStore: Block broadcast_16 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:52:13 INFO MemoryStore: Block broadcast_16_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:52:13 INFO BlockManagerInfo: Added broadcast_16_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:13 INFO SparkContext: Created broadcast 16 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:13 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 33 (MapPartitionsRDD[33] at map at StackOverflow.scala:186)
17/12/23 18:52:13 INFO TaskSchedulerImpl: Adding task set 33.0 with 6 tasks
17/12/23 18:52:13 INFO TaskSetManager: Starting task 0.0 in stage 33.0 (TID 90, localhost, executor driver, partition 0, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:13 INFO Executor: Running task 0.0 in stage 33.0 (TID 90)
17/12/23 18:52:13 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:52:13 INFO Executor: Finished task 0.0 in stage 33.0 (TID 90). 1415 bytes result sent to driver
17/12/23 18:52:13 INFO TaskSetManager: Starting task 1.0 in stage 33.0 (TID 91, localhost, executor driver, partition 1, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:13 INFO Executor: Running task 1.0 in stage 33.0 (TID 91)
17/12/23 18:52:13 INFO TaskSetManager: Finished task 0.0 in stage 33.0 (TID 90) in 501 ms on localhost (executor driver) (1/6)
17/12/23 18:52:13 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:52:14 INFO Executor: Finished task 1.0 in stage 33.0 (TID 91). 1238 bytes result sent to driver
17/12/23 18:52:14 INFO TaskSetManager: Starting task 2.0 in stage 33.0 (TID 92, localhost, executor driver, partition 2, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:14 INFO Executor: Running task 2.0 in stage 33.0 (TID 92)
17/12/23 18:52:14 INFO TaskSetManager: Finished task 1.0 in stage 33.0 (TID 91) in 460 ms on localhost (executor driver) (2/6)
17/12/23 18:52:14 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:52:14 INFO Executor: Finished task 2.0 in stage 33.0 (TID 92). 1325 bytes result sent to driver
17/12/23 18:52:14 INFO TaskSetManager: Starting task 3.0 in stage 33.0 (TID 93, localhost, executor driver, partition 3, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:14 INFO TaskSetManager: Finished task 2.0 in stage 33.0 (TID 92) in 515 ms on localhost (executor driver) (3/6)
17/12/23 18:52:14 INFO Executor: Running task 3.0 in stage 33.0 (TID 93)
17/12/23 18:52:14 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:52:15 INFO Executor: Finished task 3.0 in stage 33.0 (TID 93). 1325 bytes result sent to driver
17/12/23 18:52:15 INFO TaskSetManager: Starting task 4.0 in stage 33.0 (TID 94, localhost, executor driver, partition 4, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:15 INFO TaskSetManager: Finished task 3.0 in stage 33.0 (TID 93) in 467 ms on localhost (executor driver) (4/6)
17/12/23 18:52:15 INFO Executor: Running task 4.0 in stage 33.0 (TID 94)
17/12/23 18:52:15 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:52:15 INFO Executor: Finished task 4.0 in stage 33.0 (TID 94). 1325 bytes result sent to driver
17/12/23 18:52:15 INFO TaskSetManager: Starting task 5.0 in stage 33.0 (TID 95, localhost, executor driver, partition 5, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:15 INFO Executor: Running task 5.0 in stage 33.0 (TID 95)
17/12/23 18:52:15 INFO TaskSetManager: Finished task 4.0 in stage 33.0 (TID 94) in 505 ms on localhost (executor driver) (5/6)
17/12/23 18:52:15 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:52:15 INFO Executor: Finished task 5.0 in stage 33.0 (TID 95). 1325 bytes result sent to driver
17/12/23 18:52:15 INFO TaskSetManager: Finished task 5.0 in stage 33.0 (TID 95) in 458 ms on localhost (executor driver) (6/6)
17/12/23 18:52:15 INFO TaskSchedulerImpl: Removed TaskSet 33.0, whose tasks have all completed, from pool 
17/12/23 18:52:15 INFO DAGScheduler: ShuffleMapStage 33 (map at StackOverflow.scala:186) finished in 2.903 s
17/12/23 18:52:15 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:52:15 INFO DAGScheduler: running: Set()
17/12/23 18:52:15 INFO DAGScheduler: waiting: Set(ResultStage 34)
17/12/23 18:52:15 INFO DAGScheduler: failed: Set()
17/12/23 18:52:15 INFO DAGScheduler: Submitting ResultStage 34 (MapPartitionsRDD[35] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:52:15 INFO MemoryStore: Block broadcast_17 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:52:15 INFO MemoryStore: Block broadcast_17_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:52:15 INFO BlockManagerInfo: Added broadcast_17_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:15 INFO SparkContext: Created broadcast 17 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:15 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 34 (MapPartitionsRDD[35] at mapValues at StackOverflow.scala:190)
17/12/23 18:52:15 INFO TaskSchedulerImpl: Adding task set 34.0 with 6 tasks
17/12/23 18:52:16 INFO TaskSetManager: Starting task 0.0 in stage 34.0 (TID 96, localhost, executor driver, partition 0, ANY, 5751 bytes)
17/12/23 18:52:16 INFO Executor: Running task 0.0 in stage 34.0 (TID 96)
17/12/23 18:52:16 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:16 INFO ContextCleaner: Cleaned shuffle 8
17/12/23 18:52:16 INFO BlockManagerInfo: Removed broadcast_15_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:16 INFO BlockManagerInfo: Removed broadcast_16_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:16 INFO Executor: Finished task 0.0 in stage 34.0 (TID 96). 2145 bytes result sent to driver
17/12/23 18:52:16 INFO TaskSetManager: Starting task 1.0 in stage 34.0 (TID 97, localhost, executor driver, partition 1, ANY, 5751 bytes)
17/12/23 18:52:16 INFO Executor: Running task 1.0 in stage 34.0 (TID 97)
17/12/23 18:52:16 INFO TaskSetManager: Finished task 0.0 in stage 34.0 (TID 96) in 640 ms on localhost (executor driver) (1/6)
17/12/23 18:52:16 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:17 INFO Executor: Finished task 1.0 in stage 34.0 (TID 97). 1985 bytes result sent to driver
17/12/23 18:52:17 INFO TaskSetManager: Starting task 2.0 in stage 34.0 (TID 98, localhost, executor driver, partition 2, ANY, 5751 bytes)
17/12/23 18:52:17 INFO TaskSetManager: Finished task 1.0 in stage 34.0 (TID 97) in 362 ms on localhost (executor driver) (2/6)
17/12/23 18:52:17 INFO Executor: Running task 2.0 in stage 34.0 (TID 98)
17/12/23 18:52:17 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:17 INFO Executor: Finished task 2.0 in stage 34.0 (TID 98). 2162 bytes result sent to driver
17/12/23 18:52:17 INFO TaskSetManager: Starting task 3.0 in stage 34.0 (TID 99, localhost, executor driver, partition 3, ANY, 5751 bytes)
17/12/23 18:52:17 INFO TaskSetManager: Finished task 2.0 in stage 34.0 (TID 98) in 203 ms on localhost (executor driver) (3/6)
17/12/23 18:52:17 INFO Executor: Running task 3.0 in stage 34.0 (TID 99)
17/12/23 18:52:17 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:17 INFO Executor: Finished task 3.0 in stage 34.0 (TID 99). 2130 bytes result sent to driver
17/12/23 18:52:17 INFO TaskSetManager: Starting task 4.0 in stage 34.0 (TID 100, localhost, executor driver, partition 4, ANY, 5751 bytes)
17/12/23 18:52:17 INFO Executor: Running task 4.0 in stage 34.0 (TID 100)
17/12/23 18:52:17 INFO TaskSetManager: Finished task 3.0 in stage 34.0 (TID 99) in 300 ms on localhost (executor driver) (4/6)
17/12/23 18:52:17 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:18 INFO Executor: Finished task 4.0 in stage 34.0 (TID 100). 1953 bytes result sent to driver
17/12/23 18:52:18 INFO TaskSetManager: Starting task 5.0 in stage 34.0 (TID 101, localhost, executor driver, partition 5, ANY, 5751 bytes)
17/12/23 18:52:18 INFO Executor: Running task 5.0 in stage 34.0 (TID 101)
17/12/23 18:52:18 INFO TaskSetManager: Finished task 4.0 in stage 34.0 (TID 100) in 853 ms on localhost (executor driver) (5/6)
17/12/23 18:52:18 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:18 INFO Executor: Finished task 5.0 in stage 34.0 (TID 101). 2040 bytes result sent to driver
17/12/23 18:52:18 INFO TaskSetManager: Finished task 5.0 in stage 34.0 (TID 101) in 398 ms on localhost (executor driver) (6/6)
17/12/23 18:52:18 INFO TaskSchedulerImpl: Removed TaskSet 34.0, whose tasks have all completed, from pool 
Iteration: 6
17/12/23 18:52:18 INFO DAGScheduler: ResultStage 34 (collect at StackOverflow.scala:191) finished in 2.750 s
  * current distance: 20859.0
17/12/23 18:52:18 INFO DAGScheduler: Job 6 finished: collect at StackOverflow.scala:191, took 5.675542 s
  * desired distance: 20.0
  * means:
             (450000,4) ==>           (450000,4)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,20) ==>          (450000,24)    distance:       16
                 (0,92) ==>              (0,172)    distance:     6400
                  (0,2) ==>                (0,3)    distance:        1
                  (0,0) ==>                (0,0)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,1) ==>           (150000,1)    distance:        0
            (150000,14) ==>          (150000,19)    distance:       25
           (150000,233) ==>         (150000,303)    distance:     4900
            (300000,77) ==>         (300000,104)    distance:      729
             (300000,0) ==>           (300000,1)    distance:        1
             (300000,6) ==>           (300000,9)    distance:        9
             (50000,42) ==>           (50000,67)    distance:      625
              (50000,2) ==>            (50000,3)    distance:        1
              (50000,0) ==>            (50000,0)    distance:        0
             (200000,3) ==>           (200000,4)    distance:        1
             (200000,0) ==>           (200000,0)    distance:        0
            (200000,54) ==>          (200000,77)    distance:      529
             (500000,0) ==>           (500000,1)    distance:        1
            (500000,50) ==>          (500000,62)    distance:      144
             (500000,6) ==>           (500000,8)    distance:        4
             (350000,2) ==>           (350000,3)    distance:        1
             (350000,0) ==>           (350000,0)    distance:        0
           (350000,103) ==>         (350000,153)    distance:     2500
            (650000,56) ==>          (650000,63)    distance:       49
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,10) ==>          (650000,12)    distance:        4
             (100000,0) ==>           (100000,0)    distance:        0
            (100000,56) ==>          (100000,81)    distance:      625
             (100000,3) ==>           (100000,4)    distance:        1
           (400000,152) ==>         (400000,198)    distance:     2116
             (400000,0) ==>           (400000,1)    distance:        1
            (400000,10) ==>          (400000,15)    distance:       25
             (550000,3) ==>           (550000,3)    distance:        0
            (550000,14) ==>          (550000,16)    distance:        4
           (550000,123) ==>         (550000,158)    distance:     1225
            (250000,68) ==>          (250000,98)    distance:      900
             (250000,0) ==>           (250000,1)    distance:        1
             (250000,6) ==>           (250000,8)    distance:        4
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,13) ==>          (700000,17)    distance:       16
             (700000,2) ==>           (700000,3)    distance:        1
17/12/23 18:52:18 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:52:18 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:52:18 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:52:18 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:52:18 INFO DAGScheduler: Registering RDD 36 (map at StackOverflow.scala:186)
17/12/23 18:52:18 INFO DAGScheduler: Got job 7 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:52:18 INFO DAGScheduler: Final stage: ResultStage 39 (collect at StackOverflow.scala:191)
17/12/23 18:52:18 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 38)
17/12/23 18:52:18 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 38)
17/12/23 18:52:18 INFO DAGScheduler: Submitting ShuffleMapStage 38 (MapPartitionsRDD[36] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:52:18 INFO MemoryStore: Block broadcast_18 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:52:18 INFO MemoryStore: Block broadcast_18_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:52:18 INFO BlockManagerInfo: Added broadcast_18_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:18 INFO SparkContext: Created broadcast 18 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:18 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 38 (MapPartitionsRDD[36] at map at StackOverflow.scala:186)
17/12/23 18:52:18 INFO TaskSchedulerImpl: Adding task set 38.0 with 6 tasks
17/12/23 18:52:18 INFO TaskSetManager: Starting task 0.0 in stage 38.0 (TID 102, localhost, executor driver, partition 0, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:18 INFO Executor: Running task 0.0 in stage 38.0 (TID 102)
17/12/23 18:52:18 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:52:19 INFO Executor: Finished task 0.0 in stage 38.0 (TID 102). 1325 bytes result sent to driver
17/12/23 18:52:19 INFO TaskSetManager: Starting task 1.0 in stage 38.0 (TID 103, localhost, executor driver, partition 1, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:19 INFO TaskSetManager: Finished task 0.0 in stage 38.0 (TID 102) in 530 ms on localhost (executor driver) (1/6)
17/12/23 18:52:19 INFO Executor: Running task 1.0 in stage 38.0 (TID 103)
17/12/23 18:52:19 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:52:19 INFO Executor: Finished task 1.0 in stage 38.0 (TID 103). 1325 bytes result sent to driver
17/12/23 18:52:19 INFO TaskSetManager: Starting task 2.0 in stage 38.0 (TID 104, localhost, executor driver, partition 2, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:19 INFO Executor: Running task 2.0 in stage 38.0 (TID 104)
17/12/23 18:52:19 INFO TaskSetManager: Finished task 1.0 in stage 38.0 (TID 103) in 465 ms on localhost (executor driver) (2/6)
17/12/23 18:52:19 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:52:20 INFO Executor: Finished task 2.0 in stage 38.0 (TID 104). 1238 bytes result sent to driver
17/12/23 18:52:20 INFO TaskSetManager: Starting task 3.0 in stage 38.0 (TID 105, localhost, executor driver, partition 3, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:20 INFO TaskSetManager: Finished task 2.0 in stage 38.0 (TID 104) in 483 ms on localhost (executor driver) (3/6)
17/12/23 18:52:20 INFO Executor: Running task 3.0 in stage 38.0 (TID 105)
17/12/23 18:52:20 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:52:20 INFO Executor: Finished task 3.0 in stage 38.0 (TID 105). 1325 bytes result sent to driver
17/12/23 18:52:20 INFO TaskSetManager: Starting task 4.0 in stage 38.0 (TID 106, localhost, executor driver, partition 4, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:20 INFO Executor: Running task 4.0 in stage 38.0 (TID 106)
17/12/23 18:52:20 INFO TaskSetManager: Finished task 3.0 in stage 38.0 (TID 105) in 482 ms on localhost (executor driver) (4/6)
17/12/23 18:52:20 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:52:21 INFO Executor: Finished task 4.0 in stage 38.0 (TID 106). 1325 bytes result sent to driver
17/12/23 18:52:21 INFO TaskSetManager: Starting task 5.0 in stage 38.0 (TID 107, localhost, executor driver, partition 5, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:21 INFO Executor: Running task 5.0 in stage 38.0 (TID 107)
17/12/23 18:52:21 INFO TaskSetManager: Finished task 4.0 in stage 38.0 (TID 106) in 495 ms on localhost (executor driver) (5/6)
17/12/23 18:52:21 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:52:21 INFO Executor: Finished task 5.0 in stage 38.0 (TID 107). 1325 bytes result sent to driver
17/12/23 18:52:21 INFO TaskSetManager: Finished task 5.0 in stage 38.0 (TID 107) in 470 ms on localhost (executor driver) (6/6)
17/12/23 18:52:21 INFO TaskSchedulerImpl: Removed TaskSet 38.0, whose tasks have all completed, from pool 
17/12/23 18:52:21 INFO DAGScheduler: ShuffleMapStage 38 (map at StackOverflow.scala:186) finished in 2.923 s
17/12/23 18:52:21 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:52:21 INFO DAGScheduler: running: Set()
17/12/23 18:52:21 INFO DAGScheduler: waiting: Set(ResultStage 39)
17/12/23 18:52:21 INFO DAGScheduler: failed: Set()
17/12/23 18:52:21 INFO DAGScheduler: Submitting ResultStage 39 (MapPartitionsRDD[38] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:52:21 INFO MemoryStore: Block broadcast_19 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:52:21 INFO MemoryStore: Block broadcast_19_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:52:21 INFO BlockManagerInfo: Added broadcast_19_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:21 INFO SparkContext: Created broadcast 19 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:21 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 39 (MapPartitionsRDD[38] at mapValues at StackOverflow.scala:190)
17/12/23 18:52:21 INFO TaskSchedulerImpl: Adding task set 39.0 with 6 tasks
17/12/23 18:52:21 INFO TaskSetManager: Starting task 0.0 in stage 39.0 (TID 108, localhost, executor driver, partition 0, ANY, 5751 bytes)
17/12/23 18:52:21 INFO Executor: Running task 0.0 in stage 39.0 (TID 108)
17/12/23 18:52:21 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:21 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:22 INFO ContextCleaner: Cleaned shuffle 9
17/12/23 18:52:22 INFO BlockManagerInfo: Removed broadcast_17_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:22 INFO BlockManagerInfo: Removed broadcast_18_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:22 INFO Executor: Finished task 0.0 in stage 39.0 (TID 108). 2145 bytes result sent to driver
17/12/23 18:52:22 INFO TaskSetManager: Starting task 1.0 in stage 39.0 (TID 109, localhost, executor driver, partition 1, ANY, 5751 bytes)
17/12/23 18:52:22 INFO TaskSetManager: Finished task 0.0 in stage 39.0 (TID 108) in 679 ms on localhost (executor driver) (1/6)
17/12/23 18:52:22 INFO Executor: Running task 1.0 in stage 39.0 (TID 109)
17/12/23 18:52:22 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:22 INFO Executor: Finished task 1.0 in stage 39.0 (TID 109). 2072 bytes result sent to driver
17/12/23 18:52:22 INFO TaskSetManager: Starting task 2.0 in stage 39.0 (TID 110, localhost, executor driver, partition 2, ANY, 5751 bytes)
17/12/23 18:52:22 INFO TaskSetManager: Finished task 1.0 in stage 39.0 (TID 109) in 450 ms on localhost (executor driver) (2/6)
17/12/23 18:52:22 INFO Executor: Running task 2.0 in stage 39.0 (TID 110)
17/12/23 18:52:22 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:23 INFO Executor: Finished task 2.0 in stage 39.0 (TID 110). 1985 bytes result sent to driver
17/12/23 18:52:23 INFO TaskSetManager: Starting task 3.0 in stage 39.0 (TID 111, localhost, executor driver, partition 3, ANY, 5751 bytes)
17/12/23 18:52:23 INFO TaskSetManager: Finished task 2.0 in stage 39.0 (TID 110) in 177 ms on localhost (executor driver) (3/6)
17/12/23 18:52:23 INFO Executor: Running task 3.0 in stage 39.0 (TID 111)
17/12/23 18:52:23 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:23 INFO Executor: Finished task 3.0 in stage 39.0 (TID 111). 2040 bytes result sent to driver
17/12/23 18:52:23 INFO TaskSetManager: Starting task 4.0 in stage 39.0 (TID 112, localhost, executor driver, partition 4, ANY, 5751 bytes)
17/12/23 18:52:23 INFO Executor: Running task 4.0 in stage 39.0 (TID 112)
17/12/23 18:52:23 INFO TaskSetManager: Finished task 3.0 in stage 39.0 (TID 111) in 233 ms on localhost (executor driver) (4/6)
17/12/23 18:52:23 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:23 INFO Executor: Finished task 4.0 in stage 39.0 (TID 112). 2040 bytes result sent to driver
17/12/23 18:52:23 INFO TaskSetManager: Starting task 5.0 in stage 39.0 (TID 113, localhost, executor driver, partition 5, ANY, 5751 bytes)
17/12/23 18:52:23 INFO TaskSetManager: Finished task 4.0 in stage 39.0 (TID 112) in 724 ms on localhost (executor driver) (5/6)
17/12/23 18:52:23 INFO Executor: Running task 5.0 in stage 39.0 (TID 113)
17/12/23 18:52:23 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:24 INFO Executor: Finished task 5.0 in stage 39.0 (TID 113). 2040 bytes result sent to driver
17/12/23 18:52:24 INFO TaskSetManager: Finished task 5.0 in stage 39.0 (TID 113) in 645 ms on localhost (executor driver) (6/6)
17/12/23 18:52:24 INFO TaskSchedulerImpl: Removed TaskSet 39.0, whose tasks have all completed, from pool 
17/12/23 18:52:24 INFO DAGScheduler: ResultStage 39 (collect at StackOverflow.scala:191) finished in 2.901 s
17/12/23 18:52:24 INFO DAGScheduler: Job 7 finished: collect at StackOverflow.scala:191, took 5.840519 s
Iteration: 7
  * current distance: 33401.0
  * desired distance: 20.0
  * means:
             (450000,4) ==>           (450000,4)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,24) ==>          (450000,28)    distance:       16
                (0,172) ==>              (0,281)    distance:    11881
                  (0,3) ==>                (0,5)    distance:        4
                  (0,0) ==>                (0,0)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,1) ==>           (150000,2)    distance:        1
            (150000,19) ==>          (150000,27)    distance:       64
           (150000,303) ==>         (150000,383)    distance:     6400
           (300000,104) ==>         (300000,132)    distance:      784
             (300000,1) ==>           (300000,1)    distance:        0
             (300000,9) ==>          (300000,14)    distance:       25
             (50000,67) ==>           (50000,99)    distance:     1024
              (50000,3) ==>            (50000,4)    distance:        1
              (50000,0) ==>            (50000,0)    distance:        0
             (200000,4) ==>           (200000,5)    distance:        1
             (200000,0) ==>           (200000,0)    distance:        0
            (200000,77) ==>         (200000,103)    distance:      676
             (500000,1) ==>           (500000,1)    distance:        0
            (500000,62) ==>          (500000,74)    distance:      144
             (500000,8) ==>           (500000,9)    distance:        1
             (350000,3) ==>           (350000,5)    distance:        4
             (350000,0) ==>           (350000,0)    distance:        0
           (350000,153) ==>         (350000,209)    distance:     3136
            (650000,63) ==>          (650000,68)    distance:       25
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,12) ==>          (650000,14)    distance:        4
             (100000,0) ==>           (100000,0)    distance:        0
            (100000,81) ==>         (100000,111)    distance:      900
             (100000,4) ==>           (100000,6)    distance:        4
           (400000,198) ==>         (400000,248)    distance:     2500
             (400000,1) ==>           (400000,1)    distance:        0
            (400000,15) ==>          (400000,23)    distance:       64
             (550000,3) ==>           (550000,3)    distance:        0
            (550000,16) ==>          (550000,18)    distance:        4
           (550000,158) ==>         (550000,224)    distance:     4356
            (250000,98) ==>         (250000,135)    distance:     1369
             (250000,1) ==>           (250000,1)    distance:        0
             (250000,8) ==>          (250000,10)    distance:        4
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,17) ==>          (700000,20)    distance:        9
             (700000,3) ==>           (700000,3)    distance:        0
17/12/23 18:52:24 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:52:24 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:52:24 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:52:24 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:52:24 INFO DAGScheduler: Registering RDD 39 (map at StackOverflow.scala:186)
17/12/23 18:52:24 INFO DAGScheduler: Got job 8 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:52:24 INFO DAGScheduler: Final stage: ResultStage 44 (collect at StackOverflow.scala:191)
17/12/23 18:52:24 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 43)
17/12/23 18:52:24 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 43)
17/12/23 18:52:24 INFO DAGScheduler: Submitting ShuffleMapStage 43 (MapPartitionsRDD[39] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:52:24 INFO MemoryStore: Block broadcast_20 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:52:24 INFO MemoryStore: Block broadcast_20_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:52:24 INFO BlockManagerInfo: Added broadcast_20_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:24 INFO SparkContext: Created broadcast 20 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:24 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 43 (MapPartitionsRDD[39] at map at StackOverflow.scala:186)
17/12/23 18:52:24 INFO TaskSchedulerImpl: Adding task set 43.0 with 6 tasks
17/12/23 18:52:24 INFO TaskSetManager: Starting task 0.0 in stage 43.0 (TID 114, localhost, executor driver, partition 0, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:24 INFO Executor: Running task 0.0 in stage 43.0 (TID 114)
17/12/23 18:52:24 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:52:25 INFO Executor: Finished task 0.0 in stage 43.0 (TID 114). 1325 bytes result sent to driver
17/12/23 18:52:25 INFO TaskSetManager: Starting task 1.0 in stage 43.0 (TID 115, localhost, executor driver, partition 1, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:25 INFO Executor: Running task 1.0 in stage 43.0 (TID 115)
17/12/23 18:52:25 INFO TaskSetManager: Finished task 0.0 in stage 43.0 (TID 114) in 574 ms on localhost (executor driver) (1/6)
17/12/23 18:52:25 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:52:25 INFO Executor: Finished task 1.0 in stage 43.0 (TID 115). 1238 bytes result sent to driver
17/12/23 18:52:25 INFO TaskSetManager: Starting task 2.0 in stage 43.0 (TID 116, localhost, executor driver, partition 2, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:25 INFO TaskSetManager: Finished task 1.0 in stage 43.0 (TID 115) in 520 ms on localhost (executor driver) (2/6)
17/12/23 18:52:25 INFO Executor: Running task 2.0 in stage 43.0 (TID 116)
17/12/23 18:52:25 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:52:26 INFO Executor: Finished task 2.0 in stage 43.0 (TID 116). 1325 bytes result sent to driver
17/12/23 18:52:26 INFO TaskSetManager: Starting task 3.0 in stage 43.0 (TID 117, localhost, executor driver, partition 3, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:26 INFO TaskSetManager: Finished task 2.0 in stage 43.0 (TID 116) in 535 ms on localhost (executor driver) (3/6)
17/12/23 18:52:26 INFO Executor: Running task 3.0 in stage 43.0 (TID 117)
17/12/23 18:52:26 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:52:26 INFO Executor: Finished task 3.0 in stage 43.0 (TID 117). 1325 bytes result sent to driver
17/12/23 18:52:26 INFO TaskSetManager: Starting task 4.0 in stage 43.0 (TID 118, localhost, executor driver, partition 4, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:26 INFO TaskSetManager: Finished task 3.0 in stage 43.0 (TID 117) in 535 ms on localhost (executor driver) (4/6)
17/12/23 18:52:26 INFO Executor: Running task 4.0 in stage 43.0 (TID 118)
17/12/23 18:52:26 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:52:27 INFO Executor: Finished task 4.0 in stage 43.0 (TID 118). 1238 bytes result sent to driver
17/12/23 18:52:27 INFO TaskSetManager: Starting task 5.0 in stage 43.0 (TID 119, localhost, executor driver, partition 5, PROCESS_LOCAL, 5740 bytes)
17/12/23 18:52:27 INFO TaskSetManager: Finished task 4.0 in stage 43.0 (TID 118) in 538 ms on localhost (executor driver) (5/6)
17/12/23 18:52:27 INFO Executor: Running task 5.0 in stage 43.0 (TID 119)
17/12/23 18:52:27 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:52:27 INFO Executor: Finished task 5.0 in stage 43.0 (TID 119). 1325 bytes result sent to driver
17/12/23 18:52:27 INFO TaskSetManager: Finished task 5.0 in stage 43.0 (TID 119) in 522 ms on localhost (executor driver) (6/6)
17/12/23 18:52:27 INFO TaskSchedulerImpl: Removed TaskSet 43.0, whose tasks have all completed, from pool 
17/12/23 18:52:27 INFO DAGScheduler: ShuffleMapStage 43 (map at StackOverflow.scala:186) finished in 3.224 s
17/12/23 18:52:27 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:52:27 INFO DAGScheduler: running: Set()
17/12/23 18:52:27 INFO DAGScheduler: waiting: Set(ResultStage 44)
17/12/23 18:52:27 INFO DAGScheduler: failed: Set()
17/12/23 18:52:27 INFO DAGScheduler: Submitting ResultStage 44 (MapPartitionsRDD[41] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:52:27 INFO MemoryStore: Block broadcast_21 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:52:27 INFO MemoryStore: Block broadcast_21_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:52:27 INFO BlockManagerInfo: Added broadcast_21_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:27 INFO SparkContext: Created broadcast 21 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:27 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 44 (MapPartitionsRDD[41] at mapValues at StackOverflow.scala:190)
17/12/23 18:52:27 INFO TaskSchedulerImpl: Adding task set 44.0 with 6 tasks
17/12/23 18:52:27 INFO TaskSetManager: Starting task 0.0 in stage 44.0 (TID 120, localhost, executor driver, partition 0, ANY, 5751 bytes)
17/12/23 18:52:27 INFO Executor: Running task 0.0 in stage 44.0 (TID 120)
17/12/23 18:52:27 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:27 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:28 INFO ContextCleaner: Cleaned shuffle 10
17/12/23 18:52:28 INFO BlockManagerInfo: Removed broadcast_19_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:28 INFO BlockManagerInfo: Removed broadcast_20_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:28 INFO Executor: Finished task 0.0 in stage 44.0 (TID 120). 2235 bytes result sent to driver
17/12/23 18:52:28 INFO TaskSetManager: Starting task 1.0 in stage 44.0 (TID 121, localhost, executor driver, partition 1, ANY, 5751 bytes)
17/12/23 18:52:28 INFO Executor: Running task 1.0 in stage 44.0 (TID 121)
17/12/23 18:52:28 INFO TaskSetManager: Finished task 0.0 in stage 44.0 (TID 120) in 623 ms on localhost (executor driver) (1/6)
17/12/23 18:52:28 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:28 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:29 INFO Executor: Finished task 1.0 in stage 44.0 (TID 121). 1985 bytes result sent to driver
17/12/23 18:52:29 INFO TaskSetManager: Starting task 2.0 in stage 44.0 (TID 122, localhost, executor driver, partition 2, ANY, 5751 bytes)
17/12/23 18:52:29 INFO Executor: Running task 2.0 in stage 44.0 (TID 122)
17/12/23 18:52:29 INFO TaskSetManager: Finished task 1.0 in stage 44.0 (TID 121) in 597 ms on localhost (executor driver) (2/6)
17/12/23 18:52:29 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:52:29 INFO Executor: Finished task 2.0 in stage 44.0 (TID 122). 1985 bytes result sent to driver
17/12/23 18:52:29 INFO TaskSetManager: Starting task 3.0 in stage 44.0 (TID 123, localhost, executor driver, partition 3, ANY, 5751 bytes)
17/12/23 18:52:29 INFO Executor: Running task 3.0 in stage 44.0 (TID 123)
17/12/23 18:52:29 INFO TaskSetManager: Finished task 2.0 in stage 44.0 (TID 122) in 97 ms on localhost (executor driver) (3/6)
17/12/23 18:52:29 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:29 INFO Executor: Finished task 3.0 in stage 44.0 (TID 123). 2040 bytes result sent to driver
17/12/23 18:52:29 INFO TaskSetManager: Starting task 4.0 in stage 44.0 (TID 124, localhost, executor driver, partition 4, ANY, 5751 bytes)
17/12/23 18:52:29 INFO Executor: Running task 4.0 in stage 44.0 (TID 124)
17/12/23 18:52:29 INFO TaskSetManager: Finished task 3.0 in stage 44.0 (TID 123) in 243 ms on localhost (executor driver) (4/6)
17/12/23 18:52:29 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:30 INFO Executor: Finished task 4.0 in stage 44.0 (TID 124). 2040 bytes result sent to driver
17/12/23 18:52:30 INFO TaskSetManager: Starting task 5.0 in stage 44.0 (TID 125, localhost, executor driver, partition 5, ANY, 5751 bytes)
17/12/23 18:52:30 INFO TaskSetManager: Finished task 4.0 in stage 44.0 (TID 124) in 675 ms on localhost (executor driver) (5/6)
17/12/23 18:52:30 INFO Executor: Running task 5.0 in stage 44.0 (TID 125)
17/12/23 18:52:30 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:30 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
Iteration: 8
  * current distance: 39391.0
  * desired distance: 20.0
  * means:
             (450000,4) ==>           (450000,4)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,28) ==>          (450000,32)    distance:       16
                (0,281) ==>              (0,407)    distance:    15876
                  (0,5) ==>                (0,8)    distance:        9
                  (0,0) ==>                (0,0)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,2) ==>           (150000,2)    distance:        0
            (150000,27) ==>          (150000,37)    distance:      100
           (150000,383) ==>         (150000,471)    distance:     7744
           (300000,132) ==>         (300000,164)    distance:     1024
             (300000,1) ==>           (300000,1)    distance:        0
            (300000,14) ==>          (300000,19)    distance:       25
             (50000,99) ==>          (50000,138)    distance:     1521
              (50000,4) ==>            (50000,5)    distance:        1
              (50000,0) ==>            (50000,0)    distance:        0
17/12/23 18:52:30 INFO Executor: Finished task 5.0 in stage 44.0 (TID 125). 2130 bytes result sent to driver
17/12/23 18:52:30 INFO TaskSetManager: Finished task 5.0 in stage 44.0 (TID 125) in 698 ms on localhost (executor driver) (6/6)
             (200000,5) ==>           (200000,7)    distance:        4
17/12/23 18:52:30 INFO TaskSchedulerImpl: Removed TaskSet 44.0, whose tasks have all completed, from pool 
             (200000,0) ==>           (200000,0)    distance:        0
17/12/23 18:52:30 INFO DAGScheduler: ResultStage 44 (collect at StackOverflow.scala:191) finished in 2.930 s
           (200000,103) ==>         (200000,130)    distance:      729
17/12/23 18:52:30 INFO DAGScheduler: Job 8 finished: collect at StackOverflow.scala:191, took 6.164588 s
             (500000,1) ==>           (500000,1)    distance:        0
            (500000,74) ==>          (500000,89)    distance:      225
             (500000,9) ==>          (500000,11)    distance:        4
             (350000,5) ==>           (350000,8)    distance:        9
             (350000,0) ==>           (350000,0)    distance:        0
           (350000,209) ==>         (350000,262)    distance:     2809
            (650000,68) ==>          (650000,71)    distance:        9
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,14) ==>          (650000,15)    distance:        1
             (100000,0) ==>           (100000,0)    distance:        0
           (100000,111) ==>         (100000,145)    distance:     1156
             (100000,6) ==>           (100000,8)    distance:        4
           (400000,248) ==>         (400000,296)    distance:     2304
             (400000,1) ==>           (400000,1)    distance:        0
            (400000,23) ==>          (400000,32)    distance:       81
             (550000,3) ==>           (550000,3)    distance:        0
            (550000,18) ==>          (550000,20)    distance:        4
           (550000,224) ==>         (550000,284)    distance:     3600
           (250000,135) ==>         (250000,181)    distance:     2116
             (250000,1) ==>           (250000,1)    distance:        0
            (250000,10) ==>          (250000,12)    distance:        4
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,20) ==>          (700000,24)    distance:       16
             (700000,3) ==>           (700000,3)    distance:        0
17/12/23 18:52:30 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:52:30 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:52:30 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:52:30 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:52:30 INFO DAGScheduler: Registering RDD 42 (map at StackOverflow.scala:186)
17/12/23 18:52:30 INFO DAGScheduler: Got job 9 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:52:30 INFO DAGScheduler: Final stage: ResultStage 49 (collect at StackOverflow.scala:191)
17/12/23 18:52:30 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 48)
17/12/23 18:52:30 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 48)
17/12/23 18:52:30 INFO DAGScheduler: Submitting ShuffleMapStage 48 (MapPartitionsRDD[42] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:52:30 INFO MemoryStore: Block broadcast_22 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:52:30 INFO MemoryStore: Block broadcast_22_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:52:30 INFO BlockManagerInfo: Added broadcast_22_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:30 INFO SparkContext: Created broadcast 22 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:30 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 48 (MapPartitionsRDD[42] at map at StackOverflow.scala:186)
17/12/23 18:52:30 INFO TaskSchedulerImpl: Adding task set 48.0 with 6 tasks
17/12/23 18:52:30 INFO TaskSetManager: Starting task 0.0 in stage 48.0 (TID 126, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:30 INFO Executor: Running task 0.0 in stage 48.0 (TID 126)
17/12/23 18:52:30 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:52:31 INFO Executor: Finished task 0.0 in stage 48.0 (TID 126). 1415 bytes result sent to driver
17/12/23 18:52:31 INFO TaskSetManager: Starting task 1.0 in stage 48.0 (TID 127, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:31 INFO Executor: Running task 1.0 in stage 48.0 (TID 127)
17/12/23 18:52:31 INFO TaskSetManager: Finished task 0.0 in stage 48.0 (TID 126) in 534 ms on localhost (executor driver) (1/6)
17/12/23 18:52:31 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:52:31 INFO Executor: Finished task 1.0 in stage 48.0 (TID 127). 1325 bytes result sent to driver
17/12/23 18:52:31 INFO TaskSetManager: Starting task 2.0 in stage 48.0 (TID 128, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:31 INFO TaskSetManager: Finished task 1.0 in stage 48.0 (TID 127) in 505 ms on localhost (executor driver) (2/6)
17/12/23 18:52:31 INFO Executor: Running task 2.0 in stage 48.0 (TID 128)
17/12/23 18:52:31 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:52:32 INFO Executor: Finished task 2.0 in stage 48.0 (TID 128). 1325 bytes result sent to driver
17/12/23 18:52:32 INFO TaskSetManager: Starting task 3.0 in stage 48.0 (TID 129, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:32 INFO TaskSetManager: Finished task 2.0 in stage 48.0 (TID 128) in 542 ms on localhost (executor driver) (3/6)
17/12/23 18:52:32 INFO Executor: Running task 3.0 in stage 48.0 (TID 129)
17/12/23 18:52:32 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:52:32 INFO Executor: Finished task 3.0 in stage 48.0 (TID 129). 1238 bytes result sent to driver
17/12/23 18:52:32 INFO TaskSetManager: Starting task 4.0 in stage 48.0 (TID 130, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:32 INFO TaskSetManager: Finished task 3.0 in stage 48.0 (TID 129) in 518 ms on localhost (executor driver) (4/6)
17/12/23 18:52:32 INFO Executor: Running task 4.0 in stage 48.0 (TID 130)
17/12/23 18:52:32 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:52:33 INFO Executor: Finished task 4.0 in stage 48.0 (TID 130). 1325 bytes result sent to driver
17/12/23 18:52:33 INFO TaskSetManager: Starting task 5.0 in stage 48.0 (TID 131, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:33 INFO Executor: Running task 5.0 in stage 48.0 (TID 131)
17/12/23 18:52:33 INFO TaskSetManager: Finished task 4.0 in stage 48.0 (TID 130) in 554 ms on localhost (executor driver) (5/6)
17/12/23 18:52:33 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:52:34 INFO Executor: Finished task 5.0 in stage 48.0 (TID 131). 1325 bytes result sent to driver
17/12/23 18:52:34 INFO TaskSetManager: Finished task 5.0 in stage 48.0 (TID 131) in 522 ms on localhost (executor driver) (6/6)
17/12/23 18:52:34 INFO TaskSchedulerImpl: Removed TaskSet 48.0, whose tasks have all completed, from pool 
17/12/23 18:52:34 INFO DAGScheduler: ShuffleMapStage 48 (map at StackOverflow.scala:186) finished in 3.171 s
17/12/23 18:52:34 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:52:34 INFO DAGScheduler: running: Set()
17/12/23 18:52:34 INFO DAGScheduler: waiting: Set(ResultStage 49)
17/12/23 18:52:34 INFO DAGScheduler: failed: Set()
17/12/23 18:52:34 INFO DAGScheduler: Submitting ResultStage 49 (MapPartitionsRDD[44] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:52:34 INFO MemoryStore: Block broadcast_23 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:52:34 INFO MemoryStore: Block broadcast_23_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:52:34 INFO BlockManagerInfo: Added broadcast_23_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:34 INFO SparkContext: Created broadcast 23 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:34 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 49 (MapPartitionsRDD[44] at mapValues at StackOverflow.scala:190)
17/12/23 18:52:34 INFO TaskSchedulerImpl: Adding task set 49.0 with 6 tasks
17/12/23 18:52:34 INFO TaskSetManager: Starting task 0.0 in stage 49.0 (TID 132, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:52:34 INFO Executor: Running task 0.0 in stage 49.0 (TID 132)
17/12/23 18:52:34 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:34 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:34 INFO Executor: Finished task 0.0 in stage 49.0 (TID 132). 1985 bytes result sent to driver
17/12/23 18:52:34 INFO TaskSetManager: Starting task 1.0 in stage 49.0 (TID 133, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:52:34 INFO Executor: Running task 1.0 in stage 49.0 (TID 133)
17/12/23 18:52:34 INFO TaskSetManager: Finished task 0.0 in stage 49.0 (TID 132) in 595 ms on localhost (executor driver) (1/6)
17/12/23 18:52:34 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:34 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:34 INFO ContextCleaner: Cleaned shuffle 11
17/12/23 18:52:34 INFO BlockManagerInfo: Removed broadcast_21_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:34 INFO BlockManagerInfo: Removed broadcast_22_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:35 INFO Executor: Finished task 1.0 in stage 49.0 (TID 133). 2145 bytes result sent to driver
17/12/23 18:52:35 INFO TaskSetManager: Starting task 2.0 in stage 49.0 (TID 134, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:52:35 INFO Executor: Running task 2.0 in stage 49.0 (TID 134)
17/12/23 18:52:35 INFO TaskSetManager: Finished task 1.0 in stage 49.0 (TID 133) in 642 ms on localhost (executor driver) (2/6)
17/12/23 18:52:35 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:35 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:35 INFO Executor: Finished task 2.0 in stage 49.0 (TID 134). 2072 bytes result sent to driver
17/12/23 18:52:35 INFO TaskSetManager: Starting task 3.0 in stage 49.0 (TID 135, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:52:35 INFO TaskSetManager: Finished task 2.0 in stage 49.0 (TID 134) in 82 ms on localhost (executor driver) (3/6)
17/12/23 18:52:35 INFO Executor: Running task 3.0 in stage 49.0 (TID 135)
17/12/23 18:52:35 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:35 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:35 INFO Executor: Finished task 3.0 in stage 49.0 (TID 135). 2040 bytes result sent to driver
17/12/23 18:52:35 INFO TaskSetManager: Starting task 4.0 in stage 49.0 (TID 136, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:52:35 INFO Executor: Running task 4.0 in stage 49.0 (TID 136)
17/12/23 18:52:35 INFO TaskSetManager: Finished task 3.0 in stage 49.0 (TID 135) in 260 ms on localhost (executor driver) (4/6)
17/12/23 18:52:35 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:35 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:36 INFO Executor: Finished task 4.0 in stage 49.0 (TID 136). 1953 bytes result sent to driver
17/12/23 18:52:36 INFO TaskSetManager: Starting task 5.0 in stage 49.0 (TID 137, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:52:36 INFO Executor: Running task 5.0 in stage 49.0 (TID 137)
17/12/23 18:52:36 INFO TaskSetManager: Finished task 4.0 in stage 49.0 (TID 136) in 573 ms on localhost (executor driver) (5/6)
17/12/23 18:52:36 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:36 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:36 INFO Executor: Finished task 5.0 in stage 49.0 (TID 137). 1953 bytes result sent to driver
Iteration: 9
17/12/23 18:52:36 INFO TaskSetManager: Finished task 5.0 in stage 49.0 (TID 137) in 754 ms on localhost (executor driver) (6/6)
  * current distance: 47083.0
17/12/23 18:52:36 INFO TaskSchedulerImpl: Removed TaskSet 49.0, whose tasks have all completed, from pool 
  * desired distance: 20.0
17/12/23 18:52:36 INFO DAGScheduler: ResultStage 49 (collect at StackOverflow.scala:191) finished in 2.896 s
  * means:
17/12/23 18:52:36 INFO DAGScheduler: Job 9 finished: collect at StackOverflow.scala:191, took 6.081500 s
             (450000,4) ==>           (450000,4)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,32) ==>          (450000,36)    distance:       16
                (0,407) ==>              (0,543)    distance:    18496
                  (0,8) ==>               (0,12)    distance:       16
                  (0,0) ==>                (0,0)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,2) ==>           (150000,2)    distance:        0
            (150000,37) ==>          (150000,49)    distance:      144
           (150000,471) ==>         (150000,564)    distance:     8649
           (300000,164) ==>         (300000,195)    distance:      961
             (300000,1) ==>           (300000,1)    distance:        0
            (300000,19) ==>          (300000,26)    distance:       49
            (50000,138) ==>          (50000,183)    distance:     2025
              (50000,5) ==>            (50000,7)    distance:        4
              (50000,0) ==>            (50000,0)    distance:        0
             (200000,7) ==>           (200000,9)    distance:        4
             (200000,0) ==>           (200000,1)    distance:        1
           (200000,130) ==>         (200000,157)    distance:      729
             (500000,1) ==>           (500000,2)    distance:        1
            (500000,89) ==>         (500000,103)    distance:      196
            (500000,11) ==>          (500000,13)    distance:        4
             (350000,8) ==>          (350000,12)    distance:       16
             (350000,0) ==>           (350000,0)    distance:        0
           (350000,262) ==>         (350000,309)    distance:     2209
            (650000,71) ==>          (650000,74)    distance:        9
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,0) ==>           (100000,1)    distance:        1
           (100000,145) ==>         (100000,182)    distance:     1369
             (100000,8) ==>          (100000,11)    distance:        9
           (400000,296) ==>         (400000,351)    distance:     3025
             (400000,1) ==>           (400000,1)    distance:        0
            (400000,32) ==>          (400000,41)    distance:       81
             (550000,3) ==>           (550000,4)    distance:        1
            (550000,20) ==>          (550000,22)    distance:        4
           (550000,284) ==>         (550000,363)    distance:     6241
           (250000,181) ==>         (250000,234)    distance:     2809
             (250000,1) ==>           (250000,1)    distance:        0
            (250000,12) ==>          (250000,15)    distance:        9
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,24) ==>          (700000,26)    distance:        4
             (700000,3) ==>           (700000,4)    distance:        1
17/12/23 18:52:36 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:52:36 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:52:36 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:52:36 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:52:36 INFO DAGScheduler: Registering RDD 45 (map at StackOverflow.scala:186)
17/12/23 18:52:36 INFO DAGScheduler: Got job 10 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:52:36 INFO DAGScheduler: Final stage: ResultStage 54 (collect at StackOverflow.scala:191)
17/12/23 18:52:36 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 53)
17/12/23 18:52:36 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 53)
17/12/23 18:52:36 INFO DAGScheduler: Submitting ShuffleMapStage 53 (MapPartitionsRDD[45] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:52:36 INFO MemoryStore: Block broadcast_24 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:52:36 INFO MemoryStore: Block broadcast_24_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:52:36 INFO BlockManagerInfo: Added broadcast_24_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:36 INFO SparkContext: Created broadcast 24 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:36 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 53 (MapPartitionsRDD[45] at map at StackOverflow.scala:186)
17/12/23 18:52:36 INFO TaskSchedulerImpl: Adding task set 53.0 with 6 tasks
17/12/23 18:52:36 INFO TaskSetManager: Starting task 0.0 in stage 53.0 (TID 138, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:36 INFO Executor: Running task 0.0 in stage 53.0 (TID 138)
17/12/23 18:52:36 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:52:37 INFO Executor: Finished task 0.0 in stage 53.0 (TID 138). 1325 bytes result sent to driver
17/12/23 18:52:37 INFO TaskSetManager: Starting task 1.0 in stage 53.0 (TID 139, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:37 INFO Executor: Running task 1.0 in stage 53.0 (TID 139)
17/12/23 18:52:37 INFO TaskSetManager: Finished task 0.0 in stage 53.0 (TID 138) in 531 ms on localhost (executor driver) (1/6)
17/12/23 18:52:37 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:52:37 INFO Executor: Finished task 1.0 in stage 53.0 (TID 139). 1325 bytes result sent to driver
17/12/23 18:52:37 INFO TaskSetManager: Starting task 2.0 in stage 53.0 (TID 140, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:37 INFO Executor: Running task 2.0 in stage 53.0 (TID 140)
17/12/23 18:52:37 INFO TaskSetManager: Finished task 1.0 in stage 53.0 (TID 139) in 487 ms on localhost (executor driver) (2/6)
17/12/23 18:52:37 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:52:38 INFO Executor: Finished task 2.0 in stage 53.0 (TID 140). 1325 bytes result sent to driver
17/12/23 18:52:38 INFO TaskSetManager: Starting task 3.0 in stage 53.0 (TID 141, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:38 INFO TaskSetManager: Finished task 2.0 in stage 53.0 (TID 140) in 523 ms on localhost (executor driver) (3/6)
17/12/23 18:52:38 INFO Executor: Running task 3.0 in stage 53.0 (TID 141)
17/12/23 18:52:38 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:52:39 INFO Executor: Finished task 3.0 in stage 53.0 (TID 141). 1325 bytes result sent to driver
17/12/23 18:52:39 INFO TaskSetManager: Starting task 4.0 in stage 53.0 (TID 142, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:39 INFO TaskSetManager: Finished task 3.0 in stage 53.0 (TID 141) in 512 ms on localhost (executor driver) (4/6)
17/12/23 18:52:39 INFO Executor: Running task 4.0 in stage 53.0 (TID 142)
17/12/23 18:52:39 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:52:39 INFO Executor: Finished task 4.0 in stage 53.0 (TID 142). 1325 bytes result sent to driver
17/12/23 18:52:39 INFO TaskSetManager: Starting task 5.0 in stage 53.0 (TID 143, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:39 INFO Executor: Running task 5.0 in stage 53.0 (TID 143)
17/12/23 18:52:39 INFO TaskSetManager: Finished task 4.0 in stage 53.0 (TID 142) in 501 ms on localhost (executor driver) (5/6)
17/12/23 18:52:39 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:52:40 INFO Executor: Finished task 5.0 in stage 53.0 (TID 143). 1238 bytes result sent to driver
17/12/23 18:52:40 INFO TaskSetManager: Finished task 5.0 in stage 53.0 (TID 143) in 520 ms on localhost (executor driver) (6/6)
17/12/23 18:52:40 INFO TaskSchedulerImpl: Removed TaskSet 53.0, whose tasks have all completed, from pool 
17/12/23 18:52:40 INFO DAGScheduler: ShuffleMapStage 53 (map at StackOverflow.scala:186) finished in 3.072 s
17/12/23 18:52:40 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:52:40 INFO DAGScheduler: running: Set()
17/12/23 18:52:40 INFO DAGScheduler: waiting: Set(ResultStage 54)
17/12/23 18:52:40 INFO DAGScheduler: failed: Set()
17/12/23 18:52:40 INFO DAGScheduler: Submitting ResultStage 54 (MapPartitionsRDD[47] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:52:40 INFO MemoryStore: Block broadcast_25 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:52:40 INFO MemoryStore: Block broadcast_25_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:52:40 INFO BlockManagerInfo: Added broadcast_25_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:40 INFO SparkContext: Created broadcast 25 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:40 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 54 (MapPartitionsRDD[47] at mapValues at StackOverflow.scala:190)
17/12/23 18:52:40 INFO TaskSchedulerImpl: Adding task set 54.0 with 6 tasks
17/12/23 18:52:40 INFO TaskSetManager: Starting task 0.0 in stage 54.0 (TID 144, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:52:40 INFO Executor: Running task 0.0 in stage 54.0 (TID 144)
17/12/23 18:52:40 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:40 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:40 INFO Executor: Finished task 0.0 in stage 54.0 (TID 144). 2072 bytes result sent to driver
17/12/23 18:52:40 INFO TaskSetManager: Starting task 1.0 in stage 54.0 (TID 145, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:52:40 INFO Executor: Running task 1.0 in stage 54.0 (TID 145)
17/12/23 18:52:40 INFO TaskSetManager: Finished task 0.0 in stage 54.0 (TID 144) in 532 ms on localhost (executor driver) (1/6)
17/12/23 18:52:40 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:40 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:41 INFO ContextCleaner: Cleaned shuffle 12
17/12/23 18:52:41 INFO BlockManagerInfo: Removed broadcast_23_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:41 INFO BlockManagerInfo: Removed broadcast_24_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:41 INFO Executor: Finished task 1.0 in stage 54.0 (TID 145). 2235 bytes result sent to driver
17/12/23 18:52:41 INFO TaskSetManager: Starting task 2.0 in stage 54.0 (TID 146, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:52:41 INFO Executor: Running task 2.0 in stage 54.0 (TID 146)
17/12/23 18:52:41 INFO TaskSetManager: Finished task 1.0 in stage 54.0 (TID 145) in 693 ms on localhost (executor driver) (2/6)
17/12/23 18:52:41 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:41 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:41 INFO Executor: Finished task 2.0 in stage 54.0 (TID 146). 1985 bytes result sent to driver
17/12/23 18:52:41 INFO TaskSetManager: Starting task 3.0 in stage 54.0 (TID 147, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:52:41 INFO Executor: Running task 3.0 in stage 54.0 (TID 147)
17/12/23 18:52:41 INFO TaskSetManager: Finished task 2.0 in stage 54.0 (TID 146) in 65 ms on localhost (executor driver) (3/6)
17/12/23 18:52:41 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:41 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:41 INFO Executor: Finished task 3.0 in stage 54.0 (TID 147). 1953 bytes result sent to driver
17/12/23 18:52:41 INFO TaskSetManager: Starting task 4.0 in stage 54.0 (TID 148, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:52:41 INFO Executor: Running task 4.0 in stage 54.0 (TID 148)
17/12/23 18:52:41 INFO TaskSetManager: Finished task 3.0 in stage 54.0 (TID 147) in 265 ms on localhost (executor driver) (4/6)
17/12/23 18:52:41 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:41 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:42 INFO Executor: Finished task 4.0 in stage 54.0 (TID 148). 1953 bytes result sent to driver
17/12/23 18:52:42 INFO TaskSetManager: Starting task 5.0 in stage 54.0 (TID 149, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:52:42 INFO TaskSetManager: Finished task 4.0 in stage 54.0 (TID 148) in 488 ms on localhost (executor driver) (5/6)
17/12/23 18:52:42 INFO Executor: Running task 5.0 in stage 54.0 (TID 149)
17/12/23 18:52:42 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:42 INFO Executor: Finished task 5.0 in stage 54.0 (TID 149). 2040 bytes result sent to driver
17/12/23 18:52:42 INFO TaskSetManager: Finished task 5.0 in stage 54.0 (TID 149) in 846 ms on localhost (executor driver) (6/6)
17/12/23 18:52:42 INFO TaskSchedulerImpl: Removed TaskSet 54.0, whose tasks have all completed, from pool 
17/12/23 18:52:42 INFO DAGScheduler: ResultStage 54 (collect at StackOverflow.scala:191) finished in 2.883 s
17/12/23 18:52:42 INFO DAGScheduler: Job 10 finished: collect at StackOverflow.scala:191, took 5.970056 s
Iteration: 10
  * current distance: 46224.0
  * desired distance: 20.0
  * means:
             (450000,4) ==>           (450000,4)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,36) ==>          (450000,40)    distance:       16
                (0,543) ==>              (0,693)    distance:    22500
                 (0,12) ==>               (0,20)    distance:       64
                  (0,0) ==>                (0,1)    distance:        1
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,2) ==>           (150000,2)    distance:        0
            (150000,49) ==>          (150000,61)    distance:      144
           (150000,564) ==>         (150000,623)    distance:     3481
           (300000,195) ==>         (300000,223)    distance:      784
             (300000,1) ==>           (300000,2)    distance:        1
            (300000,26) ==>          (300000,32)    distance:       36
            (50000,183) ==>          (50000,229)    distance:     2116
              (50000,7) ==>           (50000,10)    distance:        9
              (50000,0) ==>            (50000,1)    distance:        1
             (200000,9) ==>          (200000,11)    distance:        4
             (200000,1) ==>           (200000,1)    distance:        0
           (200000,157) ==>         (200000,184)    distance:      729
             (500000,2) ==>           (500000,2)    distance:        0
           (500000,103) ==>         (500000,116)    distance:      169
            (500000,13) ==>          (500000,15)    distance:        4
            (350000,12) ==>          (350000,18)    distance:       36
             (350000,0) ==>           (350000,1)    distance:        1
           (350000,309) ==>         (350000,345)    distance:     1296
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,182) ==>         (100000,218)    distance:     1296
            (100000,11) ==>          (100000,16)    distance:       25
           (400000,351) ==>         (400000,393)    distance:     1764
             (400000,1) ==>           (400000,2)    distance:        1
            (400000,41) ==>          (400000,52)    distance:      121
             (550000,4) ==>           (550000,4)    distance:        0
            (550000,22) ==>          (550000,27)    distance:       25
           (550000,363) ==>         (550000,448)    distance:     7225
           (250000,234) ==>         (250000,300)    distance:     4356
             (250000,1) ==>           (250000,2)    distance:        1
            (250000,15) ==>          (250000,19)    distance:       16
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,26) ==>          (700000,27)    distance:        1
             (700000,4) ==>           (700000,5)    distance:        1
17/12/23 18:52:42 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:52:42 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:52:42 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:52:42 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:52:42 INFO DAGScheduler: Registering RDD 48 (map at StackOverflow.scala:186)
17/12/23 18:52:42 INFO DAGScheduler: Got job 11 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:52:42 INFO DAGScheduler: Final stage: ResultStage 59 (collect at StackOverflow.scala:191)
17/12/23 18:52:42 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 58)
17/12/23 18:52:42 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 58)
17/12/23 18:52:42 INFO DAGScheduler: Submitting ShuffleMapStage 58 (MapPartitionsRDD[48] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:52:42 INFO MemoryStore: Block broadcast_26 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:52:42 INFO MemoryStore: Block broadcast_26_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:52:42 INFO BlockManagerInfo: Added broadcast_26_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:42 INFO SparkContext: Created broadcast 26 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:42 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 58 (MapPartitionsRDD[48] at map at StackOverflow.scala:186)
17/12/23 18:52:42 INFO TaskSchedulerImpl: Adding task set 58.0 with 6 tasks
17/12/23 18:52:42 INFO TaskSetManager: Starting task 0.0 in stage 58.0 (TID 150, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:42 INFO Executor: Running task 0.0 in stage 58.0 (TID 150)
17/12/23 18:52:42 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:52:43 INFO Executor: Finished task 0.0 in stage 58.0 (TID 150). 1415 bytes result sent to driver
17/12/23 18:52:43 INFO TaskSetManager: Starting task 1.0 in stage 58.0 (TID 151, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:43 INFO Executor: Running task 1.0 in stage 58.0 (TID 151)
17/12/23 18:52:43 INFO TaskSetManager: Finished task 0.0 in stage 58.0 (TID 150) in 610 ms on localhost (executor driver) (1/6)
17/12/23 18:52:43 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:52:44 INFO Executor: Finished task 1.0 in stage 58.0 (TID 151). 1325 bytes result sent to driver
17/12/23 18:52:44 INFO TaskSetManager: Starting task 2.0 in stage 58.0 (TID 152, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:44 INFO TaskSetManager: Finished task 1.0 in stage 58.0 (TID 151) in 500 ms on localhost (executor driver) (2/6)
17/12/23 18:52:44 INFO Executor: Running task 2.0 in stage 58.0 (TID 152)
17/12/23 18:52:44 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:52:44 INFO Executor: Finished task 2.0 in stage 58.0 (TID 152). 1325 bytes result sent to driver
17/12/23 18:52:44 INFO TaskSetManager: Starting task 3.0 in stage 58.0 (TID 153, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:44 INFO Executor: Running task 3.0 in stage 58.0 (TID 153)
17/12/23 18:52:44 INFO TaskSetManager: Finished task 2.0 in stage 58.0 (TID 152) in 497 ms on localhost (executor driver) (3/6)
17/12/23 18:52:44 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:52:45 INFO Executor: Finished task 3.0 in stage 58.0 (TID 153). 1415 bytes result sent to driver
17/12/23 18:52:45 INFO TaskSetManager: Starting task 4.0 in stage 58.0 (TID 154, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:45 INFO TaskSetManager: Finished task 3.0 in stage 58.0 (TID 153) in 518 ms on localhost (executor driver) (4/6)
17/12/23 18:52:45 INFO Executor: Running task 4.0 in stage 58.0 (TID 154)
17/12/23 18:52:45 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:52:45 INFO Executor: Finished task 4.0 in stage 58.0 (TID 154). 1325 bytes result sent to driver
17/12/23 18:52:45 INFO TaskSetManager: Starting task 5.0 in stage 58.0 (TID 155, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:45 INFO Executor: Running task 5.0 in stage 58.0 (TID 155)
17/12/23 18:52:45 INFO TaskSetManager: Finished task 4.0 in stage 58.0 (TID 154) in 502 ms on localhost (executor driver) (5/6)
17/12/23 18:52:45 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:52:46 INFO Executor: Finished task 5.0 in stage 58.0 (TID 155). 1238 bytes result sent to driver
17/12/23 18:52:46 INFO TaskSetManager: Finished task 5.0 in stage 58.0 (TID 155) in 502 ms on localhost (executor driver) (6/6)
17/12/23 18:52:46 INFO TaskSchedulerImpl: Removed TaskSet 58.0, whose tasks have all completed, from pool 
17/12/23 18:52:46 INFO DAGScheduler: ShuffleMapStage 58 (map at StackOverflow.scala:186) finished in 3.127 s
17/12/23 18:52:46 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:52:46 INFO DAGScheduler: running: Set()
17/12/23 18:52:46 INFO DAGScheduler: waiting: Set(ResultStage 59)
17/12/23 18:52:46 INFO DAGScheduler: failed: Set()
17/12/23 18:52:46 INFO DAGScheduler: Submitting ResultStage 59 (MapPartitionsRDD[50] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:52:46 INFO MemoryStore: Block broadcast_27 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:52:46 INFO MemoryStore: Block broadcast_27_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:52:46 INFO BlockManagerInfo: Added broadcast_27_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:46 INFO SparkContext: Created broadcast 27 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:46 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 59 (MapPartitionsRDD[50] at mapValues at StackOverflow.scala:190)
17/12/23 18:52:46 INFO TaskSchedulerImpl: Adding task set 59.0 with 6 tasks
17/12/23 18:52:46 INFO TaskSetManager: Starting task 0.0 in stage 59.0 (TID 156, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:52:46 INFO Executor: Running task 0.0 in stage 59.0 (TID 156)
17/12/23 18:52:46 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:46 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:46 INFO Executor: Finished task 0.0 in stage 59.0 (TID 156). 2162 bytes result sent to driver
17/12/23 18:52:46 INFO TaskSetManager: Starting task 1.0 in stage 59.0 (TID 157, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:52:46 INFO Executor: Running task 1.0 in stage 59.0 (TID 157)
17/12/23 18:52:46 INFO TaskSetManager: Finished task 0.0 in stage 59.0 (TID 156) in 562 ms on localhost (executor driver) (1/6)
17/12/23 18:52:46 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:46 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:47 INFO Executor: Finished task 1.0 in stage 59.0 (TID 157). 2162 bytes result sent to driver
17/12/23 18:52:47 INFO TaskSetManager: Starting task 2.0 in stage 59.0 (TID 158, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:52:47 INFO TaskSetManager: Finished task 1.0 in stage 59.0 (TID 157) in 665 ms on localhost (executor driver) (2/6)
17/12/23 18:52:47 INFO Executor: Running task 2.0 in stage 59.0 (TID 158)
17/12/23 18:52:47 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:47 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:47 INFO Executor: Finished task 2.0 in stage 59.0 (TID 158). 1985 bytes result sent to driver
17/12/23 18:52:47 INFO TaskSetManager: Starting task 3.0 in stage 59.0 (TID 159, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:52:47 INFO Executor: Running task 3.0 in stage 59.0 (TID 159)
17/12/23 18:52:47 INFO TaskSetManager: Finished task 2.0 in stage 59.0 (TID 158) in 56 ms on localhost (executor driver) (3/6)
17/12/23 18:52:47 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:47 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:47 INFO Executor: Finished task 3.0 in stage 59.0 (TID 159). 2040 bytes result sent to driver
17/12/23 18:52:47 INFO TaskSetManager: Starting task 4.0 in stage 59.0 (TID 160, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:52:47 INFO ContextCleaner: Cleaned shuffle 13
17/12/23 18:52:47 INFO TaskSetManager: Finished task 3.0 in stage 59.0 (TID 159) in 280 ms on localhost (executor driver) (4/6)
17/12/23 18:52:47 INFO Executor: Running task 4.0 in stage 59.0 (TID 160)
17/12/23 18:52:47 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:47 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:47 INFO BlockManagerInfo: Removed broadcast_25_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:47 INFO BlockManagerInfo: Removed broadcast_26_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:48 INFO Executor: Finished task 4.0 in stage 59.0 (TID 160). 2040 bytes result sent to driver
17/12/23 18:52:48 INFO TaskSetManager: Starting task 5.0 in stage 59.0 (TID 161, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:52:48 INFO Executor: Running task 5.0 in stage 59.0 (TID 161)
17/12/23 18:52:48 INFO TaskSetManager: Finished task 4.0 in stage 59.0 (TID 160) in 484 ms on localhost (executor driver) (5/6)
17/12/23 18:52:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:48 INFO Executor: Finished task 5.0 in stage 59.0 (TID 161). 2040 bytes result sent to driver
17/12/23 18:52:48 INFO TaskSetManager: Finished task 5.0 in stage 59.0 (TID 161) in 865 ms on localhost (executor driver) (6/6)
17/12/23 18:52:48 INFO TaskSchedulerImpl: Removed TaskSet 59.0, whose tasks have all completed, from pool 
17/12/23 18:52:48 INFO DAGScheduler: ResultStage 59 (collect at StackOverflow.scala:191) finished in 2.911 s
17/12/23 18:52:48 INFO DAGScheduler: Job 11 finished: collect at StackOverflow.scala:191, took 6.055143 s
Iteration: 11
  * current distance: 127438.0
  * desired distance: 20.0
  * means:
             (450000,4) ==>           (450000,5)    distance:        1
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,40) ==>          (450000,43)    distance:        9
                (0,693) ==>              (0,839)    distance:    21316
                 (0,20) ==>               (0,36)    distance:      256
                  (0,1) ==>                (0,1)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,2) ==>           (150000,2)    distance:        0
            (150000,61) ==>          (150000,73)    distance:      144
           (150000,623) ==>         (150000,672)    distance:     2401
           (300000,223) ==>         (300000,253)    distance:      900
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,32) ==>          (300000,40)    distance:       64
            (50000,229) ==>          (50000,278)    distance:     2401
             (50000,10) ==>           (50000,15)    distance:       25
              (50000,1) ==>            (50000,1)    distance:        0
            (200000,11) ==>          (200000,14)    distance:        9
             (200000,1) ==>           (200000,1)    distance:        0
           (200000,184) ==>         (200000,210)    distance:      676
             (500000,2) ==>           (500000,2)    distance:        0
           (500000,116) ==>         (500000,128)    distance:      144
            (500000,15) ==>          (500000,17)    distance:        4
            (350000,18) ==>          (350000,29)    distance:      121
             (350000,1) ==>           (350000,1)    distance:        0
           (350000,345) ==>         (350000,385)    distance:     1600
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,218) ==>         (100000,251)    distance:     1089
            (100000,16) ==>          (100000,20)    distance:       16
           (400000,393) ==>         (400000,432)    distance:     1521
             (400000,2) ==>           (400000,2)    distance:        0
            (400000,52) ==>          (400000,63)    distance:      121
             (550000,4) ==>           (550000,4)    distance:        0
            (550000,27) ==>          (550000,31)    distance:       16
           (550000,448) ==>         (550000,749)    distance:    90601
           (250000,300) ==>         (250000,363)    distance:     3969
             (250000,2) ==>           (250000,2)    distance:        0
            (250000,19) ==>          (250000,24)    distance:       25
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,27) ==>          (700000,30)    distance:        9
             (700000,5) ==>           (700000,5)    distance:        0
17/12/23 18:52:49 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:52:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:52:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:52:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:52:49 INFO DAGScheduler: Registering RDD 51 (map at StackOverflow.scala:186)
17/12/23 18:52:49 INFO DAGScheduler: Got job 12 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:52:49 INFO DAGScheduler: Final stage: ResultStage 64 (collect at StackOverflow.scala:191)
17/12/23 18:52:49 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 63)
17/12/23 18:52:49 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 63)
17/12/23 18:52:49 INFO DAGScheduler: Submitting ShuffleMapStage 63 (MapPartitionsRDD[51] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:52:49 INFO MemoryStore: Block broadcast_28 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:52:49 INFO MemoryStore: Block broadcast_28_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:52:49 INFO BlockManagerInfo: Added broadcast_28_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:49 INFO SparkContext: Created broadcast 28 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:49 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 63 (MapPartitionsRDD[51] at map at StackOverflow.scala:186)
17/12/23 18:52:49 INFO TaskSchedulerImpl: Adding task set 63.0 with 6 tasks
17/12/23 18:52:49 INFO TaskSetManager: Starting task 0.0 in stage 63.0 (TID 162, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:49 INFO Executor: Running task 0.0 in stage 63.0 (TID 162)
17/12/23 18:52:49 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:52:49 INFO Executor: Finished task 0.0 in stage 63.0 (TID 162). 1325 bytes result sent to driver
17/12/23 18:52:49 INFO TaskSetManager: Starting task 1.0 in stage 63.0 (TID 163, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:49 INFO Executor: Running task 1.0 in stage 63.0 (TID 163)
17/12/23 18:52:49 INFO TaskSetManager: Finished task 0.0 in stage 63.0 (TID 162) in 550 ms on localhost (executor driver) (1/6)
17/12/23 18:52:49 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:52:50 INFO Executor: Finished task 1.0 in stage 63.0 (TID 163). 1325 bytes result sent to driver
17/12/23 18:52:50 INFO TaskSetManager: Starting task 2.0 in stage 63.0 (TID 164, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:50 INFO Executor: Running task 2.0 in stage 63.0 (TID 164)
17/12/23 18:52:50 INFO TaskSetManager: Finished task 1.0 in stage 63.0 (TID 163) in 525 ms on localhost (executor driver) (2/6)
17/12/23 18:52:50 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:52:50 INFO Executor: Finished task 2.0 in stage 63.0 (TID 164). 1238 bytes result sent to driver
17/12/23 18:52:50 INFO TaskSetManager: Starting task 3.0 in stage 63.0 (TID 165, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:50 INFO Executor: Running task 3.0 in stage 63.0 (TID 165)
17/12/23 18:52:50 INFO TaskSetManager: Finished task 2.0 in stage 63.0 (TID 164) in 496 ms on localhost (executor driver) (3/6)
17/12/23 18:52:50 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:52:51 INFO Executor: Finished task 3.0 in stage 63.0 (TID 165). 1325 bytes result sent to driver
17/12/23 18:52:51 INFO TaskSetManager: Starting task 4.0 in stage 63.0 (TID 166, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:51 INFO TaskSetManager: Finished task 3.0 in stage 63.0 (TID 165) in 505 ms on localhost (executor driver) (4/6)
17/12/23 18:52:51 INFO Executor: Running task 4.0 in stage 63.0 (TID 166)
17/12/23 18:52:51 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:52:51 INFO Executor: Finished task 4.0 in stage 63.0 (TID 166). 1325 bytes result sent to driver
17/12/23 18:52:51 INFO TaskSetManager: Starting task 5.0 in stage 63.0 (TID 167, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:51 INFO TaskSetManager: Finished task 4.0 in stage 63.0 (TID 166) in 542 ms on localhost (executor driver) (5/6)
17/12/23 18:52:51 INFO Executor: Running task 5.0 in stage 63.0 (TID 167)
17/12/23 18:52:51 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:52:52 INFO Executor: Finished task 5.0 in stage 63.0 (TID 167). 1238 bytes result sent to driver
17/12/23 18:52:52 INFO TaskSetManager: Finished task 5.0 in stage 63.0 (TID 167) in 535 ms on localhost (executor driver) (6/6)
17/12/23 18:52:52 INFO TaskSchedulerImpl: Removed TaskSet 63.0, whose tasks have all completed, from pool 
17/12/23 18:52:52 INFO DAGScheduler: ShuffleMapStage 63 (map at StackOverflow.scala:186) finished in 3.148 s
17/12/23 18:52:52 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:52:52 INFO DAGScheduler: running: Set()
17/12/23 18:52:52 INFO DAGScheduler: waiting: Set(ResultStage 64)
17/12/23 18:52:52 INFO DAGScheduler: failed: Set()
17/12/23 18:52:52 INFO DAGScheduler: Submitting ResultStage 64 (MapPartitionsRDD[53] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:52:52 INFO MemoryStore: Block broadcast_29 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:52:52 INFO MemoryStore: Block broadcast_29_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:52:52 INFO BlockManagerInfo: Added broadcast_29_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:52 INFO SparkContext: Created broadcast 29 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:52 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 64 (MapPartitionsRDD[53] at mapValues at StackOverflow.scala:190)
17/12/23 18:52:52 INFO TaskSchedulerImpl: Adding task set 64.0 with 6 tasks
17/12/23 18:52:52 INFO TaskSetManager: Starting task 0.0 in stage 64.0 (TID 168, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:52:52 INFO Executor: Running task 0.0 in stage 64.0 (TID 168)
17/12/23 18:52:52 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:52 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:52 INFO Executor: Finished task 0.0 in stage 64.0 (TID 168). 1985 bytes result sent to driver
17/12/23 18:52:52 INFO TaskSetManager: Starting task 1.0 in stage 64.0 (TID 169, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:52:52 INFO Executor: Running task 1.0 in stage 64.0 (TID 169)
17/12/23 18:52:52 INFO TaskSetManager: Finished task 0.0 in stage 64.0 (TID 168) in 495 ms on localhost (executor driver) (1/6)
17/12/23 18:52:52 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:52 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:53 INFO Executor: Finished task 1.0 in stage 64.0 (TID 169). 1985 bytes result sent to driver
17/12/23 18:52:53 INFO TaskSetManager: Starting task 2.0 in stage 64.0 (TID 170, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:52:53 INFO Executor: Running task 2.0 in stage 64.0 (TID 170)
17/12/23 18:52:53 INFO TaskSetManager: Finished task 1.0 in stage 64.0 (TID 169) in 690 ms on localhost (executor driver) (2/6)
17/12/23 18:52:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:53 INFO Executor: Finished task 2.0 in stage 64.0 (TID 170). 1985 bytes result sent to driver
17/12/23 18:52:53 INFO TaskSetManager: Starting task 3.0 in stage 64.0 (TID 171, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:52:53 INFO TaskSetManager: Finished task 2.0 in stage 64.0 (TID 170) in 60 ms on localhost (executor driver) (3/6)
17/12/23 18:52:53 INFO Executor: Running task 3.0 in stage 64.0 (TID 171)
17/12/23 18:52:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:53 INFO Executor: Finished task 3.0 in stage 64.0 (TID 171). 2040 bytes result sent to driver
17/12/23 18:52:53 INFO TaskSetManager: Starting task 4.0 in stage 64.0 (TID 172, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:52:53 INFO TaskSetManager: Finished task 3.0 in stage 64.0 (TID 171) in 250 ms on localhost (executor driver) (4/6)
17/12/23 18:52:53 INFO Executor: Running task 4.0 in stage 64.0 (TID 172)
17/12/23 18:52:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:54 INFO Executor: Finished task 4.0 in stage 64.0 (TID 172). 2130 bytes result sent to driver
17/12/23 18:52:54 INFO TaskSetManager: Starting task 5.0 in stage 64.0 (TID 173, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:52:54 INFO Executor: Running task 5.0 in stage 64.0 (TID 173)
17/12/23 18:52:54 INFO TaskSetManager: Finished task 4.0 in stage 64.0 (TID 172) in 428 ms on localhost (executor driver) (5/6)
17/12/23 18:52:54 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:54 INFO ContextCleaner: Cleaned shuffle 14
17/12/23 18:52:54 INFO BlockManagerInfo: Removed broadcast_27_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:54 INFO BlockManagerInfo: Removed broadcast_28_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:55 INFO Executor: Finished task 5.0 in stage 64.0 (TID 173). 2026 bytes result sent to driver
17/12/23 18:52:55 INFO TaskSetManager: Finished task 5.0 in stage 64.0 (TID 173) in 934 ms on localhost (executor driver) (6/6)
17/12/23 18:52:55 INFO TaskSchedulerImpl: Removed TaskSet 64.0, whose tasks have all completed, from pool 
17/12/23 18:52:55 INFO DAGScheduler: ResultStage 64 (collect at StackOverflow.scala:191) finished in 2.857 s
17/12/23 18:52:55 INFO DAGScheduler: Job 12 finished: collect at StackOverflow.scala:191, took 6.023070 s
Iteration: 12
  * current distance: 57183.0
  * desired distance: 20.0
  * means:
             (450000,5) ==>           (450000,6)    distance:        1
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,43) ==>          (450000,47)    distance:       16
                (0,839) ==>              (0,984)    distance:    21025
                 (0,36) ==>               (0,59)    distance:      529
                  (0,1) ==>                (0,1)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,2) ==>           (150000,2)    distance:        0
            (150000,73) ==>          (150000,85)    distance:      144
           (150000,672) ==>         (150000,730)    distance:     3364
           (300000,253) ==>         (300000,288)    distance:     1225
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,40) ==>          (300000,47)    distance:       49
            (50000,278) ==>          (50000,332)    distance:     2916
             (50000,15) ==>           (50000,20)    distance:       25
              (50000,1) ==>            (50000,1)    distance:        0
            (200000,14) ==>          (200000,18)    distance:       16
             (200000,1) ==>           (200000,1)    distance:        0
           (200000,210) ==>         (200000,233)    distance:      529
             (500000,2) ==>           (500000,2)    distance:        0
           (500000,128) ==>         (500000,138)    distance:      100
            (500000,17) ==>          (500000,19)    distance:        4
            (350000,29) ==>          (350000,41)    distance:      144
             (350000,1) ==>           (350000,1)    distance:        0
           (350000,385) ==>         (350000,427)    distance:     1764
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,251) ==>         (100000,292)    distance:     1681
            (100000,20) ==>          (100000,25)    distance:       25
           (400000,432) ==>         (400000,463)    distance:      961
             (400000,2) ==>           (400000,2)    distance:        0
            (400000,63) ==>          (400000,72)    distance:       81
             (550000,4) ==>           (550000,4)    distance:        0
            (550000,31) ==>          (550000,35)    distance:       16
           (550000,749) ==>         (550000,889)    distance:    19600
           (250000,363) ==>         (250000,417)    distance:     2916
             (250000,2) ==>           (250000,2)    distance:        0
            (250000,24) ==>          (250000,30)    distance:       36
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,30) ==>          (700000,34)    distance:       16
             (700000,5) ==>           (700000,5)    distance:        0
17/12/23 18:52:55 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:52:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:52:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:52:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:52:55 INFO DAGScheduler: Registering RDD 54 (map at StackOverflow.scala:186)
17/12/23 18:52:55 INFO DAGScheduler: Got job 13 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:52:55 INFO DAGScheduler: Final stage: ResultStage 69 (collect at StackOverflow.scala:191)
17/12/23 18:52:55 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 68)
17/12/23 18:52:55 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 68)
17/12/23 18:52:55 INFO DAGScheduler: Submitting ShuffleMapStage 68 (MapPartitionsRDD[54] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:52:55 INFO MemoryStore: Block broadcast_30 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:52:55 INFO MemoryStore: Block broadcast_30_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:52:55 INFO BlockManagerInfo: Added broadcast_30_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:52:55 INFO SparkContext: Created broadcast 30 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:55 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 68 (MapPartitionsRDD[54] at map at StackOverflow.scala:186)
17/12/23 18:52:55 INFO TaskSchedulerImpl: Adding task set 68.0 with 6 tasks
17/12/23 18:52:55 INFO TaskSetManager: Starting task 0.0 in stage 68.0 (TID 174, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:55 INFO Executor: Running task 0.0 in stage 68.0 (TID 174)
17/12/23 18:52:55 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:52:55 INFO Executor: Finished task 0.0 in stage 68.0 (TID 174). 1238 bytes result sent to driver
17/12/23 18:52:55 INFO TaskSetManager: Starting task 1.0 in stage 68.0 (TID 175, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:55 INFO Executor: Running task 1.0 in stage 68.0 (TID 175)
17/12/23 18:52:55 INFO TaskSetManager: Finished task 0.0 in stage 68.0 (TID 174) in 530 ms on localhost (executor driver) (1/6)
17/12/23 18:52:55 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:52:56 INFO Executor: Finished task 1.0 in stage 68.0 (TID 175). 1325 bytes result sent to driver
17/12/23 18:52:56 INFO TaskSetManager: Starting task 2.0 in stage 68.0 (TID 176, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:56 INFO Executor: Running task 2.0 in stage 68.0 (TID 176)
17/12/23 18:52:56 INFO TaskSetManager: Finished task 1.0 in stage 68.0 (TID 175) in 522 ms on localhost (executor driver) (2/6)
17/12/23 18:52:56 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:52:56 INFO Executor: Finished task 2.0 in stage 68.0 (TID 176). 1415 bytes result sent to driver
17/12/23 18:52:56 INFO TaskSetManager: Starting task 3.0 in stage 68.0 (TID 177, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:56 INFO Executor: Running task 3.0 in stage 68.0 (TID 177)
17/12/23 18:52:56 INFO TaskSetManager: Finished task 2.0 in stage 68.0 (TID 176) in 498 ms on localhost (executor driver) (3/6)
17/12/23 18:52:56 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:52:57 INFO Executor: Finished task 3.0 in stage 68.0 (TID 177). 1238 bytes result sent to driver
17/12/23 18:52:57 INFO TaskSetManager: Starting task 4.0 in stage 68.0 (TID 178, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:57 INFO Executor: Running task 4.0 in stage 68.0 (TID 178)
17/12/23 18:52:57 INFO TaskSetManager: Finished task 3.0 in stage 68.0 (TID 177) in 540 ms on localhost (executor driver) (4/6)
17/12/23 18:52:57 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:52:57 INFO Executor: Finished task 4.0 in stage 68.0 (TID 178). 1238 bytes result sent to driver
17/12/23 18:52:57 INFO TaskSetManager: Starting task 5.0 in stage 68.0 (TID 179, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:52:57 INFO Executor: Running task 5.0 in stage 68.0 (TID 179)
17/12/23 18:52:57 INFO TaskSetManager: Finished task 4.0 in stage 68.0 (TID 178) in 519 ms on localhost (executor driver) (5/6)
17/12/23 18:52:57 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:52:58 INFO Executor: Finished task 5.0 in stage 68.0 (TID 179). 1325 bytes result sent to driver
17/12/23 18:52:58 INFO TaskSetManager: Finished task 5.0 in stage 68.0 (TID 179) in 513 ms on localhost (executor driver) (6/6)
17/12/23 18:52:58 INFO TaskSchedulerImpl: Removed TaskSet 68.0, whose tasks have all completed, from pool 
17/12/23 18:52:58 INFO DAGScheduler: ShuffleMapStage 68 (map at StackOverflow.scala:186) finished in 3.119 s
17/12/23 18:52:58 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:52:58 INFO DAGScheduler: running: Set()
17/12/23 18:52:58 INFO DAGScheduler: waiting: Set(ResultStage 69)
17/12/23 18:52:58 INFO DAGScheduler: failed: Set()
17/12/23 18:52:58 INFO DAGScheduler: Submitting ResultStage 69 (MapPartitionsRDD[56] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:52:58 INFO MemoryStore: Block broadcast_31 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:52:58 INFO MemoryStore: Block broadcast_31_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:52:58 INFO BlockManagerInfo: Added broadcast_31_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:52:58 INFO SparkContext: Created broadcast 31 from broadcast at DAGScheduler.scala:996
17/12/23 18:52:58 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 69 (MapPartitionsRDD[56] at mapValues at StackOverflow.scala:190)
17/12/23 18:52:58 INFO TaskSchedulerImpl: Adding task set 69.0 with 6 tasks
17/12/23 18:52:58 INFO TaskSetManager: Starting task 0.0 in stage 69.0 (TID 180, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:52:58 INFO Executor: Running task 0.0 in stage 69.0 (TID 180)
17/12/23 18:52:58 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:58 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:58 INFO Executor: Finished task 0.0 in stage 69.0 (TID 180). 1985 bytes result sent to driver
17/12/23 18:52:58 INFO TaskSetManager: Starting task 1.0 in stage 69.0 (TID 181, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:52:58 INFO Executor: Running task 1.0 in stage 69.0 (TID 181)
17/12/23 18:52:58 INFO TaskSetManager: Finished task 0.0 in stage 69.0 (TID 180) in 468 ms on localhost (executor driver) (1/6)
17/12/23 18:52:58 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:58 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:59 INFO Executor: Finished task 1.0 in stage 69.0 (TID 181). 2072 bytes result sent to driver
17/12/23 18:52:59 INFO TaskSetManager: Starting task 2.0 in stage 69.0 (TID 182, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:52:59 INFO Executor: Running task 2.0 in stage 69.0 (TID 182)
17/12/23 18:52:59 INFO TaskSetManager: Finished task 1.0 in stage 69.0 (TID 181) in 690 ms on localhost (executor driver) (2/6)
17/12/23 18:52:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:59 INFO Executor: Finished task 2.0 in stage 69.0 (TID 182). 1985 bytes result sent to driver
17/12/23 18:52:59 INFO TaskSetManager: Starting task 3.0 in stage 69.0 (TID 183, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:52:59 INFO TaskSetManager: Finished task 2.0 in stage 69.0 (TID 182) in 55 ms on localhost (executor driver) (3/6)
17/12/23 18:52:59 INFO Executor: Running task 3.0 in stage 69.0 (TID 183)
17/12/23 18:52:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:52:59 INFO Executor: Finished task 3.0 in stage 69.0 (TID 183). 2040 bytes result sent to driver
17/12/23 18:52:59 INFO TaskSetManager: Starting task 4.0 in stage 69.0 (TID 184, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:52:59 INFO TaskSetManager: Finished task 3.0 in stage 69.0 (TID 183) in 252 ms on localhost (executor driver) (4/6)
17/12/23 18:52:59 INFO Executor: Running task 4.0 in stage 69.0 (TID 184)
17/12/23 18:52:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:52:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:00 INFO Executor: Finished task 4.0 in stage 69.0 (TID 184). 2040 bytes result sent to driver
17/12/23 18:53:00 INFO TaskSetManager: Starting task 5.0 in stage 69.0 (TID 185, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:53:00 INFO Executor: Running task 5.0 in stage 69.0 (TID 185)
17/12/23 18:53:00 INFO TaskSetManager: Finished task 4.0 in stage 69.0 (TID 184) in 442 ms on localhost (executor driver) (5/6)
17/12/23 18:53:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:00 INFO ContextCleaner: Cleaned shuffle 15
17/12/23 18:53:00 INFO BlockManagerInfo: Removed broadcast_29_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:00 INFO BlockManagerInfo: Removed broadcast_30_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:01 INFO Executor: Finished task 5.0 in stage 69.0 (TID 185). 2113 bytes result sent to driver
17/12/23 18:53:01 INFO TaskSetManager: Finished task 5.0 in stage 69.0 (TID 185) in 1007 ms on localhost (executor driver) (6/6)
17/12/23 18:53:01 INFO TaskSchedulerImpl: Removed TaskSet 69.0, whose tasks have all completed, from pool 
17/12/23 18:53:01 INFO DAGScheduler: ResultStage 69 (collect at StackOverflow.scala:191) finished in 2.907 s
17/12/23 18:53:01 INFO DAGScheduler: Job 13 finished: collect at StackOverflow.scala:191, took 6.042310 s
Iteration: 13
  * current distance: 87765.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,47) ==>          (450000,51)    distance:       16
                (0,984) ==>             (0,1098)    distance:    12996
                 (0,59) ==>               (0,85)    distance:      676
                  (0,1) ==>                (0,1)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,2) ==>           (150000,2)    distance:        0
            (150000,85) ==>          (150000,97)    distance:      144
           (150000,730) ==>         (150000,788)    distance:     3364
           (300000,288) ==>         (300000,319)    distance:      961
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,47) ==>          (300000,53)    distance:       36
            (50000,332) ==>          (50000,389)    distance:     3249
             (50000,20) ==>           (50000,28)    distance:       64
              (50000,1) ==>            (50000,1)    distance:        0
            (200000,18) ==>          (200000,23)    distance:       25
             (200000,1) ==>           (200000,1)    distance:        0
           (200000,233) ==>         (200000,257)    distance:      576
             (500000,2) ==>           (500000,2)    distance:        0
           (500000,138) ==>         (500000,146)    distance:       64
            (500000,19) ==>          (500000,21)    distance:        4
            (350000,41) ==>          (350000,53)    distance:      144
             (350000,1) ==>           (350000,1)    distance:        0
           (350000,427) ==>         (350000,458)    distance:      961
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,292) ==>         (100000,335)    distance:     1849
            (100000,25) ==>          (100000,32)    distance:       49
           (400000,463) ==>         (400000,479)    distance:      256
             (400000,2) ==>           (400000,2)    distance:        0
            (400000,72) ==>          (400000,80)    distance:       64
             (550000,4) ==>           (550000,4)    distance:        0
            (550000,35) ==>          (550000,40)    distance:       25
           (550000,889) ==>        (550000,1130)    distance:    58081
           (250000,417) ==>         (250000,481)    distance:     4096
             (250000,2) ==>           (250000,2)    distance:        0
            (250000,30) ==>          (250000,37)    distance:       49
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,34) ==>          (700000,38)    distance:       16
             (700000,5) ==>           (700000,5)    distance:        0
17/12/23 18:53:01 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:53:01 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:53:01 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:53:01 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:53:01 INFO DAGScheduler: Registering RDD 57 (map at StackOverflow.scala:186)
17/12/23 18:53:01 INFO DAGScheduler: Got job 14 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:53:01 INFO DAGScheduler: Final stage: ResultStage 74 (collect at StackOverflow.scala:191)
17/12/23 18:53:01 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 73)
17/12/23 18:53:01 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 73)
17/12/23 18:53:01 INFO DAGScheduler: Submitting ShuffleMapStage 73 (MapPartitionsRDD[57] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:53:01 INFO MemoryStore: Block broadcast_32 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:53:01 INFO MemoryStore: Block broadcast_32_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:53:01 INFO BlockManagerInfo: Added broadcast_32_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:01 INFO SparkContext: Created broadcast 32 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:01 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 73 (MapPartitionsRDD[57] at map at StackOverflow.scala:186)
17/12/23 18:53:01 INFO TaskSchedulerImpl: Adding task set 73.0 with 6 tasks
17/12/23 18:53:01 INFO TaskSetManager: Starting task 0.0 in stage 73.0 (TID 186, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:01 INFO Executor: Running task 0.0 in stage 73.0 (TID 186)
17/12/23 18:53:01 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:53:01 INFO Executor: Finished task 0.0 in stage 73.0 (TID 186). 1238 bytes result sent to driver
17/12/23 18:53:01 INFO TaskSetManager: Starting task 1.0 in stage 73.0 (TID 187, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:01 INFO Executor: Running task 1.0 in stage 73.0 (TID 187)
17/12/23 18:53:01 INFO TaskSetManager: Finished task 0.0 in stage 73.0 (TID 186) in 504 ms on localhost (executor driver) (1/6)
17/12/23 18:53:01 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:53:02 INFO Executor: Finished task 1.0 in stage 73.0 (TID 187). 1238 bytes result sent to driver
17/12/23 18:53:02 INFO TaskSetManager: Starting task 2.0 in stage 73.0 (TID 188, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:02 INFO Executor: Running task 2.0 in stage 73.0 (TID 188)
17/12/23 18:53:02 INFO TaskSetManager: Finished task 1.0 in stage 73.0 (TID 187) in 532 ms on localhost (executor driver) (2/6)
17/12/23 18:53:02 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:53:02 INFO Executor: Finished task 2.0 in stage 73.0 (TID 188). 1325 bytes result sent to driver
17/12/23 18:53:02 INFO TaskSetManager: Starting task 3.0 in stage 73.0 (TID 189, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:02 INFO Executor: Running task 3.0 in stage 73.0 (TID 189)
17/12/23 18:53:02 INFO TaskSetManager: Finished task 2.0 in stage 73.0 (TID 188) in 528 ms on localhost (executor driver) (3/6)
17/12/23 18:53:02 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:53:03 INFO Executor: Finished task 3.0 in stage 73.0 (TID 189). 1325 bytes result sent to driver
17/12/23 18:53:03 INFO TaskSetManager: Starting task 4.0 in stage 73.0 (TID 190, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:03 INFO Executor: Running task 4.0 in stage 73.0 (TID 190)
17/12/23 18:53:03 INFO TaskSetManager: Finished task 3.0 in stage 73.0 (TID 189) in 529 ms on localhost (executor driver) (4/6)
17/12/23 18:53:03 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:53:03 INFO Executor: Finished task 4.0 in stage 73.0 (TID 190). 1325 bytes result sent to driver
17/12/23 18:53:03 INFO TaskSetManager: Starting task 5.0 in stage 73.0 (TID 191, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:03 INFO TaskSetManager: Finished task 4.0 in stage 73.0 (TID 190) in 547 ms on localhost (executor driver) (5/6)
17/12/23 18:53:03 INFO Executor: Running task 5.0 in stage 73.0 (TID 191)
17/12/23 18:53:03 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:53:04 INFO Executor: Finished task 5.0 in stage 73.0 (TID 191). 1325 bytes result sent to driver
17/12/23 18:53:04 INFO TaskSetManager: Finished task 5.0 in stage 73.0 (TID 191) in 552 ms on localhost (executor driver) (6/6)
17/12/23 18:53:04 INFO TaskSchedulerImpl: Removed TaskSet 73.0, whose tasks have all completed, from pool 
17/12/23 18:53:04 INFO DAGScheduler: ShuffleMapStage 73 (map at StackOverflow.scala:186) finished in 3.192 s
17/12/23 18:53:04 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:53:04 INFO DAGScheduler: running: Set()
17/12/23 18:53:04 INFO DAGScheduler: waiting: Set(ResultStage 74)
17/12/23 18:53:04 INFO DAGScheduler: failed: Set()
17/12/23 18:53:04 INFO DAGScheduler: Submitting ResultStage 74 (MapPartitionsRDD[59] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:53:04 INFO MemoryStore: Block broadcast_33 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:53:04 INFO MemoryStore: Block broadcast_33_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:53:04 INFO BlockManagerInfo: Added broadcast_33_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:04 INFO SparkContext: Created broadcast 33 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:04 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 74 (MapPartitionsRDD[59] at mapValues at StackOverflow.scala:190)
17/12/23 18:53:04 INFO TaskSchedulerImpl: Adding task set 74.0 with 6 tasks
17/12/23 18:53:04 INFO TaskSetManager: Starting task 0.0 in stage 74.0 (TID 192, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:53:04 INFO Executor: Running task 0.0 in stage 74.0 (TID 192)
17/12/23 18:53:04 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:04 INFO Executor: Finished task 0.0 in stage 74.0 (TID 192). 2072 bytes result sent to driver
17/12/23 18:53:04 INFO TaskSetManager: Starting task 1.0 in stage 74.0 (TID 193, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:53:04 INFO Executor: Running task 1.0 in stage 74.0 (TID 193)
17/12/23 18:53:04 INFO TaskSetManager: Finished task 0.0 in stage 74.0 (TID 192) in 507 ms on localhost (executor driver) (1/6)
17/12/23 18:53:04 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:05 INFO Executor: Finished task 1.0 in stage 74.0 (TID 193). 2072 bytes result sent to driver
17/12/23 18:53:05 INFO TaskSetManager: Starting task 2.0 in stage 74.0 (TID 194, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:53:05 INFO Executor: Running task 2.0 in stage 74.0 (TID 194)
17/12/23 18:53:05 INFO TaskSetManager: Finished task 1.0 in stage 74.0 (TID 193) in 710 ms on localhost (executor driver) (2/6)
17/12/23 18:53:05 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:05 INFO Executor: Finished task 2.0 in stage 74.0 (TID 194). 1985 bytes result sent to driver
17/12/23 18:53:05 INFO TaskSetManager: Starting task 3.0 in stage 74.0 (TID 195, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:53:05 INFO Executor: Running task 3.0 in stage 74.0 (TID 195)
17/12/23 18:53:05 INFO TaskSetManager: Finished task 2.0 in stage 74.0 (TID 194) in 40 ms on localhost (executor driver) (3/6)
17/12/23 18:53:05 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:05 INFO Executor: Finished task 3.0 in stage 74.0 (TID 195). 2040 bytes result sent to driver
17/12/23 18:53:05 INFO TaskSetManager: Starting task 4.0 in stage 74.0 (TID 196, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:53:05 INFO TaskSetManager: Finished task 3.0 in stage 74.0 (TID 195) in 228 ms on localhost (executor driver) (4/6)
17/12/23 18:53:05 INFO Executor: Running task 4.0 in stage 74.0 (TID 196)
17/12/23 18:53:05 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:06 INFO Executor: Finished task 4.0 in stage 74.0 (TID 196). 1953 bytes result sent to driver
17/12/23 18:53:06 INFO TaskSetManager: Starting task 5.0 in stage 74.0 (TID 197, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:53:06 INFO Executor: Running task 5.0 in stage 74.0 (TID 197)
17/12/23 18:53:06 INFO TaskSetManager: Finished task 4.0 in stage 74.0 (TID 196) in 472 ms on localhost (executor driver) (5/6)
17/12/23 18:53:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
Iteration: 14
  * current distance: 30569.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,51) ==>          (450000,54)    distance:        9
               (0,1098) ==>             (0,1197)    distance:     9801
                 (0,85) ==>              (0,115)    distance:      900
                  (0,1) ==>                (0,2)    distance:        1
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,2) ==>           (150000,3)    distance:        1
            (150000,97) ==>         (150000,110)    distance:      169
           (150000,788) ==>         (150000,836)    distance:     2304
           (300000,319) ==>         (300000,346)    distance:      729
             (300000,2) ==>           (300000,2)    distance:        0
17/12/23 18:53:07 INFO Executor: Finished task 5.0 in stage 74.0 (TID 197). 1953 bytes result sent to driver
            (300000,53) ==>          (300000,58)    distance:       25
17/12/23 18:53:07 INFO TaskSetManager: Finished task 5.0 in stage 74.0 (TID 197) in 1027 ms on localhost (executor driver) (6/6)
            (50000,389) ==>          (50000,466)    distance:     5929
17/12/23 18:53:07 INFO TaskSchedulerImpl: Removed TaskSet 74.0, whose tasks have all completed, from pool 
             (50000,28) ==>           (50000,37)    distance:       81
17/12/23 18:53:07 INFO DAGScheduler: ResultStage 74 (collect at StackOverflow.scala:191) finished in 2.978 s
              (50000,1) ==>            (50000,1)    distance:        0
17/12/23 18:53:07 INFO DAGScheduler: Job 14 finished: collect at StackOverflow.scala:191, took 6.182808 s
            (200000,23) ==>          (200000,27)    distance:       16
             (200000,1) ==>           (200000,1)    distance:        0
           (200000,257) ==>         (200000,280)    distance:      529
             (500000,2) ==>           (500000,2)    distance:        0
           (500000,146) ==>         (500000,156)    distance:      100
            (500000,21) ==>          (500000,23)    distance:        4
            (350000,53) ==>          (350000,64)    distance:      121
             (350000,1) ==>           (350000,1)    distance:        0
           (350000,458) ==>         (350000,485)    distance:      729
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,335) ==>         (100000,382)    distance:     2209
            (100000,32) ==>          (100000,38)    distance:       36
           (400000,479) ==>         (400000,497)    distance:      324
             (400000,2) ==>           (400000,2)    distance:        0
            (400000,80) ==>          (400000,87)    distance:       49
             (550000,4) ==>           (550000,5)    distance:        1
            (550000,40) ==>          (550000,46)    distance:       36
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,481) ==>         (250000,561)    distance:     6400
             (250000,2) ==>           (250000,2)    distance:        0
            (250000,37) ==>          (250000,44)    distance:       49
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,38) ==>          (700000,42)    distance:       16
             (700000,5) ==>           (700000,6)    distance:        1
17/12/23 18:53:07 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:53:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:53:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:53:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:53:07 INFO DAGScheduler: Registering RDD 60 (map at StackOverflow.scala:186)
17/12/23 18:53:07 INFO DAGScheduler: Got job 15 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:53:07 INFO DAGScheduler: Final stage: ResultStage 79 (collect at StackOverflow.scala:191)
17/12/23 18:53:07 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 78)
17/12/23 18:53:07 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 78)
17/12/23 18:53:07 INFO DAGScheduler: Submitting ShuffleMapStage 78 (MapPartitionsRDD[60] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:53:07 INFO MemoryStore: Block broadcast_34 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:53:07 INFO MemoryStore: Block broadcast_34_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:53:07 INFO BlockManagerInfo: Added broadcast_34_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:07 INFO SparkContext: Created broadcast 34 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:07 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 78 (MapPartitionsRDD[60] at map at StackOverflow.scala:186)
17/12/23 18:53:07 INFO TaskSchedulerImpl: Adding task set 78.0 with 6 tasks
17/12/23 18:53:07 INFO TaskSetManager: Starting task 0.0 in stage 78.0 (TID 198, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:07 INFO Executor: Running task 0.0 in stage 78.0 (TID 198)
17/12/23 18:53:07 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:53:07 INFO ContextCleaner: Cleaned shuffle 16
17/12/23 18:53:07 INFO BlockManagerInfo: Removed broadcast_31_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:07 INFO ContextCleaner: Cleaned shuffle 17
17/12/23 18:53:07 INFO BlockManagerInfo: Removed broadcast_32_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:07 INFO BlockManagerInfo: Removed broadcast_33_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:07 INFO Executor: Finished task 0.0 in stage 78.0 (TID 198). 1398 bytes result sent to driver
17/12/23 18:53:07 INFO TaskSetManager: Starting task 1.0 in stage 78.0 (TID 199, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:07 INFO Executor: Running task 1.0 in stage 78.0 (TID 199)
17/12/23 18:53:07 INFO TaskSetManager: Finished task 0.0 in stage 78.0 (TID 198) in 566 ms on localhost (executor driver) (1/6)
17/12/23 18:53:07 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:53:08 INFO Executor: Finished task 1.0 in stage 78.0 (TID 199). 1325 bytes result sent to driver
17/12/23 18:53:08 INFO TaskSetManager: Starting task 2.0 in stage 78.0 (TID 200, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:08 INFO TaskSetManager: Finished task 1.0 in stage 78.0 (TID 199) in 502 ms on localhost (executor driver) (2/6)
17/12/23 18:53:08 INFO Executor: Running task 2.0 in stage 78.0 (TID 200)
17/12/23 18:53:08 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:53:08 INFO Executor: Finished task 2.0 in stage 78.0 (TID 200). 1415 bytes result sent to driver
17/12/23 18:53:08 INFO TaskSetManager: Starting task 3.0 in stage 78.0 (TID 201, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:08 INFO TaskSetManager: Finished task 2.0 in stage 78.0 (TID 200) in 528 ms on localhost (executor driver) (3/6)
17/12/23 18:53:08 INFO Executor: Running task 3.0 in stage 78.0 (TID 201)
17/12/23 18:53:08 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:53:09 INFO Executor: Finished task 3.0 in stage 78.0 (TID 201). 1325 bytes result sent to driver
17/12/23 18:53:09 INFO TaskSetManager: Starting task 4.0 in stage 78.0 (TID 202, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:09 INFO TaskSetManager: Finished task 3.0 in stage 78.0 (TID 201) in 487 ms on localhost (executor driver) (4/6)
17/12/23 18:53:09 INFO Executor: Running task 4.0 in stage 78.0 (TID 202)
17/12/23 18:53:09 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:53:09 INFO Executor: Finished task 4.0 in stage 78.0 (TID 202). 1325 bytes result sent to driver
17/12/23 18:53:09 INFO TaskSetManager: Starting task 5.0 in stage 78.0 (TID 203, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:09 INFO Executor: Running task 5.0 in stage 78.0 (TID 203)
17/12/23 18:53:09 INFO TaskSetManager: Finished task 4.0 in stage 78.0 (TID 202) in 580 ms on localhost (executor driver) (5/6)
17/12/23 18:53:09 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:53:10 INFO Executor: Finished task 5.0 in stage 78.0 (TID 203). 1415 bytes result sent to driver
17/12/23 18:53:10 INFO TaskSetManager: Finished task 5.0 in stage 78.0 (TID 203) in 517 ms on localhost (executor driver) (6/6)
17/12/23 18:53:10 INFO TaskSchedulerImpl: Removed TaskSet 78.0, whose tasks have all completed, from pool 
17/12/23 18:53:10 INFO DAGScheduler: ShuffleMapStage 78 (map at StackOverflow.scala:186) finished in 3.180 s
17/12/23 18:53:10 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:53:10 INFO DAGScheduler: running: Set()
17/12/23 18:53:10 INFO DAGScheduler: waiting: Set(ResultStage 79)
17/12/23 18:53:10 INFO DAGScheduler: failed: Set()
17/12/23 18:53:10 INFO DAGScheduler: Submitting ResultStage 79 (MapPartitionsRDD[62] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:53:10 INFO MemoryStore: Block broadcast_35 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:53:10 INFO MemoryStore: Block broadcast_35_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:53:10 INFO BlockManagerInfo: Added broadcast_35_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:10 INFO SparkContext: Created broadcast 35 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:10 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 79 (MapPartitionsRDD[62] at mapValues at StackOverflow.scala:190)
17/12/23 18:53:10 INFO TaskSchedulerImpl: Adding task set 79.0 with 6 tasks
17/12/23 18:53:10 INFO TaskSetManager: Starting task 0.0 in stage 79.0 (TID 204, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:53:10 INFO Executor: Running task 0.0 in stage 79.0 (TID 204)
17/12/23 18:53:10 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:10 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:10 INFO Executor: Finished task 0.0 in stage 79.0 (TID 204). 2072 bytes result sent to driver
17/12/23 18:53:10 INFO TaskSetManager: Starting task 1.0 in stage 79.0 (TID 205, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:53:10 INFO Executor: Running task 1.0 in stage 79.0 (TID 205)
17/12/23 18:53:10 INFO TaskSetManager: Finished task 0.0 in stage 79.0 (TID 204) in 480 ms on localhost (executor driver) (1/6)
17/12/23 18:53:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:11 INFO Executor: Finished task 1.0 in stage 79.0 (TID 205). 2072 bytes result sent to driver
17/12/23 18:53:11 INFO TaskSetManager: Starting task 2.0 in stage 79.0 (TID 206, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:53:11 INFO Executor: Running task 2.0 in stage 79.0 (TID 206)
17/12/23 18:53:11 INFO TaskSetManager: Finished task 1.0 in stage 79.0 (TID 205) in 724 ms on localhost (executor driver) (2/6)
17/12/23 18:53:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:11 INFO Executor: Finished task 2.0 in stage 79.0 (TID 206). 2072 bytes result sent to driver
17/12/23 18:53:11 INFO TaskSetManager: Starting task 3.0 in stage 79.0 (TID 207, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:53:11 INFO Executor: Running task 3.0 in stage 79.0 (TID 207)
17/12/23 18:53:11 INFO TaskSetManager: Finished task 2.0 in stage 79.0 (TID 206) in 37 ms on localhost (executor driver) (3/6)
17/12/23 18:53:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:12 INFO Executor: Finished task 3.0 in stage 79.0 (TID 207). 2040 bytes result sent to driver
17/12/23 18:53:12 INFO TaskSetManager: Starting task 4.0 in stage 79.0 (TID 208, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:53:12 INFO TaskSetManager: Finished task 3.0 in stage 79.0 (TID 207) in 275 ms on localhost (executor driver) (4/6)
17/12/23 18:53:12 INFO Executor: Running task 4.0 in stage 79.0 (TID 208)
17/12/23 18:53:12 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:12 INFO Executor: Finished task 4.0 in stage 79.0 (TID 208). 2040 bytes result sent to driver
17/12/23 18:53:12 INFO TaskSetManager: Starting task 5.0 in stage 79.0 (TID 209, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:53:12 INFO Executor: Running task 5.0 in stage 79.0 (TID 209)
17/12/23 18:53:12 INFO TaskSetManager: Finished task 4.0 in stage 79.0 (TID 208) in 370 ms on localhost (executor driver) (5/6)
17/12/23 18:53:12 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:13 INFO Executor: Finished task 5.0 in stage 79.0 (TID 209). 1953 bytes result sent to driver
17/12/23 18:53:13 INFO TaskSetManager: Finished task 5.0 in stage 79.0 (TID 209) in 958 ms on localhost (executor driver) (6/6)
17/12/23 18:53:13 INFO TaskSchedulerImpl: Removed TaskSet 79.0, whose tasks have all completed, from pool 
17/12/23 18:53:13 INFO DAGScheduler: ResultStage 79 (collect at StackOverflow.scala:191) finished in 2.844 s
17/12/23 18:53:13 INFO DAGScheduler: Job 15 finished: collect at StackOverflow.scala:191, took 6.031602 s
Iteration: 15
  * current distance: 30061.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,54) ==>          (450000,57)    distance:        9
               (0,1197) ==>             (0,1296)    distance:     9801
                (0,115) ==>              (0,147)    distance:     1024
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,110) ==>         (150000,124)    distance:      196
           (150000,836) ==>         (150000,882)    distance:     2116
           (300000,346) ==>         (300000,358)    distance:      144
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,58) ==>          (300000,63)    distance:       25
            (50000,466) ==>          (50000,552)    distance:     7396
             (50000,37) ==>           (50000,47)    distance:      100
              (50000,1) ==>            (50000,2)    distance:        1
            (200000,27) ==>          (200000,32)    distance:       25
             (200000,1) ==>           (200000,2)    distance:        1
           (200000,280) ==>         (200000,302)    distance:      484
             (500000,2) ==>           (500000,3)    distance:        1
           (500000,156) ==>         (500000,161)    distance:       25
            (500000,23) ==>          (500000,25)    distance:        4
            (350000,64) ==>          (350000,76)    distance:      144
             (350000,1) ==>           (350000,1)    distance:        0
           (350000,485) ==>         (350000,521)    distance:     1296
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,382) ==>         (100000,420)    distance:     1444
            (100000,38) ==>          (100000,44)    distance:       36
           (400000,497) ==>         (400000,503)    distance:       36
             (400000,2) ==>           (400000,2)    distance:        0
            (400000,87) ==>          (400000,91)    distance:       16
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,46) ==>          (550000,51)    distance:       25
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,561) ==>         (250000,636)    distance:     5625
             (250000,2) ==>           (250000,2)    distance:        0
            (250000,44) ==>          (250000,53)    distance:       81
             (700000,0) ==>           (700000,1)    distance:        1
            (700000,42) ==>          (700000,44)    distance:        4
             (700000,6) ==>           (700000,7)    distance:        1
17/12/23 18:53:13 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:53:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:53:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:53:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:53:13 INFO DAGScheduler: Registering RDD 63 (map at StackOverflow.scala:186)
17/12/23 18:53:13 INFO DAGScheduler: Got job 16 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:53:13 INFO DAGScheduler: Final stage: ResultStage 84 (collect at StackOverflow.scala:191)
17/12/23 18:53:13 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 83)
17/12/23 18:53:13 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 83)
17/12/23 18:53:13 INFO DAGScheduler: Submitting ShuffleMapStage 83 (MapPartitionsRDD[63] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:53:13 INFO MemoryStore: Block broadcast_36 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:53:13 INFO MemoryStore: Block broadcast_36_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:53:13 INFO BlockManagerInfo: Added broadcast_36_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:13 INFO SparkContext: Created broadcast 36 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:13 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 83 (MapPartitionsRDD[63] at map at StackOverflow.scala:186)
17/12/23 18:53:13 INFO TaskSchedulerImpl: Adding task set 83.0 with 6 tasks
17/12/23 18:53:13 INFO TaskSetManager: Starting task 0.0 in stage 83.0 (TID 210, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:13 INFO Executor: Running task 0.0 in stage 83.0 (TID 210)
17/12/23 18:53:13 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:53:13 INFO Executor: Finished task 0.0 in stage 83.0 (TID 210). 1325 bytes result sent to driver
17/12/23 18:53:13 INFO TaskSetManager: Starting task 1.0 in stage 83.0 (TID 211, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:13 INFO TaskSetManager: Finished task 0.0 in stage 83.0 (TID 210) in 522 ms on localhost (executor driver) (1/6)
17/12/23 18:53:13 INFO Executor: Running task 1.0 in stage 83.0 (TID 211)
17/12/23 18:53:13 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:53:14 INFO Executor: Finished task 1.0 in stage 83.0 (TID 211). 1238 bytes result sent to driver
17/12/23 18:53:14 INFO TaskSetManager: Starting task 2.0 in stage 83.0 (TID 212, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:14 INFO TaskSetManager: Finished task 1.0 in stage 83.0 (TID 211) in 512 ms on localhost (executor driver) (2/6)
17/12/23 18:53:14 INFO Executor: Running task 2.0 in stage 83.0 (TID 212)
17/12/23 18:53:14 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:53:14 INFO ContextCleaner: Cleaned shuffle 18
17/12/23 18:53:14 INFO BlockManagerInfo: Removed broadcast_34_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:14 INFO BlockManagerInfo: Removed broadcast_35_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:14 INFO Executor: Finished task 2.0 in stage 83.0 (TID 212). 1398 bytes result sent to driver
17/12/23 18:53:14 INFO TaskSetManager: Starting task 3.0 in stage 83.0 (TID 213, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:14 INFO Executor: Running task 3.0 in stage 83.0 (TID 213)
17/12/23 18:53:14 INFO TaskSetManager: Finished task 2.0 in stage 83.0 (TID 212) in 567 ms on localhost (executor driver) (3/6)
17/12/23 18:53:14 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:53:15 INFO Executor: Finished task 3.0 in stage 83.0 (TID 213). 1325 bytes result sent to driver
17/12/23 18:53:15 INFO TaskSetManager: Starting task 4.0 in stage 83.0 (TID 214, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:15 INFO TaskSetManager: Finished task 3.0 in stage 83.0 (TID 213) in 515 ms on localhost (executor driver) (4/6)
17/12/23 18:53:15 INFO Executor: Running task 4.0 in stage 83.0 (TID 214)
17/12/23 18:53:15 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:53:16 INFO Executor: Finished task 4.0 in stage 83.0 (TID 214). 1325 bytes result sent to driver
17/12/23 18:53:16 INFO TaskSetManager: Starting task 5.0 in stage 83.0 (TID 215, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:16 INFO TaskSetManager: Finished task 4.0 in stage 83.0 (TID 214) in 508 ms on localhost (executor driver) (5/6)
17/12/23 18:53:16 INFO Executor: Running task 5.0 in stage 83.0 (TID 215)
17/12/23 18:53:16 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:53:16 INFO Executor: Finished task 5.0 in stage 83.0 (TID 215). 1238 bytes result sent to driver
17/12/23 18:53:16 INFO TaskSetManager: Finished task 5.0 in stage 83.0 (TID 215) in 555 ms on localhost (executor driver) (6/6)
17/12/23 18:53:16 INFO TaskSchedulerImpl: Removed TaskSet 83.0, whose tasks have all completed, from pool 
17/12/23 18:53:16 INFO DAGScheduler: ShuffleMapStage 83 (map at StackOverflow.scala:186) finished in 3.174 s
17/12/23 18:53:16 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:53:16 INFO DAGScheduler: running: Set()
17/12/23 18:53:16 INFO DAGScheduler: waiting: Set(ResultStage 84)
17/12/23 18:53:16 INFO DAGScheduler: failed: Set()
17/12/23 18:53:16 INFO DAGScheduler: Submitting ResultStage 84 (MapPartitionsRDD[65] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:53:16 INFO MemoryStore: Block broadcast_37 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:53:16 INFO MemoryStore: Block broadcast_37_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:53:16 INFO BlockManagerInfo: Added broadcast_37_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:16 INFO SparkContext: Created broadcast 37 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:16 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 84 (MapPartitionsRDD[65] at mapValues at StackOverflow.scala:190)
17/12/23 18:53:16 INFO TaskSchedulerImpl: Adding task set 84.0 with 6 tasks
17/12/23 18:53:16 INFO TaskSetManager: Starting task 0.0 in stage 84.0 (TID 216, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:53:16 INFO Executor: Running task 0.0 in stage 84.0 (TID 216)
17/12/23 18:53:16 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:17 INFO Executor: Finished task 0.0 in stage 84.0 (TID 216). 1985 bytes result sent to driver
17/12/23 18:53:17 INFO TaskSetManager: Starting task 1.0 in stage 84.0 (TID 217, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:53:17 INFO Executor: Running task 1.0 in stage 84.0 (TID 217)
17/12/23 18:53:17 INFO TaskSetManager: Finished task 0.0 in stage 84.0 (TID 216) in 458 ms on localhost (executor driver) (1/6)
17/12/23 18:53:17 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:17 INFO Executor: Finished task 1.0 in stage 84.0 (TID 217). 2072 bytes result sent to driver
17/12/23 18:53:17 INFO TaskSetManager: Starting task 2.0 in stage 84.0 (TID 218, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:53:17 INFO Executor: Running task 2.0 in stage 84.0 (TID 218)
17/12/23 18:53:17 INFO TaskSetManager: Finished task 1.0 in stage 84.0 (TID 217) in 712 ms on localhost (executor driver) (2/6)
17/12/23 18:53:17 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:17 INFO Executor: Finished task 2.0 in stage 84.0 (TID 218). 2072 bytes result sent to driver
17/12/23 18:53:17 INFO TaskSetManager: Starting task 3.0 in stage 84.0 (TID 219, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:53:17 INFO Executor: Running task 3.0 in stage 84.0 (TID 219)
17/12/23 18:53:17 INFO TaskSetManager: Finished task 2.0 in stage 84.0 (TID 218) in 35 ms on localhost (executor driver) (3/6)
17/12/23 18:53:17 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:18 INFO Executor: Finished task 3.0 in stage 84.0 (TID 219). 2040 bytes result sent to driver
17/12/23 18:53:18 INFO TaskSetManager: Starting task 4.0 in stage 84.0 (TID 220, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:53:18 INFO TaskSetManager: Finished task 3.0 in stage 84.0 (TID 219) in 265 ms on localhost (executor driver) (4/6)
17/12/23 18:53:18 INFO Executor: Running task 4.0 in stage 84.0 (TID 220)
17/12/23 18:53:18 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:18 INFO Executor: Finished task 4.0 in stage 84.0 (TID 220). 2040 bytes result sent to driver
17/12/23 18:53:18 INFO TaskSetManager: Starting task 5.0 in stage 84.0 (TID 221, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:53:18 INFO Executor: Running task 5.0 in stage 84.0 (TID 221)
17/12/23 18:53:18 INFO TaskSetManager: Finished task 4.0 in stage 84.0 (TID 220) in 382 ms on localhost (executor driver) (5/6)
17/12/23 18:53:18 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
17/12/23 18:53:19 INFO Executor: Finished task 5.0 in stage 84.0 (TID 221). 1953 bytes result sent to driver
17/12/23 18:53:19 INFO TaskSetManager: Finished task 5.0 in stage 84.0 (TID 221) in 1000 ms on localhost (executor driver) (6/6)
17/12/23 18:53:19 INFO TaskSchedulerImpl: Removed TaskSet 84.0, whose tasks have all completed, from pool 
17/12/23 18:53:19 INFO DAGScheduler: ResultStage 84 (collect at StackOverflow.scala:191) finished in 2.849 s
17/12/23 18:53:19 INFO DAGScheduler: Job 16 finished: collect at StackOverflow.scala:191, took 6.033979 s
Iteration: 16
  * current distance: 34488.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,57) ==>          (450000,59)    distance:        4
               (0,1296) ==>             (0,1408)    distance:    12544
                (0,147) ==>              (0,180)    distance:     1089
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,124) ==>         (150000,139)    distance:      225
           (150000,882) ==>         (150000,929)    distance:     2209
           (300000,358) ==>         (300000,376)    distance:      324
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,63) ==>          (300000,67)    distance:       16
            (50000,552) ==>          (50000,667)    distance:    13225
             (50000,47) ==>           (50000,60)    distance:      169
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,32) ==>          (200000,38)    distance:       36
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,302) ==>         (200000,322)    distance:      400
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,161) ==>         (500000,165)    distance:       16
            (500000,25) ==>          (500000,28)    distance:        9
            (350000,76) ==>          (350000,87)    distance:      121
             (350000,1) ==>           (350000,1)    distance:        0
           (350000,521) ==>         (350000,552)    distance:      961
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,420) ==>         (100000,460)    distance:     1600
            (100000,44) ==>          (100000,50)    distance:       36
           (400000,503) ==>         (400000,513)    distance:      100
             (400000,2) ==>           (400000,2)    distance:        0
            (400000,91) ==>          (400000,95)    distance:       16
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,51) ==>          (550000,56)    distance:       25
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,636) ==>         (250000,672)    distance:     1296
             (250000,2) ==>           (250000,3)    distance:        1
            (250000,53) ==>          (250000,60)    distance:       49
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,44) ==>          (700000,48)    distance:       16
             (700000,7) ==>           (700000,8)    distance:        1
17/12/23 18:53:19 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:53:19 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:53:19 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:53:19 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:53:19 INFO DAGScheduler: Registering RDD 66 (map at StackOverflow.scala:186)
17/12/23 18:53:19 INFO DAGScheduler: Got job 17 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:53:19 INFO DAGScheduler: Final stage: ResultStage 89 (collect at StackOverflow.scala:191)
17/12/23 18:53:19 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 88)
17/12/23 18:53:19 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 88)
17/12/23 18:53:19 INFO DAGScheduler: Submitting ShuffleMapStage 88 (MapPartitionsRDD[66] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:53:19 INFO MemoryStore: Block broadcast_38 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:53:19 INFO MemoryStore: Block broadcast_38_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:53:19 INFO BlockManagerInfo: Added broadcast_38_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:19 INFO SparkContext: Created broadcast 38 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:19 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 88 (MapPartitionsRDD[66] at map at StackOverflow.scala:186)
17/12/23 18:53:19 INFO TaskSchedulerImpl: Adding task set 88.0 with 6 tasks
17/12/23 18:53:19 INFO TaskSetManager: Starting task 0.0 in stage 88.0 (TID 222, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:19 INFO Executor: Running task 0.0 in stage 88.0 (TID 222)
17/12/23 18:53:19 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:53:19 INFO Executor: Finished task 0.0 in stage 88.0 (TID 222). 1415 bytes result sent to driver
17/12/23 18:53:19 INFO TaskSetManager: Starting task 1.0 in stage 88.0 (TID 223, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:19 INFO Executor: Running task 1.0 in stage 88.0 (TID 223)
17/12/23 18:53:19 INFO TaskSetManager: Finished task 0.0 in stage 88.0 (TID 222) in 530 ms on localhost (executor driver) (1/6)
17/12/23 18:53:19 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:53:20 INFO Executor: Finished task 1.0 in stage 88.0 (TID 223). 1415 bytes result sent to driver
17/12/23 18:53:20 INFO TaskSetManager: Starting task 2.0 in stage 88.0 (TID 224, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:20 INFO Executor: Running task 2.0 in stage 88.0 (TID 224)
17/12/23 18:53:20 INFO TaskSetManager: Finished task 1.0 in stage 88.0 (TID 223) in 510 ms on localhost (executor driver) (2/6)
17/12/23 18:53:20 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:53:21 INFO Executor: Finished task 2.0 in stage 88.0 (TID 224). 1325 bytes result sent to driver
17/12/23 18:53:21 INFO TaskSetManager: Starting task 3.0 in stage 88.0 (TID 225, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:21 INFO Executor: Running task 3.0 in stage 88.0 (TID 225)
17/12/23 18:53:21 INFO TaskSetManager: Finished task 2.0 in stage 88.0 (TID 224) in 538 ms on localhost (executor driver) (3/6)
17/12/23 18:53:21 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:53:21 INFO Executor: Finished task 3.0 in stage 88.0 (TID 225). 1238 bytes result sent to driver
17/12/23 18:53:21 INFO TaskSetManager: Starting task 4.0 in stage 88.0 (TID 226, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:21 INFO Executor: Running task 4.0 in stage 88.0 (TID 226)
17/12/23 18:53:21 INFO TaskSetManager: Finished task 3.0 in stage 88.0 (TID 225) in 500 ms on localhost (executor driver) (4/6)
17/12/23 18:53:21 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:53:22 INFO Executor: Finished task 4.0 in stage 88.0 (TID 226). 1325 bytes result sent to driver
17/12/23 18:53:22 INFO TaskSetManager: Starting task 5.0 in stage 88.0 (TID 227, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:22 INFO Executor: Running task 5.0 in stage 88.0 (TID 227)
17/12/23 18:53:22 INFO TaskSetManager: Finished task 4.0 in stage 88.0 (TID 226) in 505 ms on localhost (executor driver) (5/6)
17/12/23 18:53:22 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:53:22 INFO ContextCleaner: Cleaned shuffle 19
17/12/23 18:53:22 INFO BlockManagerInfo: Removed broadcast_36_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:22 INFO BlockManagerInfo: Removed broadcast_37_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:22 INFO Executor: Finished task 5.0 in stage 88.0 (TID 227). 1398 bytes result sent to driver
17/12/23 18:53:22 INFO TaskSetManager: Finished task 5.0 in stage 88.0 (TID 227) in 564 ms on localhost (executor driver) (6/6)
17/12/23 18:53:22 INFO TaskSchedulerImpl: Removed TaskSet 88.0, whose tasks have all completed, from pool 
17/12/23 18:53:22 INFO DAGScheduler: ShuffleMapStage 88 (map at StackOverflow.scala:186) finished in 3.144 s
17/12/23 18:53:22 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:53:22 INFO DAGScheduler: running: Set()
17/12/23 18:53:22 INFO DAGScheduler: waiting: Set(ResultStage 89)
17/12/23 18:53:22 INFO DAGScheduler: failed: Set()
17/12/23 18:53:22 INFO DAGScheduler: Submitting ResultStage 89 (MapPartitionsRDD[68] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:53:22 INFO MemoryStore: Block broadcast_39 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:53:22 INFO MemoryStore: Block broadcast_39_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:53:22 INFO BlockManagerInfo: Added broadcast_39_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:22 INFO SparkContext: Created broadcast 39 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:22 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 89 (MapPartitionsRDD[68] at mapValues at StackOverflow.scala:190)
17/12/23 18:53:22 INFO TaskSchedulerImpl: Adding task set 89.0 with 6 tasks
17/12/23 18:53:22 INFO TaskSetManager: Starting task 0.0 in stage 89.0 (TID 228, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:53:22 INFO Executor: Running task 0.0 in stage 89.0 (TID 228)
17/12/23 18:53:22 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:23 INFO Executor: Finished task 0.0 in stage 89.0 (TID 228). 2072 bytes result sent to driver
17/12/23 18:53:23 INFO TaskSetManager: Starting task 1.0 in stage 89.0 (TID 229, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:53:23 INFO Executor: Running task 1.0 in stage 89.0 (TID 229)
17/12/23 18:53:23 INFO TaskSetManager: Finished task 0.0 in stage 89.0 (TID 228) in 529 ms on localhost (executor driver) (1/6)
17/12/23 18:53:23 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:53:23 INFO Executor: Finished task 1.0 in stage 89.0 (TID 229). 1985 bytes result sent to driver
17/12/23 18:53:23 INFO TaskSetManager: Starting task 2.0 in stage 89.0 (TID 230, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:53:23 INFO Executor: Running task 2.0 in stage 89.0 (TID 230)
17/12/23 18:53:23 INFO TaskSetManager: Finished task 1.0 in stage 89.0 (TID 229) in 705 ms on localhost (executor driver) (2/6)
17/12/23 18:53:23 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:53:23 INFO Executor: Finished task 2.0 in stage 89.0 (TID 230). 1985 bytes result sent to driver
17/12/23 18:53:23 INFO TaskSetManager: Starting task 3.0 in stage 89.0 (TID 231, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:53:23 INFO Executor: Running task 3.0 in stage 89.0 (TID 231)
17/12/23 18:53:23 INFO TaskSetManager: Finished task 2.0 in stage 89.0 (TID 230) in 38 ms on localhost (executor driver) (3/6)
17/12/23 18:53:23 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:24 INFO Executor: Finished task 3.0 in stage 89.0 (TID 231). 2127 bytes result sent to driver
17/12/23 18:53:24 INFO TaskSetManager: Starting task 4.0 in stage 89.0 (TID 232, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:53:24 INFO Executor: Running task 4.0 in stage 89.0 (TID 232)
17/12/23 18:53:24 INFO TaskSetManager: Finished task 3.0 in stage 89.0 (TID 231) in 275 ms on localhost (executor driver) (4/6)
17/12/23 18:53:24 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
17/12/23 18:53:24 INFO Executor: Finished task 4.0 in stage 89.0 (TID 232). 2040 bytes result sent to driver
17/12/23 18:53:24 INFO TaskSetManager: Starting task 5.0 in stage 89.0 (TID 233, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:53:24 INFO Executor: Running task 5.0 in stage 89.0 (TID 233)
17/12/23 18:53:24 INFO TaskSetManager: Finished task 4.0 in stage 89.0 (TID 232) in 467 ms on localhost (executor driver) (5/6)
17/12/23 18:53:24 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:25 INFO Executor: Finished task 5.0 in stage 89.0 (TID 233). 2040 bytes result sent to driver
17/12/23 18:53:25 INFO TaskSetManager: Finished task 5.0 in stage 89.0 (TID 233) in 930 ms on localhost (executor driver) (6/6)
17/12/23 18:53:25 INFO TaskSchedulerImpl: Removed TaskSet 89.0, whose tasks have all completed, from pool 
17/12/23 18:53:25 INFO DAGScheduler: ResultStage 89 (collect at StackOverflow.scala:191) finished in 2.937 s
17/12/23 18:53:25 INFO DAGScheduler: Job 17 finished: collect at StackOverflow.scala:191, took 6.097388 s
Iteration: 17
  * current distance: 49068.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,59) ==>          (450000,62)    distance:        9
               (0,1408) ==>             (0,1553)    distance:    21025
                (0,180) ==>              (0,213)    distance:     1089
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,139) ==>         (150000,153)    distance:      196
           (150000,929) ==>         (150000,970)    distance:     1681
           (300000,376) ==>         (300000,391)    distance:      225
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,67) ==>          (300000,70)    distance:        9
            (50000,667) ==>          (50000,806)    distance:    19321
             (50000,60) ==>           (50000,73)    distance:      169
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,38) ==>          (200000,44)    distance:       36
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,322) ==>         (200000,341)    distance:      361
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,165) ==>         (500000,168)    distance:        9
            (500000,28) ==>          (500000,29)    distance:        1
            (350000,87) ==>          (350000,96)    distance:       81
             (350000,1) ==>           (350000,2)    distance:        1
           (350000,552) ==>         (350000,594)    distance:     1764
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,460) ==>         (100000,510)    distance:     2500
            (100000,50) ==>          (100000,57)    distance:       49
           (400000,513) ==>         (400000,520)    distance:       49
             (400000,2) ==>           (400000,2)    distance:        0
            (400000,95) ==>          (400000,98)    distance:        9
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,56) ==>          (550000,60)    distance:       16
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,672) ==>         (250000,691)    distance:      361
             (250000,3) ==>           (250000,3)    distance:        0
            (250000,60) ==>          (250000,69)    distance:       81
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,48) ==>          (700000,53)    distance:       25
             (700000,8) ==>           (700000,9)    distance:        1
17/12/23 18:53:25 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:53:25 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:53:25 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:53:25 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:53:25 INFO DAGScheduler: Registering RDD 69 (map at StackOverflow.scala:186)
17/12/23 18:53:25 INFO DAGScheduler: Got job 18 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:53:25 INFO DAGScheduler: Final stage: ResultStage 94 (collect at StackOverflow.scala:191)
17/12/23 18:53:25 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 93)
17/12/23 18:53:25 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 93)
17/12/23 18:53:25 INFO DAGScheduler: Submitting ShuffleMapStage 93 (MapPartitionsRDD[69] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:53:25 INFO MemoryStore: Block broadcast_40 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:53:25 INFO MemoryStore: Block broadcast_40_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:53:25 INFO BlockManagerInfo: Added broadcast_40_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:25 INFO SparkContext: Created broadcast 40 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:25 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 93 (MapPartitionsRDD[69] at map at StackOverflow.scala:186)
17/12/23 18:53:25 INFO TaskSchedulerImpl: Adding task set 93.0 with 6 tasks
17/12/23 18:53:25 INFO TaskSetManager: Starting task 0.0 in stage 93.0 (TID 234, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:25 INFO Executor: Running task 0.0 in stage 93.0 (TID 234)
17/12/23 18:53:25 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:53:26 INFO Executor: Finished task 0.0 in stage 93.0 (TID 234). 1325 bytes result sent to driver
17/12/23 18:53:26 INFO TaskSetManager: Starting task 1.0 in stage 93.0 (TID 235, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:26 INFO TaskSetManager: Finished task 0.0 in stage 93.0 (TID 234) in 557 ms on localhost (executor driver) (1/6)
17/12/23 18:53:26 INFO Executor: Running task 1.0 in stage 93.0 (TID 235)
17/12/23 18:53:26 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:53:26 INFO Executor: Finished task 1.0 in stage 93.0 (TID 235). 1415 bytes result sent to driver
17/12/23 18:53:26 INFO TaskSetManager: Starting task 2.0 in stage 93.0 (TID 236, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:26 INFO TaskSetManager: Finished task 1.0 in stage 93.0 (TID 235) in 555 ms on localhost (executor driver) (2/6)
17/12/23 18:53:26 INFO Executor: Running task 2.0 in stage 93.0 (TID 236)
17/12/23 18:53:26 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:53:27 INFO Executor: Finished task 2.0 in stage 93.0 (TID 236). 1325 bytes result sent to driver
17/12/23 18:53:27 INFO TaskSetManager: Starting task 3.0 in stage 93.0 (TID 237, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:27 INFO Executor: Running task 3.0 in stage 93.0 (TID 237)
17/12/23 18:53:27 INFO TaskSetManager: Finished task 2.0 in stage 93.0 (TID 236) in 512 ms on localhost (executor driver) (3/6)
17/12/23 18:53:27 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:53:27 INFO Executor: Finished task 3.0 in stage 93.0 (TID 237). 1238 bytes result sent to driver
17/12/23 18:53:27 INFO TaskSetManager: Starting task 4.0 in stage 93.0 (TID 238, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:27 INFO TaskSetManager: Finished task 3.0 in stage 93.0 (TID 237) in 511 ms on localhost (executor driver) (4/6)
17/12/23 18:53:27 INFO Executor: Running task 4.0 in stage 93.0 (TID 238)
17/12/23 18:53:27 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:53:28 INFO Executor: Finished task 4.0 in stage 93.0 (TID 238). 1325 bytes result sent to driver
17/12/23 18:53:28 INFO TaskSetManager: Starting task 5.0 in stage 93.0 (TID 239, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:28 INFO Executor: Running task 5.0 in stage 93.0 (TID 239)
17/12/23 18:53:28 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:53:28 INFO TaskSetManager: Finished task 4.0 in stage 93.0 (TID 238) in 516 ms on localhost (executor driver) (5/6)
17/12/23 18:53:28 INFO Executor: Finished task 5.0 in stage 93.0 (TID 239). 1415 bytes result sent to driver
17/12/23 18:53:28 INFO TaskSetManager: Finished task 5.0 in stage 93.0 (TID 239) in 506 ms on localhost (executor driver) (6/6)
17/12/23 18:53:28 INFO TaskSchedulerImpl: Removed TaskSet 93.0, whose tasks have all completed, from pool 
17/12/23 18:53:28 INFO DAGScheduler: ShuffleMapStage 93 (map at StackOverflow.scala:186) finished in 3.150 s
17/12/23 18:53:28 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:53:28 INFO DAGScheduler: running: Set()
17/12/23 18:53:28 INFO DAGScheduler: waiting: Set(ResultStage 94)
17/12/23 18:53:28 INFO DAGScheduler: failed: Set()
17/12/23 18:53:28 INFO DAGScheduler: Submitting ResultStage 94 (MapPartitionsRDD[71] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:53:28 INFO MemoryStore: Block broadcast_41 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:53:28 INFO MemoryStore: Block broadcast_41_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:53:28 INFO BlockManagerInfo: Added broadcast_41_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:28 INFO SparkContext: Created broadcast 41 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:28 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 94 (MapPartitionsRDD[71] at mapValues at StackOverflow.scala:190)
17/12/23 18:53:28 INFO TaskSchedulerImpl: Adding task set 94.0 with 6 tasks
17/12/23 18:53:28 INFO TaskSetManager: Starting task 0.0 in stage 94.0 (TID 240, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:53:28 INFO Executor: Running task 0.0 in stage 94.0 (TID 240)
17/12/23 18:53:28 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:28 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:29 INFO Executor: Finished task 0.0 in stage 94.0 (TID 240). 2072 bytes result sent to driver
17/12/23 18:53:29 INFO TaskSetManager: Starting task 1.0 in stage 94.0 (TID 241, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:53:29 INFO TaskSetManager: Finished task 0.0 in stage 94.0 (TID 240) in 496 ms on localhost (executor driver) (1/6)
17/12/23 18:53:29 INFO Executor: Running task 1.0 in stage 94.0 (TID 241)
17/12/23 18:53:29 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:29 INFO ContextCleaner: Cleaned shuffle 20
17/12/23 18:53:29 INFO BlockManagerInfo: Removed broadcast_38_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:29 INFO BlockManagerInfo: Removed broadcast_39_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:29 INFO BlockManagerInfo: Removed broadcast_40_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:29 INFO Executor: Finished task 1.0 in stage 94.0 (TID 241). 2145 bytes result sent to driver
17/12/23 18:53:29 INFO TaskSetManager: Starting task 2.0 in stage 94.0 (TID 242, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:53:29 INFO Executor: Running task 2.0 in stage 94.0 (TID 242)
17/12/23 18:53:29 INFO TaskSetManager: Finished task 1.0 in stage 94.0 (TID 241) in 719 ms on localhost (executor driver) (2/6)
17/12/23 18:53:29 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:29 INFO Executor: Finished task 2.0 in stage 94.0 (TID 242). 1985 bytes result sent to driver
17/12/23 18:53:29 INFO TaskSetManager: Starting task 3.0 in stage 94.0 (TID 243, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:53:29 INFO Executor: Running task 3.0 in stage 94.0 (TID 243)
17/12/23 18:53:29 INFO TaskSetManager: Finished task 2.0 in stage 94.0 (TID 242) in 37 ms on localhost (executor driver) (3/6)
17/12/23 18:53:29 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:30 INFO Executor: Finished task 3.0 in stage 94.0 (TID 243). 2040 bytes result sent to driver
17/12/23 18:53:30 INFO TaskSetManager: Starting task 4.0 in stage 94.0 (TID 244, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:53:30 INFO Executor: Running task 4.0 in stage 94.0 (TID 244)
17/12/23 18:53:30 INFO TaskSetManager: Finished task 3.0 in stage 94.0 (TID 243) in 265 ms on localhost (executor driver) (4/6)
17/12/23 18:53:30 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:30 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:30 INFO Executor: Finished task 4.0 in stage 94.0 (TID 244). 2127 bytes result sent to driver
17/12/23 18:53:30 INFO TaskSetManager: Starting task 5.0 in stage 94.0 (TID 245, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:53:30 INFO Executor: Running task 5.0 in stage 94.0 (TID 245)
17/12/23 18:53:30 INFO TaskSetManager: Finished task 4.0 in stage 94.0 (TID 244) in 383 ms on localhost (executor driver) (5/6)
17/12/23 18:53:30 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:30 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
Iteration: 18
  * current distance: 57569.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,62) ==>          (450000,67)    distance:       25
               (0,1553) ==>             (0,1678)    distance:    15625
                (0,213) ==>              (0,245)    distance:     1024
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,153) ==>         (150000,167)    distance:      196
           (150000,970) ==>        (150000,1017)    distance:     2209
           (300000,391) ==>         (300000,407)    distance:      256
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,70) ==>          (300000,74)    distance:       16
            (50000,806) ==>          (50000,987)    distance:    32761
             (50000,73) ==>           (50000,87)    distance:      196
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,44) ==>          (200000,49)    distance:       25
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,341) ==>         (200000,360)    distance:      361
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,168) ==>         (500000,173)    distance:       25
            (500000,29) ==>          (500000,31)    distance:        4
            (350000,96) ==>         (350000,107)    distance:      121
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,594) ==>         (350000,632)    distance:     1444
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,510) ==>         (100000,554)    distance:     1936
            (100000,57) ==>          (100000,65)    distance:       64
           (400000,520) ==>         (400000,529)    distance:       81
             (400000,2) ==>           (400000,2)    distance:        0
            (400000,98) ==>         (400000,101)    distance:        9
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,60) ==>          (550000,62)    distance:        4
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,691) ==>         (250000,723)    distance:     1024
             (250000,3) ==>           (250000,3)    distance:        0
            (250000,69) ==>          (250000,78)    distance:       81
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,53) ==>          (700000,62)    distance:       81
             (700000,9) ==>          (700000,10)    distance:        1
17/12/23 18:53:31 INFO Executor: Finished task 5.0 in stage 94.0 (TID 245). 2040 bytes result sent to driver
17/12/23 18:53:31 INFO TaskSetManager: Finished task 5.0 in stage 94.0 (TID 245) in 949 ms on localhost (executor driver) (6/6)
17/12/23 18:53:31 INFO TaskSchedulerImpl: Removed TaskSet 94.0, whose tasks have all completed, from pool 
17/12/23 18:53:31 INFO DAGScheduler: ResultStage 94 (collect at StackOverflow.scala:191) finished in 2.847 s
17/12/23 18:53:31 INFO DAGScheduler: Job 18 finished: collect at StackOverflow.scala:191, took 6.010859 s
17/12/23 18:53:31 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:53:31 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:53:31 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:53:31 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:53:31 INFO DAGScheduler: Registering RDD 72 (map at StackOverflow.scala:186)
17/12/23 18:53:31 INFO DAGScheduler: Got job 19 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:53:31 INFO DAGScheduler: Final stage: ResultStage 99 (collect at StackOverflow.scala:191)
17/12/23 18:53:31 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 98)
17/12/23 18:53:31 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 98)
17/12/23 18:53:31 INFO DAGScheduler: Submitting ShuffleMapStage 98 (MapPartitionsRDD[72] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:53:31 INFO MemoryStore: Block broadcast_42 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:53:31 INFO MemoryStore: Block broadcast_42_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:53:31 INFO BlockManagerInfo: Added broadcast_42_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:31 INFO SparkContext: Created broadcast 42 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:31 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 98 (MapPartitionsRDD[72] at map at StackOverflow.scala:186)
17/12/23 18:53:31 INFO TaskSchedulerImpl: Adding task set 98.0 with 6 tasks
17/12/23 18:53:31 INFO TaskSetManager: Starting task 0.0 in stage 98.0 (TID 246, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:31 INFO Executor: Running task 0.0 in stage 98.0 (TID 246)
17/12/23 18:53:31 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:53:32 INFO Executor: Finished task 0.0 in stage 98.0 (TID 246). 1415 bytes result sent to driver
17/12/23 18:53:32 INFO TaskSetManager: Starting task 1.0 in stage 98.0 (TID 247, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:32 INFO TaskSetManager: Finished task 0.0 in stage 98.0 (TID 246) in 529 ms on localhost (executor driver) (1/6)
17/12/23 18:53:32 INFO Executor: Running task 1.0 in stage 98.0 (TID 247)
17/12/23 18:53:32 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:53:32 INFO Executor: Finished task 1.0 in stage 98.0 (TID 247). 1325 bytes result sent to driver
17/12/23 18:53:32 INFO TaskSetManager: Starting task 2.0 in stage 98.0 (TID 248, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:32 INFO Executor: Running task 2.0 in stage 98.0 (TID 248)
17/12/23 18:53:32 INFO TaskSetManager: Finished task 1.0 in stage 98.0 (TID 247) in 526 ms on localhost (executor driver) (2/6)
17/12/23 18:53:32 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:53:33 INFO Executor: Finished task 2.0 in stage 98.0 (TID 248). 1325 bytes result sent to driver
17/12/23 18:53:33 INFO TaskSetManager: Starting task 3.0 in stage 98.0 (TID 249, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:33 INFO TaskSetManager: Finished task 2.0 in stage 98.0 (TID 248) in 514 ms on localhost (executor driver) (3/6)
17/12/23 18:53:33 INFO Executor: Running task 3.0 in stage 98.0 (TID 249)
17/12/23 18:53:33 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:53:33 INFO Executor: Finished task 3.0 in stage 98.0 (TID 249). 1325 bytes result sent to driver
17/12/23 18:53:33 INFO TaskSetManager: Starting task 4.0 in stage 98.0 (TID 250, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:33 INFO TaskSetManager: Finished task 3.0 in stage 98.0 (TID 249) in 527 ms on localhost (executor driver) (4/6)
17/12/23 18:53:33 INFO Executor: Running task 4.0 in stage 98.0 (TID 250)
17/12/23 18:53:33 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:53:34 INFO Executor: Finished task 4.0 in stage 98.0 (TID 250). 1325 bytes result sent to driver
17/12/23 18:53:34 INFO TaskSetManager: Starting task 5.0 in stage 98.0 (TID 251, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:34 INFO TaskSetManager: Finished task 4.0 in stage 98.0 (TID 250) in 554 ms on localhost (executor driver) (5/6)
17/12/23 18:53:34 INFO Executor: Running task 5.0 in stage 98.0 (TID 251)
17/12/23 18:53:34 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:53:34 INFO Executor: Finished task 5.0 in stage 98.0 (TID 251). 1325 bytes result sent to driver
17/12/23 18:53:34 INFO TaskSetManager: Finished task 5.0 in stage 98.0 (TID 251) in 523 ms on localhost (executor driver) (6/6)
17/12/23 18:53:34 INFO TaskSchedulerImpl: Removed TaskSet 98.0, whose tasks have all completed, from pool 
17/12/23 18:53:34 INFO DAGScheduler: ShuffleMapStage 98 (map at StackOverflow.scala:186) finished in 3.169 s
17/12/23 18:53:34 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:53:34 INFO DAGScheduler: running: Set()
17/12/23 18:53:34 INFO DAGScheduler: waiting: Set(ResultStage 99)
17/12/23 18:53:34 INFO DAGScheduler: failed: Set()
17/12/23 18:53:34 INFO DAGScheduler: Submitting ResultStage 99 (MapPartitionsRDD[74] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:53:34 INFO MemoryStore: Block broadcast_43 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:53:34 INFO MemoryStore: Block broadcast_43_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:53:34 INFO BlockManagerInfo: Added broadcast_43_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:34 INFO SparkContext: Created broadcast 43 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:34 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 99 (MapPartitionsRDD[74] at mapValues at StackOverflow.scala:190)
17/12/23 18:53:34 INFO TaskSchedulerImpl: Adding task set 99.0 with 6 tasks
17/12/23 18:53:34 INFO TaskSetManager: Starting task 0.0 in stage 99.0 (TID 252, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:53:34 INFO Executor: Running task 0.0 in stage 99.0 (TID 252)
17/12/23 18:53:34 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:34 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:35 INFO Executor: Finished task 0.0 in stage 99.0 (TID 252). 2072 bytes result sent to driver
17/12/23 18:53:35 INFO TaskSetManager: Starting task 1.0 in stage 99.0 (TID 253, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:53:35 INFO Executor: Running task 1.0 in stage 99.0 (TID 253)
17/12/23 18:53:35 INFO TaskSetManager: Finished task 0.0 in stage 99.0 (TID 252) in 462 ms on localhost (executor driver) (1/6)
17/12/23 18:53:35 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:35 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:35 INFO Executor: Finished task 1.0 in stage 99.0 (TID 253). 2162 bytes result sent to driver
17/12/23 18:53:35 INFO TaskSetManager: Starting task 2.0 in stage 99.0 (TID 254, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:53:35 INFO Executor: Running task 2.0 in stage 99.0 (TID 254)
17/12/23 18:53:35 INFO TaskSetManager: Finished task 1.0 in stage 99.0 (TID 253) in 708 ms on localhost (executor driver) (2/6)
17/12/23 18:53:35 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:35 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:35 INFO Executor: Finished task 2.0 in stage 99.0 (TID 254). 2072 bytes result sent to driver
17/12/23 18:53:36 INFO TaskSetManager: Starting task 3.0 in stage 99.0 (TID 255, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:53:36 INFO TaskSetManager: Finished task 2.0 in stage 99.0 (TID 254) in 42 ms on localhost (executor driver) (3/6)
17/12/23 18:53:36 INFO Executor: Running task 3.0 in stage 99.0 (TID 255)
17/12/23 18:53:36 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:36 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:36 INFO ContextCleaner: Cleaned shuffle 21
17/12/23 18:53:36 INFO BlockManagerInfo: Removed broadcast_41_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:36 INFO BlockManagerInfo: Removed broadcast_42_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:36 INFO Executor: Finished task 3.0 in stage 99.0 (TID 255). 2026 bytes result sent to driver
17/12/23 18:53:36 INFO TaskSetManager: Starting task 4.0 in stage 99.0 (TID 256, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:53:36 INFO TaskSetManager: Finished task 3.0 in stage 99.0 (TID 255) in 309 ms on localhost (executor driver) (4/6)
17/12/23 18:53:36 INFO Executor: Running task 4.0 in stage 99.0 (TID 256)
17/12/23 18:53:36 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:36 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:36 INFO Executor: Finished task 4.0 in stage 99.0 (TID 256). 2040 bytes result sent to driver
17/12/23 18:53:36 INFO TaskSetManager: Starting task 5.0 in stage 99.0 (TID 257, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:53:36 INFO Executor: Running task 5.0 in stage 99.0 (TID 257)
17/12/23 18:53:36 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:36 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:36 INFO TaskSetManager: Finished task 4.0 in stage 99.0 (TID 256) in 420 ms on localhost (executor driver) (5/6)
Iteration: 19
  * current distance: 58999.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
17/12/23 18:53:37 INFO Executor: Finished task 5.0 in stage 99.0 (TID 257). 1953 bytes result sent to driver
            (450000,67) ==>          (450000,70)    distance:        9
17/12/23 18:53:37 INFO TaskSetManager: Finished task 5.0 in stage 99.0 (TID 257) in 886 ms on localhost (executor driver) (6/6)
               (0,1678) ==>             (0,1777)    distance:     9801
                (0,245) ==>              (0,273)    distance:      784
17/12/23 18:53:37 INFO TaskSchedulerImpl: Removed TaskSet 99.0, whose tasks have all completed, from pool 
                  (0,2) ==>                (0,2)    distance:        0
17/12/23 18:53:37 INFO DAGScheduler: ResultStage 99 (collect at StackOverflow.scala:191) finished in 2.817 s
             (600000,7) ==>           (600000,7)    distance:        0
17/12/23 18:53:37 INFO DAGScheduler: Job 19 finished: collect at StackOverflow.scala:191, took 6.000523 s
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,167) ==>         (150000,181)    distance:      196
          (150000,1017) ==>        (150000,1083)    distance:     4356
           (300000,407) ==>         (300000,415)    distance:       64
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,74) ==>          (300000,77)    distance:        9
            (50000,987) ==>         (50000,1186)    distance:    39601
             (50000,87) ==>          (50000,102)    distance:      225
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,49) ==>          (200000,55)    distance:       36
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,360) ==>         (200000,376)    distance:      256
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,173) ==>         (500000,176)    distance:        9
            (500000,31) ==>          (500000,32)    distance:        1
           (350000,107) ==>         (350000,118)    distance:      121
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,632) ==>         (350000,654)    distance:      484
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,554) ==>         (100000,597)    distance:     1849
            (100000,65) ==>          (100000,73)    distance:       64
           (400000,529) ==>         (400000,536)    distance:       49
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,101) ==>         (400000,104)    distance:        9
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,62) ==>          (550000,63)    distance:        1
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,723) ==>         (250000,754)    distance:      961
             (250000,3) ==>           (250000,3)    distance:        0
            (250000,78) ==>          (250000,86)    distance:       64
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,62) ==>          (700000,69)    distance:       49
            (700000,10) ==>          (700000,11)    distance:        1
17/12/23 18:53:37 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:53:37 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:53:37 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:53:37 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:53:37 INFO DAGScheduler: Registering RDD 75 (map at StackOverflow.scala:186)
17/12/23 18:53:37 INFO DAGScheduler: Got job 20 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:53:37 INFO DAGScheduler: Final stage: ResultStage 104 (collect at StackOverflow.scala:191)
17/12/23 18:53:37 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 103)
17/12/23 18:53:37 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 103)
17/12/23 18:53:37 INFO DAGScheduler: Submitting ShuffleMapStage 103 (MapPartitionsRDD[75] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:53:37 INFO MemoryStore: Block broadcast_44 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:53:37 INFO MemoryStore: Block broadcast_44_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:53:37 INFO BlockManagerInfo: Added broadcast_44_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:37 INFO SparkContext: Created broadcast 44 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:37 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 103 (MapPartitionsRDD[75] at map at StackOverflow.scala:186)
17/12/23 18:53:37 INFO TaskSchedulerImpl: Adding task set 103.0 with 6 tasks
17/12/23 18:53:37 INFO TaskSetManager: Starting task 0.0 in stage 103.0 (TID 258, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:37 INFO Executor: Running task 0.0 in stage 103.0 (TID 258)
17/12/23 18:53:37 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:53:38 INFO Executor: Finished task 0.0 in stage 103.0 (TID 258). 1238 bytes result sent to driver
17/12/23 18:53:38 INFO TaskSetManager: Starting task 1.0 in stage 103.0 (TID 259, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:38 INFO Executor: Running task 1.0 in stage 103.0 (TID 259)
17/12/23 18:53:38 INFO TaskSetManager: Finished task 0.0 in stage 103.0 (TID 258) in 547 ms on localhost (executor driver) (1/6)
17/12/23 18:53:38 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:53:38 INFO Executor: Finished task 1.0 in stage 103.0 (TID 259). 1325 bytes result sent to driver
17/12/23 18:53:38 INFO TaskSetManager: Starting task 2.0 in stage 103.0 (TID 260, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:38 INFO Executor: Running task 2.0 in stage 103.0 (TID 260)
17/12/23 18:53:38 INFO TaskSetManager: Finished task 1.0 in stage 103.0 (TID 259) in 510 ms on localhost (executor driver) (2/6)
17/12/23 18:53:38 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:53:39 INFO Executor: Finished task 2.0 in stage 103.0 (TID 260). 1325 bytes result sent to driver
17/12/23 18:53:39 INFO TaskSetManager: Starting task 3.0 in stage 103.0 (TID 261, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:39 INFO TaskSetManager: Finished task 2.0 in stage 103.0 (TID 260) in 506 ms on localhost (executor driver) (3/6)
17/12/23 18:53:39 INFO Executor: Running task 3.0 in stage 103.0 (TID 261)
17/12/23 18:53:39 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:53:39 INFO Executor: Finished task 3.0 in stage 103.0 (TID 261). 1238 bytes result sent to driver
17/12/23 18:53:39 INFO TaskSetManager: Starting task 4.0 in stage 103.0 (TID 262, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:39 INFO Executor: Running task 4.0 in stage 103.0 (TID 262)
17/12/23 18:53:39 INFO TaskSetManager: Finished task 3.0 in stage 103.0 (TID 261) in 525 ms on localhost (executor driver) (4/6)
17/12/23 18:53:39 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:53:40 INFO Executor: Finished task 4.0 in stage 103.0 (TID 262). 1325 bytes result sent to driver
17/12/23 18:53:40 INFO TaskSetManager: Starting task 5.0 in stage 103.0 (TID 263, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:40 INFO Executor: Running task 5.0 in stage 103.0 (TID 263)
17/12/23 18:53:40 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:53:40 INFO TaskSetManager: Finished task 4.0 in stage 103.0 (TID 262) in 510 ms on localhost (executor driver) (5/6)
17/12/23 18:53:40 INFO Executor: Finished task 5.0 in stage 103.0 (TID 263). 1325 bytes result sent to driver
17/12/23 18:53:40 INFO TaskSetManager: Finished task 5.0 in stage 103.0 (TID 263) in 502 ms on localhost (executor driver) (6/6)
17/12/23 18:53:40 INFO TaskSchedulerImpl: Removed TaskSet 103.0, whose tasks have all completed, from pool 
17/12/23 18:53:40 INFO DAGScheduler: ShuffleMapStage 103 (map at StackOverflow.scala:186) finished in 3.089 s
17/12/23 18:53:40 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:53:40 INFO DAGScheduler: running: Set()
17/12/23 18:53:40 INFO DAGScheduler: waiting: Set(ResultStage 104)
17/12/23 18:53:40 INFO DAGScheduler: failed: Set()
17/12/23 18:53:40 INFO DAGScheduler: Submitting ResultStage 104 (MapPartitionsRDD[77] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:53:40 INFO MemoryStore: Block broadcast_45 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:53:40 INFO MemoryStore: Block broadcast_45_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:53:40 INFO BlockManagerInfo: Added broadcast_45_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:40 INFO SparkContext: Created broadcast 45 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:40 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 104 (MapPartitionsRDD[77] at mapValues at StackOverflow.scala:190)
17/12/23 18:53:40 INFO TaskSchedulerImpl: Adding task set 104.0 with 6 tasks
17/12/23 18:53:40 INFO TaskSetManager: Starting task 0.0 in stage 104.0 (TID 264, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:53:40 INFO Executor: Running task 0.0 in stage 104.0 (TID 264)
17/12/23 18:53:40 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:40 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:41 INFO Executor: Finished task 0.0 in stage 104.0 (TID 264). 1985 bytes result sent to driver
17/12/23 18:53:41 INFO TaskSetManager: Starting task 1.0 in stage 104.0 (TID 265, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:53:41 INFO TaskSetManager: Finished task 0.0 in stage 104.0 (TID 264) in 503 ms on localhost (executor driver) (1/6)
17/12/23 18:53:41 INFO Executor: Running task 1.0 in stage 104.0 (TID 265)
17/12/23 18:53:41 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:41 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:41 INFO Executor: Finished task 1.0 in stage 104.0 (TID 265). 2072 bytes result sent to driver
17/12/23 18:53:41 INFO TaskSetManager: Starting task 2.0 in stage 104.0 (TID 266, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:53:41 INFO Executor: Running task 2.0 in stage 104.0 (TID 266)
17/12/23 18:53:41 INFO TaskSetManager: Finished task 1.0 in stage 104.0 (TID 265) in 750 ms on localhost (executor driver) (2/6)
17/12/23 18:53:41 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:41 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:42 INFO Executor: Finished task 2.0 in stage 104.0 (TID 266). 2162 bytes result sent to driver
17/12/23 18:53:42 INFO TaskSetManager: Starting task 3.0 in stage 104.0 (TID 267, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:53:42 INFO Executor: Running task 3.0 in stage 104.0 (TID 267)
17/12/23 18:53:42 INFO TaskSetManager: Finished task 2.0 in stage 104.0 (TID 266) in 33 ms on localhost (executor driver) (3/6)
17/12/23 18:53:42 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:42 INFO Executor: Finished task 3.0 in stage 104.0 (TID 267). 2130 bytes result sent to driver
17/12/23 18:53:42 INFO TaskSetManager: Starting task 4.0 in stage 104.0 (TID 268, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:53:42 INFO Executor: Running task 4.0 in stage 104.0 (TID 268)
17/12/23 18:53:42 INFO TaskSetManager: Finished task 3.0 in stage 104.0 (TID 267) in 282 ms on localhost (executor driver) (4/6)
17/12/23 18:53:42 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:42 INFO Executor: Finished task 4.0 in stage 104.0 (TID 268). 2040 bytes result sent to driver
17/12/23 18:53:42 INFO TaskSetManager: Starting task 5.0 in stage 104.0 (TID 269, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:53:42 INFO Executor: Running task 5.0 in stage 104.0 (TID 269)
17/12/23 18:53:42 INFO TaskSetManager: Finished task 4.0 in stage 104.0 (TID 268) in 383 ms on localhost (executor driver) (5/6)
17/12/23 18:53:42 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:43 INFO ContextCleaner: Cleaned shuffle 22
17/12/23 18:53:43 INFO BlockManagerInfo: Removed broadcast_43_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:43 INFO BlockManagerInfo: Removed broadcast_44_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:43 INFO Executor: Finished task 5.0 in stage 104.0 (TID 269). 2113 bytes result sent to driver
17/12/23 18:53:43 INFO TaskSetManager: Finished task 5.0 in stage 104.0 (TID 269) in 978 ms on localhost (executor driver) (6/6)
17/12/23 18:53:43 INFO TaskSchedulerImpl: Removed TaskSet 104.0, whose tasks have all completed, from pool 
17/12/23 18:53:43 INFO DAGScheduler: ResultStage 104 (collect at StackOverflow.scala:191) finished in 2.926 s
17/12/23 18:53:43 INFO DAGScheduler: Job 20 finished: collect at StackOverflow.scala:191, took 6.026355 s
Iteration: 20
  * current distance: 156680.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,70) ==>          (450000,74)    distance:       16
               (0,1777) ==>             (0,1847)    distance:     4900
                (0,273) ==>              (0,298)    distance:      625
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,181) ==>         (150000,193)    distance:      144
          (150000,1083) ==>        (150000,1153)    distance:     4900
           (300000,415) ==>         (300000,417)    distance:        4
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,77) ==>          (300000,79)    distance:        4
           (50000,1186) ==>         (50000,1562)    distance:   141376
            (50000,102) ==>          (50000,117)    distance:      225
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,55) ==>          (200000,60)    distance:       25
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,376) ==>         (200000,393)    distance:      289
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,118) ==>         (350000,127)    distance:       81
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,654) ==>         (350000,672)    distance:      324
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,597) ==>         (100000,646)    distance:     2401
            (100000,73) ==>          (100000,81)    distance:       64
           (400000,536) ==>         (400000,544)    distance:       64
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,104) ==>         (400000,107)    distance:        9
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,63) ==>          (550000,65)    distance:        4
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,754) ==>         (250000,788)    distance:     1156
             (250000,3) ==>           (250000,3)    distance:        0
            (250000,86) ==>          (250000,94)    distance:       64
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,69) ==>          (700000,71)    distance:        4
            (700000,11) ==>          (700000,12)    distance:        1
17/12/23 18:53:43 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:53:43 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:53:43 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:53:43 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:53:43 INFO DAGScheduler: Registering RDD 78 (map at StackOverflow.scala:186)
17/12/23 18:53:43 INFO DAGScheduler: Got job 21 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:53:43 INFO DAGScheduler: Final stage: ResultStage 109 (collect at StackOverflow.scala:191)
17/12/23 18:53:43 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 108)
17/12/23 18:53:43 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 108)
17/12/23 18:53:43 INFO DAGScheduler: Submitting ShuffleMapStage 108 (MapPartitionsRDD[78] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:53:43 INFO MemoryStore: Block broadcast_46 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:53:43 INFO MemoryStore: Block broadcast_46_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:53:43 INFO BlockManagerInfo: Added broadcast_46_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:43 INFO SparkContext: Created broadcast 46 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:43 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 108 (MapPartitionsRDD[78] at map at StackOverflow.scala:186)
17/12/23 18:53:43 INFO TaskSchedulerImpl: Adding task set 108.0 with 6 tasks
17/12/23 18:53:43 INFO TaskSetManager: Starting task 0.0 in stage 108.0 (TID 270, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:43 INFO Executor: Running task 0.0 in stage 108.0 (TID 270)
17/12/23 18:53:43 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:53:44 INFO Executor: Finished task 0.0 in stage 108.0 (TID 270). 1325 bytes result sent to driver
17/12/23 18:53:44 INFO TaskSetManager: Starting task 1.0 in stage 108.0 (TID 271, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:44 INFO Executor: Running task 1.0 in stage 108.0 (TID 271)
17/12/23 18:53:44 INFO TaskSetManager: Finished task 0.0 in stage 108.0 (TID 270) in 503 ms on localhost (executor driver) (1/6)
17/12/23 18:53:44 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:53:44 INFO Executor: Finished task 1.0 in stage 108.0 (TID 271). 1325 bytes result sent to driver
17/12/23 18:53:44 INFO TaskSetManager: Starting task 2.0 in stage 108.0 (TID 272, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:44 INFO TaskSetManager: Finished task 1.0 in stage 108.0 (TID 271) in 515 ms on localhost (executor driver) (2/6)
17/12/23 18:53:44 INFO Executor: Running task 2.0 in stage 108.0 (TID 272)
17/12/23 18:53:44 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:53:45 INFO Executor: Finished task 2.0 in stage 108.0 (TID 272). 1325 bytes result sent to driver
17/12/23 18:53:45 INFO TaskSetManager: Starting task 3.0 in stage 108.0 (TID 273, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:45 INFO TaskSetManager: Finished task 2.0 in stage 108.0 (TID 272) in 582 ms on localhost (executor driver) (3/6)
17/12/23 18:53:45 INFO Executor: Running task 3.0 in stage 108.0 (TID 273)
17/12/23 18:53:45 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:53:45 INFO Executor: Finished task 3.0 in stage 108.0 (TID 273). 1415 bytes result sent to driver
17/12/23 18:53:45 INFO TaskSetManager: Starting task 4.0 in stage 108.0 (TID 274, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:45 INFO Executor: Running task 4.0 in stage 108.0 (TID 274)
17/12/23 18:53:45 INFO TaskSetManager: Finished task 3.0 in stage 108.0 (TID 273) in 518 ms on localhost (executor driver) (4/6)
17/12/23 18:53:45 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:53:46 INFO Executor: Finished task 4.0 in stage 108.0 (TID 274). 1238 bytes result sent to driver
17/12/23 18:53:46 INFO TaskSetManager: Starting task 5.0 in stage 108.0 (TID 275, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:46 INFO Executor: Running task 5.0 in stage 108.0 (TID 275)
17/12/23 18:53:46 INFO TaskSetManager: Finished task 4.0 in stage 108.0 (TID 274) in 495 ms on localhost (executor driver) (5/6)
17/12/23 18:53:46 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:53:46 INFO Executor: Finished task 5.0 in stage 108.0 (TID 275). 1238 bytes result sent to driver
17/12/23 18:53:46 INFO TaskSetManager: Finished task 5.0 in stage 108.0 (TID 275) in 523 ms on localhost (executor driver) (6/6)
17/12/23 18:53:46 INFO TaskSchedulerImpl: Removed TaskSet 108.0, whose tasks have all completed, from pool 
17/12/23 18:53:46 INFO DAGScheduler: ShuffleMapStage 108 (map at StackOverflow.scala:186) finished in 3.130 s
17/12/23 18:53:46 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:53:46 INFO DAGScheduler: running: Set()
17/12/23 18:53:46 INFO DAGScheduler: waiting: Set(ResultStage 109)
17/12/23 18:53:46 INFO DAGScheduler: failed: Set()
17/12/23 18:53:46 INFO DAGScheduler: Submitting ResultStage 109 (MapPartitionsRDD[80] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:53:46 INFO MemoryStore: Block broadcast_47 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:53:46 INFO MemoryStore: Block broadcast_47_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:53:46 INFO BlockManagerInfo: Added broadcast_47_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:46 INFO SparkContext: Created broadcast 47 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:46 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 109 (MapPartitionsRDD[80] at mapValues at StackOverflow.scala:190)
17/12/23 18:53:46 INFO TaskSchedulerImpl: Adding task set 109.0 with 6 tasks
17/12/23 18:53:46 INFO TaskSetManager: Starting task 0.0 in stage 109.0 (TID 276, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:53:46 INFO Executor: Running task 0.0 in stage 109.0 (TID 276)
17/12/23 18:53:46 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:46 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:47 INFO Executor: Finished task 0.0 in stage 109.0 (TID 276). 2072 bytes result sent to driver
17/12/23 18:53:47 INFO TaskSetManager: Starting task 1.0 in stage 109.0 (TID 277, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:53:47 INFO Executor: Running task 1.0 in stage 109.0 (TID 277)
17/12/23 18:53:47 INFO TaskSetManager: Finished task 0.0 in stage 109.0 (TID 276) in 507 ms on localhost (executor driver) (1/6)
17/12/23 18:53:47 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:47 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:53:47 INFO Executor: Finished task 1.0 in stage 109.0 (TID 277). 1985 bytes result sent to driver
17/12/23 18:53:47 INFO TaskSetManager: Starting task 2.0 in stage 109.0 (TID 278, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:53:47 INFO Executor: Running task 2.0 in stage 109.0 (TID 278)
17/12/23 18:53:47 INFO TaskSetManager: Finished task 1.0 in stage 109.0 (TID 277) in 655 ms on localhost (executor driver) (2/6)
17/12/23 18:53:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:48 INFO Executor: Finished task 2.0 in stage 109.0 (TID 278). 2162 bytes result sent to driver
17/12/23 18:53:48 INFO TaskSetManager: Starting task 3.0 in stage 109.0 (TID 279, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:53:48 INFO Executor: Running task 3.0 in stage 109.0 (TID 279)
17/12/23 18:53:48 INFO TaskSetManager: Finished task 2.0 in stage 109.0 (TID 278) in 37 ms on localhost (executor driver) (3/6)
17/12/23 18:53:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
17/12/23 18:53:48 INFO Executor: Finished task 3.0 in stage 109.0 (TID 279). 1953 bytes result sent to driver
17/12/23 18:53:48 INFO TaskSetManager: Starting task 4.0 in stage 109.0 (TID 280, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:53:48 INFO TaskSetManager: Finished task 3.0 in stage 109.0 (TID 279) in 255 ms on localhost (executor driver) (4/6)
17/12/23 18:53:48 INFO Executor: Running task 4.0 in stage 109.0 (TID 280)
17/12/23 18:53:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:48 INFO Executor: Finished task 4.0 in stage 109.0 (TID 280). 1953 bytes result sent to driver
17/12/23 18:53:48 INFO TaskSetManager: Starting task 5.0 in stage 109.0 (TID 281, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:53:48 INFO Executor: Running task 5.0 in stage 109.0 (TID 281)
17/12/23 18:53:48 INFO TaskSetManager: Finished task 4.0 in stage 109.0 (TID 280) in 355 ms on localhost (executor driver) (5/6)
17/12/23 18:53:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:49 INFO Executor: Finished task 5.0 in stage 109.0 (TID 281). 1953 bytes result sent to driver
17/12/23 18:53:49 INFO TaskSetManager: Finished task 5.0 in stage 109.0 (TID 281) in 918 ms on localhost (executor driver) (6/6)
17/12/23 18:53:49 INFO TaskSchedulerImpl: Removed TaskSet 109.0, whose tasks have all completed, from pool 
17/12/23 18:53:49 INFO DAGScheduler: ResultStage 109 (collect at StackOverflow.scala:191) finished in 2.725 s
17/12/23 18:53:49 INFO DAGScheduler: Job 21 finished: collect at StackOverflow.scala:191, took 5.871700 s
Iteration: 21
  * current distance: 162772.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,74) ==>          (450000,77)    distance:        9
               (0,1847) ==>             (0,1877)    distance:      900
                (0,298) ==>              (0,318)    distance:      400
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,193) ==>         (150000,204)    distance:      121
          (150000,1153) ==>        (150000,1222)    distance:     4761
           (300000,417) ==>         (300000,423)    distance:       36
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,79) ==>          (300000,80)    distance:        1
           (50000,1562) ==>         (50000,1956)    distance:   155236
            (50000,117) ==>          (50000,135)    distance:      324
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,60) ==>          (200000,64)    distance:       16
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,393) ==>         (200000,409)    distance:      256
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,127) ==>         (350000,135)    distance:       64
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,672) ==>         (350000,677)    distance:       25
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,646) ==>         (100000,662)    distance:      256
            (100000,81) ==>          (100000,88)    distance:       49
           (400000,544) ==>         (400000,552)    distance:       64
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,107) ==>         (400000,109)    distance:        4
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,65) ==>          (550000,66)    distance:        1
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,788) ==>         (250000,802)    distance:      196
             (250000,3) ==>           (250000,3)    distance:        0
            (250000,94) ==>         (250000,101)    distance:       49
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,71) ==>          (700000,73)    distance:        4
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:53:49 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:53:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:53:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:53:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:53:49 INFO DAGScheduler: Registering RDD 81 (map at StackOverflow.scala:186)
17/12/23 18:53:49 INFO DAGScheduler: Got job 22 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:53:49 INFO DAGScheduler: Final stage: ResultStage 114 (collect at StackOverflow.scala:191)
17/12/23 18:53:49 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 113)
17/12/23 18:53:49 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 113)
17/12/23 18:53:49 INFO DAGScheduler: Submitting ShuffleMapStage 113 (MapPartitionsRDD[81] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:53:49 INFO MemoryStore: Block broadcast_48 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:53:49 INFO MemoryStore: Block broadcast_48_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:53:49 INFO BlockManagerInfo: Added broadcast_48_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:49 INFO SparkContext: Created broadcast 48 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:49 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 113 (MapPartitionsRDD[81] at map at StackOverflow.scala:186)
17/12/23 18:53:49 INFO TaskSchedulerImpl: Adding task set 113.0 with 6 tasks
17/12/23 18:53:49 INFO TaskSetManager: Starting task 0.0 in stage 113.0 (TID 282, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:49 INFO Executor: Running task 0.0 in stage 113.0 (TID 282)
17/12/23 18:53:49 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:53:49 INFO ContextCleaner: Cleaned shuffle 23
17/12/23 18:53:49 INFO BlockManagerInfo: Removed broadcast_45_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:49 INFO ContextCleaner: Cleaned shuffle 24
17/12/23 18:53:49 INFO BlockManagerInfo: Removed broadcast_46_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:49 INFO BlockManagerInfo: Removed broadcast_47_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:50 INFO Executor: Finished task 0.0 in stage 113.0 (TID 282). 1488 bytes result sent to driver
17/12/23 18:53:50 INFO TaskSetManager: Starting task 1.0 in stage 113.0 (TID 283, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:50 INFO TaskSetManager: Finished task 0.0 in stage 113.0 (TID 282) in 532 ms on localhost (executor driver) (1/6)
17/12/23 18:53:50 INFO Executor: Running task 1.0 in stage 113.0 (TID 283)
17/12/23 18:53:50 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:53:50 INFO Executor: Finished task 1.0 in stage 113.0 (TID 283). 1238 bytes result sent to driver
17/12/23 18:53:50 INFO TaskSetManager: Starting task 2.0 in stage 113.0 (TID 284, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:50 INFO Executor: Running task 2.0 in stage 113.0 (TID 284)
17/12/23 18:53:50 INFO TaskSetManager: Finished task 1.0 in stage 113.0 (TID 283) in 517 ms on localhost (executor driver) (2/6)
17/12/23 18:53:50 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:53:51 INFO Executor: Finished task 2.0 in stage 113.0 (TID 284). 1238 bytes result sent to driver
17/12/23 18:53:51 INFO TaskSetManager: Starting task 3.0 in stage 113.0 (TID 285, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:51 INFO Executor: Running task 3.0 in stage 113.0 (TID 285)
17/12/23 18:53:51 INFO TaskSetManager: Finished task 2.0 in stage 113.0 (TID 284) in 503 ms on localhost (executor driver) (3/6)
17/12/23 18:53:51 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:53:51 INFO Executor: Finished task 3.0 in stage 113.0 (TID 285). 1238 bytes result sent to driver
17/12/23 18:53:51 INFO TaskSetManager: Starting task 4.0 in stage 113.0 (TID 286, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:51 INFO Executor: Running task 4.0 in stage 113.0 (TID 286)
17/12/23 18:53:51 INFO TaskSetManager: Finished task 3.0 in stage 113.0 (TID 285) in 525 ms on localhost (executor driver) (4/6)
17/12/23 18:53:51 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:53:52 INFO Executor: Finished task 4.0 in stage 113.0 (TID 286). 1238 bytes result sent to driver
17/12/23 18:53:52 INFO TaskSetManager: Starting task 5.0 in stage 113.0 (TID 287, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:52 INFO TaskSetManager: Finished task 4.0 in stage 113.0 (TID 286) in 500 ms on localhost (executor driver) (5/6)
17/12/23 18:53:52 INFO Executor: Running task 5.0 in stage 113.0 (TID 287)
17/12/23 18:53:52 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:53:52 INFO Executor: Finished task 5.0 in stage 113.0 (TID 287). 1238 bytes result sent to driver
17/12/23 18:53:52 INFO TaskSetManager: Finished task 5.0 in stage 113.0 (TID 287) in 503 ms on localhost (executor driver) (6/6)
17/12/23 18:53:52 INFO TaskSchedulerImpl: Removed TaskSet 113.0, whose tasks have all completed, from pool 
17/12/23 18:53:52 INFO DAGScheduler: ShuffleMapStage 113 (map at StackOverflow.scala:186) finished in 3.071 s
17/12/23 18:53:52 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:53:52 INFO DAGScheduler: running: Set()
17/12/23 18:53:52 INFO DAGScheduler: waiting: Set(ResultStage 114)
17/12/23 18:53:52 INFO DAGScheduler: failed: Set()
17/12/23 18:53:52 INFO DAGScheduler: Submitting ResultStage 114 (MapPartitionsRDD[83] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:53:52 INFO MemoryStore: Block broadcast_49 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:53:52 INFO MemoryStore: Block broadcast_49_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:53:52 INFO BlockManagerInfo: Added broadcast_49_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:52 INFO SparkContext: Created broadcast 49 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:52 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 114 (MapPartitionsRDD[83] at mapValues at StackOverflow.scala:190)
17/12/23 18:53:52 INFO TaskSchedulerImpl: Adding task set 114.0 with 6 tasks
17/12/23 18:53:52 INFO TaskSetManager: Starting task 0.0 in stage 114.0 (TID 288, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:53:52 INFO Executor: Running task 0.0 in stage 114.0 (TID 288)
17/12/23 18:53:52 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:52 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:53 INFO Executor: Finished task 0.0 in stage 114.0 (TID 288). 1985 bytes result sent to driver
17/12/23 18:53:53 INFO TaskSetManager: Starting task 1.0 in stage 114.0 (TID 289, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:53:53 INFO Executor: Running task 1.0 in stage 114.0 (TID 289)
17/12/23 18:53:53 INFO TaskSetManager: Finished task 0.0 in stage 114.0 (TID 288) in 485 ms on localhost (executor driver) (1/6)
17/12/23 18:53:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:53 INFO Executor: Finished task 1.0 in stage 114.0 (TID 289). 1985 bytes result sent to driver
17/12/23 18:53:53 INFO TaskSetManager: Starting task 2.0 in stage 114.0 (TID 290, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:53:53 INFO TaskSetManager: Finished task 1.0 in stage 114.0 (TID 289) in 757 ms on localhost (executor driver) (2/6)
17/12/23 18:53:53 INFO Executor: Running task 2.0 in stage 114.0 (TID 290)
17/12/23 18:53:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:53 INFO Executor: Finished task 2.0 in stage 114.0 (TID 290). 2072 bytes result sent to driver
17/12/23 18:53:53 INFO TaskSetManager: Starting task 3.0 in stage 114.0 (TID 291, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:53:53 INFO Executor: Running task 3.0 in stage 114.0 (TID 291)
17/12/23 18:53:53 INFO TaskSetManager: Finished task 2.0 in stage 114.0 (TID 290) in 37 ms on localhost (executor driver) (3/6)
17/12/23 18:53:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:54 INFO Executor: Finished task 3.0 in stage 114.0 (TID 291). 2040 bytes result sent to driver
17/12/23 18:53:54 INFO TaskSetManager: Starting task 4.0 in stage 114.0 (TID 292, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:53:54 INFO Executor: Running task 4.0 in stage 114.0 (TID 292)
17/12/23 18:53:54 INFO TaskSetManager: Finished task 3.0 in stage 114.0 (TID 291) in 238 ms on localhost (executor driver) (4/6)
17/12/23 18:53:54 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:54 INFO Executor: Finished task 4.0 in stage 114.0 (TID 292). 2130 bytes result sent to driver
17/12/23 18:53:54 INFO TaskSetManager: Starting task 5.0 in stage 114.0 (TID 293, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:53:54 INFO Executor: Running task 5.0 in stage 114.0 (TID 293)
17/12/23 18:53:54 INFO TaskSetManager: Finished task 4.0 in stage 114.0 (TID 292) in 398 ms on localhost (executor driver) (5/6)
17/12/23 18:53:54 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:55 INFO Executor: Finished task 5.0 in stage 114.0 (TID 293). 2040 bytes result sent to driver
17/12/23 18:53:55 INFO TaskSetManager: Finished task 5.0 in stage 114.0 (TID 293) in 945 ms on localhost (executor driver) (6/6)
17/12/23 18:53:55 INFO TaskSchedulerImpl: Removed TaskSet 114.0, whose tasks have all completed, from pool 
17/12/23 18:53:55 INFO DAGScheduler: ResultStage 114 (collect at StackOverflow.scala:191) finished in 2.856 s
17/12/23 18:53:55 INFO DAGScheduler: Job 22 finished: collect at StackOverflow.scala:191, took 5.935480 s
Iteration: 22
  * current distance: 250005.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,77) ==>          (450000,78)    distance:        1
               (0,1877) ==>             (0,1924)    distance:     2209
                (0,318) ==>              (0,335)    distance:      289
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,204) ==>         (150000,213)    distance:       81
          (150000,1222) ==>        (150000,1289)    distance:     4489
           (300000,423) ==>         (300000,444)    distance:      441
             (300000,2) ==>           (300000,2)    distance:        0
            (300000,80) ==>          (300000,83)    distance:        9
           (50000,1956) ==>         (50000,2446)    distance:   240100
            (50000,135) ==>          (50000,154)    distance:      361
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,64) ==>          (200000,68)    distance:       16
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,409) ==>         (200000,422)    distance:      169
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,135) ==>         (350000,141)    distance:       36
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,677) ==>         (350000,677)    distance:        0
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,662) ==>         (100000,687)    distance:      625
            (100000,88) ==>          (100000,94)    distance:       36
           (400000,552) ==>         (400000,554)    distance:        4
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,109) ==>         (400000,110)    distance:        1
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,802) ==>         (250000,835)    distance:     1089
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,101) ==>         (250000,108)    distance:       49
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:53:55 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:53:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:53:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:53:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:53:55 INFO DAGScheduler: Registering RDD 84 (map at StackOverflow.scala:186)
17/12/23 18:53:55 INFO DAGScheduler: Got job 23 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:53:55 INFO DAGScheduler: Final stage: ResultStage 119 (collect at StackOverflow.scala:191)
17/12/23 18:53:55 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 118)
17/12/23 18:53:55 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 118)
17/12/23 18:53:55 INFO DAGScheduler: Submitting ShuffleMapStage 118 (MapPartitionsRDD[84] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:53:55 INFO MemoryStore: Block broadcast_50 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:53:55 INFO MemoryStore: Block broadcast_50_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:53:55 INFO BlockManagerInfo: Added broadcast_50_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:55 INFO SparkContext: Created broadcast 50 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:55 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 118 (MapPartitionsRDD[84] at map at StackOverflow.scala:186)
17/12/23 18:53:55 INFO TaskSchedulerImpl: Adding task set 118.0 with 6 tasks
17/12/23 18:53:55 INFO TaskSetManager: Starting task 0.0 in stage 118.0 (TID 294, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:55 INFO Executor: Running task 0.0 in stage 118.0 (TID 294)
17/12/23 18:53:55 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:53:56 INFO Executor: Finished task 0.0 in stage 118.0 (TID 294). 1325 bytes result sent to driver
17/12/23 18:53:56 INFO TaskSetManager: Starting task 1.0 in stage 118.0 (TID 295, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:56 INFO Executor: Running task 1.0 in stage 118.0 (TID 295)
17/12/23 18:53:56 INFO TaskSetManager: Finished task 0.0 in stage 118.0 (TID 294) in 500 ms on localhost (executor driver) (1/6)
17/12/23 18:53:56 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:53:56 INFO Executor: Finished task 1.0 in stage 118.0 (TID 295). 1238 bytes result sent to driver
17/12/23 18:53:56 INFO TaskSetManager: Starting task 2.0 in stage 118.0 (TID 296, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:56 INFO Executor: Running task 2.0 in stage 118.0 (TID 296)
17/12/23 18:53:56 INFO TaskSetManager: Finished task 1.0 in stage 118.0 (TID 295) in 515 ms on localhost (executor driver) (2/6)
17/12/23 18:53:56 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:53:57 INFO Executor: Finished task 2.0 in stage 118.0 (TID 296). 1325 bytes result sent to driver
17/12/23 18:53:57 INFO TaskSetManager: Starting task 3.0 in stage 118.0 (TID 297, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:57 INFO Executor: Running task 3.0 in stage 118.0 (TID 297)
17/12/23 18:53:57 INFO TaskSetManager: Finished task 2.0 in stage 118.0 (TID 296) in 500 ms on localhost (executor driver) (3/6)
17/12/23 18:53:57 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:53:57 INFO Executor: Finished task 3.0 in stage 118.0 (TID 297). 1238 bytes result sent to driver
17/12/23 18:53:57 INFO TaskSetManager: Starting task 4.0 in stage 118.0 (TID 298, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:57 INFO Executor: Running task 4.0 in stage 118.0 (TID 298)
17/12/23 18:53:57 INFO TaskSetManager: Finished task 3.0 in stage 118.0 (TID 297) in 502 ms on localhost (executor driver) (4/6)
17/12/23 18:53:57 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:53:57 INFO ContextCleaner: Cleaned shuffle 25
17/12/23 18:53:57 INFO BlockManagerInfo: Removed broadcast_48_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:53:57 INFO BlockManagerInfo: Removed broadcast_49_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:58 INFO Executor: Finished task 4.0 in stage 118.0 (TID 298). 1311 bytes result sent to driver
17/12/23 18:53:58 INFO TaskSetManager: Starting task 5.0 in stage 118.0 (TID 299, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:53:58 INFO Executor: Running task 5.0 in stage 118.0 (TID 299)
17/12/23 18:53:58 INFO TaskSetManager: Finished task 4.0 in stage 118.0 (TID 298) in 550 ms on localhost (executor driver) (5/6)
17/12/23 18:53:58 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:53:58 INFO Executor: Finished task 5.0 in stage 118.0 (TID 299). 1325 bytes result sent to driver
17/12/23 18:53:58 INFO TaskSetManager: Finished task 5.0 in stage 118.0 (TID 299) in 492 ms on localhost (executor driver) (6/6)
17/12/23 18:53:58 INFO TaskSchedulerImpl: Removed TaskSet 118.0, whose tasks have all completed, from pool 
17/12/23 18:53:58 INFO DAGScheduler: ShuffleMapStage 118 (map at StackOverflow.scala:186) finished in 3.057 s
17/12/23 18:53:58 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:53:58 INFO DAGScheduler: running: Set()
17/12/23 18:53:58 INFO DAGScheduler: waiting: Set(ResultStage 119)
17/12/23 18:53:58 INFO DAGScheduler: failed: Set()
17/12/23 18:53:58 INFO DAGScheduler: Submitting ResultStage 119 (MapPartitionsRDD[86] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:53:58 INFO MemoryStore: Block broadcast_51 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:53:58 INFO MemoryStore: Block broadcast_51_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:53:58 INFO BlockManagerInfo: Added broadcast_51_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:53:58 INFO SparkContext: Created broadcast 51 from broadcast at DAGScheduler.scala:996
17/12/23 18:53:58 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 119 (MapPartitionsRDD[86] at mapValues at StackOverflow.scala:190)
17/12/23 18:53:58 INFO TaskSchedulerImpl: Adding task set 119.0 with 6 tasks
17/12/23 18:53:58 INFO TaskSetManager: Starting task 0.0 in stage 119.0 (TID 300, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:53:58 INFO Executor: Running task 0.0 in stage 119.0 (TID 300)
17/12/23 18:53:58 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:58 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:59 INFO Executor: Finished task 0.0 in stage 119.0 (TID 300). 1985 bytes result sent to driver
17/12/23 18:53:59 INFO TaskSetManager: Starting task 1.0 in stage 119.0 (TID 301, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:53:59 INFO Executor: Running task 1.0 in stage 119.0 (TID 301)
17/12/23 18:53:59 INFO TaskSetManager: Finished task 0.0 in stage 119.0 (TID 300) in 480 ms on localhost (executor driver) (1/6)
17/12/23 18:53:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:59 INFO Executor: Finished task 1.0 in stage 119.0 (TID 301). 2072 bytes result sent to driver
17/12/23 18:53:59 INFO TaskSetManager: Starting task 2.0 in stage 119.0 (TID 302, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:53:59 INFO Executor: Running task 2.0 in stage 119.0 (TID 302)
17/12/23 18:53:59 INFO TaskSetManager: Finished task 1.0 in stage 119.0 (TID 301) in 722 ms on localhost (executor driver) (2/6)
17/12/23 18:53:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:53:59 INFO Executor: Finished task 2.0 in stage 119.0 (TID 302). 2072 bytes result sent to driver
17/12/23 18:53:59 INFO TaskSetManager: Starting task 3.0 in stage 119.0 (TID 303, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:53:59 INFO Executor: Running task 3.0 in stage 119.0 (TID 303)
17/12/23 18:53:59 INFO TaskSetManager: Finished task 2.0 in stage 119.0 (TID 302) in 38 ms on localhost (executor driver) (3/6)
17/12/23 18:53:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:53:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:00 INFO Executor: Finished task 3.0 in stage 119.0 (TID 303). 2130 bytes result sent to driver
17/12/23 18:54:00 INFO TaskSetManager: Starting task 4.0 in stage 119.0 (TID 304, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:54:00 INFO Executor: Running task 4.0 in stage 119.0 (TID 304)
17/12/23 18:54:00 INFO TaskSetManager: Finished task 3.0 in stage 119.0 (TID 303) in 255 ms on localhost (executor driver) (4/6)
17/12/23 18:54:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:00 INFO Executor: Finished task 4.0 in stage 119.0 (TID 304). 2040 bytes result sent to driver
17/12/23 18:54:00 INFO TaskSetManager: Starting task 5.0 in stage 119.0 (TID 305, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:54:00 INFO Executor: Running task 5.0 in stage 119.0 (TID 305)
17/12/23 18:54:00 INFO TaskSetManager: Finished task 4.0 in stage 119.0 (TID 304) in 397 ms on localhost (executor driver) (5/6)
17/12/23 18:54:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:01 INFO Executor: Finished task 5.0 in stage 119.0 (TID 305). 2040 bytes result sent to driver
17/12/23 18:54:01 INFO TaskSetManager: Finished task 5.0 in stage 119.0 (TID 305) in 947 ms on localhost (executor driver) (6/6)
Iteration: 23
17/12/23 18:54:01 INFO TaskSchedulerImpl: Removed TaskSet 119.0, whose tasks have all completed, from pool 
  * current distance: 784753.0
17/12/23 18:54:01 INFO DAGScheduler: ResultStage 119 (collect at StackOverflow.scala:191) finished in 2.839 s
  * desired distance: 20.0
17/12/23 18:54:01 INFO DAGScheduler: Job 23 finished: collect at StackOverflow.scala:191, took 5.907550 s
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,78) ==>          (450000,80)    distance:        4
               (0,1924) ==>             (0,1957)    distance:     1089
                (0,335) ==>              (0,350)    distance:      225
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,213) ==>         (150000,226)    distance:      169
          (150000,1289) ==>        (150000,1391)    distance:    10404
           (300000,444) ==>         (300000,466)    distance:      484
             (300000,2) ==>           (300000,3)    distance:        1
            (300000,83) ==>          (300000,85)    distance:        4
           (50000,2446) ==>         (50000,3322)    distance:   767376
            (50000,154) ==>          (50000,173)    distance:      361
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,68) ==>          (200000,72)    distance:       16
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,422) ==>         (200000,436)    distance:      196
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,141) ==>         (350000,145)    distance:       16
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,677) ==>         (350000,687)    distance:      100
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,687) ==>         (100000,746)    distance:     3481
            (100000,94) ==>         (100000,101)    distance:       49
           (400000,554) ==>         (400000,557)    distance:        9
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,110) ==>         (400000,112)    distance:        4
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,835) ==>         (250000,862)    distance:      729
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,108) ==>         (250000,114)    distance:       36
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:54:01 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:54:01 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:54:01 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:54:01 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:54:01 INFO DAGScheduler: Registering RDD 87 (map at StackOverflow.scala:186)
17/12/23 18:54:01 INFO DAGScheduler: Got job 24 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:54:01 INFO DAGScheduler: Final stage: ResultStage 124 (collect at StackOverflow.scala:191)
17/12/23 18:54:01 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 123)
17/12/23 18:54:01 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 123)
17/12/23 18:54:01 INFO DAGScheduler: Submitting ShuffleMapStage 123 (MapPartitionsRDD[87] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:54:01 INFO MemoryStore: Block broadcast_52 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:54:01 INFO MemoryStore: Block broadcast_52_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:54:01 INFO BlockManagerInfo: Added broadcast_52_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:01 INFO SparkContext: Created broadcast 52 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:01 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 123 (MapPartitionsRDD[87] at map at StackOverflow.scala:186)
17/12/23 18:54:01 INFO TaskSchedulerImpl: Adding task set 123.0 with 6 tasks
17/12/23 18:54:01 INFO TaskSetManager: Starting task 0.0 in stage 123.0 (TID 306, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:01 INFO Executor: Running task 0.0 in stage 123.0 (TID 306)
17/12/23 18:54:01 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:54:02 INFO Executor: Finished task 0.0 in stage 123.0 (TID 306). 1238 bytes result sent to driver
17/12/23 18:54:02 INFO TaskSetManager: Starting task 1.0 in stage 123.0 (TID 307, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:02 INFO Executor: Running task 1.0 in stage 123.0 (TID 307)
17/12/23 18:54:02 INFO TaskSetManager: Finished task 0.0 in stage 123.0 (TID 306) in 563 ms on localhost (executor driver) (1/6)
17/12/23 18:54:02 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:54:02 INFO Executor: Finished task 1.0 in stage 123.0 (TID 307). 1325 bytes result sent to driver
17/12/23 18:54:02 INFO TaskSetManager: Starting task 2.0 in stage 123.0 (TID 308, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:02 INFO TaskSetManager: Finished task 1.0 in stage 123.0 (TID 307) in 512 ms on localhost (executor driver) (2/6)
17/12/23 18:54:02 INFO Executor: Running task 2.0 in stage 123.0 (TID 308)
17/12/23 18:54:02 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:54:03 INFO Executor: Finished task 2.0 in stage 123.0 (TID 308). 1325 bytes result sent to driver
17/12/23 18:54:03 INFO TaskSetManager: Starting task 3.0 in stage 123.0 (TID 309, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:03 INFO Executor: Running task 3.0 in stage 123.0 (TID 309)
17/12/23 18:54:03 INFO TaskSetManager: Finished task 2.0 in stage 123.0 (TID 308) in 510 ms on localhost (executor driver) (3/6)
17/12/23 18:54:03 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:54:03 INFO Executor: Finished task 3.0 in stage 123.0 (TID 309). 1415 bytes result sent to driver
17/12/23 18:54:03 INFO TaskSetManager: Starting task 4.0 in stage 123.0 (TID 310, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:03 INFO Executor: Running task 4.0 in stage 123.0 (TID 310)
17/12/23 18:54:03 INFO TaskSetManager: Finished task 3.0 in stage 123.0 (TID 309) in 490 ms on localhost (executor driver) (4/6)
17/12/23 18:54:03 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:54:04 INFO Executor: Finished task 4.0 in stage 123.0 (TID 310). 1238 bytes result sent to driver
17/12/23 18:54:04 INFO TaskSetManager: Starting task 5.0 in stage 123.0 (TID 311, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:04 INFO Executor: Running task 5.0 in stage 123.0 (TID 311)
17/12/23 18:54:04 INFO TaskSetManager: Finished task 4.0 in stage 123.0 (TID 310) in 510 ms on localhost (executor driver) (5/6)
17/12/23 18:54:04 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:54:04 INFO Executor: Finished task 5.0 in stage 123.0 (TID 311). 1415 bytes result sent to driver
17/12/23 18:54:04 INFO TaskSetManager: Finished task 5.0 in stage 123.0 (TID 311) in 515 ms on localhost (executor driver) (6/6)
17/12/23 18:54:04 INFO TaskSchedulerImpl: Removed TaskSet 123.0, whose tasks have all completed, from pool 
17/12/23 18:54:04 INFO DAGScheduler: ShuffleMapStage 123 (map at StackOverflow.scala:186) finished in 3.097 s
17/12/23 18:54:04 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:54:04 INFO DAGScheduler: running: Set()
17/12/23 18:54:04 INFO DAGScheduler: waiting: Set(ResultStage 124)
17/12/23 18:54:04 INFO DAGScheduler: failed: Set()
17/12/23 18:54:04 INFO DAGScheduler: Submitting ResultStage 124 (MapPartitionsRDD[89] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:54:04 INFO MemoryStore: Block broadcast_53 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:54:04 INFO MemoryStore: Block broadcast_53_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:54:04 INFO BlockManagerInfo: Added broadcast_53_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:04 INFO SparkContext: Created broadcast 53 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:04 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 124 (MapPartitionsRDD[89] at mapValues at StackOverflow.scala:190)
17/12/23 18:54:04 INFO TaskSchedulerImpl: Adding task set 124.0 with 6 tasks
17/12/23 18:54:04 INFO TaskSetManager: Starting task 0.0 in stage 124.0 (TID 312, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:54:04 INFO Executor: Running task 0.0 in stage 124.0 (TID 312)
17/12/23 18:54:04 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:04 INFO ContextCleaner: Cleaned shuffle 26
17/12/23 18:54:04 INFO BlockManagerInfo: Removed broadcast_50_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:04 INFO BlockManagerInfo: Removed broadcast_51_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:04 INFO BlockManagerInfo: Removed broadcast_52_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:05 INFO Executor: Finished task 0.0 in stage 124.0 (TID 312). 2145 bytes result sent to driver
17/12/23 18:54:05 INFO TaskSetManager: Starting task 1.0 in stage 124.0 (TID 313, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:54:05 INFO Executor: Running task 1.0 in stage 124.0 (TID 313)
17/12/23 18:54:05 INFO TaskSetManager: Finished task 0.0 in stage 124.0 (TID 312) in 490 ms on localhost (executor driver) (1/6)
17/12/23 18:54:05 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:05 INFO Executor: Finished task 1.0 in stage 124.0 (TID 313). 2072 bytes result sent to driver
17/12/23 18:54:05 INFO TaskSetManager: Starting task 2.0 in stage 124.0 (TID 314, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:54:05 INFO Executor: Running task 2.0 in stage 124.0 (TID 314)
17/12/23 18:54:05 INFO TaskSetManager: Finished task 1.0 in stage 124.0 (TID 313) in 829 ms on localhost (executor driver) (2/6)
17/12/23 18:54:05 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:05 INFO Executor: Finished task 2.0 in stage 124.0 (TID 314). 2072 bytes result sent to driver
17/12/23 18:54:05 INFO TaskSetManager: Starting task 3.0 in stage 124.0 (TID 315, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:54:05 INFO TaskSetManager: Finished task 2.0 in stage 124.0 (TID 314) in 37 ms on localhost (executor driver) (3/6)
17/12/23 18:54:05 INFO Executor: Running task 3.0 in stage 124.0 (TID 315)
17/12/23 18:54:05 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:06 INFO Executor: Finished task 3.0 in stage 124.0 (TID 315). 2040 bytes result sent to driver
17/12/23 18:54:06 INFO TaskSetManager: Starting task 4.0 in stage 124.0 (TID 316, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:54:06 INFO Executor: Running task 4.0 in stage 124.0 (TID 316)
17/12/23 18:54:06 INFO TaskSetManager: Finished task 3.0 in stage 124.0 (TID 315) in 247 ms on localhost (executor driver) (4/6)
17/12/23 18:54:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:06 INFO Executor: Finished task 4.0 in stage 124.0 (TID 316). 2130 bytes result sent to driver
17/12/23 18:54:06 INFO TaskSetManager: Starting task 5.0 in stage 124.0 (TID 317, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:54:06 INFO Executor: Running task 5.0 in stage 124.0 (TID 317)
17/12/23 18:54:06 INFO TaskSetManager: Finished task 4.0 in stage 124.0 (TID 316) in 410 ms on localhost (executor driver) (5/6)
17/12/23 18:54:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:07 INFO Executor: Finished task 5.0 in stage 124.0 (TID 317). 2040 bytes result sent to driver
17/12/23 18:54:07 INFO TaskSetManager: Finished task 5.0 in stage 124.0 (TID 317) in 982 ms on localhost (executor driver) (6/6)
17/12/23 18:54:07 INFO TaskSchedulerImpl: Removed TaskSet 124.0, whose tasks have all completed, from pool 
17/12/23 18:54:07 INFO DAGScheduler: ResultStage 124 (collect at StackOverflow.scala:191) finished in 2.993 s
17/12/23 18:54:07 INFO DAGScheduler: Job 24 finished: collect at StackOverflow.scala:191, took 6.103853 s
Iteration: 24
  * current distance: 1.7746286E7
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,80) ==>          (450000,83)    distance:        9
               (0,1957) ==>             (0,2029)    distance:     5184
                (0,350) ==>              (0,365)    distance:      225
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,226) ==>         (150000,236)    distance:      100
          (150000,1391) ==>        (150000,1498)    distance:    11449
           (300000,466) ==>         (300000,492)    distance:      676
             (300000,3) ==>           (300000,3)    distance:        0
            (300000,85) ==>          (300000,89)    distance:       16
           (50000,3322) ==>         (50000,7532)    distance: 17724100
            (50000,173) ==>          (50000,198)    distance:      625
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,72) ==>          (200000,75)    distance:        9
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,436) ==>         (200000,447)    distance:      121
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,145) ==>         (350000,148)    distance:        9
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,687) ==>         (350000,697)    distance:      100
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,746) ==>         (100000,773)    distance:      729
           (100000,101) ==>         (100000,108)    distance:       49
           (400000,557) ==>         (400000,563)    distance:       36
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,112) ==>         (400000,114)    distance:        4
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,862) ==>         (250000,915)    distance:     2809
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,114) ==>         (250000,120)    distance:       36
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:54:07 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:54:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:54:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:54:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:54:07 INFO DAGScheduler: Registering RDD 90 (map at StackOverflow.scala:186)
17/12/23 18:54:07 INFO DAGScheduler: Got job 25 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:54:07 INFO DAGScheduler: Final stage: ResultStage 129 (collect at StackOverflow.scala:191)
17/12/23 18:54:07 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 128)
17/12/23 18:54:07 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 128)
17/12/23 18:54:07 INFO DAGScheduler: Submitting ShuffleMapStage 128 (MapPartitionsRDD[90] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:54:07 INFO MemoryStore: Block broadcast_54 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:54:07 INFO MemoryStore: Block broadcast_54_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:54:07 INFO BlockManagerInfo: Added broadcast_54_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:07 INFO SparkContext: Created broadcast 54 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:07 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 128 (MapPartitionsRDD[90] at map at StackOverflow.scala:186)
17/12/23 18:54:07 INFO TaskSchedulerImpl: Adding task set 128.0 with 6 tasks
17/12/23 18:54:07 INFO TaskSetManager: Starting task 0.0 in stage 128.0 (TID 318, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:07 INFO Executor: Running task 0.0 in stage 128.0 (TID 318)
17/12/23 18:54:07 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:54:08 INFO Executor: Finished task 0.0 in stage 128.0 (TID 318). 1238 bytes result sent to driver
17/12/23 18:54:08 INFO TaskSetManager: Starting task 1.0 in stage 128.0 (TID 319, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:08 INFO Executor: Running task 1.0 in stage 128.0 (TID 319)
17/12/23 18:54:08 INFO TaskSetManager: Finished task 0.0 in stage 128.0 (TID 318) in 505 ms on localhost (executor driver) (1/6)
17/12/23 18:54:08 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:54:08 INFO Executor: Finished task 1.0 in stage 128.0 (TID 319). 1325 bytes result sent to driver
17/12/23 18:54:08 INFO TaskSetManager: Starting task 2.0 in stage 128.0 (TID 320, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:08 INFO Executor: Running task 2.0 in stage 128.0 (TID 320)
17/12/23 18:54:08 INFO TaskSetManager: Finished task 1.0 in stage 128.0 (TID 319) in 497 ms on localhost (executor driver) (2/6)
17/12/23 18:54:08 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:54:09 INFO Executor: Finished task 2.0 in stage 128.0 (TID 320). 1238 bytes result sent to driver
17/12/23 18:54:09 INFO TaskSetManager: Starting task 3.0 in stage 128.0 (TID 321, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:09 INFO Executor: Running task 3.0 in stage 128.0 (TID 321)
17/12/23 18:54:09 INFO TaskSetManager: Finished task 2.0 in stage 128.0 (TID 320) in 538 ms on localhost (executor driver) (3/6)
17/12/23 18:54:09 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:54:09 INFO Executor: Finished task 3.0 in stage 128.0 (TID 321). 1325 bytes result sent to driver
17/12/23 18:54:09 INFO TaskSetManager: Starting task 4.0 in stage 128.0 (TID 322, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:09 INFO Executor: Running task 4.0 in stage 128.0 (TID 322)
17/12/23 18:54:09 INFO TaskSetManager: Finished task 3.0 in stage 128.0 (TID 321) in 502 ms on localhost (executor driver) (4/6)
17/12/23 18:54:09 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:54:10 INFO Executor: Finished task 4.0 in stage 128.0 (TID 322). 1238 bytes result sent to driver
17/12/23 18:54:10 INFO TaskSetManager: Starting task 5.0 in stage 128.0 (TID 323, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:10 INFO Executor: Running task 5.0 in stage 128.0 (TID 323)
17/12/23 18:54:10 INFO TaskSetManager: Finished task 4.0 in stage 128.0 (TID 322) in 500 ms on localhost (executor driver) (5/6)
17/12/23 18:54:10 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:54:10 INFO Executor: Finished task 5.0 in stage 128.0 (TID 323). 1238 bytes result sent to driver
17/12/23 18:54:10 INFO TaskSetManager: Finished task 5.0 in stage 128.0 (TID 323) in 525 ms on localhost (executor driver) (6/6)
17/12/23 18:54:10 INFO TaskSchedulerImpl: Removed TaskSet 128.0, whose tasks have all completed, from pool 
17/12/23 18:54:10 INFO DAGScheduler: ShuffleMapStage 128 (map at StackOverflow.scala:186) finished in 3.063 s
17/12/23 18:54:10 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:54:10 INFO DAGScheduler: running: Set()
17/12/23 18:54:10 INFO DAGScheduler: waiting: Set(ResultStage 129)
17/12/23 18:54:10 INFO DAGScheduler: failed: Set()
17/12/23 18:54:10 INFO DAGScheduler: Submitting ResultStage 129 (MapPartitionsRDD[92] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:54:10 INFO MemoryStore: Block broadcast_55 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:54:10 INFO MemoryStore: Block broadcast_55_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:54:10 INFO BlockManagerInfo: Added broadcast_55_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:10 INFO SparkContext: Created broadcast 55 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:10 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 129 (MapPartitionsRDD[92] at mapValues at StackOverflow.scala:190)
17/12/23 18:54:10 INFO TaskSchedulerImpl: Adding task set 129.0 with 6 tasks
17/12/23 18:54:10 INFO TaskSetManager: Starting task 0.0 in stage 129.0 (TID 324, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:54:10 INFO Executor: Running task 0.0 in stage 129.0 (TID 324)
17/12/23 18:54:10 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:10 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:11 INFO Executor: Finished task 0.0 in stage 129.0 (TID 324). 1985 bytes result sent to driver
17/12/23 18:54:11 INFO TaskSetManager: Starting task 1.0 in stage 129.0 (TID 325, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:54:11 INFO Executor: Running task 1.0 in stage 129.0 (TID 325)
17/12/23 18:54:11 INFO TaskSetManager: Finished task 0.0 in stage 129.0 (TID 324) in 470 ms on localhost (executor driver) (1/6)
17/12/23 18:54:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
17/12/23 18:54:11 INFO Executor: Finished task 1.0 in stage 129.0 (TID 325). 1985 bytes result sent to driver
17/12/23 18:54:11 INFO ContextCleaner: Cleaned shuffle 27
17/12/23 18:54:11 INFO TaskSetManager: Starting task 2.0 in stage 129.0 (TID 326, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:54:11 INFO Executor: Running task 2.0 in stage 129.0 (TID 326)
17/12/23 18:54:11 INFO TaskSetManager: Finished task 1.0 in stage 129.0 (TID 325) in 685 ms on localhost (executor driver) (2/6)
17/12/23 18:54:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:11 INFO BlockManagerInfo: Removed broadcast_53_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:11 INFO BlockManagerInfo: Removed broadcast_54_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:11 INFO Executor: Finished task 2.0 in stage 129.0 (TID 326). 2159 bytes result sent to driver
17/12/23 18:54:11 INFO TaskSetManager: Starting task 3.0 in stage 129.0 (TID 327, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:54:11 INFO Executor: Running task 3.0 in stage 129.0 (TID 327)
17/12/23 18:54:11 INFO TaskSetManager: Finished task 2.0 in stage 129.0 (TID 326) in 42 ms on localhost (executor driver) (3/6)
17/12/23 18:54:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:12 INFO Executor: Finished task 3.0 in stage 129.0 (TID 327). 1953 bytes result sent to driver
17/12/23 18:54:12 INFO TaskSetManager: Starting task 4.0 in stage 129.0 (TID 328, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:54:12 INFO TaskSetManager: Finished task 3.0 in stage 129.0 (TID 327) in 342 ms on localhost (executor driver) (4/6)
17/12/23 18:54:12 INFO Executor: Running task 4.0 in stage 129.0 (TID 328)
17/12/23 18:54:12 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:12 INFO Executor: Finished task 4.0 in stage 129.0 (TID 328). 2040 bytes result sent to driver
17/12/23 18:54:12 INFO TaskSetManager: Starting task 5.0 in stage 129.0 (TID 329, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:54:12 INFO Executor: Running task 5.0 in stage 129.0 (TID 329)
17/12/23 18:54:12 INFO TaskSetManager: Finished task 4.0 in stage 129.0 (TID 328) in 459 ms on localhost (executor driver) (5/6)
17/12/23 18:54:12 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:13 INFO Executor: Finished task 5.0 in stage 129.0 (TID 329). 2040 bytes result sent to driver
17/12/23 18:54:13 INFO TaskSetManager: Finished task 5.0 in stage 129.0 (TID 329) in 969 ms on localhost (executor driver) (6/6)
17/12/23 18:54:13 INFO TaskSchedulerImpl: Removed TaskSet 129.0, whose tasks have all completed, from pool 
17/12/23 18:54:13 INFO DAGScheduler: ResultStage 129 (collect at StackOverflow.scala:191) finished in 2.965 s
17/12/23 18:54:13 INFO DAGScheduler: Job 25 finished: collect at StackOverflow.scala:191, took 6.038178 s
Iteration: 25
  * current distance: 7512156.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,83) ==>          (450000,84)    distance:        1
               (0,2029) ==>             (0,2069)    distance:     1600
                (0,365) ==>              (0,379)    distance:      196
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,236) ==>         (150000,246)    distance:      100
          (150000,1498) ==>        (150000,1560)    distance:     3844
           (300000,492) ==>         (300000,525)    distance:     1089
             (300000,3) ==>           (300000,3)    distance:        0
            (300000,89) ==>          (300000,94)    distance:       25
           (50000,7532) ==>        (50000,10271)    distance:  7502121
            (50000,198) ==>          (50000,221)    distance:      529
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,75) ==>          (200000,79)    distance:       16
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,447) ==>         (200000,456)    distance:       81
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,148) ==>         (350000,151)    distance:        9
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,697) ==>         (350000,708)    distance:      121
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,773) ==>         (100000,814)    distance:     1681
           (100000,108) ==>         (100000,113)    distance:       25
           (400000,563) ==>         (400000,571)    distance:       64
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,114) ==>         (400000,116)    distance:        4
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,915) ==>         (250000,940)    distance:      625
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,120) ==>         (250000,125)    distance:       25
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:54:13 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:54:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:54:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:54:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:54:13 INFO DAGScheduler: Registering RDD 93 (map at StackOverflow.scala:186)
17/12/23 18:54:13 INFO DAGScheduler: Got job 26 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:54:13 INFO DAGScheduler: Final stage: ResultStage 134 (collect at StackOverflow.scala:191)
17/12/23 18:54:13 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 133)
17/12/23 18:54:13 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 133)
17/12/23 18:54:13 INFO DAGScheduler: Submitting ShuffleMapStage 133 (MapPartitionsRDD[93] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:54:13 INFO MemoryStore: Block broadcast_56 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:54:13 INFO MemoryStore: Block broadcast_56_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:54:13 INFO BlockManagerInfo: Added broadcast_56_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:13 INFO SparkContext: Created broadcast 56 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:13 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 133 (MapPartitionsRDD[93] at map at StackOverflow.scala:186)
17/12/23 18:54:13 INFO TaskSchedulerImpl: Adding task set 133.0 with 6 tasks
17/12/23 18:54:13 INFO TaskSetManager: Starting task 0.0 in stage 133.0 (TID 330, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:13 INFO Executor: Running task 0.0 in stage 133.0 (TID 330)
17/12/23 18:54:13 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:54:14 INFO Executor: Finished task 0.0 in stage 133.0 (TID 330). 1415 bytes result sent to driver
17/12/23 18:54:14 INFO TaskSetManager: Starting task 1.0 in stage 133.0 (TID 331, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:14 INFO Executor: Running task 1.0 in stage 133.0 (TID 331)
17/12/23 18:54:14 INFO TaskSetManager: Finished task 0.0 in stage 133.0 (TID 330) in 556 ms on localhost (executor driver) (1/6)
17/12/23 18:54:14 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:54:14 INFO Executor: Finished task 1.0 in stage 133.0 (TID 331). 1238 bytes result sent to driver
17/12/23 18:54:14 INFO TaskSetManager: Starting task 2.0 in stage 133.0 (TID 332, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:14 INFO Executor: Running task 2.0 in stage 133.0 (TID 332)
17/12/23 18:54:14 INFO TaskSetManager: Finished task 1.0 in stage 133.0 (TID 331) in 512 ms on localhost (executor driver) (2/6)
17/12/23 18:54:14 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:54:15 INFO Executor: Finished task 2.0 in stage 133.0 (TID 332). 1325 bytes result sent to driver
17/12/23 18:54:15 INFO TaskSetManager: Starting task 3.0 in stage 133.0 (TID 333, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:15 INFO Executor: Running task 3.0 in stage 133.0 (TID 333)
17/12/23 18:54:15 INFO TaskSetManager: Finished task 2.0 in stage 133.0 (TID 332) in 513 ms on localhost (executor driver) (3/6)
17/12/23 18:54:15 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:54:15 INFO Executor: Finished task 3.0 in stage 133.0 (TID 333). 1325 bytes result sent to driver
17/12/23 18:54:15 INFO TaskSetManager: Starting task 4.0 in stage 133.0 (TID 334, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:15 INFO Executor: Running task 4.0 in stage 133.0 (TID 334)
17/12/23 18:54:15 INFO TaskSetManager: Finished task 3.0 in stage 133.0 (TID 333) in 525 ms on localhost (executor driver) (4/6)
17/12/23 18:54:15 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:54:16 INFO Executor: Finished task 4.0 in stage 133.0 (TID 334). 1238 bytes result sent to driver
17/12/23 18:54:16 INFO TaskSetManager: Starting task 5.0 in stage 133.0 (TID 335, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:16 INFO Executor: Running task 5.0 in stage 133.0 (TID 335)
17/12/23 18:54:16 INFO TaskSetManager: Finished task 4.0 in stage 133.0 (TID 334) in 492 ms on localhost (executor driver) (5/6)
17/12/23 18:54:16 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:54:16 INFO Executor: Finished task 5.0 in stage 133.0 (TID 335). 1238 bytes result sent to driver
17/12/23 18:54:16 INFO TaskSetManager: Finished task 5.0 in stage 133.0 (TID 335) in 507 ms on localhost (executor driver) (6/6)
17/12/23 18:54:16 INFO TaskSchedulerImpl: Removed TaskSet 133.0, whose tasks have all completed, from pool 
17/12/23 18:54:16 INFO DAGScheduler: ShuffleMapStage 133 (map at StackOverflow.scala:186) finished in 3.103 s
17/12/23 18:54:16 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:54:16 INFO DAGScheduler: running: Set()
17/12/23 18:54:16 INFO DAGScheduler: waiting: Set(ResultStage 134)
17/12/23 18:54:16 INFO DAGScheduler: failed: Set()
17/12/23 18:54:16 INFO DAGScheduler: Submitting ResultStage 134 (MapPartitionsRDD[95] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:54:16 INFO MemoryStore: Block broadcast_57 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:54:16 INFO MemoryStore: Block broadcast_57_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:54:16 INFO BlockManagerInfo: Added broadcast_57_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:16 INFO SparkContext: Created broadcast 57 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:16 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 134 (MapPartitionsRDD[95] at mapValues at StackOverflow.scala:190)
17/12/23 18:54:16 INFO TaskSchedulerImpl: Adding task set 134.0 with 6 tasks
17/12/23 18:54:16 INFO TaskSetManager: Starting task 0.0 in stage 134.0 (TID 336, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:54:16 INFO Executor: Running task 0.0 in stage 134.0 (TID 336)
17/12/23 18:54:16 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:17 INFO Executor: Finished task 0.0 in stage 134.0 (TID 336). 1985 bytes result sent to driver
17/12/23 18:54:17 INFO TaskSetManager: Starting task 1.0 in stage 134.0 (TID 337, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:54:17 INFO Executor: Running task 1.0 in stage 134.0 (TID 337)
17/12/23 18:54:17 INFO TaskSetManager: Finished task 0.0 in stage 134.0 (TID 336) in 455 ms on localhost (executor driver) (1/6)
17/12/23 18:54:17 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:17 INFO Executor: Finished task 1.0 in stage 134.0 (TID 337). 1985 bytes result sent to driver
17/12/23 18:54:17 INFO TaskSetManager: Starting task 2.0 in stage 134.0 (TID 338, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:54:17 INFO Executor: Running task 2.0 in stage 134.0 (TID 338)
17/12/23 18:54:17 INFO TaskSetManager: Finished task 1.0 in stage 134.0 (TID 337) in 700 ms on localhost (executor driver) (2/6)
17/12/23 18:54:17 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:17 INFO Executor: Finished task 2.0 in stage 134.0 (TID 338). 2072 bytes result sent to driver
17/12/23 18:54:17 INFO TaskSetManager: Starting task 3.0 in stage 134.0 (TID 339, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:54:17 INFO Executor: Running task 3.0 in stage 134.0 (TID 339)
17/12/23 18:54:17 INFO TaskSetManager: Finished task 2.0 in stage 134.0 (TID 338) in 40 ms on localhost (executor driver) (3/6)
17/12/23 18:54:17 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:18 INFO Executor: Finished task 3.0 in stage 134.0 (TID 339). 1953 bytes result sent to driver
17/12/23 18:54:18 INFO TaskSetManager: Starting task 4.0 in stage 134.0 (TID 340, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:54:18 INFO Executor: Running task 4.0 in stage 134.0 (TID 340)
17/12/23 18:54:18 INFO TaskSetManager: Finished task 3.0 in stage 134.0 (TID 339) in 282 ms on localhost (executor driver) (4/6)
17/12/23 18:54:18 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:18 INFO Executor: Finished task 4.0 in stage 134.0 (TID 340). 1953 bytes result sent to driver
17/12/23 18:54:18 INFO TaskSetManager: Starting task 5.0 in stage 134.0 (TID 341, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:54:18 INFO Executor: Running task 5.0 in stage 134.0 (TID 341)
17/12/23 18:54:18 INFO TaskSetManager: Finished task 4.0 in stage 134.0 (TID 340) in 404 ms on localhost (executor driver) (5/6)
17/12/23 18:54:18 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:18 INFO ContextCleaner: Cleaned shuffle 28
17/12/23 18:54:18 INFO BlockManagerInfo: Removed broadcast_55_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:18 INFO BlockManagerInfo: Removed broadcast_56_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
Iteration: 26
  * current distance: 5160.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,84) ==>          (450000,86)    distance:        4
               (0,2069) ==>             (0,2111)    distance:     1764
                (0,379) ==>              (0,392)    distance:      169
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,246) ==>         (150000,255)    distance:       81
          (150000,1560) ==>        (150000,1594)    distance:     1156
17/12/23 18:54:19 INFO Executor: Finished task 5.0 in stage 134.0 (TID 341). 2113 bytes result sent to driver
17/12/23 18:54:19 INFO TaskSetManager: Finished task 5.0 in stage 134.0 (TID 341) in 975 ms on localhost (executor driver) (6/6)
17/12/23 18:54:19 INFO TaskSchedulerImpl: Removed TaskSet 134.0, whose tasks have all completed, from pool 
17/12/23 18:54:19 INFO DAGScheduler: ResultStage 134 (collect at StackOverflow.scala:191) finished in 2.854 s
17/12/23 18:54:19 INFO DAGScheduler: Job 26 finished: collect at StackOverflow.scala:191, took 5.971054 s
           (300000,525) ==>         (300000,537)    distance:      144
             (300000,3) ==>           (300000,3)    distance:        0
            (300000,94) ==>          (300000,97)    distance:        9
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,221) ==>          (50000,242)    distance:      441
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,79) ==>          (200000,82)    distance:        9
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,456) ==>         (200000,464)    distance:       64
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,151) ==>         (350000,154)    distance:        9
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,708) ==>         (350000,713)    distance:       25
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,814) ==>         (100000,836)    distance:      484
           (100000,113) ==>         (100000,119)    distance:       36
           (400000,571) ==>         (400000,578)    distance:       49
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,116) ==>         (400000,118)    distance:        4
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,940) ==>         (250000,966)    distance:      676
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,125) ==>         (250000,131)    distance:       36
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:54:19 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:54:19 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:54:19 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:54:19 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:54:19 INFO DAGScheduler: Registering RDD 96 (map at StackOverflow.scala:186)
17/12/23 18:54:19 INFO DAGScheduler: Got job 27 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:54:19 INFO DAGScheduler: Final stage: ResultStage 139 (collect at StackOverflow.scala:191)
17/12/23 18:54:19 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 138)
17/12/23 18:54:19 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 138)
17/12/23 18:54:19 INFO DAGScheduler: Submitting ShuffleMapStage 138 (MapPartitionsRDD[96] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:54:19 INFO MemoryStore: Block broadcast_58 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:54:19 INFO MemoryStore: Block broadcast_58_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:54:19 INFO BlockManagerInfo: Added broadcast_58_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:19 INFO SparkContext: Created broadcast 58 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:19 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 138 (MapPartitionsRDD[96] at map at StackOverflow.scala:186)
17/12/23 18:54:19 INFO TaskSchedulerImpl: Adding task set 138.0 with 6 tasks
17/12/23 18:54:19 INFO TaskSetManager: Starting task 0.0 in stage 138.0 (TID 342, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:19 INFO Executor: Running task 0.0 in stage 138.0 (TID 342)
17/12/23 18:54:19 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:54:20 INFO Executor: Finished task 0.0 in stage 138.0 (TID 342). 1238 bytes result sent to driver
17/12/23 18:54:20 INFO TaskSetManager: Starting task 1.0 in stage 138.0 (TID 343, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:20 INFO Executor: Running task 1.0 in stage 138.0 (TID 343)
17/12/23 18:54:20 INFO TaskSetManager: Finished task 0.0 in stage 138.0 (TID 342) in 513 ms on localhost (executor driver) (1/6)
17/12/23 18:54:20 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:54:20 INFO Executor: Finished task 1.0 in stage 138.0 (TID 343). 1325 bytes result sent to driver
17/12/23 18:54:20 INFO TaskSetManager: Starting task 2.0 in stage 138.0 (TID 344, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:20 INFO TaskSetManager: Finished task 1.0 in stage 138.0 (TID 343) in 503 ms on localhost (executor driver) (2/6)
17/12/23 18:54:20 INFO Executor: Running task 2.0 in stage 138.0 (TID 344)
17/12/23 18:54:20 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:54:21 INFO Executor: Finished task 2.0 in stage 138.0 (TID 344). 1325 bytes result sent to driver
17/12/23 18:54:21 INFO TaskSetManager: Starting task 3.0 in stage 138.0 (TID 345, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:21 INFO Executor: Running task 3.0 in stage 138.0 (TID 345)
17/12/23 18:54:21 INFO TaskSetManager: Finished task 2.0 in stage 138.0 (TID 344) in 480 ms on localhost (executor driver) (3/6)
17/12/23 18:54:21 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:54:21 INFO Executor: Finished task 3.0 in stage 138.0 (TID 345). 1415 bytes result sent to driver
17/12/23 18:54:21 INFO TaskSetManager: Starting task 4.0 in stage 138.0 (TID 346, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:21 INFO Executor: Running task 4.0 in stage 138.0 (TID 346)
17/12/23 18:54:21 INFO TaskSetManager: Finished task 3.0 in stage 138.0 (TID 345) in 497 ms on localhost (executor driver) (4/6)
17/12/23 18:54:21 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:54:22 INFO Executor: Finished task 4.0 in stage 138.0 (TID 346). 1415 bytes result sent to driver
17/12/23 18:54:22 INFO TaskSetManager: Starting task 5.0 in stage 138.0 (TID 347, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:22 INFO Executor: Running task 5.0 in stage 138.0 (TID 347)
17/12/23 18:54:22 INFO TaskSetManager: Finished task 4.0 in stage 138.0 (TID 346) in 492 ms on localhost (executor driver) (5/6)
17/12/23 18:54:22 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:54:22 INFO Executor: Finished task 5.0 in stage 138.0 (TID 347). 1325 bytes result sent to driver
17/12/23 18:54:22 INFO TaskSetManager: Finished task 5.0 in stage 138.0 (TID 347) in 508 ms on localhost (executor driver) (6/6)
17/12/23 18:54:22 INFO TaskSchedulerImpl: Removed TaskSet 138.0, whose tasks have all completed, from pool 
17/12/23 18:54:22 INFO DAGScheduler: ShuffleMapStage 138 (map at StackOverflow.scala:186) finished in 2.993 s
17/12/23 18:54:22 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:54:22 INFO DAGScheduler: running: Set()
17/12/23 18:54:22 INFO DAGScheduler: waiting: Set(ResultStage 139)
17/12/23 18:54:22 INFO DAGScheduler: failed: Set()
17/12/23 18:54:22 INFO DAGScheduler: Submitting ResultStage 139 (MapPartitionsRDD[98] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:54:22 INFO MemoryStore: Block broadcast_59 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:54:22 INFO MemoryStore: Block broadcast_59_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:54:22 INFO BlockManagerInfo: Added broadcast_59_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:22 INFO SparkContext: Created broadcast 59 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:22 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 139 (MapPartitionsRDD[98] at mapValues at StackOverflow.scala:190)
17/12/23 18:54:22 INFO TaskSchedulerImpl: Adding task set 139.0 with 6 tasks
17/12/23 18:54:22 INFO TaskSetManager: Starting task 0.0 in stage 139.0 (TID 348, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:54:22 INFO Executor: Running task 0.0 in stage 139.0 (TID 348)
17/12/23 18:54:22 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:23 INFO Executor: Finished task 0.0 in stage 139.0 (TID 348). 2072 bytes result sent to driver
17/12/23 18:54:23 INFO TaskSetManager: Starting task 1.0 in stage 139.0 (TID 349, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:54:23 INFO Executor: Running task 1.0 in stage 139.0 (TID 349)
17/12/23 18:54:23 INFO TaskSetManager: Finished task 0.0 in stage 139.0 (TID 348) in 497 ms on localhost (executor driver) (1/6)
17/12/23 18:54:23 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:23 INFO Executor: Finished task 1.0 in stage 139.0 (TID 349). 2072 bytes result sent to driver
17/12/23 18:54:23 INFO TaskSetManager: Starting task 2.0 in stage 139.0 (TID 350, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:54:23 INFO Executor: Running task 2.0 in stage 139.0 (TID 350)
17/12/23 18:54:23 INFO TaskSetManager: Finished task 1.0 in stage 139.0 (TID 349) in 690 ms on localhost (executor driver) (2/6)
17/12/23 18:54:23 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:23 INFO Executor: Finished task 2.0 in stage 139.0 (TID 350). 1985 bytes result sent to driver
17/12/23 18:54:23 INFO TaskSetManager: Starting task 3.0 in stage 139.0 (TID 351, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:54:23 INFO Executor: Running task 3.0 in stage 139.0 (TID 351)
17/12/23 18:54:23 INFO TaskSetManager: Finished task 2.0 in stage 139.0 (TID 350) in 35 ms on localhost (executor driver) (3/6)
17/12/23 18:54:23 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:24 INFO Executor: Finished task 3.0 in stage 139.0 (TID 351). 2040 bytes result sent to driver
17/12/23 18:54:24 INFO TaskSetManager: Starting task 4.0 in stage 139.0 (TID 352, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:54:24 INFO TaskSetManager: Finished task 3.0 in stage 139.0 (TID 351) in 267 ms on localhost (executor driver) (4/6)
17/12/23 18:54:24 INFO Executor: Running task 4.0 in stage 139.0 (TID 352)
17/12/23 18:54:24 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:24 INFO Executor: Finished task 4.0 in stage 139.0 (TID 352). 2040 bytes result sent to driver
17/12/23 18:54:24 INFO TaskSetManager: Starting task 5.0 in stage 139.0 (TID 353, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:54:24 INFO Executor: Running task 5.0 in stage 139.0 (TID 353)
17/12/23 18:54:24 INFO TaskSetManager: Finished task 4.0 in stage 139.0 (TID 352) in 360 ms on localhost (executor driver) (5/6)
17/12/23 18:54:24 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
Iteration: 27
  * current distance: 7976.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,86) ==>          (450000,87)    distance:        1
               (0,2111) ==>             (0,2156)    distance:     2025
                (0,392) ==>              (0,404)    distance:      144
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,255) ==>         (150000,263)    distance:       64
          (150000,1594) ==>        (150000,1629)    distance:     1225
           (300000,537) ==>         (300000,550)    distance:      169
             (300000,3) ==>           (300000,3)    distance:        0
            (300000,97) ==>         (300000,101)    distance:       16
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,242) ==>          (50000,257)    distance:      225
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,82) ==>          (200000,83)    distance:        1
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,464) ==>         (200000,471)    distance:       49
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,154) ==>         (350000,158)    distance:       16
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,713) ==>         (350000,737)    distance:      576
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,836) ==>         (100000,874)    distance:     1444
           (100000,119) ==>         (100000,125)    distance:       36
           (400000,578) ==>         (400000,581)    distance:        9
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,118) ==>         (400000,120)    distance:        4
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
           (250000,966) ==>        (250000,1010)    distance:     1936
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,131) ==>         (250000,137)    distance:       36
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:54:25 INFO Executor: Finished task 5.0 in stage 139.0 (TID 353). 2040 bytes result sent to driver
17/12/23 18:54:25 INFO TaskSetManager: Finished task 5.0 in stage 139.0 (TID 353) in 960 ms on localhost (executor driver) (6/6)
17/12/23 18:54:25 INFO TaskSchedulerImpl: Removed TaskSet 139.0, whose tasks have all completed, from pool 
17/12/23 18:54:25 INFO DAGScheduler: ResultStage 139 (collect at StackOverflow.scala:191) finished in 2.807 s
17/12/23 18:54:25 INFO DAGScheduler: Job 27 finished: collect at StackOverflow.scala:191, took 5.811946 s
17/12/23 18:54:25 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:54:25 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:54:25 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:54:25 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:54:25 INFO DAGScheduler: Registering RDD 99 (map at StackOverflow.scala:186)
17/12/23 18:54:25 INFO DAGScheduler: Got job 28 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:54:25 INFO DAGScheduler: Final stage: ResultStage 144 (collect at StackOverflow.scala:191)
17/12/23 18:54:25 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 143)
17/12/23 18:54:25 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 143)
17/12/23 18:54:25 INFO DAGScheduler: Submitting ShuffleMapStage 143 (MapPartitionsRDD[99] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:54:25 INFO MemoryStore: Block broadcast_60 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:54:25 INFO MemoryStore: Block broadcast_60_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:54:25 INFO BlockManagerInfo: Added broadcast_60_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:25 INFO SparkContext: Created broadcast 60 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:25 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 143 (MapPartitionsRDD[99] at map at StackOverflow.scala:186)
17/12/23 18:54:25 INFO TaskSchedulerImpl: Adding task set 143.0 with 6 tasks
17/12/23 18:54:25 INFO TaskSetManager: Starting task 0.0 in stage 143.0 (TID 354, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:25 INFO Executor: Running task 0.0 in stage 143.0 (TID 354)
17/12/23 18:54:25 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:54:25 INFO ContextCleaner: Cleaned shuffle 29
17/12/23 18:54:25 INFO BlockManagerInfo: Removed broadcast_57_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:25 INFO ContextCleaner: Cleaned shuffle 30
17/12/23 18:54:25 INFO BlockManagerInfo: Removed broadcast_58_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:25 INFO BlockManagerInfo: Removed broadcast_59_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:26 INFO Executor: Finished task 0.0 in stage 143.0 (TID 354). 1398 bytes result sent to driver
17/12/23 18:54:26 INFO TaskSetManager: Starting task 1.0 in stage 143.0 (TID 355, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:26 INFO Executor: Running task 1.0 in stage 143.0 (TID 355)
17/12/23 18:54:26 INFO TaskSetManager: Finished task 0.0 in stage 143.0 (TID 354) in 629 ms on localhost (executor driver) (1/6)
17/12/23 18:54:26 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:54:26 INFO Executor: Finished task 1.0 in stage 143.0 (TID 355). 1325 bytes result sent to driver
17/12/23 18:54:26 INFO TaskSetManager: Starting task 2.0 in stage 143.0 (TID 356, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:26 INFO TaskSetManager: Finished task 1.0 in stage 143.0 (TID 355) in 554 ms on localhost (executor driver) (2/6)
17/12/23 18:54:26 INFO Executor: Running task 2.0 in stage 143.0 (TID 356)
17/12/23 18:54:26 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:54:27 INFO Executor: Finished task 2.0 in stage 143.0 (TID 356). 1325 bytes result sent to driver
17/12/23 18:54:27 INFO TaskSetManager: Starting task 3.0 in stage 143.0 (TID 357, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:27 INFO Executor: Running task 3.0 in stage 143.0 (TID 357)
17/12/23 18:54:27 INFO TaskSetManager: Finished task 2.0 in stage 143.0 (TID 356) in 548 ms on localhost (executor driver) (3/6)
17/12/23 18:54:27 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:54:27 INFO Executor: Finished task 3.0 in stage 143.0 (TID 357). 1238 bytes result sent to driver
17/12/23 18:54:27 INFO TaskSetManager: Starting task 4.0 in stage 143.0 (TID 358, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:27 INFO Executor: Running task 4.0 in stage 143.0 (TID 358)
17/12/23 18:54:27 INFO TaskSetManager: Finished task 3.0 in stage 143.0 (TID 357) in 513 ms on localhost (executor driver) (4/6)
17/12/23 18:54:27 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:54:28 INFO Executor: Finished task 4.0 in stage 143.0 (TID 358). 1238 bytes result sent to driver
17/12/23 18:54:28 INFO TaskSetManager: Starting task 5.0 in stage 143.0 (TID 359, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:28 INFO Executor: Running task 5.0 in stage 143.0 (TID 359)
17/12/23 18:54:28 INFO TaskSetManager: Finished task 4.0 in stage 143.0 (TID 358) in 515 ms on localhost (executor driver) (5/6)
17/12/23 18:54:28 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:54:28 INFO Executor: Finished task 5.0 in stage 143.0 (TID 359). 1325 bytes result sent to driver
17/12/23 18:54:28 INFO TaskSetManager: Finished task 5.0 in stage 143.0 (TID 359) in 507 ms on localhost (executor driver) (6/6)
17/12/23 18:54:28 INFO TaskSchedulerImpl: Removed TaskSet 143.0, whose tasks have all completed, from pool 
17/12/23 18:54:28 INFO DAGScheduler: ShuffleMapStage 143 (map at StackOverflow.scala:186) finished in 3.262 s
17/12/23 18:54:28 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:54:28 INFO DAGScheduler: running: Set()
17/12/23 18:54:28 INFO DAGScheduler: waiting: Set(ResultStage 144)
17/12/23 18:54:28 INFO DAGScheduler: failed: Set()
17/12/23 18:54:28 INFO DAGScheduler: Submitting ResultStage 144 (MapPartitionsRDD[101] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:54:28 INFO MemoryStore: Block broadcast_61 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:54:28 INFO MemoryStore: Block broadcast_61_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:54:28 INFO BlockManagerInfo: Added broadcast_61_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:28 INFO SparkContext: Created broadcast 61 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:28 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 144 (MapPartitionsRDD[101] at mapValues at StackOverflow.scala:190)
17/12/23 18:54:28 INFO TaskSchedulerImpl: Adding task set 144.0 with 6 tasks
17/12/23 18:54:28 INFO TaskSetManager: Starting task 0.0 in stage 144.0 (TID 360, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:54:28 INFO Executor: Running task 0.0 in stage 144.0 (TID 360)
17/12/23 18:54:28 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:28 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:29 INFO Executor: Finished task 0.0 in stage 144.0 (TID 360). 1985 bytes result sent to driver
17/12/23 18:54:29 INFO TaskSetManager: Starting task 1.0 in stage 144.0 (TID 361, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:54:29 INFO Executor: Running task 1.0 in stage 144.0 (TID 361)
17/12/23 18:54:29 INFO TaskSetManager: Finished task 0.0 in stage 144.0 (TID 360) in 458 ms on localhost (executor driver) (1/6)
17/12/23 18:54:29 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:29 INFO Executor: Finished task 1.0 in stage 144.0 (TID 361). 2162 bytes result sent to driver
17/12/23 18:54:29 INFO TaskSetManager: Starting task 2.0 in stage 144.0 (TID 362, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:54:29 INFO Executor: Running task 2.0 in stage 144.0 (TID 362)
17/12/23 18:54:29 INFO TaskSetManager: Finished task 1.0 in stage 144.0 (TID 361) in 730 ms on localhost (executor driver) (2/6)
17/12/23 18:54:29 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:30 INFO Executor: Finished task 2.0 in stage 144.0 (TID 362). 1985 bytes result sent to driver
17/12/23 18:54:30 INFO TaskSetManager: Starting task 3.0 in stage 144.0 (TID 363, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:54:30 INFO Executor: Running task 3.0 in stage 144.0 (TID 363)
17/12/23 18:54:30 INFO TaskSetManager: Finished task 2.0 in stage 144.0 (TID 362) in 32 ms on localhost (executor driver) (3/6)
17/12/23 18:54:30 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:30 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:30 INFO Executor: Finished task 3.0 in stage 144.0 (TID 363). 1953 bytes result sent to driver
17/12/23 18:54:30 INFO TaskSetManager: Starting task 4.0 in stage 144.0 (TID 364, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:54:30 INFO Executor: Running task 4.0 in stage 144.0 (TID 364)
17/12/23 18:54:30 INFO TaskSetManager: Finished task 3.0 in stage 144.0 (TID 363) in 255 ms on localhost (executor driver) (4/6)
17/12/23 18:54:30 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:30 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:30 INFO Executor: Finished task 4.0 in stage 144.0 (TID 364). 1953 bytes result sent to driver
17/12/23 18:54:30 INFO TaskSetManager: Starting task 5.0 in stage 144.0 (TID 365, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:54:30 INFO Executor: Running task 5.0 in stage 144.0 (TID 365)
17/12/23 18:54:30 INFO TaskSetManager: Finished task 4.0 in stage 144.0 (TID 364) in 345 ms on localhost (executor driver) (5/6)
17/12/23 18:54:30 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:30 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
Iteration: 28
  * current distance: 11208.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2156) ==>             (0,2179)    distance:      529
                (0,404) ==>              (0,415)    distance:      121
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,263) ==>         (150000,270)    distance:       49
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,550) ==>         (300000,557)    distance:       49
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,101) ==>         (300000,104)    distance:        9
          (50000,10271) ==>        (50000,10271)    distance:        0
17/12/23 18:54:31 INFO Executor: Finished task 5.0 in stage 144.0 (TID 365). 2040 bytes result sent to driver
17/12/23 18:54:31 INFO TaskSetManager: Finished task 5.0 in stage 144.0 (TID 365) in 948 ms on localhost (executor driver) (6/6)
17/12/23 18:54:31 INFO TaskSchedulerImpl: Removed TaskSet 144.0, whose tasks have all completed, from pool 
17/12/23 18:54:31 INFO DAGScheduler: ResultStage 144 (collect at StackOverflow.scala:191) finished in 2.768 s
17/12/23 18:54:31 INFO DAGScheduler: Job 28 finished: collect at StackOverflow.scala:191, took 6.054006 s
            (50000,257) ==>          (50000,270)    distance:      169
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,83) ==>          (200000,85)    distance:        4
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,471) ==>         (200000,477)    distance:       36
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,158) ==>         (350000,163)    distance:       25
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,737) ==>         (350000,758)    distance:      441
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,874) ==>         (100000,903)    distance:      841
           (100000,125) ==>         (100000,130)    distance:       25
           (400000,581) ==>         (400000,584)    distance:        9
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,120) ==>         (400000,121)    distance:        1
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1010) ==>        (250000,1104)    distance:     8836
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,137) ==>         (250000,145)    distance:       64
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:54:31 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:54:31 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:54:31 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:54:31 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:54:31 INFO DAGScheduler: Registering RDD 102 (map at StackOverflow.scala:186)
17/12/23 18:54:31 INFO DAGScheduler: Got job 29 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:54:31 INFO DAGScheduler: Final stage: ResultStage 149 (collect at StackOverflow.scala:191)
17/12/23 18:54:31 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 148)
17/12/23 18:54:31 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 148)
17/12/23 18:54:31 INFO DAGScheduler: Submitting ShuffleMapStage 148 (MapPartitionsRDD[102] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:54:31 INFO MemoryStore: Block broadcast_62 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:54:31 INFO MemoryStore: Block broadcast_62_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:54:31 INFO BlockManagerInfo: Added broadcast_62_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:31 INFO SparkContext: Created broadcast 62 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:31 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 148 (MapPartitionsRDD[102] at map at StackOverflow.scala:186)
17/12/23 18:54:31 INFO TaskSchedulerImpl: Adding task set 148.0 with 6 tasks
17/12/23 18:54:31 INFO TaskSetManager: Starting task 0.0 in stage 148.0 (TID 366, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:31 INFO Executor: Running task 0.0 in stage 148.0 (TID 366)
17/12/23 18:54:31 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:54:32 INFO Executor: Finished task 0.0 in stage 148.0 (TID 366). 1325 bytes result sent to driver
17/12/23 18:54:32 INFO TaskSetManager: Starting task 1.0 in stage 148.0 (TID 367, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:32 INFO TaskSetManager: Finished task 0.0 in stage 148.0 (TID 366) in 520 ms on localhost (executor driver) (1/6)
17/12/23 18:54:32 INFO Executor: Running task 1.0 in stage 148.0 (TID 367)
17/12/23 18:54:32 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:54:32 INFO Executor: Finished task 1.0 in stage 148.0 (TID 367). 1238 bytes result sent to driver
17/12/23 18:54:32 INFO TaskSetManager: Starting task 2.0 in stage 148.0 (TID 368, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:32 INFO TaskSetManager: Finished task 1.0 in stage 148.0 (TID 367) in 524 ms on localhost (executor driver) (2/6)
17/12/23 18:54:32 INFO Executor: Running task 2.0 in stage 148.0 (TID 368)
17/12/23 18:54:32 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:54:33 INFO Executor: Finished task 2.0 in stage 148.0 (TID 368). 1238 bytes result sent to driver
17/12/23 18:54:33 INFO TaskSetManager: Starting task 3.0 in stage 148.0 (TID 369, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:33 INFO Executor: Running task 3.0 in stage 148.0 (TID 369)
17/12/23 18:54:33 INFO TaskSetManager: Finished task 2.0 in stage 148.0 (TID 368) in 508 ms on localhost (executor driver) (3/6)
17/12/23 18:54:33 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:54:33 INFO Executor: Finished task 3.0 in stage 148.0 (TID 369). 1325 bytes result sent to driver
17/12/23 18:54:33 INFO TaskSetManager: Starting task 4.0 in stage 148.0 (TID 370, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:33 INFO Executor: Running task 4.0 in stage 148.0 (TID 370)
17/12/23 18:54:33 INFO TaskSetManager: Finished task 3.0 in stage 148.0 (TID 369) in 502 ms on localhost (executor driver) (4/6)
17/12/23 18:54:33 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:54:33 INFO BlockManagerInfo: Removed broadcast_60_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:33 INFO BlockManagerInfo: Removed broadcast_61_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:33 INFO ContextCleaner: Cleaned shuffle 31
17/12/23 18:54:34 INFO Executor: Finished task 4.0 in stage 148.0 (TID 370). 1311 bytes result sent to driver
17/12/23 18:54:34 INFO TaskSetManager: Starting task 5.0 in stage 148.0 (TID 371, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:34 INFO TaskSetManager: Finished task 4.0 in stage 148.0 (TID 370) in 533 ms on localhost (executor driver) (5/6)
17/12/23 18:54:34 INFO Executor: Running task 5.0 in stage 148.0 (TID 371)
17/12/23 18:54:34 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:54:34 INFO Executor: Finished task 5.0 in stage 148.0 (TID 371). 1325 bytes result sent to driver
17/12/23 18:54:34 INFO TaskSetManager: Finished task 5.0 in stage 148.0 (TID 371) in 509 ms on localhost (executor driver) (6/6)
17/12/23 18:54:34 INFO TaskSchedulerImpl: Removed TaskSet 148.0, whose tasks have all completed, from pool 
17/12/23 18:54:34 INFO DAGScheduler: ShuffleMapStage 148 (map at StackOverflow.scala:186) finished in 3.094 s
17/12/23 18:54:34 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:54:34 INFO DAGScheduler: running: Set()
17/12/23 18:54:34 INFO DAGScheduler: waiting: Set(ResultStage 149)
17/12/23 18:54:34 INFO DAGScheduler: failed: Set()
17/12/23 18:54:34 INFO DAGScheduler: Submitting ResultStage 149 (MapPartitionsRDD[104] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:54:34 INFO MemoryStore: Block broadcast_63 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:54:34 INFO MemoryStore: Block broadcast_63_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:54:34 INFO BlockManagerInfo: Added broadcast_63_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:34 INFO SparkContext: Created broadcast 63 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:34 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 149 (MapPartitionsRDD[104] at mapValues at StackOverflow.scala:190)
17/12/23 18:54:34 INFO TaskSchedulerImpl: Adding task set 149.0 with 6 tasks
17/12/23 18:54:34 INFO TaskSetManager: Starting task 0.0 in stage 149.0 (TID 372, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:54:34 INFO Executor: Running task 0.0 in stage 149.0 (TID 372)
17/12/23 18:54:34 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:34 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:35 INFO Executor: Finished task 0.0 in stage 149.0 (TID 372). 2072 bytes result sent to driver
17/12/23 18:54:35 INFO TaskSetManager: Starting task 1.0 in stage 149.0 (TID 373, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:54:35 INFO Executor: Running task 1.0 in stage 149.0 (TID 373)
17/12/23 18:54:35 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:35 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:35 INFO TaskSetManager: Finished task 0.0 in stage 149.0 (TID 372) in 483 ms on localhost (executor driver) (1/6)
17/12/23 18:54:35 INFO Executor: Finished task 1.0 in stage 149.0 (TID 373). 2162 bytes result sent to driver
17/12/23 18:54:35 INFO TaskSetManager: Starting task 2.0 in stage 149.0 (TID 374, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:54:35 INFO Executor: Running task 2.0 in stage 149.0 (TID 374)
17/12/23 18:54:35 INFO TaskSetManager: Finished task 1.0 in stage 149.0 (TID 373) in 724 ms on localhost (executor driver) (2/6)
17/12/23 18:54:35 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:35 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:35 INFO Executor: Finished task 2.0 in stage 149.0 (TID 374). 1985 bytes result sent to driver
17/12/23 18:54:35 INFO TaskSetManager: Starting task 3.0 in stage 149.0 (TID 375, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:54:35 INFO Executor: Running task 3.0 in stage 149.0 (TID 375)
17/12/23 18:54:35 INFO TaskSetManager: Finished task 2.0 in stage 149.0 (TID 374) in 33 ms on localhost (executor driver) (3/6)
17/12/23 18:54:35 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:35 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:36 INFO Executor: Finished task 3.0 in stage 149.0 (TID 375). 2040 bytes result sent to driver
17/12/23 18:54:36 INFO TaskSetManager: Starting task 4.0 in stage 149.0 (TID 376, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:54:36 INFO Executor: Running task 4.0 in stage 149.0 (TID 376)
17/12/23 18:54:36 INFO TaskSetManager: Finished task 3.0 in stage 149.0 (TID 375) in 279 ms on localhost (executor driver) (4/6)
17/12/23 18:54:36 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:36 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:36 INFO Executor: Finished task 4.0 in stage 149.0 (TID 376). 1953 bytes result sent to driver
17/12/23 18:54:36 INFO TaskSetManager: Starting task 5.0 in stage 149.0 (TID 377, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:54:36 INFO Executor: Running task 5.0 in stage 149.0 (TID 377)
17/12/23 18:54:36 INFO TaskSetManager: Finished task 4.0 in stage 149.0 (TID 376) in 375 ms on localhost (executor driver) (5/6)
17/12/23 18:54:36 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:36 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:37 INFO Executor: Finished task 5.0 in stage 149.0 (TID 377). 2040 bytes result sent to driver
17/12/23 18:54:37 INFO TaskSetManager: Finished task 5.0 in stage 149.0 (TID 377) in 973 ms on localhost (executor driver) (6/6)
17/12/23 18:54:37 INFO TaskSchedulerImpl: Removed TaskSet 149.0, whose tasks have all completed, from pool 
17/12/23 18:54:37 INFO DAGScheduler: ResultStage 149 (collect at StackOverflow.scala:191) finished in 2.860 s
17/12/23 18:54:37 INFO DAGScheduler: Job 29 finished: collect at StackOverflow.scala:191, took 5.967330 s
Iteration: 29
  * current distance: 31273.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2179) ==>             (0,2203)    distance:      576
                (0,415) ==>              (0,423)    distance:       64
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,270) ==>         (150000,277)    distance:       49
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,104) ==>         (300000,105)    distance:        1
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,270) ==>          (50000,278)    distance:       64
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,85) ==>          (200000,87)    distance:        4
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,477) ==>         (200000,483)    distance:       36
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,163) ==>         (350000,168)    distance:       25
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,758) ==>         (350000,780)    distance:      484
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,903) ==>         (100000,919)    distance:      256
           (100000,130) ==>         (100000,133)    distance:        9
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1104) ==>        (250000,1276)    distance:    29584
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,145) ==>         (250000,156)    distance:      121
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:54:37 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:54:37 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:54:37 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:54:37 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:54:37 INFO DAGScheduler: Registering RDD 105 (map at StackOverflow.scala:186)
17/12/23 18:54:37 INFO DAGScheduler: Got job 30 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:54:37 INFO DAGScheduler: Final stage: ResultStage 154 (collect at StackOverflow.scala:191)
17/12/23 18:54:37 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 153)
17/12/23 18:54:37 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 153)
17/12/23 18:54:37 INFO DAGScheduler: Submitting ShuffleMapStage 153 (MapPartitionsRDD[105] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:54:37 INFO MemoryStore: Block broadcast_64 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:54:37 INFO MemoryStore: Block broadcast_64_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:54:37 INFO BlockManagerInfo: Added broadcast_64_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:37 INFO SparkContext: Created broadcast 64 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:37 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 153 (MapPartitionsRDD[105] at map at StackOverflow.scala:186)
17/12/23 18:54:37 INFO TaskSchedulerImpl: Adding task set 153.0 with 6 tasks
17/12/23 18:54:37 INFO TaskSetManager: Starting task 0.0 in stage 153.0 (TID 378, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:37 INFO Executor: Running task 0.0 in stage 153.0 (TID 378)
17/12/23 18:54:37 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:54:38 INFO Executor: Finished task 0.0 in stage 153.0 (TID 378). 1325 bytes result sent to driver
17/12/23 18:54:38 INFO TaskSetManager: Starting task 1.0 in stage 153.0 (TID 379, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:38 INFO Executor: Running task 1.0 in stage 153.0 (TID 379)
17/12/23 18:54:38 INFO TaskSetManager: Finished task 0.0 in stage 153.0 (TID 378) in 527 ms on localhost (executor driver) (1/6)
17/12/23 18:54:38 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:54:38 INFO Executor: Finished task 1.0 in stage 153.0 (TID 379). 1325 bytes result sent to driver
17/12/23 18:54:38 INFO TaskSetManager: Starting task 2.0 in stage 153.0 (TID 380, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:38 INFO Executor: Running task 2.0 in stage 153.0 (TID 380)
17/12/23 18:54:38 INFO TaskSetManager: Finished task 1.0 in stage 153.0 (TID 379) in 505 ms on localhost (executor driver) (2/6)
17/12/23 18:54:38 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:54:39 INFO Executor: Finished task 2.0 in stage 153.0 (TID 380). 1238 bytes result sent to driver
17/12/23 18:54:39 INFO TaskSetManager: Starting task 3.0 in stage 153.0 (TID 381, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:39 INFO Executor: Running task 3.0 in stage 153.0 (TID 381)
17/12/23 18:54:39 INFO TaskSetManager: Finished task 2.0 in stage 153.0 (TID 380) in 500 ms on localhost (executor driver) (3/6)
17/12/23 18:54:39 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:54:39 INFO Executor: Finished task 3.0 in stage 153.0 (TID 381). 1325 bytes result sent to driver
17/12/23 18:54:39 INFO TaskSetManager: Starting task 4.0 in stage 153.0 (TID 382, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:39 INFO Executor: Running task 4.0 in stage 153.0 (TID 382)
17/12/23 18:54:39 INFO TaskSetManager: Finished task 3.0 in stage 153.0 (TID 381) in 500 ms on localhost (executor driver) (4/6)
17/12/23 18:54:39 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:54:40 INFO Executor: Finished task 4.0 in stage 153.0 (TID 382). 1325 bytes result sent to driver
17/12/23 18:54:40 INFO TaskSetManager: Starting task 5.0 in stage 153.0 (TID 383, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:40 INFO Executor: Running task 5.0 in stage 153.0 (TID 383)
17/12/23 18:54:40 INFO TaskSetManager: Finished task 4.0 in stage 153.0 (TID 382) in 525 ms on localhost (executor driver) (5/6)
17/12/23 18:54:40 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:54:40 INFO Executor: Finished task 5.0 in stage 153.0 (TID 383). 1325 bytes result sent to driver
17/12/23 18:54:40 INFO TaskSetManager: Finished task 5.0 in stage 153.0 (TID 383) in 517 ms on localhost (executor driver) (6/6)
17/12/23 18:54:40 INFO TaskSchedulerImpl: Removed TaskSet 153.0, whose tasks have all completed, from pool 
17/12/23 18:54:40 INFO DAGScheduler: ShuffleMapStage 153 (map at StackOverflow.scala:186) finished in 3.074 s
17/12/23 18:54:40 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:54:40 INFO DAGScheduler: running: Set()
17/12/23 18:54:40 INFO DAGScheduler: waiting: Set(ResultStage 154)
17/12/23 18:54:40 INFO DAGScheduler: failed: Set()
17/12/23 18:54:40 INFO DAGScheduler: Submitting ResultStage 154 (MapPartitionsRDD[107] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:54:40 INFO MemoryStore: Block broadcast_65 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:54:40 INFO MemoryStore: Block broadcast_65_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:54:40 INFO BlockManagerInfo: Added broadcast_65_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:40 INFO SparkContext: Created broadcast 65 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:40 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 154 (MapPartitionsRDD[107] at mapValues at StackOverflow.scala:190)
17/12/23 18:54:40 INFO TaskSchedulerImpl: Adding task set 154.0 with 6 tasks
17/12/23 18:54:40 INFO TaskSetManager: Starting task 0.0 in stage 154.0 (TID 384, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:54:40 INFO Executor: Running task 0.0 in stage 154.0 (TID 384)
17/12/23 18:54:40 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:40 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:41 INFO Executor: Finished task 0.0 in stage 154.0 (TID 384). 2072 bytes result sent to driver
17/12/23 18:54:41 INFO TaskSetManager: Starting task 1.0 in stage 154.0 (TID 385, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:54:41 INFO Executor: Running task 1.0 in stage 154.0 (TID 385)
17/12/23 18:54:41 INFO TaskSetManager: Finished task 0.0 in stage 154.0 (TID 384) in 522 ms on localhost (executor driver) (1/6)
17/12/23 18:54:41 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:41 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:41 INFO ContextCleaner: Cleaned shuffle 32
17/12/23 18:54:41 INFO BlockManagerInfo: Removed broadcast_62_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:41 INFO BlockManagerInfo: Removed broadcast_63_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:41 INFO BlockManagerInfo: Removed broadcast_64_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:41 INFO Executor: Finished task 1.0 in stage 154.0 (TID 385). 2145 bytes result sent to driver
17/12/23 18:54:41 INFO TaskSetManager: Starting task 2.0 in stage 154.0 (TID 386, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:54:41 INFO Executor: Running task 2.0 in stage 154.0 (TID 386)
17/12/23 18:54:41 INFO TaskSetManager: Finished task 1.0 in stage 154.0 (TID 385) in 752 ms on localhost (executor driver) (2/6)
17/12/23 18:54:41 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:41 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:41 INFO Executor: Finished task 2.0 in stage 154.0 (TID 386). 1985 bytes result sent to driver
17/12/23 18:54:41 INFO TaskSetManager: Starting task 3.0 in stage 154.0 (TID 387, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:54:41 INFO Executor: Running task 3.0 in stage 154.0 (TID 387)
17/12/23 18:54:41 INFO TaskSetManager: Finished task 2.0 in stage 154.0 (TID 386) in 28 ms on localhost (executor driver) (3/6)
17/12/23 18:54:41 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:41 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:42 INFO Executor: Finished task 3.0 in stage 154.0 (TID 387). 1953 bytes result sent to driver
17/12/23 18:54:42 INFO TaskSetManager: Starting task 4.0 in stage 154.0 (TID 388, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:54:42 INFO Executor: Running task 4.0 in stage 154.0 (TID 388)
17/12/23 18:54:42 INFO TaskSetManager: Finished task 3.0 in stage 154.0 (TID 387) in 262 ms on localhost (executor driver) (4/6)
17/12/23 18:54:42 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:42 INFO Executor: Finished task 4.0 in stage 154.0 (TID 388). 2040 bytes result sent to driver
17/12/23 18:54:42 INFO TaskSetManager: Starting task 5.0 in stage 154.0 (TID 389, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:54:42 INFO Executor: Running task 5.0 in stage 154.0 (TID 389)
17/12/23 18:54:42 INFO TaskSetManager: Finished task 4.0 in stage 154.0 (TID 388) in 370 ms on localhost (executor driver) (5/6)
17/12/23 18:54:42 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
Iteration: 30
  * current distance: 16535.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2203) ==>             (0,2228)    distance:      625
                (0,423) ==>              (0,430)    distance:       49
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,277) ==>         (150000,282)    distance:       25
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,105) ==>         (300000,107)    distance:        4
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,278) ==>          (50000,284)    distance:       36
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,87) ==>          (200000,89)    distance:        4
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,483) ==>         (200000,489)    distance:       36
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,168) ==>         (350000,171)    distance:        9
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,780) ==>         (350000,788)    distance:       64
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,919) ==>         (100000,952)    distance:     1089
           (100000,133) ==>         (100000,138)    distance:       25
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1276) ==>        (250000,1396)    distance:    14400
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,156) ==>         (250000,169)    distance:      169
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:54:43 INFO Executor: Finished task 5.0 in stage 154.0 (TID 389). 2040 bytes result sent to driver
17/12/23 18:54:43 INFO TaskSetManager: Finished task 5.0 in stage 154.0 (TID 389) in 962 ms on localhost (executor driver) (6/6)
17/12/23 18:54:43 INFO TaskSchedulerImpl: Removed TaskSet 154.0, whose tasks have all completed, from pool 
17/12/23 18:54:43 INFO DAGScheduler: ResultStage 154 (collect at StackOverflow.scala:191) finished in 2.896 s
17/12/23 18:54:43 INFO DAGScheduler: Job 30 finished: collect at StackOverflow.scala:191, took 5.981587 s
17/12/23 18:54:43 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:54:43 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:54:43 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:54:43 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:54:43 INFO DAGScheduler: Registering RDD 108 (map at StackOverflow.scala:186)
17/12/23 18:54:43 INFO DAGScheduler: Got job 31 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:54:43 INFO DAGScheduler: Final stage: ResultStage 159 (collect at StackOverflow.scala:191)
17/12/23 18:54:43 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 158)
17/12/23 18:54:43 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 158)
17/12/23 18:54:43 INFO DAGScheduler: Submitting ShuffleMapStage 158 (MapPartitionsRDD[108] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:54:43 INFO MemoryStore: Block broadcast_66 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:54:43 INFO MemoryStore: Block broadcast_66_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:54:43 INFO BlockManagerInfo: Added broadcast_66_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:43 INFO SparkContext: Created broadcast 66 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:43 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 158 (MapPartitionsRDD[108] at map at StackOverflow.scala:186)
17/12/23 18:54:43 INFO TaskSchedulerImpl: Adding task set 158.0 with 6 tasks
17/12/23 18:54:43 INFO TaskSetManager: Starting task 0.0 in stage 158.0 (TID 390, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:43 INFO Executor: Running task 0.0 in stage 158.0 (TID 390)
17/12/23 18:54:43 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:54:44 INFO Executor: Finished task 0.0 in stage 158.0 (TID 390). 1325 bytes result sent to driver
17/12/23 18:54:44 INFO TaskSetManager: Starting task 1.0 in stage 158.0 (TID 391, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:44 INFO Executor: Running task 1.0 in stage 158.0 (TID 391)
17/12/23 18:54:44 INFO TaskSetManager: Finished task 0.0 in stage 158.0 (TID 390) in 527 ms on localhost (executor driver) (1/6)
17/12/23 18:54:44 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:54:44 INFO Executor: Finished task 1.0 in stage 158.0 (TID 391). 1325 bytes result sent to driver
17/12/23 18:54:44 INFO TaskSetManager: Starting task 2.0 in stage 158.0 (TID 392, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:44 INFO Executor: Running task 2.0 in stage 158.0 (TID 392)
17/12/23 18:54:44 INFO TaskSetManager: Finished task 1.0 in stage 158.0 (TID 391) in 500 ms on localhost (executor driver) (2/6)
17/12/23 18:54:44 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:54:45 INFO Executor: Finished task 2.0 in stage 158.0 (TID 392). 1325 bytes result sent to driver
17/12/23 18:54:45 INFO TaskSetManager: Starting task 3.0 in stage 158.0 (TID 393, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:45 INFO Executor: Running task 3.0 in stage 158.0 (TID 393)
17/12/23 18:54:45 INFO TaskSetManager: Finished task 2.0 in stage 158.0 (TID 392) in 487 ms on localhost (executor driver) (3/6)
17/12/23 18:54:45 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:54:45 INFO Executor: Finished task 3.0 in stage 158.0 (TID 393). 1325 bytes result sent to driver
17/12/23 18:54:45 INFO TaskSetManager: Starting task 4.0 in stage 158.0 (TID 394, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:45 INFO Executor: Running task 4.0 in stage 158.0 (TID 394)
17/12/23 18:54:45 INFO TaskSetManager: Finished task 3.0 in stage 158.0 (TID 393) in 512 ms on localhost (executor driver) (4/6)
17/12/23 18:54:45 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:54:46 INFO Executor: Finished task 4.0 in stage 158.0 (TID 394). 1325 bytes result sent to driver
17/12/23 18:54:46 INFO TaskSetManager: Starting task 5.0 in stage 158.0 (TID 395, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:46 INFO Executor: Running task 5.0 in stage 158.0 (TID 395)
17/12/23 18:54:46 INFO TaskSetManager: Finished task 4.0 in stage 158.0 (TID 394) in 514 ms on localhost (executor driver) (5/6)
17/12/23 18:54:46 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:54:46 INFO Executor: Finished task 5.0 in stage 158.0 (TID 395). 1325 bytes result sent to driver
17/12/23 18:54:46 INFO TaskSetManager: Finished task 5.0 in stage 158.0 (TID 395) in 539 ms on localhost (executor driver) (6/6)
17/12/23 18:54:46 INFO TaskSchedulerImpl: Removed TaskSet 158.0, whose tasks have all completed, from pool 
17/12/23 18:54:46 INFO DAGScheduler: ShuffleMapStage 158 (map at StackOverflow.scala:186) finished in 3.079 s
17/12/23 18:54:46 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:54:46 INFO DAGScheduler: running: Set()
17/12/23 18:54:46 INFO DAGScheduler: waiting: Set(ResultStage 159)
17/12/23 18:54:46 INFO DAGScheduler: failed: Set()
17/12/23 18:54:46 INFO DAGScheduler: Submitting ResultStage 159 (MapPartitionsRDD[110] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:54:46 INFO MemoryStore: Block broadcast_67 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:54:46 INFO MemoryStore: Block broadcast_67_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:54:46 INFO BlockManagerInfo: Added broadcast_67_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:46 INFO SparkContext: Created broadcast 67 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:46 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 159 (MapPartitionsRDD[110] at mapValues at StackOverflow.scala:190)
17/12/23 18:54:46 INFO TaskSchedulerImpl: Adding task set 159.0 with 6 tasks
17/12/23 18:54:46 INFO TaskSetManager: Starting task 0.0 in stage 159.0 (TID 396, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:54:46 INFO Executor: Running task 0.0 in stage 159.0 (TID 396)
17/12/23 18:54:46 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:46 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:47 INFO Executor: Finished task 0.0 in stage 159.0 (TID 396). 2072 bytes result sent to driver
17/12/23 18:54:47 INFO TaskSetManager: Starting task 1.0 in stage 159.0 (TID 397, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:54:47 INFO Executor: Running task 1.0 in stage 159.0 (TID 397)
17/12/23 18:54:47 INFO TaskSetManager: Finished task 0.0 in stage 159.0 (TID 396) in 502 ms on localhost (executor driver) (1/6)
17/12/23 18:54:47 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:47 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:47 INFO Executor: Finished task 1.0 in stage 159.0 (TID 397). 1985 bytes result sent to driver
17/12/23 18:54:47 INFO TaskSetManager: Starting task 2.0 in stage 159.0 (TID 398, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:54:47 INFO Executor: Running task 2.0 in stage 159.0 (TID 398)
17/12/23 18:54:47 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:47 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:47 INFO TaskSetManager: Finished task 1.0 in stage 159.0 (TID 397) in 729 ms on localhost (executor driver) (2/6)
17/12/23 18:54:47 INFO Executor: Finished task 2.0 in stage 159.0 (TID 398). 2072 bytes result sent to driver
17/12/23 18:54:47 INFO TaskSetManager: Starting task 3.0 in stage 159.0 (TID 399, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:54:47 INFO Executor: Running task 3.0 in stage 159.0 (TID 399)
17/12/23 18:54:47 INFO TaskSetManager: Finished task 2.0 in stage 159.0 (TID 398) in 32 ms on localhost (executor driver) (3/6)
17/12/23 18:54:47 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:47 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:48 INFO Executor: Finished task 3.0 in stage 159.0 (TID 399). 2040 bytes result sent to driver
17/12/23 18:54:48 INFO TaskSetManager: Starting task 4.0 in stage 159.0 (TID 400, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:54:48 INFO Executor: Running task 4.0 in stage 159.0 (TID 400)
17/12/23 18:54:48 INFO TaskSetManager: Finished task 3.0 in stage 159.0 (TID 399) in 268 ms on localhost (executor driver) (4/6)
17/12/23 18:54:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:48 INFO ContextCleaner: Cleaned shuffle 33
17/12/23 18:54:48 INFO BlockManagerInfo: Removed broadcast_65_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:48 INFO BlockManagerInfo: Removed broadcast_66_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:48 INFO Executor: Finished task 4.0 in stage 159.0 (TID 400). 2113 bytes result sent to driver
17/12/23 18:54:48 INFO TaskSetManager: Starting task 5.0 in stage 159.0 (TID 401, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:54:48 INFO TaskSetManager: Finished task 4.0 in stage 159.0 (TID 400) in 442 ms on localhost (executor driver) (5/6)
17/12/23 18:54:48 INFO Executor: Running task 5.0 in stage 159.0 (TID 401)
17/12/23 18:54:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 3 ms
17/12/23 18:54:49 INFO Executor: Finished task 5.0 in stage 159.0 (TID 401). 1953 bytes result sent to driver
17/12/23 18:54:49 INFO TaskSetManager: Finished task 5.0 in stage 159.0 (TID 401) in 954 ms on localhost (executor driver) (6/6)
17/12/23 18:54:49 INFO TaskSchedulerImpl: Removed TaskSet 159.0, whose tasks have all completed, from pool 
17/12/23 18:54:49 INFO DAGScheduler: ResultStage 159 (collect at StackOverflow.scala:191) finished in 2.913 s
17/12/23 18:54:49 INFO DAGScheduler: Job 31 finished: collect at StackOverflow.scala:191, took 6.007066 s
Iteration: 31
  * current distance: 26717.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2228) ==>             (0,2228)    distance:        0
                (0,430) ==>              (0,433)    distance:        9
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,282) ==>         (150000,285)    distance:        9
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,107) ==>         (300000,108)    distance:        1
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,284) ==>          (50000,292)    distance:       64
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,89) ==>          (200000,90)    distance:        1
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,489) ==>         (200000,492)    distance:        9
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,171) ==>         (350000,173)    distance:        4
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,788) ==>         (350000,788)    distance:        0
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,952) ==>         (100000,990)    distance:     1444
           (100000,138) ==>         (100000,142)    distance:       16
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1396) ==>        (250000,1554)    distance:    24964
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,169) ==>         (250000,183)    distance:      196
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:54:49 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:54:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:54:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:54:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:54:49 INFO DAGScheduler: Registering RDD 111 (map at StackOverflow.scala:186)
17/12/23 18:54:49 INFO DAGScheduler: Got job 32 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:54:49 INFO DAGScheduler: Final stage: ResultStage 164 (collect at StackOverflow.scala:191)
17/12/23 18:54:49 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 163)
17/12/23 18:54:49 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 163)
17/12/23 18:54:49 INFO DAGScheduler: Submitting ShuffleMapStage 163 (MapPartitionsRDD[111] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:54:49 INFO MemoryStore: Block broadcast_68 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:54:49 INFO MemoryStore: Block broadcast_68_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:54:49 INFO BlockManagerInfo: Added broadcast_68_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:49 INFO SparkContext: Created broadcast 68 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:49 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 163 (MapPartitionsRDD[111] at map at StackOverflow.scala:186)
17/12/23 18:54:49 INFO TaskSchedulerImpl: Adding task set 163.0 with 6 tasks
17/12/23 18:54:49 INFO TaskSetManager: Starting task 0.0 in stage 163.0 (TID 402, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:49 INFO Executor: Running task 0.0 in stage 163.0 (TID 402)
17/12/23 18:54:49 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:54:50 INFO Executor: Finished task 0.0 in stage 163.0 (TID 402). 1325 bytes result sent to driver
17/12/23 18:54:50 INFO TaskSetManager: Starting task 1.0 in stage 163.0 (TID 403, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:50 INFO Executor: Running task 1.0 in stage 163.0 (TID 403)
17/12/23 18:54:50 INFO TaskSetManager: Finished task 0.0 in stage 163.0 (TID 402) in 519 ms on localhost (executor driver) (1/6)
17/12/23 18:54:50 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:54:50 INFO Executor: Finished task 1.0 in stage 163.0 (TID 403). 1325 bytes result sent to driver
17/12/23 18:54:50 INFO TaskSetManager: Starting task 2.0 in stage 163.0 (TID 404, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:50 INFO Executor: Running task 2.0 in stage 163.0 (TID 404)
17/12/23 18:54:50 INFO TaskSetManager: Finished task 1.0 in stage 163.0 (TID 403) in 500 ms on localhost (executor driver) (2/6)
17/12/23 18:54:50 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:54:51 INFO Executor: Finished task 2.0 in stage 163.0 (TID 404). 1325 bytes result sent to driver
17/12/23 18:54:51 INFO TaskSetManager: Starting task 3.0 in stage 163.0 (TID 405, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:51 INFO Executor: Running task 3.0 in stage 163.0 (TID 405)
17/12/23 18:54:51 INFO TaskSetManager: Finished task 2.0 in stage 163.0 (TID 404) in 505 ms on localhost (executor driver) (3/6)
17/12/23 18:54:51 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:54:51 INFO Executor: Finished task 3.0 in stage 163.0 (TID 405). 1325 bytes result sent to driver
17/12/23 18:54:51 INFO TaskSetManager: Starting task 4.0 in stage 163.0 (TID 406, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:51 INFO Executor: Running task 4.0 in stage 163.0 (TID 406)
17/12/23 18:54:51 INFO TaskSetManager: Finished task 3.0 in stage 163.0 (TID 405) in 492 ms on localhost (executor driver) (4/6)
17/12/23 18:54:51 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:54:52 INFO Executor: Finished task 4.0 in stage 163.0 (TID 406). 1325 bytes result sent to driver
17/12/23 18:54:52 INFO TaskSetManager: Starting task 5.0 in stage 163.0 (TID 407, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:52 INFO Executor: Running task 5.0 in stage 163.0 (TID 407)
17/12/23 18:54:52 INFO TaskSetManager: Finished task 4.0 in stage 163.0 (TID 406) in 525 ms on localhost (executor driver) (5/6)
17/12/23 18:54:52 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:54:52 INFO Executor: Finished task 5.0 in stage 163.0 (TID 407). 1238 bytes result sent to driver
17/12/23 18:54:52 INFO TaskSetManager: Finished task 5.0 in stage 163.0 (TID 407) in 498 ms on localhost (executor driver) (6/6)
17/12/23 18:54:52 INFO TaskSchedulerImpl: Removed TaskSet 163.0, whose tasks have all completed, from pool 
17/12/23 18:54:52 INFO DAGScheduler: ShuffleMapStage 163 (map at StackOverflow.scala:186) finished in 3.039 s
17/12/23 18:54:52 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:54:52 INFO DAGScheduler: running: Set()
17/12/23 18:54:52 INFO DAGScheduler: waiting: Set(ResultStage 164)
17/12/23 18:54:52 INFO DAGScheduler: failed: Set()
17/12/23 18:54:52 INFO DAGScheduler: Submitting ResultStage 164 (MapPartitionsRDD[113] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:54:52 INFO MemoryStore: Block broadcast_69 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:54:52 INFO MemoryStore: Block broadcast_69_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:54:52 INFO BlockManagerInfo: Added broadcast_69_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:52 INFO SparkContext: Created broadcast 69 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:52 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 164 (MapPartitionsRDD[113] at mapValues at StackOverflow.scala:190)
17/12/23 18:54:52 INFO TaskSchedulerImpl: Adding task set 164.0 with 6 tasks
17/12/23 18:54:52 INFO TaskSetManager: Starting task 0.0 in stage 164.0 (TID 408, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:54:52 INFO Executor: Running task 0.0 in stage 164.0 (TID 408)
17/12/23 18:54:52 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:52 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:53 INFO Executor: Finished task 0.0 in stage 164.0 (TID 408). 1985 bytes result sent to driver
17/12/23 18:54:53 INFO TaskSetManager: Starting task 1.0 in stage 164.0 (TID 409, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:54:53 INFO Executor: Running task 1.0 in stage 164.0 (TID 409)
17/12/23 18:54:53 INFO TaskSetManager: Finished task 0.0 in stage 164.0 (TID 408) in 502 ms on localhost (executor driver) (1/6)
17/12/23 18:54:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:53 INFO Executor: Finished task 1.0 in stage 164.0 (TID 409). 1985 bytes result sent to driver
17/12/23 18:54:53 INFO TaskSetManager: Starting task 2.0 in stage 164.0 (TID 410, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:54:53 INFO Executor: Running task 2.0 in stage 164.0 (TID 410)
17/12/23 18:54:53 INFO TaskSetManager: Finished task 1.0 in stage 164.0 (TID 409) in 698 ms on localhost (executor driver) (2/6)
17/12/23 18:54:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:53 INFO Executor: Finished task 2.0 in stage 164.0 (TID 410). 2162 bytes result sent to driver
17/12/23 18:54:53 INFO TaskSetManager: Starting task 3.0 in stage 164.0 (TID 411, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:54:53 INFO Executor: Running task 3.0 in stage 164.0 (TID 411)
17/12/23 18:54:53 INFO TaskSetManager: Finished task 2.0 in stage 164.0 (TID 410) in 35 ms on localhost (executor driver) (3/6)
17/12/23 18:54:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:54 INFO Executor: Finished task 3.0 in stage 164.0 (TID 411). 1953 bytes result sent to driver
17/12/23 18:54:54 INFO TaskSetManager: Starting task 4.0 in stage 164.0 (TID 412, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:54:54 INFO Executor: Running task 4.0 in stage 164.0 (TID 412)
17/12/23 18:54:54 INFO TaskSetManager: Finished task 3.0 in stage 164.0 (TID 411) in 265 ms on localhost (executor driver) (4/6)
17/12/23 18:54:54 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:54 INFO Executor: Finished task 4.0 in stage 164.0 (TID 412). 2130 bytes result sent to driver
17/12/23 18:54:54 INFO TaskSetManager: Starting task 5.0 in stage 164.0 (TID 413, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:54:54 INFO Executor: Running task 5.0 in stage 164.0 (TID 413)
17/12/23 18:54:54 INFO TaskSetManager: Finished task 4.0 in stage 164.0 (TID 412) in 393 ms on localhost (executor driver) (5/6)
17/12/23 18:54:54 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:55 INFO ContextCleaner: Cleaned shuffle 34
17/12/23 18:54:55 INFO BlockManagerInfo: Removed broadcast_67_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:55 INFO BlockManagerInfo: Removed broadcast_68_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:55 INFO Executor: Finished task 5.0 in stage 164.0 (TID 413). 2113 bytes result sent to driver
17/12/23 18:54:55 INFO TaskSetManager: Finished task 5.0 in stage 164.0 (TID 413) in 1013 ms on localhost (executor driver) (6/6)
17/12/23 18:54:55 INFO TaskSchedulerImpl: Removed TaskSet 164.0, whose tasks have all completed, from pool 
17/12/23 18:54:55 INFO DAGScheduler: ResultStage 164 (collect at StackOverflow.scala:191) finished in 2.906 s
17/12/23 18:54:55 INFO DAGScheduler: Job 32 finished: collect at StackOverflow.scala:191, took 5.955112 s
Iteration: 32
  * current distance: 5121.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2228) ==>             (0,2228)    distance:        0
                (0,433) ==>              (0,435)    distance:        4
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,285) ==>         (150000,287)    distance:        4
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,292) ==>          (50000,299)    distance:       49
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,90) ==>          (200000,90)    distance:        0
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,492) ==>         (200000,495)    distance:        9
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,173) ==>         (350000,176)    distance:        9
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,788) ==>         (350000,788)    distance:        0
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
           (100000,990) ==>        (100000,1060)    distance:     4900
           (100000,142) ==>         (100000,147)    distance:       25
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1554) ==>        (250000,1554)    distance:        0
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,183) ==>         (250000,194)    distance:      121
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:54:55 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:54:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:54:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:54:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:54:55 INFO DAGScheduler: Registering RDD 114 (map at StackOverflow.scala:186)
17/12/23 18:54:55 INFO DAGScheduler: Got job 33 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:54:55 INFO DAGScheduler: Final stage: ResultStage 169 (collect at StackOverflow.scala:191)
17/12/23 18:54:55 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 168)
17/12/23 18:54:55 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 168)
17/12/23 18:54:55 INFO DAGScheduler: Submitting ShuffleMapStage 168 (MapPartitionsRDD[114] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:54:55 INFO MemoryStore: Block broadcast_70 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:54:55 INFO MemoryStore: Block broadcast_70_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:54:55 INFO BlockManagerInfo: Added broadcast_70_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:54:55 INFO SparkContext: Created broadcast 70 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:55 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 168 (MapPartitionsRDD[114] at map at StackOverflow.scala:186)
17/12/23 18:54:55 INFO TaskSchedulerImpl: Adding task set 168.0 with 6 tasks
17/12/23 18:54:55 INFO TaskSetManager: Starting task 0.0 in stage 168.0 (TID 414, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:55 INFO Executor: Running task 0.0 in stage 168.0 (TID 414)
17/12/23 18:54:55 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:54:56 INFO Executor: Finished task 0.0 in stage 168.0 (TID 414). 1325 bytes result sent to driver
17/12/23 18:54:56 INFO TaskSetManager: Starting task 1.0 in stage 168.0 (TID 415, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:56 INFO Executor: Running task 1.0 in stage 168.0 (TID 415)
17/12/23 18:54:56 INFO TaskSetManager: Finished task 0.0 in stage 168.0 (TID 414) in 515 ms on localhost (executor driver) (1/6)
17/12/23 18:54:56 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:54:56 INFO Executor: Finished task 1.0 in stage 168.0 (TID 415). 1325 bytes result sent to driver
17/12/23 18:54:56 INFO TaskSetManager: Starting task 2.0 in stage 168.0 (TID 416, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:56 INFO Executor: Running task 2.0 in stage 168.0 (TID 416)
17/12/23 18:54:56 INFO TaskSetManager: Finished task 1.0 in stage 168.0 (TID 415) in 519 ms on localhost (executor driver) (2/6)
17/12/23 18:54:56 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:54:57 INFO Executor: Finished task 2.0 in stage 168.0 (TID 416). 1238 bytes result sent to driver
17/12/23 18:54:57 INFO TaskSetManager: Starting task 3.0 in stage 168.0 (TID 417, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:57 INFO Executor: Running task 3.0 in stage 168.0 (TID 417)
17/12/23 18:54:57 INFO TaskSetManager: Finished task 2.0 in stage 168.0 (TID 416) in 505 ms on localhost (executor driver) (3/6)
17/12/23 18:54:57 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:54:57 INFO Executor: Finished task 3.0 in stage 168.0 (TID 417). 1325 bytes result sent to driver
17/12/23 18:54:57 INFO TaskSetManager: Starting task 4.0 in stage 168.0 (TID 418, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:57 INFO Executor: Running task 4.0 in stage 168.0 (TID 418)
17/12/23 18:54:57 INFO TaskSetManager: Finished task 3.0 in stage 168.0 (TID 417) in 532 ms on localhost (executor driver) (4/6)
17/12/23 18:54:57 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:54:58 INFO Executor: Finished task 4.0 in stage 168.0 (TID 418). 1325 bytes result sent to driver
17/12/23 18:54:58 INFO TaskSetManager: Starting task 5.0 in stage 168.0 (TID 419, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:54:58 INFO Executor: Running task 5.0 in stage 168.0 (TID 419)
17/12/23 18:54:58 INFO TaskSetManager: Finished task 4.0 in stage 168.0 (TID 418) in 511 ms on localhost (executor driver) (5/6)
17/12/23 18:54:58 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:54:58 INFO Executor: Finished task 5.0 in stage 168.0 (TID 419). 1325 bytes result sent to driver
17/12/23 18:54:58 INFO TaskSetManager: Finished task 5.0 in stage 168.0 (TID 419) in 510 ms on localhost (executor driver) (6/6)
17/12/23 18:54:58 INFO TaskSchedulerImpl: Removed TaskSet 168.0, whose tasks have all completed, from pool 
17/12/23 18:54:58 INFO DAGScheduler: ShuffleMapStage 168 (map at StackOverflow.scala:186) finished in 3.092 s
17/12/23 18:54:58 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:54:58 INFO DAGScheduler: running: Set()
17/12/23 18:54:58 INFO DAGScheduler: waiting: Set(ResultStage 169)
17/12/23 18:54:58 INFO DAGScheduler: failed: Set()
17/12/23 18:54:58 INFO DAGScheduler: Submitting ResultStage 169 (MapPartitionsRDD[116] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:54:58 INFO MemoryStore: Block broadcast_71 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:54:58 INFO MemoryStore: Block broadcast_71_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:54:58 INFO BlockManagerInfo: Added broadcast_71_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:54:58 INFO SparkContext: Created broadcast 71 from broadcast at DAGScheduler.scala:996
17/12/23 18:54:58 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 169 (MapPartitionsRDD[116] at mapValues at StackOverflow.scala:190)
17/12/23 18:54:58 INFO TaskSchedulerImpl: Adding task set 169.0 with 6 tasks
17/12/23 18:54:58 INFO TaskSetManager: Starting task 0.0 in stage 169.0 (TID 420, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:54:58 INFO Executor: Running task 0.0 in stage 169.0 (TID 420)
17/12/23 18:54:58 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:58 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:59 INFO Executor: Finished task 0.0 in stage 169.0 (TID 420). 1985 bytes result sent to driver
17/12/23 18:54:59 INFO TaskSetManager: Starting task 1.0 in stage 169.0 (TID 421, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:54:59 INFO TaskSetManager: Finished task 0.0 in stage 169.0 (TID 420) in 532 ms on localhost (executor driver) (1/6)
17/12/23 18:54:59 INFO Executor: Running task 1.0 in stage 169.0 (TID 421)
17/12/23 18:54:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:59 INFO Executor: Finished task 1.0 in stage 169.0 (TID 421). 1985 bytes result sent to driver
17/12/23 18:54:59 INFO TaskSetManager: Starting task 2.0 in stage 169.0 (TID 422, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:54:59 INFO TaskSetManager: Finished task 1.0 in stage 169.0 (TID 421) in 747 ms on localhost (executor driver) (2/6)
17/12/23 18:54:59 INFO Executor: Running task 2.0 in stage 169.0 (TID 422)
17/12/23 18:54:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:54:59 INFO Executor: Finished task 2.0 in stage 169.0 (TID 422). 1985 bytes result sent to driver
17/12/23 18:54:59 INFO TaskSetManager: Starting task 3.0 in stage 169.0 (TID 423, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:54:59 INFO Executor: Running task 3.0 in stage 169.0 (TID 423)
17/12/23 18:54:59 INFO TaskSetManager: Finished task 2.0 in stage 169.0 (TID 422) in 40 ms on localhost (executor driver) (3/6)
17/12/23 18:54:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:54:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:00 INFO Executor: Finished task 3.0 in stage 169.0 (TID 423). 1953 bytes result sent to driver
17/12/23 18:55:00 INFO TaskSetManager: Starting task 4.0 in stage 169.0 (TID 424, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:55:00 INFO Executor: Running task 4.0 in stage 169.0 (TID 424)
17/12/23 18:55:00 INFO TaskSetManager: Finished task 3.0 in stage 169.0 (TID 423) in 237 ms on localhost (executor driver) (4/6)
17/12/23 18:55:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:00 INFO Executor: Finished task 4.0 in stage 169.0 (TID 424). 2040 bytes result sent to driver
17/12/23 18:55:00 INFO TaskSetManager: Starting task 5.0 in stage 169.0 (TID 425, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:55:00 INFO Executor: Running task 5.0 in stage 169.0 (TID 425)
17/12/23 18:55:00 INFO TaskSetManager: Finished task 4.0 in stage 169.0 (TID 424) in 378 ms on localhost (executor driver) (5/6)
17/12/23 18:55:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
Iteration: 33
  * current distance: 3589.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2228) ==>             (0,2228)    distance:        0
                (0,435) ==>              (0,439)    distance:       16
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,287) ==>         (150000,290)    distance:        9
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,299) ==>          (50000,307)    distance:       64
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,90) ==>          (200000,90)    distance:        0
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,495) ==>         (200000,497)    distance:        4
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,176) ==>         (350000,178)    distance:        4
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,788) ==>         (350000,804)    distance:      256
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
          (100000,1060) ==>        (100000,1116)    distance:     3136
           (100000,147) ==>         (100000,153)    distance:       36
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1554) ==>        (250000,1554)    distance:        0
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,194) ==>         (250000,202)    distance:       64
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:55:01 INFO Executor: Finished task 5.0 in stage 169.0 (TID 425). 2040 bytes result sent to driver
17/12/23 18:55:01 INFO TaskSetManager: Finished task 5.0 in stage 169.0 (TID 425) in 942 ms on localhost (executor driver) (6/6)
17/12/23 18:55:01 INFO TaskSchedulerImpl: Removed TaskSet 169.0, whose tasks have all completed, from pool 
17/12/23 18:55:01 INFO DAGScheduler: ResultStage 169 (collect at StackOverflow.scala:191) finished in 2.872 s
17/12/23 18:55:01 INFO DAGScheduler: Job 33 finished: collect at StackOverflow.scala:191, took 5.970934 s
17/12/23 18:55:01 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:55:01 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:55:01 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:55:01 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:55:01 INFO DAGScheduler: Registering RDD 117 (map at StackOverflow.scala:186)
17/12/23 18:55:01 INFO DAGScheduler: Got job 34 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:55:01 INFO DAGScheduler: Final stage: ResultStage 174 (collect at StackOverflow.scala:191)
17/12/23 18:55:01 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 173)
17/12/23 18:55:01 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 173)
17/12/23 18:55:01 INFO DAGScheduler: Submitting ShuffleMapStage 173 (MapPartitionsRDD[117] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:55:01 INFO MemoryStore: Block broadcast_72 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:55:01 INFO MemoryStore: Block broadcast_72_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:55:01 INFO BlockManagerInfo: Added broadcast_72_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:01 INFO SparkContext: Created broadcast 72 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:01 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 173 (MapPartitionsRDD[117] at map at StackOverflow.scala:186)
17/12/23 18:55:01 INFO TaskSchedulerImpl: Adding task set 173.0 with 6 tasks
17/12/23 18:55:01 INFO TaskSetManager: Starting task 0.0 in stage 173.0 (TID 426, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:01 INFO Executor: Running task 0.0 in stage 173.0 (TID 426)
17/12/23 18:55:01 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:55:02 INFO Executor: Finished task 0.0 in stage 173.0 (TID 426). 1415 bytes result sent to driver
17/12/23 18:55:02 INFO TaskSetManager: Starting task 1.0 in stage 173.0 (TID 427, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:02 INFO Executor: Running task 1.0 in stage 173.0 (TID 427)
17/12/23 18:55:02 INFO TaskSetManager: Finished task 0.0 in stage 173.0 (TID 426) in 505 ms on localhost (executor driver) (1/6)
17/12/23 18:55:02 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:55:02 INFO Executor: Finished task 1.0 in stage 173.0 (TID 427). 1325 bytes result sent to driver
17/12/23 18:55:02 INFO TaskSetManager: Starting task 2.0 in stage 173.0 (TID 428, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:02 INFO TaskSetManager: Finished task 1.0 in stage 173.0 (TID 427) in 492 ms on localhost (executor driver) (2/6)
17/12/23 18:55:02 INFO Executor: Running task 2.0 in stage 173.0 (TID 428)
17/12/23 18:55:02 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:55:02 INFO ContextCleaner: Cleaned shuffle 35
17/12/23 18:55:02 INFO BlockManagerInfo: Removed broadcast_69_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:02 INFO ContextCleaner: Cleaned shuffle 36
17/12/23 18:55:02 INFO BlockManagerInfo: Removed broadcast_70_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:02 INFO BlockManagerInfo: Removed broadcast_71_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:03 INFO Executor: Finished task 2.0 in stage 173.0 (TID 428). 1488 bytes result sent to driver
17/12/23 18:55:03 INFO TaskSetManager: Starting task 3.0 in stage 173.0 (TID 429, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:03 INFO Executor: Running task 3.0 in stage 173.0 (TID 429)
17/12/23 18:55:03 INFO TaskSetManager: Finished task 2.0 in stage 173.0 (TID 428) in 527 ms on localhost (executor driver) (3/6)
17/12/23 18:55:03 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:55:03 INFO Executor: Finished task 3.0 in stage 173.0 (TID 429). 1238 bytes result sent to driver
17/12/23 18:55:03 INFO TaskSetManager: Starting task 4.0 in stage 173.0 (TID 430, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:03 INFO Executor: Running task 4.0 in stage 173.0 (TID 430)
17/12/23 18:55:03 INFO TaskSetManager: Finished task 3.0 in stage 173.0 (TID 429) in 513 ms on localhost (executor driver) (4/6)
17/12/23 18:55:03 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:55:04 INFO Executor: Finished task 4.0 in stage 173.0 (TID 430). 1325 bytes result sent to driver
17/12/23 18:55:04 INFO TaskSetManager: Starting task 5.0 in stage 173.0 (TID 431, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:04 INFO Executor: Running task 5.0 in stage 173.0 (TID 431)
17/12/23 18:55:04 INFO TaskSetManager: Finished task 4.0 in stage 173.0 (TID 430) in 524 ms on localhost (executor driver) (5/6)
17/12/23 18:55:04 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:55:04 INFO Executor: Finished task 5.0 in stage 173.0 (TID 431). 1325 bytes result sent to driver
17/12/23 18:55:04 INFO TaskSetManager: Finished task 5.0 in stage 173.0 (TID 431) in 555 ms on localhost (executor driver) (6/6)
17/12/23 18:55:04 INFO TaskSchedulerImpl: Removed TaskSet 173.0, whose tasks have all completed, from pool 
17/12/23 18:55:04 INFO DAGScheduler: ShuffleMapStage 173 (map at StackOverflow.scala:186) finished in 3.116 s
17/12/23 18:55:04 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:55:04 INFO DAGScheduler: running: Set()
17/12/23 18:55:04 INFO DAGScheduler: waiting: Set(ResultStage 174)
17/12/23 18:55:04 INFO DAGScheduler: failed: Set()
17/12/23 18:55:04 INFO DAGScheduler: Submitting ResultStage 174 (MapPartitionsRDD[119] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:55:04 INFO MemoryStore: Block broadcast_73 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:55:04 INFO MemoryStore: Block broadcast_73_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:55:04 INFO BlockManagerInfo: Added broadcast_73_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:04 INFO SparkContext: Created broadcast 73 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:04 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 174 (MapPartitionsRDD[119] at mapValues at StackOverflow.scala:190)
17/12/23 18:55:04 INFO TaskSchedulerImpl: Adding task set 174.0 with 6 tasks
17/12/23 18:55:04 INFO TaskSetManager: Starting task 0.0 in stage 174.0 (TID 432, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:55:04 INFO Executor: Running task 0.0 in stage 174.0 (TID 432)
17/12/23 18:55:04 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:05 INFO Executor: Finished task 0.0 in stage 174.0 (TID 432). 2072 bytes result sent to driver
17/12/23 18:55:05 INFO TaskSetManager: Starting task 1.0 in stage 174.0 (TID 433, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:55:05 INFO Executor: Running task 1.0 in stage 174.0 (TID 433)
17/12/23 18:55:05 INFO TaskSetManager: Finished task 0.0 in stage 174.0 (TID 432) in 539 ms on localhost (executor driver) (1/6)
17/12/23 18:55:05 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:05 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:06 INFO Executor: Finished task 1.0 in stage 174.0 (TID 433). 2072 bytes result sent to driver
17/12/23 18:55:06 INFO TaskSetManager: Starting task 2.0 in stage 174.0 (TID 434, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:55:06 INFO Executor: Running task 2.0 in stage 174.0 (TID 434)
17/12/23 18:55:06 INFO TaskSetManager: Finished task 1.0 in stage 174.0 (TID 433) in 805 ms on localhost (executor driver) (2/6)
17/12/23 18:55:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:06 INFO Executor: Finished task 2.0 in stage 174.0 (TID 434). 2072 bytes result sent to driver
17/12/23 18:55:06 INFO TaskSetManager: Starting task 3.0 in stage 174.0 (TID 435, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:55:06 INFO Executor: Running task 3.0 in stage 174.0 (TID 435)
17/12/23 18:55:06 INFO TaskSetManager: Finished task 2.0 in stage 174.0 (TID 434) in 39 ms on localhost (executor driver) (3/6)
17/12/23 18:55:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:06 INFO Executor: Finished task 3.0 in stage 174.0 (TID 435). 2040 bytes result sent to driver
17/12/23 18:55:06 INFO TaskSetManager: Starting task 4.0 in stage 174.0 (TID 436, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:55:06 INFO Executor: Running task 4.0 in stage 174.0 (TID 436)
17/12/23 18:55:06 INFO TaskSetManager: Finished task 3.0 in stage 174.0 (TID 435) in 263 ms on localhost (executor driver) (4/6)
17/12/23 18:55:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:06 INFO Executor: Finished task 4.0 in stage 174.0 (TID 436). 2040 bytes result sent to driver
17/12/23 18:55:06 INFO TaskSetManager: Starting task 5.0 in stage 174.0 (TID 437, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:55:06 INFO Executor: Running task 5.0 in stage 174.0 (TID 437)
17/12/23 18:55:06 INFO TaskSetManager: Finished task 4.0 in stage 174.0 (TID 436) in 387 ms on localhost (executor driver) (5/6)
17/12/23 18:55:06 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:06 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:07 INFO Executor: Finished task 5.0 in stage 174.0 (TID 437). 2040 bytes result sent to driver
17/12/23 18:55:07 INFO TaskSetManager: Finished task 5.0 in stage 174.0 (TID 437) in 1072 ms on localhost (executor driver) (6/6)
17/12/23 18:55:07 INFO TaskSchedulerImpl: Removed TaskSet 174.0, whose tasks have all completed, from pool 
17/12/23 18:55:07 INFO DAGScheduler: ResultStage 174 (collect at StackOverflow.scala:191) finished in 3.106 s
17/12/23 18:55:07 INFO DAGScheduler: Job 34 finished: collect at StackOverflow.scala:191, took 6.234916 s
Iteration: 34
  * current distance: 5809.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2228) ==>             (0,2254)    distance:      676
                (0,439) ==>              (0,442)    distance:        9
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,290) ==>         (150000,292)    distance:        4
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,307) ==>          (50000,315)    distance:       64
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,90) ==>          (200000,90)    distance:        0
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,497) ==>         (200000,498)    distance:        1
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,178) ==>         (350000,182)    distance:       16
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,804) ==>         (350000,831)    distance:      729
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
          (100000,1116) ==>        (100000,1181)    distance:     4225
           (100000,153) ==>         (100000,159)    distance:       36
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1554) ==>        (250000,1554)    distance:        0
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,202) ==>         (250000,209)    distance:       49
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:55:07 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:55:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:55:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:55:07 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:55:07 INFO DAGScheduler: Registering RDD 120 (map at StackOverflow.scala:186)
17/12/23 18:55:07 INFO DAGScheduler: Got job 35 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:55:07 INFO DAGScheduler: Final stage: ResultStage 179 (collect at StackOverflow.scala:191)
17/12/23 18:55:07 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 178)
17/12/23 18:55:07 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 178)
17/12/23 18:55:07 INFO DAGScheduler: Submitting ShuffleMapStage 178 (MapPartitionsRDD[120] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:55:07 INFO MemoryStore: Block broadcast_74 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:55:07 INFO MemoryStore: Block broadcast_74_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:55:07 INFO BlockManagerInfo: Added broadcast_74_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:07 INFO SparkContext: Created broadcast 74 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:07 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 178 (MapPartitionsRDD[120] at map at StackOverflow.scala:186)
17/12/23 18:55:07 INFO TaskSchedulerImpl: Adding task set 178.0 with 6 tasks
17/12/23 18:55:07 INFO TaskSetManager: Starting task 0.0 in stage 178.0 (TID 438, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:07 INFO Executor: Running task 0.0 in stage 178.0 (TID 438)
17/12/23 18:55:07 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:55:08 INFO Executor: Finished task 0.0 in stage 178.0 (TID 438). 1238 bytes result sent to driver
17/12/23 18:55:08 INFO TaskSetManager: Starting task 1.0 in stage 178.0 (TID 439, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:08 INFO Executor: Running task 1.0 in stage 178.0 (TID 439)
17/12/23 18:55:08 INFO TaskSetManager: Finished task 0.0 in stage 178.0 (TID 438) in 521 ms on localhost (executor driver) (1/6)
17/12/23 18:55:08 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:55:08 INFO Executor: Finished task 1.0 in stage 178.0 (TID 439). 1238 bytes result sent to driver
17/12/23 18:55:08 INFO TaskSetManager: Starting task 2.0 in stage 178.0 (TID 440, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:08 INFO TaskSetManager: Finished task 1.0 in stage 178.0 (TID 439) in 502 ms on localhost (executor driver) (2/6)
17/12/23 18:55:08 INFO Executor: Running task 2.0 in stage 178.0 (TID 440)
17/12/23 18:55:08 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:55:09 INFO Executor: Finished task 2.0 in stage 178.0 (TID 440). 1325 bytes result sent to driver
17/12/23 18:55:09 INFO TaskSetManager: Starting task 3.0 in stage 178.0 (TID 441, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:09 INFO TaskSetManager: Finished task 2.0 in stage 178.0 (TID 440) in 508 ms on localhost (executor driver) (3/6)
17/12/23 18:55:09 INFO Executor: Running task 3.0 in stage 178.0 (TID 441)
17/12/23 18:55:09 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:55:09 INFO Executor: Finished task 3.0 in stage 178.0 (TID 441). 1325 bytes result sent to driver
17/12/23 18:55:09 INFO TaskSetManager: Starting task 4.0 in stage 178.0 (TID 442, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:09 INFO Executor: Running task 4.0 in stage 178.0 (TID 442)
17/12/23 18:55:09 INFO TaskSetManager: Finished task 3.0 in stage 178.0 (TID 441) in 472 ms on localhost (executor driver) (4/6)
17/12/23 18:55:09 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:55:10 INFO Executor: Finished task 4.0 in stage 178.0 (TID 442). 1325 bytes result sent to driver
17/12/23 18:55:10 INFO TaskSetManager: Starting task 5.0 in stage 178.0 (TID 443, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:10 INFO Executor: Running task 5.0 in stage 178.0 (TID 443)
17/12/23 18:55:10 INFO TaskSetManager: Finished task 4.0 in stage 178.0 (TID 442) in 493 ms on localhost (executor driver) (5/6)
17/12/23 18:55:10 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:55:10 INFO Executor: Finished task 5.0 in stage 178.0 (TID 443). 1415 bytes result sent to driver
17/12/23 18:55:10 INFO TaskSetManager: Finished task 5.0 in stage 178.0 (TID 443) in 512 ms on localhost (executor driver) (6/6)
17/12/23 18:55:10 INFO TaskSchedulerImpl: Removed TaskSet 178.0, whose tasks have all completed, from pool 
17/12/23 18:55:10 INFO DAGScheduler: ShuffleMapStage 178 (map at StackOverflow.scala:186) finished in 3.008 s
17/12/23 18:55:10 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:55:10 INFO DAGScheduler: running: Set()
17/12/23 18:55:10 INFO DAGScheduler: waiting: Set(ResultStage 179)
17/12/23 18:55:10 INFO DAGScheduler: failed: Set()
17/12/23 18:55:10 INFO DAGScheduler: Submitting ResultStage 179 (MapPartitionsRDD[122] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:55:10 INFO MemoryStore: Block broadcast_75 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:55:10 INFO MemoryStore: Block broadcast_75_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:55:10 INFO BlockManagerInfo: Added broadcast_75_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:10 INFO SparkContext: Created broadcast 75 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:10 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 179 (MapPartitionsRDD[122] at mapValues at StackOverflow.scala:190)
17/12/23 18:55:10 INFO TaskSchedulerImpl: Adding task set 179.0 with 6 tasks
17/12/23 18:55:10 INFO TaskSetManager: Starting task 0.0 in stage 179.0 (TID 444, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:55:10 INFO Executor: Running task 0.0 in stage 179.0 (TID 444)
17/12/23 18:55:10 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:10 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:11 INFO ContextCleaner: Cleaned shuffle 37
17/12/23 18:55:11 INFO BlockManagerInfo: Removed broadcast_72_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:11 INFO BlockManagerInfo: Removed broadcast_73_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:11 INFO BlockManagerInfo: Removed broadcast_74_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:11 INFO Executor: Finished task 0.0 in stage 179.0 (TID 444). 2145 bytes result sent to driver
17/12/23 18:55:11 INFO TaskSetManager: Starting task 1.0 in stage 179.0 (TID 445, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:55:11 INFO Executor: Running task 1.0 in stage 179.0 (TID 445)
17/12/23 18:55:11 INFO TaskSetManager: Finished task 0.0 in stage 179.0 (TID 444) in 504 ms on localhost (executor driver) (1/6)
17/12/23 18:55:11 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:11 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:12 INFO Executor: Finished task 1.0 in stage 179.0 (TID 445). 2072 bytes result sent to driver
17/12/23 18:55:12 INFO TaskSetManager: Starting task 2.0 in stage 179.0 (TID 446, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:55:12 INFO Executor: Running task 2.0 in stage 179.0 (TID 446)
17/12/23 18:55:12 INFO TaskSetManager: Finished task 1.0 in stage 179.0 (TID 445) in 704 ms on localhost (executor driver) (2/6)
17/12/23 18:55:12 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:12 INFO Executor: Finished task 2.0 in stage 179.0 (TID 446). 2072 bytes result sent to driver
17/12/23 18:55:12 INFO TaskSetManager: Starting task 3.0 in stage 179.0 (TID 447, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:55:12 INFO Executor: Running task 3.0 in stage 179.0 (TID 447)
17/12/23 18:55:12 INFO TaskSetManager: Finished task 2.0 in stage 179.0 (TID 446) in 33 ms on localhost (executor driver) (3/6)
17/12/23 18:55:12 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:12 INFO Executor: Finished task 3.0 in stage 179.0 (TID 447). 2040 bytes result sent to driver
17/12/23 18:55:12 INFO TaskSetManager: Starting task 4.0 in stage 179.0 (TID 448, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:55:12 INFO TaskSetManager: Finished task 3.0 in stage 179.0 (TID 447) in 252 ms on localhost (executor driver) (4/6)
17/12/23 18:55:12 INFO Executor: Running task 4.0 in stage 179.0 (TID 448)
17/12/23 18:55:12 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:12 INFO Executor: Finished task 4.0 in stage 179.0 (TID 448). 2040 bytes result sent to driver
17/12/23 18:55:12 INFO TaskSetManager: Starting task 5.0 in stage 179.0 (TID 449, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:55:12 INFO Executor: Running task 5.0 in stage 179.0 (TID 449)
17/12/23 18:55:12 INFO TaskSetManager: Finished task 4.0 in stage 179.0 (TID 448) in 392 ms on localhost (executor driver) (5/6)
17/12/23 18:55:12 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:12 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:13 INFO Executor: Finished task 5.0 in stage 179.0 (TID 449). 2130 bytes result sent to driver
17/12/23 18:55:13 INFO TaskSetManager: Finished task 5.0 in stage 179.0 (TID 449) in 953 ms on localhost (executor driver) (6/6)
17/12/23 18:55:13 INFO TaskSchedulerImpl: Removed TaskSet 179.0, whose tasks have all completed, from pool 
17/12/23 18:55:13 INFO DAGScheduler: ResultStage 179 (collect at StackOverflow.scala:191) finished in 2.837 s
17/12/23 18:55:13 INFO DAGScheduler: Job 35 finished: collect at StackOverflow.scala:191, took 5.857876 s
Iteration: 35
  * current distance: 16862.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2254) ==>             (0,2309)    distance:     3025
                (0,442) ==>              (0,448)    distance:       36
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,292) ==>         (150000,292)    distance:        0
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,315) ==>          (50000,320)    distance:       25
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,90) ==>          (200000,91)    distance:        1
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,498) ==>         (200000,502)    distance:       16
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,182) ==>         (350000,189)    distance:       49
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,831) ==>         (350000,886)    distance:     3025
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
          (100000,1181) ==>        (100000,1263)    distance:     6724
           (100000,159) ==>         (100000,165)    distance:       36
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1554) ==>        (250000,1616)    distance:     3844
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,209) ==>         (250000,218)    distance:       81
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:55:13 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:55:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:55:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:55:13 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:55:13 INFO DAGScheduler: Registering RDD 123 (map at StackOverflow.scala:186)
17/12/23 18:55:13 INFO DAGScheduler: Got job 36 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:55:13 INFO DAGScheduler: Final stage: ResultStage 184 (collect at StackOverflow.scala:191)
17/12/23 18:55:13 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 183)
17/12/23 18:55:13 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 183)
17/12/23 18:55:13 INFO DAGScheduler: Submitting ShuffleMapStage 183 (MapPartitionsRDD[123] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:55:13 INFO MemoryStore: Block broadcast_76 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:55:13 INFO MemoryStore: Block broadcast_76_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:55:13 INFO BlockManagerInfo: Added broadcast_76_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:13 INFO SparkContext: Created broadcast 76 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:13 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 183 (MapPartitionsRDD[123] at map at StackOverflow.scala:186)
17/12/23 18:55:13 INFO TaskSchedulerImpl: Adding task set 183.0 with 6 tasks
17/12/23 18:55:13 INFO TaskSetManager: Starting task 0.0 in stage 183.0 (TID 450, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:13 INFO Executor: Running task 0.0 in stage 183.0 (TID 450)
17/12/23 18:55:13 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:55:14 INFO Executor: Finished task 0.0 in stage 183.0 (TID 450). 1325 bytes result sent to driver
17/12/23 18:55:14 INFO TaskSetManager: Starting task 1.0 in stage 183.0 (TID 451, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:14 INFO Executor: Running task 1.0 in stage 183.0 (TID 451)
17/12/23 18:55:14 INFO TaskSetManager: Finished task 0.0 in stage 183.0 (TID 450) in 550 ms on localhost (executor driver) (1/6)
17/12/23 18:55:14 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:55:14 INFO Executor: Finished task 1.0 in stage 183.0 (TID 451). 1238 bytes result sent to driver
17/12/23 18:55:14 INFO TaskSetManager: Starting task 2.0 in stage 183.0 (TID 452, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:14 INFO Executor: Running task 2.0 in stage 183.0 (TID 452)
17/12/23 18:55:14 INFO TaskSetManager: Finished task 1.0 in stage 183.0 (TID 451) in 517 ms on localhost (executor driver) (2/6)
17/12/23 18:55:14 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:55:15 INFO Executor: Finished task 2.0 in stage 183.0 (TID 452). 1415 bytes result sent to driver
17/12/23 18:55:15 INFO TaskSetManager: Starting task 3.0 in stage 183.0 (TID 453, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:15 INFO TaskSetManager: Finished task 2.0 in stage 183.0 (TID 452) in 516 ms on localhost (executor driver) (3/6)
17/12/23 18:55:15 INFO Executor: Running task 3.0 in stage 183.0 (TID 453)
17/12/23 18:55:15 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:55:15 INFO Executor: Finished task 3.0 in stage 183.0 (TID 453). 1415 bytes result sent to driver
17/12/23 18:55:15 INFO TaskSetManager: Starting task 4.0 in stage 183.0 (TID 454, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:15 INFO Executor: Running task 4.0 in stage 183.0 (TID 454)
17/12/23 18:55:15 INFO TaskSetManager: Finished task 3.0 in stage 183.0 (TID 453) in 620 ms on localhost (executor driver) (4/6)
17/12/23 18:55:15 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:55:16 INFO Executor: Finished task 4.0 in stage 183.0 (TID 454). 1325 bytes result sent to driver
17/12/23 18:55:16 INFO TaskSetManager: Starting task 5.0 in stage 183.0 (TID 455, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:16 INFO Executor: Running task 5.0 in stage 183.0 (TID 455)
17/12/23 18:55:16 INFO TaskSetManager: Finished task 4.0 in stage 183.0 (TID 454) in 493 ms on localhost (executor driver) (5/6)
17/12/23 18:55:16 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:55:16 INFO Executor: Finished task 5.0 in stage 183.0 (TID 455). 1238 bytes result sent to driver
17/12/23 18:55:16 INFO TaskSetManager: Finished task 5.0 in stage 183.0 (TID 455) in 507 ms on localhost (executor driver) (6/6)
17/12/23 18:55:16 INFO TaskSchedulerImpl: Removed TaskSet 183.0, whose tasks have all completed, from pool 
17/12/23 18:55:16 INFO DAGScheduler: ShuffleMapStage 183 (map at StackOverflow.scala:186) finished in 3.203 s
17/12/23 18:55:16 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:55:16 INFO DAGScheduler: running: Set()
17/12/23 18:55:16 INFO DAGScheduler: waiting: Set(ResultStage 184)
17/12/23 18:55:16 INFO DAGScheduler: failed: Set()
17/12/23 18:55:16 INFO DAGScheduler: Submitting ResultStage 184 (MapPartitionsRDD[125] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:55:16 INFO MemoryStore: Block broadcast_77 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:55:16 INFO MemoryStore: Block broadcast_77_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:55:16 INFO BlockManagerInfo: Added broadcast_77_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:16 INFO SparkContext: Created broadcast 77 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:16 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 184 (MapPartitionsRDD[125] at mapValues at StackOverflow.scala:190)
17/12/23 18:55:16 INFO TaskSchedulerImpl: Adding task set 184.0 with 6 tasks
17/12/23 18:55:16 INFO TaskSetManager: Starting task 0.0 in stage 184.0 (TID 456, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:55:16 INFO Executor: Running task 0.0 in stage 184.0 (TID 456)
17/12/23 18:55:16 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:16 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:17 INFO Executor: Finished task 0.0 in stage 184.0 (TID 456). 2072 bytes result sent to driver
17/12/23 18:55:17 INFO TaskSetManager: Starting task 1.0 in stage 184.0 (TID 457, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:55:17 INFO TaskSetManager: Finished task 0.0 in stage 184.0 (TID 456) in 484 ms on localhost (executor driver) (1/6)
17/12/23 18:55:17 INFO Executor: Running task 1.0 in stage 184.0 (TID 457)
17/12/23 18:55:17 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:18 INFO Executor: Finished task 1.0 in stage 184.0 (TID 457). 2072 bytes result sent to driver
17/12/23 18:55:18 INFO TaskSetManager: Starting task 2.0 in stage 184.0 (TID 458, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:55:18 INFO Executor: Running task 2.0 in stage 184.0 (TID 458)
17/12/23 18:55:18 INFO TaskSetManager: Finished task 1.0 in stage 184.0 (TID 457) in 733 ms on localhost (executor driver) (2/6)
17/12/23 18:55:18 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:55:18 INFO Executor: Finished task 2.0 in stage 184.0 (TID 458). 1985 bytes result sent to driver
17/12/23 18:55:18 INFO TaskSetManager: Starting task 3.0 in stage 184.0 (TID 459, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:55:18 INFO Executor: Running task 3.0 in stage 184.0 (TID 459)
17/12/23 18:55:18 INFO TaskSetManager: Finished task 2.0 in stage 184.0 (TID 458) in 25 ms on localhost (executor driver) (3/6)
17/12/23 18:55:18 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:18 INFO ContextCleaner: Cleaned shuffle 38
17/12/23 18:55:18 INFO BlockManagerInfo: Removed broadcast_75_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:18 INFO BlockManagerInfo: Removed broadcast_76_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:18 INFO Executor: Finished task 3.0 in stage 184.0 (TID 459). 2113 bytes result sent to driver
17/12/23 18:55:18 INFO TaskSetManager: Starting task 4.0 in stage 184.0 (TID 460, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:55:18 INFO Executor: Running task 4.0 in stage 184.0 (TID 460)
17/12/23 18:55:18 INFO TaskSetManager: Finished task 3.0 in stage 184.0 (TID 459) in 250 ms on localhost (executor driver) (4/6)
17/12/23 18:55:18 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:18 INFO Executor: Finished task 4.0 in stage 184.0 (TID 460). 1953 bytes result sent to driver
17/12/23 18:55:18 INFO TaskSetManager: Starting task 5.0 in stage 184.0 (TID 461, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:55:18 INFO Executor: Running task 5.0 in stage 184.0 (TID 461)
17/12/23 18:55:18 INFO TaskSetManager: Finished task 4.0 in stage 184.0 (TID 460) in 409 ms on localhost (executor driver) (5/6)
17/12/23 18:55:18 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:18 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:19 INFO Executor: Finished task 5.0 in stage 184.0 (TID 461). 2130 bytes result sent to driver
17/12/23 18:55:19 INFO TaskSetManager: Finished task 5.0 in stage 184.0 (TID 461) in 949 ms on localhost (executor driver) (6/6)
17/12/23 18:55:19 INFO TaskSchedulerImpl: Removed TaskSet 184.0, whose tasks have all completed, from pool 
17/12/23 18:55:19 INFO DAGScheduler: ResultStage 184 (collect at StackOverflow.scala:191) finished in 2.850 s
17/12/23 18:55:19 INFO DAGScheduler: Job 36 finished: collect at StackOverflow.scala:191, took 6.065133 s
Iteration: 36
  * current distance: 887.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2309) ==>             (0,2309)    distance:        0
                (0,448) ==>              (0,455)    distance:       49
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,292) ==>         (150000,292)    distance:        0
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,320) ==>          (50000,322)    distance:        4
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,91) ==>          (200000,92)    distance:        1
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,502) ==>         (200000,504)    distance:        4
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,189) ==>         (350000,195)    distance:       36
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,886) ==>         (350000,912)    distance:      676
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
          (100000,1263) ==>        (100000,1263)    distance:        0
           (100000,165) ==>         (100000,171)    distance:       36
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1616) ==>        (250000,1616)    distance:        0
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,218) ==>         (250000,227)    distance:       81
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:55:19 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:55:19 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:55:19 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:55:19 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:55:19 INFO DAGScheduler: Registering RDD 126 (map at StackOverflow.scala:186)
17/12/23 18:55:19 INFO DAGScheduler: Got job 37 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:55:19 INFO DAGScheduler: Final stage: ResultStage 189 (collect at StackOverflow.scala:191)
17/12/23 18:55:19 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 188)
17/12/23 18:55:19 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 188)
17/12/23 18:55:19 INFO DAGScheduler: Submitting ShuffleMapStage 188 (MapPartitionsRDD[126] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:55:19 INFO MemoryStore: Block broadcast_78 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:55:19 INFO MemoryStore: Block broadcast_78_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:55:19 INFO BlockManagerInfo: Added broadcast_78_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:19 INFO SparkContext: Created broadcast 78 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:19 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 188 (MapPartitionsRDD[126] at map at StackOverflow.scala:186)
17/12/23 18:55:19 INFO TaskSchedulerImpl: Adding task set 188.0 with 6 tasks
17/12/23 18:55:19 INFO TaskSetManager: Starting task 0.0 in stage 188.0 (TID 462, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:19 INFO Executor: Running task 0.0 in stage 188.0 (TID 462)
17/12/23 18:55:19 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:55:20 INFO Executor: Finished task 0.0 in stage 188.0 (TID 462). 1325 bytes result sent to driver
17/12/23 18:55:20 INFO TaskSetManager: Starting task 1.0 in stage 188.0 (TID 463, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:20 INFO Executor: Running task 1.0 in stage 188.0 (TID 463)
17/12/23 18:55:20 INFO TaskSetManager: Finished task 0.0 in stage 188.0 (TID 462) in 565 ms on localhost (executor driver) (1/6)
17/12/23 18:55:20 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:55:20 INFO Executor: Finished task 1.0 in stage 188.0 (TID 463). 1325 bytes result sent to driver
17/12/23 18:55:20 INFO TaskSetManager: Starting task 2.0 in stage 188.0 (TID 464, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:20 INFO Executor: Running task 2.0 in stage 188.0 (TID 464)
17/12/23 18:55:20 INFO TaskSetManager: Finished task 1.0 in stage 188.0 (TID 463) in 495 ms on localhost (executor driver) (2/6)
17/12/23 18:55:20 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:55:21 INFO Executor: Finished task 2.0 in stage 188.0 (TID 464). 1325 bytes result sent to driver
17/12/23 18:55:21 INFO TaskSetManager: Starting task 3.0 in stage 188.0 (TID 465, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:21 INFO TaskSetManager: Finished task 2.0 in stage 188.0 (TID 464) in 494 ms on localhost (executor driver) (3/6)
17/12/23 18:55:21 INFO Executor: Running task 3.0 in stage 188.0 (TID 465)
17/12/23 18:55:21 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:55:21 INFO Executor: Finished task 3.0 in stage 188.0 (TID 465). 1325 bytes result sent to driver
17/12/23 18:55:21 INFO TaskSetManager: Starting task 4.0 in stage 188.0 (TID 466, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:21 INFO TaskSetManager: Finished task 3.0 in stage 188.0 (TID 465) in 515 ms on localhost (executor driver) (4/6)
17/12/23 18:55:21 INFO Executor: Running task 4.0 in stage 188.0 (TID 466)
17/12/23 18:55:21 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:55:22 INFO Executor: Finished task 4.0 in stage 188.0 (TID 466). 1238 bytes result sent to driver
17/12/23 18:55:22 INFO TaskSetManager: Starting task 5.0 in stage 188.0 (TID 467, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:22 INFO TaskSetManager: Finished task 4.0 in stage 188.0 (TID 466) in 520 ms on localhost (executor driver) (5/6)
17/12/23 18:55:22 INFO Executor: Running task 5.0 in stage 188.0 (TID 467)
17/12/23 18:55:22 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:55:22 INFO Executor: Finished task 5.0 in stage 188.0 (TID 467). 1325 bytes result sent to driver
17/12/23 18:55:22 INFO TaskSetManager: Finished task 5.0 in stage 188.0 (TID 467) in 520 ms on localhost (executor driver) (6/6)
17/12/23 18:55:22 INFO TaskSchedulerImpl: Removed TaskSet 188.0, whose tasks have all completed, from pool 
17/12/23 18:55:22 INFO DAGScheduler: ShuffleMapStage 188 (map at StackOverflow.scala:186) finished in 3.109 s
17/12/23 18:55:22 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:55:22 INFO DAGScheduler: running: Set()
17/12/23 18:55:22 INFO DAGScheduler: waiting: Set(ResultStage 189)
17/12/23 18:55:22 INFO DAGScheduler: failed: Set()
17/12/23 18:55:22 INFO DAGScheduler: Submitting ResultStage 189 (MapPartitionsRDD[128] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:55:22 INFO MemoryStore: Block broadcast_79 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:55:22 INFO MemoryStore: Block broadcast_79_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:55:22 INFO BlockManagerInfo: Added broadcast_79_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:22 INFO SparkContext: Created broadcast 79 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:22 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 189 (MapPartitionsRDD[128] at mapValues at StackOverflow.scala:190)
17/12/23 18:55:22 INFO TaskSchedulerImpl: Adding task set 189.0 with 6 tasks
17/12/23 18:55:22 INFO TaskSetManager: Starting task 0.0 in stage 189.0 (TID 468, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:55:22 INFO Executor: Running task 0.0 in stage 189.0 (TID 468)
17/12/23 18:55:22 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:22 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:23 INFO Executor: Finished task 0.0 in stage 189.0 (TID 468). 1985 bytes result sent to driver
17/12/23 18:55:23 INFO TaskSetManager: Starting task 1.0 in stage 189.0 (TID 469, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:55:23 INFO Executor: Running task 1.0 in stage 189.0 (TID 469)
17/12/23 18:55:23 INFO TaskSetManager: Finished task 0.0 in stage 189.0 (TID 468) in 512 ms on localhost (executor driver) (1/6)
17/12/23 18:55:23 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:23 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:24 INFO Executor: Finished task 1.0 in stage 189.0 (TID 469). 2162 bytes result sent to driver
17/12/23 18:55:24 INFO TaskSetManager: Starting task 2.0 in stage 189.0 (TID 470, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:55:24 INFO Executor: Running task 2.0 in stage 189.0 (TID 470)
17/12/23 18:55:24 INFO TaskSetManager: Finished task 1.0 in stage 189.0 (TID 469) in 735 ms on localhost (executor driver) (2/6)
17/12/23 18:55:24 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:24 INFO Executor: Finished task 2.0 in stage 189.0 (TID 470). 1985 bytes result sent to driver
17/12/23 18:55:24 INFO TaskSetManager: Starting task 3.0 in stage 189.0 (TID 471, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:55:24 INFO Executor: Running task 3.0 in stage 189.0 (TID 471)
17/12/23 18:55:24 INFO TaskSetManager: Finished task 2.0 in stage 189.0 (TID 470) in 33 ms on localhost (executor driver) (3/6)
17/12/23 18:55:24 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:24 INFO Executor: Finished task 3.0 in stage 189.0 (TID 471). 2130 bytes result sent to driver
17/12/23 18:55:24 INFO TaskSetManager: Starting task 4.0 in stage 189.0 (TID 472, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:55:24 INFO Executor: Running task 4.0 in stage 189.0 (TID 472)
17/12/23 18:55:24 INFO TaskSetManager: Finished task 3.0 in stage 189.0 (TID 471) in 237 ms on localhost (executor driver) (4/6)
17/12/23 18:55:24 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:24 INFO Executor: Finished task 4.0 in stage 189.0 (TID 472). 1953 bytes result sent to driver
17/12/23 18:55:24 INFO TaskSetManager: Starting task 5.0 in stage 189.0 (TID 473, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:55:24 INFO Executor: Running task 5.0 in stage 189.0 (TID 473)
17/12/23 18:55:24 INFO TaskSetManager: Finished task 4.0 in stage 189.0 (TID 472) in 377 ms on localhost (executor driver) (5/6)
17/12/23 18:55:24 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:24 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:25 INFO BlockManagerInfo: Removed broadcast_77_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:25 INFO BlockManagerInfo: Removed broadcast_78_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:25 INFO ContextCleaner: Cleaned shuffle 39
17/12/23 18:55:25 INFO Executor: Finished task 5.0 in stage 189.0 (TID 473). 2026 bytes result sent to driver
17/12/23 18:55:25 INFO TaskSetManager: Finished task 5.0 in stage 189.0 (TID 473) in 985 ms on localhost (executor driver) (6/6)
17/12/23 18:55:25 INFO TaskSchedulerImpl: Removed TaskSet 189.0, whose tasks have all completed, from pool 
17/12/23 18:55:25 INFO DAGScheduler: ResultStage 189 (collect at StackOverflow.scala:191) finished in 2.874 s
17/12/23 18:55:25 INFO DAGScheduler: Job 37 finished: collect at StackOverflow.scala:191, took 5.993231 s
Iteration: 37
  * current distance: 5301.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2309) ==>             (0,2309)    distance:        0
                (0,455) ==>              (0,460)    distance:       25
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,292) ==>         (150000,292)    distance:        0
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,322) ==>          (50000,325)    distance:        9
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,92) ==>          (200000,92)    distance:        0
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,504) ==>         (200000,511)    distance:       49
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,195) ==>         (350000,200)    distance:       25
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,912) ==>         (350000,926)    distance:      196
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
          (100000,1263) ==>        (100000,1263)    distance:        0
           (100000,171) ==>         (100000,175)    distance:       16
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1616) ==>        (250000,1686)    distance:     4900
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,227) ==>         (250000,236)    distance:       81
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:55:25 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:55:25 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:55:25 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:55:25 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:55:25 INFO DAGScheduler: Registering RDD 129 (map at StackOverflow.scala:186)
17/12/23 18:55:25 INFO DAGScheduler: Got job 38 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:55:25 INFO DAGScheduler: Final stage: ResultStage 194 (collect at StackOverflow.scala:191)
17/12/23 18:55:25 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 193)
17/12/23 18:55:25 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 193)
17/12/23 18:55:25 INFO DAGScheduler: Submitting ShuffleMapStage 193 (MapPartitionsRDD[129] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:55:25 INFO MemoryStore: Block broadcast_80 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:55:25 INFO MemoryStore: Block broadcast_80_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:55:25 INFO BlockManagerInfo: Added broadcast_80_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:25 INFO SparkContext: Created broadcast 80 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:25 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 193 (MapPartitionsRDD[129] at map at StackOverflow.scala:186)
17/12/23 18:55:25 INFO TaskSchedulerImpl: Adding task set 193.0 with 6 tasks
17/12/23 18:55:25 INFO TaskSetManager: Starting task 0.0 in stage 193.0 (TID 474, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:25 INFO Executor: Running task 0.0 in stage 193.0 (TID 474)
17/12/23 18:55:25 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:55:26 INFO Executor: Finished task 0.0 in stage 193.0 (TID 474). 1325 bytes result sent to driver
17/12/23 18:55:26 INFO TaskSetManager: Starting task 1.0 in stage 193.0 (TID 475, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:26 INFO Executor: Running task 1.0 in stage 193.0 (TID 475)
17/12/23 18:55:26 INFO TaskSetManager: Finished task 0.0 in stage 193.0 (TID 474) in 500 ms on localhost (executor driver) (1/6)
17/12/23 18:55:26 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:55:26 INFO Executor: Finished task 1.0 in stage 193.0 (TID 475). 1238 bytes result sent to driver
17/12/23 18:55:26 INFO TaskSetManager: Starting task 2.0 in stage 193.0 (TID 476, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:26 INFO TaskSetManager: Finished task 1.0 in stage 193.0 (TID 475) in 514 ms on localhost (executor driver) (2/6)
17/12/23 18:55:26 INFO Executor: Running task 2.0 in stage 193.0 (TID 476)
17/12/23 18:55:26 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:55:27 INFO Executor: Finished task 2.0 in stage 193.0 (TID 476). 1325 bytes result sent to driver
17/12/23 18:55:27 INFO TaskSetManager: Starting task 3.0 in stage 193.0 (TID 477, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:27 INFO Executor: Running task 3.0 in stage 193.0 (TID 477)
17/12/23 18:55:27 INFO TaskSetManager: Finished task 2.0 in stage 193.0 (TID 476) in 531 ms on localhost (executor driver) (3/6)
17/12/23 18:55:27 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:55:27 INFO Executor: Finished task 3.0 in stage 193.0 (TID 477). 1325 bytes result sent to driver
17/12/23 18:55:27 INFO TaskSetManager: Starting task 4.0 in stage 193.0 (TID 478, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:27 INFO Executor: Running task 4.0 in stage 193.0 (TID 478)
17/12/23 18:55:27 INFO TaskSetManager: Finished task 3.0 in stage 193.0 (TID 477) in 557 ms on localhost (executor driver) (4/6)
17/12/23 18:55:27 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:55:28 INFO Executor: Finished task 4.0 in stage 193.0 (TID 478). 1415 bytes result sent to driver
17/12/23 18:55:28 INFO TaskSetManager: Starting task 5.0 in stage 193.0 (TID 479, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:28 INFO Executor: Running task 5.0 in stage 193.0 (TID 479)
17/12/23 18:55:28 INFO TaskSetManager: Finished task 4.0 in stage 193.0 (TID 478) in 537 ms on localhost (executor driver) (5/6)
17/12/23 18:55:28 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:55:28 INFO Executor: Finished task 5.0 in stage 193.0 (TID 479). 1238 bytes result sent to driver
17/12/23 18:55:28 INFO TaskSetManager: Finished task 5.0 in stage 193.0 (TID 479) in 518 ms on localhost (executor driver) (6/6)
17/12/23 18:55:28 INFO TaskSchedulerImpl: Removed TaskSet 193.0, whose tasks have all completed, from pool 
17/12/23 18:55:28 INFO DAGScheduler: ShuffleMapStage 193 (map at StackOverflow.scala:186) finished in 3.157 s
17/12/23 18:55:28 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:55:28 INFO DAGScheduler: running: Set()
17/12/23 18:55:28 INFO DAGScheduler: waiting: Set(ResultStage 194)
17/12/23 18:55:28 INFO DAGScheduler: failed: Set()
17/12/23 18:55:28 INFO DAGScheduler: Submitting ResultStage 194 (MapPartitionsRDD[131] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:55:28 INFO MemoryStore: Block broadcast_81 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:55:28 INFO MemoryStore: Block broadcast_81_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:55:28 INFO BlockManagerInfo: Added broadcast_81_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:28 INFO SparkContext: Created broadcast 81 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:28 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 194 (MapPartitionsRDD[131] at mapValues at StackOverflow.scala:190)
17/12/23 18:55:28 INFO TaskSchedulerImpl: Adding task set 194.0 with 6 tasks
17/12/23 18:55:28 INFO TaskSetManager: Starting task 0.0 in stage 194.0 (TID 480, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:55:28 INFO Executor: Running task 0.0 in stage 194.0 (TID 480)
17/12/23 18:55:28 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:28 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:29 INFO Executor: Finished task 0.0 in stage 194.0 (TID 480). 1985 bytes result sent to driver
17/12/23 18:55:29 INFO TaskSetManager: Starting task 1.0 in stage 194.0 (TID 481, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:55:29 INFO Executor: Running task 1.0 in stage 194.0 (TID 481)
17/12/23 18:55:29 INFO TaskSetManager: Finished task 0.0 in stage 194.0 (TID 480) in 463 ms on localhost (executor driver) (1/6)
17/12/23 18:55:29 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:29 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:30 INFO Executor: Finished task 1.0 in stage 194.0 (TID 481). 2072 bytes result sent to driver
17/12/23 18:55:30 INFO TaskSetManager: Starting task 2.0 in stage 194.0 (TID 482, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:55:30 INFO TaskSetManager: Finished task 1.0 in stage 194.0 (TID 481) in 735 ms on localhost (executor driver) (2/6)
17/12/23 18:55:30 INFO Executor: Running task 2.0 in stage 194.0 (TID 482)
17/12/23 18:55:30 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:30 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:30 INFO Executor: Finished task 2.0 in stage 194.0 (TID 482). 1985 bytes result sent to driver
17/12/23 18:55:30 INFO TaskSetManager: Starting task 3.0 in stage 194.0 (TID 483, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:55:30 INFO Executor: Running task 3.0 in stage 194.0 (TID 483)
17/12/23 18:55:30 INFO TaskSetManager: Finished task 2.0 in stage 194.0 (TID 482) in 38 ms on localhost (executor driver) (3/6)
17/12/23 18:55:30 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:30 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:30 INFO Executor: Finished task 3.0 in stage 194.0 (TID 483). 1953 bytes result sent to driver
17/12/23 18:55:30 INFO TaskSetManager: Starting task 4.0 in stage 194.0 (TID 484, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:55:30 INFO Executor: Running task 4.0 in stage 194.0 (TID 484)
17/12/23 18:55:30 INFO TaskSetManager: Finished task 3.0 in stage 194.0 (TID 483) in 235 ms on localhost (executor driver) (4/6)
17/12/23 18:55:30 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:30 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:30 INFO Executor: Finished task 4.0 in stage 194.0 (TID 484). 1953 bytes result sent to driver
17/12/23 18:55:30 INFO TaskSetManager: Starting task 5.0 in stage 194.0 (TID 485, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:55:30 INFO Executor: Running task 5.0 in stage 194.0 (TID 485)
17/12/23 18:55:30 INFO TaskSetManager: Finished task 4.0 in stage 194.0 (TID 484) in 372 ms on localhost (executor driver) (5/6)
17/12/23 18:55:30 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:30 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:31 INFO Executor: Finished task 5.0 in stage 194.0 (TID 485). 2040 bytes result sent to driver
17/12/23 18:55:31 INFO TaskSetManager: Finished task 5.0 in stage 194.0 (TID 485) in 952 ms on localhost (executor driver) (6/6)
17/12/23 18:55:31 INFO TaskSchedulerImpl: Removed TaskSet 194.0, whose tasks have all completed, from pool 
17/12/23 18:55:31 INFO DAGScheduler: ResultStage 194 (collect at StackOverflow.scala:191) finished in 2.792 s
17/12/23 18:55:31 INFO DAGScheduler: Job 38 finished: collect at StackOverflow.scala:191, took 5.957686 s
Iteration: 38
  * current distance: 6765.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2309) ==>             (0,2309)    distance:        0
                (0,460) ==>              (0,463)    distance:        9
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,292) ==>         (150000,292)    distance:        0
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,325) ==>          (50000,328)    distance:        9
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,92) ==>          (200000,93)    distance:        1
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,511) ==>         (200000,516)    distance:       25
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,200) ==>         (350000,204)    distance:       16
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,926) ==>         (350000,940)    distance:      196
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
          (100000,1263) ==>        (100000,1263)    distance:        0
           (100000,175) ==>         (100000,178)    distance:        9
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1686) ==>        (250000,1766)    distance:     6400
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,236) ==>         (250000,246)    distance:      100
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:55:31 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:55:31 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:55:31 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:55:31 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:55:31 INFO DAGScheduler: Registering RDD 132 (map at StackOverflow.scala:186)
17/12/23 18:55:31 INFO DAGScheduler: Got job 39 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:55:31 INFO DAGScheduler: Final stage: ResultStage 199 (collect at StackOverflow.scala:191)
17/12/23 18:55:31 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 198)
17/12/23 18:55:31 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 198)
17/12/23 18:55:31 INFO DAGScheduler: Submitting ShuffleMapStage 198 (MapPartitionsRDD[132] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:55:31 INFO MemoryStore: Block broadcast_82 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:55:31 INFO MemoryStore: Block broadcast_82_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:55:31 INFO BlockManagerInfo: Added broadcast_82_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:31 INFO SparkContext: Created broadcast 82 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:31 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 198 (MapPartitionsRDD[132] at map at StackOverflow.scala:186)
17/12/23 18:55:31 INFO TaskSchedulerImpl: Adding task set 198.0 with 6 tasks
17/12/23 18:55:31 INFO TaskSetManager: Starting task 0.0 in stage 198.0 (TID 486, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:31 INFO Executor: Running task 0.0 in stage 198.0 (TID 486)
17/12/23 18:55:31 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:55:32 INFO Executor: Finished task 0.0 in stage 198.0 (TID 486). 1325 bytes result sent to driver
17/12/23 18:55:32 INFO TaskSetManager: Starting task 1.0 in stage 198.0 (TID 487, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:32 INFO TaskSetManager: Finished task 0.0 in stage 198.0 (TID 486) in 499 ms on localhost (executor driver) (1/6)
17/12/23 18:55:32 INFO Executor: Running task 1.0 in stage 198.0 (TID 487)
17/12/23 18:55:32 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:55:32 INFO Executor: Finished task 1.0 in stage 198.0 (TID 487). 1238 bytes result sent to driver
17/12/23 18:55:32 INFO TaskSetManager: Starting task 2.0 in stage 198.0 (TID 488, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:32 INFO TaskSetManager: Finished task 1.0 in stage 198.0 (TID 487) in 513 ms on localhost (executor driver) (2/6)
17/12/23 18:55:32 INFO Executor: Running task 2.0 in stage 198.0 (TID 488)
17/12/23 18:55:32 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:55:32 INFO ContextCleaner: Cleaned shuffle 40
17/12/23 18:55:32 INFO BlockManagerInfo: Removed broadcast_79_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:32 INFO ContextCleaner: Cleaned shuffle 41
17/12/23 18:55:32 INFO BlockManagerInfo: Removed broadcast_80_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:32 INFO BlockManagerInfo: Removed broadcast_81_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:33 INFO Executor: Finished task 2.0 in stage 198.0 (TID 488). 1398 bytes result sent to driver
17/12/23 18:55:33 INFO TaskSetManager: Starting task 3.0 in stage 198.0 (TID 489, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:33 INFO TaskSetManager: Finished task 2.0 in stage 198.0 (TID 488) in 564 ms on localhost (executor driver) (3/6)
17/12/23 18:55:33 INFO Executor: Running task 3.0 in stage 198.0 (TID 489)
17/12/23 18:55:33 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:55:33 INFO Executor: Finished task 3.0 in stage 198.0 (TID 489). 1325 bytes result sent to driver
17/12/23 18:55:33 INFO TaskSetManager: Starting task 4.0 in stage 198.0 (TID 490, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:33 INFO TaskSetManager: Finished task 3.0 in stage 198.0 (TID 489) in 522 ms on localhost (executor driver) (4/6)
17/12/23 18:55:33 INFO Executor: Running task 4.0 in stage 198.0 (TID 490)
17/12/23 18:55:33 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:55:34 INFO Executor: Finished task 4.0 in stage 198.0 (TID 490). 1238 bytes result sent to driver
17/12/23 18:55:34 INFO TaskSetManager: Starting task 5.0 in stage 198.0 (TID 491, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:34 INFO Executor: Running task 5.0 in stage 198.0 (TID 491)
17/12/23 18:55:34 INFO TaskSetManager: Finished task 4.0 in stage 198.0 (TID 490) in 491 ms on localhost (executor driver) (5/6)
17/12/23 18:55:34 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:55:34 INFO Executor: Finished task 5.0 in stage 198.0 (TID 491). 1325 bytes result sent to driver
17/12/23 18:55:34 INFO TaskSetManager: Finished task 5.0 in stage 198.0 (TID 491) in 510 ms on localhost (executor driver) (6/6)
17/12/23 18:55:34 INFO TaskSchedulerImpl: Removed TaskSet 198.0, whose tasks have all completed, from pool 
17/12/23 18:55:34 INFO DAGScheduler: ShuffleMapStage 198 (map at StackOverflow.scala:186) finished in 3.092 s
17/12/23 18:55:34 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:55:34 INFO DAGScheduler: running: Set()
17/12/23 18:55:34 INFO DAGScheduler: waiting: Set(ResultStage 199)
17/12/23 18:55:34 INFO DAGScheduler: failed: Set()
17/12/23 18:55:34 INFO DAGScheduler: Submitting ResultStage 199 (MapPartitionsRDD[134] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:55:34 INFO MemoryStore: Block broadcast_83 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:55:34 INFO MemoryStore: Block broadcast_83_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:55:34 INFO BlockManagerInfo: Added broadcast_83_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:34 INFO SparkContext: Created broadcast 83 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:34 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 199 (MapPartitionsRDD[134] at mapValues at StackOverflow.scala:190)
17/12/23 18:55:34 INFO TaskSchedulerImpl: Adding task set 199.0 with 6 tasks
17/12/23 18:55:34 INFO TaskSetManager: Starting task 0.0 in stage 199.0 (TID 492, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:55:34 INFO Executor: Running task 0.0 in stage 199.0 (TID 492)
17/12/23 18:55:34 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:34 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:35 INFO Executor: Finished task 0.0 in stage 199.0 (TID 492). 2072 bytes result sent to driver
17/12/23 18:55:35 INFO TaskSetManager: Starting task 1.0 in stage 199.0 (TID 493, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:55:35 INFO Executor: Running task 1.0 in stage 199.0 (TID 493)
17/12/23 18:55:35 INFO TaskSetManager: Finished task 0.0 in stage 199.0 (TID 492) in 467 ms on localhost (executor driver) (1/6)
17/12/23 18:55:35 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:35 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:36 INFO Executor: Finished task 1.0 in stage 199.0 (TID 493). 2072 bytes result sent to driver
17/12/23 18:55:36 INFO TaskSetManager: Starting task 2.0 in stage 199.0 (TID 494, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:55:36 INFO Executor: Running task 2.0 in stage 199.0 (TID 494)
17/12/23 18:55:36 INFO TaskSetManager: Finished task 1.0 in stage 199.0 (TID 493) in 715 ms on localhost (executor driver) (2/6)
17/12/23 18:55:36 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:36 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:36 INFO Executor: Finished task 2.0 in stage 199.0 (TID 494). 1985 bytes result sent to driver
17/12/23 18:55:36 INFO TaskSetManager: Starting task 3.0 in stage 199.0 (TID 495, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:55:36 INFO TaskSetManager: Finished task 2.0 in stage 199.0 (TID 494) in 30 ms on localhost (executor driver) (3/6)
17/12/23 18:55:36 INFO Executor: Running task 3.0 in stage 199.0 (TID 495)
17/12/23 18:55:36 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:36 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:36 INFO Executor: Finished task 3.0 in stage 199.0 (TID 495). 1953 bytes result sent to driver
17/12/23 18:55:36 INFO TaskSetManager: Starting task 4.0 in stage 199.0 (TID 496, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:55:36 INFO Executor: Running task 4.0 in stage 199.0 (TID 496)
17/12/23 18:55:36 INFO TaskSetManager: Finished task 3.0 in stage 199.0 (TID 495) in 277 ms on localhost (executor driver) (4/6)
17/12/23 18:55:36 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:36 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:36 INFO Executor: Finished task 4.0 in stage 199.0 (TID 496). 2040 bytes result sent to driver
17/12/23 18:55:36 INFO TaskSetManager: Starting task 5.0 in stage 199.0 (TID 497, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:55:36 INFO Executor: Running task 5.0 in stage 199.0 (TID 497)
17/12/23 18:55:36 INFO TaskSetManager: Finished task 4.0 in stage 199.0 (TID 496) in 407 ms on localhost (executor driver) (5/6)
17/12/23 18:55:36 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:36 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:37 INFO Executor: Finished task 5.0 in stage 199.0 (TID 497). 1953 bytes result sent to driver
17/12/23 18:55:37 INFO TaskSetManager: Finished task 5.0 in stage 199.0 (TID 497) in 945 ms on localhost (executor driver) (6/6)
17/12/23 18:55:37 INFO TaskSchedulerImpl: Removed TaskSet 199.0, whose tasks have all completed, from pool 
17/12/23 18:55:37 INFO DAGScheduler: ResultStage 199 (collect at StackOverflow.scala:191) finished in 2.839 s
17/12/23 18:55:37 INFO DAGScheduler: Job 39 finished: collect at StackOverflow.scala:191, took 5.942263 s
Iteration: 39
  * current distance: 140.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2309) ==>             (0,2309)    distance:        0
                (0,463) ==>              (0,466)    distance:        9
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,292) ==>         (150000,292)    distance:        0
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,328) ==>          (50000,330)    distance:        4
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,93) ==>          (200000,94)    distance:        1
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,516) ==>         (200000,520)    distance:       16
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,204) ==>         (350000,207)    distance:        9
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,940) ==>         (350000,940)    distance:        0
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
          (100000,1263) ==>        (100000,1263)    distance:        0
           (100000,178) ==>         (100000,179)    distance:        1
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1766) ==>        (250000,1766)    distance:        0
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,246) ==>         (250000,256)    distance:      100
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:55:37 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:55:37 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:55:37 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:55:37 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:55:37 INFO DAGScheduler: Registering RDD 135 (map at StackOverflow.scala:186)
17/12/23 18:55:37 INFO DAGScheduler: Got job 40 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:55:37 INFO DAGScheduler: Final stage: ResultStage 204 (collect at StackOverflow.scala:191)
17/12/23 18:55:37 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 203)
17/12/23 18:55:37 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 203)
17/12/23 18:55:37 INFO DAGScheduler: Submitting ShuffleMapStage 203 (MapPartitionsRDD[135] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:55:37 INFO MemoryStore: Block broadcast_84 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:55:37 INFO MemoryStore: Block broadcast_84_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:55:37 INFO BlockManagerInfo: Added broadcast_84_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:37 INFO SparkContext: Created broadcast 84 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:37 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 203 (MapPartitionsRDD[135] at map at StackOverflow.scala:186)
17/12/23 18:55:37 INFO TaskSchedulerImpl: Adding task set 203.0 with 6 tasks
17/12/23 18:55:37 INFO TaskSetManager: Starting task 0.0 in stage 203.0 (TID 498, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:37 INFO Executor: Running task 0.0 in stage 203.0 (TID 498)
17/12/23 18:55:37 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:55:38 INFO Executor: Finished task 0.0 in stage 203.0 (TID 498). 1325 bytes result sent to driver
17/12/23 18:55:38 INFO TaskSetManager: Starting task 1.0 in stage 203.0 (TID 499, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:38 INFO Executor: Running task 1.0 in stage 203.0 (TID 499)
17/12/23 18:55:38 INFO TaskSetManager: Finished task 0.0 in stage 203.0 (TID 498) in 515 ms on localhost (executor driver) (1/6)
17/12/23 18:55:38 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:55:38 INFO Executor: Finished task 1.0 in stage 203.0 (TID 499). 1238 bytes result sent to driver
17/12/23 18:55:38 INFO TaskSetManager: Starting task 2.0 in stage 203.0 (TID 500, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:38 INFO TaskSetManager: Finished task 1.0 in stage 203.0 (TID 499) in 480 ms on localhost (executor driver) (2/6)
17/12/23 18:55:38 INFO Executor: Running task 2.0 in stage 203.0 (TID 500)
17/12/23 18:55:38 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:55:39 INFO Executor: Finished task 2.0 in stage 203.0 (TID 500). 1238 bytes result sent to driver
17/12/23 18:55:39 INFO TaskSetManager: Starting task 3.0 in stage 203.0 (TID 501, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:39 INFO Executor: Running task 3.0 in stage 203.0 (TID 501)
17/12/23 18:55:39 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:55:39 INFO TaskSetManager: Finished task 2.0 in stage 203.0 (TID 500) in 500 ms on localhost (executor driver) (3/6)
17/12/23 18:55:39 INFO Executor: Finished task 3.0 in stage 203.0 (TID 501). 1238 bytes result sent to driver
17/12/23 18:55:39 INFO TaskSetManager: Starting task 4.0 in stage 203.0 (TID 502, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:39 INFO Executor: Running task 4.0 in stage 203.0 (TID 502)
17/12/23 18:55:39 INFO TaskSetManager: Finished task 3.0 in stage 203.0 (TID 501) in 549 ms on localhost (executor driver) (4/6)
17/12/23 18:55:39 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:55:40 INFO Executor: Finished task 4.0 in stage 203.0 (TID 502). 1238 bytes result sent to driver
17/12/23 18:55:40 INFO TaskSetManager: Starting task 5.0 in stage 203.0 (TID 503, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:40 INFO Executor: Running task 5.0 in stage 203.0 (TID 503)
17/12/23 18:55:40 INFO TaskSetManager: Finished task 4.0 in stage 203.0 (TID 502) in 501 ms on localhost (executor driver) (5/6)
17/12/23 18:55:40 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:55:40 INFO Executor: Finished task 5.0 in stage 203.0 (TID 503). 1325 bytes result sent to driver
17/12/23 18:55:40 INFO TaskSetManager: Finished task 5.0 in stage 203.0 (TID 503) in 477 ms on localhost (executor driver) (6/6)
17/12/23 18:55:40 INFO TaskSchedulerImpl: Removed TaskSet 203.0, whose tasks have all completed, from pool 
17/12/23 18:55:40 INFO DAGScheduler: ShuffleMapStage 203 (map at StackOverflow.scala:186) finished in 3.016 s
17/12/23 18:55:40 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:55:40 INFO DAGScheduler: running: Set()
17/12/23 18:55:40 INFO DAGScheduler: waiting: Set(ResultStage 204)
17/12/23 18:55:40 INFO DAGScheduler: failed: Set()
17/12/23 18:55:40 INFO DAGScheduler: Submitting ResultStage 204 (MapPartitionsRDD[137] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:55:40 INFO MemoryStore: Block broadcast_85 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:55:40 INFO MemoryStore: Block broadcast_85_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:55:40 INFO BlockManagerInfo: Added broadcast_85_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:40 INFO SparkContext: Created broadcast 85 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:40 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 204 (MapPartitionsRDD[137] at mapValues at StackOverflow.scala:190)
17/12/23 18:55:40 INFO TaskSchedulerImpl: Adding task set 204.0 with 6 tasks
17/12/23 18:55:40 INFO TaskSetManager: Starting task 0.0 in stage 204.0 (TID 504, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:55:40 INFO Executor: Running task 0.0 in stage 204.0 (TID 504)
17/12/23 18:55:40 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:40 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:40 INFO ContextCleaner: Cleaned shuffle 42
17/12/23 18:55:40 INFO BlockManagerInfo: Removed broadcast_82_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:40 INFO BlockManagerInfo: Removed broadcast_83_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:40 INFO BlockManagerInfo: Removed broadcast_84_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:41 INFO Executor: Finished task 0.0 in stage 204.0 (TID 504). 2145 bytes result sent to driver
17/12/23 18:55:41 INFO TaskSetManager: Starting task 1.0 in stage 204.0 (TID 505, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:55:41 INFO Executor: Running task 1.0 in stage 204.0 (TID 505)
17/12/23 18:55:41 INFO TaskSetManager: Finished task 0.0 in stage 204.0 (TID 504) in 499 ms on localhost (executor driver) (1/6)
17/12/23 18:55:41 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:41 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:42 INFO Executor: Finished task 1.0 in stage 204.0 (TID 505). 2072 bytes result sent to driver
17/12/23 18:55:42 INFO TaskSetManager: Starting task 2.0 in stage 204.0 (TID 506, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:55:42 INFO Executor: Running task 2.0 in stage 204.0 (TID 506)
17/12/23 18:55:42 INFO TaskSetManager: Finished task 1.0 in stage 204.0 (TID 505) in 732 ms on localhost (executor driver) (2/6)
17/12/23 18:55:42 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:42 INFO Executor: Finished task 2.0 in stage 204.0 (TID 506). 2072 bytes result sent to driver
17/12/23 18:55:42 INFO TaskSetManager: Starting task 3.0 in stage 204.0 (TID 507, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:55:42 INFO Executor: Running task 3.0 in stage 204.0 (TID 507)
17/12/23 18:55:42 INFO TaskSetManager: Finished task 2.0 in stage 204.0 (TID 506) in 28 ms on localhost (executor driver) (3/6)
17/12/23 18:55:42 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:42 INFO Executor: Finished task 3.0 in stage 204.0 (TID 507). 1953 bytes result sent to driver
17/12/23 18:55:42 INFO TaskSetManager: Starting task 4.0 in stage 204.0 (TID 508, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:55:42 INFO Executor: Running task 4.0 in stage 204.0 (TID 508)
17/12/23 18:55:42 INFO TaskSetManager: Finished task 3.0 in stage 204.0 (TID 507) in 260 ms on localhost (executor driver) (4/6)
17/12/23 18:55:42 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:42 INFO Executor: Finished task 4.0 in stage 204.0 (TID 508). 1953 bytes result sent to driver
17/12/23 18:55:42 INFO TaskSetManager: Starting task 5.0 in stage 204.0 (TID 509, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:55:42 INFO Executor: Running task 5.0 in stage 204.0 (TID 509)
17/12/23 18:55:42 INFO TaskSetManager: Finished task 4.0 in stage 204.0 (TID 508) in 373 ms on localhost (executor driver) (5/6)
17/12/23 18:55:42 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:42 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:43 INFO Executor: Finished task 5.0 in stage 204.0 (TID 509). 1953 bytes result sent to driver
17/12/23 18:55:43 INFO TaskSetManager: Finished task 5.0 in stage 204.0 (TID 509) in 965 ms on localhost (executor driver) (6/6)
17/12/23 18:55:43 INFO TaskSchedulerImpl: Removed TaskSet 204.0, whose tasks have all completed, from pool 
17/12/23 18:55:43 INFO DAGScheduler: ResultStage 204 (collect at StackOverflow.scala:191) finished in 2.854 s
17/12/23 18:55:43 INFO DAGScheduler: Job 40 finished: collect at StackOverflow.scala:191, took 5.900283 s
Iteration: 40
  * current distance: 86.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2309) ==>             (0,2309)    distance:        0
                (0,466) ==>              (0,466)    distance:        0
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,292) ==>         (150000,292)    distance:        0
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,330) ==>          (50000,332)    distance:        4
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,94) ==>          (200000,95)    distance:        1
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,520) ==>         (200000,523)    distance:        9
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,207) ==>         (350000,209)    distance:        4
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,940) ==>         (350000,940)    distance:        0
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
          (100000,1263) ==>        (100000,1263)    distance:        0
           (100000,179) ==>         (100000,181)    distance:        4
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1766) ==>        (250000,1766)    distance:        0
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,256) ==>         (250000,264)    distance:       64
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:55:43 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:55:43 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:55:43 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:55:43 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:55:43 INFO DAGScheduler: Registering RDD 138 (map at StackOverflow.scala:186)
17/12/23 18:55:43 INFO DAGScheduler: Got job 41 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:55:43 INFO DAGScheduler: Final stage: ResultStage 209 (collect at StackOverflow.scala:191)
17/12/23 18:55:43 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 208)
17/12/23 18:55:43 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 208)
17/12/23 18:55:43 INFO DAGScheduler: Submitting ShuffleMapStage 208 (MapPartitionsRDD[138] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:55:43 INFO MemoryStore: Block broadcast_86 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:55:43 INFO MemoryStore: Block broadcast_86_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:55:43 INFO BlockManagerInfo: Added broadcast_86_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:43 INFO SparkContext: Created broadcast 86 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:43 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 208 (MapPartitionsRDD[138] at map at StackOverflow.scala:186)
17/12/23 18:55:43 INFO TaskSchedulerImpl: Adding task set 208.0 with 6 tasks
17/12/23 18:55:43 INFO TaskSetManager: Starting task 0.0 in stage 208.0 (TID 510, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:43 INFO Executor: Running task 0.0 in stage 208.0 (TID 510)
17/12/23 18:55:43 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:55:44 INFO Executor: Finished task 0.0 in stage 208.0 (TID 510). 1325 bytes result sent to driver
17/12/23 18:55:44 INFO TaskSetManager: Starting task 1.0 in stage 208.0 (TID 511, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:44 INFO TaskSetManager: Finished task 0.0 in stage 208.0 (TID 510) in 535 ms on localhost (executor driver) (1/6)
17/12/23 18:55:44 INFO Executor: Running task 1.0 in stage 208.0 (TID 511)
17/12/23 18:55:44 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:55:44 INFO Executor: Finished task 1.0 in stage 208.0 (TID 511). 1325 bytes result sent to driver
17/12/23 18:55:44 INFO TaskSetManager: Starting task 2.0 in stage 208.0 (TID 512, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:44 INFO Executor: Running task 2.0 in stage 208.0 (TID 512)
17/12/23 18:55:44 INFO TaskSetManager: Finished task 1.0 in stage 208.0 (TID 511) in 497 ms on localhost (executor driver) (2/6)
17/12/23 18:55:44 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:55:45 INFO Executor: Finished task 2.0 in stage 208.0 (TID 512). 1415 bytes result sent to driver
17/12/23 18:55:45 INFO TaskSetManager: Starting task 3.0 in stage 208.0 (TID 513, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:45 INFO Executor: Running task 3.0 in stage 208.0 (TID 513)
17/12/23 18:55:45 INFO TaskSetManager: Finished task 2.0 in stage 208.0 (TID 512) in 513 ms on localhost (executor driver) (3/6)
17/12/23 18:55:45 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:55:45 INFO Executor: Finished task 3.0 in stage 208.0 (TID 513). 1238 bytes result sent to driver
17/12/23 18:55:45 INFO TaskSetManager: Starting task 4.0 in stage 208.0 (TID 514, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:45 INFO Executor: Running task 4.0 in stage 208.0 (TID 514)
17/12/23 18:55:45 INFO TaskSetManager: Finished task 3.0 in stage 208.0 (TID 513) in 525 ms on localhost (executor driver) (4/6)
17/12/23 18:55:45 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:55:46 INFO Executor: Finished task 4.0 in stage 208.0 (TID 514). 1325 bytes result sent to driver
17/12/23 18:55:46 INFO TaskSetManager: Starting task 5.0 in stage 208.0 (TID 515, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:46 INFO Executor: Running task 5.0 in stage 208.0 (TID 515)
17/12/23 18:55:46 INFO TaskSetManager: Finished task 4.0 in stage 208.0 (TID 514) in 493 ms on localhost (executor driver) (5/6)
17/12/23 18:55:46 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:55:46 INFO Executor: Finished task 5.0 in stage 208.0 (TID 515). 1415 bytes result sent to driver
17/12/23 18:55:46 INFO TaskSetManager: Finished task 5.0 in stage 208.0 (TID 515) in 500 ms on localhost (executor driver) (6/6)
17/12/23 18:55:46 INFO TaskSchedulerImpl: Removed TaskSet 208.0, whose tasks have all completed, from pool 
17/12/23 18:55:46 INFO DAGScheduler: ShuffleMapStage 208 (map at StackOverflow.scala:186) finished in 3.060 s
17/12/23 18:55:46 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:55:46 INFO DAGScheduler: running: Set()
17/12/23 18:55:46 INFO DAGScheduler: waiting: Set(ResultStage 209)
17/12/23 18:55:46 INFO DAGScheduler: failed: Set()
17/12/23 18:55:46 INFO DAGScheduler: Submitting ResultStage 209 (MapPartitionsRDD[140] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:55:46 INFO MemoryStore: Block broadcast_87 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:55:46 INFO MemoryStore: Block broadcast_87_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:55:46 INFO BlockManagerInfo: Added broadcast_87_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:46 INFO SparkContext: Created broadcast 87 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:46 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 209 (MapPartitionsRDD[140] at mapValues at StackOverflow.scala:190)
17/12/23 18:55:46 INFO TaskSchedulerImpl: Adding task set 209.0 with 6 tasks
17/12/23 18:55:46 INFO TaskSetManager: Starting task 0.0 in stage 209.0 (TID 516, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:55:46 INFO Executor: Running task 0.0 in stage 209.0 (TID 516)
17/12/23 18:55:46 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:46 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:55:47 INFO Executor: Finished task 0.0 in stage 209.0 (TID 516). 1985 bytes result sent to driver
17/12/23 18:55:47 INFO TaskSetManager: Starting task 1.0 in stage 209.0 (TID 517, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:55:47 INFO Executor: Running task 1.0 in stage 209.0 (TID 517)
17/12/23 18:55:47 INFO TaskSetManager: Finished task 0.0 in stage 209.0 (TID 516) in 472 ms on localhost (executor driver) (1/6)
17/12/23 18:55:47 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:47 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:47 INFO ContextCleaner: Cleaned shuffle 43
17/12/23 18:55:47 INFO BlockManagerInfo: Removed broadcast_85_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:48 INFO BlockManagerInfo: Removed broadcast_86_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:48 INFO Executor: Finished task 1.0 in stage 209.0 (TID 517). 2145 bytes result sent to driver
17/12/23 18:55:48 INFO TaskSetManager: Starting task 2.0 in stage 209.0 (TID 518, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:55:48 INFO Executor: Running task 2.0 in stage 209.0 (TID 518)
17/12/23 18:55:48 INFO TaskSetManager: Finished task 1.0 in stage 209.0 (TID 517) in 780 ms on localhost (executor driver) (2/6)
17/12/23 18:55:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:48 INFO Executor: Finished task 2.0 in stage 209.0 (TID 518). 2072 bytes result sent to driver
17/12/23 18:55:48 INFO TaskSetManager: Starting task 3.0 in stage 209.0 (TID 519, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:55:48 INFO Executor: Running task 3.0 in stage 209.0 (TID 519)
17/12/23 18:55:48 INFO TaskSetManager: Finished task 2.0 in stage 209.0 (TID 518) in 37 ms on localhost (executor driver) (3/6)
17/12/23 18:55:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 1 ms
17/12/23 18:55:48 INFO Executor: Finished task 3.0 in stage 209.0 (TID 519). 2040 bytes result sent to driver
17/12/23 18:55:48 INFO TaskSetManager: Starting task 4.0 in stage 209.0 (TID 520, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:55:48 INFO TaskSetManager: Finished task 3.0 in stage 209.0 (TID 519) in 323 ms on localhost (executor driver) (4/6)
17/12/23 18:55:48 INFO Executor: Running task 4.0 in stage 209.0 (TID 520)
17/12/23 18:55:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:48 INFO Executor: Finished task 4.0 in stage 209.0 (TID 520). 1953 bytes result sent to driver
17/12/23 18:55:48 INFO TaskSetManager: Starting task 5.0 in stage 209.0 (TID 521, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:55:48 INFO Executor: Running task 5.0 in stage 209.0 (TID 521)
17/12/23 18:55:48 INFO TaskSetManager: Finished task 4.0 in stage 209.0 (TID 520) in 360 ms on localhost (executor driver) (5/6)
17/12/23 18:55:48 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:48 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:49 INFO Executor: Finished task 5.0 in stage 209.0 (TID 521). 2040 bytes result sent to driver
17/12/23 18:55:49 INFO TaskSetManager: Finished task 5.0 in stage 209.0 (TID 521) in 951 ms on localhost (executor driver) (6/6)
17/12/23 18:55:49 INFO TaskSchedulerImpl: Removed TaskSet 209.0, whose tasks have all completed, from pool 
17/12/23 18:55:49 INFO DAGScheduler: ResultStage 209 (collect at StackOverflow.scala:191) finished in 2.917 s
17/12/23 18:55:49 INFO DAGScheduler: Job 41 finished: collect at StackOverflow.scala:191, took 5.988091 s
Iteration: 41
  * current distance: 49.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2309) ==>             (0,2309)    distance:        0
                (0,466) ==>              (0,466)    distance:        0
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,292) ==>         (150000,292)    distance:        0
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,332) ==>          (50000,333)    distance:        1
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,95) ==>          (200000,96)    distance:        1
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,523) ==>         (200000,526)    distance:        9
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,209) ==>         (350000,210)    distance:        1
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,940) ==>         (350000,940)    distance:        0
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
          (100000,1263) ==>        (100000,1263)    distance:        0
           (100000,181) ==>         (100000,182)    distance:        1
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1766) ==>        (250000,1766)    distance:        0
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,264) ==>         (250000,270)    distance:       36
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:55:49 INFO SparkContext: Starting job: collect at StackOverflow.scala:191
17/12/23 18:55:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:55:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:55:49 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:55:49 INFO DAGScheduler: Registering RDD 141 (map at StackOverflow.scala:186)
17/12/23 18:55:49 INFO DAGScheduler: Got job 42 (collect at StackOverflow.scala:191) with 6 output partitions
17/12/23 18:55:49 INFO DAGScheduler: Final stage: ResultStage 214 (collect at StackOverflow.scala:191)
17/12/23 18:55:49 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 213)
17/12/23 18:55:49 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 213)
17/12/23 18:55:49 INFO DAGScheduler: Submitting ShuffleMapStage 213 (MapPartitionsRDD[141] at map at StackOverflow.scala:186), which has no missing parents
17/12/23 18:55:49 INFO MemoryStore: Block broadcast_88 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:55:49 INFO MemoryStore: Block broadcast_88_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:55:49 INFO BlockManagerInfo: Added broadcast_88_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:49 INFO SparkContext: Created broadcast 88 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:49 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 213 (MapPartitionsRDD[141] at map at StackOverflow.scala:186)
17/12/23 18:55:49 INFO TaskSchedulerImpl: Adding task set 213.0 with 6 tasks
17/12/23 18:55:49 INFO TaskSetManager: Starting task 0.0 in stage 213.0 (TID 522, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:49 INFO Executor: Running task 0.0 in stage 213.0 (TID 522)
17/12/23 18:55:49 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:55:50 INFO Executor: Finished task 0.0 in stage 213.0 (TID 522). 1325 bytes result sent to driver
17/12/23 18:55:50 INFO TaskSetManager: Starting task 1.0 in stage 213.0 (TID 523, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:50 INFO Executor: Running task 1.0 in stage 213.0 (TID 523)
17/12/23 18:55:50 INFO TaskSetManager: Finished task 0.0 in stage 213.0 (TID 522) in 512 ms on localhost (executor driver) (1/6)
17/12/23 18:55:50 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:55:50 INFO Executor: Finished task 1.0 in stage 213.0 (TID 523). 1238 bytes result sent to driver
17/12/23 18:55:50 INFO TaskSetManager: Starting task 2.0 in stage 213.0 (TID 524, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:50 INFO Executor: Running task 2.0 in stage 213.0 (TID 524)
17/12/23 18:55:50 INFO TaskSetManager: Finished task 1.0 in stage 213.0 (TID 523) in 515 ms on localhost (executor driver) (2/6)
17/12/23 18:55:50 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:55:51 INFO Executor: Finished task 2.0 in stage 213.0 (TID 524). 1238 bytes result sent to driver
17/12/23 18:55:51 INFO TaskSetManager: Starting task 3.0 in stage 213.0 (TID 525, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:51 INFO Executor: Running task 3.0 in stage 213.0 (TID 525)
17/12/23 18:55:51 INFO TaskSetManager: Finished task 2.0 in stage 213.0 (TID 524) in 490 ms on localhost (executor driver) (3/6)
17/12/23 18:55:51 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:55:51 INFO Executor: Finished task 3.0 in stage 213.0 (TID 525). 1325 bytes result sent to driver
17/12/23 18:55:51 INFO TaskSetManager: Starting task 4.0 in stage 213.0 (TID 526, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:51 INFO TaskSetManager: Finished task 3.0 in stage 213.0 (TID 525) in 505 ms on localhost (executor driver) (4/6)
17/12/23 18:55:51 INFO Executor: Running task 4.0 in stage 213.0 (TID 526)
17/12/23 18:55:51 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:55:52 INFO Executor: Finished task 4.0 in stage 213.0 (TID 526). 1325 bytes result sent to driver
17/12/23 18:55:52 INFO TaskSetManager: Starting task 5.0 in stage 213.0 (TID 527, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:52 INFO Executor: Running task 5.0 in stage 213.0 (TID 527)
17/12/23 18:55:52 INFO TaskSetManager: Finished task 4.0 in stage 213.0 (TID 526) in 530 ms on localhost (executor driver) (5/6)
17/12/23 18:55:52 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:55:52 INFO Executor: Finished task 5.0 in stage 213.0 (TID 527). 1325 bytes result sent to driver
17/12/23 18:55:52 INFO TaskSetManager: Finished task 5.0 in stage 213.0 (TID 527) in 508 ms on localhost (executor driver) (6/6)
17/12/23 18:55:52 INFO TaskSchedulerImpl: Removed TaskSet 213.0, whose tasks have all completed, from pool 
17/12/23 18:55:52 INFO DAGScheduler: ShuffleMapStage 213 (map at StackOverflow.scala:186) finished in 3.060 s
17/12/23 18:55:52 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:55:52 INFO DAGScheduler: running: Set()
17/12/23 18:55:52 INFO DAGScheduler: waiting: Set(ResultStage 214)
17/12/23 18:55:52 INFO DAGScheduler: failed: Set()
17/12/23 18:55:52 INFO DAGScheduler: Submitting ResultStage 214 (MapPartitionsRDD[143] at mapValues at StackOverflow.scala:190), which has no missing parents
17/12/23 18:55:52 INFO MemoryStore: Block broadcast_89 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:55:52 INFO MemoryStore: Block broadcast_89_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:55:52 INFO BlockManagerInfo: Added broadcast_89_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:52 INFO SparkContext: Created broadcast 89 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:52 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 214 (MapPartitionsRDD[143] at mapValues at StackOverflow.scala:190)
17/12/23 18:55:52 INFO TaskSchedulerImpl: Adding task set 214.0 with 6 tasks
17/12/23 18:55:52 INFO TaskSetManager: Starting task 0.0 in stage 214.0 (TID 528, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:55:52 INFO Executor: Running task 0.0 in stage 214.0 (TID 528)
17/12/23 18:55:52 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:52 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:53 INFO Executor: Finished task 0.0 in stage 214.0 (TID 528). 2072 bytes result sent to driver
17/12/23 18:55:53 INFO TaskSetManager: Starting task 1.0 in stage 214.0 (TID 529, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:55:53 INFO Executor: Running task 1.0 in stage 214.0 (TID 529)
17/12/23 18:55:53 INFO TaskSetManager: Finished task 0.0 in stage 214.0 (TID 528) in 470 ms on localhost (executor driver) (1/6)
17/12/23 18:55:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:53 INFO Executor: Finished task 1.0 in stage 214.0 (TID 529). 2072 bytes result sent to driver
17/12/23 18:55:53 INFO TaskSetManager: Starting task 2.0 in stage 214.0 (TID 530, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:55:53 INFO Executor: Running task 2.0 in stage 214.0 (TID 530)
17/12/23 18:55:53 INFO TaskSetManager: Finished task 1.0 in stage 214.0 (TID 529) in 745 ms on localhost (executor driver) (2/6)
17/12/23 18:55:53 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:53 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:55:54 INFO Executor: Finished task 2.0 in stage 214.0 (TID 530). 1985 bytes result sent to driver
17/12/23 18:55:54 INFO TaskSetManager: Starting task 3.0 in stage 214.0 (TID 531, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:55:54 INFO Executor: Running task 3.0 in stage 214.0 (TID 531)
17/12/23 18:55:54 INFO TaskSetManager: Finished task 2.0 in stage 214.0 (TID 530) in 30 ms on localhost (executor driver) (3/6)
17/12/23 18:55:54 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 2 ms
17/12/23 18:55:54 INFO Executor: Finished task 3.0 in stage 214.0 (TID 531). 1953 bytes result sent to driver
17/12/23 18:55:54 INFO TaskSetManager: Starting task 4.0 in stage 214.0 (TID 532, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:55:54 INFO Executor: Running task 4.0 in stage 214.0 (TID 532)
17/12/23 18:55:54 INFO TaskSetManager: Finished task 3.0 in stage 214.0 (TID 531) in 232 ms on localhost (executor driver) (4/6)
17/12/23 18:55:54 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:54 INFO Executor: Finished task 4.0 in stage 214.0 (TID 532). 2127 bytes result sent to driver
17/12/23 18:55:54 INFO TaskSetManager: Starting task 5.0 in stage 214.0 (TID 533, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:55:54 INFO Executor: Running task 5.0 in stage 214.0 (TID 533)
17/12/23 18:55:54 INFO TaskSetManager: Finished task 4.0 in stage 214.0 (TID 532) in 375 ms on localhost (executor driver) (5/6)
17/12/23 18:55:54 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:54 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:55 INFO ContextCleaner: Cleaned shuffle 44
17/12/23 18:55:55 INFO BlockManagerInfo: Removed broadcast_87_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:55 INFO BlockManagerInfo: Removed broadcast_88_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:55 INFO Executor: Finished task 5.0 in stage 214.0 (TID 533). 2026 bytes result sent to driver
17/12/23 18:55:55 INFO TaskSetManager: Finished task 5.0 in stage 214.0 (TID 533) in 1008 ms on localhost (executor driver) (6/6)
17/12/23 18:55:55 INFO TaskSchedulerImpl: Removed TaskSet 214.0, whose tasks have all completed, from pool 
17/12/23 18:55:55 INFO DAGScheduler: ResultStage 214 (collect at StackOverflow.scala:191) finished in 2.860 s
17/12/23 18:55:55 INFO DAGScheduler: Job 42 finished: collect at StackOverflow.scala:191, took 5.931088 s
Iteration: 42
  * current distance: 14.0
  * desired distance: 20.0
  * means:
             (450000,6) ==>           (450000,6)    distance:        0
             (450000,0) ==>           (450000,0)    distance:        0
            (450000,87) ==>          (450000,87)    distance:        0
               (0,2309) ==>             (0,2309)    distance:        0
                (0,466) ==>              (0,466)    distance:        0
                  (0,2) ==>                (0,2)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,292) ==>         (150000,292)    distance:        0
          (150000,1629) ==>        (150000,1629)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,108) ==>         (300000,108)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,333) ==>          (50000,334)    distance:        1
              (50000,2) ==>            (50000,2)    distance:        0
            (200000,96) ==>          (200000,96)    distance:        0
             (200000,2) ==>           (200000,2)    distance:        0
           (200000,526) ==>         (200000,529)    distance:        9
             (500000,3) ==>           (500000,3)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,32) ==>          (500000,32)    distance:        0
           (350000,210) ==>         (350000,210)    distance:        0
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,940) ==>         (350000,940)    distance:        0
            (650000,74) ==>          (650000,74)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
             (100000,1) ==>           (100000,1)    distance:        0
          (100000,1263) ==>        (100000,1263)    distance:        0
           (100000,182) ==>         (100000,182)    distance:        0
           (400000,584) ==>         (400000,584)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
          (250000,1766) ==>        (250000,1766)    distance:        0
             (250000,3) ==>           (250000,3)    distance:        0
           (250000,270) ==>         (250000,272)    distance:        4
             (700000,1) ==>           (700000,1)    distance:        0
            (700000,73) ==>          (700000,73)    distance:        0
            (700000,12) ==>          (700000,12)    distance:        0
17/12/23 18:55:55 INFO SparkContext: Starting job: collect at StackOverflow.scala:316
17/12/23 18:55:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 159 bytes
17/12/23 18:55:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 157 bytes
17/12/23 18:55:55 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 160 bytes
17/12/23 18:55:55 INFO DAGScheduler: Registering RDD 144 (map at StackOverflow.scala:287)
17/12/23 18:55:55 INFO DAGScheduler: Got job 43 (collect at StackOverflow.scala:316) with 6 output partitions
17/12/23 18:55:55 INFO DAGScheduler: Final stage: ResultStage 219 (collect at StackOverflow.scala:316)
17/12/23 18:55:55 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 218)
17/12/23 18:55:55 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 218)
17/12/23 18:55:55 INFO DAGScheduler: Submitting ShuffleMapStage 218 (MapPartitionsRDD[144] at map at StackOverflow.scala:287), which has no missing parents
17/12/23 18:55:55 INFO MemoryStore: Block broadcast_90 stored as values in memory (estimated size 7.3 KB, free 1916.6 MB)
17/12/23 18:55:55 INFO MemoryStore: Block broadcast_90_piece0 stored as bytes in memory (estimated size 3.6 KB, free 1916.6 MB)
17/12/23 18:55:55 INFO BlockManagerInfo: Added broadcast_90_piece0 in memory on 10.0.0.115:50865 (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:55:55 INFO SparkContext: Created broadcast 90 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:55 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 218 (MapPartitionsRDD[144] at map at StackOverflow.scala:287)
17/12/23 18:55:55 INFO TaskSchedulerImpl: Adding task set 218.0 with 6 tasks
17/12/23 18:55:55 INFO TaskSetManager: Starting task 0.0 in stage 218.0 (TID 534, localhost, executor driver, partition 0, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:55 INFO Executor: Running task 0.0 in stage 218.0 (TID 534)
17/12/23 18:55:55 INFO BlockManager: Found block rdd_15_0 locally
17/12/23 18:55:56 INFO Executor: Finished task 0.0 in stage 218.0 (TID 534). 1238 bytes result sent to driver
17/12/23 18:55:56 INFO TaskSetManager: Starting task 1.0 in stage 218.0 (TID 535, localhost, executor driver, partition 1, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:56 INFO Executor: Running task 1.0 in stage 218.0 (TID 535)
17/12/23 18:55:56 INFO TaskSetManager: Finished task 0.0 in stage 218.0 (TID 534) in 624 ms on localhost (executor driver) (1/6)
17/12/23 18:55:56 INFO BlockManager: Found block rdd_15_1 locally
17/12/23 18:55:56 INFO Executor: Finished task 1.0 in stage 218.0 (TID 535). 1325 bytes result sent to driver
17/12/23 18:55:56 INFO TaskSetManager: Starting task 2.0 in stage 218.0 (TID 536, localhost, executor driver, partition 2, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:56 INFO Executor: Running task 2.0 in stage 218.0 (TID 536)
17/12/23 18:55:56 INFO TaskSetManager: Finished task 1.0 in stage 218.0 (TID 535) in 512 ms on localhost (executor driver) (2/6)
17/12/23 18:55:56 INFO BlockManager: Found block rdd_15_2 locally
17/12/23 18:55:57 INFO Executor: Finished task 2.0 in stage 218.0 (TID 536). 1238 bytes result sent to driver
17/12/23 18:55:57 INFO TaskSetManager: Starting task 3.0 in stage 218.0 (TID 537, localhost, executor driver, partition 3, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:57 INFO Executor: Running task 3.0 in stage 218.0 (TID 537)
17/12/23 18:55:57 INFO TaskSetManager: Finished task 2.0 in stage 218.0 (TID 536) in 505 ms on localhost (executor driver) (3/6)
17/12/23 18:55:57 INFO BlockManager: Found block rdd_15_3 locally
17/12/23 18:55:57 INFO Executor: Finished task 3.0 in stage 218.0 (TID 537). 1238 bytes result sent to driver
17/12/23 18:55:57 INFO TaskSetManager: Starting task 4.0 in stage 218.0 (TID 538, localhost, executor driver, partition 4, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:57 INFO Executor: Running task 4.0 in stage 218.0 (TID 538)
17/12/23 18:55:57 INFO TaskSetManager: Finished task 3.0 in stage 218.0 (TID 537) in 490 ms on localhost (executor driver) (4/6)
17/12/23 18:55:57 INFO BlockManager: Found block rdd_15_4 locally
17/12/23 18:55:58 INFO Executor: Finished task 4.0 in stage 218.0 (TID 538). 1325 bytes result sent to driver
17/12/23 18:55:58 INFO TaskSetManager: Starting task 5.0 in stage 218.0 (TID 539, localhost, executor driver, partition 5, PROCESS_LOCAL, 5741 bytes)
17/12/23 18:55:58 INFO Executor: Running task 5.0 in stage 218.0 (TID 539)
17/12/23 18:55:58 INFO TaskSetManager: Finished task 4.0 in stage 218.0 (TID 538) in 503 ms on localhost (executor driver) (5/6)
17/12/23 18:55:58 INFO BlockManager: Found block rdd_15_5 locally
17/12/23 18:55:58 INFO Executor: Finished task 5.0 in stage 218.0 (TID 539). 1325 bytes result sent to driver
17/12/23 18:55:58 INFO TaskSetManager: Finished task 5.0 in stage 218.0 (TID 539) in 525 ms on localhost (executor driver) (6/6)
17/12/23 18:55:58 INFO TaskSchedulerImpl: Removed TaskSet 218.0, whose tasks have all completed, from pool 
17/12/23 18:55:58 INFO DAGScheduler: ShuffleMapStage 218 (map at StackOverflow.scala:287) finished in 3.159 s
17/12/23 18:55:58 INFO DAGScheduler: looking for newly runnable stages
17/12/23 18:55:58 INFO DAGScheduler: running: Set()
17/12/23 18:55:58 INFO DAGScheduler: waiting: Set(ResultStage 219)
17/12/23 18:55:58 INFO DAGScheduler: failed: Set()
17/12/23 18:55:58 INFO DAGScheduler: Submitting ResultStage 219 (MapPartitionsRDD[146] at mapValues at StackOverflow.scala:290), which has no missing parents
17/12/23 18:55:58 INFO MemoryStore: Block broadcast_91 stored as values in memory (estimated size 7.9 KB, free 1916.6 MB)
17/12/23 18:55:58 INFO MemoryStore: Block broadcast_91_piece0 stored as bytes in memory (estimated size 3.8 KB, free 1916.6 MB)
17/12/23 18:55:58 INFO BlockManagerInfo: Added broadcast_91_piece0 in memory on 10.0.0.115:50865 (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:55:58 INFO SparkContext: Created broadcast 91 from broadcast at DAGScheduler.scala:996
17/12/23 18:55:58 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 219 (MapPartitionsRDD[146] at mapValues at StackOverflow.scala:290)
17/12/23 18:55:58 INFO TaskSchedulerImpl: Adding task set 219.0 with 6 tasks
17/12/23 18:55:58 INFO TaskSetManager: Starting task 0.0 in stage 219.0 (TID 540, localhost, executor driver, partition 0, ANY, 5752 bytes)
17/12/23 18:55:58 INFO Executor: Running task 0.0 in stage 219.0 (TID 540)
17/12/23 18:55:58 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:58 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:55:59 INFO Executor: Finished task 0.0 in stage 219.0 (TID 540). 2443 bytes result sent to driver
17/12/23 18:55:59 INFO TaskSetManager: Starting task 1.0 in stage 219.0 (TID 541, localhost, executor driver, partition 1, ANY, 5752 bytes)
17/12/23 18:55:59 INFO Executor: Running task 1.0 in stage 219.0 (TID 541)
17/12/23 18:55:59 INFO TaskSetManager: Finished task 0.0 in stage 219.0 (TID 540) in 694 ms on localhost (executor driver) (1/6)
17/12/23 18:55:59 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:55:59 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:56:00 INFO Executor: Finished task 1.0 in stage 219.0 (TID 541). 2351 bytes result sent to driver
17/12/23 18:56:00 INFO TaskSetManager: Starting task 2.0 in stage 219.0 (TID 542, localhost, executor driver, partition 2, ANY, 5752 bytes)
17/12/23 18:56:00 INFO Executor: Running task 2.0 in stage 219.0 (TID 542)
17/12/23 18:56:00 INFO TaskSetManager: Finished task 1.0 in stage 219.0 (TID 541) in 969 ms on localhost (executor driver) (2/6)
17/12/23 18:56:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:56:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:56:00 INFO Executor: Finished task 2.0 in stage 219.0 (TID 542). 2451 bytes result sent to driver
17/12/23 18:56:00 INFO TaskSetManager: Starting task 3.0 in stage 219.0 (TID 543, localhost, executor driver, partition 3, ANY, 5752 bytes)
17/12/23 18:56:00 INFO Executor: Running task 3.0 in stage 219.0 (TID 543)
17/12/23 18:56:00 INFO TaskSetManager: Finished task 2.0 in stage 219.0 (TID 542) in 35 ms on localhost (executor driver) (3/6)
17/12/23 18:56:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:56:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:56:00 INFO Executor: Finished task 3.0 in stage 219.0 (TID 543). 2308 bytes result sent to driver
17/12/23 18:56:00 INFO TaskSetManager: Starting task 4.0 in stage 219.0 (TID 544, localhost, executor driver, partition 4, ANY, 5752 bytes)
17/12/23 18:56:00 INFO Executor: Running task 4.0 in stage 219.0 (TID 544)
17/12/23 18:56:00 INFO TaskSetManager: Finished task 3.0 in stage 219.0 (TID 543) in 290 ms on localhost (executor driver) (4/6)
17/12/23 18:56:00 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:56:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:56:01 INFO Executor: Finished task 4.0 in stage 219.0 (TID 544). 2226 bytes result sent to driver
17/12/23 18:56:01 INFO TaskSetManager: Starting task 5.0 in stage 219.0 (TID 545, localhost, executor driver, partition 5, ANY, 5752 bytes)
17/12/23 18:56:01 INFO Executor: Running task 5.0 in stage 219.0 (TID 545)
17/12/23 18:56:01 INFO TaskSetManager: Finished task 4.0 in stage 219.0 (TID 544) in 472 ms on localhost (executor driver) (5/6)
17/12/23 18:56:01 INFO ShuffleBlockFetcherIterator: Getting 6 non-empty blocks out of 6 blocks
17/12/23 18:56:01 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
17/12/23 18:56:01 INFO ContextCleaner: Cleaned shuffle 45
17/12/23 18:56:01 INFO BlockManagerInfo: Removed broadcast_89_piece0 on 10.0.0.115:50865 in memory (size: 3.8 KB, free: 1916.7 MB)
17/12/23 18:56:01 INFO BlockManagerInfo: Removed broadcast_90_piece0 on 10.0.0.115:50865 in memory (size: 3.6 KB, free: 1916.7 MB)
17/12/23 18:56:02 INFO Executor: Finished task 5.0 in stage 219.0 (TID 545). 2386 bytes result sent to driver
17/12/23 18:56:02 INFO TaskSetManager: Finished task 5.0 in stage 219.0 (TID 545) in 1259 ms on localhost (executor driver) (6/6)
17/12/23 18:56:02 INFO TaskSchedulerImpl: Removed TaskSet 219.0, whose tasks have all completed, from pool 
17/12/23 18:56:02 INFO DAGScheduler: ResultStage 219 (collect at StackOverflow.scala:316) finished in 3.718 s
17/12/23 18:56:02 INFO DAGScheduler: Job 43 finished: collect at StackOverflow.scala:316, took 6.887615 s
Resulting clusters:
  Score  Dominant language (%percent)  Questions
================================================
      0  MATLAB            (100.0%)         3725
      1  PHP               (100.0%)       315771
      1  Groovy            (100.0%)         2729
      1  C#                (100.0%)       361687
      1  Ruby              (100.0%)        54756
      1  CSS               (100.0%)       113597
      1  Perl              (100.0%)        12938
      1  Objective-C       (100.0%)        94745
      1  Java              (100.0%)       383473
      1  JavaScript        (100.0%)       365647
      2  MATLAB            (100.0%)         7989
      2  Scala             (100.0%)        12472
      2  Python            (100.0%)       174586
      2  Clojure           (100.0%)         3441
      2  C++               (100.0%)       181258
      4  Haskell           (100.0%)        10362
      5  Perl              (100.0%)        10979
      5  MATLAB            (100.0%)         2774
     10  Groovy            (100.0%)          310
     14  Clojure           (100.0%)          595
     27  Scala             (100.0%)          679
     53  Haskell           (100.0%)          202
     61  Groovy            (100.0%)           14
     66  Perl              (100.0%)           84
     66  Clojure           (100.0%)           57
     76  C#                (100.0%)         2732
     87  Ruby              (100.0%)          619
     97  Objective-C       (100.0%)          784
    130  Scala             (100.0%)           47
    139  PHP               (100.0%)          475
    172  CSS               (100.0%)          359
    215  C++               (100.0%)          261
    227  Python            (100.0%)          400
    249  Java              (100.0%)          483
    375  JavaScript        (100.0%)          433
    439  C#                (100.0%)          148
    503  Objective-C       (100.0%)           73
    546  Ruby              (100.0%)           34
    766  CSS               (100.0%)           26
    887  PHP               (100.0%)           13
   1130  Haskell           (100.0%)            2
   1269  Python            (100.0%)           19
   1290  C++               (100.0%)            9
   1895  JavaScript        (100.0%)           33
  10271  Java              (100.0%)            2
17/12/23 18:56:02 INFO SparkContext: Invoking stop() from shutdown hook
17/12/23 18:56:02 INFO SparkUI: Stopped Spark web UI at http://10.0.0.115:4040
17/12/23 18:56:02 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
17/12/23 18:56:03 INFO MemoryStore: MemoryStore cleared
17/12/23 18:56:03 INFO BlockManager: BlockManager stopped
17/12/23 18:56:03 INFO BlockManagerMaster: BlockManagerMaster stopped
17/12/23 18:56:03 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
17/12/23 18:56:03 INFO SparkContext: Successfully stopped SparkContext
17/12/23 18:56:03 INFO ShutdownHookManager: Shutdown hook called
17/12/23 18:56:03 INFO ShutdownHookManager: Deleting directory C:\Users\sjcsthia\AppData\Local\Temp\spark-a8946d8b-6346-46aa-9a48-fe0c6bfeb2ee

Process finished with exit code 0
