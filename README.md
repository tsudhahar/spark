**Tips and Tricks**

The Spark documentation includes good tips on tuning and is available at
http://spark.apache.org/docs/latest/tuning.html.

**Where to find logs**

Spark has very useful logs to figure out what's going on when things are not
going as expected. Spark keeps a per machine log on each machine by default in
the SPARK\_HOME/work subdirectory. Spark's web UI provides a convenient place to
see STDOUT and STDERR of each job, running and completed jobs, separated out per
worker.

**Concurrency limitations**

Spark's concurrency for operations is limited by the number of partitions.
Conversely, having too many partitions can cause excess overhead by launching
too many tasks. If you have too many partitions, you can shrink it by using the
coalesce(numPartitions,shuffle) method. The coalesce method is a good method to
pack and rebalance your RDDs (for example, after a filter operation where you
have less data after the action). If the new number of partitions is more than
what you have now, set shuffle=True, else set shuffle=false. While creating a
new RDD, you can specify the number of partitions to be used. Also, the
grouping/joining mechanism on RDDs of pairs can take the number of partitions or
a custom partitioner class. The default number of partitions for new RDDs is
controlled by spark.default.parallelism, which also controls the number of tasks
used by groupByKey and other shuffle operations that need shuffling.

**Memory usage and garbage collection**

To measure the impact of garbage collection, you can ask the JVM to print
details about the garbage collection. You can do this by adding -verbose:gc
-XX:+PrintGCDetails -XX:+PrintGCTimeStamps to your SPARK\_JAVA\_OPTS in
conf/spark-env.sh. You can also include the -Xloggc option to print the log
messages to a separate file so that log messages are kept separate. The details
will then be printed to the standard out when you run your job, which will be
available as described in the first section of this chapter.

If you find that your Spark cluster uses too much time collecting garbage, you
can reduce the amount of space used for RDD caching by changing spark.storage.
memoryFraction; here, the default is 0.6. If you are planning to run Spark for a
long time on a cluster, you may wish to enable spark.cleaner.ttl. By default,
Spark does not clean up any metadata (stages generated, tasks generated, and so
on); set this to a non-zero value in seconds to clean up the metadata after that
length of time. The documentation page
(https://spark.apache.org/docs/latest/configuration. html) has the default
settings and details about all the configuration options.

You can also control the RDD storage level if you find that you use too much
memory. I usually use top to see the memory consumption of the processes. If
your RDDs don't fit within memory and you still wish to cache them, you can try
using a different storage level shown as follows (also check the documentation
page for the latest information on RDD persistence options at
http://spark.apache.org/docs/ latest/programming-guide.html\#rdd-persistence):

1.  MEMORY\_ONLY: This stores the entire RDD in memory if it can, which is the
    default

    MEMORY\_AND\_DISK: This stores each partition in memory if it can fit; else
    it stores it on disk

    DISK\_ONLY: This stores each partition on disk regardless of whether it can
    fit in memory

These options are set when you call the persist function (rdd.persist()) on your
RDD. By default, the RDDs are stored in a deserialized form, which requires less
parsing. We can save space by adding \_SER to the storage level (for example,
MEMORY\_ONLY\_SER, MEMORY\_AND\_DISK\_SER), in which case Spark will serialize
the data to be stored, which normally saves some space but increases the
execution time.

**Serialization**

Spark supports different serialization mechanisms; the choice is a trade-off
between speed, space efficiency, and full support of all Java objects. If you
are using the serializer to cache your RDDs, you should strongly consider a fast
serializer. The default serializer uses Java's default serialization. The
KyroSerializer is much faster and generally uses about one tenth of the memory
as the default serializer. You can switch the serializer by setting
spark.serializer to spark.KryoSerializer. If you want to use KyroSerializer, you
need to make sure that the classes are serializable by KyroSerializer. Spark
provides a trait KryoRegistrator, which you can extend to register your classes
with Kyro, as shown in the following code:

>   class Reigstrer extends spark.KyroRegistrator {

>   override def registerClasses(kyro: Kyro) {

>   kyro.register(classOf[MyClass])

>   }

>   }

| Take a look at https://code.google.com/p/ kryo/\#Quickstart to figure out how to write custom serializers for your classes if you need something customized. You can substantially decrease the amount of space used for your objects by customizing your serializers. For example, rather than writing out the full class name, you can give them an integer ID by calling kyro.register(classOf[MyClass],100). |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|


**IDE integration**

For Emacs users, the ENSIME sbt plugin is a good addition. **ENhanced Scala
Interaction Mode for Emacs** (**ENSIME**) provides many features that are
available in IDEs such as error checking and symbol inspection. You can install
the latest ENSIME from https://github.com/aemoncannon/ensime/downloads (make
sure you choose the one that matches your Scala version). Or, you can run the
following commands:

**wget https://github.com/downloads/aemoncannon/ensime/ ensime\_2.10.0-RC3-
0.9.8.2.tar.gz**

**tar -xvf ensime\_2.10.0-RC3-0.9.8.2.tar.gz**

In your .emacs, add this:

>   ;; Load the ensime lisp code...

>   (add-to-list 'load-path "ENSIME\_ROOT/elisp/")

>   (require 'ensime)

>   ;; This step causes the ensime-mode to be started whenever ;; scala-mode is
>   started for a buffer. You may have to customize ;; this step if you're not
>   using the standard scala mode.

>   (add-hook 'scala-mode-hook 'ensime-scala-mode-hook)

You can then add the ENSIME sbt plugin to your project (in project/plugins.sbt):

>   addSbtPlugin("org.ensime" % "ensime-sbt-cmd" % "0.1.0")

You should then run the following commands:

**sbt**

**\> ensime generate**

If you are using Git, you will probably want to add .ensime to the .gitignore
file if it isn't already present.

If you have an IntelliJ, a similar plugin exists called sbt-idea, which can be
used to generate IntelliJ idea files. You can add the IntelliJ sbt plugin to
your project (in project/plugins.sbt) like this:

>   addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.5.1")

You should then run the following commands:

**sbt**

**\> gen-idea**

This will generate the idea file, which can be loaded into IntelliJ.

Eclipse users can also use sbt to generate Eclipse project files with the
sbteclipse plugin. You can add the Eclipse sbt plugin to your project (in
project/plugins.sbt) like this:

>   addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.3.0")

You should then run the following commands:

**sbt**

**\> eclipse**

This will generate the Eclipse project files and you can then import them into
your Eclipse project using the Import Wizard in Eclipse. Eclipse users might
also find the spark-plug project useful, which can be used to launch clusters
from within Eclipse.

An import step is to add spark-assembly-1.2.0-hadoop2.6.0.jar in your Java build
path or Maven dependency. Pay attention so you match the Spark version number
(1.2.0) with the Hadoop version number (2.6.0).

**Using Spark with other languages**

If you find yourself wanting to work with your RDD in another language, there
are a few options available for you. From Java/Scala you can try using JNI, and
with Python you can use the FFI. Sometimes however, you will want to work with a
language that isn't C or work with an already compiled program. In that case,
the easiest thing to do is to use the pipe interface that is available in all
three of the APIs. The stream API works by taking the RDD and serializing it to
strings and then piping it to the specified program. If your data happens to be
plain strings, this is very convenient, but if it's not so, you will need to
serialize your data in such a way that it can be understood on either side. JSON
or protocol buffers can be good options for this depending on how structured
your data is.

**A quick note on security**

Another important consideration in your Spark setup is security. If you are
using Spark on EC2 with the default scripts, you will notice that the access to
your Spark cluster is restricted. This is a good idea to do even if you aren't
running inside of EC2 since your Spark cluster will likely have access to the
data you would rather not share with the world (and even if it doesn't have it,
you probably don't want to allow arbitrary code execution by strangers). If your
Spark cluster is already on a private network, that is great, otherwise you
should talk with your system administrator about setting up some IPtables rules
to restrict access.

**Community developed packages**

A new package index site (http://spark-packages.org/) has a lot of packages and
libraries that work with Apache Spark. It's an essential site to visit and make
use of.

**Mailing lists**

Probably the most useful tip to finish this chapter with is that the Spark
user's mailing list is an excellent source of up-to-date information about other
people's experiences in using Spark. The best place to get information on
meetups, slides, and so forth is https://spark.apache.org/community.html. The
two Spark users mailing lists are user\@spark.apache.org and
dev\@spark.apache.org.

Some more information can be found at the following sites:

1.  http://blog.quantifind.com/posts/logging-post/

    http://jawher.net/2011/01/17/scala-development-environment-emacs-sbt-ensime/

    https://www.assembla.com/spaces/liftweb/wiki/Emacs-ENSIME

    https://github.com/shivaram/spark-ec2/blob/master/ganglia/init. sh

    https://spark.apache.org/docs/latest/tuning.html

    http://spark.apache.org/docs/latest/running-on-mesos.html

    http://kryo.googlecode.com/svn/api/v2/index.html

    https://code.google.com/p/kryo/

    http://scala-ide.org/download/current.html

    http://syndeticlogic.net/?p=311

    http://mail-archives.apache.org/mod\_mbox/incubator-spark-user/

    https://groups.google.com/forum/?fromgroups\#!forum/spark-users

**Summary**

That wraps up some common things that you can use to help improve your Spark
development experience. I wish you the best of luck with your Spark projects;
now go and solve some fun problems! :)
