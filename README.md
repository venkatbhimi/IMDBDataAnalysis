# IMDBDataAnalysis
Spark Application to do Analysis on IMDB dataset. This application will produce top X ranking titiles of the DB based on number of votes with average rating each title gets. This application can rank multiple title types as this DB dataset has the got titles types -
tvSeries, tvMiniSeries, tvMovie, tvEpisode, movie, tvSpecial, video, videoGam, tvShort, short.

This application is developed to bring up top 20 ranking movies, but this can be changed by passing a value of title type the user wants. Application also allows the number of ranks user wants to bring up as result,
as a passing value can be an integer, for example 10, 20, 50 ,100..etc.

As a result, this application will produce top ranked titles into a csv file, at the output path, with credits details, like director, writer ..etc,  and also adds the previous know titles details ..etc.

Technical Tools : This application is developed in scala programming language, using spark 2.4 version, scala version 2.11, build tool Maven.

Maven:  Maven is the buid tool, dependency management with local repository,  to dependencies resolution, maven will downloads the required dependency libraries and plugins into local repository as configured in pom.xml.
If the user already configured local repository, then if you change pom repository settings ( in pom file <repository></repository> tag), it will re-use the already downloaded libs.


Build & Run Steps:

=> git clone the project => git clone git@github.com:venkatbhimi/IMDBDataAnalysis.git

=> build the project (tests also will be run at this stage) => mvn clean install

This step will take time dendending on internet download speed, as it downloads dependencies and plugins from maven repository.
If user have already local repository you have to change it to the pom to use the local repository. or you can override repo = >  mvn install -Dmaven.repo.local=https://repo.maven.apache.org/maven2/

=> generate a package ( no need to run tests) => mvn package -DskipTests=True

=> after generating jar file, the script file can be used to run the job.

if spark bin directory setup in class path, script can on any directory. otherwise, script file needs to be copied to spark installation directory, inside "bin", and run the script file.

=> result will be saved in to output directory, which can be passed as a parameter in script.

Execution: The application can run with multiple resource sizes , and each execution times to run on mac-laptop on standalone spark cluster.

Currenlty, this applcaction ran on spark standalone with 4 executors, with executor-memory 4G with executor-cores 3 it took 3 mins to run and produce the resluts.

Performance modifications :

=> To achive the result dataset, sub datasets have to be joined, which leads to shuffle data between executors while join operations, to avoid shuffles, spark feature broadcasting can be used where ever the dataset is reasonable size.
=> currently the parallelism is passed through a spark parameter (spark.default.parallelism with value 50 ), this can be changed based on the size of the data processing, this will split the input data to partitions,
which helps in execution( as computation will be divided as a task on each partition).

Pipeline Test: This application has a integration test, along with helper tests. To run the Integration test, theres small size data recocrds for test data set has created under
test/resources directory. This test data will be used to run end to end pipeline. And the test output will be generated to test/output directory.

Logger: Been used from spark internal packages, as a tactical solution. This needs to be adressed with new log4j properties setup for future use, otherwise, it will be risk in case, the spark later versions might change the logger package or may remove completely,
in case of upgrading spark later version may get issues.
