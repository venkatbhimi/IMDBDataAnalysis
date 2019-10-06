# IMDBDataAnalysis
Spark Application to Analyse IMDB dataset.


Logger: Been used from spark internal packages, which is a tactical solution. This needs to be adressed with new log4j properties setup, otherwise, it will be risk in case, the project upgrades to new versions of the spark, this logger may changed to new project or may be removed.
