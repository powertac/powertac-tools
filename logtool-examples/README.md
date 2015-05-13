## Power TAC logtool analyzer examples

This package contains several examples of state-log analyzers built on the Power TAC logtool framework. Each of them is a class that extends `LogtoolContext` and implements `Analyzer`, and each is intended to be run as a "main" class, wrapping the framework. Unless you need the latest version or are actively developing the framework, it is not necessary to compile it, since it is deployed and maven will find it.

To compile all examples, use maven as

`mvn clean test`

After compiling, all of these examples are run as

`mvn exec:exec -Dexec.args="class-name input-file output-file"`

where classname is the fully-qualified name of the main class you want to run. In other words, if you want to run the TariffMktShare analyzer, the classname would be `org.powertac.logtool.example.TariffMktShare`.

If you don't want to unpack that compressed tar file containing the state log, you can pipe it from tar into your analyzer, as

`tar xzfO game-3-sim-logs.tar.gz log/powertac-sim-3.state | mvn exec:exec -Dexec.args="class-name - output-file"`

However, some analyzers do not seem to find the end-of-file when run in this way. Your mileage may vary.
