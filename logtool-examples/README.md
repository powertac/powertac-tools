## Power TAC logtool analyzer examples

This package contains several examples of state-log analyzers built on the Power TAC logtool framework. Each of them is a class that extends `LogtoolContext` and implements `Analyzer`, and each is intended to be run as a "main" class, wrapping the framework. Unless you are actively developing the framework, it is not necessary to compile it, since it is deployed and maven will find it.

After compiling, all of these examples are run as

`mvn exec:exec -Dexec.args="class-name input-file output-file"`

where classname is the name of the main class you want to run.

If you don't want to unpack that compressed tar file containing the state log, you can pipe it from tar into your analyzer, as

`tar xzfO game-3-sim-logs.tar.gz log/powertac-sim-3.state | mvn exec:exec -Dexec.args="class-name - output-file"`

