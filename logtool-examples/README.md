## Power TAC logtool analyzer examples

This package contains several examples of state-log analyzers built on the Power TAC logtool framework. Each of them is a class that extends `LogtoolContext` and implements `Analyzer`, and each is intended to be run as a "main" class, wrapping the framework. Unless you are actively developing the framework, it is not necessary to compile it, since it is deployed and maven will find it.

After compiling, all of these examples are run as

`mvn exec:exec -Dexec.args="class-name input-file output-file"`

where classname is the name of the main class you want to run.
