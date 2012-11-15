## Using and extending the Power TAC logtool

This is a tool for reading and analyzing the state log files produced by the Power TAC simulation server. It is intended to be extended by adding "analyzers", and can be run either top-down, in which a state log is read one and passed to several analyzers, or bottom-up under control of a specific analyzer. Most developers will likely find the bottom-up approach more usable.

The logtool currently does not use all the data in a state log; in particular, it does not read all the "new" methods, and does not read any methods other than "new" methods with the exception of the ```timeService.updateTime()``` methods. What it does is to create most of the objects it finds, and offers to let "analyzer" implementations receive them as they are read and do what it wishes with them.

The logtool is built on the Power TAC "common" module (the "logtool" branch until we merge it back), which defines all the domain types, their repositories, and the time service. A number of "core" domain types are read from the state log and stored in their respective repositories, including Brokers, Customers, Timeslots, TariffSpecifications, and their Rates. In addition, the TimeService is updated to give the "current time" as the file is read. As the tool is further developed, this capability should give a fairly complete reproduction of the server environment to analyzer developers. 

There are currently a few types that are completely ignored, including Tariff, SimPause, SimResume, PauseRequest, PauseRelease, and some inner classes that get logged because of their inheritance, such as Rate$ProbeCharge, which is a subtype of HourlyCharge. Also missing currently is Genco, but this can easily be fixed.

Several analyzers are currently available in the package org.powertac.logtool.example. Each of them has a main() method that takes a state log filename and the name of a file to dump data. They run in STS, but the maven setup to run them outside STS is left as an exercise for someone who wants to figure it out.
