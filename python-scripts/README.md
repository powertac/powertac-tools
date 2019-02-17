## Python scripts for analyzing games and tournaments

Most of these scripts fall into one of three categories:

1. Scripts that take data produced by the logfile extractors in logtool-examples and generate plots or other analyses;

2. Scripts that download and extract logfiles from tournaments and run logfile extractors on them; or

3. Scripts that do analyses and plotting of tournament data, aggregated across all or selected sets of games from tournaments. 

Most of the scripts have header comments explaining what they do. All require an installation of Python 3.x, most also require Python math (numpy), statistics (scipy, pylab), and plotting (matplotlib) libraries. Some can be run from a command-line, and some require loading into a Python IDE and running individual functions. Most of the plotting functions are intended to be used this way.

`TournamentGameIterator.csvIter(csvurl, dirPath)` downloads from `csvurl` and reads the csv file listing the games in a tournament along with the URLs for their logfile bundles. It then creates a directory under `dirPath` for each one, downloads the individual log bundles into those directories, and unpacks them. The download and unpack operations are only done in case the files do not already exist locally. It returns an iterator of dict objects of the form `{'gameId':id, 'boot':bootStateLog, 'sim':simStateLog}` giving paths to the boot and sim state logs. If you need the trace logs, you can just string-replace 'state' with 'trace' in the returned paths.

`TournamentLogtoolProcessor.dataFileIterator(csvurl, dirPath, extractorClass, datafilePrefix)` (with several additional options -- see the code) uses `TournamentGameIterator` to download and unpack game logs, then runs the specified extractor class on each one, storing the resulting data files in `dirPath/data` with the specified filename prefix. It returns an iterator of dict objects of the form `{'gameId':id, 'path':datafile-path}`. You can run this script from the command line if your python 3 installation is "conventional" -- see the `main()` function at the bottom of the file for details, or just run it without args to get instructions.

