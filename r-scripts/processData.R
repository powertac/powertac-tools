# Processes Power TAC data by running logtools and summing output
# By Mohammad Ansarin (ansarin@rsm.nl)
# July 31, 2017
library(rvest)


# Define logtool and directories here:
Experiment = 'Test_Mohammad_'
dataFolder = "C:/Users/moham/Documents/Data/ESData"
logFolder = paste(dataFolder,'/log',sep = '')
outputFolder = dataFolder
logtoolName = 'org.powertac.logtool.example.BrokerAccounting'
logtoolDir = 'C:/Users/moham/Documents/Github/powertac-tools/logtool-examples/'
dataPrefix = paste('logtoolOutput/' ,sub('.*\\.', '', logtoolName),sep='')
outputFile = paste(outputFolder,sub('.*\\.', '', logtoolName),'.csv',sep='')
options = character(0)

# Functions follow

downloadTarFiles = function(dataFolder,force = FALSE){
  #### Downloads tar.gz files. Currently set to experiment scheduler URL and to only download game log files.
  
  powertac=read_html('http://ts.powertac.org:8080/ExperimentScheduler/faces/index.xhtml')
  tarURLs = powertac %>% html_nodes("a") %>% html_attr("href")
  expURLs = tarURLs[grep(tarURLs,pattern=Experiment)]
  expURLs = expURLs[-grep(expURLs,pattern='broker')]
  for (URL in expURLs){
    fileName = tail(strsplit(URL,split = '/')[[1]],1)
    message(paste('Downloading from ',URL,'. Time is ',Sys.time(),sep=''))
    download.file(URL, paste(dataFolder,fileName,sep='/'), method='auto', quiet = FALSE)
  }
}

untarLogs = function(dataFolder,force = FALSE, logType = 'state'){
  #### Extracts state and trace files from downloaded tar files.
  if(length(list.files(logFolder))==0|force == TRUE){
    dataFiles = list.files(dataFolder,pattern='.tar.gz')
    for (file in dataFiles) {
      message(paste('Extracting ',file,'. Time is ',Sys.time(),sep=''))
      untar(file, files= paste('log/',strsplit(file,split='\\.')[[1]][1],'.',logType,sep=''), exdir = dataFolder)
    }
  }
}

runLogtool = function(logtoolName,logFolder,options,outputFolder){
  #### Runs [logtoolName] on all state files in [logFolder] with [options]. Outputs to [outputFolder].
  
  stateFiles = list.files(logFolder,pattern='state')
  if ('init.state' %in% stateFiles){
    stateFiles = stateFiles[-which(stateFiles == 'init.state')]
  }
  
  for (stateFileName in stateFiles){
    dataFileName = paste(outputFolder,dataPrefix,strsplit(stateFileName,split='\\.')[[1]][1],sep='/')
    dataFileName = paste(dataFileName,'.data',sep='')
    args = paste(logtoolName, options, paste(logFolder,stateFileName,sep='/'), dataFileName,sep=' ')
    call = paste('cd C:/Users/moham/Documents/Github/powertac-tools/logtool-examples/ &&',
                 'mvn','exec:exec','-Dexec.args="', args,'"', sep=' ')
    print(call)
    shell(call)
  }
  
}