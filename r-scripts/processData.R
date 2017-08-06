# Processes Power TAC data by running logtools and summing output
# By Mohammad Ansarin (ansarin@rsm.nl)
# Updated Aug 4 2017
library(rvest)


#### Define logtool and directories here:

Game = '2017'
dataFolder = "D:/PowerTAC/Logs/2017"
logFolder = paste(dataFolder,'/log',sep = '')
outputFolder = paste('C:/Users/Mohammad/Documents/Data/logtoolOutput/',Game,'/',sep='')
logtoolName = 'org.powertac.logtool.example.EnergyMixStats'
logtoolDir = 'C:/Users/Mohammad/Documents/Github/powertac-tools/logtool-examples/'
dataPrefix = sub('.*\\.', '', logtoolName)
csvFile = paste(outputFolder,dataPrefix,'.csv',sep='')
options = '--with-gameid'

#### Functions follow

downloadTarFiles = function(dataFolder,force = FALSE){
  #### Downloads tar.gz files. Currently set to experiment scheduler URL and to only download game log files.
  
  powertac=read_html('http://ts.powertac.org:8080/ExperimentScheduler/faces/index.xhtml')
  tarURLs = powertac %>% html_nodes("a") %>% html_attr("href")
  expURLs = tarURLs[grep(tarURLs,pattern=Game)]
  expURLs = expURLs[-grep(expURLs,pattern='broker')]
  for (URL in expURLs){
    fileName = tail(strsplit(URL,split = '/')[[1]],1)
    message(paste('Downloading from ',URL,'. Time is ',Sys.time(),sep=''))
    download.file(URL, paste(dataFolder,fileName,sep='/'), method='auto', quiet = FALSE)
  }
}

untarLogs = function(dataFolder,force = FALSE, logType = 'state'){
  #### Extracts state and trace files from downloaded tar files in [dataFolder].
  
  if(length(list.files(logFolder))==0|force == TRUE){
    dataFiles = list.files(dataFolder,pattern='.tar.gz')
    for (file in dataFiles) {
      message(paste('Extracting ',file,'. Time is ',Sys.time(),sep=''))
      untar(file, files= paste('log/',strsplit(file,split='\\.')[[1]][1],'.',logType,sep=''), exdir = dataFolder)
    }
  }
}

runLogtool = function(logtoolName,logFolder,options,outputFolder){
  #### Runs [logtoolName] on all state files in [logFolder] with [options]. Outputs data files
  ## to [outputFolder].
  
  stateFiles = list.files(logFolder,pattern='state')
  if ('init.state' %in% stateFiles){
    stateFiles = stateFiles[-which(stateFiles == 'init.state')]
  }
  
  
  for (stateFileName in stateFiles){
    dataFileName = paste(outputFolder,dataPrefix,strsplit(stateFileName,split='\\.')[[1]][1],sep='/')
    dataFileName = paste(dataFileName,'.data',sep='')
    args = paste(logtoolName, options, paste(logFolder,stateFileName,sep='/'), dataFileName,sep=' ')
    call = paste('cd',logtoolDir,'&&',
                 'mvn','exec:exec','-Dexec.args="', args,'"', sep=' ')
    if (Sys.info()["sysname"] =='Windows'){
      print(call)
      shell(call)
    } else {
      system(call)
    }
    
  }
}

collectOutput = function(outputFolder,dataPrefix,csvFile, overwrite= FALSE){
  #### Collect output from [outputFolder/dataPrefix] into a csv file titled [csvFile]. Runs slowly, needs optimization.

  outputFolder = paste(outputFolder,dataPrefix,sep='')
  files=list.files(outputFolder,pattern='.data',full.names = TRUE)
  
  if (overwrite == TRUE){
    file.create(csvFile)
  }
  flag = 0
  for (dataFile in files){
    #message('working on ',dataFile,'. Time is ',Sys.time())
    for (line in readLines(dataFile)){
      line = strsplit(line,split=',')[[1]]
      if (line[1] == 'Summary'){
        next
        }
      if (line[1] == 'game' & flag == 1){
        next
      }
      if (line[2] != ' slot'){
        line[2] = as.integer(line[2]) - 360
        }
      write.table(x=t(line),file = csvFile,append=TRUE, quote=FALSE,sep = ',',col.names = FALSE, row.names = FALSE)
      }
    flag = 1
  }
}