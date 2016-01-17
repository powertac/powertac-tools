# Base code for plotting imbalanceSummary plots
# Mohammad Ansarin (ansarin@rsm.nl)
# Created December 17, 2015
#
# Prepares datasets and uses them to plot exports from ImbalanceSummary logtool-example.
# Instructions:
# 1. Add datasets to imbaDataPrep(year) function, if previously prepared datasets are inadequate.
# 2. Add each plot as a new function after imbaDataPrep.
#
# Game info is summarized in pergame as:
# game-id,n_brokers,c_total,cr_total,p_total,pr_total,i_total,i_rms,ir_total
# where n_brokers is the number of competing brokers (not including the
# default broker), c_total and p_total are the total consumption and
# production recorded by tariff transactions, cr_total and pr_total are
# revenue (or cost) associated with consumption and production,
# i_total is the total imbalance recorded by balancing transactions, 
# i_rms is the rms imbalance, ir_total is the overall imbalance cost
# paid by all brokers. Signs are from the viewpoint of the broker; positive
# values represent incoming cash or energy.
# The second section is per-broker summary information, formatted as one
# line per broker per game, in data frame "perbroker":
# broker-name,c_broker,cr_broker,p_broker,pr_broker,i_broker,i_rms-broker,ir_broker
# where the fields are per-broker versions of the aggregate data.

print("Use 'imbaDataPrep(year)' to prepare datasets and plotting functions to plot stuff.")

library(ggplot2)
plotPath = file.path("~/Google Drive/PhD/PowerTAC Analysis/Plotting/plots")
plotWidth = 6
plotHeight = 4

imbaDataPrep = function(year) {
  
  ############################ Data Init and Cleanup ######################################
  
  # ADJUST PATHS HERE
  setwd("~/Google Drive/PhD/PowerTAC Analysis/Plotting/12-17 Imbalance plotting")
  code.path = getwd()
  data.path = code.path
  csv.path = file.path("~/Google Drive/PhD/PowerTAC Analysis/Plotting/output csvs",year)
  out.file <- file.path(data.path,paste("imbalanceSummary",year,".RData",sep=''))
  in.file  <- file.path(csv.path, "imbalanceSummary.csv")
  
  if (file.exists(out.file)) {
    message("Loading imbalance summary from ", out.file)
    load(out.file)
  } else {
    message("Loading imbalance summary from ", in.file)
    imbalanceSummary = read.csv(in.file, header = FALSE, sep = ",", stringsAsFactors = FALSE)
    save(imbalanceSummary, file=out.file)
  }
  assign("imbalanceSummary", imbalanceSummary, envir = .GlobalEnv)
  
  ############### Handling per game data #################
  if (year == "2014") {
    N = 72
  }
  else if (year == "2015") {
    N = 230
  }
  pergame = data.frame("game_id" = rep(NA, N), "n_brokers" = rep(NA,N), "c_total" = rep(NA,N),
                       "cr_total" = rep(NA,N),"p_total" = rep(NA,N), "pr_total" = rep(NA,N), 
                       "i_total" = rep(NA,N), "i_rms" = rep(NA,N), "ir_total" = rep(NA,N), 
                       stringsAsFactors = FALSE) # ADD DATA FRAME INIT
  
  for (i in 1:N) {
      pergame[i,]=imbalanceSummary[imbalanceSummary$V1 == paste("game",as.character(i),sep="-"),]
      pergame$game_id[i] = i
  }
  assign("pergame", pergame, envir = .GlobalEnv)
  save(pergame, file = paste("pergame",year,".RData",sep=''))
  
  ############ Handling per broker per game data #############
  # broker-name,c_broker,cr_broker,p_broker,pr_broker,i_broker,i_rms-broker,ir_broker
  
  perbroker = data.frame()
  
  line = data.frame("game_id" = NA, "n_brokers" = NA, "broker_name" = NA,
                    "c_broker" = NA,"cr_broker" = NA, "p_broker" = NA, 
                    "pr_broker" = NA, "i_broker" = NA, "i_rms_broker" = NA, 
                    "ir_broker" = NA, stringsAsFactors = FALSE)
  for (i in 1:N) {
    imbalanceSummary$V1 == paste("game",as.character(i),sep="-")
    linenum = pmatch(paste("game",as.character(i),sep="-"),imbalanceSummary$V1)
    brokerNum = imbalanceSummary[linenum,2]
    for (j in 1:(brokerNum+1)) {
      line$game_id = i
      line$n_brokers = brokerNum
      line[,3:10] = imbalanceSummary[linenum+j,1:8]
      
      perbroker = rbind(perbroker,line)
    }
  }
  perbroker = data.frame(perbroker, "market_share" = NA)
  for (game_id in pergame$game_id) {
    for (broker in perbroker$broker_name[(perbroker$game_id == game_id)]) {
      perbroker$market_share[(perbroker$game_id == game_id) & (perbroker$broker_name == broker)] = 
        perbroker$c_broker[perbroker$game_id == game_id & perbroker$broker_name == broker] / pergame$c_total[pergame$game_id == game_id]
    }
  }
  assign("perbroker", perbroker, envir = .GlobalEnv)
  save(perbroker, file=paste("perbroker",year,".RData",sep=''))
  
  brokerNames = unique(perbroker$broker_name) 
  brokerNames = brokerNames[-c(which(brokerNames == "default broker"))]
  assign("brokerNames", brokerNames, envir = .GlobalEnv)
  
  ########## gameSizes #########
  if (year == 2014) {
    gameSizes = c(2,4,7)
  }
  else if (year == 2015) {
    gameSizes = c(3,9,11)
  }
  assign("gameSizes", gameSizes, envir = .GlobalEnv)

  
  ########## brokerImbalances #########
  brokerImbalances = list()
  for (i in gameSizes ) {
    brokerImbalances[[i]] = list()
  }
  
  for (gamesize in gameSizes) {
    for (broker in brokerNames) {
      brokerImbalances[[gamesize]][[broker]] = subset(perbroker$i_rms_broker, 
                                                      (perbroker$broker_name == broker) & (perbroker$n_brokers == gamesize | perbroker$n_brokers == gamesize - 1))
    }
  }
  brokerImbalances = Filter(length,brokerImbalances)
  assign("brokerImbalances", brokerImbalances, envir = .GlobalEnv)
}

############################# Plotting ######################################
plotAveImba = function(year) {
  # Plots RSM imbalance versus game size. Also shows standard deviations.
  
  
  load(paste("perbroker",year,".RData",sep=''))
  load(paste("pergame",year,".RData",sep=''))
  print(pergame)
  
  RMSimba = data.frame("Average" = numeric(length(gameSizes)), "STD" = numeric(length(gameSizes)))
  
  for (i in 1:length(gameSizes)){
    plotVals = numeric(0)
    
    for (broker in brokerNames) {
      plotVals = c(plotVals,max(0,ave(brokerImbalances[[i]][[broker]])))
    }
    RMSimba$Average[i] = mean(plotVals)
    RMSimba$STD[i] = sd(plotVals)
  }
  #plot(RMSimba$Average, type = "b", xlab = "Game Size", ylab = "RMS Imbalance (MWh?)",
  #     main = "RMS Imbalance, 2014 Finals; averaged over game sizes & brokers", xaxt="n")
  #axis(1, at=1:length(gameSizes), labels = as.character(gameSizes))
  #arrows(x,CI.dn,x,CI.up,code=3,length=0.2,angle=90,col='red') ADJUST THIS FOR SD!
  
  RMSimba = data.frame(RMSimba,gameSizes)
  limits = aes(ymax = RMSimba$Average + RMSimba$STD, ymin = RMSimba$Average - RMSimba$STD)
  print(RMSimba)
  imba = ggplot(RMSimba,aes(x=RMSimba$gameSizes)) +
    geom_line(aes(y=RMSimba$Average),size = 1) +
    geom_point(aes(y=RMSimba$Average),size = 4.5) +
    geom_errorbar(limits, position = "dodge", width = 0.25)
  imba +
    scale_x_continuous(breaks=c(1:11)) +
    labs(title = paste("RMS Imbalance,",year,"Finals; averaged over game sizes & brokers"), x = "Game Size", y = "RMS Imbalance (MWh)") +
    theme(text = element_text(size=15))
}

plotBrokerImbas = function(year){
  # Plots RMS imbalance over all games per broker. Also shows standard deviations 
  # and average RMS imbalance.
  plotVals = data.frame(matrix(0,length(brokerNames),length(gameSizes)*2+3))
  colnames(plotVals) = c("broker","Ave3","Ave9","Ave11","SD3","SD9","SD11","TotAve","TotSD")
  plotVals$broker = brokerNames
  for (game in 1:length(gameSizes)){
    for (broker in brokerNames) { #NOTE: LIMITS ADDED TO FIX -INF IMBALANCE PROBLEM FOR 2 GAMES
      plotVals[plotVals$broker == broker,game+1] = max(0,mean(brokerImbalances[[game]][[broker]]))
      plotVals[plotVals$broker == broker,game+4] = min(10000,sd(brokerImbalances[[game]][[broker]]))
    }
  }
  
  for (broker in brokerNames){
    plotVals[plotVals$broker == broker, 8] = mean(as.numeric(plotVals[plotVals$broker == broker, 2:4]))
    plotVals[plotVals$broker == broker, 9] = sqrt(mean((plotVals[plotVals$broker == broker, 5:7])^2))
  }  
  assign("plotVals",plotVals, envir = .GlobalEnv)
  
  brokerimba = ggplot(plotVals, aes(x = plotVals$broker))
  limits = aes(ymax = plotVals$TotAve + plotVals$TotSD, ymin = plotVals$TotAve - plotVals$TotSD)
  brokerimba +
    geom_point(aes(y = plotVals$TotAve),stat="identity", size = 3, colour = "blue") +
    geom_hline(aes(yintercept = mean(plotVals$TotAve)),  colour = "orange") +
    geom_errorbar(limits, position = "dodge", width = 0.25) +
    scale_y_continuous(limits = c(0,max(plotVals$TotAve + plotVals$TotSD) + 1000)) +
    labs(title = paste("RMS Imbalance per broker,",year,"Finals"), x = "Broker Name", y = "RMS Imbalance (MWh)") +
    theme(text = element_text(size=15),axis.text.x = element_text(angle = 30, hjust = 1))
  ggsave(paste("RMS Imbalance per broker,",year,"Finals,png"),
         path = plotPath, width = plotWidth, height = plotHeight)
}

plotHHI = function (year) {
  if (is.null(pergame$HHI)){
    pergame = data.frame(pergame,"HHI" = NA)
  }
  for (game in pergame$game_id){
    pergame$HHI[pergame$game_id == game] = sum((perbroker$market_share[perbroker$game_id == game]*100)^2)
  }
  assign("pergame",pergame, envir = .GlobalEnv)
  
  AveHHI = data.frame("n_brokers" = numeric(3), "Average" = numeric(3), "SD" = numeric(3))
  AveHHI$n_brokers = gameSizes
  for (game in gameSizes) {
    AveHHI$Average[AveHHI$n_brokers == game] = 
      mean(subset(pergame$HHI,(pergame$n_brokers == game) | (pergame$n_brokers == game - 1)))
    AveHHI$SD[AveHHI$n_brokers == game] = 
      sd(pergame$HHI[(pergame$n_brokers == game) | (pergame$n_brokers == game - 1)])  
  }
  
  HHIplot = ggplot(pergame)
  HHIplot +
    geom_point(aes(x = pergame$n_brokers, y = pergame$HHI)) +
    scale_y_continuous(limits = c(0,10000)) +
    scale_x_discrete(breaks = gameSizes) +
    labs(title = paste("HHI Index of all games,",year,"Finals"), x = "Game Size", y = "Herfindahl-Hirschman Index") +
    theme(text = element_text(size=15),axis.text.x = element_text(angle = 0, hjust = 1))
  assign("HHIplot",HHIplot, envir = .GlobalEnv)
  ggsave(paste("HHI Index of all games,",year,"Finals.png"),
         path = plotPath, width = plotWidth, height = plotHeight)
  
  
  #print(HHIplot)
  HHIAvePlot = ggplot(AveHHI, aes(x = AveHHI$n_brokers))
  limits = aes(ymax = AveHHI$Average + AveHHI$SD, ymin = AveHHI$Average - AveHHI$SD)
  HHIAvePlot +
    geom_point(aes(y = AveHHI$Average), size = 4) +
    geom_errorbar(limits, width = 0.25) +
    scale_y_continuous(limits = c(0,10000)) +
    scale_x_discrete(breaks = gameSizes) +
    labs(title = paste("Average HHI Index of all games,",year,"Finals"), x = "Game Size", y = "Herfindahl-Hirschman Index") +
    theme(text = element_text(size=15),axis.text.x = element_text(angle = 0, hjust = 1))
  ggsave(paste("Average HHI Index of all games,",year,"Finals.png"),
         path = plotPath, width = plotWidth, height = plotHeight)
  
}