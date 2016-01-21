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
setwd("~/Google Drive/PhD/PowerTAC Analysis/Plotting/Imbalance")

imbaDataPrep = function(year) {
  ############################ Data Init and Cleanup ######################################
  
  #### Adjust Paths here:
  csv.path = file.path("~/Google Drive/PhD/PowerTAC Analysis/Plotting/output csvs",year)
  out.file <- file.path(getwd(),paste("imbalanceSummary",year,".RData",sep=''))
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
  
  ############ Handling per broker data #############
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
  
  ########## gameSizes ############
  if (year == 2014) {
    gameSizes = c(2,4,7)
  }
  else if (year == 2015) {
    gameSizes = c(3,9,11)
  }
  assign("gameSizes", gameSizes, envir = .GlobalEnv)

  
  ########## brokerImbalances ###########
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
  
  ############## Broker Profits (imported from cashposition.csv) ################################
  #### Loads data from cashposition and brokerinfo. Adds profits info to perbroker data frame.
  if (year == 2015) {
    in.file.cash  = file.path(csv.path, "cashposition.csv")
    out.file.cash = file.path(getwd(),paste("cash",year,".RData",sep=''))
  
    if (file.exists(out.file.cash)) {
      message("Loading cash position data from ", out.file.cash)
      load(out.file.cash)
    } else {
      message("Loading cash position data from ", in.file.cash)
      cash <- read.csv(in.file.cash, header = TRUE, sep = ";")
      save(cash, file=out.file.cash)
    }
    assign("cash", cash, envir = .GlobalEnv)
    
    in.file.broker  <- file.path(csv.path, "broker.csv")
    out.file.broker <- file.path(getwd(), paste("broker",year,".RData",sep=''))  
    if (file.exists(out.file.broker)) {
      message("Loading broker data from ", out.file.broker)
      load(out.file.broker)
    } else {
      message("Loading 2015 broker data from ", in.file.broker)
      brokerinfo <- read.csv(in.file.broker, header = TRUE, sep = ";")
      save(brokerinfo, file=out.file.broker)
    }
    assign("brokerinfo", brokerinfo, envir = .GlobalEnv)
    
    if (is.null(perbroker$profits)){
      perbroker = data.frame(perbroker, "profits" = as.numeric(0))
    }
    
    for (j in 1:tail(cash$game_id,n=1)) {
      count = nrow(subset(cash, (cash$game_id == j) & (cash$timeslot == 1)))
      compcash = cash[cash$game_id == j,]### <<<< Added for computational speed. Can also be added to lower subset function
      for (i in 1:count){
        broker.name = toString(brokerinfo[brokerinfo$broker_id == i,"broker_name"])
        temp = subset(compcash, compcash$broker_id==i)
        result = tail(temp$balance, n=1)
        perbroker[(perbroker$game_id == j) & (perbroker$broker_name == broker.name),"profits"] = result
      }
    }
    assign("perbroker",perbroker, envir = .GlobalEnv)
  }
}

############################# Plotting ######################################
plotAveImba = function(year) {
  #### Plots RSM imbalance versus game size. Also shows standard deviations.
  
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
  #### Plots RMS imbalance over all games per broker. Also shows standard deviations 
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
  #ggsave(paste("RMS Imbalance per broker,",year,"Finals,png"),
  #       path = plotPath, width = plotWidth, height = plotHeight)
}

plotHHI = function (year) {
  if (is.null(pergame$HHI)){
    pergame = data.frame(pergame,"HHI" = NA)
  }
  for (game in pergame$game_id){
    pergame$HHI[pergame$game_id == game] = sum((perbroker$market_share[perbroker$game_id == game]*100)^2)
  }
  assign("pergame",pergame, envir = .GlobalEnv)
  
  AveHHI = data.frame("year" = numeric(3), "n_brokers" = numeric(3),
                      "Average" = numeric(3), "SD" = numeric(3))
  AveHHI$n_brokers = gameSizes
  AveHHI$year = year
  for (game in gameSizes) {
    AveHHI$Average[AveHHI$n_brokers == game] = 
      mean(subset(pergame$HHI,(pergame$n_brokers == game) | (pergame$n_brokers == game - 1)))
    AveHHI$SD[AveHHI$n_brokers == game] = 
      sd(pergame$HHI[(pergame$n_brokers == game) | (pergame$n_brokers == game - 1)])
    save(AveHHI,file = paste("AveHHI",year,".RData",sep=""))
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


plotImbaVersusProfits = function(year){
  #### Plots broker imbalance versus profits.
  
  #### Data manipulation here. Cleaning, unneeded data, bad data, etc.
  perbroker = perbroker[is.finite(perbroker$profits),] ## Remove all "-Inf" results. (Only one)
  perbroker = perbroker[perbroker$broker_name != "default broker",] ## Remove default broker's data.
  perbroker = perbroker[perbroker$profits != 0,] ## Remove passive brokers.
  
  #### Plotting here. Use ggplot or sth.
  #plot(perbroker$i_rms_broker,perbroker$profits) ## For testing purposes
  
  xtitle = "RMS Imbalance (MWh)"
  ytitle = "Profits"
  plottitle=paste(ytitle,"versus",xtitle,"for",year,"Finals per broker")
  
  vsProfitsPlot = ggplot(perbroker) +
    geom_point(aes(x = perbroker$i_rms_broker,y = perbroker$profits, colour = perbroker$broker_name), size = 2) +
    scale_y_continuous(limits = c(-5e05,5e06)) +
    scale_x_continuous(limits = c(0,3e04)) +
    scale_colour_discrete(name  ="Broker ID") +
    
  labs(title = plottitle , x = xtitle, y = ytitle)
  ggsave(paste(plottitle,".png",sep=''), vsProfitsPlot,
         path = plotPath, width = plotWidth, height = plotHeight, scale = 1.5)
  print(vsProfitsPlot)
  
  
  rmsfit = lm(perbroker$profits ~ perbroker$i_rms_broker)
  print(summary(rmsfit))
  irfit = lm(perbroker$profits ~ perbroker$ir_broker)
  print(summary(irfit))
}

plotAllHHI = function () {
  #### Plots all HHI values for both Finals competition in one plot. Depends on plotHHI(year) and
  # imbaDataPrep(year)

  imbaDataPrep(2014)
  plotHHI(2014)
  imbaDataPrep(2015)
  plotHHI(2015)
  load("AveHHI2015.RData")
  AveHHI2015 = AveHHI
  load("AveHHI2014.Rdata")
  AveHHI2014 = AveHHI
  AllHHI = rbind(AveHHI2014,AveHHI2015)
  AllHHI$year = as.factor(AllHHI$year)
  #print(AllHHI)
  
  #### Plotting here:
  
  limits = aes(ymax = AllHHI$Average + AllHHI$SD, ymin = AllHHI$Average - AllHHI$SD)
  xtitle = "Game Size"
  ytitle = "Herfindahl-Hirschman Index"
  plottitle="Average Herfindahl-Hirschman Index of all games"
  
  AllHHIPlot = ggplot(AllHHI, aes(x = AllHHI$n_brokers)) +
    geom_errorbar(limits, width = 0.25, size = 0.8) +
    geom_point(aes(y = AllHHI$Average, colour = AllHHI$year), size = 4) +
    scale_y_continuous(limits = c(0,10000)) +
    scale_x_discrete(breaks = 1:12) +
    labs(title = plottitle, x = xtitle, y = ytitle) +
    scale_colour_discrete(name  ="Year") +
    theme(text = element_text(size=15),axis.text.x = element_text(angle = 0, hjust = 1))
  ggsave(paste(plottitle,".png",sep=""),
         path = plotPath, width = plotWidth, height = plotHeight, scale = 1.5)
  print(AllHHIPlot)
}