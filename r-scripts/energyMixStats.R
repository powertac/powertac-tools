# Base code for plotting energyMixStats plots
# Mohammad Ansarin (ansarin@rsm.nl)
# Created December 19, 2015
#
# Prepares datasets and plots exports from EnergyMixStats logtool-example.
#
# For each timeslot, report includes
#   purchased energy and cost
#   customer energy consumption and value
#   customer energy production and cost
#   total customer-provided balancing energy and cost
#   total imbalance and cost


library(ggplot2)
print("Use 'energyDataPrep(year)' to prepare datasets and 'dailyPlot()' to plot stuff.")


energyDataPrep = function(year) {
  
  ############################ Data Init and Cleanup ################################
  
  # ADJUST PATHS HERE
  code.path = getwd()
  data.path = code.path
  #csv.path = "~/Google Drive/PhD/PowerTAC Analysis/Plotting/output csvs/2014/"
  csv.path = file.path("~/Google Drive/PhD/PowerTAC Analysis/Plotting/output csvs",year)
  
  out.file <- file.path(data.path,paste("energyMixStats",year,".RData",sep=''))
  in.file  <- file.path(csv.path, "energyMixStats.csv")
  
  if (file.exists(out.file)) {
    message("Loading energyMixStats data from ", out.file)
    load(out.file)
  } else {
    message("Loading energyMixStats data from ", in.file)
    energyMixStats = read.csv(in.file, header = TRUE, sep = ",", stringsAsFactors = FALSE)
    save(energyMixStats, file=out.file)
  }
  assign("energyMixStats", energyMixStats, envir = .GlobalEnv)
  
  if (!exists("plotStats", where = .GlobalEnv)) {
    message("Preparing plotStats (Stats averages versus hour of day)...")
    energyMixStats$dayhour = (energyMixStats$slot-1) %% 24
    plotStats = data.frame(matrix(0,nrow = 24, ncol = 15))
    colnames(plotStats) = colnames(energyMixStats)
    plotStats$game.id = NULL
    plotStats$slot = NULL
    plotStats$dayhour = 0:23
    for (game in 1:max(energyMixStats$game.id)){
      itersubset = subset(energyMixStats, energyMixStats$game.id == game)
      for (j in colnames(plotStats)[-which(colnames(plotStats) == "dayhour")]){
        for (i in plotStats$dayhour){
          plotStats[[j]][i+1] = plotStats[[j]][i+1] +
            mean(subset(itersubset[[j]],itersubset$dayhour == i))/max(energyMixStats$game.id)
        }
      }
      assign("plotStats", plotStats, envir = .GlobalEnv)
    }
  }
}

dailyPlot = function(year) {
  #plotData = subset(energyMixStats, (energyMixStats$game.id == 1) & (energyMixStats$slot < 140) & (energyMixStats$slot > 100))
  #plot(plotData$slot,plotData$cons, type = "l")
  
  plot(plotStats$dayhour,plotStats$imbalance, type = "b")
  p = ggplot(plotStats, aes(x=dayhour))
  p +
    geom_line(aes(y=-imbalance,colour = "Imbalance"), size = 1) +
    geom_point(aes(y=-imbalance),size = 2) +
    geom_line(aes(y=-cons, colour = "Consumption"), size = 1.25) +
    geom_line(aes(y=prod, colour = "Production"), size = 1.25) +
    scale_colour_manual("", 
                        breaks = c("Imbalance", "Consumption", "Production"),
                        values = c("Production"="green", "Consumption"="red", 
                                   "Imbalance"="blue")) +
    labs(title = paste("Energy versus hour of day (",year,", all games)",sep=''), x = "Hour of Day", y = "Energy (MWh)") +
    theme(text = element_text(size=15))
}