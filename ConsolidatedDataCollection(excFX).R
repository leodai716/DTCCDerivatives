# This file is scripted for conveniently looping through and collecting data for all the database

AssetClasslist <- c('EQ', 'IR', 'CO', 'CD')

for (i in 1:length(AssetClasslist)){
  AssetClass <- AssetClasslist[i]
  source("D:\\Projects\\DTCCDerivatives\\DTCC data.all asset class.R")
}
