# This file is scripted to refine the data scraping process
# in DTCC data.R only FX is scraped, user still have to define columns, but in this updated version such detection will be made automated 
starttime <- Sys.time()
## init
# install.packages('dataonderivatives')
library(dataonderivatives)



#################### Inputs ####################
DTCCdb_StartDate <- as.Date('2017-01-01')
DTCCdb_EndDate <- Sys.Date() -1
AssetClass <- 'CR'
################################################



### Step 1 Ceate function 

getDTCCdata <- function(start, end, AC){
  # this is just a dummy that help to column numbers and namef for subsequent step, exact date does not matter
  df.dummy <- ddr(as.Date('2019-01-01'), AC)
  
  DTCCdb.temp <- data.frame(matrix(nrow = 0, ncol = ncol(df.dummy)))
  colnames(DTCCdb.temp) <- colnames(df.dummy)
  
  # data scraping
  dbDateRange <- seq(start, end, 'days')
  for (d in 1:length(dbDateRange)){
    DTCCdta.temp <- ddr(date = dbDateRange[d], asset_class = AssetClass)
    if (nrow(DTCCdta.temp) >= 1) {
      DTCCdta.temp$TRANSACTION_RECORD_DATE <-dbDateRange[d]
      DTCCdb.temp <- rbind(DTCCdb.temp, DTCCdta.temp)
    }
    if (d %% 100 == 0 ) {
      Sys.sleep(1)
    }
  }
  
  
  # because different asset class may have unique structure, data cleansing will not be handled withing the function
  
  #return data frame 
  return(DTCCdb.temp)
}



### Step 2 Get data
# for initial db building 
DTCCdb <- getDTCCdata(DTCCdb_StartDate, DTCCdb_EndDate, AssetClass)

# for updating db
DTCCdb_filename <- paste0("D:/R/DTCC Transaction/", "DTCCdb_", AssetClass, ".rds")
readRDS(DTCCdb_filename)


### Step 3 save the data
DTCCdb_filename <- paste0("D:/R/DTCC Transaction/", "DTCCdb_", AssetClass, ".rds")
saveRDS(DTCCdb, file = DTCCdb_filename)



###########
endtime <- Sys.time()
totaltime <- endtime - starttime
print(totaltime)