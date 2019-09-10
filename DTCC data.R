## This file is scripted for studying DTCC FX derivatives data
# start_time <- Sys.time()
## init
# install.packages('dataonderivatives')
# install.packages('data.table')
library(dataonderivatives)
library(data.table)


################ Inputs ################
DTCCdb_StartDate <- as.Date('2017-01-01')
DTCCdb_EndDate <- Sys.Date()-1
OptionStockdb_StartDate <- as.Date('2019-01-01')
OptionStockdb_EndDate <- Sys.Date()
########################################


### Step 1 Define function for data scraping and cleaning

getDTCCdata <- function(start, end){

  # a. create empty data frame
  DTCCdb.temp <- data.frame(matrix(nrow = 0, ncol = 45))
  colnames(DTCCdb.temp) <- c( "DISSEMINATION_ID",                                   "ORIGINAL_DISSEMINATION_ID",                          "ACTION",
                              "EXECUTION_TIMESTAMP",                                "CLEARED",                                            "INDICATION_OF_COLLATERALIZATION",
                              "INDICATION_OF_END_USER_EXCEPTION",                   "INDICATION_OF_OTHER_PRICE_AFFECTING_TERM",           "BLOCK_TRADES_AND_LARGE_NOTIONAL_OFF-FACILITY_SWAPS",
                              "EXECUTION_VENUE",                                    "EFFECTIVE_DATE",                                     "END_DATE",
                              "DAY_COUNT_CONVENTION",                               "SETTLEMENT_CURRENCY",                                "ASSET_CLASS",
                              "SUB-ASSET_CLASS_FOR_OTHER_COMMODITY",                "TAXONOMY",                                           "PRICE_FORMING_CONTINUATION_DATA",
                              "UNDERLYING_ASSET_1",                                 "UNDERLYING_ASSET_2",                                 "PRICE_NOTATION_TYPE",
                              "PRICE_NOTATION",                                     "ADDITIONAL_PRICE_NOTATION_TYPE",                     "ADDITIONAL_PRICE_NOTATION",
                              "NOTIONAL_CURRENCY_1",                                 "NOTIONAL_CURRENCY_2",                                "ROUNDED_NOTIONAL_AMOUNT_1",
                              "ROUNDED_NOTIONAL_AMOUNT_2",                          "PAYMENT_FREQUENCY_1",                                "PAYMENT_FREQUENCY_2",
                              "RESET_FREQUENCY_1",                                  "RESET_FREQUENCY_2",                                  "EMBEDED_OPTION",
                              "OPTION_STRIKE_PRICE",                                "OPTION_TYPE",                                        "OPTION_FAMILY",
                              "OPTION_CURRENCY",                                    "OPTION_PREMIUM",                                     "OPTION_LOCK_PERIOD",
                              "OPTION_EXPIRATION_DATE",                             "PRICE_NOTATION2_TYPE",                               "PRICE_NOTATION2",
                              "PRICE_NOTATION3_TYPE",                               "PRICE_NOTATION3",                                    "TRANSACTION_RECORD_DATE"
                              )



  # b. data scraping
  dbDateRange <- seq(start, end, 'days')
  for (d in 1:length(dbDateRange)){
    DTCCdta.temp <- ddr(date = dbDateRange[d], asset_class = 'FX')
    if (nrow(DTCCdta.temp) >= 1) {
      DTCCdta.temp$TRANSACTION_RECORD_DATE <-dbDateRange[d]
      DTCCdb.temp <- rbind(DTCCdb.temp, DTCCdta.temp)
    }
    if (d %% 100 == 0 ) {
      Sys.sleep(1)
    }
  }



  # c. data cleaning
  #removing special character
  DTCCdb.temp$ROUNDED_NOTIONAL_AMOUNT_1 <- gsub("[+ ,]", "", as.character(DTCCdb.temp$ROUNDED_NOTIONAL_AMOUNT_1))
  DTCCdb.temp$ROUNDED_NOTIONAL_AMOUNT_2 <- gsub("[+ ,]", "", as.character(DTCCdb.temp$ROUNDED_NOTIONAL_AMOUNT_2))

  # change data type
  DTCCdb.temp$ROUNDED_NOTIONAL_AMOUNT_1 <- as.double(DTCCdb.temp$ROUNDED_NOTIONAL_AMOUNT_1)
  DTCCdb.temp$ROUNDED_NOTIONAL_AMOUNT_2 <- as.double(DTCCdb.temp$ROUNDED_NOTIONAL_AMOUNT_2)

  DTCCdb.temp$EFFECTIVE_DATE <- as.Date(DTCCdb.temp$EFFECTIVE_DATE)
  DTCCdb.temp$END_DATE <- as.Date(DTCCdb.temp$END_DATE)

  # d. return DTCCdb.temp
  return(DTCCdb.temp)
}





### Step 2 define function for getting HKDUSDOptiondb
getHKDUSDOptiondata <- function(dataDTCC){
  # set filter criteria
  filter_criteria <- (((dataDTCC$NOTIONAL_CURRENCY_1 == "HKD" & dataDTCC$NOTIONAL_CURRENCY_2 == "USD")|(dataDTCC$NOTIONAL_CURRENCY_1 == "USD" & dataDTCC$NOTIONAL_CURRENCY_2 == "HKD")) & dataDTCC$TAXONOMY == "ForeignExchange:VanillaOption"  & dataDTCC$ACTION == "NEW")

  # filter  the data
  DTCC_HKDUSDOption_db.temp <- dataDTCC[which(filter_criteria),]

  # extra cleaning
  # denominating in USD
  DTCC_HKDUSDOption_db.temp$NOTIONAL_USD <- ifelse(DTCC_HKDUSDOption_db.temp$NOTIONAL_CURRENCY_1 == "USD", DTCC_HKDUSDOption_db.temp$ROUNDED_NOTIONAL_AMOUNT_1,
                                                   ifelse(DTCC_HKDUSDOption_db.temp$NOTIONAL_CURRENCY_2 == "USD", DTCC_HKDUSDOption_db.temp$ROUNDED_NOTIONAL_AMOUNT_2,
                                                          NA))
  # change Option currency to USD
  DTCC_HKDUSDOption_db.temp$OPTION_TYPE.Con <- ifelse(DTCC_HKDUSDOption_db.temp$OPTION_CURRENCY == "USD", DTCC_HKDUSDOption_db.temp$OPTION_TYPE,
                                                      ifelse(DTCC_HKDUSDOption_db.temp$OPTION_CURRENCY == "HKD" & DTCC_HKDUSDOption_db.temp$OPTION_TYPE == "C-", "	P-",
                                                             ifelse(DTCC_HKDUSDOption_db.temp$OPTION_CURRENCY == "HKD" & DTCC_HKDUSDOption_db.temp$OPTION_TYPE == "P-", "	C-",
                                                                    NA
                                                             )
                                                      )
  )

  return(DTCC_HKDUSDOption_db.temp)
}


### Step 3 define function for getting stock option data
getHKDUSDOptiontockdata <- function(dataDTCCHKDUSDOption, start, end){
  # create empty df
  HKDUSDOptiontockdb.temp <- data.frame(matrix(nrow = 0, ncol = 13))
  colnames(HKDUSDOptiontockdb.temp) <- c('date',
                                         'CALL >= 7.85','CALL >= 7.85 %', 'CALL 7.75 - 7.85','CALL 7.75 - 7.85 %', 'CALL <= 7.75', 'CALL <= 7.75 %',
                                         'PUT >= 7.85', 'PUT >= 7.85 %', 'PUT 7.75 - 7.85', 'PUT 7.75 - 7.85 %', 'PUT <= 7.75', 'PUT <= 7.75 %')

  # loop through to get stock info
  # WB = weakbound, IR = in-range, SB = strong-bound
  dbDateRange <- seq(start, end, 'days')
  for (d in 1: length(dbDateRange)){
    DATE <- dbDateRange[d]
    C_WB <- sum(dataDTCCHKDUSDOption$NOTIONAL_USD[which(dataDTCCHKDUSDOption$OPTION_TYPE.Con == "C-" &
                                                        dataDTCCHKDUSDOption$OPTION_STRIKE_PRICE >= 7.85 &
                                                        dataDTCCHKDUSDOption$OPTION_EXPIRATION_DATE >= dbDateRange[d] &
                                                        dataDTCCHKDUSDOption$TRANSACTION_RECORD_DATE < dbDateRange[d])],
                na.rm = T)
    C_IR <- sum(dataDTCCHKDUSDOption$NOTIONAL_USD[which(dataDTCCHKDUSDOption$OPTION_TYPE.Con == "C-" &
                                                          dataDTCCHKDUSDOption$OPTION_STRIKE_PRICE < 7.85 & dataDTCCHKDUSDOption$OPTION_STRIKE_PRICE > 7.75 &
                                                          dataDTCCHKDUSDOption$OPTION_EXPIRATION_DATE >= dbDateRange[d]&
                                                          dataDTCCHKDUSDOption$TRANSACTION_RECORD_DATE < dbDateRange[d])],
                na.rm = T)
    C_SB <- sum(dataDTCCHKDUSDOption$NOTIONAL_USD[which(dataDTCCHKDUSDOption$OPTION_TYPE.Con == "C-" &
                                                          dataDTCCHKDUSDOption$OPTION_STRIKE_PRICE <= 7.75 &
                                                          dataDTCCHKDUSDOption$OPTION_EXPIRATION_DATE >= dbDateRange[d]&
                                                          dataDTCCHKDUSDOption$TRANSACTION_RECORD_DATE < dbDateRange[d])],
                na.rm = T)
    C_WB_pct <- C_WB/(C_WB+C_IR+C_SB)
    C_IR_pct <- C_IR/(C_WB+C_IR+C_SB)
    C_SB_pct <- C_SB/(C_WB+C_IR+C_SB)
    P_WB <- sum(dataDTCCHKDUSDOption$NOTIONAL_USD[which(dataDTCCHKDUSDOption$OPTION_TYPE.Con == "P-" &
                                                          dataDTCCHKDUSDOption$OPTION_STRIKE_PRICE >= 7.85 &
                                                          dataDTCCHKDUSDOption$OPTION_EXPIRATION_DATE >= dbDateRange[d]&
                                                          dataDTCCHKDUSDOption$TRANSACTION_RECORD_DATE < dbDateRange[d])],
                na.rm = T)
    P_IR <- sum(dataDTCCHKDUSDOption$NOTIONAL_USD[which(dataDTCCHKDUSDOption$OPTION_TYPE.Con == "P-" &
                                                          dataDTCCHKDUSDOption$OPTION_STRIKE_PRICE < 7.85 & dataDTCCHKDUSDOption$OPTION_STRIKE_PRICE > 7.75 &
                                                          dataDTCCHKDUSDOption$OPTION_EXPIRATION_DATE >= dbDateRange[d]&
                                                          dataDTCCHKDUSDOption$TRANSACTION_RECORD_DATE < dbDateRange[d])],
                na.rm = T)
    P_SB <- sum(dataDTCCHKDUSDOption$NOTIONAL_USD[which(dataDTCCHKDUSDOption$OPTION_TYPE.Con == "P-" &
                                                          dataDTCCHKDUSDOption$OPTION_STRIKE_PRICE <= 7.75 &
                                                          dataDTCCHKDUSDOption$OPTION_EXPIRATION_DATE >= dbDateRange[d]&
                                                          dataDTCCHKDUSDOption$TRANSACTION_RECORD_DATE < dbDateRange[d])],
                na.rm = T)
    P_WB_pct <- P_WB/(P_WB+P_IR+P_SB)
    P_IR_pct <- P_IR/(P_WB+P_IR+P_SB)
    P_SB_pct <- P_SB/(P_WB+P_IR+P_SB)
    # create temp df
    HKDUSDOptiontockdta.temp <- data.frame('date' = DATE,
                                           'CALL >= 7.85' = C_WB,'CALL >= 7.85 %' = C_WB_pct, 'CALL 7.75 - 7.85' = C_IR,'CALL 7.75 - 7.85 %' = C_IR_pct, 'CALL <= 7.75' = C_SB, 'CALL <= 7.75 %' = C_SB_pct,
                                           'PUT >= 7.85' = P_WB, 'PUT >= 7.85 %' = P_WB_pct, 'PUT 7.75 - 7.85' = P_IR, 'PUT 7.75 - 7.85 %' = P_IR_pct, 'PUT <= 7.75' = P_SB, 'PUT <= 7.75 %' = P_SB_pct)

    # append data
    HKDUSDOptiontockdb.temp <- rbind(HKDUSDOptiontockdb.temp, HKDUSDOptiontockdta.temp)
  }


  #USD Billion

  HKDUSDOptiontockdb.temp[,c(2,4,6,8,10,12)] <- data.frame(lapply(HKDUSDOptiontockdb.temp[,c(2,4,6,8,10,12)], function(x){x/1000000000}))


  # significant figures
  HKDUSDOptiontockdb.temp[,c(2,4,6,8,10,12)] <- data.frame(lapply(HKDUSDOptiontockdb.temp[,c(2,4,6,8,10,12)], function(x){signif(x, 3)}))

  #return db
  return(HKDUSDOptiontockdb.temp)
}



# ### Step 4 Setting up data base
# DTCCdb <- getDTCCdata(DTCCdb_StartDate, DTCCdb_EndDate)
# DTCC_HKDUSDOption_db <- getHKDUSDOptiondata(dataDTCC = DTCCdb)
# DTCC_HKDUSDOption_Stock_db <- getHKDUSDOptiontockdata(dataDTCCHKDUSDOption = DTCC_HKDUSDOption_db, start = OptionStockdb_StartDate, end = OptionStockdb_EndDate)
# colnames(DTCC_HKDUSDOption_Stock_db) <- c('date',
#                                           'CALL >= 7.85','CALL >= 7.85 %', 'CALL 7.75 - 7.85','CALL 7.75 - 7.85 %', 'CALL <= 7.75', 'CALL <= 7.75 %',
#                                           'PUT >= 7.85', 'PUT >= 7.85 %', 'PUT 7.75 - 7.85', 'PUT 7.75 - 7.85 %', 'PUT <= 7.75', 'PUT <= 7.75 %')
#
# saveRDS(DTCCdb, file = "D:/R/DTCC Transaction/DTCCdb_FX.rds")
# saveRDS(DTCC_HKDUSDOption_db, file = "D:/R/DTCC Transaction/DTCC_HKDUSDOption_db.rds")
# saveRDS(DTCC_HKDUSDOption_Stock_db, file = "D:/R/DTCC Transaction/DTCC_HKDUSDOption_Stock_db.rds")
# fwrite(DTCC_HKDUSDOption_Stock_db, file = "D:/R/DTCC Transaction/DTCC_HKDUSDOption_Stock_db.csv")


### Step 5 updating database

#import existing files
DTCCdb <- readRDS("D:/R/DTCC Transaction/DTCCdb_FX.rds")
DTCC_HKDUSDOption_db <- readRDS("D:/R/DTCC Transaction/DTCC_HKDUSDOption_db.rds")
DTCC_HKDUSDOption_Stock_db <- readRDS("D:/R/DTCC Transaction/DTCC_HKDUSDOption_Stock_db.rds")



# update DTCCdb and DTCC_HKDUSDOption_db
DTCCdb_append_StartDate <- max(DTCCdb$TRANSACTION_RECORD_DATE) +1
DTCCdb_append_EndDate <- Sys.Date() -1

t <- try(seq(DTCCdb_append_StartDate, DTCCdb_append_EndDate, by = 'days'))
if( class(t) == "try-error" ) {
  q(save = 'no')
}

DTCCdb.append <- getDTCCdata(DTCCdb_append_StartDate, DTCCdb_append_EndDate)

if(nrow(DTCCdb.append) > 0) {
  DTCCdb <- rbind(DTCCdb, DTCCdb.append)
  DTCCdb <- readRDS("D:/R/DTCC Transaction/DTCCdb_FX.rds")



  # update DTCC_HKDUSDOption_db
  DTCC_HKDUSDOption_db.append <- getHKDUSDOptiondata(dataDTCC = DTCCdb.append)
  if(nrow(DTCC_HKDUSDOption_db.append) > 0 {
    DTCC_HKDUSDOption_db <- rbind(DTCC_HKDUSDOption_db, DTCC_HKDUSDOption_db.append)
    saveRDS(DTCC_HKDUSDOption_db, file = "D:/R/DTCC Transaction/DTCC_HKDUSDOption_db.rds")
  })
}



# update DTCC_HKDUSDOption_Stock_db
Stockdb_StartDate <- max(DTCC_HKDUSDOption_Stock_db$date)+1
Stockdb_EndDate <- Sys.Date() -1

t <- try(seq(Stockdb_StartDate, Stockdb_EndDate, by = 'days'))
if( class(t) == "try-error" ) {
  q(save = 'no')
}

DTCC_HKDUSDOption_Stock_db.append <- getHKDUSDOptiontockdata(dataDTCCHKDUSDOption = DTCC_HKDUSDOption_db, start = Stockdb_StartDate, end = Stockdb_EndDate)
colnames(DTCC_HKDUSDOption_Stock_db.append) <- c('date',
                                          'CALL >= 7.85','CALL >= 7.85 %', 'CALL 7.75 - 7.85','CALL 7.75 - 7.85 %', 'CALL <= 7.75', 'CALL <= 7.75 %',
                                          'PUT >= 7.85', 'PUT >= 7.85 %', 'PUT 7.75 - 7.85', 'PUT 7.75 - 7.85 %', 'PUT <= 7.75', 'PUT <= 7.75 %')
DTCC_HKDUSDOption_Stock_db <- rbind(DTCC_HKDUSDOption_Stock_db, DTCC_HKDUSDOption_Stock_db.append)
saveRDS(DTCC_HKDUSDOption_Stock_db, file = "D:/R/DTCC Transaction/DTCC_HKDUSDOption_Stock_db.rds")
fwrite(DTCC_HKDUSDOption_Stock_db, file = "D:/R/DTCC Transaction/DTCC_HKDUSDOption_Stock_db.csv")


#############################################################
end_time <- Sys.time()

Total_time <- end_time - start_time

print(Total_time)
