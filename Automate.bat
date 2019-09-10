@echo off

rem this folder is scripted to facilitated automation of data collection

"C:\Program Files\R\R-3.6.1\bin\R.exe" R CMD BATCH "D:\\Projects\\DTCCDerivatives\\ConsolidatedDataCollection(excFX).R"

"C:\Program Files\R\R-3.6.1\bin\R.exe" R CMD BATCH "D:\\Projects\\DTCCDerivatives\\DTCC data.R"
