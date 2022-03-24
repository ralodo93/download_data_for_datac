###### This script is used to download all covid data from JHU ######

setwd("./covid_data/")

system_name = Sys.info()[1] #Identify system to use threads. Windows does not support paralelization

mc_cores = ifelse(system_name != "Windows",5,1) #Assign 5 threads if the system is different to Windows

endDate = Sys.Date() - 1
urlMain = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'
colnamesGlobal = c()

### The first time we launch the script, it's neccesary to save all possible column names from all different files
### When we want to update the data, we simply load these column names from the file pre.RData
if (!(file.exists("pre.RData"))){
  theDate = as.Date("2020-01-22")
  while (theDate <= endDate){
    fileContent = read.csv(paste0(urlMain,format(theDate,"%m-%d-%Y"),".csv"),stringsAsFactors = F)
    colnames(fileContent) = gsub("/","_",colnames(fileContent))
    colnames(fileContent) = gsub("-","_",colnames(fileContent))
    colnames(fileContent) = gsub("_",".",colnames(fileContent))
    colnamesGlobal = unique(c(colnamesGlobal,colnames(fileContent)))
    theDate <- theDate + 1
  }
} else{
  load("pre.RData") 
  theDate = lastDate
  while (theDate <= endDate){
    fileContent = read.csv(paste0(urlMain,format(theDate,"%m-%d-%Y"),".csv"),stringsAsFactors = F)
    colnames(fileContent) = gsub("/","_",colnames(fileContent))
    colnames(fileContent) = gsub("-","_",colnames(fileContent))
    colnames(fileContent) = gsub("_",".",colnames(fileContent))
    colnamesGlobal = unique(c(colnamesGlobal,colnames(fileContent)))
    theDate <- theDate + 1
  }
  colnamesGlobal = unique(c(colnamesGlobal,columnames))
}

# Save the last date updated as well as the column names
lastDate = theDate - 1
columnames = colnames(dataDF)

save(lastDate,columnames,file="pre.RData")

theDates = seq(as.Date("01-22-2020","%m-%d-%y"),Sys.Date() - 1,by="days")

read_date_file = function(i,theDates,urlMain,fileContent,colnamesGlobal){
  # This function is used to read and manipulate all files from JHU
  theDate = theDates[i]
  url = paste0(urlMain,format(theDate,"%m-%d-%Y"),".csv")
  fileContent = read.csv(url,stringsAsFactors = F)
  colnames(fileContent) = gsub("/","_",colnames(fileContent))
  colnames(fileContent) = gsub("-","_",colnames(fileContent))
  colnames(fileContent) = gsub("_",".",colnames(fileContent))
  insertData = data.frame(matrix(ncol = length(colnamesGlobal)+1,nrow = nrow(fileContent)))
  colnames(insertData) = c(colnamesGlobal, "Date")
  insertData[,colnames(fileContent)] = fileContent
  date <- rep(theDate,nrow(insertData))
  insertData$Date <- date
  return(insertData)
}

library(parallel)
# To be more efficient, we use some threads
dataDF = do.call("rbind",mclapply(1:length(theDates),read_date_file,theDates,urlMain,fileContent,colnamesGlobal,mc.cores = mc_cores))

# This table contains some variables of interest from covid by each country, with regions and each day
print(tail(dataDF))

Regions = ifelse(dataDF$Province.State == "",dataDF$Country.Region,dataDF$Province.State)
dataDF$Regions = Regions
covidCountryData = dataDF[,c("Regions","Country.Region","Confirmed","Deaths","Lat","Long.","Incidence.Rate","Case.Fatality.Ratio",
                             "Incident.Rate","Date")]
colnames(covidCountryData) <- c("State", "CountryRegion", "Confirmed", "Deaths",
                                "Lat", "Long", "IncidenceRate", "CFR", "IncidentRate", "date")


trim <- function (x) gsub("^\\s+|\\s+$", "", x)
covidCountryData$State = trim(covidCountryData$State)
covidCountryData$CountryRegion = trim(covidCountryData$CountryRegion)
covidUnique = unique(covidCountryData[,c("State","CountryRegion","Lat","Long")])

# We change some names. For example: United States is named as US. All changes are located in this file:
matchingNames = read.delim("../geographic_data/matchingNames.tsv")

for (i in 1:nrow(matchingNames)){
  reg = matchingNames[i,1]
  change = matchingNames[i,2]
  covidUnique[covidUnique$CountryRegion == reg,"CountryRegion"] = change
  covidUnique[covidUnique$State == reg,"State"] = change

  covidCountryData[covidCountryData$CountryRegion == reg,"CountryRegion"] = change
  covidCountryData[covidCountryData$State == reg,"State"] = change
}

#We remove those data from "countries" such as cruises or olympic games
covidUnique = covidUnique[covidUnique$CountryRegion != "Delete",]
covidCountryData = covidCountryData[covidCountryData$CountryRegion != "Delete",]

covidCountryData = covidCountryData[covidCountryData$State != "Delete",]
covidUnique = covidUnique[covidUnique$State != "Delete",]

# We need to rename some regions
regionNames = read.delim("../geographic_data/regions.txt")
for (i in 1:nrow(regionNames)){
  tname = regionNames[i,1]
  fname = regionNames[i,2]
  covidUnique[covidUnique$CountryRegion == fname & covidUnique$State == tname,"CountryRegion"] = tname
  covidCountryData[covidCountryData$CountryRegion == fname & covidCountryData$State == tname,"CountryRegion"] = tname
}

#Rename some countries
covidUnique[covidUnique$State=="St Martin","State"] = "Saint-Martin"
covidUnique[covidUnique$CountryRegion=="St Martin","CountryRegion"] = "Saint-Martin"
covidUnique[covidUnique$State=="Fench Guiana","State"] = "French Guiana"
covidUnique[covidUnique$CountryRegion=="Fench Guiana","CountryRegion"] = "French Guiana"
covidUnique[covidUnique$State=="Saint Helena, Ascension and Tristan da Cunha","State"] = "Saint Helena"
covidUnique[covidUnique$CountryRegion=="Saint Helena, Ascension and Tristan da Cunha","CountryRegion"] = "Saint Helena"
covidUnique[covidUnique$State=="Falkland Islands (Malvinas)","State"] = "Falkland Islands"
covidUnique[covidUnique$CountryRegion=="Falkland Islands (Malvinas)","CountryRegion"] = "Falkland Islands"
covidUnique[covidUnique$State=="Falkland Islands (Islas Malvinas)","State"] = "Falkland Islands"
covidUnique[covidUnique$CountryRegion=="Falkland Islands (Islas Malvinas)","CountryRegion"] = "Falkland Islands"
covidUnique[covidUnique$State=="Virgin Islands","State"] = "Virgin Islands, U.S."
covidUnique[covidUnique$CountryRegion=="Virgin Islands","CountryRegion"] = "Virgin Islands, U.S."
covidUnique[covidUnique$State=="United States Virgin Islands","State"] = "Virgin Islands, U.S."
covidUnique[covidUnique$CountryRegion=="United States Virgin Islands","CountryRegion"] = "Virgin Islands, U.S."


covidCountryData[covidCountryData$State=="St Martin","State"] = "Saint-Martin"
covidCountryData[covidCountryData$CountryRegion=="St Martin","CountryRegion"] = "Saint-Martin"
covidCountryData[covidCountryData$State=="Fench Guiana","State"] = "French Guiana"
covidCountryData[covidCountryData$CountryRegion=="Fench Guiana","CountryRegion"] = "French Guiana"
covidCountryData[covidCountryData$State=="Saint Helena, Ascension and Tristan da Cunha","State"] = "Saint Helena"
covidCountryData[covidCountryData$CountryRegion=="Saint Helena, Ascension and Tristan da Cunha","CountryRegion"] = "Saint Helena"
covidCountryData[covidCountryData$State=="Falkland Islands (Malvinas)","State"] = "Falkland Islands"
covidCountryData[covidCountryData$CountryRegion=="Falkland Islands (Malvinas)","CountryRegion"] = "Falkland Islands"
covidCountryData[covidCountryData$State=="Falkland Islands (Islas Malvinas)","State"] = "Falkland Islands"
covidCountryData[covidCountryData$CountryRegion=="Falkland Islands (Islas Malvinas)","CountryRegion"] = "Falkland Islands"
covidCountryData[covidCountryData$State=="Virgin Islands","State"] = "Virgin Islands, U.S."
covidCountryData[covidCountryData$CountryRegion=="Virgin Islands","CountryRegion"] = "Virgin Islands, U.S."
covidCountryData[covidCountryData$State=="United States Virgin Islands","State"] = "Virgin Islands, U.S."
covidCountryData[covidCountryData$CountryRegion=="United States Virgin Islands","CountryRegion"] = "Virgin Islands, U.S."


### Fixing problem with States names from USA

usastates = read.csv("../geographic_data/USAStates.csv")[,c(1,3)]

for (i in 1:nrow(usastates)){
  stateName = usastates$State[i]
  stateCode = usastates$Code[i]
  stateCode = paste0(", ",stateCode)
  covidUnique[grep(stateCode,covidUnique$State),"State"] = stateName
  covidCountryData[grep(stateCode,covidCountryData$State),"State"] = stateName
}

### The same with Canada States

covidUnique[covidUnique$State == "Toronto, ON","State"]="Ontario"
covidUnique[covidUnique$State == "London, ON","State"]="Ontario"
covidUnique[covidUnique$State == "Montreal, QC","State"]="Quebec"
covidUnique[covidUnique$State == "Calgary, Alberta","State"]="Alberta"
covidUnique[covidUnique$State == "Edmonton, Alberta","State"]="Alberta"

covidCountryData[covidCountryData$State == "Toronto, ON","State"]="Ontario"
covidCountryData[covidCountryData$State == "London, ON","State"]="Ontario"
covidCountryData[covidCountryData$State == "Montreal, QC","State"]="Quebec"
covidCountryData[covidCountryData$State == "Calgary, Alberta","State"]="Alberta"
covidCountryData[covidCountryData$State == "Edmonton, Alberta","State"]="Alberta"


## Some regions fixing
covidUnique[covidUnique$State == "Bavaria","State"]="Bayern"
covidCountryData[covidCountryData$State == "Bavaria","State"]="Bayern"

covidUnique[covidUnique$State == "P.A. Bolzano","State"]="Bolzano"
covidCountryData[covidCountryData$State == "P.A. Bolzano","State"]="Bolzano"
covidUnique[covidUnique$State == "P.A. Trento","State"]="Trento"
covidCountryData[covidCountryData$State == "P.A. Trento","State"]="Trento"

covidUnique[covidUnique$State == "Moscow","State"]="Moscow Oblast"
covidCountryData[covidCountryData$State == "Moscow","State"]="Moscow Oblast"
covidUnique[covidUnique$State == "Saint Petersburg","State"]="Leningrad Oblast"
covidCountryData[covidCountryData$State == "Saint Petersburg","State"]="Leningrad Oblast"
covidUnique[covidUnique$State == "Khakassia Republic","State"]="Khakass"
covidCountryData[covidCountryData$State == "Khakassia Republic","State"]="Khakass"

covidUnique[covidUnique$State == "Kiev","State"]="Kiev Oblast"
covidCountryData[covidCountryData$State == "Kiev","State"]="Kiev Oblast"

covidUnique[covidUnique$State == "Chicago","State"]="Illinois"
covidCountryData[covidCountryData$State == "Chicago","State"]="Illinois"

#Some rows whose country name is None. Remove all
none_regions = covidUnique[covidUnique$State == "None",]
none_regions = covidCountryData[covidCountryData$State == "None",]

for (i in 1:nrow(none_regions)){
  count = none_regions[i,"CountryRegion"]
  covidCountryData[covidCountryData$State=="None" & covidCountryData$CountryRegion==count,"State"] = count
  covidUnique[covidUnique$State=="None" &covidUnique$CountryRegion==count,"State"] = count
}

# Change CFR

cfrFun = function(i,covidCountryData){
  cfrval = covidCountryData[i,"CFR"]
  if (!(is.na(cfrval))){
    cfrval = as.numeric(cfrval)
  } else{
    cfrval = ""
  }

  if (is.na(cfrval)){
    cfrval = ""
  }
  return(cfrval)
}

library(parallel)
ilist = seq(1,nrow(covidCountryData))
res = unlist(mclapply(ilist,cfrFun,covidCountryData=covidCountryData,mc.cores = mc_cores))
covidCountryData$CFR = res
covidCountryData[covidCountryData$CFR=="","CFR"] = NA
covidCountryData$CFR = as.numeric(covidCountryData$CFR)

## Some fixing 

covidCountryData[is.na(covidCountryData$Confirmed),"Confirmed"] = 0
covidCountryData$Confirmed = as.numeric(covidCountryData$Confirmed)
covidCountryData[is.na(covidCountryData$Deaths),"Deaths"] = 0
covidCountryData$Deaths = as.numeric(covidCountryData$Deaths)
covidCountryData$IncidenceRate = as.numeric(covidCountryData$IncidenceRate)
covidCountryData$IncidentRate = as.numeric(covidCountryData$IncidentRate)

ir = rowMeans(covidCountryData[,c("IncidenceRate", "IncidentRate")], na.rm=TRUE)
covidCountryData$IncidentRateGlobal = ir
covidCountryData[covidCountryData$IncidentRateGlobal == "NaN","IncidentRateGlobal"] = NA

# Final Step: Format the table

sum_up_region = function(reg,covidCountryData,country){
  covidData = covidCountryData[covidCountryData$State==reg & covidCountryData$CountryRegion == country,]
  covidData = covidData %>% group_by(date) %>% summarise_at(c("Confirmed","Deaths"),sum,na.rm=TRUE)
  covidData$date = as.Date(covidData$date)
  covidData$CFR = covidData$Deaths / covidData$Confirmed
  toIncident = covidCountryData[covidCountryData$State==reg & covidCountryData$CountryRegion == country,]
  toIncident = toIncident %>% group_by(date) %>% summarise_at("IncidentRateGlobal",mean,na.rm=TRUE)
  covidData$IncidentRate = toIncident$IncidentRateGlobal
  covidData$date = as.character(covidData$date)
  covidReAnalysedData = data.frame(Country=country,Region=reg,Confirmed=covidData$Confirmed,
                                   Deaths=covidData$Deaths,CFR=covidData$CFR,IncidentRate=covidData$IncidentRate,
                                   Date=covidData$date)
  return(covidReAnalysedData)
}

library(dplyr)

sum_up_country = function(country,covidCountryData){
  covidData = covidCountryData[covidCountryData$CountryRegion == country,]
  covidData = covidData %>% group_by(date) %>% summarise_at(c("Confirmed","Deaths"),sum,na.rm=TRUE)
  covidData$date = as.Date(covidData$date)
  covidData$CFR = (covidData$Deaths / covidData$Confirmed)*100
  toIncident = covidCountryData[covidCountryData$CountryRegion == country,]
  toIncident = toIncident %>% group_by(date) %>% summarise_at("IncidentRateGlobal",mean,na.rm=TRUE)
  covidData$IncidentRate = toIncident$IncidentRateGlobal
  covidData$date = as.character(covidData$date)
  covidReAnalysedData = data.frame(Country=country,Region=country,Confirmed=covidData$Confirmed,
                                                             Deaths=covidData$Deaths,CFR=covidData$CFR,
                                                             IncidentRate=covidData$IncidentRate,Date=covidData$date)
  regs = unique(covidUnique[covidUnique$CountryRegion == country,"State"])
  regs = regs[regs != country]
  regs = regs[regs != "Unknown"]
  if (length(regs) > 0){
    covidReAnalysedDataReg = do.call("rbind",lapply(regs,sum_up_region,covidCountryData,country))
    covidReAnalysedData = rbind(covidReAnalysedData,covidReAnalysedDataReg)
  }
  return(covidReAnalysedData)
}



covidReAnalysedData = do.call("rbind",mclapply(unique(covidUnique$CountryRegion),sum_up_country,covidCountryData,mc.cores = mc_cores))

save(covidReAnalysedData,file = "covidCountryData_end.rda")

print("Covid data finished")

setwd("../")
