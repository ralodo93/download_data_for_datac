setwd("/hospital_and_icu_eu")
library(glue)

readFile = read.csv("https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv") # Read file

indicators = c("Daily hospital occupancy", "Daily ICU occupancy") # Variables of interest
readFile = readFile[readFile$indicator %in% indicators,] # Filter the DataFrame

load("../covid_data/covidCountryData_end.rda") # Load prebuilt covid dataframe
covidReAnalysedData = unique(covidReAnalysedData[,c("Country","Region"),drop=F]) # Get unique country_region pairs

##### This part is use to check that the name of countries from ECDC match with our names ####
countries_ecdc = unique(readFile$country)
not_match = countries_ecdc[!(countries_ecdc %in% covidReAnalysedData$Country)] # There aren't countries with problems

##### Convert dataframe to: Country Date Hospital_EU ICU_EU
allDates = unique(readFile$date) # List all dates

saveDataFrame = expand.grid(countries_ecdc,allDates) # Get all combinations of dates and countries
saveDataFrame$Hospital_EU = saveDataFrame$ICU_EU = NA # Fill the dataframe with zeros
colnames(saveDataFrame)[c(1,2)] = c("Country","Date") # Change name of cols
rownames(saveDataFrame) = glue("{saveDataFrame$Country}_{saveDataFrame$Date}") # We use the rownames to do the match easier

for (indicator in indicators){
  if (indicator == "Daily ICU occupancy"){  # Change the colname
    colnameIndicator = "ICU_EU"
  } else{
    colnameIndicator = "Hospital_EU"
  }
  ind_df = readFile[readFile$indicator == indicator,] # Filter by indicator
  ind_df = ind_df[,c("country","date","value")] # Keep the variables of interest
  rownames(ind_df) = glue("{ind_df$country}_{ind_df$date}") # Change rownames as we have done earlier
  colnames(ind_df) = c("Country","Date",colnameIndicator) # Change colnames
  saveDataFrame[rownames(ind_df),colnameIndicator] = ind_df[,colnameIndicator] # Match and fill the final dataframe
  
}


hosp_and_icu_eu = saveDataFrame # Change variable name
save(hosp_and_icu_eu,file = "hosp_icu.RData")
setwd("../")