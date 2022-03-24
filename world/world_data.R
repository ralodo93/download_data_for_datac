setwd("world/")

load("../covid_data/covidCountryData_end.rda")
covidReAnalysedData = unique(covidReAnalysedData[,"Country",drop=F])

content = read.csv("https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv")
countries = unique(content$location)

content[content$location == "Congo","location"] = "Republic of Congo"
content[content$location == "Democratic Republic of Congo","location"] = "Democratic Republic of the Congo"
content[content$location == "Cote d'Ivoire","location"] = "Côte d'Ivoire"
content[content$location == "Eswatini","location"] = "Swaziland"
content[content$location == "Sao Tome and Principe","location"] = "São Tomé and Príncipe"
content[content$location == "Bonaire Sint Eustatius and Saba","location"] = "Bonaire, Sint Eustatius and Saba"
content[content$location == "Curacao","location"] = "Curaçao"
content[content$location == "Sint Maarten (Dutch part)","location"] = "Sint Maarten"
content[content$location == "Palestine","location"] = "Palestina"
content[content$location == "Timor","location"] = "Timor-Leste"
content[content$location == "Czechia","location"] = "Czech Republic"
content[content$location == "Faeroe Islands","location"] = "Faroe Islands"

content[content$location == "Vatican","location"] = "Vatican City"
content[content$location == "North Macedonia","location"] = "Macedonia"
content[content$location == "Micronesia (country)","location"] = "Micronesia"


content = content[content$location %in% covidReAnalysedData$Country,]

countries = unique(content$location)

library(parallel)

VacFunc = function(country,content){
  variables = c("population","new_cases","new_deaths","icu_patients","hosp_patients","stringency_index")
  content = content[content$location == country,]
  datesStr = unique(content$date)
  dateStr = datesStr[2]
  for (dateStr in datesStr){
    val = content[content$date == dateStr,variables]
    val[is.na(val)]=0
    content[content$date == dateStr,variables] = val[,variables]
  }
  
  content = content[,c("location","date",variables)]
  return(content)
}

res = mclapply(countries,VacFunc,content=content)

world_data = do.call("rbind",res)

save(world_data,file="world_data.RData")

setwd("../")
