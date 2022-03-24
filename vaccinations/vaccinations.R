setwd("./vaccinations")

# Load Data From Covid_Data
load("../covid_data/covidCountryData_end.rda")
covidReAnalysedData = unique(covidReAnalysedData[,"Country",drop=F])

# Read Vaccionation Data
content = read.csv('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv')
content = content[content$location %in% covidReAnalysedData$Country,]

countries = unique(content$location)

library(parallel)

VacFunc = function(country,content){
  # This function is used to calculate and control the vaccination data from each country
  variables = c("total_vaccinations","people_vaccinated","people_fully_vaccinated","people_vaccinated_per_hundred","people_fully_vaccinated_per_hundred")
  content = content[content$location == country,]
  datesStr = unique(content$date)
  people_vaccinated = 0
  people_fully_vaccinated = 0
  people_vaccinated_per_hundred = 0
  people_fully_vaccinated_per_hundred = 0
  total_vaccinations = 0
  dateStr = datesStr[1]
  for (dateStr in datesStr){
    val = content[content$date == dateStr,variables]
    val[is.na(val)]=0
    if (val$people_vaccinated < people_vaccinated){
      val$people_vaccinated = people_vaccinated
    }
    if (val$people_vaccinated_per_hundred < people_vaccinated_per_hundred){
      val$people_vaccinated_per_hundred = people_vaccinated_per_hundred
      }
    if (val$people_fully_vaccinated < people_fully_vaccinated){
      val$people_fully_vaccinated = people_fully_vaccinated
    }
    if (val$people_fully_vaccinated_per_hundred < people_fully_vaccinated_per_hundred){
      val$people_fully_vaccinated_per_hundred = people_fully_vaccinated_per_hundred
    }
    if (val$total_vaccinations < total_vaccinations){
      val$total_vaccinations = total_vaccinations
    }
    
    
    
    people_vaccinated = val$people_vaccinated
    people_fully_vaccinated = val$people_fully_vaccinated
    people_vaccinated_per_hundred = val$people_vaccinated_per_hundred
    people_fully_vaccinated_per_hundred = val$people_fully_vaccinated_per_hundred
    total_vaccinations = val$total_vaccinations
    
    content[content$date == dateStr,variables] = val[,variables]
    
  }
  
  content = content[,c("location","date",variables)]
  return(content)
}

vaccinations = do.call("rbind",mclapply(countries,VacFunc,content=content,mc.cores = 10))

save(vaccinations,file="vaccinations.RData")

print("Vaccinations finished")

setwd("../")