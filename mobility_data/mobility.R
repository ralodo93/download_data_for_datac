setwd("./mobility_data")

# Download all Mobility Data
urlMain = 'https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip'
download.file(urlMain, "regions.zip")

unzip("regions.zip")


# Load Covid Data
load("../covid_data/covidCountryData_end.rda")
covidReAnalysedData = unique(covidReAnalysedData[,c("Country","Region"),drop=F])

csvFiles = list.files(pattern = "*Report.csv")

library(parallel)

# Make some changes in the columns
content = do.call("rbind",mclapply(csvFiles,read.csv,mc.cores = 10))
countries = unique(content$country_region)
content[is.na(content$sub_region_1),"sub_region_1"] = ""
content[is.na(content$sub_region_2),"sub_region_2"] = ""
content[is.na(content$metro_area),"metro_area"] = ""
variables = colnames(content)[10:15]
content = content[,c("country_region","sub_region_1","sub_region_2","metro_area","date",variables)]

content[content$country_region == "The Bahamas","country_region"] = "Bahamas"
content[content$country_region == "Czechia","country_region"] = "Czech Republic"
content[content$country_region == "North Macedonia","country_region"] = "Macedonia"
content[content$country_region == "Myanmar (Burma)","country_region"] = "Myanmar"
content[content$country_region == "Réunion","country_region"] = "Reunion"

content_countries = content[content$sub_region_1 == "" & content$sub_region_2 == "" & content$metro_area == "",]
content_countries = content_countries[,c("country_region","sub_region_1","date",variables)]
content_countries$sub_region_1 = content_countries$country_region

countries_w_regions = unique(covidReAnalysedData[covidReAnalysedData$Country != covidReAnalysedData$Region,"Country"])

content = content[content$country_region %in% countries_w_regions,]

#### United Kingdom requires preprocessing ##
download.file("https://raw.githubusercontent.com/andreafalzetti/uk-counties-list/master/uk-counties/uk-counties-list.csv","UK.csv")
UK_regs = read.csv("UK.csv",header = F)
colnames(UK_regs) = c("Country","Region")
content_UK = content[content$country_region == "United Kingdom",]
content_UK = content_UK[content_UK$sub_region_1 %in% UK_regs$Region,]
content_UK = merge(content_UK,UK_regs,by.x = "sub_region_1",by.y = "Region")
content_UK$sub_region_1 = content_UK$Country
content_UK = content_UK[content_UK$sub_region_1 %in% c("England","Wales","Scotland","Northern Ireland"),]
content_UK = content_UK[,colnames(content)]
content_UK = aggregate(x = list(content_UK$retail_and_recreation_percent_change_from_baseline,
                                 content_UK$grocery_and_pharmacy_percent_change_from_baseline,
                                 content_UK$parks_percent_change_from_baseline,
                                 content_UK$transit_stations_percent_change_from_baseline,
                                 content_UK$workplaces_percent_change_from_baseline,
                                 content_UK$residential_percent_change_from_baseline),
                        by = list(content_UK$sub_region_1,content_UK$date), FUN = "mean",na.rm = TRUE)
colnames(content_UK) = c("sub_region_1","date",variables)
content_UK$country_region = "United Kingdom"
content_UK$sub_region_2 = ""
content_UK$metro_area = ""
content_UK = content_UK[,colnames(content)]
content = content[content$country_region != "United Kingdom",]
# content = rbind(content,content_UK)
###########################

##### Belgium need preprocessing too ###

content_bel = content[content$country_region == "Belgium",]
content_bel[content_bel$sub_region_1 == "Brussels","sub_region_2"] = "Brussels"
content_bel = content_bel[content_bel$sub_region_2 != "",]
content_bel = aggregate(x = list(content_bel$retail_and_recreation_percent_change_from_baseline,
                                content_bel$grocery_and_pharmacy_percent_change_from_baseline,
                                content_bel$parks_percent_change_from_baseline,
                                content_bel$transit_stations_percent_change_from_baseline,
                                content_bel$workplaces_percent_change_from_baseline,
                                content_bel$residential_percent_change_from_baseline),
                       by = list(content_bel$sub_region_2,content_bel$date), FUN = "mean",na.rm = TRUE)

colnames(content_bel) = c("sub_region_1","date",variables)
content_bel$country_region = "Belgium"
content_bel$sub_region_2 = ""
content_bel$metro_area = ""
content_bel = content_bel[,colnames(content)]
content_bel$sub_region_1 = ifelse(content_bel$sub_region_1 == "Province of Namur","Namur",content_bel$sub_region_1)
content = content[content$country_region != "Belgium",]

#######

######## Ukraine needs a little change ###

content_ukranie = content[content$country_region == "Ukraine" & content$metro_area != "",]
content_ukranie$sub_region_1 = "Kiev Oblast"
content_ukranie$metro_area = ""
content = content[content$country_region != "Ukraine",]

##### As well as Russia ###

content_russia = content[content$country_region == "Russia" & content$metro_area != "",]
content_russia[content_russia$metro_area == "Chelyabinsk Metropolitan Area","sub_region_1"] = "Chelyabinsk Oblast"
content_russia[content_russia$metro_area == "Kazan Metropolitan Area","sub_region_1"] = "Tatarstan Republic"
content_russia[content_russia$metro_area == "Krasnodar Metropolitan Area","sub_region_1"] = "Krasnodar Krai"
content_russia[content_russia$metro_area == "Krasnoyarsk Metropolitan Area","sub_region_1"] = "Krasnoyarsk Krai"
content_russia[content_russia$metro_area == "Moscow Metropolitan Area","sub_region_1"] = "Moscow Oblast"
content_russia[content_russia$metro_area == "Perm Metropolitan Area","sub_region_1"] = "Perm Krai"
content_russia[content_russia$metro_area == "Nizhny Novgorod Metropolitan Area","sub_region_1"] = "Nizhny Novgorod Oblast"
content_russia[content_russia$metro_area == "Novosibirsk Metropolitan Area","sub_region_1"] = "Novosibirsk Oblast"
content_russia[content_russia$metro_area == "Omsk Metropolitan Area","sub_region_1"] = "Omsk Oblast"
content_russia[content_russia$metro_area == "Rostov-on-Don Metropolitan Area","sub_region_1"] = "Rostov Oblast"
content_russia[content_russia$metro_area == "Saint Petersburg Metropolitan Area","sub_region_1"] = "Leningrad Oblast"
content_russia[content_russia$metro_area == "Samara Metropolitan Area","sub_region_1"] = "Samara Oblast"
content_russia[content_russia$metro_area == "Sochi Metropolitan Area","sub_region_1"] = "Krasnodar Krai"
content_russia[content_russia$metro_area == "Ufa Metropolitan Area","sub_region_1"] = "Bashkortostan Republic"
content_russia[content_russia$metro_area == "Voronezh Metropolitan Area","sub_region_1"] = "Voronezh Oblast"
content_russia[content_russia$metro_area == "Yekaterinburg Metropolitan Area","sub_region_1"] = "Sverdlovsk Oblast"
content_russia$metro_area = ""

content_russia = aggregate(x = list(content_russia$retail_and_recreation_percent_change_from_baseline,
                                 content_russia$grocery_and_pharmacy_percent_change_from_baseline,
                                 content_russia$parks_percent_change_from_baseline,
                                 content_russia$transit_stations_percent_change_from_baseline,
                                 content_russia$workplaces_percent_change_from_baseline,
                                 content_russia$residential_percent_change_from_baseline),
                        by = list(content_russia$sub_region_1,content_russia$date), FUN = "mean",na.rm = TRUE)

colnames(content_russia) = c("sub_region_1","date",variables)
content_russia$country_region = "Russia"
content_russia$sub_region_2 = ""
content_russia$metro_area = ""
content_russia = content_russia[,colnames(content)]

content = content[content$country_region != "Russia",]


########## Some country changes ####

content[content$country_region == "Mexico" & content$sub_region_1 == "Michoacán","sub_region_1"] = "Michoacan"
content[content$country_region == "Mexico" & content$sub_region_1 == "Querétaro","sub_region_1"] = "Queretaro"
content[content$country_region == "Mexico" & content$sub_region_1 == "Mexico City","sub_region_1"] = "Ciudad de Mexico"

content[content$country_region == "Brazil","sub_region_1"] = gsub("State of ","",content[content$country_region == "Brazil","sub_region_1"])
content[content$country_region == "Brazil" & content$sub_region_1 == "Amapá","sub_region_1"] = "Amapa"
content[content$country_region == "Brazil" & content$sub_region_1 == "Federal District","sub_region_1"] = "Distrito Federal"
content[content$country_region == "Brazil" & content$sub_region_1 == "Ceará","sub_region_1"] = "Ceara"
content[content$country_region == "Brazil" & content$sub_region_1 == "Goiás","sub_region_1"] = "Goias"
content[content$country_region == "Brazil" & content$sub_region_1 == "Pará","sub_region_1"] = "Para"
content[content$country_region == "Brazil" & content$sub_region_1 == "Espírito Santo","sub_region_1"] = "Espirito Santo"
content[content$country_region == "Brazil" & content$sub_region_1 == "Maranhão","sub_region_1"] = "Maranhao"
content[content$country_region == "Brazil" & content$sub_region_1 == "Paraná","sub_region_1"] = "Parana"
content[content$country_region == "Brazil" & content$sub_region_1 == "Paraíba","sub_region_1"] = "Paraiba"
content[content$country_region == "Brazil" & content$sub_region_1 == "São Paulo","sub_region_1"] = "Sao Paulo"
content[content$country_region == "Brazil" & content$sub_region_1 == "Rondônia","sub_region_1"] = "Rondonia"
content[content$country_region == "Brazil" & content$sub_region_1 == "Piauí","sub_region_1"] = "Piaui"

content[content$country_region == "Colombia" & content$sub_region_1 == "San Andres and Providencia","sub_region_1"] = "San Andres y Providencia"
content[content$country_region == "Colombia" & content$sub_region_1 == "North of Santander","sub_region_1"] = "Norte de Santander"

content[content$country_region == "Germany" & content$sub_region_1 == "Baden-Württemberg","sub_region_1"] = "Baden-Wurttemberg"
content[content$country_region == "Germany" & content$sub_region_1 == "Bavaria","sub_region_1"] = "Bayern"
content[content$country_region == "Germany" & content$sub_region_1 == "Lower Saxony","sub_region_1"] = "Niedersachsen"
content[content$country_region == "Germany" & content$sub_region_1 == "North Rhine-Westphalia","sub_region_1"] = "Nordrhein-Westfalen"
content[content$country_region == "Germany" & content$sub_region_1 == "Rhineland-Palatinate","sub_region_1"] = "Rheinland-Pfalz"
content[content$country_region == "Germany" & content$sub_region_1 == "Saxony","sub_region_1"] = "Sachsen"
content[content$country_region == "Germany" & content$sub_region_1 == "Saxony-Anhalt","sub_region_1"] = "Sachsen-Anhalt"
content[content$country_region == "Germany" & content$sub_region_1 == "Thuringia","sub_region_1"] = "Thuringen"

content[content$country_region == "India" & content$sub_region_1 == "Andaman and Nicobar Islands","sub_region_1"] = "Andaman and Nicobar"
content[content$country_region == "India" & content$sub_region_1 == "Delhi","sub_region_1"] = "NCT of Delhi"
# content[content$country_region == "India" & content$sub_region_1 == "Dadra and Nagar Haveli","sub_region_1"] = "Dadra and Nagar Haveli and Daman and Diu"
# content[content$country_region == "India" & content$sub_region_1 == "Daman and Diu","sub_region_1"] = "Dadra and Nagar Haveli and Daman and Diu"

content[content$country_region == "Italy" & content$sub_region_1 == "Friuli-Venezia Giulia","sub_region_1"] = "Friuli Venezia Giulia"
content[content$country_region == "Italy" & content$sub_region_1 == "Aosta","sub_region_1"] = "Valle d'Aosta"
content[content$country_region == "Italy" & content$sub_region_1 == "Apulia","sub_region_1"] = "Puglia"
content[content$country_region == "Italy" & content$sub_region_1 == "Lombardy","sub_region_1"] = "Lombardia"
content[content$country_region == "Italy" & content$sub_region_1 == "Piedmont","sub_region_1"] = "Piemonte"
content[content$country_region == "Italy" & content$sub_region_1 == "Sardinia","sub_region_1"] = "Sardegna"
content[content$country_region == "Italy" & content$sub_region_1 == "Sicily","sub_region_1"] = "Sicilia"
content[content$country_region == "Italy" & content$sub_region_1 == "Tuscany","sub_region_1"] = "Toscana"

content[content$country_region == "Sweden","sub_region_1"] = gsub(" County","",content[content$country_region == "Sweden","sub_region_1"])
content[content$country_region == "Sweden" & content$sub_region_1 == "Örebro","sub_region_1"] = "Orebro"
content[content$country_region == "Sweden" & content$sub_region_1 == "Jamtland","sub_region_1"] = "Jamtland Harjedalen"
content[content$country_region == "Sweden" & content$sub_region_1 == "Östergötland","sub_region_1"] = "Ostergotland"
content[content$country_region == "Sweden" & content$sub_region_1 == "Skåne","sub_region_1"] = "Skane"
content[content$country_region == "Sweden" & content$sub_region_1 == "Södermanland","sub_region_1"] = "Sormland"
content[content$country_region == "Sweden" & content$sub_region_1 == "Västerbotten","sub_region_1"] = "Vasterbotten"
content[content$country_region == "Sweden" & content$sub_region_1 == "Västernorrland","sub_region_1"] = "Vasternorrland"
content[content$country_region == "Sweden" & content$sub_region_1 == "Västmanland","sub_region_1"] = "Vastmanland"
content[content$country_region == "Sweden" & content$sub_region_1 == "Västra Götaland","sub_region_1"] = "Vastra Gotaland"

content[content$country_region == "Spain" & content$sub_region_1 == "Balearic Islands","sub_region_1"] = "Baleares"
content[content$country_region == "Spain" & content$sub_region_1 == "Basque Country","sub_region_1"] = "Pais Vasco"
content[content$country_region == "Spain" & content$sub_region_1 == "Canary Islands","sub_region_1"] = "Canarias"
content[content$country_region == "Spain" & content$sub_region_1 == "Castile and León","sub_region_1"] = "Castilla y Leon"
content[content$country_region == "Spain" & content$sub_region_1 == "Community of Madrid","sub_region_1"] = "Madrid"
content[content$country_region == "Spain" & content$sub_region_1 == "Castile-La Mancha","sub_region_1"] = "Castilla - La Mancha"
content[content$country_region == "Spain" & content$sub_region_1 == "Navarre","sub_region_1"] = "Navarra"
content[content$country_region == "Spain" & content$sub_region_1 == "Region of Murcia","sub_region_1"] = "Murcia"
content[content$country_region == "Spain" & content$sub_region_1 == "Valencian Community","sub_region_1"] = "C. Valenciana"

content[content$country_region == "Pakistan" & content$sub_region_1 == "Islamabad Capital Territory","sub_region_1"] = "Islamabad"

content[content$country_region == "Netherlands" & content$sub_region_1 == "North Brabant","sub_region_1"] = "Noord-Brabant"
content[content$country_region == "Netherlands" & content$sub_region_1 == "North Holland","sub_region_1"] = "Noord-Holland"
content[content$country_region == "Netherlands" & content$sub_region_1 == "South Holland","sub_region_1"] = "Zuid-Holland"


content[content$country_region == "Chile" & content$sub_region_1 == "Aysén","sub_region_1"] = "Aysen"
content[content$country_region == "Chile" & content$sub_region_1 == "Bio Bio","sub_region_1"] = "Biobio"
content[content$country_region == "Chile" & content$sub_region_1 == "Los Ríos","sub_region_1"] = "Los Rios"
content[content$country_region == "Chile" & content$sub_region_1 == "Magallanes and Chilean Antarctica","sub_region_1"] = "Magallanes"
content[content$country_region == "Chile" & content$sub_region_1 == "Ñuble" ,"sub_region_1"] = "Nuble"
content[content$country_region == "Chile" & content$sub_region_1 == "O'Higgins","sub_region_1"] = "OHiggins"
content[content$country_region == "Chile" & content$sub_region_1 == "Santiago Metropolitan Region","sub_region_1"] = "Metropolitana"
content[content$country_region == "Chile" & content$sub_region_1 == "Tarapacá","sub_region_1"] = "Tarapaca"
content[content$country_region == "Chile" & content$sub_region_1 == "Valparaíso","sub_region_1"] = "Valparaiso"

content[content$country_region == "Peru" & content$sub_region_1 == "Callao Region","sub_region_1"] = "Callao"
content[content$country_region == "Peru" & content$sub_region_1 == "Lima Region","sub_region_1"] = "Lima"

content = content[content$sub_region_2 == "" & content$metro_area == "" & content$sub_region_1 != "",]
content_regions = rbind(content,content_bel,content_UK,content_russia,content_ukranie)
content_regions = content_regions[,colnames(content_countries)]

rm(content,content_bel,content_russia,content_UK,content_ukranie,UK_regs)
  
mobility = rbind(content_countries,content_regions)

system("rm *csv *RData *zip")

save(mobility,file="mobility.RData")

print("Mobility finished")

setwd("../")

