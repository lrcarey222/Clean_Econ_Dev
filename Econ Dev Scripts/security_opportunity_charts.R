##Clean Technology Partnerships-----------------

allies<- country_info %>%
  filter(iso3c %in% c("USA","CAN", "JPN","AUS", "IND","MEX","KOR","GBR","DEU","FRA","ITA","BRA","SAU", "ZAF", "IDN", "NOR", "UAE","VNM","KEN","DNK","ARG","MAR","CHL")) %>%
  mutate(country=recode(
    country,
    "Vietnam" = "Viet Nam",
    "Iran, Islamic Rep." = "Iran",
    "Turkey"          = "Turkiye",
    "United kingdom"  = "United Kingdom",
    "Curacao"         = "Curaçao",
    "Saudi arabia"         = "Saudi Arabia",
    "Russian Federation" = "Russia",
    "Czechia"="Czech Republic",
    "Yemen, Rep."="Yemen",
    "Venezuela, RB"="Venezuela"
  )) 

 
#Energy Security-----------------------

#Consumption per Capita

allied_consumption <- energy_consumption_clean %>%
  filter(Country %in% allies$country,
         data_type=="index") %>%
  select(Country,tech,value) %>%
  pivot_wider(names_from="tech",values_from="value")
write.csv(allied_consumption,"Downloads/consumption.csv")  


#BNEF Installed Capacity
glimpse(neo_cap)

allied_cap_growth <- neo_cap %>%
  