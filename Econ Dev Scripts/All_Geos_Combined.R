##Combining All Geographies-------------------------------

# Load libraries-----------
library(lubridate)
library(tidyverse)
library(ggplot2)
library(zoo)
library(usmapdata)
library(readxl)
library(jsonlite)
library(httr)
library(stringr)
library(readxl)
library(sf)
library(tigris)
library(censusapi)
library(fuzzyjoin)
Sys.setenv(CENSUS_KEY='0b3d37ac56ab19c5a65cbc188f82d8ce5b36cfe6')
# Reload .Renviron
readRenviron("~/.Renviron")
# Check to see that the expected key is output in your R console
Sys.getenv("CENSUS_KEY")

#Upload Geographic Data-----------------------------
county_gdp<- read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/county_gdp_2022.csv',skip=3)
state_gdp<- read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/state_gdp_22.csv',skip=3)
msa_gdp<- read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/msa_gdp_2022.csv',skip=3)
states_simple <- read.csv('OneDrive - RMI/Documents/Data/US Maps etc/Regions/rmi_regions.csv')
county_cbsa<-read.csv("OneDrive - RMI/Documents/Data/US Maps etc/Regions/csa_cbsa_county.csv",skip=2)
EAs<-read_excel("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/BEA Economic Areas and Counties.xls",2)
cd_119_county<-read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/cd_119_counties.csv")
congress_119<-sf::st_read("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/congress_119.geojson") 

#https://www.americancommunities.org/
file_url <- 'https://www.americancommunities.org/wp-content/uploads/2023/08/2023-Typology-1.xlsx'
temp_file <- tempfile(fileext = ".xlsx")
GET(url = file_url, write_disk(temp_file, overwrite = TRUE))
us_communities <- read_excel(temp_file, sheet = 1)  # 'sheet = 1' to read the first sheet

county_2020<-us_communities %>%
  as.data.frame() %>%
  rename("GeoName"="County name",
         "fips"="Fips",
         "Community"="2023 Typology") %>%
  mutate(GEOID=substr(GEO_ID,10,14)) %>%
  select(GeoName,GEOID,fips,Community)

county_cbsa<-county_cbsa %>%
  mutate(state.fips=str_pad(FIPS.State.Code,2,pad="0"),
         county.fips=str_pad(FIPS.County.Code,3,pad="0")) %>%
  mutate(fips=as.numeric(paste0(state.fips,county.fips)))

cd_119<-cd_119_county %>%
  left_join(states_simple,by=c("State"="full")) %>%
  mutate(cd_119=paste0(abbr,"-",sprintf("%02d", as.numeric(CD119FP)))) %>%
  select(cd_119,GEOID_2,GEOID,percent_district)

pea_county <- read_excel("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/FCC_PEA_website.xlsx",3)
pea<-read_excel("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/FCC_PEA_website.xlsx",2)
pea<- pea_county %>%
  mutate(fips=as.numeric(FIPS)) %>%
  left_join(pea,by="FCC_PEA_Number") %>%
  select(fips,FCC_PEA_Name,FCC_PEA_Number)

geo<-county_2020 %>%
  mutate(state.fips=as.numeric(substr(GEOID,1,2))) %>%
  select(state.fips,GeoName,fips,Community) %>%
  left_join(states_simple %>%
              select(fips,abbr,full),by=c("state.fips"="fips")) %>%
  rename(State.Name=full,
         state_abbr=abbr) %>%
  left_join(census_divisions,by=c("State.Name"="State","state_abbr"="State.Code")) %>%
  select(Region,Division,State.Name,state_abbr,state.fips,GeoName,fips,Community) %>%
  left_join(county_cbsa %>%
              select(fips,CBSA.Title,CBSA.Code),by="fips") %>%
  left_join(pea %>%
              rename("PEA"="FCC_PEA_Name"),by="fips") %>%
  left_join(cd_119,by=c("fips"="GEOID")) %>%
  left_join(county_gdp %>%
              mutate(gdp=as.numeric(X2022),
                     fips=as.numeric(GeoFips))%>%
              select(fips,gdp),by=c("fips")) 


# For States
geo_states <- geo %>% 
  select(geo_name = State.Name, geo_code = state.fips) %>% 
  distinct() %>% 
  mutate(geo = "State",
         geo_code = as.character(geo_code))

# For Counties
geo_counties <- geo %>% 
  select(geo_name = GeoName, geo_code = fips) %>% 
  distinct() %>% 
  mutate(geo = "County",
         geo_code = as.character(geo_code))

# For Congressional Districts
geo_cd <- geo %>% 
  select(geo_name = cd_119, geo_code = GEOID_2) %>% 
  distinct() %>% 
  mutate(geo = "Congressional District",
         geo_code=as.character(geo_code))

# For Economic Areas
geo_pea <- geo %>% 
  select(geo_name = PEA, geo_code = FCC_PEA_Number) %>% 
  distinct() %>% 
  mutate(geo = "Economic Area",
         geo_code = as.character(geo_code))

# For Metro Areas
geo_metro <- geo %>% 
  select(geo_name = CBSA.Title, geo_code = CBSA.Code) %>% 
  distinct() %>% 
  mutate(geo = "Metro Area")

# Now all geo_code columns are character, so bind_rows() will work
geo_long_all <- bind_rows(geo_states, geo_counties, geo_cd, geo_pea, geo_metro)

geo_long <- geo %>%
  select(state_abbr, State.Name, PEA, CBSA.Title, cd_119,GeoName) %>%
  pivot_longer(
    cols = c(State.Name, PEA, CBSA.Title, cd_119,GeoName),
    names_to = "geo_type",
    values_to = "geo_name"
  ) %>%
  distinct(geo_name, state_abbr) %>%
  filter(!is.na(geo_name))

#CIM Recent Announcements------------------------------
investment<- read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q4_2024/quarterly_actual_investment.csv',skip=5)
facilities <- read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q4_2024/manufacturing_energy_and_industry_facility_metadata.csv',skip=5)
socioecon<- read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q4_2024/socioeconomics.csv',skip=5)

facilities_clean <- facilities %>%
  as.data.frame() %>%
  mutate(fips=county_2020_geoid) %>%
  #select(unique_id:Production_Date,coal_closure_community:energy_community,GEOID) %>%
  mutate(announce_year=substr(Announcement_Date,1,7))  %>%
  rename(cd_119=CD119_2024_Name) %>%
  left_join(geo %>%
              select(-cd_119),by="fips") %>%
  distinct()

#Technology
# List of geographies
geographies <- c("State.Name", "cd_119", "PEA", "CBSA.Title","GeoName")

# Initialize an empty list to store results
results_list <- list()

# Loop through each geography
for (geog in geographies) {
  # Process data for the current geography
  facilities_tech_geog <- facilities_clean %>%
    filter(State.Name != "",
           !is.na(State.Name),
           !is.na(`PEA`),
           !is.na(CBSA.Title),
           !is.na(cd_119), !is.na(GeoName),
           !is.na(GeoName),
           Current_Facility_Status != "C",
           substr(announce_year,1,4) %in% c("2021","2022","2023","2024")) %>%
    group_by(!!sym(geog), Segment, Technology) %>%
    distinct(!!sym(geog), Segment, Technology,Estimated_Total_Facility_CAPEX) %>%
    summarize(Estimated_Total_Facility_CAPEX  = sum(Estimated_Total_Facility_CAPEX , na.rm = TRUE), .groups = "drop") %>%
    group_by(Segment) %>%
    mutate(Percentile = percent_rank(-Estimated_Total_Facility_CAPEX ),
           Rank = rank(-Estimated_Total_Facility_CAPEX )) %>%
    arrange(desc(Estimated_Total_Facility_CAPEX )) %>%
    mutate(industry = ifelse(Segment == "Manufacturing", 
                             paste0(Technology, " Manufacturing"), 
                             Technology)) %>%
    ungroup() %>%
    mutate(geography = geog)  # Add the geography as an identifier
  
  # Append the result to the list
  results_list[[geog]] <- facilities_tech_geog
}

# Combine all dataframes into one
facilities_tech <- bind_rows(results_list)
facilities_tech<-facilities_tech %>%
  select(State.Name,cd_119,PEA,CBSA.Title,GeoName,industry,Estimated_Total_Facility_CAPEX) %>%
  filter(industry %in% c("Solar",
                         "Storage",
                         "Batteries Manufacturing",
                         "Hydrogen",
                         "Wind Manufacturing",
                         "Zero Emission Vehicles Manufacturing",
                         "Solar Manufacturing",
                         "Critical Minerals Manufacturing")) %>%
  pivot_wider(names_from=industry, names_prefix = "inv_",values_from=Estimated_Total_Facility_CAPEX)


#Top 5 Technologies

# Initialize an empty list to store results
results_list <- list()

# Loop through each geography
for (geog in geographies) {
  # Process data for the current geography
  facilities_geog_tech5 <- facilities_clean %>%
    filter(State.Name != "",
           !is.na(State.Name),
           !is.na(PEA),
           !is.na(CBSA.Title),
           !is.na(cd_119), !is.na(GeoName),
           Current_Facility_Status != "C",
           substr(announce_year,1,4) %in% c("2023","2024")) %>%
    group_by(!!sym(geog), Segment, Technology,Subcategory) %>%
    distinct(!!sym(geog), Segment, Technology,Subcategory,Estimated_Total_Facility_CAPEX) %>%
    summarize_at(vars(Estimated_Total_Facility_CAPEX),sum,na.rm=T)%>%
    group_by(!!sym(geog)) %>%
    mutate(Rank = rank(-Estimated_Total_Facility_CAPEX)) %>%
    filter(Rank < 6) %>%
    mutate(top_inds = ifelse(Segment=="Manufacturing",paste0(Rank,". ",Subcategory," ",Segment," ($",round(Estimated_Total_Facility_CAPEX,1),"m), "),
                             paste0(Rank,". ",Subcategory," ($",round(Estimated_Total_Facility_CAPEX,1),"m), "))) %>%
    arrange(!!sym(geog),Rank) %>%
    select(!!sym(geog),Rank,top_inds) %>%
    pivot_wider(names_from=Rank,values_from=top_inds) %>%
    mutate(across(`1`:`5`, ~ replace_na(as.character(.), ""))) %>%
    # Combine the columns into inv_description
    mutate(inv_description = paste(`1`, `2`, `3`, `4`, `5`, sep = "")) %>%
    # Remove commas left at the end of the string
    mutate(inv_description = str_remove_all(inv_description, ", $")) %>%
    select(!!sym(geog),inv_description)
  # Append the result to the list
  results_list[[geog]] <- facilities_geog_tech5
}
facilities_tech5 <- bind_rows(results_list)

facilities_tech5<-facilities_tech5 %>%
  mutate(inv_description=str_replace_all(inv_description,"NULL",""))


#Total Investment
# Initialize an empty list to store results
results_list <- list()

# Loop through each geography
for (geog in geographies) {
  # Process data for the current geography
  facilities_geog_total <- facilities_clean %>%
    mutate(announce_year = as.Date(Announcement_Date)) %>%
    filter(Investment_Status != "",
           announce_year > "2022-08-15") %>%
    mutate(Estimated_Total_Facility_CAPEX = ifelse(is.na(Estimated_Total_Facility_CAPEX), 0, Estimated_Total_Facility_CAPEX)) %>%
    group_by(!!sym(geog), Current_Facility_Status) %>%
    distinct(!!sym(geog), Current_Facility_Status, Estimated_Total_Facility_CAPEX) %>%
    summarize_at(vars(Estimated_Total_Facility_CAPEX), sum, na.rm = TRUE) %>%
    mutate(Estimated_Total_Facility_CAPEX = ifelse(is.na(Estimated_Total_Facility_CAPEX), 0, Estimated_Total_Facility_CAPEX)) %>%
    pivot_wider(names_from = Current_Facility_Status, values_from = Estimated_Total_Facility_CAPEX) %>%
    rename(
      "Operating Investment since IRA" = `Operating`,
      "Announced Investment since IRA" = `Announced`,
      "Investment under Construction since IRA" = `Under Construction`
    ) %>%
    ungroup() %>%
    mutate(
      Operating_Investment_Rank = rank(-`Operating Investment since IRA`),
      total_investment = rowSums(across(c(`Operating Investment since IRA`,
                                          `Announced Investment since IRA`,
                                          `Investment under Construction since IRA`), ~ replace_na(.x, 0)))) %>%
    arrange(desc(`Operating Investment since IRA`))
  
  facilities_geog_total <- facilities_geog_total %>%
    mutate(Total_Investment_Rank = rank(-total_investment)) %>%
    arrange(desc(total_investment))
  
  
  results_list[[geog]] <- facilities_geog_total
  
}
facilities_total <- bind_rows(results_list)

#Total Manufacturing Investment
# Initialize an empty list to store results
results_list <- list()

# Loop through each geography
for (geog in geographies) {
  facilities_geog_total <- facilities_clean %>%
    mutate(
      announce_year = as.Date(Announcement_Date),
      Estimated_Total_Facility_CAPEX = ifelse(is.na(Estimated_Total_Facility_CAPEX), 0, Estimated_Total_Facility_CAPEX)
    ) %>%
    filter(
      Decarb_Sector == "Clean Tech Manufacturing",
      Investment_Status != "C",
      Investment_Status != "R",
      announce_year > "2022-08-15"
    ) %>%
    group_by(!!sym(geog), Current_Facility_Status) %>%
    distinct(!!sym(geog), Current_Facility_Status, Estimated_Total_Facility_CAPEX) %>%
    summarize(
      Estimated_Total_Facility_CAPEX = sum(Estimated_Total_Facility_CAPEX, na.rm = TRUE),
      .groups = "drop" # Ungroup after summarization
    ) %>%
    pivot_wider(
      names_from = Current_Facility_Status,
      values_from = Estimated_Total_Facility_CAPEX,
      values_fill = list(Estimated_Total_Facility_CAPEX = 0)
    ) %>%
    rename(
      "Operating Cleantech Manufacturing Investment since IRA" = `Operating`,
      "Announced Cleantech Manufacturing Investment since IRA" = `Announced`,
      "Cleantech Manufacturing Investment under Construction since IRA" = `Under Construction`
    ) %>%
    mutate(
      Total_Manufacturing_Investment = rowSums(across(c(
        `Operating Cleantech Manufacturing Investment since IRA`,
        `Announced Cleantech Manufacturing Investment since IRA`,
        `Cleantech Manufacturing Investment under Construction since IRA`
      ), ~ replace_na(.x, 0)))
    )
  
  # Compute ranks after ungrouping
  facilities_geog_total <- facilities_geog_total %>%
    mutate(Total_Manufacturing_Investment_Rank = rank(-Total_Manufacturing_Investment)) %>%
    arrange(desc(Total_Manufacturing_Investment))
  
  results_list[[geog]] <- facilities_geog_total
}

# Combine results into a single dataframe
facilities_man_total <- bind_rows(results_list)



#Single Largest Investment
results_list <- list()

# Loop through each geography
for (geog in geographies) {
  # Process data for the current geography
  facilities_geog_total <- facilities_clean %>%
    mutate(announce_year=as.Date(Announcement_Date)) %>%
    filter(announce_year > "2022-08-15",
           Investment_Status != "C",
           Investment_Status != "R") %>%
    group_by(!!sym(geog)) %>%
    slice_max(order_by=Estimated_Total_Facility_CAPEX,n=1) %>%
    mutate(topinv_desc = paste0("In ", substr(announce_year,1,4),", ", Company,ifelse(Current_Facility_Status=="False"," announced an investment of "," made an investment of "),
                                round(Estimated_Total_Facility_CAPEX,1)," million dollars in a ",ifelse(Segment=="Manufacturing",paste0(Technology," Manufacturing"),Technology),
                                " project.")) %>%
    select(!!sym(geog),topinv_desc)
  results_list[[geog]] <- facilities_geog_total
  
}
facilities_top <- bind_rows(results_list)
facilities_top <- facilities_top %>% distinct()

results_list <- list()

#Largest Companies

results_list <- list()

# Loop through each geography
for (geog in geographies) {
  facility_company <- facilities_clean %>%
    filter(Investment_Status != "C",
           Investment_Status != "R") %>%
    mutate(date = as.Date(Announcement_Date)) %>%
    filter(date>"2022-08-15") %>%
    mutate(Company = gsub("\\([0-9]+\\)", "", Company),
           Company=gsub(", LLC","",Company)) %>%
    group_by(!!sym(geog),Company) %>%
    distinct(!!sym(geog),Company,Estimated_Total_Facility_CAPEX) %>%
    summarize_at(vars(Estimated_Total_Facility_CAPEX),sum,na.rm=T)  %>%
    group_by(!!sym(geog)) %>%
    slice_max(order_by=Estimated_Total_Facility_CAPEX,n=3) %>%
    arrange(desc(Estimated_Total_Facility_CAPEX), .by_group = TRUE) %>%  # Arrange by weighted_density within each group
    summarise(
      top_industries = paste(
        head(paste0(Company, " ($", round(Estimated_Total_Facility_CAPEX,1), "m)"), 3),
        collapse = ", "
      )) %>%
    ungroup()
  results_list[[geog]] <- facility_company
  
}
facility_company <- bind_rows(results_list)

#Investment/GDP
results_list <- list()

for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    gdp_geo <- geo %>%
      group_by(!!sym(geog)) %>%
      summarize(
        gdp = sum(gdp * percent_district / 100, na.rm = TRUE),
        .groups = "drop"
      )
  } else {
    # General case for other geographies
    gdp_geo <- geo %>%
      group_by(!!sym(geog)) %>%
      distinct(!!sym(geog),gdp) %>%
      summarize(
        gdp = sum(gdp, na.rm = TRUE),
        .groups = "drop"
      )
  }
  
  results_list[[geog]] <- gdp_geo
}

# Combine all results into one dataframe
gdp_geos <- bind_rows(
  lapply(names(results_list), function(name) {
    results_list[[name]] %>%
      mutate(geo = name)  # Add a column to indicate the geography
  })
)

# View the final results
View(gdp_geos)
gdp_geos %>%
  mutate(geo=ifelse(!is.na(State.Name),"State",
                    ifelse(!is.na(cd_119), "Congressional District",
                           ifelse(!is.na(PEA),"Economic Area",
                                  ifelse(!is.na(GeoName),"County","Metro Area"))))) %>%
  mutate(geo_name=ifelse(!is.na(State.Name),State.Name,
                         ifelse(!is.na(cd_119), cd_119,
                                ifelse(!is.na(PEA),PEA,
                                       ifelse(!is.na(GeoName),GeoName,CBSA.Title))))) %>%
  filter(!is.na(geo_name),
         geo_name != "",
         geo_name != "NA-NA") %>%
  filter(geo != "Economic Area") %>%
  arrange(desc(gdp)) %>%
  head(20) %>%
  write.csv("Downloads/gdp_geo.csv")

facilities_all <-facilities_total %>%
  select(State.Name,cd_119,PEA,CBSA.Title,GeoName,total_investment,Total_Investment_Rank,`Operating Investment since IRA`,`Announced Investment since IRA`,`Investment under Construction since IRA`) %>%
  left_join(gdp_geos,by=geographies) %>%
  mutate_at(vars(total_investment,
                 `Operating Investment since IRA`,
                 `Announced Investment since IRA`,
                 `Investment under Construction since IRA`),
            ~ replace_na(., 0)) %>%
  mutate(inv_gdp=(total_investment/gdp)*100) %>%
  left_join(facilities_man_total,by=geographies) %>%
  left_join(facilities_tech,by=geographies) %>%
  left_join(facilities_tech5,by=geographies) %>%
  left_join(facilities_top,by=geographies) %>%
  left_join(facility_company,by=geographies) %>%
  mutate(geo=ifelse(!is.na(State.Name),"State",
                    ifelse(!is.na(cd_119), "Congressional District",
                           ifelse(!is.na(PEA),"Economic Area",
                                  ifelse(!is.na(GeoName),"County","Metro Area"))))) %>%
  mutate(geo_name=ifelse(!is.na(State.Name),State.Name,
                         ifelse(!is.na(cd_119), cd_119,
                                ifelse(!is.na(PEA),PEA,
                                       ifelse(!is.na(GeoName),GeoName,CBSA.Title))))) %>%
  filter(!is.na(geo_name),
         geo_name != "",
         geo_name != "NA-NA") %>%
  ungroup()%>%
  select(geo,geo_name,total_investment:top_industries) %>%
  group_by(geo) %>%
  mutate(CAPEX_GDP_Rank= rank(-inv_gdp)) 

facilities_clean_geo<-facilities_clean %>%
  select(-percent_district,
         -State) %>%
  filter(as.Date(Announcement_Date)>"2022-08-01") %>%
  rename("State"="State.Name",
         "Economic Area" = "PEA",
         "Metro Area"="CBSA.Title",
         "Congressional District" = "cd_119",
         "County"="GeoName") %>%
  #left_join(census_divisions,by=c("State"="State")) %>%
  distinct(Region,Division,State,`Economic Area`,`Metro Area`,`Congressional District`,`County`,Company,Decarb_Sector,Technology,Estimated_Total_Facility_CAPEX,Latitude,Longitude) 

#Renewable Energy Capacity-------------------
url <- 'https://www.eia.gov/electricity/data/eia860m/xls/february_generator2025.xlsx'
destination_folder<-'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/States Data/'
file_path <- paste0(destination_folder, "eia_op_gen.xlsx")
downloaded_content <- GET(url, write_disk(file_path, overwrite = TRUE))

#Operating Generation
op_gen <- read_excel(file_path, sheet = 1,skip=2)

op_gen_clean <- op_gen %>% 
  filter(!is.na(Latitude) & !is.na(Longitude))
op_gen_sf <- st_as_sf(op_gen_clean, coords = c("Longitude", "Latitude"), crs = 4326)
op_gen_sf <- st_transform(op_gen_sf, crs = st_crs(counties))

op_gen_with_county <- st_join(op_gen_sf, counties)
op_gen_sf <- st_transform(op_gen_sf, crs = st_crs(congress_119))
op_gen_cd<-st_join(op_gen_sf,congress_119)
op_gen_cd <- op_gen_cd %>%
  st_drop_geometry() %>%
  left_join(state_lookup, by = "STATEFP") %>%
  mutate(
    district_num = sprintf("%02d", as.numeric(CD119FP)),
    cd_119 = paste0(state_abbrev, "-", district_num)
  ) 

op_gen_geo <- op_gen_with_county %>%
  st_drop_geometry() %>%
  filter(Status=="(OP) Operating") %>%
  mutate(fips = as.numeric(GEOID),
         unique_id=paste0(as.character(`Entity ID`),as.character(`Generator ID`))) %>%
  select(unique_id,`Plant State`, Technology, `Operating Year`, `Nameplate Capacity (MW)`, fips) %>%
  left_join(
    op_gen_cd %>%
      select(`Plant State`,cd_119, Technology, `Operating Year`, `Nameplate Capacity (MW)`),
    by = c("Plant State", "Technology", "Operating Year", "Nameplate Capacity (MW)")
  ) %>%
  left_join(
    geo %>%
      select(-cd_119,-percent_district) %>%
      distinct(),
    by = "fips"
  )  %>%
  distinct()

results_list <- list()
for (geog in geographies) {
  region_rengen <- op_gen_geo %>%
    as.data.frame() %>%
    mutate(clean = case_when(
      Technology == "Natural Gas Steam Turbine" ~ "Fossil",
      Technology == "Natural Gas Fired Combined Cycle" ~ "Fossil",
      Technology == "Natural Gas Internal Combustion Engine" ~ "Fossil",
      Technology == "Natural Gas Fired Combustion Turbine" ~ "Fossil",
      Technology == "Conventional Steam Coal" ~ "Fossil",
      Technology == "Conventional Hydroelectric" ~ "Clean",
      Technology == "Onshore Wind Turbine" ~ "Clean",
      Technology == "Batteries" ~ "Clean",
      Technology == "Solar Photovoltaic" ~ "Clean",
      Technology == "Solar Thermal with Energy Storage" ~ "Clean",
      Technology == "Hydroelectric Pumped Storage" ~ "Clean",
      Technology == "Geothermal" ~ "Clean",
      Technology == "Wood/Wood Waste Biomass" ~ "Clean",
      Technology == "Nuclear" ~ "Clean",
      Technology == "Petroleum Liquids" ~ "Fossil",
      TRUE ~ NA_character_  # Catch any unmatched cases
    )) %>%
    filter(!is.na(clean)) %>%
    group_by(!!sym(geog), clean,Technology) %>%  # Group only by geog and clean
    complete(`Operating Year` = 1900:2024, fill = list(capacity = 0)) %>%  # Complete the full range of years
    ungroup() %>%  # Ungroup to avoid issues with cumsum
    group_by(!!sym(geog), clean, Technology,`Operating Year`) %>%
    summarize(capacity = sum(`Nameplate Capacity (MW)`, na.rm = TRUE), .groups = "drop") %>%  # Aggregate capacity 
    group_by(!!sym(geog),Technology,clean) %>%
    mutate(cum_cap = cumsum(capacity))  # Calculate cumulative capacity
  
  results_list[[geog]] <- region_rengen
}

# Bind the results into a single dataframe if needed
final_results <- bind_rows(results_list)


rengen <- final_results %>%
  ungroup() %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA",
         !is.na(clean)) %>%
  select(-State.Name, -cd_119, -PEA, -CBSA.Title,-GeoName,-capacity) %>%
  group_by(geo,geo_name,clean,`Operating Year`) %>%
  summarize_at(vars(cum_cap),sum,na.rm=T) %>%
  filter(`Operating Year` %in% c("2019","2021","2024")) %>%
  pivot_wider(names_from=`Operating Year`,values_from=cum_cap) %>%
  mutate(change_19_24= round(`2024` - `2019`, 1),
         growth_19_24 = round((`2024` / `2019` - 1) * 100, 1),
         change_21_24= round(`2024` - `2021`, 1),
         growth_21_24 = round((`2024` / `2021` - 1) * 100, 1)) %>%
  select(-`2019`,-`2021`) %>%
  pivot_wider(
    names_from = clean,
    values_from = c(`2024`, growth_19_24,change_19_24,growth_21_24,change_21_24),
    names_sep = "_"
  )%>%
  mutate(`2024_Fossil`=ifelse(is.na(`2024_Fossil`),0,`2024_Fossil`)) %>%
  mutate(clean_share = round(`2024_Clean` / (`2024_Clean` + `2024_Fossil`) * 100, 1))


tech_cap <- final_results %>%
  ungroup() %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA",
         !is.na(clean)) %>%
  select(-State.Name, -cd_119, -PEA, -CBSA.Title,-GeoName,-capacity) %>%
  filter(`Operating Year` %in% c("2014","2021","2024")) %>%
  pivot_wider(names_from=`Operating Year`,values_from=cum_cap) %>%
  mutate(growth_14_24 = round(`2024` - `2014`,1),
         growth_21_24 = round((`2024`-`2021`),1))


#Electricity Consumption------------------------

# Define the URL
zones <- list("US-CAR-YAD",
              "US-SW-AZPS",
              "US-MIDW-AECI",
              "US-NW-AVA",
              "US-CAL-BANC",
              "US-NW-BPAT",
              "US-CAL-CISO",
              "US-NW-TPWR",
              "US-FLA-TAL",
              "US-CAR-DUK",
              "US-FLA-FPC",
              "US-CAR-CPLE",
              "US-CAR-CPLW",
              "US-SW-EPE",
              "US-TEX-ERCO",
              "US-FLA-FMPP",
              "US-FLA-FPL",
              "US-FLA-GVL",
              "US-NW-GRID",
              "US-NW-IPCO",
              "US-CAL-IID",
              "US-NE-ISNE",
              "US-FLA-JEA",
              "US-CAL-LDWP",
              "US-MIDW-LGEE",
              "US-MIDW-MISO",
              "US-NW-NEVP",
              "US-NY-NYIS",
              "US-NW-NWMT",
              "US-MIDA-PJM",
              "US-NW-CHPD",
              "US-NW-DOPD",
              "US-NW-GCPD",
              "US-NW-PACE",
              "US-NW-PACW",
              "US-NW-PGE",
              "US-NW-PSCO",
              "US-SW-PNM",
              "US-NW-PSEI",
              "US-SW-SRP",
              "US-NW-SCL",
              "US-FLA-SEC",
              "US-CAR-SCEG",
              "US-CAR-SC",
              "US-SE-SOCO",
              "US-CENT-SWPP",
              "US-CENT-SPA",
              "US-FLA-TEC",
              "US-TEN-TVA",
              "US-SW-TEPC",
              "US-CAL-TIDC",
              "US-SW-WALC",
              "US-NW-WACM",
              "US-NW-WAUW"
)

# Initialize an empty list to store dataframes
yearly_2023_dataframes <- list()

for (i in seq_along(zones)) {
  # Read the CSV from the URL
  yearly_2023_dataframes[[i]] <- read.csv(
    paste0("https://data.electricitymaps.com/2024-01-17/", zones[i], "_2023_yearly.csv")
  )
}

monthly_2023<-read.csv("https://data.electricitymaps.com/2024-01-17/US-NW-WAUW_2023_monthly.csv")

electricity_maps <- bind_rows(yearly_2023_dataframes)


#Retail Service Territories from EIA Energy Atlas
#https://atlas.eia.gov/datasets/f4cd55044b924fed9bc8b64022966097
shapefile <- st_read("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/Electric_Retail_Service_Territories.shp")

sbrgn_sf_transformed <- st_transform(shapefile, crs = st_crs(counties))
sbrgn_sf_transformed <- st_make_valid(sbrgn_sf_transformed)
sf::sf_use_s2(FALSE)

# Perform the spatial join with countiesto get retail territories by county
matched <- st_join(sbrgn_sf_transformed, counties, join = st_intersects)
sf::sf_use_s2(TRUE)
matched<-as.data.frame(matched)
matched <- matched %>%
  select(ID,NAME.x,STATE,CNTRL_AREA,HOLDING_CO,NET_GEN,CUSTOMERS,STATEFP,COUNTYFP,GEOID) %>%
  mutate(id=as.numeric(ID))


electricity_maps_match<-electricity_maps %>%
  mutate(Zone.Name=case_when(
    Zone.Name == "Northwestern Energy" ~ "NORTHWESTERN ENERGY (NWMT)",
    Zone.Name == "Duke Energy Progress West" ~ "",
    Zone.Name == "Florida Power and Light Company" ~ "FLORIDA POWER & LIGHT COMPANY",
    Zone.Name == "Gridforce Energy Management, LLC" ~ "",
    Zone.Name == "Jacksonville Electric Authority" ~ "JEA",
    Zone.Name == "Midcontinent Independent Transmission System Operator, Inc." ~ "MIDCONTINENT INDEPENDENT TRANSMISSION SYSTEM OPERATOR, INC..",
    Zone.Name == "PUD No. 1 of Chelan County" ~ "PUBLIC UTILITY DISTRICT NO. 1 OF CHELAN COUNTY",
    Zone.Name == "PUD No. 2 of Grant County, Washington" ~ "PUBLIC UTILITY DISTRICT NO. 2 OF GRANT COUNTY, WASHINGTON",
    Zone.Name == "Pacificorp East" ~ "PACIFICORP - EAST",
    Zone.Name == "Pacificorp West" ~ "PACIFICORP - WEST",
    TRUE ~ Zone.Name)) %>%
  mutate(cntrl_area=toupper(Zone.Name)) %>%
  left_join(matched %>%
              select(GEOID,CNTRL_AREA),by=c("cntrl_area"="CNTRL_AREA")) %>%
  mutate(fips=as.numeric(GEOID)) %>%
  left_join(geo,by="fips")

results_list <- list()

for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    elec_grid <- electricity_maps_match %>%
      group_by(!!sym(geog)) %>%
      summarize(
        across(
          c(Carbon.Intensity.gCO.eq.kWh..direct., Renewable.Percentage),
          ~ weighted.mean(.x, w = gdp * percent_district / 100, na.rm = TRUE)
        ),
        .groups = "drop"
      )
  } else {
    # General case for other geographies
    elec_grid <- electricity_maps_match %>%
      group_by(!!sym(geog)) %>%
      distinct(!!sym(geog), gdp, Carbon.Intensity.gCO.eq.kWh..direct., Renewable.Percentage) %>%
      summarize(
        across(
          c(Carbon.Intensity.gCO.eq.kWh..direct., Renewable.Percentage),
          ~ weighted.mean(.x, w = gdp, na.rm = TRUE)
        ),
        .groups = "drop"
      )
  }
  
  results_list[[geog]] <- elec_grid
}

# Combine all results into one dataframe
elec_grid <- bind_rows(results_list)
elec_grid <- elec_grid %>%
  ungroup() %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(-State.Name, -cd_119, -PEA, -CBSA.Title,-GeoName) 
colnames(elec_grid)[1]<-"Electricity Consumption Carbon Intensity (CO2eq/kWh)"
colnames(elec_grid)[2]<-"Electricity Consumption Renewable Percentage"

elec_grid<-elec_grid %>%
  group_by(geo) %>%
  mutate("Grid Carbon Intensity Rank"=rank(-`Electricity Consumption Carbon Intensity (CO2eq/kWh)`),
         "Grid Renewables Percentage Rank"=rank(-`Electricity Consumption Renewable Percentage`))

#2022 Average Manufacturing Pay------------
cbp_2022_nat <- getCensus(
  name = "cbp",
  vars=c("NAICS2017",
         "PAYANN",
         "EMP"),
  region = "us:*",
  vintage = 2022)

manshare_us <- cbp_2022_nat %>%
  filter(NAICS2017 %in% c("31-33", "00")) %>%
  select(-PAYANN) %>%
  pivot_wider(names_from = NAICS2017, values_from = EMP) %>%
  mutate(man_share = round(`31-33` / `00` * 100, 1))

cbp_2022 <- getCensus(
  name = "cbp",
  vars=c("STATE",
         "NAICS2017",
         "PAYANN",
         "EMP"),
  region = "county:*",
  vintage = 2022)

manpay <- cbp_2022 %>%
  filter(NAICS2017 %in% c("31-33")) %>%
  mutate(worker_pay=PAYANN/EMP*1000,
         fips=as.numeric(paste0(state,county))) %>%
  select(fips,worker_pay,EMP) %>%
  left_join(geo,by="fips") 

results_list <- list()

for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    manpay_geo<-manpay %>%
      group_by(!!sym(geog)) %>%
      summarize(
        man_pay = weighted.mean(worker_pay,w=EMP * percent_district / 100, na.rm = TRUE),
        .groups = "drop"
      )
  } else {
    # General case for other geographies
    manpay_geo <- manpay %>%
      group_by(!!sym(geog)) %>%
      distinct(!!sym(geog),worker_pay,EMP) %>%
      summarize(
        man_pay = weighted.mean(worker_pay,w=EMP, na.rm = TRUE),
        .groups = "drop"
      )
  }
  
  results_list[[geog]] <- manpay_geo
}

# Combine all results into one dataframe
manpay_geo <- bind_rows(
  lapply(names(results_list), function(name) {
    results_list[[name]] %>%
      mutate(geo = name)  # Add a column to indicate the geography
  })
)

manpay_geo<-manpay_geo %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(geo,geo_name,man_pay) %>%
  group_by(geo) %>%
  mutate(Manpay_rank=rank(-man_pay))


##2022 Average Manufacturing Share of Employment

cbp_2022<-cbp_2022 %>%
  mutate(fips=as.numeric(paste0(state,county))) %>%
  left_join(geo,by="fips")
results_list <- list()

for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    manshare <- cbp_2022 %>%
      filter(NAICS2017 %in% c("31-33", "00")) %>%
      group_by(!!sym(geog), NAICS2017) %>%
      summarize(
        EMP = sum(EMP * percent_district / 100, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      pivot_wider(names_from = NAICS2017, values_from = EMP) %>%
      mutate(man_share = round(`31-33` / `00` * 100, 1))
  } else {
    # General case for other geographies
    manshare <- cbp_2022 %>%
      filter(NAICS2017 %in% c("31-33", "00")) %>%
      distinct(!!sym(geog),NAICS2017,EMP) %>%
      group_by(!!sym(geog), NAICS2017) %>%
      summarize(
        EMP = sum(EMP, na.rm = TRUE),  # No percent_district adjustment for general case
        .groups = "drop"
      ) %>%
      pivot_wider(names_from = NAICS2017, values_from = EMP) %>%
      mutate(man_share = round(`31-33` / `00` * 100, 1))
  }
  
  results_list[[geog]] <- manshare
}

manshare_results <- bind_rows(results_list)

manshare<-manshare_results %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(geo,geo_name,man_share) %>%
  group_by(geo) %>%
  mutate(Manshare_rank=rank(-man_share)) %>%
  mutate(manufacturing_lq=round(man_share/9,1))

#County GDP by Industry-----------------------

#GDP by Industry----------------------------
url <- "https://apps.bea.gov/regional/zip/CAGDP11.zip"
temp_zip <- tempfile(fileext = ".zip")
download(url, temp_zip, mode = "wb")
temp_dir <- tempdir()
unzip(temp_zip, exdir = temp_dir)
files <- list.files(temp_dir, full.names = TRUE)

county_gdp_ind<-read.csv(files[grepl("CAGDP11__ALL_AREAS_2002_2023.csv", files)], stringsAsFactors = FALSE)

url <- "https://apps.bea.gov/regional/zip/CAGDP2.zip"
temp_zip <- tempfile(fileext = ".zip")
download(url, temp_zip, mode = "wb")
temp_dir <- tempdir()
unzip(temp_zip, exdir = temp_dir)
files <- list.files(temp_dir, full.names = TRUE)

county_gdp_ind2<-read.csv(files[grepl("CAGDP2__ALL_AREAS_2001_2023.csv", files)], stringsAsFactors = FALSE)


countygdpind<-county_gdp_ind2 %>%
  filter(LineCode %in% c("1","11","13","10","60")) %>%
  mutate(across(c(X2001:X2023), ~ as.numeric(as.character(.)))) %>%
  #pivot_longer(cols=c(X2001:X2023),names_to = "Year") %>%
  #group_by(GeoFIPS,GeoName,Region,TableName,LineCode,Description,Year) %>%
  #fill(value, .direction = "downup") %>%
  #pivot_wider(names_from=Year,values_from=value) %>%
  select(-GeoName) %>%
  mutate(fips=as.numeric(GeoFIPS)) %>%
  inner_join(geo,by="fips") 

results_list <- list()

for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    countygdp <- countygdpind %>%
      group_by(!!sym(geog), Description) %>%
      summarize(
        GDP13 = sum(X2013 * percent_district / 100, na.rm = TRUE),
        GDP18 = sum(X2018 * percent_district / 100, na.rm = TRUE),
        GDP22 = sum(X2022 * percent_district / 100, na.rm = TRUE),
        GDP23 = sum(X2023 * percent_district / 100, na.rm = TRUE),
        .groups = "drop"
      ) 
  } else {
    # General case for other geographies
    countygdp <- countygdpind %>%
      distinct(!!sym(geog),Description,X2013,X2018,X2022,X2023) %>%
      group_by(!!sym(geog), Description) %>%
      summarize(
        GDP13 = sum(X2013, na.rm = TRUE),
        GDP18 = sum(X2018, na.rm = TRUE),
        GDP22 = sum(X2022, na.rm = TRUE),
        GDP23 = sum(X2023, na.rm = TRUE),
        .groups = "drop"
      ) 
  }
  
  results_list[[geog]] <- countygdp
}

county_gdp_industry <- bind_rows(results_list)

countygdp_industry2 <- county_gdp_industry %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  mutate(
    gdp_10yr = round((GDP23 / GDP13 - 1) * 100, 1),
    gdp_5yr = round((GDP23 / GDP18 - 1) * 100, 1),
    gdp_1yr = round((GDP23 / GDP22 - 1) * 100, 1)
  ) %>%
  group_by(geo, Description) %>%
  mutate(
    total_gdp_diff_2223 = sum(GDP23 - GDP22, na.rm = TRUE),
    gdp_22_23 = ifelse(total_gdp_diff_2223 == 0, 0, round(((GDP23 - GDP22) / total_gdp_diff_2223) * 100, 1)),
    total_gdp_diff1323=sum(GDP23-GDP13),
    gdp_13_23=round(((GDP23-GDP13)/total_gdp_diff1323)*100,1)
  ) %>%
  ungroup() %>%
  select(geo, geo_name, Description, gdp_10yr, gdp_5yr, gdp_1yr, GDP23, gdp_22_23,gdp_13_23) %>%
  mutate(Description = str_trim(Description))

county_gdp_5yr<-countygdp_industry2 %>%
  select(geo, geo_name, Description, gdp_5yr) %>%
  mutate(Description = case_when(
    Description=="All industry total" ~ "total_5yrgdp",
    Description=="Durable goods manufacturing" ~ "durable_man_5yrgdp",
    Description=="Professional, scientific, and technical services" ~"prof_science_tech_5yrgdp")) %>%
  filter(Description %in% c("total_5yrgdp",
                            "durable_man_5yrgdp",
                            "prof_science_tech_5yrgdp")) %>%
  pivot_wider(names_from=Description,values_from=gdp_5yr)

county_gdp_contr<-countygdp_industry2 %>%
  select(geo, geo_name, Description, gdp_22_23) %>%
  mutate(Description = case_when(
    Description=="All industry total" ~ "total_gdp_2223_contr",
    Description=="Durable goods manufacturing" ~ "durable_man_gdp_2223_contr",
    Description=="Professional, scientific, and technical services" ~"prof_science_tech_gdp_2223_contr")) %>%
  filter(Description %in% c("total_gdp_2223_contr",
                            "durable_man_gdp_2223_contr",
                            "prof_science_tech_gdp_2223_contr")) %>%
  pivot_wider(names_from=Description,values_from=gdp_22_23)

county_gdp_10yr<-countygdp_industry2 %>%
  select(geo, geo_name, Description, gdp_10yr) %>%
  mutate(Description = case_when(
    Description=="All industry total" ~ "total_gdp_10yr")) %>%
  filter(Description %in% c("total_gdp_10yr")) %>%
  pivot_wider(names_from=Description,values_from=gdp_10yr)

county_gdp_1yr<-countygdp_industry2 %>%
  select(geo, geo_name, Description, gdp_1yr) %>%
  mutate(Description = case_when(
    Description=="All industry total" ~ "total_gdp_1yr")) %>%
  filter(Description %in% c("total_gdp_1yr")) %>%
  pivot_wider(names_from=Description,values_from=gdp_1yr)

county_gdp_ind<-left_join(county_gdp_10yr,county_gdp_1yr,by=c("geo","geo_name"))
county_gdp_ind<-left_join(county_gdp_ind,county_gdp_5yr,by=c("geo","geo_name"))
county_gdp_ind<-left_join(county_gdp_ind,county_gdp_contr,by=c("geo","geo_name"))


#Vitality Stats----------
acs_5yr_23<- getCensus(
  name = "acs/acs5",
  vars = c("B19013_001E",
           "B17020_001E",
           "B99172_001E",
           "C18120_003E",
           "C18120_002E",
           "B23025_001E",
           "B25004_001E",
           "B01003_001E"),
  region = "county:*",
  vintage = 2023)

acs_5yr_23<-acs_5yr_23 %>%
  rename(med_house_inc = B19013_001E,
         pov_tot = B17020_001E,
         pov_family = B99172_001E,
         empl = C18120_003E,
         lab_force = C18120_002E,
         emp_21 = B23025_001E,
         vacancy = B25004_001E,
         pop = B01003_001E) %>%
  mutate(unemp=(1-empl/lab_force)*100) %>%
  mutate(pov_rate = (1-pov_tot/pop)*100) %>%
  mutate(emp_pop = empl/pop*100) %>%
  mutate(med_inc_perc = med_house_inc/median(med_house_inc)*100)

acs_5yr_23 <-acs_5yr_23 %>%
  mutate(fips=paste(state,county)) %>%
  mutate(geoid=gsub(" ","",fips)) %>%
  mutate(fips=as.numeric(geoid)) %>%
  left_join(geo,by="fips")


results_list <- list()

for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    vit<-acs_5yr_23 %>%
      group_by(!!sym(geog)) %>%
      summarize(across(c(med_house_inc, med_inc_perc, pov_rate, emp_pop, vacancy, unemp), 
                       weighted.mean, 
                       w = .data$pop*percent_district/100, 
                       na.rm = TRUE))
  }else {
    # General case for other geographies
    vit<-acs_5yr_23 %>%
      group_by(!!sym(geog)) %>%
      distinct(!!sym(geog),pop,med_house_inc, med_inc_perc, pov_rate, emp_pop, vacancy, unemp) %>%
      summarize(across(c(med_house_inc, med_inc_perc, pov_rate, emp_pop, vacancy, unemp), 
                       weighted.mean, 
                       w = .data$pop, 
                       na.rm = TRUE))
  }
  results_list[[geog]] <- vit
}

vit <- bind_rows(results_list)
vit <- vit %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(geo,geo_name,med_house_inc, med_inc_perc, pov_rate, emp_pop, vacancy, unemp) %>%
  group_by(geo) %>%
  mutate(med_inc_rank=rank(-med_house_inc))

#Population
results_list <- list()

for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    pop<-acs_5yr_23 %>%
      group_by(!!sym(geog)) %>%
      summarize(pop=sum(pop*percent_district/100,na.rm=T))
  }else {
    # General case for other geographies
    pop<-acs_5yr_23 %>%
      group_by(!!sym(geog)) %>%
      distinct(!!sym(geog),pop) %>%
      summarize_at(vars(pop),sum,na.rm=T)
  }
  results_list[[geog]] <- pop
}

pop <- bind_rows(results_list)
pop <- pop %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(geo,geo_name,pop) 



#Property Values----------
#Source:https://placeslab.org/fmv-usa/
county_prop <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/county_property_values.csv")

county_prop <-county_prop %>%
  select(-NAME) %>%
  left_join(geo,by=c("GEOID"="fips")) %>%
  left_join(counties %>% 
              select(GEOID,ALAND) %>%
              mutate(fips=as.numeric(GEOID)),by=c("GEOID"="fips"))


results_list <- list()

for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    prop<-county_prop %>%
      group_by(!!sym(geog)) %>%
      summarize(across(c(PropertyValueUSD), 
                       weighted.mean, 
                       w = .data$ALAND*percent_district/100, 
                       na.rm = TRUE))
  }else {
    # General case for other geographies
    prop<-county_prop %>%
      group_by(!!sym(geog)) %>%
      distinct(!!sym(geog),PropertyValueUSD,ALAND) %>%
      summarize(across(c(PropertyValueUSD), 
                       weighted.mean, 
                       w = .data$ALAND, 
                       na.rm = TRUE))
  }
  results_list[[geog]] <- prop
}

prop <- bind_rows(results_list)


prop <- prop %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(geo,geo_name,PropertyValueUSD) %>%
  group_by(geo) %>%
  mutate(PropertyValue_Rank=rank(-PropertyValueUSD))


#Renewable Generation Potential-----------------------------
tech_pot_county <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/techpot_baseline_county.csv")
tech_pot_county <- tech_pot_county %>%
  mutate(
    Geoid = as.numeric(paste0(substr(Geography.ID, 2, 3), substr(Geography.ID, 5, 7))),
    tech_gen = Technical.Generation.Potential...MWh.MWh,
    tech = ifelse(
      Technology == "land_based_wind", "Wind Potential (MWh)",
      ifelse(
        Technology == "utility_pv", "Utility Solar Potential (MWh)",
        ifelse(
          Technology %in% c("residential_pv", "commercial_pv"), "Rooftop Solar Potential (MWh)",
          NA_character_ # Specify what to return if no conditions match
        )
      )
    )
  ) %>%
  group_by(Geoid, tech) %>%
  summarize_at(vars(tech_gen), sum, na.rm = TRUE) %>%
  ungroup() %>%
  filter(!is.na(tech)) %>%
  left_join(geo,by=c("Geoid"="fips"))


results_list <- list()

for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    tech_pot<-tech_pot_county %>%
      group_by(!!sym(geog),tech) %>%
      summarize(
        tech_pot = sum(tech_gen * percent_district / 100, na.rm = TRUE),
        .groups = "drop"
      )
  } else {
    # General case for other geographies
    tech_pot<-tech_pot_county %>%
      group_by(!!sym(geog),tech) %>%
      distinct(!!sym(geog),tech,tech_gen) %>%
      summarize(
        tech_pot = sum(tech_gen, na.rm = TRUE),
        .groups = "drop"
      )
  }
  
  results_list[[geog]] <- tech_pot
}

# Combine all results into one dataframe
tech_pot_geo <- bind_rows(results_list)
library(dplyr)
library(tidyr)

tech_pot_geo <- tech_pot_geo %>%
  pivot_wider(
    names_from = tech,
    values_from = tech_pot,
    values_fn = list(tech_pot = sum) # Summarize duplicates
  ) %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(
    geo, geo_name,
    `Rooftop Solar Potential (MWh)`,
    `Utility Solar Potential (MWh)`,
    `Wind Potential (MWh)`
  ) %>%
  group_by(geo) %>%
  mutate(
    Solar_potential_rank = rank(-as.numeric(`Utility Solar Potential (MWh)`), ties.method = "min"),
    Wind_potential_rank = rank(-as.numeric(`Wind Potential (MWh)`), ties.method = "min"),
    Rooftop_solar_potential_rank = rank(-as.numeric(`Rooftop Solar Potential (MWh)`), ties.method = "min")
  )

#Renewable Supply Curves----------------------------
solar_county <- read.csv(paste0(raw_data,"solar_lcoe_county.csv"))
wind_county <- read.csv(paste0(raw_data,"wind_lcoe_county.csv"))
geothermal_county <- read.csv(paste0(raw_data,"geothermal_county.csv"))

renewable_supplycurve <- solar_county %>%
  select(-X) %>%
  rename(solar_mean_lcoe=mean_lcoe,
         solar_total_lcoe=total_lcoe,
         solar_capacity_mw_dc=capacity_mw_dc) %>%
  left_join(wind_county %>% 
              select(-X) %>%
              rename(wind_mean_lcoe=mean_lcoe,
                     wind_total_lcoe=total_lcoe,
                     wind_capacity_mw=capacity_mw),by=c("state","county","cnty_fips")) %>%
  left_join(geothermal_county %>% 
              select(-X) %>%
              rename(geothermal_capacity_MW=capacity_MW,
                     geothermal_resource_lcoe=resource_lcoe,
                     geothermal_total_lcoe=total_lcoe), by=c("cnty_fips"="GEOID"))  %>%
  left_join(geo,by=c("cnty_fips"="fips"))


results_list <- list()

for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    supplycurve<-renewable_supplycurve %>%
      group_by(!!sym(geog)) %>%
      summarize(
        solar_totallcoe = weighted.mean(solar_total_lcoe,w= solar_capacity_mw_dc*percent_district / 100, na.rm = TRUE),
        wind_totallcoe = weighted.mean(wind_total_lcoe,w= wind_capacity_mw*percent_district / 100, na.rm = TRUE),
        geothermal_totallcoe = weighted.mean(geothermal_total_lcoe,w= geothermal_capacity_MW*percent_district / 100, na.rm = TRUE),
        solar_capacitymw_dc=sum(solar_capacity_mw_dc*percent_district/100,na.rm=T),
        wind_capacitymw=sum(wind_capacity_mw*percent_district/100,na.rm=T),
        geothermal_capacityMW=sum(geothermal_capacity_MW*percent_district/100,na.rm=T),
        .groups = "drop"
      )
  } else {
    # General case for other geographies
    supplycurve<-renewable_supplycurve %>%
      group_by(!!sym(geog)) %>%
      distinct(!!sym(geog),solar_total_lcoe,wind_total_lcoe,geothermal_total_lcoe,solar_capacity_mw_dc,wind_capacity_mw,geothermal_capacity_MW) %>%
      summarize(
        solar_totallcoe = weighted.mean(solar_total_lcoe,w= solar_capacity_mw_dc, na.rm = TRUE),
        wind_totallcoe = weighted.mean(wind_total_lcoe,w= wind_capacity_mw, na.rm = TRUE),
        geothermal_totallcoe = weighted.mean(geothermal_total_lcoe,w= geothermal_capacity_MW, na.rm = TRUE),
        solar_capacitymw_dc=sum(solar_capacity_mw_dc,na.rm=T),
        wind_capacitymw=sum(wind_capacity_mw,na.rm=T),
        geothermal_capacityMW=sum(geothermal_capacity_MW,na.rm=T),
        .groups = "drop"
      )
  }
  
  results_list[[geog]] <- supplycurve
}

# Combine all results into one dataframe
supplycurve_geo <- bind_rows(results_list)

supplycurve_geo <- supplycurve_geo %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(geo,geo_name,solar_totallcoe:geothermal_capacityMW) %>%
  group_by(geo) %>%
  mutate(
    Solar_potential_rank = rank(-as.numeric(solar_capacitymw_dc), ties.method = "min"),
    Wind_potential_rank = rank(-as.numeric(wind_capacitymw), ties.method = "min"),
    Geothermal_potential_rank = rank(-as.numeric(geothermal_capacityMW), ties.method = "min"),
    Solar_cost_rank = rank(as.numeric(solar_totallcoe), ties.method = "min"),
    Wind_cost_rank = rank(as.numeric(wind_totallcoe), ties.method = "min"),
    Geothermal_cost_rank = rank(as.numeric(geothermal_totallcoe), ties.method = "min"),
  )

write.csv(supplycurve_geo,paste0(raw_data,"supplycurve_geo.csv"))

#Federal Grants-----------------------------------------

ira_bil <- read_excel(paste0(raw_data,"Investment Data - SHARED.xlsx"),sheet=3)

# Convert ira_bil into an sf object using the Longitude and Latitude columns.
ira_bil_sf <- st_as_sf(ira_bil, coords = c("Longitude", "Latitude"), crs = 4326)

# Ensure that the counties dataset has the same CRS. If not, transform ira_bil_sf:
ira_bil_sf <- st_transform(ira_bil_sf, crs = st_crs(counties))

# Perform the spatial join. This will join the county attributes (including GEOID)
# to each point in ira_bil_sf based on spatial intersection.
ira_bil_joined <- st_join(ira_bil_sf, counties[, c("GEOID")], join = st_intersects)

fed_inv_county <- ira_bil_joined %>% 
  as.data.frame() %>%
  select(-geometry) %>%
  mutate(`Federal Funding` = as.numeric(`Funding Amount`),
         district_id = as.numeric(`District ID`),
         fips=as.numeric(GEOID)) %>%
  group_by(district_id, fips, `Funding Source`) %>%
  summarize(federal_funds = sum(`Federal Funding`, na.rm = TRUE), .groups = "drop") %>%
  left_join(geo %>%
              select(-percent_district) %>%
              distinct(),
            by = c("district_id" = "GEOID_2", "fips" = "fips"))

results_list <- list()

for (geog in geographies) {
  fed_inv<-fed_inv_county %>%
    group_by(!!sym(geog),`Funding Source`) %>%
    distinct(!!sym(geog),`Funding Source`,federal_funds) %>%
    summarize(
      federal_funds = sum(federal_funds, na.rm = TRUE),
      .groups = "drop"
    )
  
  results_list[[geog]] <- fed_inv
}
fed_inv_geo<-bind_rows(results_list)
fed_inv_geo<-fed_inv_geo %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(
    geo, geo_name,
    `Funding Source`,federal_funds
  ) %>%
  pivot_wider(names_from=`Funding Source`,values_from=federal_funds) %>%
  rename("BIL_grants"="BIL",
         "IRA_grants"="IRA") %>%
  rowwise() %>%
  mutate(total_bil_ira_grants = sum(c_across(`BIL_grants`:`BIL/IRA`), na.rm = TRUE)) %>%
  ungroup()

#Life Expectancy---------
life_19<-read.csv("OneDrive - RMI/Documents/Data/US Maps etc/Econ/Life Expectancy/IHME_county_life_expectancy/IHME_USA_LE_COUNTY_RACE_ETHN_2000_2019_LT_2019_BOTH_Y2022M06D16.csv")

life_19<-life_19 %>%
  filter(age_name=="<1 year" & race_name=="Total") %>%
  left_join(acs_5yr_22,by=c("fips"="fips"))

results_list <- list()

for (geog in geographies) {
  life_geo<-life_19 %>%
    group_by(!!sym(geog)) %>%
    summarize(across(c(val), 
                     weighted.mean, 
                     w = .data$pop, 
                     na.rm = TRUE)) %>%
    mutate(life_expectancy=as.numeric(val)) %>%
    select(!!sym(geog),life_expectancy)
  
  results_list[[geog]] <- life_geo
  
}
life_geo <- bind_rows(results_list)
life_geo<-life_geo %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(
    geo, geo_name,
    life_expectancy
  )



#Tax Credits!-------------------------------
#Federal Tax Credit Incentives State-Level Estimates
tax_inv_cat<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q4_2024/federal_actual_investment_by_category.csv',skip=5)
tax_inv_cat_tot <- tax_inv_cat%>% group_by(Segment,Category) %>%
  summarize_at(vars(Total.Federal.Investment),sum,na.rm=T)
tax_inv_state<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q4_2024/federal_actual_investment_by_state.csv',skip=5)
tax_inv_state_tot <- tax_inv_state %>% group_by(State) %>%
  summarize_at(vars(Total.Federal.Investment),sum,na.rm=T) 


# Loop through each geography
results_list <- list()

for (geog in geographies) {
  # Print geog for debugging
  print(paste("Processing:", geog))
  
  # General case for other geographies
  fac_45x <- facilities_clean %>%
    filter(
      Decarb_Sector == "Clean Tech Manufacturing",
      Technology %in% c("Solar", "Wind", "Critical Minerals", "Batteries"),
      Investment_Status == "O",
      State.Name != "",
      !is.na(State.Name),
      !is.na(PEA),
      !is.na(CBSA.Title),
      !is.na(cd_119), !is.na(GeoName),
      Current_Facility_Status != "C"
    ) %>%
    distinct(!!sym(geog), unique_id,Segment,Estimated_Total_Facility_CAPEX) %>%
    group_by(!!sym(geog), Segment) %>%
    summarize(
      Estimated_Total_Facility_CAPEX=sum(Estimated_Total_Facility_CAPEX,na.rm=T),
      .groups = "drop") %>%
    group_by(Segment) %>%
    mutate(cap_share = Estimated_Total_Facility_CAPEX / sum(Estimated_Total_Facility_CAPEX)) %>%
    left_join(
      tax_inv_cat_tot %>%
        filter(Category == "Advanced Manufacturing Tax Credits"),
      by = c("Segment")
    ) %>%
    mutate(local_45x = Total.Federal.Investment * cap_share)  
  gdp_geo <- geo %>%
    group_by(!!sym(geog)) %>%
    distinct(!!sym(geog),gdp) %>%
    summarize(
      gdp = sum(gdp, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Append the result to the list
  results_list[[geog]] <- fac_45x
}

geo_45X<-bind_rows(results_list)
geo_45X<-geo_45X %>%
  ungroup() %>%
  mutate(
    geo = case_when(
      !is.na(State.Name) ~ "State",
      !is.na(cd_119) ~ "Congressional District",
      !is.na(PEA) ~ "Economic Area",
      !is.na(GeoName) ~ "County",
      TRUE ~ "Metro Area"
    ),
    geo_name = case_when(
      !is.na(State.Name) ~ State.Name,
      !is.na(cd_119) ~ cd_119,
      !is.na(PEA) ~ PEA,
      !is.na(GeoName) ~ GeoName,
      TRUE ~ CBSA.Title
    )
  ) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(
    geo, geo_name,local_45x
  )


#45V & 45Q
results_list <- list()

for (geog in geographies) {
  
  fac_45vq <- facilities_clean %>%
    filter(
      Segment=="Energy and Industry",
      Technology %in% c("Hydrogen","Carbon Management","Cement","Iron & Steel","Pulp & Paper"),
      Investment_Status == "O",
      State.Name != "",
      !is.na(State.Name),
      !is.na(PEA),
      !is.na(CBSA.Title),
      !is.na(cd_119), !is.na(GeoName),
    ) %>%
    distinct(!!sym(geog), unique_id,Segment,Estimated_Total_Facility_CAPEX) %>%
    group_by(!!sym(geog), Segment) %>%
    summarize(
      Estimated_Total_Facility_CAPEX=sum(Estimated_Total_Facility_CAPEX,na.rm=T),
      .groups = "drop") %>%
    group_by(Segment) %>%
    mutate(cap_share = Estimated_Total_Facility_CAPEX / sum(Estimated_Total_Facility_CAPEX)) %>%
    left_join(
      tax_inv_cat_tot %>%
        filter(Category == "Emerging Climate Technology Tax Credits"),
      by = c("Segment")
    ) %>%
    mutate(local_45vq = Total.Federal.Investment * cap_share)
  
  # Append the result to the list
  results_list[[geog]] <- fac_45vq
}

geo_45vq<-bind_rows(results_list)
geo_45vq<-geo_45vq %>%
  ungroup() %>%
  mutate(
    geo = case_when(
      !is.na(State.Name) ~ "State",
      !is.na(cd_119) ~ "Congressional District",
      !is.na(PEA) ~ "Economic Area",
      !is.na(GeoName) ~ "County",
      TRUE ~ "Metro Area"
    ),
    geo_name = case_when(
      !is.na(State.Name) ~ State.Name,
      !is.na(cd_119) ~ cd_119,
      !is.na(PEA) ~ PEA,
      !is.na(GeoName) ~ GeoName,
      TRUE ~ CBSA.Title
    )
  ) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(
    geo, geo_name,local_45vq
  )


#45Z & 40B
results_list <- list()

for (geog in geographies) {
  
  fac_45zb <- facilities_clean %>%
    filter(
      Segment=="Energy and Industry",
      Technology %in% c("Sustainable Aviation Fuels","Clean Fuels"),
      Investment_Status == "O",
      State.Name != "",
      !is.na(State.Name),
      !is.na(PEA),
      !is.na(CBSA.Title),
      !is.na(cd_119), !is.na(GeoName),
    ) %>%
    distinct(!!sym(geog), unique_id,Segment,Estimated_Total_Facility_CAPEX) %>%
    group_by(!!sym(geog), Segment) %>%
    summarize(
      Estimated_Total_Facility_CAPEX=sum(Estimated_Total_Facility_CAPEX,na.rm=T),
      .groups = "drop") %>%
    group_by(Segment) %>%
    mutate(cap_share = Estimated_Total_Facility_CAPEX / sum(Estimated_Total_Facility_CAPEX)) %>%
    left_join(
      tax_inv_cat_tot %>%
        filter(Category == "Emerging Climate Technology Tax Credits"),
      by = c("Segment")
    ) %>%
    mutate(local_45z_40b = Total.Federal.Investment * cap_share)
  
  # Append the result to the list
  results_list[[geog]] <- fac_45zb
}

fac_45zb<-bind_rows(results_list)
fac_45zb<-fac_45zb %>%
  ungroup() %>%
  mutate(
    geo = case_when(
      !is.na(State.Name) ~ "State",
      !is.na(cd_119) ~ "Congressional District",
      !is.na(PEA) ~ "Economic Area",
      !is.na(GeoName) ~ "County",
      TRUE ~ "Metro Area"
    ),
    geo_name = case_when(
      !is.na(State.Name) ~ State.Name,
      !is.na(cd_119) ~ cd_119,
      !is.na(PEA) ~ PEA,
      !is.na(GeoName) ~ GeoName,
      TRUE ~ CBSA.Title
    )
  ) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(
    geo, geo_name,local_45z_40b
  )


#45
results_list <- list()

for (geog in geographies) {
  # General case for other geographies
  fac_45 <- facilities_clean %>%
    filter(
      Decarb_Sector=="Power",
      !Technology %in% c("Other","Nuclear"),
      Investment_Status == "O",
      State.Name != "",
      !is.na(State.Name),
      !is.na(PEA),
      !is.na(CBSA.Title),
      !is.na(cd_119), !is.na(GeoName),
      Production_Date>as.Date("2022-08-15")
    ) %>%
    distinct(!!sym(geog), unique_id,Segment,Estimated_Total_Facility_CAPEX) %>%
    group_by(!!sym(geog), Segment) %>%
    summarize(
      Estimated_Total_Facility_CAPEX=sum(Estimated_Total_Facility_CAPEX,na.rm=T),
      .groups = "drop") %>%
    group_by(Segment) %>%
    mutate(cap_share = Estimated_Total_Facility_CAPEX / sum(Estimated_Total_Facility_CAPEX)) %>%
    left_join(
      tax_inv_cat_tot %>%
        filter(Category == "Clean Electricity Tax Credits"),
      by = c("Segment")
    ) %>%
    mutate(local_45 = Total.Federal.Investment * cap_share)
  
  # Append the result to the list
  results_list[[geog]] <- fac_45
}

geo_45<-bind_rows(results_list)

geo_45<-geo_45 %>%
  ungroup() %>%
  mutate(
    geo = case_when(
      !is.na(State.Name) ~ "State",
      !is.na(cd_119) ~ "Congressional District",
      !is.na(PEA) ~ "Economic Area",
      !is.na(GeoName) ~ "County",
      TRUE ~ "Metro Area"
    ),
    geo_name = case_when(
      !is.na(State.Name) ~ State.Name,
      !is.na(cd_119) ~ cd_119,
      !is.na(PEA) ~ PEA,
      !is.na(GeoName) ~ GeoName,
      TRUE ~ CBSA.Title
    )
  ) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(
    geo, geo_name,local_45
  )

#30D

pop <- pop %>%
  select(geo,geo_name,pop) %>%
  left_join(geo_long, by = "geo_name") %>%
  left_join(census_divisions,by=c("state_abbr"="State.Code"))



fac_30d <- investment %>%
  mutate(quarter = as.yearqtr(quarter, format = "%Y-Q%q")) %>%
  filter(
    Segment=="Retail",
    Technology %in% c("Zero Emission Vehicles"),
    quarter>("2022 Q1")
  ) %>%
  group_by(State,Segment) %>%
  summarize_at(vars(Estimated_Actual_Quarterly_Expenditure), sum, na.rm = TRUE) %>%
  group_by(Segment) %>%
  mutate(cap_share = Estimated_Actual_Quarterly_Expenditure / sum(Estimated_Actual_Quarterly_Expenditure)) %>%
  left_join(
    tax_inv_cat_tot %>%
      filter(Category == "Zero Emission Vehicle Tax Credits"),
    by = c("Segment")
  ) %>%
  mutate(state_30d = Total.Federal.Investment * cap_share) %>%
  left_join(pop %>%
              mutate(state2=substr(geo_name, nchar(geo_name) - 1, nchar(geo_name))) %>%
              select(-State,-Region,-Division,-state_abbr) %>%
              distinct(),by=c("State"="state2")) %>%
  group_by(State) %>%
  mutate(pop_share=pop/sum(pop)) %>%
  ungroup() %>%
  mutate(local_30d=state_30d*pop_share) %>%
  select(geo:local_30d,-pop,-pop_share)


geo_credits<-geo_long_all %>%
  select(geo,geo_name) %>%
  left_join(geo_45X %>%
              ungroup() %>%
              select(geo,geo_name,local_45x),by=c("geo","geo_name")) %>%
  left_join(geo_45vq %>%
              ungroup() %>%
              select(geo,geo_name,local_45vq),by=c("geo","geo_name")) %>%
  left_join(geo_45 %>%
              ungroup() %>%
              select(geo,geo_name,local_45),by=c("geo","geo_name")) %>%
  left_join(fac_30d %>%
              ungroup(),by=c("geo","geo_name")) %>%
  ungroup() %>%
  distinct() %>%
  group_by(geo) %>%
  mutate(rank_45x=rank(-local_45x),
         rank_45vq=rank(-local_45vq),
         rank_45=rank(-local_45),
         rank_30d=rank(-local_30d))


#Politics--------------------------
cbs<-read.csv('https://raw.githubusercontent.com/cbs-news-data/election-2024-maps/refs/heads/master/output/all_counties_clean_2024.csv')
cbs<-left_join(cbs,geo,by="fips")

results_list <- list()

for (geog in geographies) {
  politics_geo<-cbs %>%
    group_by(!!sym(geog)) %>%
    summarize_at(vars(vote_Harris,totalVote),sum,na.rm=T) %>%
    mutate(demshare=vote_Harris/totalVote) %>%
    mutate(partisan = case_when(demshare<0.4 ~ 1,
                                demshare<0.45~2,
                                demshare<0.55~3,
                                demshare<0.65~4,
                                demshare>0.65~5)) %>%
    mutate(partisan = factor(partisan,
                             levels=c(1:5),
                             labels=c("Strong Republican",
                                      "Leans Republican",
                                      "Battleground",
                                      "Leans Democratic",
                                      "Strong Democratic")))
  
  results_list[[geog]] <- politics_geo
}

pres2024<-bind_rows(results_list) 
pres2024<-pres2024 %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  dplyr::select(geo,geo_name,demshare,partisan)

#StatsAmerica Innovation Intelligence -----------
statsamerica_zip_url <- "https://www.statsamerica.org/downloads/Innovation-Intelligence.zip"

# Download and Load StatsAmerica Data
temp_zip <- tempfile(fileext = ".zip")
download_success <- tryCatch({
  safe_download_subproc(statsamerica_zip_url, temp_zip, mode = "wb")
  TRUE
}, error = function(e) {
  debug_log(sprintf("Failed to download StatsAmerica zip file from %s: %s", statsamerica_zip_url, e$message), "ERROR")
  FALSE
})

index_data <- NULL
measures_data <- NULL
if(download_success && file.exists(temp_zip)) {
  debug_log("Downloaded StatsAmerica zip file.", "DEBUG")
  unzip_dir <- tempdir()
  unzip_success <- tryCatch({
    unzip(temp_zip, exdir = unzip_dir)
    TRUE
  }, error = function(e) {
    debug_log(sprintf("Failed to unzip StatsAmerica file %s: %s", temp_zip, e$message), "ERROR")
    FALSE
  })
  
  if (unzip_success) {
    index_file <- file.path(unzip_dir, "Innovation Intelligence - Index Values - States and Counties.csv")
    measures_file <- file.path(unzip_dir, "Innovation Intelligence - Measures - States and Counties.csv")
    
    if(file.exists(index_file)) {
      index_data <- safe_read_csv_subproc(index_file, show_col_types = FALSE)
      debug_log(sprintf("index_data loaded with %d rows.", nrow(index_data)), "DEBUG")
      log_glimpse(index_data, "Glimpse of index_data (StatsAmerica)")
    } else {
      debug_log(sprintf("StatsAmerica index file not found after unzipping: %s", index_file), "WARN")
    }
    
    if(file.exists(measures_file)) {
      measures_data <- safe_read_csv_subproc(measures_file, show_col_types = FALSE)
      debug_log(sprintf("measures_data loaded with %d rows.", nrow(measures_data)), "DEBUG")
      log_glimpse(measures_data, "Glimpse of measures_data (StatsAmerica)")
    } else {
      debug_log(sprintf("StatsAmerica measures file not found after unzipping: %s", measures_file), "WARN")
    }
  }
  # Clean up downloaded zip file
  unlink(temp_zip)
} else if (!download_success) {
  debug_log("StatsAmerica data skipped due to download failure.", "WARN")
}

innovation_measures <- measures_data %>%
  filter(`Summary Level` == "50") %>%
  select(geo_id, `Code Description`, `Measure Value`) %>%
  group_by(geo_id, `Code Description`) %>%
  summarise(`Measure Value` = mean(`Measure Value`, na.rm = TRUE), .groups = "drop") %>%
  pivot_wider(names_from = `Code Description`, values_from = `Measure Value`)%>%
  left_join(geo,by=c("geo_id"="fips")) 

innovation_measures2 <- index_data %>%
  select(geo_id, `description...5`, `Index Value`) %>%
  group_by(geo_id, `description...5`) %>%
  summarise(`Index Value` = mean(`Index Value`, na.rm = TRUE), .groups = "drop") %>%
  pivot_wider(names_from = `description...5`, values_from = `Index Value`)%>%
  left_join(geo,by=c("geo_id"="fips")) %>%
  filter(!is.na(GeoName))


results_list <- list()

for (geog in geographies) {
  innovation_geo<-innovation_measures %>%
    group_by(!!sym(geog)) %>%
    summarize(across(c(`Technology-Based Knowledge Occupation Clusters`,
                       `Average STEM Degree Creation (per 1,000 Population)`,
                       `Average High-Tech Industry Employment Share`,
                       `Average Annual Venture Capital (scaled by GDP)`,
                       `Average Annual Expansion Stage Venture Capital (scaled by GDP)`,
                      `Average Annual High-Tech Industry Venture Capital (scaled by GDP)`,
                      `Industry Diversity`,
                      `Industry Cluster Growth Factor`,
                      `Average Gross Domestic Product (per Worker)`,
                      `Change in Share of High-Tech Industry Employment`,
                      `Per Capita Personal Income Growth`,
                      `Income Inequality (Mean to Median Ratio)`,
                      `Government Transfers to Total Personal Income Ratio`,
                      `Average Net Migration`), 
                     weighted.mean, 
                     w = .data$gdp, 
                     na.rm = TRUE))
  
  results_list[[geog]] <- innovation_geo
  
}

innovation_geo <- bind_rows(results_list)


results_list <- list()

for (geog in geographies) {
  innovation_geo2<-innovation_measures2 %>%
    group_by(!!sym(geog)) %>%
    summarize(across(c(`Headline Innovation Index`,
                       `Educational Attainment`,
                       `Economic Well-Being`,
                       `Business Dynamics`,
                       `Traded Sector Establishment Births to Deaths Ratio`,
                       `Foreign Direct Investment Attractiveness`,
                       `Industry Performance`,
                       `Latent Innovation`,
                       `Industry Cluster Performance`,
                       `Industry Cluster Strength`), 
                     weighted.mean, 
                     w = .data$gdp, 
                     na.rm = TRUE))
  
  results_list[[geog]] <- innovation_geo2
  
}

innovation_geo2 <- bind_rows(results_list)


innovation_geo<-left_join(innovation_geo,innovation_geo2,by=geographies)

innovation_geo<-innovation_geo %>%
  mutate(geo = case_when(
    !is.na(State.Name) ~ "State",
    !is.na(cd_119) ~ "Congressional District",
    !is.na(PEA) ~ "Economic Area",
    !is.na(GeoName) ~"County",
    TRUE ~ "Metro Area"
  )) %>%
  mutate(geo_name = case_when(
    !is.na(State.Name) ~ State.Name,
    !is.na(cd_119) ~ cd_119,
    !is.na(PEA) ~ PEA,
    !is.na(GeoName) ~ GeoName,
    TRUE ~ CBSA.Title
  )) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") 


#Put County-level Together---------------------

all_geos <- geo_long_all %>%
  left_join(rengen,by=c("geo","geo_name")) %>%  
  left_join(facilities_all,by=c("geo","geo_name")) %>%
  ungroup() %>% 
  left_join(geo_credits,by=c("geo","geo_name")) %>%
  left_join(fed_inv_geo,by=c("geo","geo_name")) %>%
  left_join(elec_grid,by=c("geo","geo_name")) %>%
  left_join(supplycurve_geo,by=c("geo","geo_name")) %>%
  left_join(tech_pot_geo,by=c("geo","geo_name")) %>%
  left_join(county_gdp_ind,by=c("geo","geo_name")) %>%
  left_join(manpay_geo,by=c("geo","geo_name"))%>%
  left_join(manshare,by=c("geo","geo_name")) %>%
  left_join(vit,by=c("geo","geo_name")) %>%
  left_join(pop,by=c("geo","geo_name")) %>%
  left_join(prop,by=c("geo","geo_name")) %>%
  left_join(life_geo,by=c("geo","geo_name")) %>%
  left_join(pres2024,by=c("geo","geo_name")) %>%
  left_join(innovation_geo,by=c("geo","geo_name")) %>%
  distinct() %>%
  mutate(across(where(is.numeric), ~ replace_na(., 0))) %>%
  select(-state_abbr,-State,-Region,-Division)

write.csv(all_geos %>%
            filter(geo=="Congressional District") %>%
            select(-gdp) %>%
            arrange(desc(total_investment)),"OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/Congressional District Data.csv")


##STATE VARIABLES------------------------------

#Energy Policy Simulator--------------------------
eps<-read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/eps.csv")

eps_elec<-eps %>%
  filter(scenario=="BAU",
         var1=="Electricity Generation by Type" ,
         var2 %in% c("onshore wind","solar PV","hard coal","natural gas combined cycle","natural gas peaker")) %>%
  mutate(index=round(100*Value/Value[Year=="2021"],1)) %>%
  select(Division,State,var2,Year,index)

#Identify and filter for 7 largest sources of generation over the cumulative dataset


#Econ Dev Incentives----------------

gjf<- read.csv("OneDrive - RMI/Regional Investment Strategies/Great Lakes Investment Strategy/Great Lakes Overview/Econ Development/gjf_complete.csv")

gjf_statetotal_1924<-gjf %>%
  filter(Year>2019) %>%
  group_by(region,Location) %>%
  summarize_at(vars(subs_m),sum,na.rm=T) %>%
  arrange(desc(subs_m)) %>%
  ungroup() %>%
  inner_join(state_gdp, by=c("Location"="GeoName")) %>%
  mutate(incent_gdp=subs_m/X2022*100,
         incent_gdp_rank = rank(-subs_m/X2022))


#State GDP Growth------------------
#GDP by Industry
url <- "https://apps.bea.gov/regional/zip/SQGDP.zip"
temp_zip <- tempfile(fileext = ".zip")
download(url, temp_zip, mode = "wb")
temp_dir <- tempdir()
unzip(temp_zip, exdir = temp_dir)
files <- list.files(temp_dir, full.names = TRUE)

gdp_q <- read.csv(files[grepl("SQGDP1__ALL_AREAS_2005_2024.csv", files)], stringsAsFactors = FALSE)

gdp_q<-gdp_q %>%
  filter(LineCode==1) %>%
  mutate(state_gdp_1yr=round((`X2024.Q4`-`X2023.Q4`)/`X2023.Q4`*100,1),
         state_gdp_5yr=round((`X2024.Q4`-`X2019.Q4`)/`X2019.Q4`*100,1)) %>%
  arrange(desc(state_gdp_5yr)) 


#Climate Policy------------
xchange_pol_index<-read.csv("OneDrive - RMI/Documents/Data/US Maps etc/Policy/xchange_climate_policy_index.csv")
xchange_pol_index<-xchange_pol_index %>%
  select(abbr, climate_policy_index)


#State Emissions------------

url<-'https://www.eia.gov/environment/emissions/state/excel/table1.xlsx'
destination_folder<-"C:/Users/LCarey/Downloads/"
file_path <- paste0(destination_folder, "eia_ems.xlsx")
downloaded_content <- GET(url, write_disk(file_path, overwrite = TRUE))
state_ems <- read_excel(file_path, sheet = 1,skip=4)

state_ems<-state_ems %>%
  mutate(state_ems_change_1722 = round((`2022`-`2017`)/`2017`*100,3)) %>%
  select(State,`2022`,state_ems_change_1722) %>%
  rename("emissions_2022"="2022") 



#Taxes------------
corporate_tax<-read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/2024 State Corporate Income Tax Rates Brackets.csv")
corporate_tax<-corporate_tax %>%
  mutate(Rates=as.numeric(gsub("%","",Rates))) %>%
  rename("corporate_tax"="Rates") %>%
  mutate(corporate_tax=ifelse(X=="(b)",0,
                              ifelse(X=="None",0,corporate_tax))) %>%
  mutate(State=gsub("\\([a-zA-Z]\\)","",State),
         State=trimws(State)) %>%
  select(State,corporate_tax) %>%
  group_by(State) %>%
  slice_max(order_by=corporate_tax,n=1)



#Electricity Price in Industrial Sector----------

#Natural Gas Prices--------------
#https://www.eia.gov/dnav/ng/ng_pri_sum_a_epg0_pin_dmcf_m.htm
url <- 'https://www.eia.gov/dnav/ng/xls/NG_PRI_SUM_A_EPG0_PIN_DMCF_M.xls'
destination_folder<-'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/States Data/'
file_path <- paste0(destination_folder, "eia_gas.xls")
downloaded_content <- GET(url, write_disk(file_path, overwrite = TRUE))
eia_gas <- read_excel(file_path, sheet = 2,skip=2)

library(dplyr)
library(stringr)

# Clean column names
clean_colnames <- function(names) {
  names %>%
    str_remove_all(" Natural Gas Industrial Price \\(Dollars per Thousand Cubic Feet\\)") %>%
    str_trim()
}

# Apply to dataframe
colnames(eia_gas) <- c("Date", clean_colnames(colnames(eia_gas)[-1]))

eia_gas<-pivot_longer(eia_gas,cols=c(`United States`:`Wyoming`),names_to="state",values_to = "dollars_mcf")
eia_gas<-left_join(eia_gas,census_divisions,by=c("state"="State"))

eia_gas_2024<-eia_gas %>%
  mutate(year=substr(Date,1,4)) %>%
  group_by(state,year) %>%
  summarize(across(c(dollars_mcf),mean,na.rm=T)) %>%
  filter(year=="2024") %>%
  dplyr::select(-year) %>%
  rename(gas_price=dollars_mcf)

#Monthly Industrial Electricity Prices---------------
url <- 'https://www.eia.gov/electricity/data/eia861m/xls/sales_revenue.xlsx'
destination_folder<-'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/States Data/'
file_path <- paste0(destination_folder, "eia_sales.xlsx")
downloaded_content <- GET(url, write_disk(file_path, overwrite = TRUE))

#EIA Sales
eia_sales <- read_excel(file_path, sheet = 1,skip=2)

#State Totals Industrial Prices
ind_price_m <- eia_sales %>%
  mutate(ind_price_m=`Cents/kWh...16`) %>%
  select(State,Year,Month,ind_price_m)

ind_price <- ind_price_m %>%
  group_by(State,Year) %>%
  summarize(ind_price=mean(ind_price_m,na.rm=T)) %>%
  filter(Year %in% c("2014","2019","2024")) %>%
  pivot_wider(names_from=Year,values_from=ind_price) %>%
  mutate(ind_price_10yr=(`2024`/`2014`-1)*100,
         ind_price_5yr=(`2024`/`2019`-1)*100,
         ind_price_cents_kwh=`2024`) %>%
  select(State,ind_price_10yr,ind_price_5yr,ind_price_cents_kwh)

#CNBC Business rankings-----------
cnbc <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/cnbc_bus_rankings.csv")
colnames(cnbc)[1]<-"cnbc_rank"
colnames(cnbc)[2]<-"state"
cnbc<-cnbc %>%
  select(state,cnbc_rank)



##Putting State Variables Together-------------------
state_vars<- census_divisions %>%
  left_join(gjf_statetotal_1924 %>%
              select(Location,incent_gdp,incent_gdp_rank),by=c("State"="Location")) %>%
  left_join(pres2024 %>%
              filter(geo=="State") %>%
              select(-geo,-partisan)%>%
              rename("demshare_state"="demshare"),by=c("State"="geo_name")) %>%
  left_join(ind_price,by=c("State.Code"="State")) %>%
  left_join(eia_gas_2024,by=c("State"="state")) %>%
  left_join(corporate_tax,by=c("State"="State")) %>%
  left_join(state_ems,by=c("State"="State")) %>%
  left_join(xchange_pol_index,by=c("State.Code"="abbr")) %>%
  left_join(gdp_q %>%
              select(GeoName,state_gdp_1yr,state_gdp_5yr),
            by=c("State"="GeoName")) %>%
  left_join(cnbc,by=c("State"="state")) 


##Feasibility (ALL)------------------------------
cgt_county<-read.csv('C:/Users/LCarey/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/CGT_county_data/cgt_county_data_08_29_2024.csv')
county_feas<- cgt_county %>%
  left_join(geo,by=c("county"="fips"))

results_list <- list()
for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    geo_feas<- county_feas %>%
      group_by(!!sym(geog),aggregation_level,aggregation_level_desc,industry_desc,industry_code) %>%
      summarize(across(c(density,pci),
                       weighted.mean,
                       w=.data$gdp*.data$percent_district/100,
                       na.rm=T,
                       .groups = "drop"))
  } else {
    # General case for other geographies
    geo_feas<- county_feas %>%
      group_by(!!sym(geog),aggregation_level,aggregation_level_desc,industry_desc,industry_code) %>%
      distinct(!!sym(geog),aggregation_level,aggregation_level_desc,industry_desc,industry_code,density,pci,gdp) %>%
      summarize(across(c(density,pci),
                       weighted.mean,
                       w=.data$gdp,
                       na.rm=T,
                       .groups = "drop")) 
  }
  results_list[[geog]] <- geo_feas
}

# Combine all results into one dataframe
county_feas <- bind_rows(results_list)

feas <- county_feas %>%
  ungroup()%>%
  mutate(
    geo = case_when(
      !is.na(State.Name) ~ "State",
      !is.na(cd_119) ~ "Congressional District",
      !is.na(PEA) ~ "Economic Area",
      #!is.na(GeoName) ~ "County",
      TRUE ~ "Metro Area"
    ),
    geo_name = case_when(
      !is.na(State.Name) ~ State.Name,
      !is.na(cd_119) ~ cd_119,
      !is.na(PEA) ~ PEA,
      #!is.na(GeoName) ~ GeoName,
      TRUE ~ CBSA.Title
    )
  ) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(
    geo, geo_name,
    aggregation_level,aggregation_level_desc,industry_desc,industry_code,density,pci
  ) %>%
  group_by(geo,aggregation_level,industry_desc) %>%
  mutate(industry_feas_perc=percent_rank(density)) %>%
  group_by(geo,geo_name,aggregation_level) %>%
  mutate(region_feas_rank=rank(-industry_feas_perc))

write.csv(feas,'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Clean-growth-project/raw/ClimateandEconomicJusticeTool/feasibility_geo.csv')
saveRDS(feas,file="C:/Users/LCarey/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/ChatGPT/map_app/feas.rds")

#Feasibility in 'investing' sectors------------------------------------------
CIM_eco_eti<-read.csv(paste0(raw_data,"CIM_eco_eti_invest_categories.csv"))
eco_eti_2<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/eco_rmi_updated_crosswalk.csv')

CIM_naics<-left_join(CIM_eco_eti %>%
                       select(-X),eco_eti_2 %>%
                       select(-X),by=c("clean_industry","Production.Phase"))
clean_industry_naics <- read.csv(paste0(raw_data,"clean_industry_naics.csv")) %>% select(-X)

county_feas_strategic<- cgt_county %>%
  mutate(county_geoid=as.numeric(county_geoid)) %>%
  left_join(geo,by=c("county_geoid"="fips")) %>%
  inner_join(clean_industry_naics,by=c("industry_code"="X6.Digit.Code")) 

results_list <- list()
for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    geo_feas<- county_feas_strategic %>%
      group_by(!!sym(geog),clean_industry,Production.Phase) %>%
      summarize(across(c(density),
                       weighted.mean,
                       w=.data$gdp*.data$percent_district/100,
                       na.rm=T,
                       .groups = "drop"))
  } else {
    # General case for other geographies
    geo_feas<- county_feas_strategic %>%
      group_by(!!sym(geog),clean_industry,Production.Phase) %>%
      distinct(!!sym(geog),clean_industry,Production.Phase,density,pci,gdp) %>%
      summarize(across(c(density),
                       weighted.mean,
                       w=.data$gdp,
                       na.rm=T,
                       .groups = "drop")) 
  }
  results_list[[geog]] <- geo_feas
}

# Combine all results into one dataframe
county_feas_strategic <- bind_rows(results_list)

feas_strategic <- county_feas_strategic %>%
  ungroup()%>%
  mutate(
    geo = case_when(
      !is.na(State.Name) ~ "State",
      !is.na(cd_119) ~ "Congressional District",
      !is.na(PEA) ~ "Economic Area",
      !is.na(GeoName) ~ "County",
      TRUE ~ "Metro Area"
    ),
    geo_name = case_when(
      !is.na(State.Name) ~ State.Name,
      !is.na(cd_119) ~ cd_119,
      !is.na(PEA) ~ PEA,
      !is.na(GeoName) ~ GeoName,
      TRUE ~ CBSA.Title
    )
  ) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  mutate(industry=ifelse(Production.Phase=="Manufacturing",paste(clean_industry,Production.Phase),
                         ifelse(Production.Phase=="Operations",clean_industry,NA))) %>%
  filter(!is.na(industry)) %>%
  select(
    geo, geo_name,
    industry,density
  ) %>%
  pivot_wider(names_from=industry,names_prefix = "Feasibility_",values_from=density) %>%
  rowwise() %>%
  mutate(Strategic_Feasibility= mean(c_across(where(is.numeric)), na.rm = TRUE))

write.csv(feas_strategic,'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Clean-growth-project/raw/ClimateandEconomicJusticeTool/feasibility_geo_strategic.csv')

#Economic Complexity Index----------------------------------

county_eci<- cgt_county %>%
  mutate(county_geoid=as.numeric(county_geoid)) %>%
  left_join(geo,by=c("county_geoid"="fips")) 

results_list <- list()
for (geog in geographies) {
  if (geog == "cd_119") {
    # Special case for cd_119
    geo_feas<- county_eci %>%
      group_by(!!sym(geog)) %>%
      summarize(across(c(eci),
                       weighted.mean,
                       w=.data$gdp*.data$percent_district/100,
                       na.rm=T,
                       .groups = "drop"))
  } else {
    # General case for other geographies
    geo_feas<- county_eci %>%
      group_by(!!sym(geog)) %>%
      distinct(!!sym(geog),eci,gdp) %>%
      summarize(across(c(eci),
                       weighted.mean,
                       w=.data$gdp,
                       na.rm=T,
                       .groups = "drop")) 
  }
  results_list[[geog]] <- geo_feas
}

# Combine all results into one dataframe
county_eci <- bind_rows(results_list)

county_eci <- county_eci %>%
  ungroup()%>%
  mutate(
    geo = case_when(
      !is.na(State.Name) ~ "State",
      !is.na(cd_119) ~ "Congressional District",
      !is.na(PEA) ~ "Economic Area",
      !is.na(GeoName) ~ "County",
      TRUE ~ "Metro Area"
    ),
    geo_name = case_when(
      !is.na(State.Name) ~ State.Name,
      !is.na(cd_119) ~ cd_119,
      !is.na(PEA) ~ PEA,
      !is.na(GeoName) ~ GeoName,
      TRUE ~ CBSA.Title
    )
  ) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(
    geo, geo_name,
    eci
  ) %>%
  rename(economic_complexity=eci)


#Combine all data--------

# Ensure consistent column names for matching

# Join the `geo_long` dataframe with `all_geos`
all_geos <- all_geos %>%
  left_join(geo_long, by = c("geo_name")) %>%
  left_join(census_divisions,by=c("state_abbr"="State.Code"))


all_geos<-left_join(all_geos,state_vars,by=c("State","state_abbr"="State.Code","Region","Division"))

all_geos<- all_geos %>%
  left_join(county_eci,by=c("geo","geo_name")) %>%
  left_join(feas_strategic,by=c("geo","geo_name")) %>%
  mutate(geo_name=case_when(geo_name=="ND-00" ~ "ND-AL",
                            geo_name=="SD-00" ~ "SD-AL",
                            geo_name=="VT-00" ~ "VT-AL",
                            geo_name == "AK-00" ~ "AK-AL",
                            geo_name=="WY-00" ~ "WY-AL",
                            TRUE ~ geo_name)
  ) 

#Create the Indexes--------------
normalized_geos <- all_geos %>%
  filter(state_abbr != "CT") %>%
  # Replace NA and Inf values with group-wise mean
  group_by(geo) %>%
  mutate(across(
    where(is.numeric),
    ~ ifelse(is.na(.) | is.infinite(.), mean(.[!is.infinite(.)], na.rm = TRUE), .)
  )) %>%
  # Normalize within each geo group
  mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>% # Ungroup to avoid group-related conflicts
  # Calculate indices
  rowwise() %>%
  mutate(
    invest_index = 0.1*economic_complexity+
      0.05 * incent_gdp +
      0.05 * (1 - corporate_tax) +
      0.05*total_gdp_10yr+
      0.05*total_gdp_1yr+
      0.025*durable_man_5yrgdp +
      0.025*prof_science_tech_5yrgdp+
      0.1 * (1 - man_pay) +
      0.1 * (man_share) +
      0.15 * (1 - ind_price_cents_kwh) +
      0.15*(1-gas_price)+
      0.15 * (1 - PropertyValueUSD) +
      0.1 * (1 - cnbc_rank) +
      0.025 * state_gdp_5yr +
      0.025 * state_gdp_1yr+
      0.1 * inv_gdp,
    socioecon_index = 0.2 * pov_rate +
      0.2 * (1 - med_house_inc) +
      0.2 * (1 - emp_pop) +
      0.3 * (1 - life_expectancy) +
      0.1 * (1 - vacancy),
    energy_clim_index = 0.2*Strategic_Feasibility +
      0.1 * climate_policy_index +
      0.1 * inv_gdp +
      0.1 * `Electricity Consumption Carbon Intensity (CO2eq/kWh)` +
      0.1 * `Electricity Consumption Renewable Percentage` +
      0.5 * (1-Solar_cost_rank)+
      0.5 * (1-Wind_cost_rank)+
      0.5 * (1-Geothermal_cost_rank)+
      0.1 * inv_gdp +
      0.1 * clean_share +
      0.1 * growth_19_24_Clean +
      0.1 * state_ems_change_1722
  ) %>%
  ungroup() %>% # Ungroup after rowwise operations
  distinct()

index <- normalized_geos %>%
  select(geo, geo_name, socioecon_index, invest_index, energy_clim_index) %>%
  # Normalize final indices within each geo group
  group_by(geo) %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE))
  )) %>%
  ungroup()

write.csv(index %>%
            filter(geo=="Congressional District"),"Downloads/index.csv")

#join indexes to msa data and upload-----------------------
all_geo_data<-all_geos %>%
  left_join(index,by=c("geo","geo_name")) %>%
  distinct() 

saveRDS(all_geo_data,file="C:/Users/LCarey/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/ChatGPT/map_app/all_geo_data.rds")
all_geos<-readRDS("C:/Users/LCarey/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/ChatGPT/map_app/all_geo_data.rds")

write.csv(all_geo_data,'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Clean-growth-project/raw/ClimateandEconomicJusticeTool/all_geo_data_complete_dataset_q4_2024.csv')
write.csv(all_geo_data %>%
            filter(geo=="County"),'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/county_data_complete_dataset.csv')
write.csv(all_geo_data,'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/all_geo_complete_dataset.csv')

write.csv(geo,'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Clean-growth-project/raw/ClimateandEconomicJusticeTool/geography_crosswalk.csv')
write.csv(geo,'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Clean-growth-project/raw/Crosswalk/regions/geography_crosswalk.csv')

#Join the indexes to the feasibility data and upload--------------
feas_all<-feas %>%
  left_join(index,by=c("geo","geo_name")) %>%
  left_join(msa_data,by=c("geo","geo_name"))

write.csv(feas_all,'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Clean-growth-project/raw/ClimateandEconomicJusticeTool/feasibility_complete_dataset_q2_2024.csv')
write.csv(feas_all,'RMI/US Program - Regional Investment Strategies/Great Lakes Investment Strategy/Reference Data/feasibility_complete_dataset.csv')

feas<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/feasibility_complete_dataset.csv')

