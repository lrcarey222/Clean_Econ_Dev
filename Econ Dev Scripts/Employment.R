#Employment Data

#This script contains:
#1. County Business Patterns Data download
#2. Fossil Fuel Employment Location Quotient Map
#3. Employment Diversity (Hachman Index) Map


# State Variable - Set this to the abbreviation of the state you want to analyze
state_abbreviation <- "WY"  # Replace with any US state abbreviation
state_name <- "New Mexico"  # Replace with the full name of any US state
region_name <- "Great Falls, MT"

great_falls<-EAs %>%
  filter(`EA Name`=="Great Falls, MT"|
           County %in% c("Judith Basin County, MT",
                         "Fergus County, MT",
                         "Lewis and Clark County, MT"))

#Make a region_id for your state/region of interest
region_id <- us_counties %>%
  filter(fips %in% great_falls$FIPS) 


#BLS QCEW Data

install.packages("blsAPI")
library(blsAPI)
install_github('mikeasilva/blsAPI')

#load in QCEW data


#New Mexico example - Annual, Statewide
state_fips<-states_simple %>%
  filter(abbr==state_abbreviation) 

years<-2014:2023

all_years_data <- list()
for (yr in years){
  NMdata <- blsQCEW('Area', year=yr, quarter='a', area=paste0(state_fips$fips,'000'))
  
  all_years_data[[yr]]<-NMdata
}

NM_combined_data <- do.call(rbind, all_years_data)

available_NMdata <- NM_combined_data %>% filter(disclosure_code != "N",
                                      own_code == 5)
USdata <- blsQCEW('Area', year=yr, quarter='a', area='US000')

#Annual, County-level

# Filter for counties in New Mexico (state abbreviation NM)
state_counties <- county_gdp %>%
  mutate(
    GeoName = str_trim(GeoName),
    state_fips = substr(GeoName, nchar(GeoName) - 1, nchar(GeoName))
  ) %>%
  filter(state_fips == "NM")  # Replace "NM" with the actual abbreviation for New Mexico if needed

# Initialize an empty list to store results for each county
all_county_data <- list()

# Loop through each county GeoFips and pull data
for (county in state_counties$GeoFips) {
  # Pull data for the county
  NMdata_county <- blsQCEW('Area', year = '2023', quarter = 'a', area = county)
  
  # Append the result to the list
  all_county_data[[county]] <- NMdata_county
}

# Combine all data frames into one
NM_combined_data <- do.call(rbind, all_county_data)



#County Business Patterns-------------------------------
cbp_2022 <- getCensus(
  name = "cbp",
  vars=c("STATE",
         "COUNTY",
         "NAICS2017",
         "SECTOR",
         "SUBSECTOR",
         "INDLEVEL",
         "ESTAB",
         "EMP",
         "PAYANN"),
  region = "county:*",
  vintage = 2022)

cbp_22<-cbp_2022

#Join with State Abbreviations for state names etc
cbp_22$state<-as.numeric(cbp_22$state)
cbp_22<-left_join(states_simple,cbp_22,by=c("fips"="state"))

#join with NAICS Codes to get industry descriptions
cbp_22$naics2017<-as.numeric(cbp_22$NAICS2017)
#cbp_22 <-left_join(cbp_22,eti_long,by=c("naics2017"="6-Digit Code"))

#Six Digit Level
cbp_22_6d <- cbp_22 %>%
  filter(INDLEVEL=="6")

#Six Digit Level
cbp_22_4d <- cbp_22 %>%
  filter(INDLEVEL=="4")

#Two Digit Level
cbp22_2d <- cbp_22 %>%
  select(-fips) %>%
  mutate(state=as.numeric(STATE)) %>%
  filter(INDLEVEL=="2")  %>%
  mutate(FIPS=paste0(STATE, COUNTY)) %>%
  left_join(EAs,by=c("FIPS"="FIPS")) %>%
  left_join(naics2017 %>% mutate(naics2017=(`2017 NAICS US   Code`)) %>%
                                   select(naics2017,`2017 NAICS US Title`),by=c("NAICS2017"="naics2017")) %>%
  rename(naics_desc=`2017 NAICS US Title`)

#Filter just for region of interest
region_cbp_2d <- cbp22_2d %>%
  filter(abbr==state_abbreviation) %>%
  mutate(region_id=ifelse(fips %in% region_id$fips,1,0)) %>%
  mutate(code=ifelse(NAICS2017 %in% c("00","11","21","22","23","31-33","42","48-49","54"),NAICS2017,"Other")) %>%
  group_by(region_id,code) %>%
  summarize_at(vars(EMP),sum,na.rm=T) %>%
  group_by(code) %>%
  mutate(share=EMP/sum(EMP,na.rm=T)) 

#Total Employment in Region
region_totalemp<-region_cbp_2d %>%
  filter(code=="00",
         region_id==1) 

#Calculate proportions
total_emp <- cbp22_2d %>%
  filter(NAICS2017=="00")
total_emp_nat<-cbp22_2d  %>%
  filter(NAICS2017=="00") %>%
  summarize_at(vars(EMP),sum,na.rm=T) %>%
  ungroup() 

total_emp_region <- total_emp %>%
  filter(FIPS %in% region_id$fips) %>%
  summarize_at(vars(EMP),sum,na.rm=T) %>%
  ungroup()

#Fossil Fuel Employment--------------------------
fossil_codes <- tibble(
  NAICS_code = c(211, 2121, 213111, 213112, 213113, 32411, 4861, 4862,221112
                 ),
  Description = c("Oil and Gas Extraction",
                  "Coal Mining",
                  "Drilling Oil and Gas Wells",
                  "Support Activities for Oil and Gas Operations",
                  "Support Activities for Coal Mining",
                  "Petroleum Refineries",
                  "Pipeline Transportation of Crude Oil",
                  "Pipeline Transportation of Natural Gas",
                  "Fossil Fuel Electric Power Generation"))

fossil_emp_national <- cbp_22 %>%
  mutate(fossil = ifelse(NAICS2017 %in% fossil_codes$NAICS_code,1,0)) %>%
  group_by(fossil) %>%
  summarize_at(vars(EMP),sum,na.rm=T) %>%
  mutate(EMP_nat=total_emp_nat$EMP)%>%
  ungroup() %>%
  mutate(emp_share_national = EMP / EMP_nat)

fossil_emp_county <- cbp_22 %>%
  mutate(fossil = ifelse(NAICS2017 %in% fossil_codes$NAICS_code,1,0)) %>%
  filter(fossil==1) %>%
  group_by(abbr,full,STATE,COUNTY,fossil) %>%
  summarize_at(vars(EMP),sum,na.rm=T) %>%
  left_join(total_emp %>% select(STATE,COUNTY,EMP), by = c("STATE"="STATE","COUNTY"="COUNTY"), suffix = c("", "_total")) %>%
  select(abbr,full,STATE,COUNTY,EMP,EMP_total,fossil) %>%
  ungroup() %>%
  mutate(emp_share = EMP / EMP_total) %>%
  left_join(fossil_emp_national, by = c("fossil" = "fossil")) %>%
  mutate(lq=emp_share/emp_share_national)

fossil_emp_map <- fossil_emp_county %>%
  filter(full==state_name) %>%
  mutate(FIPS=paste0(STATE,COUNTY))%>%
  select(full,FIPS,fossil,lq) %>%
  pivot_wider(names_from=fossil,values_from=lq) %>%
  mutate(region_id=ifelse(FIPS %in% region_counties$fips,1,1),
         lq = ifelse(is.na(`1`),0,`1`)) %>%
  select(full,FIPS,region_id,lq) 

#Fossil Fuel Employment-Map
us_counties<-us_map("counties")
fossil_map_data <- left_join(us_counties %>%
                               filter(full==state_name), fossil_emp_map, by = c("fips" = "FIPS"))
fossil_map_data <- fossil_map_data %>%
  mutate(across(where(is.numeric), ~ifelse(is.na(.), 0, .))) %>%
  mutate(lq_bin=cut(lq, breaks = c(-1, 1, 5, 20, 100), labels = c("Below Average (0-1)","Above Average (1-5)", "High (5-20)", "Very High (20+)"))) 

county_labels<-centroid_labels(regions = c("counties"))
county_labels <- county_labels %>%
  inner_join(fossil_emp_map,by=c("fips"="FIPS")) 
#%>%  mutate(NAME=ifelse(region_id==1,NAME,""))

library(ggrepel)
fossil_lq_map<-ggplot() +
  geom_polygon(data = fossil_map_data, aes(x = x, y = y, group = group, fill = lq_bin, alpha = region_id), color = "darkgrey") +
  geom_text_repel(data = county_labels, aes(x = x, y = y, label = county), size = 2, color = "black", fontface = "bold") +
  scale_fill_manual(values=rmi_palette, name = "Specialization") +
  scale_alpha_identity() +
  labs(title = paste("Fossil Fuel Specialization in ", state_name), 
       subtitle = "Fossil employment share by county, relative to the national average.",
       fill = "Location Quotient",
       caption = "Source: Census Bureau, County Business Patterns") +
  theme_void() +
  theme(legend.position = "right",
        plot.background = element_rect(fill = "white", color = "white"),
        panel.background = element_rect(fill = "white", color = "white"))

ggsave(file.path(output_folder, paste0(state_abbreviation,"_fossil_lq_map", ".png")), 
       plot = fossil_lq_map,
       width = 8,   # Width of the plot in inches
       height = 8,   # Height of the plot in inches
       dpi = 300)

#Bar Charts
fossil_emp_state <- fossil_emp_county %>%
  filter(full==state_name) %>%
  mutate(FIPS=paste0(STATE,COUNTY)) %>%
  left_join(county_labels,by=c("FIPS"="fips")) 


#Diversity------------------------
emp_proportions <- cbp22_2d %>%
  left_join(total_emp %>% select(STATE,COUNTY,full,EMP), by = c("full"="full","STATE"="STATE","COUNTY"="COUNTY"), suffix = c("", "_total")) %>%
  mutate(FIPS=paste0(STATE,COUNTY))%>%
  select(FIPS,full,SECTOR,EMP,EMP_total) %>%
  group_by(FIPS,full,SECTOR) %>%
  filter(SECTOR != "00") %>%
  mutate(emp_share=EMP/EMP_total) %>%
  filter(!is.na(full)) %>%
  arrange(desc(emp_share)) %>%
  distinct()

# National-level proportions
national_proportions <- emp_proportions %>%
  ungroup() %>%
  select(SECTOR, EMP) %>%
  filter(SECTOR != "00") %>%
  group_by(SECTOR) %>%
  summarize_at(vars(EMP),sum,na.rm=T) %>%
  ungroup()%>%
  mutate(emp_share_national = EMP / sum(EMP, na.rm = TRUE))

#Location Quotients
location_quotients_county <- emp_proportions %>%
  left_join(national_proportions, by = c("SECTOR")) %>%
  filter(SECTOR != "00") %>%
  mutate(LQ = emp_share / emp_share_national,
         weighted_LQ = LQ * emp_share)  # Weighting by regional share

#Region Location Quotients
location_quotients_region <- cbp_22 %>%
  mutate(FIPS=paste0(STATE,COUNTY))%>%
  filter(SECTOR != "00",
         FIPS %in% region_id$fips,
         INDLEVEL=="3") %>%
  group_by(NAICS2017) %>%
  summarize_at(vars(EMP),sum,na.rm=T) %>%
  mutate(emp_total=total_emp_region$EMP,
         emp_share=EMP/emp_total) %>%
  left_join(cbp_22 %>%
              filter(SECTOR != "00") %>%
              select(NAICS2017, EMP) %>%
              group_by(NAICS2017) %>%
              summarize_at(vars(EMP),sum,na.rm=T) %>%
              ungroup()%>%
              mutate(emp_share_national = EMP / sum(EMP, na.rm = TRUE)), by = c("NAICS2017")) %>%
  mutate(LQ = emp_share / emp_share_national) %>%
  left_join(naics2017 %>%
              select(`2017 NAICS US   Code`,
                     `2017 NAICS US Title`),by=c("NAICS2017"="2017 NAICS US   Code")) 

lq_region_clean <- location_quotients_region %>%
  mutate(naics2017=as.numeric(NAICS2017)) %>%
  inner_join(eti_long,by=c("naics2017"="3-Digit Code"))

write.csv(lq_region_clean,paste0(output_folder,"/",state_abbreviation,"_lq_region.csv"))



# National-level proportions
national_proportions <- emp_proportions %>%
  ungroup() %>%
  select(SECTOR, EMP) %>%
  filter(SECTOR != "00") %>%
  group_by(SECTOR) %>%
  summarize_at(vars(EMP),sum,na.rm=T) %>%
  ungroup()%>%
  mutate(emp_share_national = EMP / sum(EMP, na.rm = TRUE))

#Location Quotients
location_quotients_county <- emp_proportions %>%
  left_join(national_proportions, by = c("SECTOR")) %>%
  filter(SECTOR != "00") %>%
  mutate(LQ = emp_share / emp_share_national,
         weighted_LQ = LQ * emp_share)  # Weighting by regional share



# Compute the Hachman index-----------------------------
hachman_indices_county <- location_quotients_county %>%
  filter(full==state_name) %>%
  #mutate(region_id=ifelse(GEOID %in% region_counties$fips,1,0)) %>% # Identify the region of interest
  group_by(FIPS) %>%
  summarize(HI = 100 / sum(weighted_LQ, na.rm = TRUE), .groups = 'drop') %>% # Reciprocal of the sum of weighted LQs scaled by 100
  mutate(HI_bin = cut(HI, breaks = c(0, 50,60,70,80,90, 100), labels = c("Very Low (0-50)","Low (50-60)", "Low-Medium (60-70)","Medium-High (70-80)", "High (80-90)", "Very High (90-100)")))


# Plotting the state map with Hachman Index

# Get US county map data

county_map_data <- inner_join(us_counties, hachman_indices_county, by = c("fips" = "FIPS"))
county_map_data <- county_map_data %>%
  mutate(alpha = ifelse(region_id == 1, 1, 0.5))

county_labels<-centroid_labels(regions = c("counties"))
county_labels <- county_labels %>%
  inner_join(hachman_indices_county,by=c("fips"="FIPS"))

hachman_map_county_d<-ggplot() +
  geom_polygon(data = county_map_data, aes(x = x, y = y, group = group, fill = HI), color = "white") +
  geom_text(data = county_labels, aes(x = x, y = y, label = county), size = 2, color = "white", fontface = "bold") +
  scale_fill_gradient(low="#F8931D",high="#0989B1", na.value = "grey90", name = "Hachman Index") +
  scale_alpha_identity() +
  labs(title = paste("Economic Diversity within ", state_name), 
       subtitle = "A Hachman Index score ranges from 0 to 100. A higher score indicates that the subject area's industrial distribution more closely resembles that of the US as a whole, and is therefore diverse.",
       fill = "Hachman Index",
       caption = "Source: Census Bureau, County Business Patterns") +
  theme_void() +
  theme(legend.position = "right",
        plot.background = element_rect(fill = "white", color = "white"),
        panel.background = element_rect(fill = "white", color = "white"))

ggsave(file.path(output_folder, paste0(state_abbreviation,"_hachman_map_county", ".png")), 
       plot = hachman_map_county_d,
       width = 8,   # Width of the plot in inches
       height = 8,   # Height of the plot in inches
       dpi = 300)


#Quarterly Census of Employment and Wages------------------------------------

#2018:2024 data for industries in Clean Investment MOnitor
#Fastest:
cim_qcew <- read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/QCEW.csv')
tech_mapping$`6-Digit Code`<-as.character(tech_mapping$`6-Digit Code`)

# Initialize empty data frame for results
cim_qcew <- data.frame()

# Filter out any NA or empty codes in tech_mapping
industry_codes <- tech_mapping$`6-Digit Code`[!is.na(tech_mapping$`6-Digit Code`) & tech_mapping$`6-Digit Code` != ""]

# Loop through years, quarters, and industry codes
for (year in c('2018', '2019', '2020', '2021')) {
  for (quarter in c('1', '2', '3', '4')) {
    for (industry_code in industry_codes) {
      
      # Try-catch to handle potential errors
      current_data <- tryCatch({
        # Pull data for the current combination
        blsQCEW(method = 'Industry', year = year, quarter = quarter, industry = industry_code)
      }, error = function(e) {
        message("Error with year: ", year, ", quarter: ", quarter, ", industry: ", industry_code)
        return(NULL)  # Return NULL if there's an error
      })
      
      # Append data only if the request was successful
      if (!is.null(current_data)) {
        cim_qcew <- bind_rows(cim_qcew, current_data)
      }
      
      # Pause to avoid hitting rate limits
      Sys.sleep(1)
    }
  }
}

cim_qcew2<-data.frame()
# Initialize the result data frame
cim_qcew2 <- data.frame()

# Load necessary library for bind_rows
library(dplyr)

# Filter out any NA or blank codes in tech_mapping
industry_codes <- tech_mapping$`2022 NAICS Code`[!is.na(tech_mapping$`2022 NAICS Code`) & tech_mapping$`2022 NAICS Code` != ""]
industry_codes <- tech_mapping %>%
  distinct(`4-Digit Code`) %>%
  filter(!is.na(`4-Digit Code`))


# Diagnostic Loop for years 2022 and 2023 with NAICS2022 codes
for (year in c('2018','2019','2020','2021','2022','2023', '2024')) {
  
  # Determine quarters based on the year
  quarters <- if (year == '2024') c('1', '2') else c('1', '2', '3', '4')
  
  for (quarter in quarters) {
    
    for (industry_code in industry_codes$`4-Digit Code`) {
      
      # Print diagnostic message for the current iteration
      cat("Processing Year:", year, "| Quarter:", quarter, "| Industry Code:", industry_code, "\n")
      
      # Attempt to pull data with error handling
      current_data <- tryCatch({
        
        # Pull data for the current combination
        blsQCEW(method = 'Industry', year = year, quarter = quarter, industry = industry_code)
        
      }, error = function(e) {
        
        # Print error message for diagnostics
        message("Error with Year:", year, ", Quarter:", quarter, ", Industry Code:", industry_code)
        message("Error details: ", e$message)
        
        # Print traceback if available to capture additional details
        traceback()
        
        return(NULL)  # Return NULL if there's an error
      })
      
      # Check if current_data is NULL (failed API call)
      if (is.null(current_data)) {
        message("Skipped Year:", year, "| Quarter:", quarter, "| Industry Code:", industry_code, " due to error.")
      } else {
        # Append the current data to the overall data frame if successful
        cim_qcew2 <- bind_rows(cim_qcew2, current_data)
      }
      
      # Pause to avoid hitting rate limits
      Sys.sleep(1)
    }
  }
}

cim_qcew3<-data.frame()
# Total manufacturing
for (year in c('2018','2019','2020','2021','2022','2023', '2024')) {
  
  # Determine quarters based on the year
  quarters <- if (year == '2024') c('1', '2') else c('1', '2', '3', '4')
  
  for (quarter in quarters) {
    
    for (industry_code in '1013') {
      
      # Print diagnostic message for the current iteration
      cat("Processing Year:", year, "| Quarter:", quarter, "| Industry Code:", industry_code, "\n")
      
      # Attempt to pull data with error handling
      current_data <- tryCatch({
        
        # Pull data for the current combination
        blsQCEW(method = 'Industry', year = year, quarter = quarter, industry = industry_code)
        
      }, error = function(e) {
        
        # Print error message for diagnostics
        message("Error with Year:", year, ", Quarter:", quarter, ", Industry Code:", industry_code)
        message("Error details: ", e$message)
        
        # Print traceback if available to capture additional details
        traceback()
        
        return(NULL)  # Return NULL if there's an error
      })
      
      # Check if current_data is NULL (failed API call)
      if (is.null(current_data)) {
        message("Skipped Year:", year, "| Quarter:", quarter, "| Industry Code:", industry_code, " due to error.")
      } else {
        # Append the current data to the overall data frame if successful
        cim_qcew3 <- bind_rows(cim_qcew3, current_data)
      }
      
      # Pause to avoid hitting rate limits
      Sys.sleep(1)
    }
  }
}

# Print completion message
cat("Data collection loop completed.\n")

cim_qcew2<-cim_qcew2 %>% 
  select(area_fips,
         industry_code,
         year,qtr,month1_emplvl,
         month2_emplvl,
         month3_emplvl)

cim_qcew <- cim_qcew %>%
  select(area_fips,
         industry_code,
         year,qtr,month1_emplvl,
         month2_emplvl,
         month3_emplvl) %>%
  filter(year != "2023") %>%
  rbind(cim_qcew2)


# US Total Employment
cim_qcew_us <- cim_qcew2 %>%
  rbind(cim_qcew3) %>%
  filter(area_fips == "US000") %>%
  #select(3,6,7,10:12) %>%
  pivot_longer(cols = c(month1_emplvl, month2_emplvl, month3_emplvl), 
               names_to = "month",
               values_to = "emplvl") %>%
  mutate(date = as.Date(paste0(year, "-", sprintf("%02d", qtr *3-(3-as.integer(gsub("\\D", "", month)))), "-01"))) %>%
  left_join(tech_mapping %>%
              select(`4-Digit Code`,`4-Digit Description`) %>%
              mutate(`4-Digit Code`=as.numeric(`4-Digit Code`)),by=c("industry_code"="4-Digit Code")) %>%
  #left_join(tech_mapping,by=c("industry_code"="2022 NAICS Code")) %>%
  #mutate(tech=ifelse(is.na(tech.x),tech.y,tech.x),
   #      tech=ifelse(is.na(tech.y),tech.x,tech)) %>%
  group_by(date,industry_code,`4-Digit Description`) %>%
  summarize_at(vars(emplvl),sum,na.rm=T) %>%
  filter(!is.na(industry_code))

ggplot(data=cim_qcew_us,aes(x=date,y=emplvl,color=industry_code))+
  geom_line()+
  theme_minimal()

cim_qcew_us_tech <- cim_qcew_us %>%
  mutate(date = as.Date(date)) %>%
  group_by(industry_code, `4-Digit Description`) %>%
  mutate(
    emplvl_q4 = rollmean(emplvl, 4, align = 'right', fill = NA),
    ira_index = 100 * emplvl_q4 / emplvl_q4[date == "2022-08-01"],
    yoy_growth = (emplvl / lag(emplvl, n = 4) - 1) * 100,
    yoy_growth_moving_avg = rollmean(yoy_growth, 4, align = 'right', na.pad = TRUE))
      

cim_qcew_tech_growth<-cim_qcew_us_tech %>%
  filter(date>2020-21-01) %>%
  group_by(industry_code,`4-Digit Description`)

ggplot(data=cim_qcew_us_tech,aes(x=date,y=ira_q4,color=industry_code))+
  geom_line()+
  theme_minimal()

#2021-2023 Growth
qcew_us_2123 <- cim_qcew_us_tech %>%
  #select(-`EA Name`,-ira_index) %>%
  filter(date =="2021-01-01"|date =="2023-09-01") %>%
  pivot_wider(names_from=date,values_from=emplvl) %>%
  mutate(emp_2123=`2023-09-01`-`2021-01-01`,
         emp_2123_perc=emp_2123/`2021-01-01`*100) %>%
  arrange(desc(emp_2123))

pos_ira_emp <- cim_qcew_us_tech %>%
  filter(date=="2024-03-01",
         ira_index>100)
  

ira_emp_wide <- cim_qcew_us_tech%>%
  ungroup() %>%
  filter(`4-Digit Description` %in% pos_ira_emp$`4-Digit Description`|is.na(`4-Digit Description`)) %>%
  select(date,`4-Digit Description`,yoy_growth_moving_avg) %>%
  pivot_wider(names_from=`4-Digit Description`,values_from=yoy_growth_moving_avg)

write.csv(ira_emp_wide,"Downloads/ira_emp_wide.csv")


# US Average Wages
cim_qcew_us_wage <- cim_qcew %>%
  filter(area_fips == "US000") %>%
  select(3,6,7,16) %>%
  mutate(date = as.Date(paste0(year, "-", sprintf("%02d", qtr *3-2), "-01"))) %>%
  filter(qtr=="3") %>%
  left_join(tech_mapping,by=c("industry_code"="6-Digit Code")) %>%
  left_join(tech_mapping,by=c("industry_code"="2022 NAICS Code")) %>%
  mutate(Segment=ifelse(is.na(Segment.x),Segment.y,Segment.x),
         Technology=ifelse(is.na(Technology.x),Technology.y,Technology.x)) %>%
  mutate(technology=ifelse(Segment=="Manufacturing",paste0(Technology," ",Segment),Technology)) %>%
  distinct(date,technology,industry_code,avg_wkly_wage)

cim_qcew_us_tech_wage <- cim_qcew_us_wage %>%
  group_by(date,technology) %>%
  summarize_at(vars(avg_wkly_wage),mean,na.rm=T) %>%
  group_by(technology) %>%
  mutate(ira_index = 100*avg_wkly_wage/avg_wkly_wage[date=="2022-07-01"]) 

ira_wage_wide <- cim_qcew_us_tech_wage%>%
  select(date,technology,avg_wkly_wage) %>%
  pivot_wider(names_from=technology,values_from=avg_wkly_wage)

write.csv(ira_wage_wide,"C:/Users/LCarey.RMI/Downloads/ira_wage_wide.csv")


# US Establishments
cim_qcew_us_estab <- cim_qcew %>%
  filter(area_fips == "US000") %>%
  select(3,6,7,9) %>%
  mutate(date = as.Date(paste0(year, "-", sprintf("%02d", qtr *3-2), "-01"))) %>%
  left_join(tech_mapping,by=c("industry_code"="6-Digit Code")) %>%
  left_join(tech_mapping,by=c("industry_code"="2022 NAICS Code")) %>%
  mutate(Segment=ifelse(is.na(Segment.x),Segment.y,Segment.x),
         Technology=ifelse(is.na(Technology.x),Technology.y,Technology.x)) %>%
  mutate(technology=ifelse(Segment=="Manufacturing",paste0(Technology," ",Segment),Technology)) %>%
  distinct(date,technology,industry_code,qtrly_estabs)

cim_qcew_us_tech_estab <- cim_qcew_us_estab %>%
  group_by(date,technology) %>%
  summarize_at(vars(qtrly_estabs),mean,na.rm=T) %>%
  group_by(technology) %>%
  mutate(ira_index = 100*qtrly_estabs/qtrly_estabs[date=="2022-07-01"]) 

ira_qtrly_estabs_wide <- cim_qcew_us_tech_estab%>%
  select(date,technology,qtrly_estabs) %>%
  pivot_wider(names_from=technology,values_from=qtrly_estabs)

write.csv(ira_qtrly_estabs_wide,"C:/Users/LCarey.RMI/Downloads/ira_qtrly_estabs_wide.csv")


#state Clean Energy Employment
cim_qcew_state <- cim_qcew %>%
  filter(agglvl_code=="58") %>%  
  select(1,3,6,7,10:12) %>%
  pivot_longer(cols = c(month1_emplvl, month2_emplvl, month3_emplvl), 
               names_to = "month",
               values_to = "emplvl") %>%
  mutate(date = as.Date(paste0(year, "-", sprintf("%02d", qtr *3-(3-as.integer(gsub("\\D", "", month)))), "-01"))) %>%
  left_join(tech_mapping,by=c("industry_code"="6-Digit Code")) %>%
  left_join(tech_mapping,by=c("industry_code"="2022 NAICS Code")) %>%
  mutate(Segment=ifelse(is.na(Segment.x),Segment.y,Segment.x),
         Technology=ifelse(is.na(Technology.x),Technology.y,Technology.x)) %>%
  mutate(technology=ifelse(Segment=="Manufacturing",paste0(Technology," ",Segment),Technology)) %>%
  distinct(area_fips,date,technology,industry_code,emplvl) 

cim_qcew_states <-cim_qcew_state %>%
  group_by(area_fips,date,technology) %>%
  summarize_at(vars(emplvl),sum,na.rm=T)  %>%
  group_by(area_fips,technology) %>%
  mutate(ira_index = 100*emplvl/emplvl[date=="2022-07-01"],na.rm=T) 


qcew_states_2123 <- cim_qcew_states %>%
  filter(date =="2021-01-01"|date =="2023-09-01") %>%
  pivot_wider(names_from=date,values_from=emplvl) %>%
  mutate(emp_2123=`2023-09-01`-`2021-01-01`,
         emp_2123_perc=emp_2123/`2021-01-01`*100) %>%
  left_join(state_gdp %>% select(GeoFips,GeoName,gdp_22),by=c("area_fips"="GeoFips"))

qcew_states_2123_top1 <- qcew_states_2123 %>%
  #filter(technology != "Wind Manufacturing",
  #      technology != "Storage Manufacturing",
  #     technology != "Solar Manufacturing") %>%
  mutate(technology = case_when(
    technology == "Storage"~"Generation, Transmission & Storage Construction" ,
    technology == "Zero Emission Vehicles Manufacturing" ~ "Vehicle Manufacturing",
    technology == "SAF" ~ "Biofuels",
    technology == "Solar" ~ "Solar Operations",
    technology == "Wind" ~ "Wind Operations",
    technology == "Wind Manufacturing" ~ "Turbine (incl Wind) and Related Machinery Manufacturing",
    technology == "Solar Manufacturing" ~ "Electrical & Semiconductor Equipment (incl Solar) Manufacturing",
    TRUE ~ technology
  )) %>%
  group_by(GeoName,area_fips) %>%
  slice_max(`2023-09-01`,n=1) 
write.csv(qcew_states_2123_top1,"C:/Users/LCarey.RMI/Downloads/qcew_states_top1.csv")


cip_qcew_state_spec<-cim_qcew_states %>%
  left_join(state_inv_spec,by=c("area_fips"="GeoFips"))

solar_spec<- cip_qcew_state_spec %>%
  select(State.Name,date,technology.x,emplvl) %>%
  #rbind(cim_qcew_us_tech) %>%
  filter(technology.x=="Batteries Manufacturing") %>%
  filter(!is.na(State.Name))

ggplot(data=solar_spec,aes(x=date,y=emplvl,color=State.Name))+
  geom_line()


#Latest Employment - all industries-------------------------------

eti_long<-left_join(eti_long,naics2022 %>% select("2022 NAICS Code",
                                                  "2022 NAICS Title",
                                                  "2017 NAICS Code"),
                    by=c("6-Digit Code"="2017 NAICS Code"))

state_qcew<-data.frame()
state_gdp<-state_gdp %>% slice(2:52)
for (area_code in state_gdp$GeoFips) {
  current_data <- blsQCEW(method = 'Area',
                          year = '2023',
                          quarter = '3',
                          area = area_code)
  state_qcew <- rbind(state_qcew, current_data)
}

all_industries<-blsQCEW(method = 'Industry',
                        year = '2023',
                        quarter = '3',
                        industry = '10')

all_state_industries <- all_industries %>%
  filter(own_code=="5",agglvl_code=="51") %>%
  select(area_fips,month3_emplvl)
all_us_industries <- all_industries %>%
  filter(own_code=="5",area_fips=="US000") %>%
  select(area_fips,month3_emplvl)

US_emp <-state_qcew %>%
  filter(own_code=="5") %>%
  mutate(industry_code=as.numeric(industry_code)) %>%
  inner_join(eti_long,by=c("industry_code"="2022 NAICS Code")) %>%
  rename(tech="Primary Transition Products / Technologies") %>%
  filter(!is.na(industry_code)) %>%
  distinct(area_fips,tech,industry_code,`2022 NAICS Title`,month3_emplvl) %>%
  group_by(tech) %>%
  summarize_at(vars(month3_emplvl),sum,na.rm=T) %>%
  rename(US_ind_emp=month3_emplvl)%>%
  cbind(all_us_industries) %>%
  mutate(emp_share_us=US_ind_emp/month3_emplvl)


state_emp <- state_qcew %>%
  filter(own_code=="5",
         agglvl_code=="58") %>%
  mutate(industry_code=as.numeric(industry_code)) %>%
  inner_join(eti_long,by=c("industry_code"="2022 NAICS Code")) %>%
  rename(tech="Primary Transition Products / Technologies") %>%
  filter(`Transition Sector Category` %in% c("Energy End-Use Sector",
                                             "Industrial End-Use Sector",
                                             "Transportation End-Use Sector",
                                             "Transition Chemical, Mineral, and Metal Manufacturing Sector",
                                             "Transition Mineral and Metal Mining Sector")) %>%
  filter(!is.na(industry_code)) %>%
  group_by(area_fips,tech) %>%
  summarize_at(vars(month3_emplvl),sum,na.rm=T) %>%
  inner_join(state_gdp %>% select(GeoFips,GeoName,gdp_22) %>% mutate(GeoFips =as.numeric(GeoFips)),by=c("area_fips"="GeoFips")) %>%
  inner_join(all_state_industries %>% mutate(area_fips =as.numeric(area_fips)),by=c("area_fips"="area_fips")) %>%
  mutate(emp_share=month3_emplvl.x/month3_emplvl.y) %>%
  inner_join(US_emp,by=c("tech"="tech")) %>%
  mutate(lq=emp_share/emp_share_us) %>%
  arrange(desc(lq)) %>%
  distinct(GeoName,tech,month3_emplvl.x,emp_share,lq)

state_spec_1 <- state_emp %>%
  group_by(GeoName) %>%
  slice_max(order_by=lq,n=1) %>%
  mutate(tech = ifelse(grepl("Vehicles", tech, ignore.case = TRUE), 
                       "Electric Vehicles", 
                       tech))
write.csv(state_spec_1,'C:/Users/LCarey.RMI/Downloads/state_spec_1.csv')

#DOE Energy EMployment Report------------------------------------------------
destination_folder<-'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/'
file_path <- paste0(destination_folder, "USEER 2024 Public Data.xlsx")

state_useer <- read_excel(file_path, sheet = 7,skip=6) %>%
  pivot_longer(cols=c("Solar":"Did not hire"),names_to="name") 
write.csv(state_useer %>%
            distinct(category),"Downloads/state_useer_cat.csv")

state_useer_sum <- state_useer %>%
  mutate(category="",
         category=ifelse(name %in% 
                           c("Solar",
                             "Wind",
                             "Traditional hydropower",
                             "Low impact hydropower, marine, and hydrokinetics",
                             "Geothermal electricity",
                             "Bioenergy/Combined heat and power",
                             "Natural gas electricity",
                             "Coal electricity",
                             "Oil and other fossil fuel electricity",
                             "Nuclear electricity",
                             "Other electricity",
                             "Electric power generation total"),
                         "Electric Power Generation",category),
         category=ifelse(name %in% 
                           c("Traditional transmission and distribution",
                             "Clean storage",
                             "Other storage",
                             "Storage total",
                             "Smart grid",
                             "Micro grid",
                             "Other grid modernization",
                             "Other (including commodity flows)",
                             "Transmission, distribution, and storage total"),
                         "Transmission, Distribution, and Storage",category),
         category=ifelse(name %in% 
                           c("Energy STAR and efficient lighting",
                             "Traditional HVAC with an efficiency component",
                             "High efficiency HVAC and renewable heating and cooling",
                             "Advanced materials",
                             "Other",
                             "Energy efficiency total"),
                         "Energy Efficiency",category),
         category=ifelse(name %in% 
                           c("Coal fuels",
                             "Oil (petroleum and other fossil fuels)",
                             "Natural gas fuels",
                             "Corn ethanol",
                             "Other ethanol and non-woody biomass",
                             "Woody biomass",
                             "Other biofuels",
                             "Nuclear fuels",
                             "Other fuels",
                             "Fuels total"),
                         "Fuels",category),
         category=ifelse(name %in% 
                           c("Gasoline and diesel vehicles",
                             "Hybrid electric vehicles",
                             "Plug-in hybrid electric vehicles",
                             "Battery electric vehicles",
                             "Natural gas vehicles",
                             "Hydrogen/fuel cell vehicles",
                             "Other vehicles",
                             "Motor vehicle commodity flows",
                             "Motor vehicle total"),
                         "Vehicles",category),
         category=ifelse(name %in% 
                           c("Clean jobs without transmission and distribution",
                             "Clean jobs with transmission and distribution"),
                         "Clean Jobs",category),
         category=ifelse(name %in%
                           c( "Agriculture and Forestry",
                              "Mining and Extraction",
                              "Utilities",
                              "Construction",
                              "Manufacturing",
                              "Trade",
                              "Pipeline Transport & Commodity Flows",
                              "Professional Services",
                              "Other Services"),
                         "Jobs by Industry",category),
         category=ifelse(name %in%
                           c("Very difficult hiring",
                             "Somewhat difficult hiring",
                             "Not at all difficult hiring",	
                             "Did not hire"),
                         "Hiring Difficulty",category))



state_useer_total <- state_useer %>%
  filter(category=="Jobs by Industry") %>%
  group_by(`State abbreviation`,State) %>%
  summarize_at(vars(value),sum,na.rm=T) %>%
  mutate(name="Total energy jobs",
         category="Totals")


nat_useer_totals<- state_useer_sum %>%
  group_by(name,category) %>%
  summarize_at(vars(value),sum,na.rm=T) 

nat_useer_total<- state_useer %>%
  filter(category=="Jobs by Industry") %>%
  summarize_at(vars(value),sum,na.rm=T) %>%
  mutate(name="Total energy jobs",
         category="Totals")

state_clean_useer<-left_join(state_useer,state_useer_total,by=c("State"="State"))
state_clean_useer<-state_clean_useer %>%
  mutate(state_share=value.x/value.y) 
nat_clean_useer<-cbind(nat_useer_totals,nat_useer_total) 
nat_clean_useer<-nat_clean_useer %>%
  mutate(nat_share=value...3/value...4)

state_clean_useer<-state_clean_useer %>%
  left_join(nat_clean_useer %>% select(name...1,category...2,nat_share),by=c("name.x"="name...1","category.x"="category...2")) %>%
  mutate(emp_lq = state_share/nat_share) %>%
  arrange(desc(emp_lq)) %>%
  select(State,name.x,category.x,value.x,state_share,emp_lq) %>%
  filter(category.x != "Hiring Difficulty",
         category.x != "Totals") %>%
  mutate(clean = ifelse(name.x %in%
                          c("Solar",
                            "Wind",
                            "Traditional hydropower",
                            "Nuclear electricity",
                            "Nuclear fuels",
                            "Bioenergy/Combined heat and power",
                            "Corn ethanol",
                            "Woody biomass",
                            "Other ethanol and non-woody biomass",
                            "Other biofuels",
                            "Clean storage",
                            "Smart grid",
                            "Micro grid",
                            "Other grid modernization",
                            "Low impact hydropower, marine, and hydrokinetics",
                            "Geothermal electricity",
                            "Battery electric vehicles",
                            "Hybrid electric vehicles",
                            "Plug-in hybrid electric vehicles",
                            "Energy STAR and efficient lighting",
                            "Traditional HVAC with an efficiency component",
                            "Hydrogen/fuel cell vehicles",
                            "High efficiency HVAC and renewable heating and cooling",
                            "Advanced materials"),1,0))

#State Level Specialization and Share across US
state_emp_spec_useer <- state_clean_useer %>%
  filter(clean==1,
         category.x != "Fuels") %>%
  group_by(State) %>%
  slice_max(order_by=emp_lq,n=1) 
write.csv(state_emp_spec_useer,file.path(output_folder, paste0(state_abbreviation,"_emp_spec_useer", ".csv")))

state_emp_share_useer <- state_clean_useer %>%
  filter(clean==1,
         category.x != "Clean Jobs",
         category.x != "Jobs by Industry") %>%
  mutate(state_share=round(state_share*100,1)) %>%
  group_by(State) %>%
  slice_max(order_by=state_share,n=1) 


#Individual state breakdown
state_useer_breakdown <- state_clean_useer %>%
  filter(State==state_name,
         category.x %in% 
           c("Electric Power Generation",
             "Transmission, Distribution, and Storage",
             "Energy Efficiency",
             "Vehicles",
             "Fuels"),
         !grepl("total",name.x )) %>% 
  group_by(category.x,clean) %>%
  summarize_at(vars(value.x),sum,na.rm=T) %>%
  select(clean,category.x,value.x) %>%
  pivot_wider(names_from=category.x,values_from=value.x) %>%
  write.csv(file.path(output_folder, paste0(state_abbreviation,"_useer_breakdown", ".csv")))

state_useer_breakdown_ind <- state_clean_useer %>%
  filter(State==state_name,
         category.x %in% 
           c("Jobs by Industry"),
         !grepl("total",name.x )) %>% 
  select(name.x,value.x)%>%
  arrange(desc(value.x)) %>%
  write.csv(file.path(output_folder, paste0(state_abbreviation,"_useer_breakdown_ind", ".csv")))
  
  
#BLS QCEW--------------------
clean_industry_naics <- read.csv(paste0(raw_data,"clean_industry_naics.csv")) %>% select(-X)
fossil_codes<-fossil_codes %>% 
  mutate(Production.Phase=ifelse(grepl("Electric",Description),"Operations",
                                 ifelse(grepl("Refineries",Description),"Manufacturing","Input")),
         Industry="Oil, Gas, and Coal")
energy_codes<-clean_industry_naics %>%
  filter(nchar(X6.Digit.Code)==6) %>%
  rename("NAICS_code"="X6.Digit.Code",
         "Description"="naics_desc",
         "Industry"="clean_industry") %>%
  rbind(fossil_codes)
  


# Loop through years, quarters, and industry codes
us_qcew<-data.frame()
for (year in c('2018','2019','2020','2021','2022','2023')) {
      
      # Try-catch to handle potential errors
      current_data <- tryCatch({
        # Pull data for the current combination
        blsQCEW('Area', year = year, quarter = 'a', area = 'US000')
        }, error = function(e) {
        message("Error with year: ", year, ", quarter: ", quarter, ", industry: ", industry_code)
        return(NULL)  # Return NULL if there's an error
      })
      
      # Append data only if the request was successful
      if (!is.null(current_data)) {
        us_qcew <- bind_rows(us_qcew, current_data)
      }
      
      # Pause to avoid hitting rate limits
      Sys.sleep(1)
    }

US_energy_emp <-us_qcew %>%
  filter(disclosure_code != "N", own_code == 5) %>%
  mutate(industry_code=as.numeric(industry_code)) %>%
  inner_join(energy_codes,by=c("industry_code"="NAICS_code")) %>%
  select(Industry,Production.Phase,Description,industry_code,year,annual_avg_emplvl,annual_avg_wkly_wage)

us_energy_ind_emp<-US_energy_emp %>%
  filter(Industry != "Wave Energy") %>%
  mutate(Industry=ifelse(Production.Phase=="Design_Engineering","Design & Engineering",Industry)) %>%
  group_by(Industry,Production.Phase,year) %>%
  summarize(emp=sum(annual_avg_emplvl,na.rm=T),
            wage=weighted.mean(annual_avg_wkly_wage,w=annual_avg_emplvl,na.rm=T)) %>%
  arrange(year) %>%
  group_by(Industry,Production.Phase) %>%
  mutate(emp_index=emp/emp[year=="2018"]*100) %>%
  mutate(industry_full=paste(Industry,Production.Phase))