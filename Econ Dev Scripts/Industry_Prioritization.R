##Industry Prioritization Matrix##

# Libraries-----------------
library(httr)
library(data.table)
library(magrittr)
library(blsAPI)
library(dplyr)
library(stringr)
library(tidyr)

#Adjust folder locations as necessary----------------------------------

setwd("C:/Users/LCarey/")
raw_data<-"OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/"
acre_data<-"OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data"

#Clean Industry NAICS codes and industry category crosswalk
clean_industry_naics <- read.csv(paste0(raw_data,"clean_industry_naics.csv")) %>% select(-X)


#Employment - Location Quotients, Employment Change------------------------------------------

# Set State Parameter 

state_name <- "Iowa" #Change to desired state full name
state_abbr <- "IA"  # Change to desired state, e.g., "SC", "TN", "NY", etc.

state_fips <- c(
  "AL"="01", "AK"="02", "AZ"="04", "AR"="05", "CA"="06", "CO"="08", "CT"="09",
  "DE"="10", "FL"="12", "GA"="13", "HI"="15", "ID"="16", "IL"="17", "IN"="18",
  "IA"="19", "KS"="20", "KY"="21", "LA"="22", "ME"="23", "MD"="24", "MA"="25",
  "MI"="26", "MN"="27", "MS"="28", "MO"="29", "MT"="30", "NE"="31", "NV"="32",
  "NH"="33", "NJ"="34", "NM"="35", "NY"="36", "NC"="37", "ND"="38", "OH"="39",
  "OK"="40", "OR"="41", "PA"="42", "RI"="44", "SC"="45", "SD"="46", "TN"="47",
  "TX"="48", "UT"="49", "VT"="50", "VA"="51", "WA"="53", "WV"="54", "WI"="55",
  "WY"="56"
)
state_area <- paste0(state_fips[state_abbr], "000")

#-------Set Region Paramater
  region<-c("42003",
            "42005",
            "42007",
            "42019",
            "42051",
            "42059",
            "42063",
            "42073",
            "42125",
            "42129"
            )
# ----- Download QCEW Data for 2023 and 2015 
state_data   <- blsQCEW('Area', year = '2024', quarter = 'a', area = state_area)
USdata       <- blsQCEW('Area', year = '2024', quarter = 'a', area = 'US000')
state_data15 <- blsQCEW('Area', year = '2015', quarter = 'a', area = state_area)
USdata15     <- blsQCEW('Area', year = '2015', quarter = 'a', area = 'US000')


##OPTIONAL - If you want to look at a specific sub-state region, otherwise PLEASE IGNORE-------------------------------
region_data<-data.frame()

for(county in region){
  df   <- blsQCEW('Area', year = '2023', quarter = 'a', area = county)
  
  region_data<-rbind(region_data,df)
  
}

state_data <- region_data %>%
  group_by(own_code, industry_code, agglvl_code, size_code, disclosure_code, year) %>%
  summarize(
    annual_avg_emplvl = sum(annual_avg_emplvl, na.rm = TRUE)) 

state_data2<-region_data %>%
  group_by(own_code, industry_code, agglvl_code, size_code, disclosure_code, year) %>%
  summarize(across(c(lq_annual_avg_emplvl,avg_annual_pay,lq_annual_avg_wkly_wage), 
                 weighted.mean, 
                 w = .data$annual_avg_emplvl, 
                 na.rm = TRUE))

state_data<-left_join(state_data,state_data2,by=c("own_code", "industry_code", "agglvl_code", "size_code", "disclosure_code", "year"))

# Filter to include only disclosed state data (own_code == 5)----------------------------
available_state_data   <- state_data   %>% filter(disclosure_code != "N", own_code == 5)
available_state_data15 <- state_data15 %>% filter(disclosure_code != "N", own_code == 5)

# ----- Prepare Pivot Data (from clean_industry_naics) -----
filtered_pivot <- clean_industry_naics %>% 
  mutate(detailed_naics = as.character(X6.Digit.Code),
         naics_3 = str_sub(detailed_naics, 1, 3)) %>% 
  filter(!is.na(detailed_naics))

# ----- Function: Match NAICS Codes at 6-, 5-, 4-, and 3-digit levels -----
match_naics <- function(pivot, available_data) {
  matched <- pivot %>% 
    # 6-digit match
    left_join(available_data, by = c("detailed_naics" = "industry_code")) %>% 
    mutate(match_level = ifelse(!is.na(lq_annual_avg_emplvl), "6-digit", NA_character_)) %>%
    # 5-digit match
    mutate(naics_5 = substr(detailed_naics, 1, 5)) %>%
    left_join(available_data, by = c("naics_5" = "industry_code"), suffix = c("", "_5")) %>%
    mutate(match_level = ifelse(is.na(match_level) & !is.na(lq_annual_avg_emplvl_5), "5-digit", match_level),
           lq_annual_avg_emplvl = coalesce(lq_annual_avg_emplvl, lq_annual_avg_emplvl_5),
           annual_avg_emplvl   = coalesce(annual_avg_emplvl, annual_avg_emplvl_5),
           avg_annual_pay      = coalesce(avg_annual_pay, avg_annual_pay_5),
           lq_annual_avg_wkly_wage = coalesce(lq_annual_avg_wkly_wage, lq_annual_avg_wkly_wage_5)) %>%
    # 4-digit match
    mutate(naics_4 = substr(detailed_naics, 1, 4)) %>%
    left_join(available_data, by = c("naics_4" = "industry_code"), suffix = c("", "_4")) %>%
    mutate(match_level = ifelse(is.na(match_level) & !is.na(lq_annual_avg_emplvl_4), "4-digit", match_level),
           lq_annual_avg_emplvl = coalesce(lq_annual_avg_emplvl, lq_annual_avg_emplvl_4),
           annual_avg_emplvl   = coalesce(annual_avg_emplvl, annual_avg_emplvl_4),
           avg_annual_pay      = coalesce(avg_annual_pay, avg_annual_pay_4),
           lq_annual_avg_wkly_wage = coalesce(lq_annual_avg_wkly_wage, lq_annual_avg_wkly_wage_4)) %>%
    # 3-digit match
    mutate(naics_3 = substr(detailed_naics, 1, 3)) %>%
    left_join(available_data, by = c("naics_3" = "industry_code"), suffix = c("", "_3")) %>%
    mutate(match_level = ifelse(is.na(match_level) & !is.na(lq_annual_avg_emplvl_3), "3-digit", match_level),
           lq_annual_avg_emplvl = coalesce(lq_annual_avg_emplvl, lq_annual_avg_emplvl_3),
           annual_avg_emplvl   = coalesce(annual_avg_emplvl, annual_avg_emplvl_3),
           avg_annual_pay      = coalesce(avg_annual_pay, avg_annual_pay_3),
           lq_annual_avg_wkly_wage = coalesce(lq_annual_avg_wkly_wage, lq_annual_avg_wkly_wage_3)) %>%
    # Finalize industry code based on match level
    mutate(industry_code = case_when(
      match_level == "6-digit" ~ detailed_naics,
      match_level == "5-digit" ~ naics_5,
      match_level == "4-digit" ~ naics_4,
      match_level == "3-digit" ~ naics_3,
      TRUE ~ NA_character_
    )) %>%
    select(clean_industry, Production.Phase, industry_code, naics_desc, detailed_naics,
           match_level, lq_annual_avg_emplvl, annual_avg_emplvl, avg_annual_pay, lq_annual_avg_wkly_wage)
  return(matched)
}

# ----- Process 2023 Data -----
state_energy <- match_naics(filtered_pivot, available_state_data) %>%
  mutate(matched_naics = case_when(
    match_level == "6-digit" ~ substr(detailed_naics, 1, 6),
    match_level == "5-digit" ~ substr(detailed_naics, 1, 5),
    match_level == "4-digit" ~ substr(detailed_naics, 1, 4),
    match_level == "3-digit" ~ substr(detailed_naics, 1, 3)
  ))

state_energylqs <- state_energy %>%
  group_by(clean_industry, industry_code) %>%
  mutate(is_duplicate = n() > 1,
         Production.Phase = if_else(
           is_duplicate,
           case_when(
             startsWith(as.character(industry_code), "2") ~ "Operations",
             startsWith(as.character(industry_code), "3") ~ "Manufacturing",
             TRUE ~ Production.Phase
           ),
           Production.Phase
         )) %>%
  ungroup() %>%
  select(-is_duplicate, -detailed_naics) %>%
  distinct()

# Helper: Remove Nested NAICS Codes
get_unique_naics <- function(codes) {
  codes <- unique(codes[order(nchar(codes))])
  unique_codes <- c()
  for (code in codes) {
    if (!any(sapply(unique_codes, function(x) str_starts(code, x) && code != x)))
      unique_codes <- c(unique_codes, code)
  }
  unique_codes
}

state_subsectors <- state_energylqs %>%
  mutate(matched_naics = as.character(matched_naics)) %>%
  filter(!is.na(matched_naics)) %>%
  group_by(clean_industry, Production.Phase) %>%
  summarise(unique_naics = list(get_unique_naics(matched_naics)), .groups = 'drop') %>%
  unnest(unique_naics)

state_qcew_tot <- state_data %>% filter(own_code == 0) %>% pull(annual_avg_emplvl)
US_qcew_tot    <- USdata     %>% filter(own_code == 0) %>% pull(annual_avg_emplvl)

state_ind_emp <- state_subsectors %>% 
  left_join(state_data, by = c("unique_naics" = "industry_code")) %>%
  filter(!is.na(unique_naics), own_code == 5) %>%
  group_by(clean_industry, Production.Phase) %>%
  summarise(state_ind_emp = sum(annual_avg_emplvl, na.rm = TRUE), .groups = 'drop') %>%
  mutate(state_emp_perc = state_ind_emp / state_qcew_tot)

US_ind_emp <- state_subsectors %>% 
  left_join(USdata, by = c("unique_naics" = "industry_code")) %>%
  filter(!is.na(unique_naics), own_code == 5) %>%
  group_by(clean_industry, Production.Phase) %>%
  summarise(US_ind_emp = sum(annual_avg_emplvl, na.rm = TRUE), .groups = 'drop') %>%
  mutate(US_emp_perc = US_ind_emp / US_qcew_tot)

ind_emp_combined <- left_join(US_ind_emp, state_ind_emp, by = c("clean_industry", "Production.Phase")) %>%
  mutate(consolidated_ind_lq = state_emp_perc / US_emp_perc)

# ----- Process 2015 Data -----
state_energy15 <- match_naics(filtered_pivot, available_state_data15) %>%
  mutate(matched_naics = case_when(
    match_level == "6-digit" ~ substr(detailed_naics, 1, 6),
    match_level == "5-digit" ~ substr(detailed_naics, 1, 5),
    match_level == "4-digit" ~ substr(detailed_naics, 1, 4),
    match_level == "3-digit" ~ substr(detailed_naics, 1, 3)
  ))

state_energylqs15 <- state_energy15 %>%
  group_by(clean_industry, industry_code) %>%
  mutate(is_duplicate = n() > 1,  # Identify duplicates
         Production.Phase = if_else(
    is_duplicate,  # Apply case_when only to duplicates
    case_when(
      startsWith(as.character(industry_code), "2") ~ "Operations",
      startsWith(as.character(industry_code), "3") ~ "Manufacturing",
      TRUE ~ Production.Phase
    ),
    Production.Phase  # Keep original for non-duplicates
  )) %>%
  ungroup() %>%
  select(-detailed_naics) %>%
  distinct()

state_subsectors15 <- state_energylqs15 %>%
  mutate(matched_naics = as.character(matched_naics)) %>%
  filter(!is.na(matched_naics)) %>%
  group_by(clean_industry, Production.Phase) %>%
  summarise(unique_naics = list(get_unique_naics(matched_naics)), .groups = 'drop') %>%
  unnest(unique_naics)

state_qcew_tot15 <- state_data15 %>% filter(own_code == 0) %>% pull(annual_avg_emplvl)
US_qcew_tot15    <- USdata15     %>% filter(own_code == 0) %>% pull(annual_avg_emplvl)

state_ind_emp15 <- state_subsectors15 %>%
  left_join(state_data15, by = c("unique_naics" = "industry_code")) %>%
  filter(!is.na(unique_naics), own_code == 5) %>%
  group_by(clean_industry, Production.Phase) %>%
  summarise(state_ind_emp15 = sum(annual_avg_emplvl, na.rm = TRUE), .groups = 'drop') %>%
  mutate(state_emp_perc15 = state_ind_emp15 / state_qcew_tot15)

US_ind_emp15 <- state_subsectors15 %>% 
  left_join(USdata, by = c("unique_naics" = "industry_code")) %>%
  filter(!is.na(unique_naics), own_code == 5) %>%
  group_by(clean_industry, Production.Phase) %>%
  summarise(US_ind_emp15 = sum(annual_avg_emplvl, na.rm = TRUE), .groups = 'drop') %>%
  mutate(US_emp_perc15 = US_ind_emp15 / US_qcew_tot15)

ind_emp_combined15 <- left_join(US_ind_emp15, state_ind_emp15, by = c("clean_industry", "Production.Phase")) %>%
  mutate(consolidated_ind_lq_2015 = state_emp_perc15 / US_emp_perc15) %>%
  select(clean_industry, Production.Phase, consolidated_ind_lq_2015, state_ind_emp15) %>%
  left_join(ind_emp_combined, by = c("clean_industry", "Production.Phase")) %>%
  mutate(employment_change = (state_ind_emp - state_ind_emp15) / state_ind_emp15,
         lq_change         = consolidated_ind_lq - consolidated_ind_lq_2015) %>%
  filter(clean_industry != "Wave Energy") %>%
  select(clean_industry, Production.Phase, state_ind_emp, state_ind_emp15, employment_change,
         consolidated_ind_lq, consolidated_ind_lq_2015, lq_change)

# ----- Final Output -----
# ind_emp_combined and ind_emp_combined15 now contain the calculated measures for 2023,
# and the changes from 2015 to 2023, respectively.



#Feasibility------------------------

#feas<-read.csv(paste0(acre_data,/Clean-growth-project/raw/ClimateandEconomicJusticeTool/feasibility_geo.csv'))

feas_state<-feas %>%
  filter(geo=="State",
         geo_name==state_name,
         aggregation_level==4)%>%
  inner_join(clean_industry_naics %>%
               mutate(naics_6=as.numeric(X6.Digit.Code)),by=c("industry_code"="naics_6")) %>%
  left_join(state_energy %>%
              mutate(detailed_naics=as.numeric(detailed_naics)),by=c("clean_industry","Production.Phase","X6.Digit.Code"="detailed_naics")) %>%
  group_by(clean_industry,Production.Phase) %>%
  summarize(across(c(annual_avg_emplvl,industry_feas_perc,density,pci), 
                   weighted.mean, 
                   w = .data$annual_avg_emplvl, 
                   na.rm = TRUE))


#Clean investment Monitor Data---------------------------
investment_data_path <- paste0(raw_data,'clean_investment_monitor_q1_2025/quarterly_actual_investment.csv')
facilities_data_path <- paste0(raw_data,'/clean_investment_monitor_q1_2025/manufacturing_energy_and_industry_facility_metadata.csv')
socioeconomics_data_path <- 'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q4_2024/socioeconomics.csv'

investment <- read.csv(investment_data_path, skip=5)
facilities <- read.csv(facilities_data_path, skip=5)
socioecon <- read.csv(socioeconomics_data_path, skip=5)

CIM_eco_eti<-read.csv(paste0(raw_data,"CIM_eco_eti_invest_categories.csv"))

investment_eco<-investment %>%
  left_join(CIM_eco_eti,by=c("Technology","Segment")) %>%
  mutate(clean_industry=ifelse(Technology=="Other" & Subcategory=="Geothermal","Geothermal",clean_industry),
         Production.Phase=ifelse(clean_industry=="Geothermal","Operations",Production.Phase)) %>%
  filter(clean_industry!= "") %>%
  group_by(State,clean_industry,Production.Phase,quarter) %>%
  summarize_at(vars(Estimated_Actual_Quarterly_Expenditure),sum,na.rm=T) %>%
  left_join(socioecon,by=c("State","quarter")) %>%
  mutate(inv_gdp=Estimated_Actual_Quarterly_Expenditure/real_gdp) %>%
  mutate(year=as.numeric(substr(quarter,1, 4))) %>%
  filter(year>2021) %>%
  group_by(State,StateName,clean_industry,Production.Phase) %>%
  summarize_at(vars(Estimated_Actual_Quarterly_Expenditure,real_gdp),sum,na.rm=T) %>%
  group_by(clean_industry,Production.Phase) %>%
  mutate(inv_gdp=Estimated_Actual_Quarterly_Expenditure/real_gdp,
         inv_gdp_rank=rank(inv_gdp))

division_eco <- census_divisions %>%
  filter(State==state_name)

write.csv(investment_eco %>%
            ungroup() %>%
            left_join(census_divisions %>%
                        select(-State),by=c("State"="State.Code")) %>%
            filter(Division %in% division_eco$Division) %>%
            group_by(State,clean_industry) %>%
            summarize(inv_gdp = sum(inv_gdp,na.rm=T)) %>%
            pivot_wider(names_from=State,values_from=inv_gdp),"Downloads/investment_Division.csv")

facilities_eco <-facilities %>%
  left_join(CIM_eco_eti,by=c("Technology","Segment")) %>%
  mutate(clean_industry=ifelse(Technology=="Other" & Subcategory=="Geothermal","Geothermal",clean_industry),
         Production.Phase=ifelse(clean_industry=="Geothermal","Operations",Production.Phase)) %>%
  filter(State==state_abbr,
         clean_industry!= "",
         Current_Facility_Status %in% c("Under Construction",
                                        "Announced")) %>%
  group_by(State,clean_industry,Production.Phase) %>%
  summarize_at(vars(Estimated_Total_Facility_CAPEX),sum,na.rm=T)


#USEER Employment Data ---------------------------------
destination_folder<-'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/'
file_path <- paste0(destination_folder, "USEER 2024 Public Data.xlsx")

state_useer <- read_excel(file_path, sheet = 7,skip=6) %>%
  pivot_longer(cols=c("Solar":"Did not hire"),names_to="category") 
useer_eco <- read.csv(paste0(raw_data,"/state_useer_cat.csv"))
useer_eco<-useer_eco %>%
  filter(clean_industry != "")

state_useer_eco<-state_useer %>%
  inner_join(useer_eco,by="category") %>%
  filter(State==state_name) %>%
  group_by(clean_industry,Production.Phase) %>%
  summarize_at(vars(value),sum,na.rm=T) %>%
  arrange(desc(value)) 


#Energy Potential - NREL Supply Curve Data------------------------------
supplycurve_geo<-read.csv(paste0(raw_data,"supplycurve_geo.csv")) %>% select(-X)

supplycurve_state <- supplycurve_geo %>%
  filter(geo=="State") %>%
  select(geo_name,Solar_potential_rank:Geothermal_cost_rank) %>%
  pivot_longer(
    cols = Solar_potential_rank:Geothermal_cost_rank,
    names_to = c("clean_industry", "metric"),
    names_pattern = "([A-Za-z]+)_(potential|cost)_rank"
  ) %>%
  pivot_wider(
    names_from = metric,
    values_from = value
  ) %>%
  mutate(Production.Phase="Operations") %>%
  mutate(clean_industry=ifelse(clean_industry=="Wind","Wind Energy",clean_industry))


#Manufacturing Potential - Employment & Cost Data-------------------------
state_data_man   <- blsQCEW('Area', year = '2024', quarter = '3', area = state_area)



#Federal Policy Support---------------------------------
federal_support <- clean_industry_naics %>%
  distinct(clean_industry,Production.Phase) %>%
  mutate(fed_support = ifelse(clean_industry == "Solar" & 
                                Production.Phase == "Operations",1,0),
         fed_support = ifelse(clean_industry == "Wind" & 
                                Production.Phase == "Operations",1,fed_support),
         fed_support = ifelse(clean_industry == "Solar" & 
                                Production.Phase == "Manufacturing",1,fed_support),
         fed_support = ifelse(clean_industry == "Wind" & 
                                Production.Phase == "Manufacturing",1,fed_support),
         fed_support = ifelse(clean_industry == "Batteries" & 
                                Production.Phase == "Manufacturing",1,fed_support),
         fed_support = ifelse(clean_industry == "Inverters" & 
                                Production.Phase == "Manufacturing",1,fed_support),
         fed_support = ifelse(clean_industry == "Green Hydrogen" & 
                                Production.Phase == "Operations",1,fed_support),
         fed_support = ifelse(clean_industry == "Nuclear" & 
                                Production.Phase == "Operations",1,fed_support),
         fed_support = ifelse(clean_industry == "Critical Minerals" & 
                                Production.Phase == "Manufacturing",1,fed_support),
         fed_support = ifelse(clean_industry == "Geothermal" & 
                                Production.Phase == "Operations",1,fed_support),
         fed_support = ifelse(clean_industry == "Biofuels" & 
                                Production.Phase == "Manufacturing",1,fed_support),
         fed_support = ifelse(clean_industry == "Energy Storage" & 
                                Production.Phase == "Operations",1,fed_support)
  )

#State Policy Support------------------------------------
xchange_state<-read.csv(paste0(raw_data,"xchange.csv")) %>% select(-X) %>%
  filter(grepl("index",Policy),
         abbr==state_abbr) 

state_support <- clean_industry_naics %>%
  distinct(clean_industry,Production.Phase) %>%
  mutate(state_support = ifelse(clean_industry == "Solar" & 
                                Production.Phase == "Operations",xchange_state$value[xchange_state$Topic == "Electricity"],""),
         state_support = ifelse(clean_industry == "Wind" & 
                                Production.Phase == "Operations",xchange_state$value[xchange_state$Topic == "Electricity"],state_support),
         state_support = ifelse(clean_industry == "Green Hydrogen" & 
                                Production.Phase == "Operations",xchange_state$value[xchange_state$Topic == "Industry"],state_support),
         state_support = ifelse(clean_industry == "Nuclear" & 
                                Production.Phase == "Operations",xchange_state$value[xchange_state$Topic == "Electricity"],state_support),
         state_support = ifelse(clean_industry == "Geothermal" & 
                                Production.Phase == "Operations",xchange_state$value[xchange_state$Topic == "Electricity"],state_support),
         state_support = ifelse(clean_industry == "Biofuels" & 
                                Production.Phase == "Manufacturing",xchange_state$value[xchange_state$Topic == "Industry"],state_support),
         state_support = ifelse(clean_industry == "Energy Transition Metals" & 
                                  Production.Phase == "Manufacturing",xchange_state$value[xchange_state$Topic == "Industry"],state_support),
         state_support = ifelse(clean_industry == "Energy Storage" & 
                                Production.Phase == "Operations",xchange_state$value[xchange_state$Topic == "Electricity"],state_support),
         state_support = ifelse(clean_industry == "Transmission & Distribution" & 
                                  Production.Phase == "Construction",xchange_state$value[xchange_state$Topic == "Electricity"],state_support),
         state_support = ifelse(clean_industry == "Energy Efficient Heating/Cooling",xchange_state$value[xchange_state$Topic == "Buildings"],state_support),
         state_support = ifelse(clean_industry == "Energy Efficient Lighting",xchange_state$value[xchange_state$Topic == "Buildings"],state_support),
         state_support = ifelse(clean_industry == "Energy Efficient Appliances",xchange_state$value[xchange_state$Topic == "Buildings"],state_support))


#Technology Readiness from IEA -----------------------------------
iea_cleantech_sector<-read.csv(paste0(raw_data,"iea_cleantech_sector.csv"))

#National Investment from CIM----------------------

investment_national<-investment %>%
  left_join(CIM_eco_eti,by=c("Technology","Segment")) %>%
  mutate(clean_industry=ifelse(Technology=="Other" & Subcategory=="Geothermal","Geothermal",clean_industry),
         Production.Phase=ifelse(clean_industry=="Geothermal","Operations",Production.Phase)) %>%
  filter(clean_industry!= "") %>%
  mutate(year=as.numeric(substr(quarter,1, 4))) %>%
  group_by(clean_industry,Production.Phase,year) %>%
  summarize(value=sum(Estimated_Actual_Quarterly_Expenditure,na.rm=T),
            .groups='drop') %>%
  pivot_wider(names_from="year",values_from='value') %>%
  mutate(growth_2224=(`2024`-`2022`)/`2022`*100) %>%
  rename("inv_24"="2024") %>%
  select(clean_industry,Production.Phase,growth_2224,inv_24)

#Innovation Funding from ITIF-------------------------------------
url <- 'https://cdn.sanity.io/files/03hnmfyj/production/77717b609392dedba6f8ba316ce16d6629bf6666.xlsx'
temp_file <- tempfile(fileext = ".xlsx")
GET(url = url, write_disk(temp_file, overwrite = TRUE))
innov_state <- read_excel(temp_file, sheet = 3)  # 'sheet = 1' to read the first sheet
innov_metro <- read_excel(temp_file, sheet = 4)  # 'sheet = 1' to read the first sheet
innov_vars<- read_excel(temp_file, sheet = 1,skip=58)  # 'sheet = 1' to read the first sheet
key_vars <-innov_vars %>% filter(Subindex %in% c("Knowledge Development and Diffusion (KDD)","Entrepreneurial Experimentation (EE)"))
sectors <- c("bioenergy", "ccus", "efficiency", "geothermal", 
             "grid", "hydrogen", "mfg", "nuclear", "solar", 
             "storage", "transport", "water", "wind","all")

# Build a regular expression that matches columns ending in one of the sectors.
regex <- paste0("^(.*)_(", paste(sectors, collapse = "|"), ")$")

# Pivot the data longer: keep 'year' and 'statecode' and split the other columns
innov_state_long <- innov_state %>%
  pivot_longer(
    cols = matches(regex),
    names_to = c("measure", "sector"),
    names_pattern = regex
  ) %>%
  distinct(year,statecode,measure,sector,value) %>%
  inner_join(innov_vars %>% 
               filter(Subindex %in% c("Knowledge Development and Diffusion (KDD)","Entrepreneurial Experimentation (EE)")) %>%
               select(`Variable Name`,Subindex),by=c("measure"="Variable Name"))%>% 
  group_by(statecode,measure,Subindex,sector) %>%
  summarize_at(vars(value),sum,na.rm=T) %>%
  group_by(measure,Subindex,sector) %>%
  mutate(rank=rank(value))

# --- US-level normalization ---
# Step 1.1: Compute the US-wide baseline (the "all" sector) for each measure & Subindex.
us_denom <- innov_state_long %>%
  filter(sector != "all") %>%
  group_by(measure, Subindex) %>%
  summarize(us_denom = sum(value, na.rm = TRUE), .groups = "drop")

# Step 1.2: For each (measure, sector, Subindex), sum values across states and compute the US norm.
us_norm <- innov_state_long %>%
  group_by(measure, sector, Subindex) %>%
  summarize(us_value = sum(value, na.rm = TRUE), .groups = "drop") %>%
  left_join(us_denom, by = c("measure", "Subindex")) %>%
  mutate(us_norm = us_value / us_denom) %>%
  select(measure, sector, Subindex, us_norm)

# --- State-level normalization ---
# Step 2.1: Compute the state-level baseline (for sector == "all") for each state, measure, Subindex.
state_denom <- innov_state_long %>%
  filter(sector != "all") %>%
  group_by(statecode, measure, Subindex) %>%
  summarize(state_denom = sum(value, na.rm = TRUE), .groups = "drop")

# Step 2.2: Compute the state-level value for each (state, measure, sector, Subindex)
# and calculate the state normalized value.
state_norm <- innov_state_long %>%
  group_by(statecode, measure, sector, Subindex) %>%
  summarize(state_value = sum(value, na.rm = TRUE), .groups = "drop") %>%
  left_join(state_denom, by = c("statecode", "measure", "Subindex")) %>%
  mutate(state_norm = state_value / state_denom) 

# --- Combine and create the state index ---
# Step 3: For each (state, measure, sector, Subindex), merge the state and US norms and compute the ratio.
combined <- state_norm %>%
  left_join(us_norm, by = c("measure", "sector", "Subindex")) %>%
  mutate(ratio = state_norm / us_norm) %>%
  group_by(measure,sector,Subindex) %>%
  mutate(min_val = min(ratio, na.rm = TRUE),
         max_val = max(ratio, na.rm = TRUE),
         norm_0_1 = if_else(max_val == min_val, 0, (ratio - min_val) / (max_val - min_val))) 

# Step 4: Now average the ratio within each state and Subindex.
state_index <- combined %>%
  group_by(statecode, Subindex, sector) %>%
  summarize(index = mean(ratio, na.rm = TRUE), .groups = "drop")

rdd_state_index <- state_index %>%
  filter(Subindex == "Knowledge Development and Diffusion (KDD)",
         sector != "all") %>%
  mutate(clean_industry=ifelse(sector=="bioenergy","Biofuels",""),
         clean_industry=ifelse(sector=="ccus","Carbon Capture",clean_industry),
         clean_industry=ifelse(sector=="geothermal","Geothermal",clean_industry),
         clean_industry=ifelse(sector=="grid","Transmission & Distribution",clean_industry),
         clean_industry=ifelse(sector=="efficiency","Energy Efficient Appliances|Energy Efficient Heating/Cooling|Energy Efficient Lighting",clean_industry),
         clean_industry=ifelse(sector=="hydrogen","Green Hydrogen",clean_industry),
         clean_industry=ifelse(sector=="nuclear","Nuclear",clean_industry),
         clean_industry=ifelse(sector=="solar","Solar",clean_industry),
         clean_industry=ifelse(sector=="storage","Energy Storage|Batteries",clean_industry),
         clean_industry=ifelse(sector=="transport","Electric Vehicles",clean_industry),
         clean_industry=ifelse(sector=="wind","Wind Energy",clean_industry),
         clean_industry=ifelse(sector=="water","Water Purification",clean_industry)) %>%
  separate_rows(clean_industry, sep = "\\|")

state_sector_norm <- rdd_state_index %>%
  group_by(Subindex, clean_industry) %>%
  mutate(min_val = min(index, na.rm = TRUE),
         max_val = max(index, na.rm = TRUE),
         norm_0_1 = if_else(max_val == min_val, 0, (index - min_val) / (max_val - min_val))) %>%
  ungroup()


state_sector_norm <- state_sector_norm %>% 
  select(statecode,sector,norm_0_1) %>%
  rename("RDD_specialization"="norm_0_1") 

innov_state_sum <- innov_state_long %>%
  filter(Subindex=="Knowledge Development and Diffusion (KDD)",
         sector != "all") %>%
  group_by(sector,measure) %>%
  mutate(min_val = min(value, na.rm = TRUE),
         max_val = max(value, na.rm = TRUE),
         norm_0_1 = if_else(max_val == min_val, 0, (value - min_val) / (max_val - min_val))) %>%
  group_by(statecode,sector) %>%
  summarize_at(vars(norm_0_1),sum,na.rm=T) %>%
  rename("RDD_total"="norm_0_1") %>%
  left_join(state_sector_norm,by=c("statecode","sector")) %>%
  filter(clean_industry != "") %>%
  select(-sector)


#Putting the Matrix Together------------------
ind_matrix<-left_join(ind_emp_combined15,feas_state,by=c("clean_industry","Production.Phase")) %>%
  left_join(investment_eco %>%
                 filter(State==state_abbr) %>%
                 select(clean_industry,Production.Phase,Estimated_Actual_Quarterly_Expenditure,inv_gdp_rank),
               by=c("clean_industry","Production.Phase")) %>%
  left_join(facilities_eco %>%
              ungroup() %>%
              select(-State) %>%
              mutate(across(where(is.numeric), ~ tidyr::replace_na(.x, 0))),by=c("clean_industry","Production.Phase")) %>%
  left_join(state_useer_eco,by=c("clean_industry","Production.Phase")) %>%
  left_join(supplycurve_state %>%
            filter(geo_name==state_name) %>%
            select(-geo_name) %>%
            mutate(potential=51-potential,
                   cost=51-cost
                   ),by=c("clean_industry","Production.Phase")) %>%
  left_join(iea_cleantech_sector %>%
              select(-X),by=c("clean_industry"="rmi_sector")) %>%
  left_join(federal_support,by=c("clean_industry","Production.Phase")) %>%
  left_join(state_support,by=c("clean_industry","Production.Phase")) %>%
  left_join(innov_state_sum %>%
              filter(statecode==state_abbr) %>%
              select(-statecode),by="clean_industry")


#Normalized Matrix & Index----------------------------------------- 
normalized_ind_matrix <- ind_matrix %>%
  ungroup() %>%
  select(-state_ind_emp15, -consolidated_ind_lq_2015) %>%
  # Replace infinite values with the column max and NAs with the column min (only on numeric cols)
  mutate(across(
    where(is.numeric),
    ~ case_when(
      is.infinite(.) ~ max(.[!is.infinite(.)], na.rm = TRUE),
      is.na(.)       ~ min(.[!is.infinite(.)], na.rm = TRUE),
      TRUE           ~ .
    )
  )) %>%
  # Normalize specific columns on an external scale (1-50 becomes 0-1)
  mutate(
    potential    = (potential    - 1) / (50 - 1),
    cost         = (cost         - 1) / (50 - 1),
    inv_gdp_rank = (inv_gdp_rank - 1) / (50 - 1)
  ) %>%
  # Normalize all other numeric columns (excluding the externally normalized ones)
  mutate(across(
    .cols = setdiff(names(select(., where(is.numeric))), c("potential", "cost", "inv_gdp_rank")),
    ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE))
  )) %>%
  rowwise() %>%
  mutate(
    # Calculate index, giving density 3× (or 4× in this code) weight if non-zero
    index = {
      row_vals <- c_across(where(is.numeric))
      if ("density" %in% names(row_vals)) {
        if (row_vals[["density"]] != 0) {
          other_vals <- row_vals[names(row_vals) != "density"]
          new_vals <- c(other_vals, rep(row_vals[["density"]], 4))
        } else {
          new_vals <- row_vals[names(row_vals) != "density"]
        }
      } else {
        new_vals <- row_vals
      }
      mean(new_vals, na.rm = TRUE)
    }
  ) %>%
  ungroup() %>%
  # Final normalization for all numeric columns except our externally normalized ones
  mutate(across(
    .cols = setdiff(names(select(., where(is.numeric))), c("potential", "cost", "inv_gdp_rank")),
    ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE))
  ))

#Final Matrix----------------------------
ind_matrix_final <- left_join(ind_matrix,normalized_ind_matrix %>%
                           select(clean_industry,Production.Phase,index),by=c("clean_industry","Production.Phase")) %>%
  group_by(Production.Phase,state_ind_emp,potential) %>%
  filter(n() < 2) %>%
  ungroup() %>%
  arrange(desc(index))

#Write to CSV----------------------------------------
write.csv(ind_matrix_final %>%
            mutate(across(where(is.numeric), ~ ifelse(is.na(.), 0, .))) %>%
            select(clean_industry,
                   Production.Phase,
                   index,
                   density,
                   consolidated_ind_lq,
                   state_ind_emp,
                   Estimated_Actual_Quarterly_Expenditure,
                   Estimated_Total_Facility_CAPEX,
                   potential,
                   cost,
                   trl2023,
                   pci),paste0("Downloads/ind_matrix_",state_abbr,".csv"))

