#Industry Prioritization Matrix

#Clean Industry NAICS
clean_industry_naics <- read.csv(paste0(raw_data,"clean_industry_naics.csv")) %>% select(-X)


#Employment - Location Quotients, Employment Change------------------------------------------

# Libraries
library(httr)
library(data.table)
library(magrittr)
library(blsAPI)
library(dplyr)
library(stringr)
library(tidyr)

# ----- Set State Parameter -----
state_name <- "New Mexico" #Change to desired state full name
state_abbr <- "NM"  # Change to desired state, e.g., "SC", "TN", "NY", etc.
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

# ----- Download QCEW Data for 2023 and 2015 -----
state_data   <- blsQCEW('Area', year = '2023', quarter = 'a', area = state_area)
USdata       <- blsQCEW('Area', year = '2023', quarter = 'a', area = 'US000')
state_data15 <- blsQCEW('Area', year = '2015', quarter = 'a', area = state_area)
USdata15     <- blsQCEW('Area', year = '2015', quarter = 'a', area = 'US000')

# Filter to include only disclosed state data (own_code == 5)
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

#feas<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Clean-growth-project/raw/ClimateandEconomicJusticeTool/feasibility_geo.csv')

feas_state<-feas %>%
  filter(geo=="State",
         geo_name==state_name,
         aggregation_level==4)%>%
  inner_join(clean_industry_naics %>%
               mutate(naics_6=as.numeric(X6.Digit.Code)),by=c("industry_code"="naics_6")) %>%
  left_join(state_energy %>%
              mutate(detailed_naics=as.numeric(detailed_naics)),by=c("clean_industry","Production.Phase","X6.Digit.Code"="detailed_naics")) %>%
  group_by(clean_industry,Production.Phase) %>%
  summarize(across(c(density,pci), 
                   weighted.mean, 
                   w = .data$annual_avg_emplvl, 
                   na.rm = TRUE))


#Clean investment Monitor Data---------------------------
investment_categories<-investment %>%
  select(Technology,Segment)%>%
  distinct()
write.csv(investment_categories,"Downloads/invest_categories.csv")
eco_eti_categories<-eco_eti_2 %>%
  select(clean_industry,Production.Phase)%>%
  distinct()
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
useer_eco <- read.csv("Downloads/state_useer_cat.csv")
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


#Technology Readiness from IEA -----------------------------------
iea_cleantech_sector<-read.csv(paste0(raw_data,"iea_cleantech_sector.csv"))

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
              select(-X),by=c("clean_industry"="rmi_sector"))


#Prioritization Index-----------------------------------------
normalized_ind_matrix <- ind_matrix %>%
  ungroup() %>%
  select(-state_ind_emp15, -consolidated_ind_lq_2015) %>%
  mutate(across(
    where(is.numeric),
    ~ case_when(
      is.infinite(.) ~ max(.[!is.infinite(.)], na.rm = TRUE),
      is.na(.)       ~ 0,
      TRUE           ~ .
    )
  )) %>%
  # Normalize within each geo group (using na.rm = TRUE)
  mutate(across(
    where(is.numeric),
    ~ (. - min(., na.rm = TRUE)) /
      (max(., na.rm = TRUE) - min(., na.rm = TRUE))
  )) %>%
  rowwise() %>%
  mutate(
    index = {
      row_vals <- c_across(where(is.numeric))
      if("density" %in% names(row_vals)) {
        if(row_vals[["density"]] != 0) {
          # Include density three times if it is non-zero
          other_vals <- row_vals[names(row_vals) != "density"]
          new_vals <- c(other_vals, rep(row_vals[["density"]], 3))
        } else {
          # Exclude density if it is zero
          new_vals <- row_vals[names(row_vals) != "density"]
        }
      } else {
        new_vals <- row_vals
      }
      mean(new_vals, na.rm = TRUE)
    }
  ) %>%
  ungroup() %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(., na.rm = TRUE)) /
      (max(., na.rm = TRUE) - min(., na.rm = TRUE))
  ))

ind_matrix_final <- left_join(ind_matrix,normalized_ind_matrix %>%
                           select(clean_industry,Production.Phase,index),by=c("clean_industry","Production.Phase")) %>%
  #group_by(Production.Phase,state_ind_emp) %>%
  #filter(n() < 2) %>%
  ungroup() %>%
  arrange(desc(index))


