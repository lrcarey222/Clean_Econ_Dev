##1. Enhanced Feasibility for Key Industries------------------------

#1.1 Industries-------------------
#Battery & EV Manufacturing
#Solar PV Manufacturing
#Critical Minerals Mining & Processing
#Green Hydrogen
#Green Steel
#Solar Deployment
#Wind Deployment 

#1.2 Feasibility Indicators-------------------
#Workforce - CGT Feasibility
#Natural Resources - solar, wind, mineral, and land availability
#Infrastructure - rail, road, port, and grid access
#Policy - state and federal incentives
#Supply chain - existing facilities in region
#Demand - existing market in region; economic growth
#Cost - land, labor, and energy costs
#Risk - political, environmental, and economic risks


##2 Cleaned, simplified feasibility spreadsheet-----------------------

#2.1 Set the Working Directory to your Username
setwd("C:/Users/LCarey/")

#Load Latest Clean Growth Tool Data-----------------------------
#County Level
cgt_county<-read.csv('C:/Users/LCarey/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/CGT_county_data/cgt_county_data_08_29_2024.csv')
cgt_county<-cgt_county %>%
  filter(aggregation_level=="2",
         industry_desc %in% c("Batteries & Components",
                              "Solar Energy Components",
                              "Solar Electric Power",
                              "Wind Electric Power",
                              "Low-Carbon Minerals",
                              "Green Hydrogen",
                              "Low-Carbon Iron & Steel",
                              "Energy Transition Metals",
                              "Low-Carbon Metals",
                              "Electric Vehicle Components",
                              "Electric Vehicles",
                              "Low-Carbon Vehicles",
                              "Low-Carbon Vehicle Components"))

##Additional Data Sets--------------------------


# Clean investment Monitor Data - Check it's the latest quarter available
facilities_data_path <- 'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q2_2024/manufacturing_energy_and_industry_facility_metadata.csv'
facilities <- read.csv(facilities_data_path, skip=5)

tech_mapping <- data.frame(
  Segment = c("Manufacturing", "Manufacturing", "Manufacturing", "Manufacturing", "Manufacturing", "Manufacturing", "Manufacturing", "Manufacturing", "Energy and Industry", "Energy and Industry", "Energy and Industry", "Energy and Industry", "Energy and Industry", "Energy and Industry"),
  Technology = c("Batteries", "Solar", "Critical Minerals", "Fueling Equipment", "Zero Emission Vehicles", "Electrolyzers", "Storage", "Wind", "Hydrogen", "SAF", "Storage", "Nuclear", "Solar", "Wind"),
  tech = c("Batteries & Components", "Solar Energy Components", "Low-Carbon Minerals", "Low-Carbon Industrial Equipment", "Electric Vehicles", "Low-Carbon Industrial Equipment", "Batteries & Components", "Wind Energy Components", "Green Hydrogen", "Biofuels", "Energy Utility Systems", "Nuclear Electric Power", "Solar Electric Power", "Wind Electric Power")
)
facilities<-inner_join(facilities,tech_mapping,by=c("Segment","Technology"))

#Vitality Stats from Census
acs_5yr_22 <- getCensus(
  name = "acs/acs5",
  vars = c("B19013_001E", "B17020_001E", "B99172_001E", "C18120_003E", "C18120_002E", "B23025_001E", "B25004_001E", "B01003_001E"),
  region = "county:*",
  vintage = 2022
) %>%
  rename(
    med_house_inc = B19013_001E,
    pov_tot = B17020_001E,
    pov_family = B99172_001E,
    empl = C18120_003E,
    lab_force = C18120_002E,
    emp_21 = B23025_001E,
    vacancy = B25004_001E,
    pop = B01003_001E
  ) %>%
  mutate(
    unemp = (1 - empl / lab_force) * 100,
    pov_rate = (1 - pov_tot / pop) * 100,
    emp_pop = empl / pop * 100,
    med_inc_perc = med_house_inc / median(med_house_inc) * 100,
    geoid = str_remove_all(paste(state, county), " "),
    fips = as.numeric(geoid)
  )

# County Business Patterns
cbp_2022 <- getCensus(
  name = "cbp",
  vars = c("STATE", "COUNTY", "NAICS2017", "SECTOR", "SUBSECTOR", "INDLEVEL", "ESTAB", "EMP", "PAYANN"),
  region = "county:*",
  vintage = 2022
)

# Climate Policy
xchange <- read.csv("OneDrive - RMI/Documents/Data/US Maps etc/Policy/xchange.csv")

#County-level property values
county_prop <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/county_property_values.csv")

#Renewable Generation Potential
tech_pot_county <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/techpot_baseline_county.csv")
tech_pot_county <- tech_pot_county %>%
  mutate(Geoid=as.numeric(paste0(substr(Geography.ID,2,3),substr(Geography.ID,5,7)))) %>%
  rename(tech_gen=Technical.Generation.Potential...MWh.MWh) %>%
  group_by(Geoid) %>%
  summarize_at(vars(tech_gen),sum,na.rm=T) 

#Interstate highway through county
road_counties<-read.csv('C:/Users/LCarey/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/us_counties_major_roads.csv') 
road_counties <- road_counties %>%
  filter(RTTYP != "")

# Industrial Electricity Expenditure & Consumption out to 2050 from NREL Estimates
county_elec_cons <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/energy_consumption_expenditure_business_as_usual_county.csv")
ind_price <- county_elec_cons %>%
  filter(Year %in% c("2022","2023","2024"),
         Sector=="industrial") %>%
  mutate(FIPS=as.numeric(paste0(substr(Geography.ID,2,3),substr(Geography.ID,5,7)))) %>%
  group_by(FIPS) %>%
  summarize_at(vars(Consumption.MMBtu,Expenditure.US.Dollars),sum,na.rm=T) %>%
  mutate(price=Expenditure.US.Dollars/Consumption.MMBtu) 


#CNBC Business rankings
cnbc <- read.csv("C:/Users/LCarey/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/cnbc_bus_rankings.csv")
colnames(cnbc)[1]<-"cnbc_rank"
colnames(cnbc)[2]<-"state"
cnbc<-cnbc %>%
  rename(workforce=WORK..FORCE.,
         infrastructure=INFRA..STRUCTURE.,
         business_cost=COST.OF.DOING.BUSINESS.,
         economy=ECONOMY.,
         quality_life=LIFE..HEALTH...INCLUSION.,
         education=EDUCATION.,
         technology=TECHN...INNOVATION.,
         business_friendliness=BUSINESS.FRIENDLI..NESS.,
         access_capital=ACCESS.TO.CAPITAL.,
         cost_living=COST.OF.LIVING.)

#Population Density
popdens <- st_read("OneDrive - RMI/Documents/Data/US Maps etc/Shapefiles/US Counties Population and Density/County.shp")

popdens <- popdens %>%
  as.data.frame() %>%
  select(GEOID,B01001_cal)

#Politics
pres <- read.csv("OneDrive - RMI/Documents/Data/US Maps etc/Politics/Presidential_County/countypres_2000-2020.csv")
pres_2020<-pres %>%
  spread(party,candidatevotes) %>%  
  group_by(year,office,state_po,county_fips) %>%
  summarize_at(vars(DEMOCRAT,REPUBLICAN),sum,na.rm=T) %>%
  mutate(demshare=DEMOCRAT/(DEMOCRAT+REPUBLICAN)) %>%
  filter(year==2020,
         grepl("PRESIDENT",office)) %>%
  ungroup()
pres_2020_state<-pres %>%
  spread(party,candidatevotes) %>%  
  group_by(year,office,state_po) %>%
  summarize_at(vars(DEMOCRAT,REPUBLICAN),sum,na.rm=T) %>%
  mutate(demshare_state=DEMOCRAT/(DEMOCRAT+REPUBLICAN)) %>%
  filter(year==2020,
         grepl("PRESIDENT",office)) %>%
  ungroup() %>%
  left_join(states_simple,by=c("state_po"="abbr"))

#State Level Economic Development Incentives
gjf<- read.csv("OneDrive - RMI/Regional Investment Strategies/Great Lakes Investment Strategy/Great Lakes Overview/Econ Development/gjf_complete.csv")

gjf_statetotal_18_23<-gjf %>%
  filter(Year>2017) %>%
  group_by(region,Location) %>%
  summarize_at(vars(subs_m),sum,na.rm=T) %>%
  arrange(desc(subs_m)) %>%
  ungroup() %>%
  inner_join(state_gdp, by=c("Location"="GeoName")) %>%
  mutate(incent_gdp = subs_m/X2022)

#CHIPS Act Subsidies
url <- 'https://www.whitehouse.gov/wp-content/uploads/2023/11/Invest.gov_PublicInvestments_Map_Data_CURRENT.xlsx'
temp_file <- tempfile(fileext = ".xlsx")
GET(url = url, write_disk(temp_file, overwrite = TRUE))
fed_inv <- read_excel(temp_file, sheet = 4)  # 'sheet = 1' to read the first sheet
chips<-fed_inv %>% 
  filter(`Funding Source` %in% c("CHIPS")) %>%
  mutate(`Funding Amount` = as.numeric(`Funding Amount Excluding Loans`)) %>%
  left_join(states_simple %>% select(fips,full),by=c("State"="full")) %>%
  left_join(counties %>%
              mutate(as.data.frame(.),
                     STATEFP=as.numeric(STATEFP)) %>%
              select(NAME,STATEFP,GEOID),by=c("County"="NAME","fips"="STATEFP")) %>%
  group_by(State,fips,County,GEOID) %>%
  summarize(chips_funds=sum(`Funding Amount`,na.rm=T))
doe_fed<-fed_inv %>%
  filter(!is.na(County),
         Subcategory=="Clean Energy and Power",
         `Agency Name`=="Department of Energy") %>%
  mutate(`Funding Amount` = as.numeric(`Funding Amount Excluding Loans`)) %>%
  left_join(states_simple %>% select(fips,full),by=c("State"="full")) %>%
  left_join(counties %>%
              mutate(as.data.frame(.),
                     STATEFP=as.numeric(STATEFP)) %>%
              select(NAME,STATEFP,GEOID),by=c("County"="NAME","fips"="STATEFP")) %>%
  group_by(State,fips,County,GEOID) %>%
  summarize(doe_funds=sum(`Funding Amount`,na.rm=T))
doe_batt_supply_chain<-fed_inv %>%
  filter(!is.na(County),
         Subcategory=="Clean Energy and Power",
         `Agency Name`=="Department of Energy",
         `Program Name` %in% c("Battery Materials Processing Grants",
                               "Advanced Energy Manufacturing and Recycling Grants",
                               "Advanced Technology Vehicle Manufacturing Loan Program",
                               "Battery and Critical Mineral Recycling",
                               "Critical Material Supply Chain Research Facility",
                               "Critical Material Innovation, Efficiency, And Alternatives",
                               "Battery Manufacturing and Recycling Grants")) %>%
  rowwise() %>%
  mutate(`Funding Amount` = sum(as.numeric(`Funding Amount Excluding Loans`), 
                                as.numeric(`Loan Amount`), na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(states_simple %>% select(fips,full),by=c("State"="full")) %>%
  left_join(counties %>%
              mutate(as.data.frame(.),
                     STATEFP=as.numeric(STATEFP)) %>%
              select(NAME,STATEFP,GEOID),by=c("County"="NAME","fips"="STATEFP")) %>%
  group_by(State,fips,County,GEOID) %>%
  summarize(doe_batt=sum(`Funding Amount`,na.rm=T))
  
#Natural Gas Pipeline
pipeline_counties<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/natural_gas_pipeline_counties.csv') 
#CO2 Pipeline
co2_pipeline_counties<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/co2_pipeline_counties.csv') 
#CO2 Storage Potential
co2_storage_counties<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/co2_storage_counties.csv') 


#County-Level DataFrame w/ All Variables---------------------------

feas_simple <-  cgt_county %>%
  left_join(county_prop %>%
    select(-NAME),
    by=c("county"="GEOID")) %>%
  #County Business Patterns
  left_join(cbp_2022 %>%
              filter(INDLEVEL == "2",
                     SECTOR == "00") %>%
              mutate(worker_pay = PAYANN / EMP,
                     GEOID = as.numeric(paste0(STATE, COUNTY))) %>%
              select(GEOID, worker_pay),
            by = c("county" = "GEOID")) %>%
  #Vitality Statistics
  left_join(acs_5yr_22 %>%
              select(fips, pov_rate, emp_pop, med_house_inc),
            by = c("county" = "fips")) %>%
  mutate(road=ifelse(county %in% road_counties$GEOID,1,0)) %>%
  left_join(cnbc,by=c("state_name"="state")) %>%
  #population density
  left_join(popdens %>% 
              mutate(county=as.numeric(GEOID),
                     popdens=B01001_cal) %>%
              select(county,popdens),by=c("county"="county")) %>%
  #Economic Development Incentives
  left_join(gjf_statetotal_18_23 %>% select(Location,incent_gdp),
            by=c("state_name"="Location")) %>%
  #Politics
  left_join(pres_2020 %>%
              select(county_fips,demshare),by=c("county"="county_fips")) %>%
  left_join(pres_2020_state %>%
              select(full,demshare_state),by=c("state_name"="full")) %>%
  #Xchange Climate policy data
  left_join(xchange %>%
              filter(Policy %in% c("clean_electricity_policy_index",
                                   "clean_ind_policy_index",
                                   "All")) %>%
              select(State,Policy,value) %>%
              pivot_wider(names_from=Policy,values_from=value),by=c("state_name"="State"))%>%
  #Industrial Electricity Price by County
  left_join(ind_price %>% select(FIPS,price),by=c("county"="FIPS")) %>%
  #County GDP
  left_join(county_gdp %>%
              mutate(county_gdp=as.numeric(X2022))%>%
              select(fips,county_gdp),by=c("county"="fips")) %>%
  #CHIPS Funding
  left_join(chips %>% select(GEOID,chips_funds)%>%
              mutate(GEOID=as.numeric(GEOID)),
            by=c("county"="GEOID")) %>%
  #DOE Funding
  left_join(doe_fed %>%
              select(GEOID,doe_funds) %>%
              mutate(GEOID=as.numeric(GEOID)),
            by=c("county"="GEOID")) %>%
  #DOE Battery Funds 
  left_join(doe_batt_supply_chain %>%
              select(GEOID,doe_batt)%>%
              mutate(GEOID=as.numeric(GEOID)),
            by=c("county"="GEOID")) %>%
  #Technical Generation Potential
  left_join(tech_pot_county,by=c("county"="Geoid"))%>%
  #Pipelines
  mutate(ng_pipeline=ifelse(county %in% pipeline_counties$GEOID,1,0)) %>%
  mutate(co2_pipeline=ifelse(county %in% co2_pipeline_counties$GEOID,1,0)) %>%
  left_join(co2_storage_counties %>%
              select(GEOID,CSI_mean),by=c("county"="GEOID")) %>%
  #H2 Hubs
  left_join(states_simple %>% select(abbr,full),by=c("state_name"="full")) %>%
  mutate(h2hub = ifelse(abbr %in% c("PA",
                                    "WV",
                                    "OH",
                                    "CA",
                                    "TX",
                                    "MN",
                                    "SD",
                                    "ND",
                                    "DE",
                                    "NJ",
                                    "IL",
                                    "IN",
                                    "MI",
                                    "WA",
                                    "OR",
                                    "MT"),1,0)) %>%
  select(-abbr)%>%
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), 0, .))) %>%
  ungroup() %>%
  select(-share_good_jobs,
         -coi,
         -cog,
         -M,
         -aggregation_level,
         -aggregation_level_desc,
         -diversity,
         -st,
         -State.x,
         -fips.x,
         -County.x,
         -State.y,
         -fips.y,
         -County.y,
         -State,
         -fips,
         -County,
         -transition_sector_category,
         -transition_subsector_category,
         -msa,
         -ea,
         -primary_transition_products_technologies) %>%
  left_join(facilities %>% 
              mutate(Year=substr(Announcement_Date,1,4))%>%
              group_by(county_2020_geoid,tech,Year) %>%
              summarize_at(vars(Total_Facility_CAPEX_Estimated),sum,na.rm=T),
            by=c("county"="county_2020_geoid","industry_desc"="tech")) %>%
  mutate(Total_Facility_CAPEX_Estimated=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated),
         Investment_Flag=ifelse(Total_Facility_CAPEX_Estimated > 0, 1, 0))
# Define the path to your GitHub repository's folder
github_folder <- "OneDrive - RMI/Documents/GitHub/Clean_Econ_Dev/Data"

# Write the CSV to that folder
write.csv(feas_simple, file = file.path(github_folder, "county_vars.csv"), row.names = FALSE)

#3 EV Supply Chain Feasibility-----------------------
#Understanding Correlation 
cgt_ev<-cgt_county %>%
  select(county,county_name,industry_desc,aggregation_level,density)%>%
  filter(aggregation_level=="4") %>%
  left_join(facilities %>% 
              filter(Segment=="Manufacturing",
                     Technology %in% c("Zero Emission Vehicles")) %>%
              select(county_2020_geoid,Total_Facility_CAPEX_Estimated),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated))%>%
  group_by(industry_desc) %>%
  summarize(correlation = cor(density, Total_Facility_CAPEX_Estimated, use = "complete.obs")) %>%
  arrange(desc(correlation))

#Putting it all together
feas_simple_ev <- feas_simple %>%
  filter(industry_desc %in% c("Electric Vehicle Components",
                              "Electric Vehicles")) %>%
  group_by(state_name,ea_name,county,county_name) %>%
  summarize(across(where(is.numeric), ~ mean(., na.rm = TRUE))) %>%
  ungroup() %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct() 
  

# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_ev_model <- feas_simple_ev %>%
  mutate(across(density:Total_Facility_CAPEX_Estimated, ~ scale(., center = TRUE, scale = TRUE)))

#Binomial Regression
model <- glm(Investment_Flag ~ density + 
               rca+
               incent_gdp+
               PropertyValueUSD +
               demshare+
               demshare_state+
               popdens+
               county_gdp+
               worker_pay+
               price+
               road+
               cnbc_rank+
               med_house_inc+
              doe_funds+
               doe_batt
               , 
               family=binomial,
               data = feas_simple_ev_model)
summary(model)
exp(coef(model))

#OLS Regression
model <- lm(Total_Facility_CAPEX_Estimated ~ density + 
               rca+
               incent_gdp+
               PropertyValueUSD +
               demshare+
               demshare_state+
               #Total_Facility_CAPEX_Estimated.x+
               worker_pay+
               #ind_elec_price+
               #price+
               road+
               #demshare+
               #workforce+
               infrastructure
             #business_cost+
             #economy+
             #cnbc_rank.x
             #pov_rate+
             #emp_pop+
             #med_house_inc+
             #gdp_17_22
             , 
             data = feas_simple_ev_model)
summary(model)

#Index
feas_simple_ev_index<-feas_simple_ev %>%
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), mean(., na.rm = TRUE), .))) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  mutate(ev_supply_chain_feas = 
           1.8*coalesce(density, 0) +
           1.2*coalesce(rca, 0) +
           1.1*coalesce(county_gdp, 0) +
           #0.1*coalesce(eci.x, 0) +
           #0.05*coalesce(Total_Facility_CAPEX_Estimated, 0) +
           0.64*coalesce(1 - incent_gdp_rank, 0) +
           0.22*coalesce(1 - PropertyValueUSD , 0) +
           1.4*coalesce(worker_pay, 0) +
           #0.025*coalesce(1 - ind_elec_price, 0) +
           #0.15*coalesce(1 - state_effective_tax_rate, 0) +
           2.2*coalesce(road, 0) +
           0.51*coalesce(1 - infrastructure, 0)) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  #select(state_name,ea,ea_name,county_name,density,ev_supply_chain_feas) %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct()



write.csv(feas_simple_ev_index %>%
            #mutate(ev_clus=ifelse(Total_Facility_CAPEX_Estimated>0,1,0)) %>%
            select(state_name,ea,ea_name,county,county_name,density,ev_supply_chain_feas),'C:/Users/LCarey/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/feas_simple_ev.csv',row.names=FALSE)

#compare with facilities data
feas_simple_ev_bins <-feas_simple_ev %>%
  left_join(feas_simple_ev_index %>%
              select(county,ev_supply_chain_feas),by=c("county"="county")) %>%
  mutate(density_bin=ntile(density,n=10),
         ev_supply_chain_feas_bin=ntile(ev_supply_chain_feas,n=10)) %>%
  left_join(facilities %>% filter(Segment=="Manufacturing",
                                  Technology %in% c("Batteries",
                                                    "Zero Emission Vehicles")),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated.y),0,Total_Facility_CAPEX_Estimated.y)) 

feas_density_bins <- feas_simple_ev_bins %>%
  group_by(density_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y,na.rm=T)*100)
write.csv(feas_density_bins %>% select(-share),'C:/Users/LCarey.RMI/Downloads/feas_density_bins.csv',row.names=FALSE)

feas_evfeas_bins <- feas_simple_ev_bins %>%
  group_by(ev_supply_chain_feas_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y,na.rm=T)*100)

ggplot(data=feas_evfeas_bins, aes(x=ev_supply_chain_feas_bin, y=Total_Facility_CAPEX_Estimated.y)) +
  geom_col() +
  theme_minimal()


#State Average
feas_simple_ev_state <- feas_simple_ev_index %>%
              select(state_name,county, county_gdp,ev_supply_chain_feas) %>%
  group_by(state_name) %>%
  # Handle cases where county_gdp might have NAs
  summarize(ev_supply_chain_feas_weighted = weighted.mean(ev_supply_chain_feas, county_gdp, na.rm = TRUE))



#4 Batteries Manufacturing Readiness----------
cgt_batt<-cgt_county %>%
  select(county,county_name,industry_desc,aggregation_level,density)%>%
  filter(aggregation_level=="4") %>%
  left_join(facilities %>% 
              filter(Segment=="Manufacturing",
                     Technology %in% c("Batteries")) %>%
              select(county_2020_geoid,Total_Facility_CAPEX_Estimated),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated))%>%
  group_by(industry_desc) %>%
  summarize(correlation = cor(density, Total_Facility_CAPEX_Estimated, use = "complete.obs")) %>%
  arrange(desc(correlation))

battery_facilities_ea<-facilities %>%
  filter(Segment=="Manufacturing",
         Technology %in% c("Batteries")) %>%
  left_join(EAs,by=c("county_2020_geoid"="fips")) %>%
  group_by(`EA Name`) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated),sum,na.rm=T)

feas_simple_batt <- feas_simple %>%
  filter(industry_desc %in% c("Batteries & Components"))

# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_batt_model <- feas_simple_batt %>%
  select(-state_name,-msa_name,-ea_name,-Segment,-Technology) %>%
  mutate(across(density:Total_Facility_CAPEX_Estimated, ~ scale(., center = TRUE, scale = TRUE)))

#Binomial Regression
model <- glm(Investment_Flag ~ density + 
               rca+
               incent_gdp+
               #eci+
               county_gdp+
               PropertyValueUSD +
               worker_pay+
               demshare_state+
               demshare+
               price+
               road+
               All+
               infrastructure+
               #business_cost+
               #economy+
               cnbc_rank+
               doe_batt+
               doe_funds
               #pov_rate+
               #emp_pop+
               #med_house_inc
               #gdp_17_22
             , 
             family=binomial,
             data = feas_simple_batt_model)
summary(model)
exp(coef(model))

#Create the Readiness Index
feas_simple_batt_index<-feas_simple_batt %>%
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), mean(., na.rm = TRUE), .))) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  mutate(batt_feas = 
           1.5*coalesce(density, 0) +
           1*coalesce(rca, 0) +
           2.1*coalesce(eci.x, 0) +
           1.2*coalesce(county_gdp, 0) +
           1.7*coalesce(Total_Facility_CAPEX_Estimated.x, 0) +
           0.7*coalesce(1 - incent_gdp_rank, 0) +
           0.33*coalesce(1 - PropertyValueUSD , 0) +
           1.2*coalesce(worker_pay, 0) +
           #0.8*coalesce(1 - price, 0) +
           #0.15*coalesce(1 - state_effective_tax_rate, 0) +
           2.2*coalesce(road, 0) 
           #1.3*gdp_17_22+
           #0.6*coalesce(1 - infrastructure, 0)
          )%>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  #select(state_name,ea,ea_name,county_name,density,ev_supply_chain_feas) %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct()

write.csv(feas_simple_batt_index %>%
            select(state_name,ea,ea_name,county,county_name,density,
                   batt_feas,
                   rca,
                   Total_Facility_CAPEX_Estimated.x,
                   incent_gdp_rank,
                   PropertyValueUSD ,
                   worker_pay,
                   price,
                   road,
                   gdp_17_22,
                   cnbc_rank),
          'C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/feas_simple_batt.csv',row.names=FALSE)

#compare with facilities data
feas_simple_batt_bins <-feas_simple_batt %>%
  left_join(feas_simple_batt_index %>%
              select(county,batt_feas),by=c("county"="county")) %>%
  mutate(density_bin=ntile(density,n=10),
         batt_feas_bin=ntile(batt_feas,n=10))

feas_density_bins <- feas_simple_batt_bins %>%
  group_by(density_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y,na.rm=T))
write.csv(feas_density_bins %>% select(-share),'C:/Users/LCarey.RMI/Downloads/feas_density_bins_batt.csv',row.names=FALSE)
feas_battfeas_bins <- feas_simple_batt_bins %>%
  group_by(batt_feas_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y,na.rm=T))

#5 Solar PV Manufacturing Readiness-------------
#Solar Man & Industry density correlation
cgt_solarman<-cgt_county %>%
  select(county,county_name,industry_code,industry_desc,aggregation_level,density,rca)%>%
  filter(aggregation_level=="4") %>%
  left_join(facilities %>% 
              filter(Segment=="Manufacturing",
                     Technology %in% c("Solar")) %>%
              select(county_2020_geoid,Total_Facility_CAPEX_Estimated),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated)) %>%
  group_by(industry_code,industry_desc) %>%
  summarize(correlation = cor(rca, Total_Facility_CAPEX_Estimated, use = "complete.obs")) %>%
  arrange(desc(correlation))


feas_simple_solar <- feas_simple %>%
  filter(industry_desc %in% c("Solar Energy Components")) %>%
  left_join(cgt_county %>%
              filter(industry_code=="327211") %>%
              mutate(density_fgm=density,
                     rca_fgm=rca) %>%
              select(county,density_fgm,rca_fgm),by="county") %>%
  group_by(state_name,ea,ea_name,county,county_name) %>%
  summarize(across(where(is.numeric), ~ mean(., na.rm = TRUE))) %>%
  ungroup() %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct() %>%
  left_join(facilities %>% 
              filter(Segment=="Manufacturing",
                     Technology %in% c("Solar")) %>%
              select(county_2020_geoid,Total_Facility_CAPEX_Estimated),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated),
         Investment_Flag=ifelse(Total_Facility_CAPEX_Estimated > 0, 1, 0))

# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_solar_model <- feas_simple_solar %>%
  mutate(across(density:Total_Facility_CAPEX_Estimated, ~ scale(., center = TRUE, scale = TRUE)))

#Binomial Regression
model <- glm(Investment_Flag ~ 
               density + 
               #rca+
               #density_fgm+
               rca_fgm+
               incent_gdp_rank+
               PropertyValueUSD +
               eci.x+
               county_gdp+
               worker_pay+
               #Total_Facility_CAPEX_Estimated.x+
               #demshare_state+
               #demshare+
               #right_to_work+
               #ind_elec_price+
               price+
               #state_effective_tax_rate+
               #road+
               #chips+
               #All+
               clean_electricity_policy_index+
               #workforce+
               infrastructure
               #business_cost+
               #economy
               #cnbc_rank.x
             #pov_rate+
             #emp_pop+
             #med_house_inc+
             #gdp_17_22
             , 
             family=binomial,
             data = feas_simple_solar_model)
summary(model)
exp(coef(model))

#Create the Readiness Index
feas_simple_solar_index<-feas_simple_solar %>%
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), mean(., na.rm = TRUE), .))) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  mutate(solar_feas = 
           #1.5*coalesce(density, 0) +
           1.2*coalesce(rca_fgm, 0) +
           6*coalesce(eci.x, 0) +
           1*coalesce(county_gdp, 0) +
           #1.7*coalesce(Total_Facility_CAPEX_Estimated.x, 0) +
           #0.74*coalesce(1 - incent_gdp_rank, 0) +
           0.1*coalesce(1 - PropertyValueUSD , 0) +
           #0.7*coalesce(1 - worker_pay, 0) +
           0.45*coalesce(1 - price, 0) +
           #0.15*coalesce(1 - state_effective_tax_rate, 0) +
           #1*coalesce(road, 0) +
           #1.3*gdp_17_22+
           0.4*coalesce(1 - infrastructure, 0)) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  #select(state_name,ea,ea_name,county_name,density,ev_supply_chain_feas) %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct()

write.csv(feas_simple_solar_index %>%
            select(state_name,ea,ea_name,county,county_name,density,
                   solar_feas,
                   rca,
                   eci.x,
                   PropertyValueUSD ,
                   infrastructure),
          'C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/feas_simple_solar.csv',row.names=FALSE)

#compare with facilities data
feas_simple_solar_bins <-feas_simple_solar %>%
  left_join(feas_simple_solar_index %>%
              select(county,solar_feas),by=c("county"="county")) %>%
  mutate(density_bin=ntile(density,n=10),
         solar_feas_bin=ntile(solar_feas,n=10))

feas_density_bins <- feas_simple_solar_bins %>%
  group_by(density_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated/sum(Total_Facility_CAPEX_Estimated,na.rm=T))
write.csv(feas_density_bins %>% select(-share),'C:/Users/LCarey.RMI/Downloads/feas_density_bins_batt.csv',row.names=FALSE)

feas_solarfeas_bins <- feas_simple_solar_bins %>%
  group_by(solar_feas_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated/sum(Total_Facility_CAPEX_Estimated,na.rm=T))

#5 Hydrogen Readiness-------------
cgt_h2<-cgt_county %>%
  select(county,county_name,industry_code,industry_desc,aggregation_level,density,rca)%>%
  filter(aggregation_level=="4") %>%
  left_join(facilities %>% 
              filter(Technology %in% c("Hydrogen")) %>%
              select(county_2020_geoid,Total_Facility_CAPEX_Estimated),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated))%>%
  group_by(industry_code,industry_desc) %>%
  summarize(correlation = cor(rca, Total_Facility_CAPEX_Estimated, use = "complete.obs")) %>%
  arrange(desc(correlation))

hydrogen_facilities_ea<-facilities %>%
  filter(Technology %in% c("Hydrogen"),
         !is.na(county_2020_geoid)) %>%
  left_join(EAs,by=c("county_2020_geoid"="fips")) %>%
  group_by(`EA Name`) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated),sum,na.rm=T)

weights <- c(
  "325120" = 0.3,  # Industrial Gas Manufacturing
  "325311" = 0.1,  # Nitrogenous Fertilizer Manufacturing
  "325199" = 0.1,  # All Other Basic Organic Chemical Manufacturing (Methanol)
  "331110" = 0.1,  # Iron and Steel Mills
  "331313" = 0.1,  # Alumina Refining and Primary Aluminum Production
  "483111" = 0.05,  # Deep Sea Freight Transportation
  "483211" = 0.05,  # Inland Water Freight Transportation
  "484121" = 0.1,  # General Freight Trucking, Long-Distance, Truckload
  "486210" = 0.1   # Pipeline Transportation of Natural Gas
)


#Put it all together for Hydrogen
feas_simple_h2 <- cgt_county %>%
  #Feasibility by county for production and hydrogen uses
  filter(industry_code %in% c("325120", #Industrial Gas Manufacturing
                              "325110", #Petrochemical Manufacturing
                              "486990", #All Other Pipeline Transportation
                              "333132", #Oil and Gas Field Machinery and Equipment Manufacturing
                              "325311", #Nitrogenous Fertilizer Manufacturing
                              "325312", #Phosphatic Fertilizer Manufacturing
                              "325199", #All Other Basic Organic Chemical Manufacturing (Methanol)
                              "331110", #Iron and Steel Mills
                              "331313", #Alumina Refining and Primary Aluminum Production
                              "483111", #Deep Sea Freight Transportation
                              "483211", #Inland Water Freight Transportation
                              "483113", #Coastal and Great Lakes Freight Transportation
                              "484121", #General Freight Trucking, Long-Distance, Truckload
                              "486210" #Pipeline Transportation of Natural Gas
  )) %>%
  filter(rca != 0) %>%  #Remove counties with no relevant industry
  group_by(state_name,ea,ea_name,county,county_name) %>%
  summarize(across(where(is.numeric), 
                   mean, na.rm = TRUE))  %>%
  ungroup() %>%
    #Add Hydrogen investment data from CIM
  left_join(hydrogen_facilities_ea,by=c("ea_name"="EA Name")) %>%
  distinct() %>%
   %>%
  left_join(facilities 
            %>% filter(Technology %in% c("Hydrogen")) %>%
              select(county_2020_geoid,Total_Facility_CAPEX_Estimated),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated.y),0,Total_Facility_CAPEX_Estimated.y)) %>%
  mutate(Investment_Flag = ifelse(Total_Facility_CAPEX_Estimated.y > 0, 1, 0))


# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_h2_model <- feas_simple_h2 %>%
  #select(-inv_description,-right_to_work) %>%
  mutate(across(density:Total_Facility_CAPEX_Estimated.y, ~ scale(., center = TRUE, scale = TRUE)))


#Regression model for Hydrogen Feasibility
model <- glm(Investment_Flag ~ density + 
               rca+
               tech_gen+
               eci+
               #Total_Facility_CAPEX_Estimated.x+
               #demshare+
               #demshare_state+
               #BATHPR+
               #ren_cagr_20_23+
               worker_pay+
               price+
               #ind_elec_price+
               #clean_electricity_policy_index+
               ng_pipeline+
               co2_pipeline+
               CSI_mean+
               #h2hub+
               clean_ind_policy_index
               #infrastructure+
               #economy+
               #workforce+
               #demshare+
               #cnbc_rank.x
             , 
             family=binomial,
             data = feas_simple_h2_model)
summary(model)
exp(coef(model))

#Create the Index using coefficients
h2_feas_index<-feas_simple_h2 %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  mutate(h2_feas = 
           2*coalesce(density, 0) +
           1*coalesce(rca, 0) +
           1.2*coalesce(tech_gen,0)+
           2.5*coalesce(eci, 0) +
           #1.7*coalesce(Total_Facility_CAPEX_Estimated.x, 0) +
           #0.1*coalesce(BATHPR, 0) +
           #0.05*coalesce(1 - BANBC2E, 0) +
           #0.7*coalesce(ren_cagr_20_23, 0) +
           #0.025*coalesce(1 - incent_gdp_rank, 0) +
           #0.05*coalesce(1 - PropertyValueUSD , 0) +
           0.3*coalesce(1 - price, 0) +
           #0.025*coalesce(1 - state_effective_tax_rate, 0) +
           #0.6*coalesce(clean_electricity_policy_index, 0) +
           1.6*coalesce(clean_ind_policy_index, 0) +
           #1*co2_pipeline+
           2.4*ng_pipeline+
           1.3*CSI_mean
           #0.7*h2hub) %>%
           #1.1*coalesce(1 - cnbc_rank.x, 0)
           ) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  select(state_name,ea,ea_name,county_name,density,h2_feas,rca,tech_gen,clean_ind_policy_index,co2_pipeline,ng_pipeline,CSI_mean,h2hub) %>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct()

write.csv(h2_feas_index,'C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/feas_simple_h2.csv',row.names=FALSE)


#Checking against investment data
feas_simple_h2_bins <-feas_simple_h2 %>%
  left_join(h2_feas_index %>%
              select(county.y,h2_feas),by=c("county"="county.y")) %>%
  mutate(density_bin=ntile(density,n=10),
         h2_feas_bin=ntile(h2_feas,n=10)) %>%
  left_join(facilities %>% filter(Technology %in% c("Hydrogen")),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated)) 

feas_density_bins <- feas_simple_h2_bins %>%
  group_by(density_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y)*100)
write.csv(feas_density_bins %>% select(-share),'C:/Users/LCarey.RMI/Downloads/feas_density_bins_h2.csv',row.names=FALSE)

feas_h2feas_bins <- feas_simple_h2_bins %>%
  group_by(h2_feas_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y)*100)

#All Cleantech Manufacturing

#6 Solar Readiness-------------


#Utility Climate Goals
crt<-read_excel("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/SEPA+Utility+Carbon-Reduction+Tracker+Dataset.xlsx",2)

crt <- crt %>%
  mutate(
    `Attainment Year` = as.numeric(`Attainment Year`),
    index_year = case_when(
      `Attainment Year` > 2050 ~ 2,
      `Attainment Year` > 2040 ~ 3,
      `Attainment Year` > 2030 ~ 4,
      `Attainment Year` > 2020 ~ 5,
      TRUE ~ 1
    ),
    index_mand = ifelse(`Mandatory or Voluntary Target` == "Mandatory", 2, 1),
    index_scope = case_when(
      grepl("Scope 3", `Target Scope`) ~ 4,
      grepl("Scope 2", `Target Scope`) ~ 3,
      grepl("Scope 1", `Target Scope`) ~ 2,
      TRUE ~ 1
    )
  ) %>%
  mutate(
    across(index_year:index_scope,
           ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE))
           )
    ) %>%
  mutate(util_index = rowSums(select(., index_year:index_scope), na.rm = TRUE)) %>%
  select(`Utility/Entity`, `State(s)*`, util_index)

# Join to find county-level data
crt_processed <- crt %>%
  mutate(utility = str_to_lower(`Utility/Entity`),
         utility = gsub("electric", "", utility),
         utility = gsub("city of", "", utility),
         utility = gsub("cooperative", "", utility),
         utility = gsub("coop", "", utility),
         utility = gsub(" co", "", utility),
         utility = gsub("utilities", "", utility),
         utility = gsub("power", "", utility),
         utility = gsub("gas &", "", utility),
         utility = gsub("&", " ", utility),
         utility = gsub("association", "", utility),
         utility = gsub(", inc.", "", utility),
         utility = gsub(", inc", "", utility),
         utility = gsub(" inc", "", utility),
         utility = gsub("department", "", utility),
         utility = gsub("corporation", "", utility),
         utility = gsub("- \\(.*?\\)", "", utility), # Remove "- ( )"
         utility = str_trim(utility),
         state = `State(s)*`) # Extract the state column

matched_processed <- matched %>%
  mutate(utility = str_to_lower(`NAME.x`),
         utility = gsub("electric", "", utility),
         utility = gsub("city of", "", utility),
         utility = gsub("cooperative", "", utility),
         utility = gsub("coop", "", utility),
         utility = gsub(" co", "", utility),
         utility = gsub("utilities", "", utility),
         utility = gsub("association", "", utility),
         utility = gsub("power", "", utility),
         utility = gsub("light &", "", utility),
         utility = gsub("gas &", "", utility),
         utility = gsub("&", " ", utility),
         utility = gsub(", inc.", "", utility),
         utility = gsub(", inc", "", utility),
         utility = gsub(" inc", "", utility),
         utility = gsub("corporation", "", utility),
         utility = gsub("department", "", utility),
         utility = gsub("- \\(.*?\\)", "", utility), # Remove "- ( )"
         utility = str_trim(utility), # Trim any leading or trailing spaces,
         state = STATE) # Extract the state column

# Use stringdist_left_join with additional condition on state
crt_match <- stringdist_left_join(crt_processed,
                                  matched_processed,
                                  by = c("utility" = "utility", "state" = "state"),
                                  method = "jw", # Jaro-Winkler distance for utility
                                  max_dist = 0.15) # Adjust max_dist as needed
na_count <- sum(is.na(crt_match$NAME.x))
print(na_count)

crt_match<-crt_match %>%
  filter(!is.na(NAME.x)) %>%
  group_by(GEOID) %>%
  summarize_at(vars(util_index),mean,na.rm=T) 

#Technical Potential
tech_pot_solar <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/techpot_baseline_county.csv")
tech_pot_solar <- tech_pot %>%
  filter(Technology=="utility_pv") %>%
  mutate(Geoid=as.numeric(paste0(substr(Geography.ID,2,3),substr(Geography.ID,5,7)))) %>%
  rename(tech_gen=Technical.Generation.Potential...MWh.MWh) %>%
  group_by(Geoid) %>%
  summarize_at(vars(tech_gen),sum,na.rm=T) %>%
  left_join(counties %>%
              mutate(GEOID=as.numeric(GEOID)) %>%
              select(GEOID,ALAND),by=c("Geoid"="GEOID")) %>%
  mutate(tech_gen=tech_gen/ALAND) %>%
  select(-geometry)
  

#EA Solar Facilities
solar_ea<-facilities %>%
  filter(Segment=="Energy and Industry",
         Technology %in% c("Solar")) %>%
  left_join(EAs,by=c("county_2020_geoid"="fips")) %>%
  group_by(`EA Name`) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated),sum,na.rm=T)


#Electricity Prices

#EIA Monthly rates
url <- 'https://www.eia.gov/electricity/data/eia861m/xls/sales_revenue.xlsx'
destination_folder<-'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/States Data/'
file_path <- paste0(destination_folder, "eia_price.xlsx")
downloaded_content <- GET(url, write_disk(file_path, overwrite = TRUE))

eia_rates <- read_excel(file_path, sheet = 1,skip=2)

eia_rates_total <- eia_rates %>%
  filter(Year=="2024") %>%
  select( State,`Cents/kWh...24`) %>%
  rename(price=`Cents/kWh...24`) %>%
  group_by(State) %>%
  summarize(price=mean(price,na.rm=T)) 


#Industrial Electricity Expenditure & Consumption out to 2050 from NREL Estimates
county_elec_cons <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/energy_consumption_expenditure_business_as_usual_county.csv")

ea_energy_price <- county_elec_cons %>%
  filter(Year %in% c("2017","2018","2019","2020","2021","2022","2023","2024")) %>%
  mutate(FIPS=as.numeric(paste0(substr(Geography.ID,2,3),substr(Geography.ID,5,7)))) %>%
  left_join(EAs,by=c("FIPS"="fips")) %>%
  group_by(`EA Name`) %>%
  summarize_at(vars(Consumption.MMBtu,Expenditure.US.Dollars),sum,na.rm=T) %>%
  mutate(price=Expenditure.US.Dollars/Consumption.MMBtu) 

feas_simple_solar <- feas_simple %>%
  filter(industry_desc %in% c("Solar Electric Power")) %>%
  left_join(solar_ea,by=c("ea_name"="EA Name")) %>%
  #select(state_name,ea,ea_name,county,county_name,rca,density,density_county_perc,density_county_rank,pci,eci.x,30:59) %>%
  #Add balancing authority generation data from EPA eGrid
  #left_join(matched_ba,by=c("ea_name"="EA Name")) %>%
  #Add Xchange policy data
  left_join(xchange %>%
              filter(Policy %in% c("clean_electricity_policy_index")) %>%
              select(State,Policy,value) %>%
              pivot_wider(names_from=Policy,values_from=value),by=c("state_name"="State")) %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  #Add Technical Generation Potential
  left_join(tech_pot_solar,by=c("county"="Geoid"))%>%
  distinct() %>%
  #Add Energy prices
  left_join(ea_energy_price,by=c("ea_name"="EA Name")) %>%
  #Add Utility Climate Targets
  left_join(crt_match %>%
              mutate(GEOID=as.numeric(GEOID)),by=c("county"="GEOID")) %>%
  left_join(facilities %>% filter(Segment=="Energy and Industry",
                                  Technology %in% c("Solar")),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated.y),0,Total_Facility_CAPEX_Estimated.y),
         Investment_Flag=ifelse(Total_Facility_CAPEX_Estimated.y > 0, 1, 0))  %>%
  distinct(state_name,ea,ea_name,county_name,density,
           rca,Total_Facility_CAPEX_Estimated.x,util_index,
           PropertyValueUSD ,tech_gen,ren_cagr_20_23,
           clean_electricity_policy_index,price,cnbc_rank,Investment_Flag)

# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_solar_model <- feas_simple_solar %>%
  mutate(across(density:cnbc_rank, ~ scale(., center = TRUE, scale = TRUE)))

# Run a logistic regression model to predict the probability of investment
model_logistic <- glm(Investment_Flag ~ density + 
                        rca +
                        Total_Facility_CAPEX_Estimated.x+
                        tech_gen +
                        PropertyValueUSD  +
                        util_index+
                       # BATHPR +
                        ren_cagr_20_23 +
                        clean_electricity_policy_index +
                        price +
                        cnbc_rank, 
                      data = feas_simple_solar_model, 
                      family = binomial)

# View the summary of the logistic regression model
summary(model_logistic)
exp(coef(model_logistic))


# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_solar_model_ols <- feas_simple_solar %>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  left_join(facilities %>% filter(Segment=="Energy and Industry",
                                  Technology %in% c("Solar")),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated)) %>%
  select(state_name,ea,ea_name,county_name,density,
         rca,Total_Facility_CAPEX_Estimated.x,
         PropertyValueUSD ,tech_gen,ren_cagr_20_23,util_index,
         clean_electricity_policy_index,price,cnbc_rank,Total_Facility_CAPEX_Estimated.y) %>%
  mutate(across(density:Total_Facility_CAPEX_Estimated.y, ~ scale(., center = TRUE, scale = TRUE)))

# Run a OLS regression model to predict the probability of investment
model_ols <- lm(Total_Facility_CAPEX_Estimated.y ~ density + 
                  #rca +
                  #Total_Facility_CAPEX_Estimated.x+
                  tech_gen +
                  #PropertyValueUSD  +
                  #BATHPR +
                  ren_cagr_20_23 +
                  util_index+
                  clean_electricity_policy_index +
                  #price +
                  cnbc_rank, 
                data = feas_simple_solar_model_ols)

# View the summary of the logistic regression model
summary(model_ols)

feas_simple_solar_index<-feas_simple_solar %>%
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), mean(., na.rm = TRUE), .))) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  #select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct() %>%
  mutate(solar_feas = 
           2*coalesce(density, 0) +
           #0.02*coalesce(rca, 0) +
           #1.277*coalesce(Total_Facility_CAPEX_Estimated.x, 0) +
           #0.04*coalesce(PropertyValueUSD , 0) +
           1.75*coalesce(tech_gen,0)+
           1.5*coalesce(util_index,0)+
           1.7*coalesce(price,0)+
           #-0.03*coalesce(BATHPR, 0) +
           #0.05*coalesce(1 - BANBC2E, 0) +
           #0.09*coalesce(ren_cagr_20_23, 0)+
           1.8*coalesce(clean_electricity_policy_index,0)+
           0.8*coalesce(1 - cnbc_rank, 0)
  ) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  select(state_name,ea,ea_name,county_name,density,solar_feas,rca,Total_Facility_CAPEX_Estimated.x,PropertyValueUSD ,tech_gen,ren_cagr_20_23,clean_electricity_policy_index,price,cnbc_rank) %>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct()

feas_simple_solar<-feas_simple_solar %>%left_join(feas_simple_solar_index %>%
                                                    select(county,county_name,solar_feas),by=c("county_name"="county_name")) 

write.csv(feas_simple_solar,'C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/feas_simple_solar.csv',row.names=FALSE)

#Checking against investment data
feas_simple_solar_bins <-feas_simple_solar %>%
  
  left_join(facilities %>% filter(Segment=="Energy and Industry",
                                Technology %in% c("Solar")),
         by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated)) %>%
  mutate(density_bin=ntile(density,n=10),
         solar_feas_bin=ntile(solar_feas,n=10))

feas_density_bins <- feas_simple_solar_bins %>%
  group_by(density_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y,na.rm=T))

feas_solarfeas_bins <- feas_simple_solar_bins %>%
  group_by(solar_feas_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y,na.rm=T))


#7 Wind Readiness-------------

#Technical Potential
tech_pot <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/techpot_baseline_county.csv")
tech_pot_wind <- tech_pot %>%
  filter(Technology=="land_based_wind") %>%
  mutate(Geoid=as.numeric(paste0(substr(Geography.ID,2,3),substr(Geography.ID,5,7)))) %>%
  rename(tech_gen=Technical.Generation.Potential...MWh.MWh) %>%
  group_by(Geoid) %>%
  summarize_at(vars(tech_gen),sum,na.rm=T) %>%
  left_join(counties %>%
              mutate(GEOID=as.numeric(GEOID)) %>%
              select(GEOID,ALAND),by=c("Geoid"="GEOID")) %>%
  mutate(tech_gen=tech_gen/ALAND) 

#EA Wind Facilities
wind_ea<-facilities %>%
  filter(Segment=="Energy and Industry",
         Technology %in% c("Wind")) %>%
  left_join(EAs,by=c("county_2020_geoid"="fips")) %>%
  group_by(`EA Name`) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated),sum,na.rm=T)



feas_simple_wind <- feas_simple %>%
  filter(industry_desc %in% c("Wind Electric Power")) %>%
  left_join(wind_ea,by=c("ea_name"="EA Name")) %>%
  #select(state_name,ea,ea_name,county,county_name,rca,density,density_county_perc,density_county_rank,pci,eci.x,30:59) %>%
  #Add balancing authority generation data from EPA eGrid
  #left_join(matched_ba,by=c("ea_name"="EA Name")) %>%
  #Add Xchange policy data
  left_join(xchange %>%
              filter(Policy %in% c("clean_electricity_policy_index")) %>%
              select(State,Policy,value) %>%
              pivot_wider(names_from=Policy,values_from=value),by=c("state_name"="State")) %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  #Add Technical Generation Potential
  left_join(tech_pot_wind,by=c("county"="Geoid"))%>%
  distinct() %>%
  #Add Utility Targets 
  left_join(crt_match %>%
              mutate(GEOID=as.numeric(GEOID)),
            by=c("county"="GEOID")) %>%
  #Add Energy prices
  left_join(ea_energy_price,by=c("ea_name"="EA Name")) %>%
  left_join(facilities %>% filter(Segment=="Energy and Industry",
                                  Technology %in% c("Wind"),
                                  Subcategory=="Onshore Wind Turbine"),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated.y),0,Total_Facility_CAPEX_Estimated.y),
         Investment_Flag=ifelse(Total_Facility_CAPEX_Estimated.y > 0, 1, 0))  %>%
  distinct(state_name,ea,ea_name,county_name,density,
           rca,Total_Facility_CAPEX_Estimated.x,util_index,
           PropertyValueUSD ,tech_gen,ren_cagr_20_23,
           clean_electricity_policy_index,price,cnbc_rank,Investment_Flag,Total_Facility_CAPEX_Estimated.y)

# Standardize columns 
feas_simple_wind_model <- feas_simple_wind %>%
  mutate(across(density:cnbc_rank, ~ scale(., center = TRUE, scale = TRUE)))

# Run a logistic regression model to predict the probability of investment
model_logistic <- glm(Investment_Flag ~ density + 
                        #rca +
                        #Total_Facility_CAPEX_Estimated.x+
                        tech_gen +
                        PropertyValueUSD  +
                        #BATHPR +
                        ren_cagr_20_23 +
                        util_index+
                        clean_electricity_policy_index +
                        price +
                        cnbc_rank, 
                      data = feas_simple_wind_model, 
                      family = binomial)

# View the summary of the logistic regression model
summary(model_logistic)
exp(coef(model_logistic))


# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_wind_model_ols <- feas_simple_wind %>%
  select(state_name,ea,ea_name,county_name,density,
         rca,Total_Facility_CAPEX_Estimated.x,util_index,
         PropertyValueUSD ,tech_gen,ren_cagr_20_23,
         clean_electricity_policy_index,price,cnbc_rank,Total_Facility_CAPEX_Estimated.y) %>%
  mutate(across(density:Total_Facility_CAPEX_Estimated.y, ~ scale(., center = TRUE, scale = TRUE)))

# Run a OLS regression model to predict the probability of investment
model_ols <- lm(Total_Facility_CAPEX_Estimated.y ~ density + 
                  rca +
                  #Total_Facility_CAPEX_Estimated.x+
                  tech_gen +
                  util_index+
                  PropertyValueUSD  +
                  #BATHPR +
                  ren_cagr_20_23 +
                  clean_electricity_policy_index +
                  price +
                  cnbc_rank, 
                data = feas_simple_solar_model_ols)

# View the summary of the logistic regression model
summary(model_ols)
exp(coef(model_logistic))

feas_simple_wind_index<-feas_simple_wind %>%
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), mean(., na.rm = TRUE), .))) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  #select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct() %>%
  mutate(wind_feas = 
           1.4*coalesce(density, 0) +
           12.3*coalesce(rca, 0) +
           #0.277*coalesce(Total_Facility_CAPEX_Estimated.x, 0) +
           0.75*coalesce(1-PropertyValueUSD , 0) +
           2*coalesce(tech_gen,0)+
           #0.04*coalesce(price,0)+
           #0.03*coalesce(BATHPR, 0) +
           #0.05*coalesce(1 - BANBC2E, 0) +
           #0.09*coalesce(ren_cagr_20_23, 0)+
           1.3*coalesce(clean_electricity_policy_index,0)+
           1.8*coalesce(util_index,0)+
           1.4*coalesce(1 - cnbc_rank, 0)
  ) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  select(state_name,ea,ea_name,county_name,density,wind_feas,rca,Total_Facility_CAPEX_Estimated.x,PropertyValueUSD ,tech_gen,util_index,ren_cagr_20_23,clean_electricity_policy_index,price,cnbc_rank) %>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct()

feas_simple_wind<-feas_simple_wind %>%left_join(feas_simple_wind_index %>%
                                                    select(county,county_name,wind_feas),by=c("county_name"="county_name")) 

write.csv(feas_simple_solar,'C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/feas_simple_solar.csv',row.names=FALSE)

#Checking against investment data
feas_simple_wind_bins <-feas_simple_wind %>%
  
  #left_join(facilities %>% filter(Segment=="Energy and Industry",
  #                              Technology %in% c("Solar")),
  #       by=c("county"="county_2020_geoid")) %>%
  #mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated)) %>%
  mutate(density_bin=ntile(density,n=10),
         wind_feas_bin=ntile(wind_feas,n=10))

feas_density_bins <- feas_simple_wind_bins %>%
  group_by(density_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y,na.rm=T))

feas_wind_bins <- feas_simple_wind_bins %>%
  group_by(wind_feas_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y,na.rm=T))


