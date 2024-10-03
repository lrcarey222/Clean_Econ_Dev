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
setwd("C:/Users/LCarey.RMI/")

#Load Latest Clean Growth Tool Data
cgt<-readRDS('OneDrive - RMI/Documents/Data/Raw Data/acre_tool_final_data_042624')

msa_data<-cgt$msa_data
naics_msa_data<-cgt$naics_msa_data
naics_data<-cgt$naics_data
states_msa<-cgt$states_msa
naics6d_data<-cgt$naics6d_data
transition<-cgt$transition_sector_data

#2.2 Combine and Clean Data---------------------

cgt_county<-read.csv('C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/CGT_county_data/cgt_county_data_08_29_2024.csv')

feas_simple <- cgt_county %>%
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
                              "Low-Carbon Vehicle Components")) %>%
  left_join(msa_data %>%
              mutate(msa=as.numeric(msa)),by=c("ea"="msa")) %>%
  select(state_name,ea,ea_name,county,county_name,industry_code,industry_desc,rca,density,density_county_perc,density_county_rank,pci,eci.x,37:65)


##3 EV Supply Chain Feasibility-----------------------
road_counties<-read.csv('C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/us_counties_major_roads.csv') 
road_counties <- road_counties %>%
  filter(RTTYP != "")

# Industrial Electricity Expenditure & Consumption out to 2050 from NREL Estimates
county_elec_cons <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/energy_consumption_expenditure_business_as_usual_county.csv")

ea_ind_price <- county_elec_cons %>%
  filter(Year %in% c("2017","2018","2019","2020","2021","2022","2023","2024"),
         Sector=="industrial") %>%
  mutate(FIPS=as.numeric(paste0(substr(Geography.ID,2,3),substr(Geography.ID,5,7)))) %>%
  left_join(EAs,by=c("FIPS"="fips")) %>%
  group_by(`EA Name`) %>%
  summarize_at(vars(Consumption.MMBtu,Expenditure.US.Dollars),sum,na.rm=T) %>%
  mutate(price=Expenditure.US.Dollars/Consumption.MMBtu) 


ev_facilities_ea<-facilities %>%
  filter(Segment=="Manufacturing",
         Technology %in% c("Zero Emission Vehicles")) %>%
  left_join(EAs,by=c("county_2020_geoid"="fips")) %>%
  group_by(`EA Name`) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated),sum,na.rm=T)

#CNBC Business rankings
cnbc <- read.csv("C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/cnbc_bus_rankings.csv")
colnames(cnbc)[1]<-"cnbc_rank"
colnames(cnbc)[2]<-"state"
cnbc<-cnbc %>%
  rename(workforce=WORK..FORCEÂ.,
         infrastructure=INFRA..STRUCTUREÂ.,
         business_cost=COST.OF.DOING.BUSINESSÂ.,
         economy=ECONOMYÂ.,
         quality_life=LIFE..HEALTH...INCLUSIONÂ.,
         education=EDUCATIONÂ.,
         technology=TECHN...INNOVATIONÂ.,
         business_friendliness=BUSINESS.FRIENDLI..NESSÂ.,
         access_capital=ACCESS.TO.CAPITALÂ.,
         cost_living=COST.OF.LIVINGÂ.)

#Politics
pres <- read.csv("C:/Users/LCarey.RMI/OneDrive - RMI/Documents/Data/US Maps etc/Politics/Presidential_County/countypres_2000-2020.csv")
pres_2020<-pres %>%
  spread(party,candidatevotes) %>%  
  group_by(year,office,county_fips) %>%
  summarize_at(vars(DEMOCRAT,REPUBLICAN),sum,na.rm=T) %>%
  mutate(demshare=DEMOCRAT/(DEMOCRAT+REPUBLICAN)) %>%
  filter(year==2020,
         office=="PRESIDENT") %>%
  ungroup()

feas_simple_ev <- feas_simple %>%
  mutate(right_to_work=ifelse(right_to_work=="Yes",1,0),
         right_to_work=ifelse(is.na(right_to_work),0,right_to_work)) %>%
  group_by(industry_desc) %>%
  filter(industry_desc %in% c("Electric Vehicle Components",
                              "Electric Vehicles",
                              "Low-Carbon Vehicles",
                              "Low-Carbon Vehicle Components")) %>%
  group_by(state_name,ea,ea_name,county,county_name) %>%
  summarize(across(where(is.numeric), ~ mean(., na.rm = TRUE))) %>%
  left_join(ev_facilities_ea,by=c("ea_name"="EA Name")) %>%
  ungroup() %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct() %>%
  mutate(road=ifelse(county %in% road_counties$GEOID,1,0)) %>%
  left_join(cnbc,by=c("state_name"="state")) %>%
  left_join(pres_2020 %>%
              select(county_fips,demshare),by=c("county"="county_fips")) %>%
  left_join(ea_ind_price %>% select(`EA Name`,price),by=c("ea_name"="EA Name")) %>%
  left_join(facilities %>% 
              filter(Segment=="Manufacturing",
                     Technology %in% c("Zero Emission Vehicles")) %>%
              select(county_2020_geoid,Total_Facility_CAPEX_Estimated),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated.y),0,Total_Facility_CAPEX_Estimated.y),
         Investment_Flag=ifelse(Total_Facility_CAPEX_Estimated.y > 0, 1, 0))

# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_ev_model <- feas_simple_ev %>%
  mutate(across(density:Total_Facility_CAPEX_Estimated.y, ~ scale(., center = TRUE, scale = TRUE)))

#Binomial Regression
model <- glm(Investment_Flag ~ density + 
               rca+
               incent_gdp_rank+
               property_value_usd+
               #eci.x+
               #Total_Facility_CAPEX_Estimated.x+
               #worker_pay_x+
               #right_to_work+
               #ind_elec_price+
               #price+
               #state_effective_tax_rate+
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
             family=binomial,
             data = feas_simple_ev_model)
summary(model)
exp(coef(model))


feas_simple_ev_index<-feas_simple_ev %>%
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), mean(., na.rm = TRUE), .))) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  mutate(ev_supply_chain_feas = 
           2.9*coalesce(density, 0) +
           1.25*coalesce(rca, 0) +
           0.1*coalesce(eci.x, 0) +
           #0.05*coalesce(Total_Facility_CAPEX_Estimated, 0) +
           0.71*coalesce(1 - incent_gdp_rank, 0) +
           0.75*coalesce(1 - property_value_usd, 0) +
           #0.025*coalesce(1 - worker_pay_x, 0) +
           #0.025*coalesce(1 - ind_elec_price, 0) +
           #0.15*coalesce(1 - state_effective_tax_rate, 0) +
           1.35*coalesce(road, 0) +
           0.6*coalesce(1 - infrastructure, 0)) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  #select(state_name,ea,ea_name,county_name,density,ev_supply_chain_feas) %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct()



write.csv(feas_simple_ev_index %>%
            #mutate(ev_clus=ifelse(Total_Facility_CAPEX_Estimated>0,1,0)) %>%
            select(state_name,ea,ea_name,county,county_name,density,ev_supply_chain_feas),'C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/feas_simple_ev.csv',row.names=FALSE)

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

feas_evfeas_bins <- feas_simple_ev_bins %>%
  group_by(ev_supply_chain_feas_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y,na.rm=T)*100)


#4 Batteries Manufacturing Readiness----------
feas_simple_batt <- feas_simple %>%
  mutate(right_to_work=ifelse(right_to_work=="Yes",1,0),
         right_to_work=ifelse(is.na(right_to_work),0,right_to_work)) %>%
  group_by(industry_desc) %>%
  filter(industry_desc %in% c("Batteries & Components")) %>%
  group_by(state_name,ea,ea_name,county,county_name) %>%
  summarize(across(where(is.numeric), ~ mean(., na.rm = TRUE))) %>%
  left_join(battery_facilities_ea,by=c("ea_name"="EA Name")) %>%
  ungroup() %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct() %>%
  mutate(road=ifelse(county %in% road_counties$GEOID,1,0)) %>%
  left_join(cnbc,by=c("state_name"="state")) %>%
  left_join(pres_2020 %>%
              select(county_fips,demshare),by=c("county"="county_fips")) %>%
  left_join(ea_ind_price %>% select(`EA Name`,price),by=c("ea_name"="EA Name")) %>%
  left_join(facilities %>% 
              filter(Segment=="Manufacturing",
                     Technology %in% c("Batteries")) %>%
              select(county_2020_geoid,Total_Facility_CAPEX_Estimated),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated.y),0,Total_Facility_CAPEX_Estimated.y),
         Investment_Flag=ifelse(Total_Facility_CAPEX_Estimated.y > 0, 1, 0))

# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_batt_model <- feas_simple_batt %>%
  mutate(across(density:Total_Facility_CAPEX_Estimated.y, ~ scale(., center = TRUE, scale = TRUE)))

#Binomial Regression
model <- glm(Investment_Flag ~ density + 
               rca+
               incent_gdp_rank+
               property_value_usd+
               worker_pay_x+
               Total_Facility_CAPEX_Estimated.x+
               #right_to_work+
               #ind_elec_price+
               price+
               #state_effective_tax_rate+
               road+
               #demshare+
               #workforce+
               #infrastructure+
               #business_cost+
               #economy+
               cnbc_rank.x+
               #pov_rate+
               #emp_pop+
               #med_house_inc+
               gdp_17_22
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
           2.7*coalesce(density, 0) +
           1*coalesce(rca, 0) +
           #0.1*coalesce(eci.x, 0) +
           1.7*coalesce(Total_Facility_CAPEX_Estimated.x, 0) +
           0.74*coalesce(1 - incent_gdp_rank, 0) +
           0.62*coalesce(1 - property_value_usd, 0) +
           1*coalesce(1 - worker_pay_x, 0) +
           0.8*coalesce(1 - price, 0) +
           #0.15*coalesce(1 - state_effective_tax_rate, 0) +
           1*coalesce(road, 0) +
           1.3*gdp_17_22+
           1*coalesce(1 - cnbc_rank.x, 0)) %>%
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
                   property_value_usd,
                   worker_pay_x,
                   price,
                   road,
                   gdp_17_22,
                   cnbc_rank.x),
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

feas_battfeas_bins <- feas_simple_batt_bins %>%
  group_by(batt_feas_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y,na.rm=T))

#5 Hydrogen Readiness-------------
pipeline_counties<-read.csv('C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/natural_gas_pipeline_counties.csv') 
co2_pipeline_counties<-read.csv('C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/co2_pipeline_counties.csv') 
co2_storage_counties<-read.csv('C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/co2_storage_counties.csv') 

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



#EPA eGRID Data for balancing authority generation (2022)
file_url <- 'https://www.epa.gov/system/files/documents/2024-01/egrid2022_data.xlsx'
temp_file <- tempfile(fileext = ".xlsx")
GET(url = file_url, write_disk(temp_file, overwrite = TRUE))
epa_ba22 <- read_excel(temp_file, sheet = 6,skip=1)

ba22<-epa_ba22 %>%
  mutate(BANAME = tolower(BANAME)) %>%
  select(BANAME,
         BACODE,
         GEOID,
         BATHPR
         , #BA total renewables generation percent (resource mix)
         BANBC2E, #BA annual CO2 equivalent non-baseload output emission rate (lb/MWh)
         BAGENACY,
         BAGENACN
  ) %>%
  mutate(total_gen=BAGENACY+BAGENACN) %>%
  distinct()

matched_ba<- matched %>%
  mutate(CNTRL_AREA=tolower(CNTRL_AREA)) %>%
  left_join(ba22 %>% select(BANAME,
                            BACODE,
                            GEOID,
                            BATHPR
                            , #BA total renewables generation percent (resource mix)
                            BANBC2E,
                            total_gen),by=c("CNTRL_AREA"="BANAME")) %>%
  mutate(fips=as.numeric(GEOID.y)) %>%
  left_join(EAs %>% select(`EA Name`,fips),by=c("fips"="fips")) %>%
  group_by(`EA Name`) %>%
  #weighted mean by total_gen
  summarize(across(where(is.numeric), 
                   ~ weighted.mean(., w = total_gen, na.rm = TRUE)))  

#Policy
xchange <- read.csv("C:/Users/LCarey.RMI/OneDrive - RMI/Documents/Data/US Maps etc/Policy/xchange.csv")

#Renewable Generation Potential
tech_pot_county <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/techpot_baseline_county.csv")
tech_pot_county <- tech_pot_county %>%
  mutate(Geoid=as.numeric(paste0(substr(Geography.ID,2,3),substr(Geography.ID,5,7)))) %>%
  rename(tech_gen=Technical.Generation.Potential...MWh.MWh) %>%
  group_by(Geoid) %>%
  summarize_at(vars(tech_gen),sum,na.rm=T) 



#Put it all together for Hydrogen
feas_simple_h2 <- cgt_county %>%
  #Feasibility by county for production and hydrogen uses
  filter(industry_code %in% c("325120", #Industrial Gas Manufacturing
                              "325311", #Nitrogenous Fertilizer Manufacturing
                              "325199", #All Other Basic Organic Chemical Manufacturing (Methanol)
                              "331110", #Iron and Steel Mills
                              "331313", #Alumina Refining and Primary Aluminum Production
                              "483111", #Deep Sea Freight Transportation
                              "483211", #Inland Water Freight Transportation
                              "484121", #General Freight Trucking, Long-Distance, Truckload
                              "486210" #Pipeline Transportation of Natural Gas
  )) %>%
  group_by(state_name,ea,ea_name,county,county_name) %>%
  summarize(across(where(is.numeric), 
                   ~ weighted.mean(., w = weights[as.character(industry_code)], na.rm = TRUE)))  %>%
  ungroup() %>%
  #Add MSA Data from Clean Growth Tool
  left_join(msa_data %>%
              mutate(msa=as.numeric(msa)),by=c("ea"="msa")) %>%
  select(state_name,ea,ea_name,county,county_name,rca,density,density_county_perc,density_county_rank,pci,eci.x,30:59) %>%
  #Add Hydrogen investment data from CIM
  left_join(hydrogen_facilities_ea,by=c("ea_name"="EA Name")) %>%
  #Add balancing authority generation data from EPA eGrid
  left_join(matched_ba,by=c("ea_name"="EA Name")) %>%
  #Add Xchange policy data
  left_join(xchange %>%
              filter(Policy %in% c("clean_electricity_policy_index",
                                   "clean_ind_policy_index")) %>%
              select(State,Policy,value) %>%
              pivot_wider(names_from=Policy,values_from=value),by=c("state_name"="State")) %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  #Add Technical Generation Potential
  left_join(tech_pot_county,by=c("county"="Geoid"))%>%
  distinct() %>%
  left_join(cnbc,by=c("state_name"="state")) %>%
  left_join(pres_2020 %>%
              select(county_fips,demshare),by=c("county"="county_fips")) %>%
  #Pipelines
  mutate(ng_pipeline=ifelse(county %in% pipeline_counties$GEOID,1,0)) %>%
  mutate(co2_pipeline=ifelse(county %in% co2_pipeline_counties$GEOID,1,0)) %>%
  left_join(co2_storage_counties %>%
              select(GEOID,CSI_mean),by=c("county"="GEOID")) %>%
  #H2 Hybs
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
  select(-abbr) %>%
  left_join(facilities 
            %>% filter(Technology %in% c("Hydrogen")) %>%
              select(county_2020_geoid,Total_Facility_CAPEX_Estimated),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated.y),0,Total_Facility_CAPEX_Estimated.y)) %>%
  mutate(Investment_Flag = ifelse(feas_simple_h2_bins$Total_Facility_CAPEX_Estimated.y > 0, 1, 0))


# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_h2_model <- feas_simple_h2 %>%
  select(-inv_description,-right_to_work) %>%
  mutate(across(density:Total_Facility_CAPEX_Estimated.y, ~ scale(., center = TRUE, scale = TRUE)))


#Regression model for Hydrogen Feasibility
model <- glm(Investment_Flag ~ density + 
               rca+
               tech_gen+
               Total_Facility_CAPEX_Estimated.x+
               #BATHPR+
               ren_cagr_20_23+
               #ind_elec_price+
               clean_electricity_policy_index+
               #ng_pipeline+
               co2_pipeline+
               CSI_mean+
               h2hub+
               clean_ind_policy_index+
               #demshare+
               cnbc_rank.x
             , 
             family=binomial,
             data = feas_simple_h2_model)
summary(model)
exp(coef(model))

#Create the Index using coefficients
h2_feas_index<-feas_simple_h2 %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  mutate(h2_feas = 
           2.6*coalesce(density, 0) +
           1*coalesce(rca, 0) +
           1.2*coalesce(tech_gen,0)+
           #0.025*coalesce(eci.x, 0) +
           1.7*coalesce(Total_Facility_CAPEX_Estimated.x, 0) +
           #0.1*coalesce(BATHPR, 0) +
           #0.05*coalesce(1 - BANBC2E, 0) +
           0.7*coalesce(ren_cagr_20_23, 0) +
           #0.025*coalesce(1 - incent_gdp_rank, 0) +
           #0.05*coalesce(1 - property_value_usd, 0) +
           #0.1*coalesce(1 - ind_elec_price, 0) +
           #0.025*coalesce(1 - state_effective_tax_rate, 0) +
           0.6*coalesce(clean_electricity_policy_index, 0) +
           1.5*coalesce(clean_ind_policy_index, 0) +
           0.9*co2_pipeline+
           #0.05*ng_pipeline+
           0.9*CSI_mean+
           0.8*h2hub+
           1.1*coalesce(1 - cnbc_rank.x, 0)) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  select(state_name,ea,ea_name,county_name,density,h2_feas,rca,tech_gen,Total_Facility_CAPEX_Estimated.x,BATHPR,ren_cagr_20_23,ind_elec_price,clean_electricity_policy_index,clean_ind_policy_index,co2_pipeline,ng_pipeline,CSI_mean,h2hub,cnbc_rank.x) %>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct()

write.csv(h2_feas_index,'C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/feas_simple_h2.csv',row.names=FALSE)


#Checking against investment data
feas_simple_h2_bins <-feas_simple_h2 %>%
  left_join(h2_feas_index %>%
              select(county,h2_feas),by=c("county"="county")) %>%
  mutate(density_bin=ntile(density,n=10),
         h2_feas_bin=ntile(h2_feas,n=10)) %>%
  left_join(facilities %>% filter(Technology %in% c("Hydrogen")),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated.y),0,Total_Facility_CAPEX_Estimated.y)) 

feas_density_bins <- feas_simple_h2_bins %>%
  group_by(density_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y)*100)

feas_h2feas_bins <- feas_simple_h2_bins %>%
  group_by(h2_feas_bin) %>%
  summarize_at(vars(Total_Facility_CAPEX_Estimated.y),sum,na.rm=T)%>%
  mutate(share=Total_Facility_CAPEX_Estimated.y/sum(Total_Facility_CAPEX_Estimated.y)*100)



#6 Solar Readiness-------------

#Technical Potential
tech_pot_solar <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/techpot_baseline_county.csv")
tech_pot_solar <- tech_pot_solar %>%
  filter(grepl("_pv",Technology)) %>%
  mutate(Geoid=as.numeric(paste0(substr(Geography.ID,2,3),substr(Geography.ID,5,7)))) %>%
  rename(tech_gen=Technical.Generation.Potential...MWh.MWh) %>%
  group_by(Geoid) %>%
  summarize_at(vars(tech_gen),sum,na.rm=T) 

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
  left_join(matched_ba,by=c("ea_name"="EA Name")) %>%
  #Add Xchange policy data
  left_join(xchange %>%
              filter(Policy %in% c("clean_electricity_policy_index")) %>%
              select(State,Policy,value) %>%
              pivot_wider(names_from=Policy,values_from=value),by=c("state_name"="State")) %>%
  select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  #Add Technical Generation Potential
  left_join(tech_pot_county,by=c("county"="Geoid"))%>%
  distinct() %>%
  #Add Energy prices
  left_join(ea_energy_price,by=c("ea_name"="EA Name")) %>%
  left_join(facilities %>% filter(Segment=="Energy and Industry",
                                  Technology %in% c("Solar")),
            by=c("county"="county_2020_geoid")) %>%
  mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated.y),0,Total_Facility_CAPEX_Estimated.y),
         Investment_Flag=ifelse(Total_Facility_CAPEX_Estimated.y > 0, 1, 0))  %>%
  distinct(state_name,ea,ea_name,county_name,density,
           rca,Total_Facility_CAPEX_Estimated.x,
           property_value_usd,tech_gen,BATHPR,ren_cagr_20_23,
           clean_electricity_policy_index,price,cnbc_rank,Investment_Flag)

# Standardize columns from density to Total_Facility_CAPEX_Estimated.y
feas_simple_solar_model <- feas_simple_solar %>%
  mutate(across(density:cnbc_rank, ~ scale(., center = TRUE, scale = TRUE)))

# Run a logistic regression model to predict the probability of investment
model_logistic <- glm(Investment_Flag ~ density + 
                        rca +
                        Total_Facility_CAPEX_Estimated.x+
                        tech_gen +
                        property_value_usd +
                        BATHPR +
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
  select(state_name,ea,ea_name,county_name,density,
         rca,Total_Facility_CAPEX_Estimated.x,
         property_value_usd,tech_gen,BATHPR,ren_cagr_20_23,
         clean_electricity_policy_index,price,cnbc_rank,Total_Facility_CAPEX_Estimated.y) %>%
  mutate(across(density:Total_Facility_CAPEX_Estimated.y, ~ scale(., center = TRUE, scale = TRUE)))

# Run a OLS regression model to predict the probability of investment
model_ols <- lm(Total_Facility_CAPEX_Estimated.y ~ density + 
                  rca +
                  #Total_Facility_CAPEX_Estimated.x+
                  tech_gen +
                  property_value_usd +
                  BATHPR +
                  ren_cagr_20_23 +
                  clean_electricity_policy_index +
                  price +
                  cnbc_rank, 
                data = feas_simple_solar_model_ols)

# View the summary of the logistic regression model
summary(model_ols)
exp(coef(model_logistic))

feas_simple_solar_index<-feas_simple_solar %>%
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), mean(., na.rm = TRUE), .))) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  #select(-county)%>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct() %>%
  mutate(solar_feas = 
           #1.6*coalesce(density, 0) +
           0.02*coalesce(rca, 0) +
           0.277*coalesce(Total_Facility_CAPEX_Estimated.x, 0) +
           0.04*coalesce(property_value_usd, 0) +
           0.21*coalesce(tech_gen,0)+
           -0.04*coalesce(price,0)+
           -0.03*coalesce(BATHPR, 0) +
           #0.05*coalesce(1 - BANBC2E, 0) +
           0.09*coalesce(ren_cagr_20_23, 0)+
           -0.05*coalesce(clean_electricity_policy_index,0)+
           0.09*coalesce(1 - cnbc_rank, 0)
  ) %>%
  mutate(across(where(is.numeric), ~ (. - min(., na.rm = TRUE)) / (max(., na.rm = TRUE) - min(., na.rm = TRUE)))) %>%
  select(state_name,ea,ea_name,county_name,density,solar_feas,rca,Total_Facility_CAPEX_Estimated.x,property_value_usd,tech_gen,BATHPR,ren_cagr_20_23,clean_electricity_policy_index,price,cnbc_rank) %>%
  right_join(cgt_county %>% select(county,county_name),by=c("county_name")) %>%
  distinct()

feas_simple_solar<-feas_simple_solar %>%left_join(feas_simple_solar_index %>%
                                                    select(county,county_name,solar_feas),by=c("county_name"="county_name")) 

write.csv(feas_simple_solar,'C:/Users/LCarey.RMI/OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/feas_simple_solar.csv',row.names=FALSE)

#Checking against investment data
feas_simple_solar_bins <-feas_simple_solar %>%
  
  #left_join(facilities %>% filter(Segment=="Energy and Industry",
  #                              Technology %in% c("Solar")),
  #       by=c("county"="county_2020_geoid")) %>%
  #mutate(Total_Facility_CAPEX_Estimated.y=ifelse(is.na(Total_Facility_CAPEX_Estimated),0,Total_Facility_CAPEX_Estimated)) %>%
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


