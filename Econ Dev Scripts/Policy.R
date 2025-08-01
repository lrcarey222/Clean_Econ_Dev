
#POLICY!

#Federal investments--------------
url <- 'https://www.whitehouse.gov/wp-content/uploads/2023/11/Invest.gov_PublicInvestments_Map_Data_CURRENT.xlsx'
temp_file <- tempfile(fileext = ".xlsx")
GET(url = url, write_disk(temp_file, overwrite = TRUE))
fed_inv <- read_excel(temp_file, sheet = 4)  # 'sheet = 1' to read the first sheet
write.csv(fed_inv,'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/White Houe Public Investments.csv')

#Total expenditure by program-------------------------------------
program_spend <- fed_inv %>%
  filter(Category=="Clean Energy, Buildings, and Manufacturing") %>%
  mutate(`Funding Amount` = as.numeric(`Funding Amount`)) %>%
  group_by(`Program Name`) %>%
  summarize_at(vars(`Funding Amount`),sum,na.rm=T) %>%
  ungroup() %>%
  mutate(share=round(`Funding Amount`/sum(`Funding Amount`)*100,3)) %>%
  arrange(desc(`Funding Amount`)) 

#State of Interest investment by program----------------------------------------
state_spend <- fed_inv %>%
  mutate(`Funding Amount` = as.numeric(`Funding Amount`)) %>%
  filter(Category=="Clean Energy, Buildings, and Manufacturing") %>%
  filter(State==state_name) %>%
  group_by(Subcategory,`Funding Source`,`Program Name`) %>%
  summarize_at(vars(`Funding Amount`),sum,na.rm=T) %>%
  ungroup() %>%
  mutate(share=round(`Funding Amount`/sum(`Funding Amount`)*100,3)) %>%
  arrange(desc(`Funding Amount`)) %>%
  inner_join(program_spend,by="Program Name") %>%
  mutate(state_fed_lq=share.x/share.y,
         fund_m=`Funding Amount.x`/1000000) %>%
  select(`Funding Source`,`Program Name`,Subcategory,fund_m,state_fed_lq) 
write.csv(state_spend,paste0(output_folder,"/",state_abbreviation,"_state_spend.csv"),row.names=F)

state_fedspend_plot<-ggplot(data=state_spend %>% filter(state_fed_lq>2),aes(x=reorder(`Program Name`,state_fed_lq),y=state_fed_lq,fill=Subcategory))+
  geom_col()+
  coord_flip()+
  scale_fill_manual(values=rmi_palette)+
  labs(title=paste0("Federal Investment in Clean Energy, Buildings, and Manufacturing in ",state_name),
       substitle="Federal programs where investment in New Mexico is more than double the national average share",
       x="Program",
       y="Funding relative to national average)",
       fill="Subcategory")+
  theme_classic()+
  theme(legend.position="bottom",
        plot.margin = unit(c(1, 1, 1, 1), "cm")) +  # Adjust margins as needed
  scale_y_continuous(expand = c(0, 0))+
  theme(plot.title = element_text(hjust = 1, vjust = 0.5),  # Center the plot title
        legend.position = c(0.7,0.2))

ggsave(paste0(output_folder,"/",state_abbreviation,"_state_fedspend_plot.png"),plot=state_fedspend_plot,width=8,height=6,units="in",dpi=300)

state_spend_cleantot<-state_spend %>%
  summarize(total=sum(fund_m)) 

state_spend_cap<-fed_inv %>%
  mutate(`Funding Amount` = as.numeric(`Funding Amount`)) %>%
  filter(Category=="Clean Energy, Buildings, and Manufacturing") %>%
  group_by(State) %>%
  summarize_at(vars(`Funding Amount`),sum,na.rm=T) %>%
  ungroup() %>%
  left_join(socioecon %>%
              filter(quarter=="2024-Q1") %>%
              select(StateName,population),by=c("State"="StateName")) %>%
  filter(State != "Multiple") %>%
  mutate(cap_per_capita=`Funding Amount`/population,
         state_share=`Funding Amount`/sum(`Funding Amount`)) %>%
  arrange(desc(cap_per_capita))

#Map
fed_inv_map <- read_excel(temp_file, sheet = 3)  # 'sheet = 1' to read the first sheet
state_fedinv_map <- fed_inv_map %>%
  filter(State==state_name,
         Category=="Clean Energy, Buildings, and Manufacturing") 
write.csv(state_fedinv_map,paste0(output_folder,"/",state_abbreviation,"_state_fedinv_map.csv"),row.names=F)


#BIL-----------------------------
bil<-fed_inv %>% 
  filter(`Funding Source`=="BIL") %>%
  mutate(`Funding Amount` = as.numeric(`Funding Amount Excluding Loans`)) %>%
  group_by(State) %>%
  summarize_at(vars(`Funding Amount`),sum,na.rm=T) %>%
  ungroup() %>%
  left_join(socioecon %>%
              filter(quarter=="2024-Q1") %>%
              select(StateName,real_gdp,population),by=c("State"="StateName")) %>%
  filter(!is.na(real_gdp)) %>%
  mutate(share_bil=round(`Funding Amount`/sum(`Funding Amount`)*100,3),
         share_gdp=round(real_gdp/sum(real_gdp,na.rm=T)*100,3),
         gdp_cap=real_gdp/population,
         share_pop=population/sum(population)*100,
         lq=share_bil/share_gdp,
         lq2=share_bil/share_pop)

ggplot(data=bil,aes(y=share_bil,x=share_gdp,size=`Funding Amount`))+
  geom_point()+
  geom_smooth(method="lm",se=F)+
  labs(title="BIL Federal Funding",
       subtitle="Share of BIL investment in states",
       x="Share of BIL Funding",
       y="Share of GDP")+
  theme_classic()+
  theme(plot.title = element_text(hjust = 1, vjust = 0.5),  # Center the plot title
        plot.subtitle = element_text(hjust = 1, vjust = 0.5),
        legend.position="none")+  # Center the plot subtitle
annotate("text", y = max(bil$share_bil) * 0.8, x = max(bil$share_gdp) * 0.9,
         label = cor_text3, size = 5, hjust = 1) +  # Add the correlation text
  geom_text_repel(aes(label=State), size=3)  # Add state labels with geom_text_repel

#IRA_CHIPS-----------------------------
ira_chips<-fed_inv %>% 
  filter(`Funding Source` %in% c("CHIPS")) %>%
  mutate(`Funding Amount` = as.numeric(`Funding Amount Excluding Loans`)) %>%
  group_by(State) %>%
  summarize_at(vars(`Funding Amount`),sum,na.rm=T) %>%
  ungroup() %>%
  left_join(socioecon %>%
              filter(quarter=="2024-Q1") %>%
              select(StateName,real_gdp,population),by=c("State"="StateName")) %>%
  filter(!is.na(real_gdp)) %>%
  mutate(share_irachips=round(`Funding Amount`/sum(`Funding Amount`)*100,3),
         share_gdp=round(real_gdp/sum(real_gdp,na.rm=T)*100,3),
         gdp_cap=real_gdp/population,
         share_pop=population/sum(population)*100,
         lq=share_irachips/share_gdp,
         lq2=share_irachips/share_pop)

ggplot(data=ira_chips,aes(y=share_irachips,x=share_gdp,size=`Funding Amount`))+
  geom_point()+
  geom_smooth(method="lm",se=F)+
  labs(title="CHIPS Federal Funding",
       subtitle="Share of CHIPS investment in states",
       y="Share of CHIPS Funding",
       x="Share of GDP")+
  theme_classic()+
  theme(plot.title = element_text(hjust = 1, vjust = 0.5),  # Center the plot title
        plot.subtitle = element_text(hjust = 1, vjust = 0.5),
        legend.position="none")+  # Center the plot subtitle
  annotate("text", y = max(ira_chips$share_irachips) * 0.8, x = max(ira_chips$share_gdp) * 0.9,
           label = cor_text2, size = 5, hjust = 1) +  # Add the correlation text
  geom_text_repel(aes(label=State), size=3)  # Add state labels with geom_text_repel

#Estimated Federal Tax Credits (from Clean Investment Monitor)------------------


#Federal Tax Credit Incentives State-Level Estimates
tax_inv_cat<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q1_2025/extended_data/federal_actual_investment_by_category.csv',skip=5)
tax_inv_cat_tot <- tax_inv_cat%>% group_by(Segment,Category) %>%
  summarize_at(vars(Total.Federal.Investment),sum,na.rm=T)
tax_inv_state<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q3_2024/extended_CIM_data/federal_actual_investment_by_state.csv',skip=5)
tax_inv_state_tot <- tax_inv_state %>% group_by(State) %>%
  summarize_at(vars(Total.Federal.Investment),sum,na.rm=T) 


#45X
fac_45x<-facilities %>%
  filter(Decarb_Sector=="Clean Tech Manufacturing",
         Technology %in% c("Solar",
                           "Wind",
                           "Critical Minerals",
                           "Batteries"),
         Investment_Status=="O"
        )%>%
  group_by(State,Segment) %>%
  summarize_at(vars(Estimated_Total_Facility_CAPEX),sum,na.rm=T) %>%
  group_by(Segment) %>%
  mutate(cap_share=Estimated_Total_Facility_CAPEX/sum(Estimated_Total_Facility_CAPEX)) %>%
  left_join( tax_inv_cat_tot %>%
              filter(Category=="Advanced Manufacturing Tax Credits"),by=c("Segment")) %>%
  mutate(state_45x = Total.Federal.Investment*cap_share) 

#45V & 45Q
fac_45vq<-investment %>%
  filter(Segment=="Energy and Industry",
         Technology %in% c("Hydrogen")|
           Technology=="Carbon Management" & Subcategory %in% c("CCUS","Direct Air Capture")|
           Technology=="Sustainable Aviation Fuels") %>%
  group_by(State,Segment) %>%
  summarize_at(vars(Estimated_Actual_Quarterly_Expenditure),sum,na.rm=T) %>%
  group_by(Segment) %>%
  mutate(cap_share=Estimated_Actual_Quarterly_Expenditure/sum(Estimated_Actual_Quarterly_Expenditure)) %>%
  left_join(tax_inv_cat_tot %>% filter(Category=="Emerging Climate Technology Tax Credits"),by=c("Segment")) %>%
  left_join(tax_inv_state_tot %>% select(State,Total.Federal.Investment..2023.Billion.USD.),by=c("State")) %>%
  mutate(state_45vq = Total.Federal.Investment.2023USBn*cap_share) 

#45
url <- 'https://www.eia.gov/electricity/data/eia860m/xls/june_generator2024.xlsx'
destination_folder<-'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/States Data/'
file_path <- paste0(destination_folder, "eia_op_gen.xlsx")
downloaded_content <- GET(url, write_disk(file_path, overwrite = TRUE))

#Operating Generation
op_gen <- read_excel(file_path, sheet = 1,skip=2)

state_45 <- op_gen %>%
  filter(Status=="(OP) Operating",
         Technology %in% c("Onshore Wind Turbine",
                           "Solar Photovoltaic",
                           "Batteries",
                           "Solar Thermal with Energy Storage",
                           "Geothermal",
                           "Conventional Hydroelectric",
                           "Landfill Gas",
                           "Wood/Wood Waste Biomass")) %>%
  group_by(`Plant State`) %>%
  summarize_at(vars(`Nameplate Capacity (MW)`),sum,na.rm=T) %>%
  ungroup() %>%
  mutate(share_mw=`Nameplate Capacity (MW)`/sum(`Nameplate Capacity (MW)`)) %>%
  left_join(tax_inv_state_tot %>% select(State,Total.Federal.Investment..2023.Billion.USD.),by=c("Plant State"="State")) %>%
  cbind(tax_inv_cat_tot %>% filter(Category=="Clean Electricity Tax Credits")) %>%
  mutate(state_45 = Total.Federal.Investment.2023USBn*share_mw)

#48
url <- 'https://www.eia.gov/electricity/monthly/xls/table_6_01_b.xlsx'
dest_file <- tempfile(fileext = ".xlsx")
download.file(url, destfile = dest_file, mode = "wb")

data <- read_excel(dest_file)

rooftop_state<-read_excel("C:/Users/LCarey.RMI/OneDrive - RMI/Documents/Data/Raw Data/small_scale_solar_2024.xlsx",sheet=1,skip=2)
rooftop_state <- rooftop_state %>%
  rename_with(~c("res_cap",
                 "com_cap",
                 "ind_cap",
                 "total_cap",
                 "res_gen", 
                 "com_gen",
                 "ind_gen",
                 "total_gen"), .cols = 5:12) %>%
  mutate(across(c(res_cap:total_gen),as.numeric)) 

state_48 <- rooftop_state %>%
  filter(!State %in% c("US"),
         !is.na(State)) %>%
  mutate(com_gen = replace_na(com_gen, 0),
         res_gen = replace_na(res_gen, 0),
         ind_gen = replace_na(ind_gen, 0)) %>%
  select(State,ind_gen,com_gen,res_gen) %>%
  mutate(com_share=(ind_gen+com_gen)/sum((ind_gen+com_gen)),na.rm=T,
         res_share=res_gen/sum(res_gen),na.rm=T) %>%
  left_join(tax_inv_state_tot %>% select(State,Total.Federal.Investment..2023.Billion.USD.),by=c("State")) %>%
  cbind(tax_inv_cat_tot %>% filter(Category=="Non-residential Distributed Energy Tax Credits")) %>%
  left_join(tax_inv_cat_tot %>% filter(Category=="Residential Energy & Efficiency Tax Credits"),by=c("Segment")) %>%
  mutate(state_48_res = Total.Federal.Investment.2023USBn.y*(res_share)) %>%
  mutate(state_48_com=  Total.Federal.Investment.2023USBn.x*(com_share))


#zev
zev<-investment %>%
  filter(Segment=="Retail",Technology=="Zero Emission Vehicles",
         quarter %in% c("2022-Q2",
                        "2022-Q3",
                        "2022-Q4",
                        "2023-Q1",
                        "2023-Q2",
                        "2023-Q3",
                        "2023-Q4",
                        "2024-Q1")) %>%
  group_by(State,Segment) %>%
  summarize_at(vars(Estimated_Actual_Quarterly_Expenditure),sum,na.rm=T) %>%
  group_by(Segment) %>%
  mutate(share_ev=Estimated_Actual_Quarterly_Expenditure/sum(Estimated_Actual_Quarterly_Expenditure)) %>%
  left_join(tax_inv_cat_tot %>% filter(Category=="Zero Emission Vehicle Tax Credits"),by=c("Segment")) %>%
  mutate(state_zev = Total.Federal.Investment.2023USBn*share_ev)

#combine
state_estimates<-state_45 %>%
  rename(State=`Plant State`) %>%
  select(State,state_45) %>%
  left_join(fac_45x %>% select(State,state_45x),by=c("State")) %>%
  left_join(fac_45vq %>% select(State,state_45vq),by=c("State")) %>%
  left_join(state_48 %>% select(State,state_48_res,state_48_com),by=c("State")) %>%
  left_join(zev %>% select(State,state_zev),by=c("State")) %>%
  ungroup() %>%
  select(-Segment.x,-Segment.y) %>%
  mutate(across(where(is.numeric), ~replace_na(., 0))) %>%
  mutate(total=(state_45+state_45x+state_45vq+state_48_res+state_48_com+state_zev)) %>%
  left_join(tax_inv_state_tot %>% select(State,Total.Federal.Investment..2023.Billion.USD.),by=c("State")) %>%
  mutate("Clean Electricity Tax Credits"=state_45/total*Total.Federal.Investment..2023.Billion.USD.,
         "Advanced Manufacturing Tax Credits"=state_45x/total*Total.Federal.Investment..2023.Billion.USD.,
         "Emerging Climate Technology Tax Credits"=state_45vq/total*Total.Federal.Investment..2023.Billion.USD.,
         "Residential Energy & Efficiency Tax Credits"=state_48_res/total*Total.Federal.Investment..2023.Billion.USD.,
         "Non-residential Distributed Energy Tax Credits"=state_48_com/total*Total.Federal.Investment..2023.Billion.USD.,
         "Zero Emission Vehicle Tax Credits"=state_zev/total*Total.Federal.Investment..2023.Billion.USD.) %>%
  mutate(total2=`Clean Electricity Tax Credits`+
           `Advanced Manufacturing Tax Credits`+
           `Emerging Climate Technology Tax Credits`+
           `Residential Energy & Efficiency Tax Credits`+
           `Non-residential Distributed Energy Tax Credits`+
           `Zero Emission Vehicle Tax Credits`) 

ggplot(data=state_estimates,aes(x=`Advanced Manufacturing Tax Credits`,y=state_45x))+
  geom_point()+
  #log x and y axis
  #scale_x_log10()+
  #scale_y_log10()+
  geom_label(aes(label=State))+
  theme_minimal()

cat_estimate<- state_estimates %>%
  mutate(across(where(is.numeric),~sum(.)))

state_estimates2<-state_estimates %>%
  select(State,`Clean Electricity Tax Credits`,
         `Advanced Manufacturing Tax Credits`,
         `Emerging Climate Technology Tax Credits`,
         `Residential Energy & Efficiency Tax Credits`,
         `Non-residential Distributed Energy Tax Credits`,
         `Zero Emission Vehicle Tax Credits`) %>%
  pivot_longer(cols=-State,names_to="Category",values_to="Federal Investment (Billions 2023 USD)") %>%
  left_join(socioecon %>% filter(quarter=="2024-Q2"), by=c("State")) %>%
  mutate("Federal Investment per Capita"=round(`Federal Investment (Billions 2023 USD)`*1000000000/population),
         "Federal Investment (Billions 2023 USD)"=round(`Federal Investment (Billions 2023 USD)`,2),
         "State GDP (Billions 2023 USD)"=round(real_gdp/1000,2),
         "Federal Investment (% of State GDP)"=round(`Federal Investment (Billions 2023 USD)`/`State GDP (Billions 2023 USD)`*100,3)) %>%
  select(-quarter,-real_gdp,-population)
write.csv(state_estimates2,"OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/IRA_taxcredits_estimate.csv")
state_estimates2<-read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/IRA_taxcredits_estimate.csv")

#Charts
ggplot(data=state_estimates2) +
  geom_col(aes(x=reorder(State,-`Federal.Investment..Billions.2023.USD.`),y=`Federal.Investment..Billions.2023.USD.`,fill=Category),position="stack") +
  coord_flip() +
  scale_fill_manual(values=rmi_palette) +
  labs(title = "Federal IRA Investment by State, Cateogry", 
       subtitle = "",
       x="State",
       fill = "Tax Credit",
       caption="Source: Clean Investment Monitor")+
  scale_y_continuous(expand=c(0,0))+
  theme_classic()+
  theme(legend.position=c(0.8,0.8)) 

ggplot(data=state_estimates2) +
  geom_col(aes(x=reorder(State,-`Federal Investment (% of State GDP)`),y=`Federal Investment (% of State GDP)`,fill=Category),position="stack") +
  coord_flip() +
  scale_fill_manual(values=rmi_palette) +
  labs(title = "Federal IRA Investment by State, Cateogry", 
       subtitle = "Percentage of 2022 GDP",
       x="State",
       fill = "Tax Credit",
       caption="Source: Clean Investment Monitor")+
  scale_y_continuous(expand=c(0,0))+
  theme_classic()+
  theme(legend.position=c(0.8,0.8)) 


#Regional State Comparisons for Datawrapper------------------
state_estimates2<- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/IRA_taxcredits_estimate.csv")
state_estimates2<-state_estimates2 %>%
  rename("Federal Investment (% of State GDP)"=Federal.Investment....of.State.GDP.,
         "Federal Investment (Billions 2023 USD)"=Federal.Investment..Billions.2023.USD.)

division_of_interest<-census_divisions %>%
  filter(State.Code==state_abbreviation)
state_ira <- state_estimates2 %>%
  left_join(census_divisions,by=c("State"="State.Code")) %>%
  filter(Division==division_of_interest$Division,
         State != "DC") 

state_ira_dw <- state_ira %>%
  select(State,Category,`Federal Investment (% of State GDP)`) %>%
  pivot_wider(names_from=Category,values_from=`Federal Investment (% of State GDP)`) %>%
  #new column of total of all numeric columns
  mutate(total = rowSums(select(., `Clean Electricity Tax Credits`:`Zero Emission Vehicle Tax Credits`))) %>%
  arrange(desc(total)) %>%
write.csv(paste0(output_folder,"/",state_abbreviation,"_state_ira.csv"),row.names=F)

ggplot(data=state_ira) +
  geom_col(aes(x=reorder(State,-`Federal Investment (% of State GDP)`),y=`Federal Investment (% of State GDP)`,fill=Category),position="stack") +
  coord_flip() +
  scale_fill_manual(values=rmi_palette) +
  labs(title = "Federal IRA Investment by State, Cateogry", 
       subtitle = "Percentage of 2023 GDP",
       x="State",
       fill = "Tax Credit",
       caption="Source: Clean Investment Monitor")+
  scale_y_continuous(expand=c(0,0))+
  theme_classic()+
  theme(legend.position=c(0.8,0.8)) 

  
#RMI Economic Tides Analysis - June 18 Data---------------------
  # Load necessary library
library(readxl)

ira_allstates<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Sprint24/Analysis/IRA Downscaling/IRA Funding to states_ econ tides 2.0/July 16 data/Analysis/allstates_output_formatted.csv')

#Totals relative to population/gdp
colnames(ira_allstates)[6]<-"CBO National Estimate ($)"
colnames(ira_allstates)[7]<-"CBO Downscaled State Estimate ($)"
colnames(ira_allstates)[8]<-"Climate-Aligned Estimate ($)"

sum_ira_allstates<-ira_allstates %>%
  group_by(State) %>%
  summarize_at(vars(`CBO National Estimate ($)`,`CBO Downscaled State Estimate ($)`,`Climate-Aligned Estimate ($)`),sum,na.rm=T) %>%
  ungroup() %>%
  left_join(socioecon %>% filter(quarter=="2024-Q1") %>% select(State,StateName,population),by=c("State"="State")) %>%
  left_join(state_gdp,by=c("StateName"="GeoName")) %>%
  mutate(
    cbo_cap = `CBO Downscaled State Estimate ($)` / population,
    climate_cap = `Climate-Aligned Estimate ($)` / population
  ) %>%
  mutate(
    cbp_gdp = `CBO Downscaled State Estimate ($)` /(X2022*1000000),
    climate_gdp= `Climate-Aligned Estimate ($)` /(X2022*1000000)
  ) %>%
  mutate(across(c(`CBO Downscaled State Estimate ($)`,`Climate-Aligned Estimate ($)`),~round(./1000000000,3))) %>%
  
  select(State,`CBO Downscaled State Estimate ($)`,`Climate-Aligned Estimate ($)`,cbo_cap,climate_cap,cbp_gdp,climate_gdp,population,X2022)


#State of Interest largest IRA Provisions
state_abbr_ira <- ira_allstates %>%
  filter(State == "NV") %>%
  arrange(desc(`Climate-Aligned Estimate ($)`)) %>%
  mutate(across(where(is.numeric), ~round(./1000000000, 3))) %>%
  mutate(share = `Climate-Aligned Estimate ($)` / sum(`Climate-Aligned Estimate ($)`))

state_10ira<- state_abbr_ira %>%
  slice_max(order_by=`Climate-Aligned Estimate ($)`,n=10) %>%
  select(Provision,`CBO Downscaled State Estimate ($)`,`Climate-Aligned Estimate ($)`)

write.csv(state_10ira,paste0(output_folder,"/",state_abbreviation,"_state_10ira.csv"),row.names=F)

state_10ira_plot <- ggplot(data=state_10ira) +
  geom_col(aes(x=reorder(Provision,`Climate-Aligned Estimate ($)`),y=`Climate-Aligned Estimate ($)`,fill=Sector),position="stack") +
  coord_flip() +
  scale_fill_manual(values=rmi_palette) +
  labs(title = "Top 10 IRA Provisions in New Mexico in a Climate-Aligned Scenario", 
       subtitle = "",
       x="Provision",
       y="Climate-Aligned Estimate ($b)",
       fill = "Sector",
       caption="Source: RMI, June 2024 Update")+
  scale_y_continuous(expand=c(0,0))+
  theme_classic()+
  theme(legend.position=c(0.8,0.2)) 

ggsave(paste0(output_folder,"/",state_abbreviation,"_ira_provisions.png"),plot=state_10ira_plot,width=8,height=6,units="in",dpi=300)


#State Climate and Clean Energy Policy-----------------------------------------

xchange <- read.csv("C:/Users/LCarey/OneDrive - RMI/Documents/Data/US Maps etc/Policy/xchange.csv")
xchange_pol_index <- read.csv("C:/Users/LCarey/OneDrive - RMI/Documents/Data/US Maps etc/Policy/xchange_climate_policy_index.csv")

write.csv(xchange,paste0(raw_data,"xchange.csv"))
write.csv(xchange_pol_index,"Downloads/xchange_pol.csv")


xchange_label<-xchange_pol_index %>%
  filter(region %in% region_abbrv$region) %>%
  mutate(label=paste(State," \n(Climate Policy Score=",round(value*100,1),")")) %>%
  ungroup() %>%
  rename(total=value) %>%
  select(State,total,label)

state_totals <- xchange %>%
  filter(region %in% region_abbrv$region) %>%
  group_by(State) %>%
  summarize(total = sum(value)) %>%
  left_join(xchange_label %>% select(State,label), by = "State")

#Divisional Policy comparison
division_xchange <- xchange %>%
  left_join(census_divisions, by = c("abbr" = "State.Code")) %>%
  filter(Division == division_of_interest$Division) %>%
  group_by(State.x,Topic) %>%
  summarize(value=sum(value,na.rm=T)) %>%
  pivot_wider(names_from=Topic,values_from=value) %>%
  write.csv(paste0(output_folder,"/",state_abbreviation,"_division_xchange.csv"),row.names=F)

#Comparing Policies and Outcomes
state_pol <- xchange_pol_index %>%



#Economic Development Incentives----------------------------------

#Good Jobs First Data
gjf<- read.csv(paste0(raw_data,"Good Jobs First/gjf_complete.csv"))


#Manufacturing Incentives
gjf_man <- gjf %>%
  filter(grepl("Manufacturing|manufacturing",Sector))

gjf_man_20 <- gjf_man %>%
  filter(Year>2019) %>%
  group_by(Location) %>%
  summarize_at(vars(subs_m),sum,na.rm=T) %>%
  arrange(desc(subs_m)) %>%
  ungroup() %>%
  inner_join(state_gdp %>%
               select(GeoName,X2022), by=c("Location"="GeoName")) %>%
  mutate(incent_gdp_rank = rank(-subs_m/X2022))

write.csv(gjf_man_20 %>%mutate(subs_m=subs_m*1000000),"Downloads/gjf_man.csv")

gjf_man_ts <- gjf_man %>%
  group_by(Year) %>%
  summarize_at(vars(subs_m),sum,na.rm=T) %>%
  mutate(subs_m=subs_m*1000000) %>%
  arrange(Year) %>%
  write.csv("Downloads/gjf_man_ts.csv")

#State Totals -> 2019-
gjf_statetotal_19<-gjf %>%
  filter(Year>2019) %>%
  group_by(Location) %>%
  summarize_at(vars(subs_m),sum,na.rm=T) %>%
  arrange(desc(subs_m)) %>%
  ungroup() %>%
  inner_join(state_gdp, by=c("Location"="GeoName")) %>%
  mutate(incent_gdp_rank = rank(-subs_m/X2022))

#State Totals by Awarding Agency
gjf_stateagency <- gjf %>%
  filter(Location== state_name,
         Year >2019) %>%
  group_by(Location, Awarding.Agency) %>%
  summarize_at(vars(subs_m,investment_m), sum, na.rm = TRUE) %>%
  arrange(desc(subs_m))

#State Totals by Program Name
gjf_stateprogram_1923<-gjf %>%
  filter(Location== state_name,
         Year >2019) %>%
  group_by(Location,Program.Name) %>%
  summarize_at(vars(subs_m,investment_m),sum,na.rm=T) %>%
  ungroup()  %>%
  arrange(desc(subs_m))

#Project Subsidies >2% of investment value
gjf_meaningful_1923 <- gjf %>%
  filter(investment_m != 0,
         Location== "Ohio",
         subs_m/investment_m>0.01,
         Year==2022) %>%
  mutate(subs_share=round(subs_m/investment_m*100,1)) %>%
  select(abbr,Location,Year,subs_share,subs_m,Investment.Data,investment_m,Company,Project.Description,Major.Industry.of.Parent, Sector,Awarding.Agency,Program.Name,Type.of.Subsidy,Notes) %>%
  arrange(desc(subs_share))


#State Taxes-----------------------------------------
library(rvest)
library(purrr)
# URL of the page
url <- "https://www.ncsl.org/fiscal/state-tax-actions-database"

# Set additional headers to mimic a real browser
headers <- c(
  "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
  "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
  "Accept-Encoding" = "gzip, deflate, br",
  "Accept-Language" = "en-US,en;q=0.9"
)

response <- GET(url, add_headers(.headers = headers), timeout(120))

# Read the HTML content from the response
webpage <- read_html(response)

# Extract tables from the webpage
tables <- html_table(webpage, fill = TRUE)

# Assuming `tables` is your list of data frames
years <- 2023:2015  # Create a vector of years from 2023 to 2015

# Add a 'year' column to each table and rbind them
combined_data <- purrr::map2_df(tables, years, ~ mutate(.x, year = .y))

combined_data <- combined_data %>%
  mutate(State=ifelse(is.na(State),Jurisdiction,State))%>%
  mutate(State=ifelse(is.na(State),Juridiction,State))%>%
  mutate(`Revenue Type`=ifelse(is.na(`Revenue Type`),Type,`Revenue Type`)) %>%
  mutate(total=ifelse(is.na(`FY 2024 (millions)`),`FY 2023 (millions)`,
                      ifelse(is.na(`FY 2023 (millions)`),`Fiscal Year 2022`,
                             ifelse(is.na(`Fiscal Year 2022`),`Fiscal Year 2021`,`FY 2024 (millions)`)))) %>%
  mutate(total=as.numeric(gsub("[^0-9.]", "", total))) 

#Climate Taxes

keywords<- c("carbon","climate","emission",
             "greenhouse","renewable","solar","wind",
             "energy","fuel","gas","electric","vehicle",
             "EV","transportation","manufacturing","job","jobs","oil")
pattern <- paste0("\\b(", paste(keywords, collapse = "|"), ")\\b")

climate_taxes <- combined_data %>%
  filter(grepl(pattern, Description, ignore.case = TRUE)) %>%
  filter(!Description %in% c("Revenue Total=",
                             "Tax Revenue=",
                             "Non-Tax Revenue=",
                             "Non-Tax Revenue =")) 

write.csv(climate_taxes ,"C:/Users/LCarey.RMI/Downloads/climate_taxes.csv")

nm_clim_taxchanges<-climate_taxes %>%
  filter(State=="New Mexico")

sc_clim_taxchanges<-climate_taxes %>%
  filter(State=="South Carolina")



#Climate/Clean ENergy/Manufacturing Incentive Policies------------------------------------------

dev_pol <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/dbo_Program.csv")

keywords<- c("carbon","climate","emission",
             "greenhouse","renewable","solar","wind",
             "energy","fuel","gas","electric","vehicle",
             "EV","transportation","manufacturing","job","jobs","oil","research","R&D","innovation","sustainable","building","industry","industrial")
pattern <- paste0("(?i)\\b(", paste(keywords, collapse = "|"), ")\\b")

climate_dev_pol <- dev_pol %>%
  # Extract all matching keywords as a list column
  mutate(Keywords = str_extract_all(ProgramDescription, pattern)) %>%
  # Keep only rows where at least one keyword was found
  filter(lengths(Keywords) > 0) %>%
  # Add new columns for the first two keywords and convert to sentence case
  mutate(
    Theme1 = map_chr(Keywords, ~ str_to_sentence(.x[1] %||% NA_character_)),  # First keyword in sentence case
    Theme2 = map_chr(Keywords, ~ str_to_sentence(.x[2] %||% NA_character_))   # Second keyword in sentence case
  ) %>%
  # Replace any instance of "Job" with "Jobs" in Theme1
  mutate(Theme1 = str_replace(Theme1, "\\bJob\\b", "Jobs")) %>%  
  select(State,Program_Name,Theme1,Theme2,Program_Status,Agency,ProgramDescription,ProgramObjective,ProgramSpecifics,EligibilityRequirements,LegalCitation,Website1)

climate_dev_pol_sum<-climate_dev_pol %>%
  filter(Program_Status=="Active")%>%
  group_by(State) %>%
  summarize_at(vars(Program_Name),n_distinct) 
ggplot(data=climate_dev_pol_sum, aes(x=reorder(State,Program_Name),y=Program_Name)) +
  geom_col() +
  coord_flip() +
  labs(title = "Climate/Clean Energy/Manufacturing Incentive Policies",
       x = "Program Description",
       y = "Count") +
  theme_classic()


#policies in State of Interest
state_climate_pol <- climate_dev_pol %>%
  filter(State==state_name) %>%
  arrange(Theme1)
write.csv(state_climate_pol,paste0(output_folder,"/",state_abbreviation,"_state_climate_pol.csv"),row.names=F)

state_climate_pol_sum<-state_climate_pol %>%
  group_by(Theme1) %>%
  summarize_at(vars(Program_Name),n_distinct)


#Climate Legislation------------------------------------
climate_leg<-read.csv("C:/Users/LCarey.RMI/OneDrive - RMI/Documents/Data/Raw Data/climate_leg.csv")

climate_leg_state<-climate_leg %>%
  filter(statename==state_name) %>%
  select(statename,statustype,bill_id,bill_name,bill_description,bill_type_1,bill_type_2,issue_type_1,issue_type_2,source_link,sponsors_list)


#State Capacity
# Define the URL and destination file
url <- "https://dataverse.unc.edu/api/access/datafile/7531153"
destfile <- "datafile.RData"

# Download the file
download.file(url, destfile, mode = "wb")
load(destfile)

# create dataset with factor scores and state data from library(usmap) data
sc_data = as.data.frame(sc.fa_1$scores)
sc_data$abbr = NA
sc_data$abbr = row.names(sc_data)
sc_data = left_join(statepop, sc_data)
sc_data$SC = -sc_data$MR1 # this is our state capacity factor

#Clim Index & State Capacity
sc_clim<-left_join(xchange_pol_index,sc_data,by="abbr")
ggplot(data = sc_clim, aes(x = SC, y = climate_policy_index)) +
  geom_point() +
  geom_text(aes(label = abbr), vjust = -0.5) +  # Adjust label placement with `vjust`
  geom_smooth(method="lm",se = FALSE) +  # Default aesthetics are sufficient; no need for aes(x~y)
  theme_minimal()


#-------------State Budgets-------------------------------------------
library(readxl)
library(httr)
library(jsonlite)
library(bea.R)
library(tidyverse)


state_budgets<-read_excel("C:/Users/LCarey.RMI/OneDrive - RMI/Documents/Data/Raw Data/1991-2022 State Expenditure Report Data.xlsm",1)
state_budg_key<-read_excel("C:/Users/LCarey.RMI/OneDrive - RMI/Documents/Data/Raw Data/1991-2022 State Expenditure Report Data.xlsm",2)

state_key_TOT<-state_budg_key %>%
  filter(grepl("_TOT",`COLUMN HEADING`))

state_budget<- state_budgets %>%
  rename(state="...2") %>%
  select(YEAR,state,state_key_TOT$`COLUMN HEADING`)

#Totals for all states form 2012 to 2022
state_budg_1222 <- state_budget %>%
  filter(YEAR>2011) %>%
  summarize(across(c(ELSED_TOT:OTHCP_TOT),sum,na.rm=T)) %>%
  pivot_longer(ELSED_TOT:OTHCP_TOT,names_to="key",values_to="value") %>%
  inner_join(state_key_TOT,by=c("key"="COLUMN HEADING")) %>%
  mutate(share=round(value/sum(value)*100,2)) %>%
  write.csv("C:/Users/LCarey.RMI/Downloads/state_budget_spending_1222.csv")

#Capital Spending by State
state_key_cap<-state_budg_key %>%
  filter(grepl("Capital Total",`...2`))

state_cap_spend<-state_budgets %>%
  rename(state="...2") %>%
  select(YEAR,state, TOTAL_CAP) 

#GDP by Industry by State
library(bea.R)

# Set your API key (replace "YOUR_API_KEY" with your actual API key)
beaKey <- "B163ADB6-C048-4D1F-A065-33D642873C1B"

# Retrieve linecodes as dataframe
linecode <- beaParamVals(beaKey = beaKey, "Regional", "LineCode")$ParamValue

SAGDP9N<-linecode %>% filter(str_detect(Desc,"SAGDP2N"))

#GDP Growth
result<-data.frame()

for (i in c(1)) {
  beaSpecs <- list(
    'UserID' = beaKey,
    'Method' = 'GetData',
    'datasetname' = 'Regional',
    'TableName' = 'SAGDP2N',
    'LineCode' = i,
    'GeoFIPS' = 'STATE',
    'Frequency' = 'A',
    'Year' = 'ALL',
    'resultFormat' = 'json'
  )
  
  # Make the API call and extract the desired data
  api_result <- beaGet(beaSpecs)
  
  result<-rbind(result,api_result)
}

state_gdp_growth<-result %>%
  mutate(growth_17_22 = round((DataValue_2022-DataValue_2017)/DataValue_2017*100,2))

stategdp_long<-result %>%
  pivot_longer(DataValue_2017:DataValue_2022,names_to="year",values_to="gdp") %>%
  mutate(year = str_trim(year, side = "right")) %>%  # remove trailing white spaces
  mutate(year = as.numeric(substr(year, nchar(year)-3, nchar(year)))) 

state_cap_spend_1722<-state_cap_spend %>%
  filter(YEAR>2016) %>%
  inner_join(stategdp_long %>% select(GeoName,year,gdp),by=c("state"="GeoName","YEAR"="year")) %>%
  mutate(cap_gdp = TOTAL_CAP/gdp*100)

write.csv(state_cap_spend_1722 ,"C:/Users/LCarey.RMI/OneDrive - RMI/Documents/Data/Raw Data/state_cap_spend_1722.csv")


#-------------State Taxes----------------
#https://www.census.gov/programs-surveys/stc/data/datasets.html
census_stc <- read_excel(paste0(raw_data,"STC-Historical-DB.xlsx"),sheet=1,skip=1)

census_stc <- census_stc %>%
  rename("Year"="...1",
         "State"="...2",
         "Name"="...3",
         "FY Ending Date"="...4") %>%
  mutate(state_abbr=substr(Name,1,2))

stc_1823 <- census_stc %>%
  select(-State) %>%
  left_join(census_divisions,by=c("state_abbr"="State.Code")) %>%
  left_join(gdp_ind_a %>%
              filter(Description=="All industry total ") %>%
              select(GeoName,X1997:X2024) %>%
              pivot_longer(cols=c(X1997:X2024),names_to="Year",values_to="GDP") %>%
              mutate(Year=as.numeric(str_replace(Year,"X",""))),by=c("Year","State"="GeoName")) %>%
  filter(Year %in% 2018:2023) %>%
  group_by(state_abbr) %>%
  mutate(across(`Total Taxes`:GDP, as.numeric)) %>%
  summarize(total_tax=sum(`Total Taxes`,na.rm=T),
            property_tax=sum(`Property Tax (T01)`,na.rm=T),
            sales_receipt_tax=sum(`Tot Sales & Gr Rec Tax`,na.rm=T),
            fuel_tax=sum(`Motor Fuels Tax (T13)`,na.rm=T),
            total_income_tax=sum(`Total Income Taxes`,na.rm=T),
            individual_income_tax=sum(`Individual Income Tax (T40)`,na.rm=T),
            corp_income_tax=sum(`Corp Net Income Tax (T41)`,na.rm=T),
            severance_tax=sum(`Severance Tax (T53)`,na.rm=T),
            GDP=sum(GDP*1000,na.rm=T)) %>%
  ungroup() %>%
  mutate(property_tax_rate=property_tax/total_tax*100,
         sales_tax_rate=sales_receipt_tax/total_tax*100,
         fuel_tax_rate=fuel_tax/total_tax*100,
         income_tax_rate=total_income_tax/total_tax*100,
         ind_income_tax_rate=individual_income_tax/total_tax*100,
         corp_inc_tax_rate=corp_income_tax/total_tax*100,
         severance_tax_rate=severance_tax/total_tax*100) %>%
  mutate(total_tax_gdp=total_tax/GDP*100,
         property_tax_gdp=property_tax/GDP*100,
         sales_tax_gdp=sales_receipt_tax/GDP*100,
         fuel_tax_gdp=fuel_tax/GDP*100,
         income_tax_gdp=total_income_tax/GDP*100,
         ind_income_tax_gdp=individual_income_tax/GDP*100,
         corp_inc_tax_gdp=corp_income_tax/GDP*100,
         severance_tax_gdp=severance_tax/GDP*100) %>%
  mutate(property_tax_lq=property_tax_rate/property_tax_rate[state_abbr=="US"],
         sales_tax_lq=sales_tax_rate/sales_tax_rate[state_abbr=="US"],
         fuel_tax_lq=fuel_tax_rate/fuel_tax_rate[state_abbr=="US"],
         income_tax_lq=income_tax_rate/income_tax_rate[state_abbr=="US"],
         corp_inc_tax_lq=corp_inc_tax_rate/corp_inc_tax_rate[state_abbr=="US"],
         ind_income_tax_lq=ind_income_tax_rate/ind_income_tax_rate[state_abbr=="US"],
         severance_tax_lq=severance_tax_rate/severance_tax_rate[state_abbr=="US"])

state_toptax <- stc_1823 %>%
  select(state_abbr,property_tax_lq,sales_tax_lq,fuel_tax_lq,income_tax_lq,corp_inc_tax_lq,ind_income_tax_lq,severance_tax_lq) %>%
  pivot_longer(cols=c(property_tax_lq:severance_tax_lq),values_to="LQ") %>%
  group_by(state_abbr) %>%
  slice_max(order_by=LQ,n=1) %>%
  mutate(name=str_replace_all(name,"_"," "),
         name=str_replace(name,"lq",""),
         name=str_to_title(name),
         name=str_replace(name,"Corp Inc","Corporate Income"),
         name=str_replace(name,"Ind","Individual"))

write.csv(state_toptax,"Downloads/state_toptax.csv")

mountain_division<-census_divisions %>% filter(Division=="Mountain")

mountain_stc_1823<-stc_1823 %>%
  filter(state_abbr %in% c("CA","NM","AZ","CO","TX","OK","ND","AK","UT","NV")) %>%
  arrange(desc(total_tax_gdp)) %>%
  select(state_abbr,property_tax_gdp,sales_tax_gdp,fuel_tax_gdp,corp_inc_tax_gdp,ind_income_tax_gdp,severance_tax_gdp) %>%
  pivot_longer(cols=c(property_tax_gdp,sales_tax_gdp,fuel_tax_gdp,corp_inc_tax_gdp,ind_income_tax_gdp,severance_tax_gdp)) %>%
  pivot_wider(names_from=state_abbr,values_from=value) %>%
  write.csv("Downloads/mountain_taxes.csv")

