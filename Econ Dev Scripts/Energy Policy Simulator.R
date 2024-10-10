#Energy Policy Simulator

#Load Master Libraries-------------------------------
eps_bau_master<-read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/eps_bau_master.csv")
eps_ndc_master<-read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/eps_ndc_master.csv")


#Clean Files----------------------------------------
eps_bau <- eps_bau_master %>%
  separate(Time, into = c("var1", "rest"), sep = "\\[", remove = FALSE) %>%
  separate(rest, into = c("var2", "var3"), sep = ",", fill = "right") %>%
  separate(var3, into = c("var3", "var4"), sep = ",", fill = "right") %>%
  mutate(var2=gsub("\\d{2}T\\d{2}", "", var2),
         var2 = gsub("\\d|\\]", "", var2),
         var2 = gsub("\\bes\\b", "", var2),
         var2=trimws(var2),
         var3 = gsub("\\]", "", var3),
         var3=gsub("\\bif\\b", "", var3),        # Remove standalone word "if" (surrounded by spaces or end of string)
         var3 = trimws(var3)) %>%
  pivot_longer(cols=X2021:X2050,names_to="Year",values_to="Value") %>%
  mutate(Year=as.numeric(gsub("X","",Year)),
         Value=as.numeric(Value),
         scenario="BAU") %>%
  left_join(census_divisions,by=c("state"="State.Code")) %>%
  select(Region,Division,State,state,var1,var2,var3,var4,Year,Value,scenario)

eps_ndc <- eps_ndc_master %>%
  separate(Time, into = c("var1", "rest"), sep = "\\[", remove = FALSE) %>%
  separate(rest, into = c("var2", "var3"), sep = ",", fill = "right") %>%
  separate(var3, into = c("var3", "var4"), sep = ",", fill = "right") %>%
  mutate(var2=gsub("\\d{2}T\\d{2}", "", var2),
         var2 = gsub("\\d|\\]", "", var2),
         var2 = gsub("\\bes\\b", "", var2),
         var2=trimws(var2),
         var3 = gsub("\\]", "", var3),
         var3=gsub("\\bif\\b", "", var3),        # Remove standalone word "if" (surrounded by spaces or end of string)
         var3 = trimws(var3)) %>%
  pivot_longer(cols=X2021:X2050,names_to="Year",values_to="Value") %>%
  mutate(Year=as.numeric(gsub("X","",Year)),
         Value=as.numeric(Value),
         scenario="NDC") %>%
  left_join(census_divisions,by=c("state"="State.Code")) %>%
  select(Region,Division,State,state,var1,var2,var3,var4,Year,Value,scenario)

eps<-rbind(eps_bau,eps_ndc)

#Electricity Capacity, Generation, and Demand------------------------------------------


#Add scenario column, rowbind, and pivot long
eps_elec<-eps_elec_bau %>%
  mutate(scenario="BAU") %>%
  bind_rows(eps_elec_NDC %>%
              mutate(scenario="NDC")) %>%
  pivot_longer(cols=c(Geothermal:Imported.Electricity),names_to="Source",values_to="Generation")

#Identify and filter for 7 largest sources of generation over the cumulative dataset
top_sources<-eps_elec %>%
  group_by(Source) %>%
  summarize(total_gen=sum(Generation,na.rm=T)) %>%
  arrange(desc(total_gen)) %>%
  head(7) %>%
  pull(Source)

eps_elec_top <- eps_elec %>%
  filter(Source %in% top_sources)

#Plot Facet Wrap Charts by Source
eps_elec_plot<-ggplot(data=eps_elec_top,aes(x=Year,y=Generation,group=scenario,color=scenario)) +
  geom_line() +
  facet_wrap(~Source) +
  labs(title="Electricity Generation by Source",
       subtitle="In a Business as Usual and Climate-Aligned Scenario",
       x="Year",
       y="TWh",
       caption="Source: Energy Policy Simulator") +
  theme_classic()+
  scale_color_manual(values = rmi_palette)

#Pivot Wide and Simplify for Datawrapper
eps_elec_wide<-eps_elec_top %>%
  pivot_wider(names_from=Source,values_from=Generation) %>%
  write.csv(file.path(output_folder, paste0("eps_elec", ".csv")))

#BAU
write.csv(eps_elec_top %>% 
            filter(scenario=="BAU") %>%
            pivot_wider(names_from=Source,values_from=Generation),
          file.path(output_folder, paste0("eps_elec_bau", ".csv")))

#NDC
write.csv(eps_elec_top %>% 
            filter(scenario=="NDC") %>%
            pivot_wider(names_from=Source,values_from=Generation),
          file.path(output_folder, paste0("eps_elec_NDC", ".csv")))

##BAU Growth Charts
#Generation
top_change_sources<-eps_elec %>%
  filter(Year %in% c("2021","2050"),
         scenario=="BAU") %>%
  pivot_wider(names_from=Year,values_from=Generation) %>%
  mutate(change=round((`2050`-`2021`),2)) %>%
  arrange(desc(change)) %>%
  head(3) %>%
  pull(Source)

eps_elec_index <- eps_elec %>%
  filter(scenario=="BAU",
         Source %in% top_change_sources) %>%
  select(-scenario) %>%
  group_by(Source) %>%
  #mutate(ind_index=100*Generation/Generation[Year==2030]) %>%
  #select(-Generation) %>%
  pivot_wider(names_from=Source,values_from=Generation) %>%
  write.csv(file.path(output_folder, paste0("eps_elec_index", ".csv")))

#Demand by Sector-----------------------------------------------
eps_elec_demand<- read.csv(paste0("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/",state_abbreviation," - BAU - Electricity Demand by Sector.csv"))
names(eps_elec_demand) <- gsub("\\.{2,}terawatt\\.hours\\.{2,}TWh\\.{2,}year\\.", "", names(eps_elec_demand))

top_change_sector<-eps_elec_demand %>%
  filter(Year %in% c("2021","2050")) %>%
  pivot_longer(cols=c(2:7),names_to="Sector",values_to="Demand") %>%
  pivot_wider(names_from=Year,values_from=Demand) %>%
  mutate(change=round((`2050`-`2021`),2)) %>%
  arrange(desc(change)) %>%
  head(3) %>%
  pull(Sector)

eps_demand_bau <- eps_elec_demand %>%
  pivot_longer(cols=c(2:7),names_to="Sector",values_to="Demand") %>%
  filter(Sector %in% top_change_sector) %>%
  pivot_wider(names_from=Sector,values_from=Demand) %>%
  write.csv(file.path(output_folder, paste0("eps_demand_bau", ".csv")))

#Car Sales-------------------------------------------------
eps_car_sales<- read.csv(paste0("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/",state_abbreviation," - BAU - Sales - Cars and SUVs.csv"))
names(eps_car_sales) <- gsub("\\.Vehicle\\.{2,}million\\.vehicles\\.{2,}year\\.", "", names(eps_car_sales))
names(eps_car_sales) <- gsub("\\.", " ", names(eps_car_sales))

top_change_cars<-eps_car_sales %>%
  filter(Year %in% c("2021","2050")) %>%
  pivot_longer(cols=c(2:8),names_to="Vehicle",values_to="Sales") %>%
  pivot_wider(names_from=Year,values_from=Sales) %>%
  mutate(change=abs((`2050`-`2021`))) %>%
  arrange(desc(change)) %>%
  head(3) %>%
  pull(Vehicle)

eps_cars_bau <- eps_car_sales %>%
  pivot_longer(cols=c(2:8),names_to="Vehicle",values_to="Sales") %>%
  filter(Vehicle %in% top_change_cars) %>%
  pivot_wider(names_from=Vehicle,values_from=Sales) %>%
  write.csv(file.path(output_folder, paste0("eps_cars_bau", ".csv")))


#BAU v NDC Charts----------------

#Creates two wide csv files for BAU and NDC by Year, for time series charts in Datawrapper
bau_ndc_wide <- function(var) {
  # Process for BAU
  eps %>%
    filter(scenario=="BAU",
           var1 == var,
           state == state_abbreviation) %>%
    mutate(var2=gsub("sector","",var2),
           var2=str_to_sentence(var2)) %>%
    select(var2,Year,Value) %>%
    pivot_wider(names_from = var2, values_from = Value) %>%
    write.csv(file.path(output_folder, paste0(var, "_bau", ".csv")))
  
  # Process for NDC
  eps %>%
    filter(scenario=="NDC",
           var1 == var,
           state == state_abbreviation) %>%
    mutate(var2=gsub("sector","",var2),
           var2=str_to_sentence(var2)) %>%
    select(var2,Year,Value) %>%
    pivot_wider(names_from = var2, values_from = Value) %>%
    write.csv(file.path(output_folder, paste0(var, "_ndc", ".csv")))
}

#Creates one csv file comparing 2024-2050 % change for BAU and NDC
bau_ndc_perc_diff <- function(var) {
  # Percentage change 2024-2050
  eps %>%
    filter(Year %in% c("2024","2050"),
           var1 == var,
           state == state_abbreviation) %>%
    mutate(var2=gsub("sector","",var2),
           var2=str_to_sentence(var2)) %>%
    select(scenario,var2,Year,Value) %>%
    pivot_wider(names_from = Year, values_from = Value) %>%
    mutate(change=round((`2050`-`2024`)/`2024`*100,2)) %>%
    select(scenario,var2,change) %>%
    pivot_wider(names_from = scenario, values_from = change) %>%
    write.csv(file.path(output_folder, paste0(var, "_percdiff", ".csv")))
  
}

#Creates one csv file comparing 2024-2050 absolute change for BAU and NDC
bau_ndc_abs_diff <- function(var) {
  # Percentage change 2024-2050
  eps %>%
    filter(Year %in% c("2024","2050"),
           var1 == var,
           state == state_abbreviation) %>%
    mutate(var2=gsub("sector","",var2),
           var2=str_to_sentence(var2)) %>%
    select(scenario,var2,Year,Value) %>%
    pivot_wider(names_from = Year, values_from = Value) %>%
    mutate(change=(`2050`-`2024`)) %>%
    filter(change != 0) %>%
    select(scenario,var2,change) %>%
    pivot_wider(names_from = scenario, values_from = change) %>%
    write.csv(file.path(output_folder, paste0(var, "_absdiff", ".csv")))
  
}

# Example usage:
bau_ndc_wide("Output Process Emissions in CO2e by Industry")
bau_ndc_perc_diff("Output Process Emissions in CO2e by Industry")
bau_ndc_abs_diff("Output Electricity Generation by Type")



