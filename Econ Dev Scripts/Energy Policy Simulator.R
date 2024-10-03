#Energy Policy Simulator

#Load Master Libraries
eps_bau_master<-read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/eps_bau_master.csv")


#Clean Files
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
         Value=as.numeric(Value)) %>%
  left_join(census_divisions,by=c("state"="State.Code")) %>%
  select(Region,Division,State,state,var1,var2,var3,var4,Year,Value)


#Electricity Capacity Test
elec_capac_eps <- eps_bau %>%
  filter(var1=="Electricity Generation Capacity by Type Including Distributed Generation") %>%
  group_by(state,var2) %>%
  mutate(index=Value/Value[Year==2024]*100) 


solar_capac<-elec_capac_eps %>%
  filter(var2=="solar PV",
         Division==division_of_interest$Division) %>%
  select(state,Year,Value) %>%
  pivot_wider(names_from=state,values_from=Value) %>%
  write.csv(paste0(output_folder,"/",state_abbreviation,"_solar_capac.csv"))
