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
  filter(variable %in% c("share_24","growth_2435","X2024_pc"),
         tech != "Green Hydrogen",
         data_type=="raw",
         Country %in% c("Australia","Japan","South Korea")) %>%
  mutate(ind=paste(tech,supply_chain)) %>%
  select(Country,tech,variable,value) %>%
  pivot_wider(names_from="variable",values_from="value") %>%
  write.csv("Downloads/cap_growth.csv")
  

#Critical Mineral Reserves

aus_crit <- critical_min_reserves %>%
  filter(Country=="Australia",
         data_type=="index") %>%
  distinct(Mineral,value)

aus_crit_prod <-critical_min_production %>%
  filter(Country=="Australia",
         data_type=="index") %>%
  distinct(Mineral,value) %>%
  left_join(aus_crit,by=c("Mineral"))

aus_critmin_trade <- critmin_trade %>%
  group_by(country,mineral,supply_chain) %>%
  summarize(exports=sum(exports,na.rm=T)) %>%
  group_by(mineral,supply_chain) %>%
  mutate(export_index=median_scurve(exports)) %>%
  filter(country=="Australia",
         supply_chain=="Upstream") %>%
  select(mineral,export_index) %>%
  inner_join(aus_crit_prod,by=c("mineral"="Mineral")) %>%
  write.csv("Downloads/auscritmin.csv")


kor_crit <- critical_min_reserves %>%
  filter(Country=="South Korea",
         data_type=="index") %>%
  distinct(Mineral,value)

kor_crit_prod <-critical_min_production %>%
  filter(Country=="South Korea",
         data_type=="index") %>%
  distinct(Mineral,value) %>%
  left_join(kor_crit,by=c("Mineral"))

kor_critmin_trade <- critmin_trade %>%
  group_by(country,mineral,supply_chain) %>%
  summarize(exports=sum(exports,na.rm=T)) %>%
  group_by(mineral,supply_chain) %>%
  mutate(export_index=median_scurve(exports)) %>%
  filter(country=="South Korea",
         supply_chain=="Upstream") %>%
  select(mineral,export_index) %>%
  inner_join(kor_crit_prod,by=c("mineral"="Mineral")) %>%
  write.csv("Downloads/korcritmin.csv")


#Overall Indices
trio_scatter <- scatter_index %>%
  filter(Country %in% c("Australia","Japan","South Korea"))

trio_security <- trio_scatter %>%
  filter(variable=="Overall Energy Security Index") %>%
  select(Country,tech, supply_chain,value) %>%
  pivot_wider(names_from="Country",values_from="value") %>%
  mutate(ind=paste(tech,supply_chain)) %>%
  arrange(desc(Australia)) %>%
  write.csv("Downloads/security.csv")
  
trio_opportunity <- trio_scatter %>%
  filter(variable=="Overall Economic Opportunity Index") %>%
  select(Country,tech, supply_chain,value) %>%
  pivot_wider(names_from="Country",values_from="value") %>%
  mutate(ind=paste(tech,supply_chain)) %>%
  arrange(desc(Australia)) %>%
  write.csv("Downloads/opportunity.csv")   
  
  
#Trade
  #Allies
  res_list_aus<- purrr::map(code_chunks, ~{
    Sys.sleep(0.4)  # adjust if you hit rate limits
    safe_ct(
      reporter       = "AUS",
      partner        = country_info_iso$iso3c,
      commodity_code = .x,
      start_date     = 2020,
      end_date       = 2024,
      flow_direction = "export"
    )
  })
aus_energy_exports<-res_list_aus %>%
  discard(is.null) %>%
  dplyr::bind_rows()

res_list_jpn<- purrr::map(code_chunks, ~{
  Sys.sleep(0.4)  # adjust if you hit rate limits
  safe_ct(
    reporter       = "JPN",
    partner        = country_info_iso$iso3c,
    commodity_code = .x,
    start_date     = 2020,
    end_date       = 2024,
    flow_direction = "export"
  )
})
jpn_energy_exports<-res_list_jpn %>%
  discard(is.null) %>%
  dplyr::bind_rows()


res_list_kor<- purrr::map(code_chunks, ~{
  Sys.sleep(0.4)  # adjust if you hit rate limits
  safe_ct(
    reporter       = "KOR",
    partner        = country_info_iso$iso3c,
    commodity_code = .x,
    start_date     = 2020,
    end_date       = 2024,
    flow_direction = "export"
  )
})
kor_energy_exports<-res_list_kor %>%
  discard(is.null) %>%
  dplyr::bind_rows()



#Export Opportunity index---------------------
#Australia
aus_exports <- aus_energy_exports %>%
  left_join(energy_codes,by=c("cmd_code"="code6")) %>%
  distinct(reporter_iso,partner_iso,partner_desc,period,industry,cmd_desc,primary_value) %>%
  group_by(reporter_iso,partner_iso,industry,period) %>%
  summarize(exports=sum(primary_value,na.rm=T)) %>%
  ungroup() %>%
  pivot_wider(names_from="period",values_from="exports") %>%
  mutate(export_growth=`2024`/`2020`-1) %>%
  rename(exports="2024") %>%
  mutate(aus_export_index=median_scurve(exports),
         aus_exportgrowth_index=median_scurve(export_growth)) %>%
  rowwise() %>%
  mutate(aus_trade_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  group_by() %>%
  mutate(aus_trade_index = median_scurve(aus_trade_index)) %>%
  filter(!is.na(aus_trade_index),
         !is.na(industry)) %>%
  distinct(reporter_iso,partner_iso,industry,aus_trade_index)

aus_opportunity_index <- aus_exports %>%
  
  separate(
    col    = industry,
    into   = c("tech", "supply_chain"),
    sep    = " (?=Downstream|Midstream|Upstream$)"
  ) %>%
  left_join(all_countries,by=c("partner_iso"="ISO3166_alpha3")) %>%
  left_join(econ_opp_index %>%
              ungroup() %>%
              filter(Country=="Australia",
                     variable=="Overall Economic Opportunity Index") %>%
              mutate(industry=paste(tech,supply_chain),
                     econ_opp_index=value) %>%
              select(tech,supply_chain,econ_opp_index) %>%
              mutate(econ_opp_index=median_scurve(econ_opp_index)),by=c("tech","supply_chain")) %>%
  inner_join(energy_security_index %>%
              ungroup() %>%
               filter(variable=="Overall Energy Security Index") %>%
               mutate(industry=paste(tech,supply_chain),
                      energy_security_index=1-value) %>%
               select(Country,tech,supply_chain,industry,energy_security_index),by=c("Country"="Country","tech","supply_chain")) %>%
  left_join(tech_ghg,by=c("tech"="Tech")) %>%
  left_join(cat %>%
              mutate(Country=str_to_title(Country)) %>%
              mutate(Country=recode(
                Country,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Curaçao",
                "Saudi arabia"         = "Saudi Arabia",
                "Vietnam" = "Viet Name",
                "United arab emirates" = "United Arab Emirates"
              )) ,by=c("Country"="Country")) %>%
  mutate(
    ghg_index            = replace_na(ghg_index,
                                      median(tech_ghg$ghg_index, na.rm = TRUE)),
    climate_policy_index = replace_na(climate_policy_index,
                                      median(cat$climate_policy_index, na.rm = TRUE))
  ) %>%
  rowwise() %>%
  mutate(
    aus_opportunity_index = {                 # braces let you run many lines
      vals <- c(aus_trade_index,econ_opp_index,energy_security_index)
      wts  <- c(2, 2,0.5)
      keep <- !is.na(vals) & !is.na(wts)
      weighted.mean(vals[keep], wts[keep])   # returns a single number
    }
  ) %>%
  mutate(penalty    = (1 - ghg_index) * climate_policy_index,
         aus_opportunity_index=aus_opportunity_index-0.25*penalty) %>%
  filter(!Country %in% c("China","Russia"),
         tech %in% techs,
         tech != "Geothermal") %>%
  
  ungroup() %>%
  mutate(aus_opportunity_index = median_scurve(aus_opportunity_index)) %>%
  arrange(desc(aus_opportunity_index))

aus_opportunity_map <- aus_opportunity_index %>%
  group_by(Country) %>%
  slice_max(aus_opportunity_index,n=1)

write.csv(aus_opportunity_map,"Downloads/aus_opp_map.csv")

#Japan
jpn_exports <- jpn_energy_exports %>%
  left_join(energy_codes,by=c("cmd_code"="code6")) %>%
  distinct(reporter_iso,partner_iso,partner_desc,period,industry,cmd_desc,primary_value) %>%
  group_by(reporter_iso,partner_iso,industry,period) %>%
  summarize(exports=sum(primary_value,na.rm=T)) %>%
  ungroup() %>%
  pivot_wider(names_from="period",values_from="exports") %>%
  mutate(export_growth=`2024`/`2020`-1) %>%
  rename(exports="2024") %>%
  mutate(jpn_export_index=median_scurve(exports),
         jpn_exportgrowth_index=median_scurve(export_growth)) %>%
  rowwise() %>%
  mutate(jpn_trade_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  group_by() %>%
  mutate(jpn_trade_index = median_scurve(jpn_trade_index)) %>%
  filter(!is.na(jpn_trade_index),
         !is.na(industry)) %>%
  distinct(reporter_iso,partner_iso,industry,jpn_trade_index)

jpn_opportunity_index <- jpn_exports %>%
  
  separate(
    col    = industry,
    into   = c("tech", "supply_chain"),
    sep    = " (?=Downstream|Midstream|Upstream$)"
  ) %>%
  left_join(all_countries,by=c("partner_iso"="ISO3166_alpha3")) %>%
  left_join(econ_opp_index %>%
              ungroup() %>%
              filter(Country=="Japan",
                     variable=="Overall Economic Opportunity Index") %>%
              mutate(industry=paste(tech,supply_chain),
                     econ_opp_index=value) %>%
              select(tech,supply_chain,econ_opp_index) %>%
              mutate(econ_opp_index=median_scurve(econ_opp_index)),by=c("tech","supply_chain")) %>%
  inner_join(energy_security_index %>%
               ungroup() %>%
               filter(variable=="Overall Energy Security Index") %>%
               mutate(industry=paste(tech,supply_chain),
                      energy_security_index=1-value) %>%
               select(Country,tech,supply_chain,industry,energy_security_index),by=c("Country"="Country","tech","supply_chain")) %>%
  left_join(tech_ghg,by=c("tech"="Tech")) %>%
  left_join(cat %>%
              mutate(Country=str_to_title(Country)) %>%
              mutate(Country=recode(
                Country,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Curaçao",
                "Saudi arabia"         = "Saudi Arabia",
                "Vietnam" = "Viet Name",
                "United arab emirates" = "United Arab Emirates"
              )) ,by=c("Country"="Country")) %>%
  mutate(
    ghg_index            = replace_na(ghg_index,
                                      median(tech_ghg$ghg_index, na.rm = TRUE)),
    climate_policy_index = replace_na(climate_policy_index,
                                      median(cat$climate_policy_index, na.rm = TRUE))
  ) %>%
  rowwise() %>%
  mutate(
    jpn_opportunity_index = {                 # braces let you run many lines
      vals <- c(jpn_trade_index,econ_opp_index,energy_security_index)
      wts  <- c(2, 2,0.5)
      keep <- !is.na(vals) & !is.na(wts)
      weighted.mean(vals[keep], wts[keep])   # returns a single number
    }
  ) %>%
  mutate(penalty    = (1 - ghg_index) * climate_policy_index,
         jpn_opportunity_index=jpn_opportunity_index-0.25*penalty) %>%
  filter(!Country %in% c("China","Russia"),
         tech %in% techs,
         tech != "Geothermal") %>%
  
  ungroup() %>%
  mutate(jpn_opportunity_index = median_scurve(jpn_opportunity_index)) %>%
  arrange(desc(jpn_opportunity_index))

jpn_opportunity_map <- jpn_opportunity_index %>%
  group_by(Country) %>%
  slice_max(jpn_opportunity_index,n=1)

write.csv(jpn_opportunity_map,"Downloads/jpn_opp_map.csv")

#Korea
kor_exports <- kor_energy_exports %>%
  left_join(energy_codes,by=c("cmd_code"="code6")) %>%
  distinct(reporter_iso,partner_iso,partner_desc,period,industry,cmd_desc,primary_value) %>%
  group_by(reporter_iso,partner_iso,industry,period) %>%
  summarize(exports=sum(primary_value,na.rm=T)) %>%
  ungroup() %>%
  pivot_wider(names_from="period",values_from="exports") %>%
  mutate(export_growth=`2024`/`2020`-1) %>%
  rename(exports="2024") %>%
  mutate(kor_export_index=median_scurve(exports),
         kor_exportgrowth_index=median_scurve(export_growth)) %>%
  rowwise() %>%
  mutate(kor_trade_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  group_by() %>%
  mutate(kor_trade_index = median_scurve(kor_trade_index)) %>%
  filter(!is.na(kor_trade_index),
         !is.na(industry)) %>%
  distinct(reporter_iso,partner_iso,industry,kor_trade_index)

kor_opportunity_index <- kor_exports %>%
  
  separate(
    col    = industry,
    into   = c("tech", "supply_chain"),
    sep    = " (?=Downstream|Midstream|Upstream$)"
  ) %>%
  left_join(all_countries,by=c("partner_iso"="ISO3166_alpha3")) %>%
  left_join(econ_opp_index %>%
              ungroup() %>%
              filter(Country=="South Korea",
                     variable=="Overall Economic Opportunity Index") %>%
              mutate(industry=paste(tech,supply_chain),
                     econ_opp_index=value) %>%
              select(tech,supply_chain,econ_opp_index) %>%
              mutate(econ_opp_index=median_scurve(econ_opp_index)),by=c("tech","supply_chain")) %>%
  inner_join(energy_security_index %>%
               ungroup() %>%
               filter(variable=="Overall Energy Security Index") %>%
               mutate(industry=paste(tech,supply_chain),
                      energy_security_index=1-value) %>%
               select(Country,tech,supply_chain,industry,energy_security_index),by=c("Country"="Country","tech","supply_chain")) %>%
  left_join(tech_ghg,by=c("tech"="Tech")) %>%
  left_join(cat %>%
              mutate(Country=str_to_title(Country)) %>%
              mutate(Country=recode(
                Country,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Curaçao",
                "Saudi arabia"         = "Saudi Arabia",
                "Vietnam" = "Viet Name",
                "United arab emirates" = "United Arab Emirates"
              )) ,by=c("Country"="Country")) %>%
  mutate(
    ghg_index            = replace_na(ghg_index,
                                      median(tech_ghg$ghg_index, na.rm = TRUE)),
    climate_policy_index = replace_na(climate_policy_index,
                                      median(cat$climate_policy_index, na.rm = TRUE))
  ) %>%
  rowwise() %>%
  mutate(
    kor_opportunity_index = {                 # braces let you run many lines
      vals <- c(kor_trade_index,econ_opp_index,energy_security_index)
      wts  <- c(2, 2,0.5)
      keep <- !is.na(vals) & !is.na(wts)
      weighted.mean(vals[keep], wts[keep])   # returns a single number
    }
  ) %>%
  mutate(penalty    = (1 - ghg_index) * climate_policy_index,
         kor_opportunity_index=kor_opportunity_index-0.25*penalty) %>%
  filter(!Country %in% c("China","Russia"),
         tech %in% techs,
         tech != "Geothermal") %>%
  
  ungroup() %>%
  mutate(kor_opportunity_index = median_scurve(kor_opportunity_index)) %>%
  arrange(desc(kor_opportunity_index))

kor_opportunity_map <- kor_opportunity_index %>%
  group_by(Country) %>%
  slice_max(kor_opportunity_index,n=1)

write.csv(kor_opportunity_map,"Downloads/kor_opp_map.csv")
