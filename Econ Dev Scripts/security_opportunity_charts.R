library(comtradr)
set_primary_comtrade_key('2940653b9bbe4671b3f7fde2846d14be')

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
#All Energy Trade
country_info_iso <- country_info %>%
  filter(!iso3c %in% c("ASM", "CHI", "GUM", "IMN", "LIE", "MAF", "MCO", "PRI", "XKX"))

# 1) Clean & prep the HS6 codes
codes <- sectors$code_6 %>%
  as.character() %>%
  str_replace_all("\\D", "") %>%        # keep digits only, just in case
  str_pad(width = 6, side = "left", pad = "0") %>%
  na.omit() %>%
  unique()

# 2) Split into chunks by max characters allowed in the commodity_code param
split_by_nchar <- function(x, max_chars = 2500) {
  chunks <- list(); cur <- character(); cur_len <- 0
  for (code in x) {
    add_len <- nchar(code) + ifelse(length(cur) == 0, 0, 1) # comma if not first
    if (cur_len + add_len > max_chars) {
      chunks[[length(chunks) + 1]] <- cur
      cur <- code
      cur_len <- nchar(code)
    } else {
      cur <- c(cur, code)
      cur_len <- cur_len + add_len
    }
  }
  if (length(cur)) chunks[[length(chunks) + 1]] <- cur
  chunks
}

code_chunks <- split_by_nchar(codes, max_chars = 2500)  # conservative margin under 4096

# 3) Pull each chunk (add a tiny pause to be polite to the API)
safe_ct <- purrr::possibly(ct_get_data, otherwise = NULL)



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


#Friendshore Index------------------
allies<- country_info %>%
  filter(iso3c %in% c("USA","CAN", "JPN","AUS", "IND","MEX","KOR","GBR","DEU","FRA","ITA","BRA","SAU", "ZAF", "IDN", "ESP", "VNM","DNK","CHL")) 

# Allies vector
ref_areas <- allies$iso3c

#Country of interest

coi <- "South Korea"
coi_iso <- pull(country_info %>% filter(country==coi) %>% distinct(iso3c))

#OECD Data-----------------

# Fixed counterpart tail (unchanged from your query)
.partner_tail <- paste0(
  "BEL_LUX+E_X+W_X+OECD00+E+F+A+S+O+EU27_2020+EU28+EU27+EU25+EU15+ASEAN+G20XEU+G20OECD+G20XOECD+F98+ODA+ODAE+ODAF+ODAA+ODAS+ODAO+",
  "AUS+AUT+BEL+CHL+COL+CRI+CZE+DNK+EST+FIN+GRC+HUN+ISL+IRL+ISR+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+",
  "ALB+AND+BLR+BIH+BGR+HRV+CYP+FRO+GIB+GGY+VAT+IMN+JEY+XKV+LIE+MKD+MLT+MDA+MNE+ROU+RUS+SMR+SRB+SCG_F+UKR+DZA+EGY+LBY+MAR+TUN+",
  "AGO+BEN+BWA+IOT+BFA+BDI+CMR+CPV+CAF+TCD+COM+COG+COD+CIV+DJI+GNQ+ERI+ETH+GAB+GMB+GHA+GIN+GNB+KEN+LSO+LBR+MDG+MWI+MLI+MRT+",
  "MUS+MOZ+NAM+NER+NGA+RWA+SHN+STP+SEN+SYC+SLE+SOM+ZAF+SSD+SDN+SWZ+TZA+TGO+UGA+ZMB+ZWE+F4+F_O+GRL+AIA+ATG+ABW+BHS+BRB+BLZ+BMU+",
  "BES+CYM+CUB+CUW+DMA+DOM+SLV+GRD+GTM+HTI+HND+JAM+MSR+ANT_F+NIC+PAN+KNA+LCA+VCT+SXM+TTO+TCA+VGB+VIR+ARG+BOL+BRA+ECU+FLK+GUY+",
  "PRY+PER+SUR+URY+VEN+A2+A5+A7+BHR+IRQ+KWT+OMN+QAT+SAU+ARE+YEM+ARM+AZE+GEO+JOR+LBN+PSE+SYR+GULF+S3_O+AFG+BGD+BTN+BRN+KHM+HKG+",
  "IND+IDN+IRN+KAZ+PRK+KGZ+LAO+MAC+MYS+MDV+MNG+MMR+NPL+PAK+PHL+SGP+LKA+TWN+TJK+THA+TLS+TKM+UZB+VNM+S_OXOECD+S3+S_O+ASM+ATA+BVT+",
  "CXR+CCK+COK+FJI+PYF+ATF+GUM+HMD+KIR+MHL+FSM+NRU+NCL+NIU+NFK+MNP+PLW+PNG+PCN+WSM+SLB+SGS+TKL+TON+TUV+UMI+VUT+WLF+CHN+USA+GBR+",
  "ITA+JPN+FRA+DEU+CAN+W..A_B+C+C29_30+F+C24T28X27+_T.A."
)

build_url <- function(ref_area) {
  paste0(
    "https://sdmx.oecd.org/public/rest/data/OECD.DAF.INV,DSD_FDI@DF_FDI_CTRY_IND_SUMM,/",
    ref_area, ".T_FA_F.USD_EXC.", "DO", ".....", .tail,
    "?startPeriod=2019&dimensionAtObservation=AllDimensions&format=csvfilewithlabels"
  )
}

# Fetch one reporter with retry/backoff; read EVERYTHING as character
fetch_one <- function(ref_area, max_tries = 5, base_delay = 0.6) {
  url <- build_url(ref_area)
  for (i in seq_len(max_tries)) {
    out <- tryCatch(
      {
        readr::read_csv(
          url,
          col_types = cols(.default = col_character()),
          progress = FALSE
        ) |>
          mutate(REF_AREA = coalesce(REF_AREA, ref_area))
      },
      error = function(e) {
        msg <- conditionMessage(e)
        if (str_detect(msg, "HTTP error 404")) {
          message("No data for ", ref_area, " (404). Skipping.")
          return(structure(list(.skip = TRUE), class = "skip"))
        }
        if (str_detect(msg, "HTTP error 429")) {
          wait <- base_delay * 2^(i - 1) + runif(1, 0, 0.5)
          message("Rate limited (429) for ", ref_area, ". Retrying in ", round(wait, 2), "s.")
          Sys.sleep(wait)
          return(NULL)
        }
        if (str_detect(msg, "timeout|Timeout was reached|timed out")) {
          wait <- base_delay * 2^(i - 1) + runif(1, 0, 0.5)
          message("Timeout for ", ref_area, ". Retrying in ", round(wait, 2), "s.")
          Sys.sleep(wait)
          return(NULL)
        }
        message("Failed for ", ref_area, ": ", msg)
        return(structure(list(.skip = TRUE), class = "skip"))
      }
    )
    if (inherits(out, "data.frame")) return(out)
    if (inherits(out, "skip")) return(tibble())
    # else loop and retry
  }
  message("Giving up on ", ref_area)
  tibble()
}

# Optional: small delay between reporters to avoid 429s
slow_fetch <- purrr::slowly(fetch_one, rate = purrr::rate_delay(0.5))

# ---- DOWNLOAD & BIND ----
fdi_all <- purrr::map_dfr(ref_areas, slow_fetch)

# Standardize key fields after binding (we read chars above to avoid type clashes)
fdi_all <- fdi_all %>%
  mutate(
    OBS_VALUE   = readr::parse_double(OBS_VALUE, na = c("", "NA")),
    TIME_PERIOD = readr::parse_integer(TIME_PERIOD)
  )

partners_vec <- if (exists("country_info_iso")) country_info_iso$iso3c else country_info$iso3c

outbound <- fdi_all %>%
  filter(COUNTERPART_AREA %in% partners_vec) %>%
  select(
    REF_AREA,
    Measure,
    any_of(c("UNIT_MEASURE", "Unit multiplier", "Measurement principle",
             "FDI component", "Institutional sector")),
    COUNTERPART_AREA, `Counterpart area`,
    `Economic activity`, TIME_PERIOD, OBS_VALUE
  ) %>%
  group_by(REF_AREA, COUNTERPART_AREA, `Economic activity`) %>%
  summarise(total = sum(OBS_VALUE, na.rm = TRUE), .groups = "drop") %>%   # <- fix
  left_join(gdp %>% filter(year == "2024"), by = c("COUNTERPART_AREA" = "iso3c")) %>%
  filter(!is.na(GDP)) %>%
  mutate(
    GDP = GDP / 1e6,
    total_share = total / GDP
  ) %>%
  group_by(`Economic activity`) %>%
  mutate(
    total_index       = median_scurve(scales::rescale(total,       to = c(0, 1))),
    total_share_index = median_scurve(scales::rescale(total_share, to = c(0, 1)))
  ) %>%
  ungroup()


outbound_indexed <-outbound %>%  
  # 3) Compute composite segment-index = mean(size_index, share_index)
  filter(type == "index") %>%
  group_by(Country, supply_chain) %>%
  summarise(
    outbound_inv_index = mean(value, na.rm = TRUE)
  ) %>%
  filter(Country %in% country_info$country) %>%
  ungroup() %>%
  mutate(outbound_inv_index=median_scurve(outbound_inv_index))

View(outbound_indexed)


# ---- Stable IMF/CDIS fetch using httr::RETRY (no httr2 args) -----------------

fdi_all <-read.csv(paste0(raw_data,"imf_dip.csv"))

# ---- Clean + join (same as before) -------------------------------------------

fdi_allies <- fdi_all %>%
  filter(COUNTRY %in% allies$country,
         !COUNTERPART_COUNTRY %in% c("World",
                                     "Not Specified (including Confidential)" ,
                                     "Europe",
                                     "North and Central America",
                                     "Oceania and Polar Regions",
                                     "Other Near and Middle East Economies",
                                     "Economies of Persian Gulf",
                                     "Central and South Asia",
                                     "East Asia",
                                     "British Virgin Islands", 
                                     "Cayman Islands", 
                                     "Bermuda", 
                                     "Luxembourg",
                                     "Bahamas", 
                                     "Bahamas, The",
                                     "Barbados",
                                     "Turks and Caicos Islands",
                                     "Ireland",
                                     "Isle of Man", 
                                     "Guernsey", 
                                     "Cyprus", 
                                     "Mauritius"),
         DV_TYPE=="Reported official data",
         INDICATOR == "Outward Direct investment, Assets (gross), Debt instruments, All entities") %>%
  mutate(COUNTERPART_COUNTRY=recode(
    COUNTERPART_COUNTRY,
    "Vietnam" = "Viet Nam",
    "Iran, Islamic Rep." = "Iran",
    "Turkey"          = "Turkiye",
    "United kingdom"  = "United Kingdom",
    "Curacao"         = "Curaçao",
    "Saudi arabia"         = "Saudi Arabia",
    "Russian Federation" = "Russia",
    "Czechia"="Czech Republic",
    "Yemen, Rep."="Yemen",
    "Venezuela, RB"="Venezuela",
    "Ethiopia, The Federal Democratic Republic of"="Ethiopia",
    "Mauritania, Islamic Republic of"="Mauritania",
    "Mozambique, Republic of" ="Mozambique",
    "Tanzania, United Republic of" ="Tanzania",
    "Iran, Islamic Republic of" ="Iran",
    "Afghanistan, Islamic Republic of"="Afghanistan",
    "Netherlands, The" ="Netherlands",
    "Korea, Republic of" ="South Korea",
    "Egypt, Arab Republic of" = "Egypt",
    "Kazakhstan, Republic of"="Kazakhstan",
    "Venezuela, República Bolivariana de" = "Venezuela",
    "Türkiye, Republic of" = "Turkiye",
    "Estonia, Republic of"="Estonia",
    "Poland, Republic of" = "Poland",
    "Lao People's Democratic Republic" ="Laos",
    "Belarus, Republic of"  ="Belarus",
    "China, People's Republic of" ="China"
  )) %>%
  select(COUNTRY,COUNTERPART_COUNTRY,DI_DIRECTION,DI_ENTITY,SCALE,INSTR_ASSET,X2019:X2023) %>%
  rowwise() %>%
  mutate(total=sum(across(c(X2019:X2023)),na.rm=T)) %>%
  arrange(desc(total)) 

gdp_2024 <- gdp %>% filter(year == "2024") %>% select(country, GDP)

outbound <- fdi_allies %>%
  ungroup() %>%
  left_join(gdp_2024, by = c("COUNTERPART_COUNTRY" = "country")) %>%
  left_join(gdp_2024, by = c("COUNTRY" = "country")) %>%
  filter(!is.na(GDP.x)) %>%
  mutate(GDP_cp = GDP.x / 1e6, 
         GDP = GDP.y / 1e6, 
         total_counterpart_share = total / GDP_cp,
         outbound_relative=(total/GDP)/GDP_cp) %>%
  mutate(
    total_index       = median_scurve(scales::rescale(total,       to = c(0, 1))),
    total_share_index = median_scurve(scales::rescale(total_counterpart_share, to = c(0, 1))),
    outbound_relative_index = median_scurve(scales::rescale(outbound_relative, to = c(0, 1)))
  ) %>%
  rowwise()%>%
  mutate(outbound_index=mean(total_index:outbound_relative_index)) %>%
  group_by(COUNTRY) %>%
  mutate(outbound_index=median_scurve(outbound_index))


#Total Friendshore Index------------------------
res_list<- purrr::map(code_chunks, ~{
  Sys.sleep(0.4)  # adjust if you hit rate limits
  safe_ct(
    reporter       = coi_iso,
    partner        = country_info_iso$iso3c,
    commodity_code = .x,
    start_date     = 2020,
    end_date       = 2024,
    flow_direction = "import"
  )
})
energy_imports<-res_list %>%
  discard(is.null) %>%
  dplyr::bind_rows()

imports <- energy_imports %>%
  left_join(energy_codes,by=c("cmd_code"="code6")) %>%
  distinct(reporter_iso,partner_iso,partner_desc,period,industry,cmd_desc,primary_value) %>%
  group_by(reporter_iso,partner_iso,industry,period) %>%
  summarize(imports=sum(primary_value,na.rm=T)) %>%
  ungroup() %>%
  pivot_wider(names_from="period",values_from="imports") %>%
  mutate(import_growth=`2024`/`2020`-1) %>%
  rename(imports="2024") %>%
  mutate(import_index=median_scurve(imports),
         importgrowth_index=median_scurve(import_growth)) %>%
  rowwise() %>%
  mutate(trade_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  group_by() %>%
  mutate(trade_index = median_scurve(trade_index)) %>%
  filter(!is.na(trade_index),
         !is.na(industry)) %>%
  distinct(reporter_iso,partner_iso,industry,import_index,importgrowth_index,trade_index)


friendshore_index<- imports %>%
 left_join(country_info,by=c("reporter_iso"="iso3c")) %>%
  left_join(energy_security_index %>%
              ungroup() %>%
              filter(Country== coi,
                     variable=="Overall Energy Security Index") %>%
              mutate(industry=paste(tech,supply_chain),
                     energy_security_index=1-value) %>%
              select(Country,tech,supply_chain,industry,energy_security_index),by=c("country"="Country","industry")) %>%
  left_join(econ_opp_index %>%
              ungroup() %>%
              filter(Country== coi,
                     variable=="Overall Economic Opportunity Index") %>%
              mutate(industry=paste(tech,supply_chain),
                     econ_opp_index=1-value) %>%
              select(Country,industry,econ_opp_index),by=c("country"="Country","industry")) %>%
  left_join(country_info,by=c("partner_iso"="iso3c")) %>%
  inner_join(econ_opp_index %>%
               mutate(Country=str_to_title(Country)) %>%
               ungroup() %>%
               filter(variable=="Overall Economic Opportunity Index") %>%
               mutate(industry=paste(tech,supply_chain),
                      econ_opp_index2=value) %>%
               select(Country,industry,econ_opp_index2),by=c("country.y"="Country","industry")) %>%
  left_join(outbound %>%
              filter(COUNTRY== coi),
            by=c("country.y"="COUNTERPART_COUNTRY")) %>%
  #climate adjustment
  left_join(tech_ghg,by=c("tech"="Tech")) %>%
  left_join(cat %>%
              mutate(Country=recode(
                Country,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Curaçao",
                "Saudi arabia"         = "Saudi Arabia"
              )),
            by=c("country.y"="Country")) %>%
  left_join(cat %>%
              mutate(Country=recode(
                Country,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Curaçao",
                "Saudi arabia"         = "Saudi Arabia"
              )),
            by=c("country.x"="Country")) %>%
  mutate(
    ghg_index            = replace_na(ghg_index,
                                      median(tech_ghg$ghg_index, na.rm = TRUE)),
    climate_policy_index.x = replace_na(climate_policy_index.x,                                      median(cat$climate_policy_index, na.rm = TRUE)),
    #climate_policy_index.y = replace_na(climate_policy_index.y,                                        median(cat$climate_policy_index, na.rm = TRUE))
  ) %>%
  filter(!country.y %in% c("China",
                           "Russia")) %>%
  rowwise() %>% 
  mutate(
    friendshore_index_unadj = {                 # braces let you run many lines
      vals <- c(trade_index,
                energy_security_index,
                econ_opp_index2,
                outbound_index)
      wts  <- c(3, 2,1,2)
      keep <- !is.na(vals) & !is.na(wts)
      weighted.mean(vals[keep], wts[keep])   # returns a single number
    }
  ) %>% 
  mutate(penalty1    = (1 - ghg_index) *climate_policy_index.x,
         penalty2    = (1 - ghg_index) *climate_policy_index.y,
         friendshore_index=friendshore_index_unadj-0.1*penalty1-0.1*penalty2) %>%
  ungroup() %>% 
  #mutate(friendshore_index=median_scurve(friendshore_index)) %>%
  #filter(!grepl("Geothermal", tech)) %>% 
  arrange(desc(friendshore_index)) %>%
  distinct(reporter_iso,country.x,partner_iso,country.y,industry,friendshore_index,friendshore_index_unadj,
           trade_index,energy_security_index,econ_opp_index,econ_opp_index2,
           outbound_index,climate_policy_index.x,climate_policy_index.y)

friendshore_1<-friendshore_index %>%
  group_by(country.y) %>%
  slice_max(order_by=friendshore_index,n=1)

friendshore_index_total<-friendshore_index %>%
  group_by(country.y) %>%
  slice_max(order_by=friendshore_index,n=5) %>%
  summarize(total_friendshore_index=mean(friendshore_index,na.rm=T)) %>%
  ungroup() %>%
  mutate(total_friendshore_index=median_scurve(total_friendshore_index)) %>%
  arrange(desc(total_friendshore_index))


friendshore_scatter<-friendshore_index %>%
  ungroup() %>%
  mutate(country_industry=paste0(country.y,": ",industry))%>%
  select(country_industry,energy_security_index,econ_opp_index2) %>%
  mutate(energy_security_index=median_scurve(energy_security_index))
