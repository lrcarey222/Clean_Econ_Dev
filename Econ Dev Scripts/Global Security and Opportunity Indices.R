#Energy Security, Economic Opportunity, and Partnership Strength Indices
library(glue)
library(WDI)
library(scales)
library(dplyr)
library(tidyr)
library(purrr)
library(jsonlite)

median_scurve <- function(x, gamma = 0.5) {
  # 1) turn raw x into a [0,1] percentile
  r <- dplyr::percent_rank(x)
  # 2) compress around 0.5 by using
  #    f(r) = r^gamma / (r^gamma + (1-r)^gamma)
  #
  # When gamma < 1, slope at r=0.5 is <1 (flat middle)
  #       and slope ??? ??? as r???0 or 1 (steep tails).
  idx <- (r^gamma) / (r^gamma + (1 - r)^gamma)
  idx
}

library(WDI)
gdp_data<-WDI(indicator = "NY.GDP.MKTP.CD", start = 2007, end = 2024) 
gdp <- gdp_data %>%           # iso3c, year, NY.GDP.MKTP.CD
  rename(GDP = NY.GDP.MKTP.CD)

iso3c <- c("USA",
           "CAN",
           "JPN",
           "AUS",
           "IND",
           "MEX",
           "KOR",
           "GBR",
           "DEU",
           "FRA",
           "ITA",
           "BRA",
           "SAU",
           "ZAF",
           "IDN",
           "NOR",
           "ARE",
           "VNM",
           "KEN",
           "DNK",
           "ARG",
           "MAR",
           "CHL")  # Example definition
#Partner Mapping - The US needs to consider not only which sectors to prioritize, but which sectors among which allies
country_info <- WDI_data$country %>%
  filter(region!="Aggregates") %>%
  mutate(country=ifelse(country=="Russian Federation","Russia",country),
         country=ifelse(iso3c=="KOR","South Korea",country),
         country=ifelse(iso3c=="COD","Democratic Republic of Congo",country))

allies<- country_info %>%
  filter(iso3c %in% c("USA","CAN", "JPN","AUS", "IND","MEX","KOR","GBR","DEU","FRA","ITA","BRA","SAU", "ZAF", "IDN", "NOR", "UAE","VNM","KEN","DNK","ARG","MAR","CHL"))

#Energy Technologies of interest - not just clean. Need to be tech-neutral - what sectors bring the US the great energy security and economic opportunity benefits:
techs <- c("Electric Vehicles",
           "Nuclear","Coal","Batteries","Green Hydrogen","Wind","Oil",                       
           "Solar", "Gas", "Geothermal","Electric Grid")

ei<-read.csv(paste0(raw_data,"ei_stat_review_world_energy.csv"))  %>%
  mutate(Country=ifelse(Country=="US","United States",Country))

all_countries <- ei %>%
  filter(Year == "2024",
         !grepl("World|Other|Total|OECD|OPEC", Country)) %>%
  mutate(Country=recode(
    Country,
    "Vietnam" = "Viet Nam",
    "Iran, Islamic Rep." = "Iran",
    "Turkey"          = "Turkiye",
    "United kingdom"  = "United Kingdom",
    "Curacao"         = "Cura�ao",
    "Saudi arabia"         = "Saudi Arabia",
    "Russian Federation" = "Russia",
    "Czechia"="Czech Republic",
    "Yemen, Rep."="Yemen",
    "Venezuela, RB"="Venezuela"
  )) %>%
  distinct(ISO3166_alpha3,Country) 

##----------Energy Security------------------------
##Energy security - the assurance that energy is available where and when it's needed - can be measured or benchmarked by looking at energy availability, energy prices, affordability, natural resource availability, import reliance, and global market share

#Energy Consumption----------

#energy consumption per capita as a proxy for the availability of abundant energy supplies


make_ec_long <- function(ei, year) {
  ei %>%
    filter(
      Year == year,
      Var %in% c("pop", "coalcons_ej", "oilcons_ej", "gascons_ej", 
                 "solar_ej", "wind_ej", "nuclear_ej"),
      !grepl("World|Other|Total|OECD|OPEC", Country)
    ) %>%
    select(Country, Var, Value) %>%
    pivot_wider(names_from = Var, values_from = Value) %>%
    transmute(
      Country,
      coal_raw    = coalcons_ej  / pop,
      oil_raw     = oilcons_ej   / pop,
      gas_raw     = gascons_ej   / pop,
      solar_raw   = solar_ej     / pop,
      wind_raw    = wind_ej      / pop,
      nuclear_raw = nuclear_ej   / pop
    ) %>%
    mutate(across(ends_with("_raw"), ~replace_na(.x, 0))) %>%
    mutate(across(
      ends_with("_raw"),
      median_scurve,
      .names = "{str_remove(.col,'_raw')}_index"      # <-- no repeated "access"
    )) %>%
    pivot_longer(
      cols = -Country,
      names_to      = c("tech","data_type"),
      names_pattern = "(.*)_(raw|index)",
      values_to     = "value"
    ) %>%
    transmute(
      Country,
      tech         = str_to_sentence(tech),
      supply_chain = "Downstream",
      category     = "Energy Access",
      variable     = "Energy consumption per capita",
      data_type,                                       # raw / index
      value,
      Year        = year,
      source      = "EI Statistical Review of World Energy (2024)",
      explanation = case_when(
        data_type == "raw"   ~ str_glue("Per-capita {tech} consumption = {tech}cons_ej � pop"),
        data_type == "index" ~ str_glue("Normalized index of per-capita {tech} consumption")
      )
    ) 
}

# 1. make the 2019 and 2024 series
ec_2019 <- make_ec_long(ei, "2019")
ec_2024 <- make_ec_long(ei, "2024")

# 2. join and compute growth
ec_growth <- ec_2019 %>%
  inner_join(ec_2024,
             by = c("Country","tech","supply_chain","category","variable","data_type"),
             suffix = c("_2019","_2024")) %>%
  filter(data_type == "raw") %>%
  mutate(
    growth_raw = (value_2024 - value_2019) / value_2019
  ) %>%
  group_by(Country) %>%
  mutate(
    growth_index = median_scurve(growth_raw)
  ) %>%
  ungroup() %>%
  select(-data_type) %>%
  # - pivot just the two growth columns -
  pivot_longer(
    cols        = c(growth_raw, growth_index),
    names_to    = c("metric", "data_type"),
    names_pattern = "(.*)_(raw|index)",
    values_to   = "value"
  ) %>%
  
  # now build your final shape
  transmute(
    Country,
    tech         = str_to_sentence(tech),
    supply_chain = "Downstream",
    category     = "Energy Access",
    variable     = paste(variable, metric),
    data_type,                      # "raw" or "index"
    value,
    Year         = "2019-2024",
    source       = "EI Statistical Review of World Energy (2024)",
    explanation  = case_when(
      data_type == "raw"   ~ "2019-2024 growth of per-capita consumption",
      data_type == "index" ~ "Normalized index of per-capita consumption growth"
    )
  )

# inspect
energy_consumption_clean <- ec_2024 %>% select(-Year)
energy_consumption_growth <-ec_growth %>% select(-Year)

#BNEF Installed Capacity----------------------
bnef_neo <- read.csv(paste0(raw_data,"2024-10-29 - New Energy Outlook 2024.csv"),skip=2)

# ---- 1. Build a look-up table of population -------------------------------
pop_tbl <- bnef_neo %>% 
  filter(Indicator == "Population",
         Macro.sector == "All sectors",
         !Region %in% c("Global", "Rest of World"),
         Scenario == "ETS") %>% 
  transmute(
    Country = recode(Region, "US" = "United States", "UK" = "United Kingdom"),
    pop_2024 = as.numeric(X2024),
    pop_2035 = as.numeric(X2035)
  )

# ---- 2. Process the energy indicators and join population -----------------
neo_cap <- bnef_neo %>%
  filter(
    Indicator %in% c("Installed electric capacity", "Final energy consumption"),
    Macro.sector %in% c("All sectors", "Energy industry"),
    !Region %in% c("Global", "Rest of World"),
    !grepl("Other", Region),
    Scenario == "ETS",
    (Indicator == "Installed electric capacity" & Fuel.type != "Hydrogen") | 
      (Indicator == "Final energy consumption" & Fuel.type == "Hydrogen")
  ) %>% 
  mutate(
    Fuel.type = recode(Fuel.type,
                       "CCGT"            = "Gas",
                       "Coal with CCS" = "Coal",
                       "Gas peaker with CCS" = "Gas",
                       "CCGT with CCS" = "Gas",
                       "Utility-scale PV" = "Solar",
                       "Unabated oil"  = "Oil",
                       "Gas production" = "Gas",
                       "Gas peaker"  = "Gas",
                       "Hydrogen"        = "Green Hydrogen",
                       "Battery storage" = "Batteries",
                       "Small modular nuclear" = "Nuclear",
                       "Small-scale PV"="Solar")
  ) %>% 
  select(
    Country = Region,
    tech    = Fuel.type,
    Indicator,
    X2024, X2035
  ) %>% 
  mutate(
    X2024 = as.numeric(X2024),
    X2035 = as.numeric(X2035),
    Country = recode(Country,
                     "US" = "United States",
                     "UK" = "United Kingdom")
  ) %>% 
  
  # ---- 2a. attach the population columns ----------------------------------
left_join(pop_tbl, by = "Country") %>% 
  
  # ---- 3. create per-capita variables -------------------------------------
mutate(
  X2024_pc = X2024 / pop_2024,      # capacity or energy per person
  X2035_pc = X2035 / pop_2035
) %>% 
  
  # ---- 4. carry on with your existing calculations ------------------------
group_by(Country,tech) %>% 
  summarize(X2024_pc=sum(X2024_pc,na.rm=T),
            X2035_pc=sum(X2035_pc,na.rm=T),
            X2024=sum(X2024,na.rm=T),
            X2035=sum(X2035,na.rm=T)) %>%
  group_by(Country) %>%
  mutate(
    share_24       = X2024 / sum(X2024),              # share of per-capita total
    growth_2435    = (X2035_pc - X2024_pc) / X2024_pc
  ) %>% 
  filter(tech %in% techs) %>% 
  group_by(tech) %>% 
  mutate(installed_cap_index = median_scurve(X2024_pc)) %>% 
  group_by(Country) %>% 
  mutate(elec_growth_index   = median_scurve(growth_2435)) %>% 
  pivot_longer(
    cols      = c(X2024_pc, X2035_pc, share_24, growth_2435,
                  installed_cap_index, elec_growth_index),
    names_to  = "variable",
    values_to = "value"
  ) %>% 
  mutate(
    supply_chain = "Downstream",
    category     = "Consumption",
    data_type    = if_else(variable %in% c("installed_cap_index", "elec_growth_index"),
                           "index", "raw"),
    source       = "BNEF New Energy Outlook 2024",
    explanation  = case_when(
      variable == "X2024_pc"          ~ "2024 installed electric capacity **per capita**",
      variable == "X2035_pc"          ~ "Forecast 2035 installed electric capacity **per capita**",
      variable == "share_24"          ~ "2024 share of *per-capita* installed capacity",
      variable == "installed_cap_index" ~ "Index of per-capita capacity share (2024)",
      variable == "growth_2435"       ~ "Growth in per-capita capacity 2024-35",
      variable == "elec_growth_index" ~ "Index of per-capita capacity growth 2024-35"
    )
  ) %>% 
  select(Country, tech, supply_chain, category,
         variable, data_type, value, source, explanation)


#Critical Minerals------------------

#IEA Critical Minerals Demand----------------------------
critical<-read.csv(paste0(raw_data,"iea_criticalminerals_25.csv")) 

mineral_demand<-critical %>%
  filter(Pillar=="3.1 Cleantech demand by tech",
         !grepl("Other|Total", `Sector.Country`)) %>%
  mutate(growth = X2035/X2024-1) %>%
  group_by(Pillar,Mineral) %>% 
  mutate(share_24=X2024/sum(X2024),
         share_35=X2035/sum(X2035)) %>%
  select(1:3,X2024,X2035,growth,share_24,share_35) %>%
  group_by(Pillar) %>%
  mutate(
    across(c(share_24,growth),
           index,
           .names = "{.col}_index")
  ) %>%
  rowwise() %>%    # allows c_across() to work row-wise
  mutate(
    demand_index    = mean(c_across(c(share_24_index,
                                      growth_index)),  na.rm = TRUE))

totals_2035 <- critical %>% 
  filter(Pillar == "3.1 Cleantech demand by tech",
         grepl("^Total", Sector.Country)) %>%      # be as specific as you like
  transmute(Mineral,
            total_2035 = X2035)                    # keep just the total

# 2) Bring the totals into every other row of the same mineral
mineral_demandindex <- critical %>% 
  filter(Pillar == "3.1 Cleantech demand by tech") %>% 
  select(Mineral, Sector.Country, value_2035 = X2035) %>% 
  left_join(totals_2035, by = "Mineral") %>%       # adds NA where no total exists
  mutate(demand_share = value_2035 / total_2035) %>%
  # drop the helper column if you like
  select(-total_demand_2035) %>%
  filter(!grepl("Other",Sector.Country),
         !grepl("Total",Sector.Country)) %>%
  ungroup() %>%
  
  mutate(tech = case_when(
    str_detect(Sector.Country, regex("^Solar", ignore_case=TRUE))            ~ "Solar",
    str_detect(Sector.Country, regex("^Wind", ignore_case=TRUE))             ~ "Wind",
    str_detect(Sector.Country, regex("Electric vehicles?", ignore_case=TRUE))~ "Electric Vehicles",
    str_detect(Sector.Country, regex("Grid battery storage", ignore_case=TRUE)) ~ "Batteries",
    str_detect(Sector.Country, regex("Electricity networks?", ignore_case=TRUE)) ~ "Electric Grid",
    str_detect(Sector.Country, regex("Hydrogen", ignore_case=TRUE))          ~ "Green Hydrogen",
    str_detect(Sector.Country, regex("Heat pumps?", ignore_case=TRUE))       ~ "Heat Pumps",
    str_detect(Sector.Country, regex("^Coal$", ignore_case=TRUE))            ~ "Coal",
    str_detect(Sector.Country, regex("^Nuclear$", ignore_case=TRUE))         ~ "Nuclear",
    str_detect(Sector.Country, regex("^Oil$", ignore_case=TRUE))             ~ "Oil",
    str_detect(Sector.Country, regex("^Gas$", ignore_case=TRUE))             ~ "Gas",
    str_detect(Sector.Country, regex("Hydroelectric", ignore_case=TRUE))     ~ "Hydroelectric Power",
    str_detect(Sector.Country, regex("Geothermal", ignore_case=TRUE))        ~ "Geothermal",
    TRUE                                                                   ~ NA_character_
  )) %>%
  mutate(Mineral=ifelse(grepl("Graphite",Mineral),"Graphite",Mineral)) %>%
  filter(!is.na(tech)) %>%
  group_by(tech) %>%
  mutate(demand_index=median_scurve(demand_share))

mineral_demand_clean <- mineral_demand %>%
  # 1a) map Sector.Country to your tech names
  mutate(tech = case_when(
    str_detect(Sector.Country, regex("^Solar", ignore_case=TRUE))            ~ "Solar",
    str_detect(Sector.Country, regex("^Wind", ignore_case=TRUE))             ~ "Wind",
    str_detect(Sector.Country, regex("Electric vehicles?", ignore_case=TRUE))~ "Electric Vehicles",
    str_detect(Sector.Country, regex("Grid battery storage", ignore_case=TRUE)) ~ "Batteries",
    str_detect(Sector.Country, regex("Electricity networks?", ignore_case=TRUE)) ~ "Electric Grid",
    str_detect(Sector.Country, regex("Hydrogen", ignore_case=TRUE))          ~ "Green Hydrogen",
    str_detect(Sector.Country, regex("Heat pumps?", ignore_case=TRUE))       ~ "Heat Pumps",
    str_detect(Sector.Country, regex("^Coal$", ignore_case=TRUE))            ~ "Coal",
    str_detect(Sector.Country, regex("^Nuclear$", ignore_case=TRUE))         ~ "Nuclear",
    str_detect(Sector.Country, regex("^Oil$", ignore_case=TRUE))             ~ "Oil",
    str_detect(Sector.Country, regex("^Gas$", ignore_case=TRUE))             ~ "Gas",
    str_detect(Sector.Country, regex("Hydroelectric", ignore_case=TRUE))     ~ "Hydroelectric Power",
    str_detect(Sector.Country, regex("Geothermal", ignore_case=TRUE))        ~ "Geothermal",
    TRUE                                                                   ~ NA_character_
  )) %>%
  mutate(Mineral=ifelse(grepl("Graphite",Mineral),"Graphite",Mineral))

Upstream_demand<-mineral_demand %>%                     #SM: doesnt look like Upstream_demand is used anywhere
  mutate(supply_chain="Upstream") %>%
  rename(tech=`Sector.Country`) %>%
  group_by(tech,supply_chain) %>%
  summarize(demand_index=mean(demand_index,na.rm=T)) %>%
  ungroup() %>%
  mutate(demand_index=median_scurve(demand_index))



#Resource Availability---------------------

#-- Reserves from EI ExcelSheet --
read_reserves <- function(path, sheet, skip, nm_col, val_col,
                          tech_name, unit_desc, include_row = TRUE){
  
  raw <- read_excel(path, sheet = sheet, skip = skip) %>% 
    rename(Country = all_of(nm_col),
           raw_value = all_of(val_col)) %>% 
    mutate(
      Country = case_when(
        Country %in% c("Rest of World", "Rest of world", "Rest of World^") ~ "Rest of World",
        Country == "US"       ~ "United States",
        Country == "DR Congo" ~ "Democratic Republic of Congo",
        Country == "Russia Federation" ~ "Russia",
        TRUE                  ~ Country)
    ) %>% 
    filter(
      (Country %in% all_countries),
      !is.na(Country),
      !grepl("Total World|Other|OECD|OPEC|Orinoco", Country)
    ) %>% 
    mutate(raw_value = as.numeric(raw_value) %>% replace_na(0)) 
  
  dummy_zero <- tibble(Country = "_ZERO_", raw_value = 0)
  
  ranked <- bind_rows(raw, dummy_zero) %>%
    mutate(index_value = median_scurve(raw_value)) %>%
    filter(Country != "_ZERO_") %>% 
    tidyr::complete(
      Country = all_countries,
      fill = list(raw_value = 0,
                  index_value=0)
    ) %>% 
    pivot_longer(c(raw_value, index_value),
                 names_to  = "data_type",
                 values_to = "value") %>% 
    mutate(
      tech         = tech_name,
      supply_chain = "Upstream",
      category     = "Reserves",
      variable     = str_glue("{tech_name} Reserves"),
      data_type    = if_else(data_type == "raw_value", "raw", "index"),
      source       = "EI Statistical Review of World Energy (2024)",
      explanation  = case_when(
        data_type == "raw"   ~ str_glue("{tech_name} reserves ({unit_desc}) from sheet {sheet}"),
        data_type == "index" ~ "Percent-rank of reserves across reporting entities (countries + RoW)"
      )
    ) %>% 
    select(Country, tech, supply_chain, category, variable,
           data_type, value, source, explanation)
}


path_excel <- paste0(raw_data, "ei_stat_review_world_energy_wide.xlsx")

oil_res <- read_reserves(
  path        = path_excel,
  sheet       = 13,
  skip        = 4,
  nm_col      = "Thousand million barrels",
  val_col     = "2020...42",
  tech_name   = "Oil",
  unit_desc   = "Thousand million barrels"
)

gas_res <- read_reserves(
  path        = path_excel,
  sheet       = 32,
  skip        = 4,
  nm_col      = "Trillion cubic metres",
  val_col     = "2020...42",
  tech_name   = "Gas",
  unit_desc   = "Trillion cubic metres"
)

coal_res <- read_reserves(
  path        = path_excel,
  sheet       = 46,
  skip        = 5,
  nm_col      = "Million tonnes",
  val_col     = "Total",
  tech_name   = "Coal",
  unit_desc   = "Million tonnes"
)

cobalt_res <- read_reserves(
  path        = path_excel,
  sheet       =83,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "At end of 2024",
  tech_name   = "Cobalt",
  unit_desc   = "Thousand tonnes"
)

lithium_res <- read_reserves(
  path        = path_excel,
  sheet       =84,
  skip        = 2,
  nm_col      = "Thousand tonnes of Lithium content",
  val_col     = "At end of 2024",
  tech_name   = "Lithium",
  unit_desc   = "Thousand tonnes"
)


graphite_res <- read_reserves(
  path        = path_excel,
  sheet       =85,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "At end of 2024",
  tech_name   = "Graphite",
  unit_desc   = "Thousand tonnes"
)

rareearths_res <- read_reserves(
  path        = path_excel,
  sheet       =86,
  skip        = 2,
  nm_col      = "Thousand tonnes1",
  val_col     = "At end of 2024",
  tech_name   = "Rare Earths",
  unit_desc   = "Thousand tonnes"
)

copper_res <- read_reserves(
  path        = path_excel,
  sheet       =87,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "At end of 2024",
  tech_name   = "Copper",
  unit_desc   = "Thousand tonnes"
)

manganese_res <- read_reserves(
  path        = path_excel,
  sheet       =88,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "At end of 2024",
  tech_name   = "Manganese",
  unit_desc   = "Thousand tonnes"
)

nickel_res <- read_reserves(
  path        = path_excel,
  sheet       =89,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "At end of 2024",
  tech_name   = "Nickel",
  unit_desc   = "Thousand tonnes"
)

zinc_res <- read_reserves(
  path        = path_excel,
  sheet       =90,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "At end of 2024",
  tech_name   = "Zinc",
  unit_desc   = "Thousand tonnes"
)

pgm_res <- read_reserves(
  path        = path_excel,
  sheet       =91,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "At end of 2024",
  tech_name   = "PGMs",
  unit_desc   = "Thousand tonnes"
)


#Critical Minerals Reserves
critical_min_reserves<-bind_rows(cobalt_res,
                                 lithium_res,
                                 copper_res,
                                 rareearths_res,
                                 graphite_res,
                                 manganese_res,
                                 zinc_res,
                                 nickel_res,
                                 pgm_res) %>%
  rename("Mineral"="tech") %>%
  inner_join(mineral_demand_clean %>%
               ungroup() %>%
               select(Mineral,tech,share_24)%>%                                        #SM correction to share 24: share 23 does not exist
               mutate(Mineral=ifelse(grepl("rare",Mineral),"Rareearths",
                                     ifelse(Mineral=="Battery-grade graphite","Graphite",Mineral))),
             by=c("Mineral")
  ) %>%
  filter(!is.na(tech)) %>%
  group_by(Country,tech,data_type) %>%
  mutate(share_24=share_24/sum(share_24)) %>% ungroup() 

critical_min_res<-critical_min_reserves %>%
  filter(data_type=="index") %>%
  group_by(Country,tech,supply_chain,category,data_type,source) %>%
  summarize(value=weighted.mean(value,w=share_24,na.rm=T)) %>%   
  mutate(variable=str_glue("{tech} Reserves"),
         explanation="Weighted average of reserve availability by share of demand in technology") %>%
  filter(value>0) %>%
  group_by(tech,supply_chain) %>%
  mutate(value=median_scurve(value)) %>%
  rbind(critical_min_reserves %>%
          select(-tech) %>%
          rename("tech"="Mineral") %>%
          distinct(Country,tech,supply_chain,category,data_type,variable, value,source,explanation)
  ) %>%
  group_by(tech, supply_chain, category, data_type, source, variable, explanation) %>%
  complete(Country = all_countries, fill = list(value = 0)) 


#-- 3. Combine all reserves tidily --
reserves_clean <- bind_rows(critical_min_res, oil_res, gas_res, coal_res) %>%
  mutate(Country=ifelse(Country=="US","United States",Country)) %>%
  group_by(tech, supply_chain, category, data_type, source, variable, explanation) %>%
  complete(Country = all_countries, fill = list(value = 0)) 

#-- Inspect --
View(reserves_clean)

#Critical Mineral Production----------
minerals <- critical %>%
  filter(grepl("Total supply",Pillar),
         !grepl("Top 3|Total", `Sector.Country`),
         Sector.Country !="",
         Mineral!="") %>%
  separate(Mineral, into = c("mineral", "supply_chain_raw"), sep = " - ", extra = "merge") %>%
  distinct(mineral)
countries <- inner_join(critical %>%
                          distinct(Sector.Country),country_info %>% select(country),by=c("Sector.Country"="country"))

mineral_supply<-critical %>%
  filter(grepl("Total supply",Pillar),
         !grepl("Top 3|Total", `Sector.Country`),
         Sector.Country !="",
         Mineral!="")  %>%
  # since Sector.Country here is actually country, rename it for clarity
  rename(country = Sector.Country) %>%
  # split Mineral in the same way
  separate(Mineral, into = c("mineral", "supply_chain_raw"), sep = " - ", extra = "merge") %>%
  #mutate(supply_chain ="Upstream") %>%
  mutate(
    supply_chain = if_else(
      str_detect(supply_chain_raw, regex("Refining|Chemical", ignore_case=TRUE)),
      "Midstream",
      "Upstream"
    )
  ) %>%
  ungroup() %>%
  select(mineral,country,supply_chain,X2024,X2035) %>%
  tidyr::complete(
    mineral      = minerals$mineral,           # or a vector you build
    supply_chain = c("Upstream","Midstream"),
    country      = countries$Sector.Country,
    fill = list(
      X2024     = 0,
      X2035     = 0
    )
  ) %>%
  inner_join(mineral_demand_clean %>%
               filter(!is.na(tech)) %>%
               ungroup() %>%
               select(Mineral,tech,share_24,share_35),by=c("mineral"="Mineral")) %>%
  mutate(supply_24=share_24*X2024,
         supply_35=share_35*X2035) %>%
  group_by(mineral,tech,supply_chain) %>%
  mutate(
    # convert percent ??? fraction
    share_frac_24 = supply_24/sum(supply_24,na.rm=T),
    # HHI = sum of squared shares
    HHI_24 = sum(share_frac_24^2, na.rm = TRUE),
    share_frac_35 = supply_35/sum(supply_35,na.rm=T),
    # HHI = sum of squared shares
    HHI_35 = sum(share_frac_35^2, na.rm = TRUE)
  ) %>%
  filter(country != "Rest of world",
         !is.na(share_frac_24),
         !is.na(share_frac_35)
  ) %>%
  ungroup() %>%
  mutate(market_share24_index=median_scurve(share_frac_24),
         market_share35_index=median_scurve(share_frac_35),
         hhi24_index=median_scurve(1-HHI_24),
         hhi35_index=median_scurve(1-HHI_35)) %>%
  rowwise() %>%    # allows c_across() to work row-wise
  mutate(
    security_index    = mean(c_across(c(market_share24_index:hhi35_index)),  na.rm = TRUE)) %>%
  group_by(supply_chain) %>%
  #mutate(security_index=median_scurve(security_index)) %>%
  arrange(desc(security_index)) 

criticalmineral_supply <- mineral_supply %>%
  group_by(country,tech,supply_chain) %>%
  summarize(security_index=weighted.mean(security_index,w=share_35,na.rm=T)) %>%
  group_by(tech,supply_chain) %>%
  #mutate(security_index=median_scurve(security_index)) %>%
  pivot_longer(
    cols      = c(security_index),
    names_to  = "name",
    values_to = "value"
  ) %>%
  mutate(
    category = "Foreign Dependency",
    variable = "Mineral Supply",
    data_type    = "index",
    source       = "IEA Critical Minerals Database",
    explanation  = case_when(
      data_type == "index" ~ str_glue(
        "Percent-rank of {tech} market share across countries, and HHI across critical minerals"
      )
    )
  ) %>%
  select(country, tech, supply_chain, category,variable,data_type, value, source, explanation) %>%
  group_by(tech, supply_chain, category, data_type, source, variable, explanation) %>%
  complete(country = all_countries, fill = list(value = 0)) 



#Critial Mineral Production (EI Data)--------------
read_production <- function(path, sheet, skip, nm_col, val_col,
                            tech_name, unit_desc, include_row = TRUE){
  
  raw <- read_excel(path, sheet = sheet, skip = skip) %>% 
    rename(Country = all_of(nm_col),
           raw_value = all_of(val_col)) %>% 
    mutate(
      Country = case_when(
        Country %in% c("Rest of World", "Rest of world", "Rest of World^") ~ "Rest of World",
        Country == "US"       ~ "United States",
        Country == "DR Congo" ~ "Democratic Republic of Congo",
        Country == "Russia Federation" ~ "Russia",
        TRUE                  ~ Country)
    ) %>% 
    filter(
      (Country %in% all_countries),
      !is.na(Country),
      !grepl("Total World|Other|OECD|OPEC|Orinoco", Country)
    ) %>% 
    mutate(raw_value = as.numeric(raw_value) %>% replace_na(0)) 
  
  dummy_zero <- tibble(Country = "_ZERO_", raw_value = 0)
  
  ranked <- bind_rows(raw, dummy_zero) %>%
    mutate(index_value = median_scurve(raw_value)) %>%
    filter(Country != "_ZERO_") %>% 
    tidyr::complete(
      Country = all_countries,
      fill = list(raw_value = 0,
                  index_value=0)
    ) %>% 
    pivot_longer(c(raw_value, index_value),
                 names_to  = "data_type",
                 values_to = "value") %>% 
    mutate(
      tech         = tech_name,
      supply_chain = "Upstream",
      category     = "Production",
      variable     = str_glue("{tech_name} Production"),
      data_type    = if_else(data_type == "raw_value", "raw", "index"),
      source       = "EI Statistical Review of World Energy (2024)",
      explanation  = case_when(
        data_type == "raw"   ~ str_glue("{tech_name} production ({unit_desc}) from sheet {sheet}"),
        data_type == "index" ~ "Percent-rank of production across reporting entities (countries + RoW)"
      )
    ) %>% 
    select(Country, tech, supply_chain, category, variable,
           data_type, value, source, explanation)
}

cobalt_prod <- read_production(
  path        = path_excel,
  sheet       =83,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "2024...31",
  tech_name   = "Cobalt",
  unit_desc   = "Thousand tonnes"
)

lithium_prod <- read_production(
  path        = path_excel,
  sheet       =84,
  skip        = 2,
  nm_col      = "Thousand tonnes of Lithium content",
  val_col     = "2024...31",
  tech_name   = "Lithium",
  unit_desc   = "Thousand tonnes"
)


graphite_prod <- read_production(
  path        = path_excel,
  sheet       =85,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "2024...31",
  tech_name   = "Graphite",
  unit_desc   = "Thousand tonnes"
)

rareearths_prod <- read_production(
  path        = path_excel,
  sheet       =86,
  skip        = 2,
  nm_col      = "Thousand tonnes1",
  val_col     = "2024...31",
  tech_name   = "Rare Earths",
  unit_desc   = "Thousand tonnes"
)

copper_prod <- read_production(
  path        = path_excel,
  sheet       =87,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "2024...12",
  tech_name   = "Copper",
  unit_desc   = "Thousand tonnes"
)

manganese_prod <- read_production(
  path        = path_excel,
  sheet       =88,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "2024...13",
  tech_name   = "Manganese",
  unit_desc   = "Thousand tonnes"
)

nickel_prod <- read_production(
  path        = path_excel,
  sheet       =89,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "2024...13",
  tech_name   = "Nickel",
  unit_desc   = "Thousand tonnes"
)

zinc_prod <- read_production(
  path        = path_excel,
  sheet       =90,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "2024...13",
  tech_name   = "Zinc",
  unit_desc   = "Thousand tonnes"
)

pgm_prod <- read_production(
  path        = path_excel,
  sheet       =91,
  skip        = 2,
  nm_col      = "Thousand tonnes",
  val_col     = "2024...12",
  tech_name   = "PGMs",
  unit_desc   = "Thousand tonnes"
)

critical_min_production<-bind_rows(cobalt_prod,
                                   lithium_prod,
                                   copper_prod,
                                   rareearths_prod,
                                   graphite_prod,
                                   manganese_prod,
                                   zinc_prod,
                                   nickel_prod,
                                   pgm_prod) %>%
  rename("Mineral"="tech") %>%
  inner_join(mineral_demand_clean %>%
               ungroup() %>%
               select(Mineral,tech,share_24)%>%                                        #SM correction to share 24: share 23 does not exist
               mutate(Mineral=ifelse(grepl("rare",Mineral),"Rareearths",
                                     ifelse(Mineral=="Battery-grade graphite","Graphite",Mineral))),
             by=c("Mineral")
  ) %>%
  filter(!is.na(tech)) %>%
  group_by(Country,tech,data_type) %>%
  mutate(share_24=(share_24/sum(share_24))) %>% ungroup() 

critical_min_prod<-critical_min_production %>%
  filter(data_type=="index") %>%
  group_by(Country,tech,supply_chain,category,data_type,source,explanation) %>%
  summarize(value=weighted.mean(value,w=share_24,na.rm=T))%>%   #SM correction to share 24: share 23 does not exist
  mutate(variable=str_glue("{tech} Production")) %>%
  filter(value>0) %>%
  group_by(tech) %>%
  mutate(value=median_scurve(value)) %>%
  rbind(critical_min_production %>%
          select(-tech) %>%
          rename("tech"="Mineral") %>%
          distinct(Country,tech,supply_chain,category,data_type,variable, value,source,explanation)
  ) %>%
  group_by(tech, supply_chain, category, data_type, source, variable, explanation) %>%
  complete(Country = all_countries, fill = list(value = 0)) 

#Critical Minerals - USGS Data ----------------------------------
usgs <- read.csv(paste0(raw_data,"MCS2025_World_Data.csv")) %>%
  mutate(COMMODITY = recode(COMMODITY,
    "Platinum-Group metals"="PGMs"
  ),
  RESERVES_2024=as.numeric(RESERVES_2024)) 

# helper flags
is_world <- function(x) str_detect(x, regex("^World", ignore_case = TRUE))
is_other <- function(x) str_detect(x, regex("^Other", ignore_case = TRUE))

# S-curve that expects a percentile r in [0,1]
scurve_from_rank <- function(r, gamma = 0.5) (r^gamma) / (r^gamma + (1 - r)^gamma)

# Weighted mid-rank percentile (handles ties like percent_rank's "mid" behavior)
weighted_midrank <- function(x, w) {
  res <- rep(NA_real_, length(x))
  ok  <- !is.na(x) & !is.na(w) & w > 0
  if (!any(ok)) return(res)
  xv <- x[ok]; wv <- w[ok]
  tot <- sum(wv)
  u   <- sort(unique(xv))
  lower <- vapply(u, function(v) sum(wv[xv < v]), numeric(1))
  equal <- vapply(u, function(v) sum(wv[xv == v]), numeric(1))
  ranks <- (lower + 0.5 * equal) / tot
  res[ok] <- ranks[match(xv, u)]
  res
}

# Choose M dynamically per group/column (fallback to 10)
estimate_M <- function(share, other_flag, world_flag) {
  other_share <- sum(share[other_flag], na.rm = TRUE)
  named_min   <- suppressWarnings(min(share[!other_flag & !world_flag & share > 0], na.rm = TRUE))
  if (is.finite(named_min) && named_min > 0 && is.finite(other_share) && other_share > 0) {
    M <- round(other_share / named_min)
    M <- max(1, min(M, 50))  # clamp for sanity
  } else {
    M <- 10
  }
  M
}

usgs_idx <- usgs %>%
  group_by(COMMODITY, TYPE) %>%
  mutate(
    across(
      where(is.numeric),
      \(col) {
        wflag <- is_world(COUNTRY)
        oflag <- is_other(COUNTRY)
        
        W <- suppressWarnings(first(na.omit(col[wflag])))
        if (is.na(W) || W == 0) return(rep(NA_real_, length(col)))
        
        share <- col / W
        
        # weights: 1 for named countries, 0 for world, M for "Other Countries"
        wt <- rep(1, length(share))
        wt[wflag] <- 0
        if (any(oflag, na.rm = TRUE)) wt[oflag] <- estimate_M(share, oflag, wflag)
        
        r <- weighted_midrank(share, wt)         # percentile including "Other" mass
        idx <- scurve_from_rank(r, gamma = 0.5)  # apply your median_scurve shape
        idx[wflag] <- NA_real_                   # no index for world rows
        idx
      },
      .names = "{.col}_idx"
    )
  ) %>%
  ungroup() %>%
  mutate(index=PROD_EST_.2024_idx,
         index=ifelse(!is.na(PROD_EST_.2024_idx),PROD_EST_.2024_idx,
                      ifelse(!is.na(PROD_2023_idx),PROD_2023_idx,
                             ifelse(!is.na(CAP_EST_.2024_idx),CAP_EST_.2024_idx,
                                    ifelse(!is.na(RESERVES_2024_idx),RESERVES_2024_idx,0)))))

usgs_et<-usgs_idx %>%
  select(COUNTRY,COMMODITY,TYPE,UNIT_MEAS,index) %>%
  filter(!grepl("Other|World",COUNTRY)) %>%
  inner_join(mineral_demand_clean %>%
               mutate(Mineral = recode(Mineral,
                                         "Battery-grade graphite"="Graphite"
               )),by=c("COMMODITY"="Mineral") ) %>%
  group_by(COUNTRY,tech) %>%
  mutate(share_24=share_24/sum(share_24)) %>% ungroup() %>%
  group_by(COUNTRY,tech) %>%
  
  summarize(usgs_index=weighted.mean(index,
                                                            share_24,
                                                            na.rm=T))



#Energy Imports----------------------------------
fossil_imports<-ei%>%
  select(Country,Year,Var,Value) %>%
  filter(!grepl("Other|Total|OECD|OPEC", Country),
         Year %in% c("2024"),
         Var %in% c("oilcons_ej",
                    "oilcons_kbd",
                    "oilprod_kbd",
                    "primary_ej_pc",
                    "gasprod_ej",
                    "gascons_ej",
                    "coalcons_ej",
                    "coalprod_ej",
                    "ren_power_ej",
                    "ren_power_twh",
                    "primary_ej",
                    "electbyfuel_ren_power",
                    "electbyfuel_total")) %>%
  distinct() %>%
  pivot_wider(names_from=Var,values_from=Value) %>%
  filter(oilcons_ej != 0) %>%
  mutate(across(c(coalcons_ej:coalprod_ej), ~replace_na(., 0))) %>%                               # or whatever your dataframe is called
  mutate(
    ## ?????? 1. derive all import flows & shares ?????????????????????????????????????????????????????????????????????????????????
    oil_calc        = oilcons_ej / oilcons_kbd,
    oilprod_ej      = oil_calc * oilprod_kbd,
    oil_imports_ej  = oilprod_ej  - oilcons_ej,
    oil_imports_kbd = oilprod_kbd - oilcons_kbd,
    
    gas_imports_ej      = gasprod_ej  - gascons_ej,
    coal_imports_ej     = coalprod_ej - coalcons_ej,
    fossil_imports_ej   = oil_imports_ej + gas_imports_ej + coal_imports_ej,
    
    oil_imports_share   = 100 * oil_imports_ej   / oilcons_ej,
    gas_imports_share   = 100 * gas_imports_ej   / gascons_ej,
    coal_imports_share  = 100 * coal_imports_ej  / coalcons_ej,
    fossil_import_share = 100 * fossil_imports_ej/ (oilcons_ej+gascons_ej+coalcons_ej)  ) %>% 
  mutate(
    oil_import_index    = median_scurve(oil_imports_share),
    gas_import_index    = median_scurve(gas_imports_share),
    coal_import_index    = median_scurve(coal_imports_share),
    fossil_import_index    = median_scurve(fossil_import_share)
  ) %>% 
  ungroup()

imports_clean <- fossil_imports %>%
  # rename the ren_gen_Index column so it fits our pivot pattern
  #rename(ren_gen_index = ren_gen_Index) %>%
  
  # pick out the raw vs. composite-index columns we care about
  select(
    Country,
    oil_imports_ej,    oil_import_index,
    gas_imports_ej,    gas_import_index,
    coal_imports_ej,   coal_import_index,
    fossil_imports_ej, fossil_import_index
  ) %>%
  
  # give friendly names that follow "<Tech>_<raw|index>"
  rename_with(~ str_replace(.x, "oil_imports_ej",    "Oil_raw"),    everything()) %>%
  rename_with(~ str_replace(.x, "oil_import_index",  "Oil_index"),  everything()) %>%
  rename_with(~ str_replace(.x, "gas_imports_ej",    "Gas_raw"),    everything()) %>%
  rename_with(~ str_replace(.x, "gas_import_index",  "Gas_index"),  everything()) %>%
  rename_with(~ str_replace(.x, "coal_imports_ej",   "Coal_raw"),   everything()) %>%
  rename_with(~ str_replace(.x, "coal_import_index", "Coal_index"), everything()) %>%
  rename_with(~ str_replace(.x, "fossil_imports_ej", "Fossil_raw"), everything()) %>%
  rename_with(~ str_replace(.x, "fossil_import_index","Fossil_index"), everything()) %>%
  
  # pivot into long form
  pivot_longer(
    cols       = -Country,
    names_to   = c("tech","data_type"),
    names_pattern = "^(.*)_(raw|index)$",
    values_to  = "value"
  ) %>%
  
  # add the remaining cols
  mutate(
    supply_chain = "Upstream",
    category="Energy Imports",
    variable = "Production surplus/deficit",
    source       = "EI Statistical Review of World Energy (2024)",
    explanation  = case_when(
      data_type=="raw" & tech=="Oil"        ~ "Oil import share (%) = oil production minus consumption as a share of consumption",
      data_type=="raw" & tech=="Gas"        ~ "Gas import share (%) = gas production minus consumption as a share of consumption",
      data_type=="raw" & tech=="Coal"       ~ "Coal import share (%) = coal production minus consumption as a share of consumption",
      data_type=="raw" & tech=="Fossil"     ~ "Fossil import share (%) = sum of oil, gas, and coal imports as a share of consumption",
      data_type=="raw" & tech=="Renewables" ~ "Renewable generation share (%) = electbyfuel_ren_power � electbyfuel_total � 100",
      
      data_type=="index" & tech %in% c("Oil","Gas","Coal","Fossil") ~
        str_glue("Mean of percent-ranked import share & absolute imports for {tech}"),
      data_type=="index" & tech=="Renewables" ~
        "Percent-rank of renewable-generation share across countries"
    )
  ) %>%
  select(Country, tech, supply_chain, category,variable,data_type, value, source, explanation) %>%
  mutate(Country=ifelse(Country=="US","United States",Country))

##Chart:
# choose fuels and peer set (modify as you like)
tech_order <- c("Oil", "Gas", "Coal")
peers <- c("United States", "China", "Germany", "Japan", "United Kingdom",
           "France", "Italy", "Canada", "India","Australia","South Korea","Russian Federation")

plot_df <- imports_clean %>% 
  filter(variable == "Production surplus/deficit",
         data_type == "raw",
         tech %in% tech_order,
         Country %in% peers) %>% 
  # keep order within each facet by magnitude (largest deficit at top)
  mutate(Country = reorder_within(Country, value, tech),
         is_US   = Country == "United States",
         surplus = ifelse(value>0,"TRUE","FALSE"))

ggplot(plot_df,
       aes(x = value, y = Country, fill = surplus)) +
  geom_col(width = 0.7) +
  scale_fill_manual(values = c("TRUE" = "#003A61", "FALSE" = "#F8931D"),
                    guide = "none") +
  #scale_x_continuous(labels = label_number(accuracy = 0.1),
  #breaks = pretty_breaks(),
  # expand = expansion(mult = 0.05)) +
  tidytext::scale_y_reordered() +
  facet_wrap(~ tech, scales = "free_y") +
  labs(title = "Energy Import Dependence vs. Surplus (latest year)",
       subtitle = "Positive = net exporter; negative = net importer.  U.S. bar highlighted.",
       x = "Production - Consumption (EJ)",
       y = NULL) +
  theme_minimal(base_size = 11) +
  theme(panel.grid.major.y = element_blank(),
        strip.text = element_text(face = "bold"))

#Cleantech Midstream---------------------------------------
cleantech_man <- read.csv(paste0(raw_data, "iea_cleantech_Midstream.csv")) %>%
  rename(Country = X) %>%
  mutate(Country=ifelse(Country=="US","United States",Country))

cleantech_long <- cleantech_man %>%
  pivot_longer(
    cols        = -Country,
    names_to    = c("Technology","Status"),
    names_pattern = "(.*)\\.\\.\\.(Current|Pipeline)",
    values_to   = "Value"
  ) %>%
  mutate(
    Year = if_else(Status == "Current", "2024", "2035")
  )

# 2) Build country grouping (collapse EU members ??? "EU")
country_map <- ei %>%
  distinct(Country, EU) %>%
  mutate(
    Country_clean = if_else(Country == "Vietnam", "Viet Nam", Country),
    country2      = if_else(EU == 1,             "EU", Country_clean)
  ) %>%
  select(country2) %>%
  distinct()

# 3) Join & keep only 2035, compute each tech's global market share
ct_ms <- country_map %>%
  left_join(
    cleantech_long,
    by = c("country2" = "Country")
  ) %>%
  filter(Year == "2035") %>%
  group_by(Technology) %>%
  mutate(
    market_share = Value / sum(Value, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  select(Country = country2, Technology, market_share) 

# 4) Percent-rank each tech's share
ct_idx <- ct_ms %>%
  group_by(Technology) %>%
  mutate(
    market_share_index = median_scurve(market_share)
  ) %>%
  ungroup()

# 5) Pivot raw vs. index into the tidy long format
cleantech_clean <- ct_idx %>%
  pivot_longer(
    cols      = c(market_share, market_share_index),
    names_to  = "name",
    values_to = "value"
  ) %>%
  mutate(
    tech         = Technology,
    supply_chain = "Midstream",
    category = "Foreign Dependency",
    variable = "Market Share",
    data_type    = if_else(name == "market_share", "raw", "index"),
    source       = "IEA Energy Technology Perspectives 2024",
    explanation  = case_when(
      data_type == "raw" ~ str_glue(
        "2035 market share for {tech} = Value � global total for {tech}"
      ),
      data_type == "index" ~ str_glue(
        "Percent-rank of {tech} market share across countries"
      )
    )
  ) %>%
  select(Country, tech, supply_chain, category,variable,data_type, value, source, explanation)

# 6) Composite clean-tech Midstream index
comp_ct <- ct_idx %>%
  group_by(Country) %>%
  summarize(
    mean_idx = mean(market_share_index, na.rm = TRUE)
  ) %>%
  mutate(
    composite_index = median_scurve(mean_idx),
    tech            = "Clean Tech Midstream",
    supply_chain    = "Midstream",
    category = "Foreign Dependency",
    variable = "Market Share",
    data_type       = "index",
    source          = "IEA Energy Technology Perspectives 2024",
    explanation     = "Percent-rank of mean of individual clean-tech Midstream indexes(NB: All EU countries are given same market share)"
  ) %>%
  select(Country, tech, supply_chain, category, variable,data_type,
         value = composite_index, source, explanation)

# 7) Bind them together
cleantech_final <- bind_rows(cleantech_clean, comp_ct) %>%
  mutate(Country=ifelse(Country=="US","United States",Country))

cleantech_clean <- ei %>%
  distinct(Country,EU) %>%
  mutate(Country=ifelse(Country=="Vietnam","Viet Nam",Country),
         country2=ifelse(EU==1,"EU",Country),
         country2=ifelse(country2=="US","United States",country2)) %>%
  left_join(cleantech_final,by=c("country2"="Country")) %>%
  filter(!is.na(tech)) %>%
  select(-country2,-EU) %>%
  mutate(Country=ifelse(Country=="US","United States",Country))

##Chart:
tech_order <- c("Solar", "Wind", "Batteries", "Electrolyzers", "Heat.Pumps")
top_n      <- 8                       # show top-8 manufacturers per tech

plot_df <- cleantech_clean %>% 
  filter(variable == "Market Share",
         data_type == "raw",
         tech %in% tech_order) %>% 
  group_by(tech) %>% 
  #filter(rank(desc(value)) <= top_n | Country == "United States") %>% 
  ungroup() %>% 
  mutate(is_US   = Country == "United States",
         Country = reorder_within(Country, value, tech))

ggplot(plot_df,
       aes(x = value, y = Country, fill = is_US)) +
  geom_col(width = 0.8) +
  scale_fill_manual(values = c("TRUE" = "#e34a33",   # U.S. in red
                               "FALSE" = "#6baed6"), guide = "none") +
  scale_x_continuous(labels = percent_format(accuracy = 1),
                     expand = expansion(mult = 0.03)) +
  tidytext::scale_y_reordered() +
  facet_wrap(~ tech, scales = "free_y") +
  labs(title = "Global Midstream Market Share by Clean-Tech (2035 projection)",
       subtitle = "Top producers (share of world output).  U.S. bar highlighted.",
       x = "Share of global Midstream output",
       y = NULL,
       caption = "Source: IEA *Energy Technology Perspectives�2024*; author calculations.") +
  theme_minimal(base_size = 11) +
  theme(panel.grid.major.y = element_blank(),
        strip.text = element_text(face = "bold"))


#EV Midstream - IEA EV Outlook----------------------
ev_man<-read.csv(paste0(raw_data,"ev_Midstream_capacity.csv")) %>%
  filter(Year=="2024") %>%
  mutate(EU=ifelse(Region=="European Union",1,2)) %>%
  left_join(ei %>% distinct(Country,EU),by=c("EU")) %>%
  mutate(SubRegion=ifelse(Region=="North America","North America",
                          ifelse(Region=="Other Asia Pacific","Asia Pacific",
                                 ifelse(Region=="European Union","EU",
                                        ifelse(Region=="China","CHN","Rest of World"))))) %>%
  left_join(ei %>% 
              filter(Country != "China") %>%
              distinct(Country,SubRegion),by=c("SubRegion")) %>%
  left_join(ei %>% distinct(Country,SubRegion)
            %>% mutate(SubRegion=ifelse(SubRegion %in% c("Asia Pacific","North America","Europe"),"boop","Rest of World")),by=c("SubRegion")) %>%
  mutate(country=ifelse(Region=="China","China",
                        ifelse(SubRegion=="North America",Country.y,
                               ifelse(SubRegion=="Asia Pacific",Country.y,
                                      ifelse(EU==1,Country.x,Country))))) %>%
  mutate(production_index=median_scurve(Domestic.Production),
         sales_index=median_scurve(Domestic.sales),
         import_index=median_scurve(import_share),
         market_share_index=median_scurve(market_share)) %>%
  rowwise() %>%
  mutate(ev_Midstream_index=mean(production_index:market_share_index)) %>%
  select(country,Domestic.sales:market_share,production_index:ev_Midstream_index) %>%
  pivot_longer(
    cols      = -c(country),
    names_to  = "variable",
    values_to = "value"
  ) %>%
  # 2. add the new constant columns
  mutate(
    tech= "Electric Vehicles", 
    supply_chain = "Midstream",
    category     = "Foreign Dependency",
    # 3. classify raw vs index based on the _index suffix
    data_type = if_else(str_detect(variable, "_index$"),
                        "index", "raw"),
    # 4. strip off the "_index" so variable matches the pre-index name
    variable  = str_remove(variable, "_index$"),
    # 5. fill in source/explanation
    source      = "IEA EV Outlook",
    explanation = "Combined index of domestic sales, production, imports and market share"
  ) %>%
  # 6. match the column names/order of trade_tidy
  rename(Country = country) %>%
  select(
    Country, tech, supply_chain, category,
    variable, data_type, value, source, explanation
  ) %>%
  distinct()



#Trade-------------------------------
greenplexity <- read.csv(paste0(raw_data,"/greenplexity_scrape_full.csv")) %>%
  mutate(gross_export=as.numeric(str_remove_all(gross_export, "[$, ]")),
         Gross.Import=as.numeric(str_remove_all(Gross.Import, "[$, ]")),
         sector=ifelse(sector=="Stone","Minerals",sector)) 

green_codes<-greenplexity %>%
  mutate(phase=ifelse(sector %in% c("Agriculture",
                                    "Minerals",
                                    "Metals"),
                      "Upstream",
                      ifelse(sector %in% c("Chemicals",
                                           "Machinery",
                                           "Electronics"),
                             "Midstream",
                             ifelse(sector=="Vehicles","Midstream",NA))),
         supplyChain=ifelse(grepl("Solar",supplyChain),"Solar",
                            ifelse(grepl("Wind",supplyChain),"Wind",supplyChain)),
         industry=paste(supplyChain,phase)) %>%
  select(supplyChain,nameShortEn,sector,industry,code) %>% distinct()

#all sectors
hs_codes<-read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/hts_codes_categories.csv')
hs_codes<-hs_codes %>%
  mutate(code_10=substr(HTS.Code,1,10),
         code_6=substr(HTS.Code,1,6),
         code_4=substr(HTS.Code,1,4)) %>%
  select(-Start.Year.Valid,-End.Year.Valid)

sectors <- hs_codes %>%
  mutate(
    industry = case_when(
      Sub.Sector == "Wind"                       ~ "Wind Midstream",
      str_detect(Sub.Sector, "Solar")            ~ "Solar Midstream",
      str_detect(Sub.Sector, "Battery Inputs")   ~ "Batteries Upstream",
      str_detect(Sub.Sector, "Batteries and Parts") ~ "Batteries Midstream",
      Sub.Sector %in% c(
        "Lubricating Oils", "Quenching Oils", "Greases", "Mineral Oils",
        "Petroleum Oils: Other", "Waste Oils", "Fuel Oils: No. 2 & 3",
        "Fuel Oils: Light Oils", "Fuel Oils: No. 4", "Fuel Oils: Heavy Oils",
        "Kerosene", "Petroleum Solids", "Gasoline", "Naphtha",
        "Motor Fuel", "Fuel Oils: Other"
      )                                          ~ "Oil Upstream",
      Sector == "Thermal Power" & Sub.Sector %in% c("Internal Combustion Generating\nSets","Generators") ~ "Oil Midstream",
      Sector == "Thermal Power" & Sub.Sector== "Steam (Coal, Nuclear,\nGeothermal, Combined Cycle\nGas, Etc.)" ~ "Coal Midstream; Gas Midstream; Nuclear Midstream",
      Sub.Sector == "Crude Oils"                 ~ "Oil Upstream",
      Sub.Sector %in% c("Gas","Ethylene", "Propylene", "Butylene", "Butadiene")
      ~ "Gas Midstream",
      Sub.Sector %in% c("Propane", "Butane", "Ethane", "Petroleum Gases","Natural Gas")
      ~ "Gas Upstream",
      str_detect(Sector, "Coal")                 ~ "Coal Upstream",
      Sub.Sector == "Nuclear Fuel"               ~ "Nuclear Upstream",
      Sector == "Electricity\nInfrastructure"    ~ "Electric Grid Midstream",
      Sub.Sector == "Equipment"                  ~ "Nuclear Midstream",
      Sector == "Fossil Energy:\nEquipment"  ~ "Oil Midstream; Gas Midstream",
      # default:
      TRUE                                       ~ NA
    )
  ) %>%
  separate_rows(industry, sep = "\\s*;\\s*") %>%
  mutate(industry = str_squish(industry))


# Inspect the result
glimpse(sectors)


energy_codes<-rbind(sectors %>%
                      rename(code=code_4,
                             product=HTS.Code) %>%
                      select(industry,code,product),
                    green_codes %>%
                      rename(product=nameShortEn) %>%
                      select(industry,code,product)) %>%
  distinct() %>%
  mutate(industry=str_replace_all(industry," NA"," Upstream"),
         industry=str_replace_all(industry,"Battery","Batteries"),
         industry=str_replace_all(industry,"Nuclear Power","Nuclear"),
         industry=str_replace_all(industry,"Critical Minerals","Batteries")) %>%
  mutate(code6 = stringr::str_extract(product, "^\\d{6}"))

#4 Digit
aec_4_data<-read.csv(paste0(raw_data,"hs92_country_product_year_4.csv")) %>%
  filter(year=="2022") %>%
  inner_join(energy_codes,by=c("product_hs92_code"="code"))

aec_4_all <- aec_4_data  %>%
  distinct(country_iso3_code,industry,product_hs92_code,export_value,import_value,export_rca,distance) %>%
  group_by(country_iso3_code,industry) %>%
  summarize(exports=sum(export_value,na.rm=T),
            imports=sum(import_value,na.rm=T),
            export_rca=weighted.mean(export_rca,w=export_value,na.rm=T),
            feasibility=1-mean(distance,na.rm=T)) %>%
  group_by(industry) %>%
  mutate(market_share=exports/sum(exports)*100)


trade_indices <- aec_4_all %>%
  ungroup() %>%
  right_join(country_info %>% 
               select(country,iso3c),by=c("country_iso3_code"="iso3c")) %>%
  select(country,industry,exports,export_rca,feasibility) %>%
  group_by(industry) %>%
  mutate(export_size_index=median_scurve(exports),
         rca_index=median_scurve(export_rca),
         feas_index=median_scurve(feasibility)) %>%
  rowwise() %>%
  mutate(trade_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  group_by(industry) %>%
  mutate(trade_index = median_scurve(trade_index))


#6 Digit
aec_6_data<-read.csv(paste0(raw_data,"hs92_country_product_year_6.csv"))

aec_ts<-aec_6_data %>%
  left_join(sectors,by=c("product_hs92_code"="code_6"))%>%
  distinct(year,country_iso3_code,industry,product_hs92_code,export_value,import_value,global_market_share) %>%
  mutate(industry=str_replace_all(industry," NA"," Downstream"),
         industry=str_replace_all(industry,"Battery","Batteries"))%>%
  group_by(industry,year) %>%
  summarize(imports=sum(import_value,na.rm=T)) %>%
  ungroup() 

aec_ts_index<-aec_ts %>%
  arrange(desc(year)) %>%
  group_by(industry) %>%
  mutate(import_3yma=rollmean(imports, k = 3, align = "left", fill = NA)) %>%
  mutate(imp_index_15 = 100*import_3yma/import_3yma[year=="2010"]) %>%
  select(industry,year,imp_index_15) %>%
  pivot_wider(names_from="industry",values_from="imp_index_15") %>%
  write.csv("Downloads/world_trade.csv")

aec_6_data2<-aec_6_data %>%
  filter(year=="2023") %>%
  left_join(sectors,by=c("product_hs92_code"="code_6"))%>%
  distinct(country_iso3_code,industry,product_hs92_code,export_value,import_value,global_market_share) %>%
  mutate(industry=str_replace_all(industry," NA"," Downstream"),
         industry=str_replace_all(industry,"Battery","Batteries"))

aec_6_world<-aec_6_data2 %>%
  group_by(industry) %>%
  summarize(exports=sum(export_value,na.rm=T),
            imports=sum(import_value,na.rm=T)) %>%
  ungroup() %>%
  mutate(world_share=exports/sum(exports)*100)

aec_6_all <- aec_6_data2 %>%
  group_by(country_iso3_code,industry) %>%
  summarize(exports=sum(export_value,na.rm=T),
            imports=sum(import_value,na.rm=T)) %>%
  group_by(country_iso3_code) %>%
  mutate(country_share=exports/sum(exports)*100) %>%
  group_by(industry) %>%
  mutate(market_share=exports/sum(exports)*100) %>%
  left_join(aec_6_world %>%
              select(industry,world_share),by=c("industry")) %>%
  mutate(rca=country_share/world_share)

country_rca <-  aec_6_all %>%
  select(-country_share, -world_share) %>%
  rename(export_rca = rca)%>%
  # bind_rows handles name-mismatches for us
  bind_rows(
    aec_4_all %>%
      filter(!industry %in% aec_6_all$industry)  %>%
      # drop feasibility so it will be added back later
      select(-feasibility)
  ) %>%
  
  # finally add feasibility back (rename country_iso3_code???iso3c for the join)
  left_join(
    aec_4_all %>%
      select(country_iso3_code, industry, feasibility),
    by = c("country_iso3_code", "industry")
  ) %>%
  mutate(deficit=exports-imports) %>%
  filter(!is.na(industry)) %>%
  left_join(gdp_data %>%
              filter(year=="2024") %>%
              rename(gdp=NY.GDP.MKTP.CD) %>%
              select(iso3c,gdp),by=c("country_iso3_code"="iso3c")) %>%
  mutate(deficit_gdp=deficit/gdp) %>%
  group_by(industry) %>%
  mutate(market_share_index=median_scurve(market_share),
         #deficit_index=median_scurve(deficit_gdp),
         rca_index=median_scurve(export_rca)) %>%
  group_by(country_iso3_code) %>%
  mutate(export_size_index=median_scurve(exports),
         feas_index=median_scurve(feasibility)) %>%
  rowwise() %>%
  mutate(overall_trade_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  group_by(industry) %>%
  mutate(overall_trade_index = median_scurve(overall_trade_index))

trade_indices<-country_info %>%
  select(country,iso3c) %>%
  left_join(country_rca,by=c("iso3c"="country_iso3_code")) %>%
  arrange(desc(overall_trade_index))  %>%
  separate(
    col    = industry,
    into   = c("tech", "supply_chain"),
    sep    = " (?=Downstream|Midstream|Upstream$)"
  ) %>%
  distinct()

trade_indices_hhi <- trade_indices %>%
  group_by(tech, supply_chain) %>%
  mutate(
    # convert percent ??? fraction
    share_frac = market_share / 100,
    # HHI = sum of squared shares
    HHI = sum(share_frac^2, na.rm = TRUE)
  ) %>%
  select(-share_frac) %>%
  ungroup() %>%
  mutate(HHI_index=median_scurve(-HHI)) %>%
  distinct(tech,supply_chain,HHI,HHI_index)


# 1) start from your per-country trade_index table
#    (rename country???Country for consistency)
trade_tidy <- trade_indices %>%
  rename(Country = country) %>%
  mutate(tech=ifelse(tech=="Natural Gas","Gas",tech)) %>%
  
  # 2) bring in the HHI per tech / supply_chain
  left_join(
    trade_indices_hhi, 
    by = c("tech","supply_chain")
  ) %>%
  
  # 3) pivot the two metrics into long form
  
  pivot_longer(
    cols      = exports:HHI_index,      # everything from exports up through HHI
    names_to  = "variable",
    values_to = "value"
  ) %>%
  mutate(
    data_type = if_else(str_ends(variable, "_index"), "index", "raw"),
    variable    = str_remove(variable, "_index$")   # drop the suffix
  ) %>%
  rename(
    #Country      = country,        # if your column was still `country`
    tech         = tech,
    supply_chain = supply_chain
  ) %>%
  mutate(
    category    = "Trade",
    source      = "Harvard Atlas of Economic Complexity",
    variable=ifelse(variable=="overall_trade","Overall Trade Index",variable),
    explanation = case_when(
      data_type=="raw"   ~ str_glue("{variable}: raw value from Atlas"),
      data_type=="index" ~ str_glue("{variable}: percent-rank or HHI index")
    )
  ) %>%
  select(Country, tech, supply_chain, category,variable,
         data_type, value, source, explanation)



#UN Comtrade--------------
library(comtradr)
set_primary_comtrade_key('2940653b9bbe4671b3f7fde2846d14be')

crit_hs <- read.csv(paste0(raw_data,"Columbia University Critical Minerals Dashboard/unique_comtrade.csv"))

minerals_pattern <- paste0(paste0(mineral_demand$Mineral, collapse = "|"),"|","Graphite")

crit_hs_filtered <- crit_hs %>%
  filter(
    str_detect(comtradeDescription, regex(minerals_pattern, ignore_case = TRUE))
  )


critmin_import <- ct_get_data(
  reporter = country_info$iso3c,
  partner = "World",
  commodity_code = crit_hs_filtered$hscode,
  start_date = 2024,
  end_date = 2024,
  flow_direction = 'import'
)

critmin_export <- ct_get_data(
  reporter = country_info$iso3c,
  partner = "World",
  commodity_code = crit_hs_filtered$hscode,
  start_date = 2024,
  end_date = 2024,
  flow_direction = 'export'
)

total_import <- ct_get_data(
  reporter = country_info$iso3c,
  partner = "World",
  commodity_code = 'TOTAL',
  start_date = 2024,
  end_date = 2024,
  flow_direction = 'import'
)

total_export <- ct_get_data(
  reporter = country_info$iso3c,
  partner = "World",
  commodity_code = 'TOTAL',
  start_date = 2024,
  end_date = 2024,
  flow_direction = 'export'
)

critmin_trade <-critmin_import %>%
  select(reporter_iso,reporter_desc,cmd_code,cmd_desc,primary_value) %>%
  rename(imports=primary_value) %>%
  left_join(critmin_export %>%
              select(reporter_iso,reporter_desc,cmd_code,cmd_desc,primary_value) %>%
              rename(exports=primary_value),by=c("reporter_iso","reporter_desc","cmd_code","cmd_desc")) %>%
  left_join(country_info %>% select(iso3c,country),by=c("reporter_iso"="iso3c")) %>%
  select(-reporter_desc) %>%
  mutate(imports=ifelse(is.na(imports),0,imports),
         exports=ifelse(is.na(exports),0,exports),
         trade_balance=(exports-imports)/(exports+imports)) %>%
  mutate(
    mineral = str_to_sentence(str_extract(
      cmd_desc,
      regex(minerals_pattern, ignore_case = TRUE)
    )
    )) %>%
  mutate(supply_chain=ifelse(str_starts(cmd_code, "2"),"Upstream","Midstream"))


critmin_hhi <- critmin_trade %>%
  group_by(reporter_iso,country,mineral,supply_chain) %>%
  summarize(exports=sum(exports,na.rm=T)) %>%
  left_join(total_export %>%
              select(reporter_iso,
                     total_exports=primary_value),by=c("reporter_iso")) %>%
  mutate(exp_share=exports/total_exports) %>%
  group_by(mineral,supply_chain) %>%
  mutate(market_share=exports/sum(exports,na.rm=T)) %>%
  mutate(
    # convert percent ??? fraction
    share_frac_24 = exports/sum(exports,na.rm=T),
    # HHI = sum of squared shares
    HHI_24 = sum(share_frac_24^2, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(hhi_index=median_scurve(-HHI_24)
  )


critmin_trade_tech <- critmin_hhi %>%
  group_by(mineral,supply_chain) %>%
  mutate(criticalmineral_marketshare_index=median_scurve(market_share),
         criticalmineral_exportshare_index=median_scurve(exp_share),
         criticalmineral_hhi_index=hhi_index) %>%
  left_join(mineral_demand_clean %>%
              mutate(Mineral=ifelse(grepl("graphite",Mineral),"Graphite",Mineral)) %>%
              ungroup() %>%
              select(Mineral,tech,share_24),
            by=c("mineral"="Mineral")) %>%
  filter(!is.na(tech)) %>%
  
  group_by(country,tech,supply_chain) %>%
  mutate(share_24=share_24/sum(share_24)) %>% ungroup() %>%
  group_by(country,tech,supply_chain) %>%
  
  summarize(criticalmineral_marketshare=weighted.mean(market_share,
                                                            share_24,
                                                            na.rm=T),
            criticalmineral_marketshare_index=weighted.mean(criticalmineral_marketshare_index,
                                                            share_24,
                                                            na.rm=T),
            criticalmineral_exportshare_index=weighted.mean(criticalmineral_exportshare_index,
                                                            share_24,
                                                            na.rm=T),
            criticalmineral_hhi_index=weighted.mean(hhi_index,
                                                    share_24,
                                                    na.rm=T)) %>%
  rowwise() %>% 
  mutate(
    ## average the three index columns, ignoring any NAs
    critmin_trade_index = criticalmineral_marketshare_index
  )

critmin_trade_tidy <- critmin_trade_tech %>%
  select(country, tech,supply_chain,critmin_trade_index) %>%
  # 1. pivot all your metric-columns into a long format
  pivot_longer(
    cols      = -c(country, tech,supply_chain),
    names_to  = "variable",
    values_to = "value"
  ) %>%
  # 2. add the new constant columns
  mutate(
    category     = "Trade",
    # 3. classify raw vs index based on the _index suffix
    data_type = if_else(str_detect(variable, "_index$"),
                        "index", "raw"),
    # 4. strip off the "_index" so variable matches the pre-index name
    variable  = str_remove(variable, "_index$"),
    # 5. fill in source/explanation
    source      = "UN Comtrade",
    explanation = "UN Comtrade export data"
  ) %>%
  # 6. match the column names/order of trade_tidy
  rename(Country = country) %>%
  select(
    Country, tech, supply_chain, category,
    variable, data_type, value, source, explanation
  )

market_share_us<-trade_tidy %>%
  filter(Country=="United States",
         tech != "Green Hydrogen",
         !is.na(tech),!is.na(supply_chain),
         data_type=="raw",
         variable=="market_share") %>%
  left_join(critmin_trade_tech %>%
              filter(country=="United States",
                     supply_chain=="Upstream") %>%
              select(tech,supply_chain,criticalmineral_marketshare),by=c("tech","supply_chain")) %>%
  mutate(value=ifelse(is.na(criticalmineral_marketshare),value,criticalmineral_marketshare*100)) %>%
  select(tech,supply_chain,value) %>%
  pivot_wider(names_from="tech", values_from="value") %>%
write.csv("Downloads/market_share_us.csv")

deficit_us<-trade_tidy %>%
  filter(Country=="United States",
         tech != "Green Hydrogen",
         !is.na(tech),!is.na(supply_chain),
         data_type=="raw",
         variable %in% c("exports","deficit")) %>%
    select(tech,supply_chain,variable,value) %>%
  pivot_wider(names_from="variable", values_from="value") %>%
  mutate(imports=(exports-deficit),
         import_share=imports/(exports+imports)*100) %>%
  select(tech,supply_chain,import_share) %>%
  pivot_wider(names_from="tech",values_from="import_share") %>%
  write.csv("Downloads/import_share_us.csv")


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

res_list <- purrr::map(code_chunks, ~{
  Sys.sleep(0.4)  # adjust if you hit rate limits
  safe_ct(
    reporter       = country_info_iso$iso3c,
    partner        = "World",
    commodity_code = .x,
    start_date     = 2024,
    end_date       = 2024,
    flow_direction = "import"
  )
})

# 4) Bind all successful chunks
energy_import <- res_list %>%
  discard(is.null) %>%
  dplyr::bind_rows()


res_list <- purrr::map(code_chunks, ~{
  Sys.sleep(0.4)  # adjust if you hit rate limits
  safe_ct(
    reporter       = country_info_iso$iso3c,
    partner        = "World",
    commodity_code = .x,
    start_date     = 2024,
    end_date       = 2024,
    flow_direction = "export"
  )
})

# 4) Bind all successful chunks
energy_export <- res_list %>%
  discard(is.null) %>%
  dplyr::bind_rows()



#Energy Security index-------------------------------

all_security_data <- cleantech_clean %>%
  rbind(imports_clean) %>%
  rbind(reserves_clean) %>%
  rbind(energy_consumption_clean) %>%
  rbind(criticalmineral_supply %>%
          rename(Country=country)) %>%
  rbind(critical_min_prod) %>%
  rbind(trade_tidy %>%
          filter(!is.na(Country),
                 variable %in% c("market_share","HHI")))  %>%
  rbind(critmin_trade_tidy %>%
          filter(variable=="critmin_trade")) %>%
  rbind(ev_man) %>%
  rbind(neo_cap) %>%
  mutate(Country=recode(
    Country,
    "Vietnam" = "Viet Nam",
    "Iran, Islamic Rep." = "Iran",
    "Turkey"          = "Turkiye",
    "United kingdom"  = "United Kingdom",
    "Curacao"         = "Cura�ao",
    "Saudi arabia"         = "Saudi Arabia",
    "Russian Federation" = "Russia",
    "Czechia"="Czech Republic",
    "Yemen, Rep."="Yemen",
    "Venezuela, RB"="Venezuela"
  )) %>%
  filter(!grepl("Total|Other|Rest|Africa|USSR",Country),
         Country %in% all_countries)

security_weights <- c(
  "Foreign Dependency"   = 6,
  "Energy Imports"    = 3,
  "Reserves"        = 4,
  "Resource availability"=3,
  "Trade"    = 2,
  "critmin_trade" = 3,
  "Production" = 6,
  "Energy Access" = 3,
  "Consumption" = 5
)

energy_security_index<-all_security_data %>%
  #inner_join(variable_count %>%                filter(var_count>1) %>%                select(-var_count),by=c("Country","tech","supply_chain")) %>%
  filter(data_type=="index",
         !grepl("Copper|Graphite|Lithium|Rare",variable)) %>%
  group_by(Country,tech,supply_chain) %>%
  summarize(energy_security_index=
              weighted.mean(value,
                            w = ifelse(is.na(security_weights[category]),
                                       0, security_weights[category]),
                            na.rm=T)) %>%
  filter(tech %in% techs) %>%
  transmute(
    Country,
    tech,
    supply_chain,
    Pillar = "Energy Security",
    category = "Energy Security",
    variable = "Overall Energy Security Index",
    data_type = "index",                                       # raw / index
    value = energy_security_index,
    source      = "Author calculation",
    explanation = case_when(
      data_type == "index" ~ str_glue("Mean across energy security category indices")
    )
  ) %>%
  rbind(all_security_data %>%
          mutate(Pillar = "Energy Security"))


energy_security_subcategories <-all_security_data %>%
  inner_join(country_info %>%
               filter(region != "Aggregates") %>%
               select(country),by=c("Country"="country")) %>%
  filter(data_type=="index") %>%
  mutate(category=ifelse(category=="Energy Imports","Foreign Dependency",
                         ifelse(category=="Trade","Foreign Dependency",
                                ifelse(category=="Energy Access","Consumption",
                                       ifelse(category=="Reserves","Resource availability",category))))) %>%
  group_by(Country,category,supply_chain) %>%
  summarize(energy_security_index=mean(value,na.rm=T))


#-------------Economic Opportunity-------------------
#Energy Prices---------------------------------


#EI Data
gas_price_indexed <- read_excel(
  paste0(raw_data, "ei_stat_review_world_energy_wide.xlsx"),
  sheet = 40, skip = 3
) %>% 
  rename(Year = `...1`) %>% 
  
  ## ?????? 1. keep only real data rows ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
  filter(str_detect(Year, "^\\d{4}$")) %>%          # drops the extra header row
  
  ## ?????? 2. drop the benchmark columns you don't need ??????????????????????????????????????????????????????????????????
  select(-matches("US Gulf Coast6|Northwest Europe6|Middle East7|Far East Asia6")) %>% 
  
  ## ?????? 3. standardise missing + convert to numeric ?????????????????????????????????????????????????????????????????????
  mutate(across(-Year, ~ na_if(as.character(.), "-"))) %>%  # "-" ??? NA  (everything is character)
  mutate(across(-Year, parse_number)) %>%                   # "1.865." ??? 1.865 (numeric)
  
  ## ?????? 4. filter to the year you want and pivot long ???????????????????????????????????????????????????????????????
  filter(Year %in% c(2019:2024)) %>% 
  pivot_longer(
    cols = -Year,
    names_to  = "Country",
    values_to = "Value",
    values_drop_na = TRUE
  ) %>%
  ## ?????? 1. normalise Country names ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
  mutate(
    Country = Country %>% 
      str_remove_all("\\d+") %>%                 # drop trailing digits (Japan1 ??? Japan)
      str_remove_all("\\s*\\(Mainland\\)") %>%   # remove "(Mainland)"
      str_trim() %>%                             # tidy whitespace
      recode(                                    # recode a few special cases
        
        "UK" = "United Kingdom"
      )
  ) %>% 
  
  ## ?????? 2. add EU flag & join key ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
  mutate(
    EU        = if_else(Country == "Zeebrugge", 1L, 0L),
    country2  = if_else(EU == 1L, "EU", Country)  # same rule you use in `ei`
  ) %>%
  group_by(country2) %>%
  summarize(Gas = mean(Value,na.rm=T))


coal_price_indexed <- read_excel(
  paste0(raw_data, "ei_stat_review_world_energy_wide.xlsx"),
  sheet = 50, skip = 3
) %>% 
  rename(Year = `...1`) %>% 
  
  ## ?????? 1. keep only real data rows ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
  filter(str_detect(Year, "^\\d{4}$")) %>%          # drops the extra header row
  
  ## ?????? 2. drop the benchmark columns you don't need ??????????????????????????????????????????????????????????????????
  select(-matches("Canada9")) %>% 
  
  ## ?????? 3. standardise missing + convert to numeric ?????????????????????????????????????????????????????????????????????
  mutate(across(-Year, ~ na_if(as.character(.), "-"))) %>%  # "-" ??? NA  (everything is character)
  mutate(across(-Year, parse_number)) %>%                   # "1.865." ??? 1.865 (numeric)
  
  ## ?????? 4. filter to the year you want and pivot long ???????????????????????????????????????????????????????????????
  filter(Year %in% c(2019:2024)) %>% 
  pivot_longer(
    cols = -Year,
    names_to  = "Country",
    values_to = "Value",
    values_drop_na = TRUE
  ) %>%
  ## ?????? 1. normalise Country names ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
  mutate(
    Country = Country %>% 
      str_remove_all("\\d+") %>%                 # drop trailing digits (Japan1 ??? Japan)
      str_remove_all("\\s*\\(Mainland\\)") %>%   # remove "(Mainland)"
      str_trim() %>%                             # tidy whitespace
      recode(                                    # recode a few special cases
        "South China" = "China",
        "UK" = "United Kingdom"
      )
  ) %>% 
  
  ## ?????? 2. add EU flag & join key ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
  mutate(
    EU        = if_else(Country == "Northwest Europe", 1L, 0L),
    country2  = if_else(EU == 1L, "EU", Country)  # same rule you use in `ei`
  ) %>%
  group_by(country2) %>%
  summarize(Coal = mean(Value,na.rm=T))


ei_joined <- ei %>% 
  mutate(country2 = if_else(EU == 1L, "EU", Country)) %>%  # assure matching key
  distinct(Country,country2) %>%
  left_join(
    gas_price_indexed %>% select(country2, Gas),
    by = "country2"
  )  %>%
  mutate(Country=ifelse(Country=="US","United States",Country),
         country2=ifelse(country2=="US","United States",country2))%>%
  left_join(
    coal_price_indexed %>% select(country2, Coal),
    by = "country2"
  ) %>%
  select(-country2) %>%
  rename(gas_price = Gas,
         coal_price = Coal) %>%
  filter(!is.na(gas_price),
         !is.na(coal_price)) %>%
  mutate(gas_price_index=median_scurve(gas_price),
         coal_price_index=median_scurve(coal_price)) %>%
  pivot_longer(cols=c(gas_price:coal_price_index),names_to="tech",values_to="variable") %>%
  mutate(supply_chain="Upstream")

eo_price_clean <- ei_joined %>%
  # 1. Create a new `data_type` based on whether `tech` ends in "_index"
  mutate(
    data_type = if_else(str_detect(tech, "_index$"), "index", "raw"),
    # 2. Strip the "_index" suffix (if present) and convert to Title Case
    tech = str_remove(tech, "_index$") %>%
      str_replace_all("_", " ") %>%
      str_to_sentence(),
    # 3. Assign constant columns
    supply_chain = "Downstream",
    category     = "Energy Prices",
    #variable     = "Fuel Prices",    # name of the "variable" column
    value        = variable,         # copy the old numeric column into `value`
    source       = "EI Statistical Review of World Energy (2024)",
    explanation  = case_when(
      data_type == "raw"   ~ "Price",
      data_type == "index" ~ "Percent-rank of prices among countries"
    )
  ) %>%
  # 4. Keep only the columns you actually want
  transmute(
    Country,
    tech,
    supply_chain,
    category,
    variable,
    data_type,
    value,
    source,
    explanation
  )

#BNEF LCOE Data-------------------
lcoe_bnef <-read.csv(paste0(raw_data,"2025-03-24 - 2025 LCOE Data Viewer Tool.csv"),skip=8) 

lcoe_bnef <- lcoe_bnef %>%
  mutate(
    Technology = recode(
      Technology,
      
      # Gas-fired turbines (gen-set categories) ??? "Gas"
      "CCGT"             = "Gas",
      "Coal"    = "Coal",
      "PV fixed-axis"           = "Solar",
      "PV fixed-axis + storage" = "Solar",
      "Wind onshore"        = "Wind",
      # Utility-scale batteries ??? "Batteries"
      "Utility-scale battery (1h)" = "Batteries",
      "Utility-scale battery (4h)" = "Batteries",
      
      # If you eventually have an explicit "Green Hydrogen" row, map it here:
      #"Green Hydrogen" = "Green Hydrogen",
      
      # Anything not listed above will become NA; you can change .default if you want a fallback
      .default = NA_character_
    )
  ) %>%
  filter(Scenario=="Mid",
         Metric=="LCOE",
         !is.na(Technology)) %>%
  group_by(Technology,Region) %>% 
  summarize(lcoe_24_raw=mean(as.numeric(X2024),na.rm=T),
            lcoe_50_raw=mean(as.numeric(X2050),na.rm=T)) %>%
  group_by(Technology) %>%
  mutate(lcoe_24_index=median_scurve(-lcoe_24_raw),
         lcoe_50_index=median_scurve(-lcoe_50_raw)) %>%
  select(Region,Technology,lcoe_24_raw,lcoe_50_raw,lcoe_24_index,lcoe_50_index) %>%
  pivot_longer(
    cols=c(lcoe_24_raw:lcoe_50_index),
    names_to  = c("variable","data_type"),
    names_pattern = "(.*)_(raw|index)",
    values_to = "value"
  ) %>%
  ungroup() %>%
  transmute(
    Country = Region,
    tech         = Technology,
    supply_chain = "Downstream",
    category = "Energy Prices",
    variable,
    data_type,                                       # raw / index
    value,
    source      = "BNEF LCOE Estimates (2025)",
    explanation = case_when(
      data_type == "raw"   ~ str_glue("Levelized cost of energy"),
      data_type == "index" ~ str_glue("Percent-rank of LCOE across countries")
    )
  )

#Energy Production----------------------------
production_growth<-ei%>%
  select(Country,Year,Var,Value) %>%
  filter(!grepl("Other|Total|OECD|OPEC|World", Country),
         Year %in% c("2014","2019","2024"),
         Var %in% c("oilprod_kbd",
                    "gasprod_ej",
                    "coalprod_ej",
                    "solar_twh",
                    "wind_twh",
                    "nuclear_twh",
                    "electbyfuel_coal",
                    "electbyfuel_gas",
                    "electbyfuel_oil")) %>%
  distinct() %>%
  mutate(
    Var = case_when(
      Var == "oilprod_kbd"             ~ "Oil Upstream",
      Var == "gasprod_ej"      ~ "Gas Upstream",
      Var == "coalprod_ej" ~ "Coal Upstream",
      Var == "solar_twh"         ~ "Solar Downstream",
      Var == "wind_twh"           ~ "Wind Downstream",
      Var == "nuclear_twh" ~ "Nuclear Downstream",
      Var == "electbyfuel_coal" ~ "Coal Downstream",
      Var == "electbyfuel_gas" ~ "Gas Downstream",
      Var == "electbyfuel_oil" ~ "Oil Downstream",
      TRUE                                        ~ Var
    )) %>%
  group_by(Country,Year,Var) %>%
  summarize_at(vars(Value),sum,na.rm=T) %>%
  ungroup() %>%
  complete(Country,
           Year,
           Var,
           fill = list(Value = 0)) %>%                      # <- missing Value ??? 0
  pivot_wider(names_from="Year",values_from="Value") %>%
  mutate(change_5yr=(`2024`/`2019`-1)*100,
         change_10yr=(`2024`/`2014`-1)*100,
         change_5yr_abs=`2024`-`2019`) %>%
  pivot_longer(
    cols = `2014`:change_5yr_abs,
    names_to = "year",
    values_to = "value"
  ) %>%
  pivot_wider(names_from=year,values_from=value) %>%
  separate(
    Var,
    into      = c("tech","supply_chain"),
    sep       = " (?=Upstream|Downstream)",
    remove    = T
  ) %>% 
  group_by(tech,supply_chain) %>%
  mutate(size_index=median_scurve(`2024`),
         growth_index=median_scurve(change_5yr),
         growth_abs_index=median_scurve(`change_5yr_abs`)
  ) %>%
  rowwise() %>%
  mutate(overall_production_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  group_by(tech,supply_chain) %>%
  mutate(overall_production_index = median_scurve(overall_production_index)) %>%
  mutate(Country=ifelse(Country=="US","United States",Country))

mineral_production<-mineral_supply %>%
  mutate(supply_growth=(X2035-X2024)/X2024) %>%
  select(mineral,country,tech,supply_chain,X2024,X2035,supply_growth,HHI_24) %>%
  group_by(tech,supply_chain) %>%
  mutate(size_index=median_scurve(X2024),
         growth_abs_index=median_scurve(supply_growth),
         HHI_index=median_scurve(-HHI_24)) %>%
  group_by(country,tech,supply_chain) %>%
  summarize_at(vars(X2024:HHI_index),mean,na.rm=T) %>%
  rowwise() %>%
  mutate(overall_production_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  group_by(tech,supply_chain) %>%
  #mutate(overall_production_index = median_scurve(overall_production_index)) %>%
  mutate(Country=ifelse(country=="US","United States",country)) %>%
  select(-country)

production_growth2 <- production_growth %>%
  transmute(
    Country,
    tech,
    supply_chain,
    X2024=`2024`,
    size_index,
    growth_abs_index,
    overall_production_index
  ) %>%
  rbind(mineral_production %>%
          transmute(
            Country,
            tech,
            supply_chain,
            X2024,
            size_index,
            growth_abs_index,
            overall_production_index
          ))


production_tidy <- production_growth2 %>%
  filter(supply_chain != "Midstream") %>%
  # 2) pivot ALL metrics (2014,2019,2024,change_5yr,change_10yr,change_5yr_abs,
  #    size_index,growth_abs_index,growth_5yr_index,growth_10yr_index,production_index)
  pivot_longer(
    cols      = c(X2024:overall_production_index),
    names_to  = "metric",
    values_to = "value"
  ) %>%
  
  # 3) classify raw vs index
  mutate(
    data_type   = if_else(str_ends(metric, "_index"), "index", "raw"),
    metric_clean= str_remove(metric, "_index$")
  ) %>%
  
  # 4) attach category, source, explanation
  mutate(
    category   = "Production",
    source     = "EI Statistical Review World Energy",
    explanation = case_when(
      metric_clean %in% c("X2024") ~ 
        glue("Raw production of {tech} ({supply_chain}) in {metric_clean}"),
      
      metric_clean == "size" ~ 
        glue("Min-max scaled production in 2024 for {tech}"),
      metric_clean == "growth_abs" ~ 
        glue("Min-max scaled absolute growth index for {tech}"),
      metric_clean == "overall_production" ~ 
        glue("Composite index: mean of size & growth indices, then min-max scaled for {tech}"),
      TRUE ~ metric_clean
    )
  ) %>%
  
  # 5) select & rename to your final schema
  transmute(
    Country,
    tech,
    supply_chain,
    category,
    variable=ifelse(metric_clean=="overall_production","Overall Production",metric_clean),
    data_type,
    value,
    source,
    explanation
  )

# Inspect
production_tidy %>% filter(variable=="Overall Production")


#Supply Chain Capacity-------------------
overcapacity_bnef <- read_excel(paste0(raw_data,"BNEF_Energy Transition Supply Chains 2025.xlsx"),sheet=3,skip=9) %>%
  fill(Sector) %>%
  mutate(Sector=ifelse(Sector=="H???","Green Hydrogen",
                       ifelse(Sector=="Battery","Batteries",Sector)),
         Sector=ifelse(Component %in% c("Separators",
                                        "Electrolytes",
                                        "Cathodes",
                                        "Anodes",
                                        "cells"),"Batteries",
                       ifelse(Component %in% c("Onshore nacelles"),"Wind",
                              ifelse(Component %in% c("Wafers",
                                                      "Cells",
                                                      "Modules"),"Solar",Sector)))) %>%
  group_by(Sector) %>%
  summarize(Overcapacity=mean(`Overcapacity Ratio`,na.rm=T))%>%
  transmute(tech=Sector,
            supply_chain="Midstream",
            Overcapacity) %>%
  mutate(`Overall Overcapacity_index` = 2^(-Overcapacity)) %>%
  pivot_longer(cols=c(Overcapacity,`Overall Overcapacity_index`),names_to="variable",values_to="value") %>%
  mutate(
    data_type   = if_else(str_ends(variable, "_index"), "index", "raw"),
    variable= str_remove(variable, "_index$")
  ) %>%
  mutate(
    category   = "Technology Demand",
    source     = "BNEF Energy Transition Supply Chains 2025",
    explanation = "Ratio of Midstream capacity to forecast"
  )

overcapacity_tidy <- trade_tidy %>%
  distinct(Country,tech,supply_chain) %>%
  inner_join(overcapacity_bnef,by=c("tech","supply_chain"))

#Investment-----------------------------
#BNEF
path <- paste0(raw_data, "2025-06-03 - Energy Transition Investment Trends 2025 Countries Annex.xlsx")

# 2) Get every sheet name in that workbook
all_sheets <- excel_sheets(path)

# 3) Define a named vector that maps each "Pg N" sheet to its country label.
#    **?????? Make sure the names here exactly match the sheet names in your file.**
country_map <- c(
  "Pg 7"  = "United States",
  "Pg 8"  = "Canada",
  "Pg 9"  = "Brazil",
  "Pg 10" = "Mexico",
  "Pg 11" = "Chile",
  "Pg 14" = "China",
  "Pg 15" = "Japan",
  "Pg 16" = "India",
  "Pg 17" = "Australia",
  "Pg 18" = "South Korea",
  "Pg 19" = "Indonesia",
  "Pg 21" = "Vietnam",
  "Pg 24" = "United Kingdom",
  "Pg 25" = "Germany",
  "Pg 26" = "France",
  "Pg 27" = "Spain",
  "Pg 28" = "Italy",
  "Pg 29" = "Poland",
  "Pg 30" = "South Africa"
)

# 4) We only want to loop over the sheets that actually exist AND appear in country_map.
#    Using `intersect()` ensures we don't accidentally try to read a sheet that doesn't exist.
target_sheets <- intersect(all_sheets, names(country_map))

#  Optional check: Warn if any key in country_map is missing from the file
missing_sheets <- setdiff(names(country_map), all_sheets)
if (length(missing_sheets) > 0) {
  warning("The following sheet names were in country_map but not found in the workbook:\n",
          paste(missing_sheets, collapse = ", "))
}

# 5) Read each sheet, skip the first 9 rows, and add Country = country_map[<sheet_name>]
list_of_dfs <- lapply(target_sheets, function(sheet_nm) {
  # Read the sheet (skip = 9 as you requested)
  df <- read_excel(path = path, sheet = sheet_nm, skip = 9)
  
  # Look up the country label
  country_label <- country_map[[sheet_nm]]
  
  # Append a Country column
  df %>%
    mutate(Country = country_label)
})

# 6) Bind them all into one data frame
combined_by_country <- bind_rows(list_of_dfs)

bnef_inv <- combined_by_country %>%
  select(-`Grand Total`) %>%
  rename(Year = 1) %>%
  pivot_longer(cols=c(`Renewable energy`:`Power grids`),
               names_to="tech",
               values_to="value") %>%
  pivot_wider(names_from= Year,values_from=value) %>%
  mutate(growth_2422=`2024`/`2022`-1) %>%
  mutate(tech = tech %>%
           recode())


# Clean investment Monitor Data - Check it's the latest quarter available
investment_data_path <- 'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q4_2024/quarterly_actual_investment.csv'

# Read Data
investment <- read.csv(investment_data_path, skip=5)

cim_sectors<-investment %>% distinct(Technology,Segment) %>%
  mutate(
    supply_chain = case_when(
      Segment == "Energy and Industry"             ~ "Downstream",
      Segment == "Midstream"      ~ "Midstream",
      #Segment == "Retail" ~ "Downstream",
      TRUE                                        ~ Segment
    ),
    tech = case_when(
      Technology== "Storage" ~ "Batteries",
      Technology == "Critical Minerals" ~ "Batteries",
      Technology == "Hydrogen" ~ "Green Hydrogen",
      Technology == "Zero Emission Vehicles" ~ "Electric Vehicles",
      TRUE ~Technology
    )) %>%
  filter(tech %in% allies_rca$sector)

investment_growth <- investment %>%
  inner_join(cim_sectors,by=c("Technology","Segment")) %>%
  mutate(year=substr(quarter,1,4)) %>%
  group_by(year,tech,supply_chain) %>%
  summarize(investment=sum(Estimated_Actual_Quarterly_Expenditure,na.rm=T)) %>%
  mutate(investment=as.numeric(investment)) %>%
  pivot_wider(names_from="year",values_from="investment") %>%
  mutate(change_5yr=round((`2024`-`2020`)/`2020`*100),1,
         change_5yr_abs=(`2024`-`2020`)) %>%
  ungroup() %>%
  mutate(size_index=median_scurve(`2024`),
         growth_5yr_index=median_scurve(change_5yr),
         growth_5yr_abs_index=median_scurve(change_5yr_abs)) %>%
  rowwise() %>%
  mutate(investment_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(investment_index = median_scurve(investment_index))

investment_clean <- investment_growth %>% 
  #rename(tech = tech) %>% 
  transmute(country       = "United States",
            tech,
            supply_chain,
            `2024`,`change_5yr`,size_index,growth_5yr_index,investment_index) %>%
  pivot_longer(
    cols      = c(`2024`:investment_index),
    names_to  = "variable",
    values_to = "value"
  ) %>%
  
  # 3) classify raw vs index
  mutate(
    data_type   = if_else(str_ends(variable, "_index"), "index", "raw"),
    variable= str_remove(variable, "_index$")
  ) %>%
  
  # 4) attach category, source, explanation
  mutate(
    category   = "Investment",
    source     = "Clean investment Monitor",
    explanation = case_when(
      variable %in% c("2024") ~ 
        glue("2024 investment of {tech} ({supply_chain})"),
      
      variable == "change_5yr" ~ 
        glue("5-year % change: (2024 / 2019 ??? 1) � 100 for {tech} investment"),
      variable == "size" ~ 
        glue("Min-max scaled investment in 2043 for {tech}"),
      variable == "investment" ~
        glue("Mean of investment-related indices"),
      TRUE ~ variable
    )
  ) %>%
  mutate(variable=ifelse(variable=="investment","Overall Investment",variable))


#Cost Competitiveness-----------
iea_cost<-read.csv(paste0(raw_data,"Relative_Costs_IEA.csv")) %>%
  mutate( Product = Product %>%
            recode(                                    
              "Solar PV"="Solar",
              "Wind turbines"="Wind" ,
              "Electrolysers"="Green Hydrogen"
            ),
          Region=ifelse(Region=="Korea","South Korea",Region)) %>%
  group_by(Product) %>%
  mutate(supply_chain="Midstream",
         country2=ifelse(Region=="European Union","EU",
                         ifelse(Region=="Other Southeast Asia","Asia Pacific",Region)),
         cost_index=1-median_scurve(Value)) %>%
  rename(tech=Product) %>%
  select(country2,tech,Region,supply_chain,cost_index) %>%
  left_join( ei %>% 
               mutate(Country=ifelse(Country=="US","United States",Country)) %>%
               mutate(country2 = if_else(EU == 1L, "EU", Country)) %>%  # assure matching key
               distinct(Country,country2),
             by = "country2"
  ) %>%
  left_join(ei %>% 
              filter(Country != "South Korea") %>%
              distinct(Country,SubRegion),by=c("country2"="SubRegion")) %>%
  mutate(Country=ifelse(is.na(Country.x),Country.y,Country.x),
         supply_chain=ifelse(tech=="Ammonia","Downstream",supply_chain),
         tech=ifelse(tech=="Ammonia","Green Hydrogen",tech)) %>%
  select(Country,tech,supply_chain,cost_index)


iea_cost_clean <- iea_cost %>% 
  pivot_longer(
    cols      = c(cost_index),
    names_to  = "variable",
    values_to = "value"
  ) %>%
  
  # 3) classify raw vs index
  mutate(
    data_type   = if_else(str_ends(variable, "_index"), "index", "raw"),
    variable= str_remove(variable, "_index$")
  ) %>%
  
  # 4) attach category, source, explanation
  mutate(
    category   = "Cost Competitiveness",
    source     = "IEA Energy Technology Perspectives 2024",
    variable = "Overall input cost index",
    explanation = case_when(
      variable %in% c("cost") ~ 
        glue("Cost competitiveness relative to China"),
      TRUE ~ variable
    )
  )



#Future Demand---------------
iea_weo<-read.csv(paste0(raw_data,"WEO2024_AnnexA_Free_Dataset_World.csv")) %>%
  filter(SCENARIO=="Stated Policies Scenario",
         CATEGORY=="Energy",
         PRODUCT %in% c("Solar",
                        "Wind",
                        "Nuclear",
                        "Natural gas",
                        "Oil",
                        "Coal",
                        "Solar PV",
                        "Hydrogen: low-emissions",
                        "Geothermal"),
         FLOW=="Total energy supply"|grepl("Hydrogen",FLOW)) %>%
  group_by(YEAR,PRODUCT) %>%
  summarize(VALUE=sum(VALUE,na.rm=T)) %>%
  ungroup() %>%
  pivot_wider(names_from="YEAR",values_from="VALUE") %>%
  mutate(growth=`2035`-`2022`) %>%
  ungroup() %>%
  mutate(PRODUCT=ifelse(PRODUCT=="Natural gas","Gas",PRODUCT),
         PRODUCT=ifelse(grepl("Hydrogen",PRODUCT),"Green Hydrogen",PRODUCT),
         demand_size_index=median_scurve(`2035`),
         demand_growth_index=median_scurve(growth)) %>%
  rename("tech"="PRODUCT") %>%
  rowwise() %>%
  mutate(
    demand_index = (demand_size_index + 2 * demand_growth_index) / 3
  ) %>%
  ungroup() %>%
  mutate(demand_index=median_scurve(demand_index)) %>%
  mutate(Upstream="Upstream",
         Midstream="Midstream",
         Downstream="Downstream") %>%
  pivot_longer(cols=c(Upstream:Downstream),names_to="supply_chain",values_to="supply_chain2") %>%
  select(tech,supply_chain,`2035`,`growth`,demand_size_index,demand_growth_index,demand_index) %>% 
  pivot_longer(
    cols      = c(`2035`:demand_index),
    names_to  = "variable",
    values_to = "value"
  ) %>%
  
  # 3) classify raw vs index
  mutate(
    data_type   = if_else(str_ends(variable, "_index"), "index", "raw"),
    variable= str_remove(variable, "_index$")
  ) %>%
  
  # 4) attach category, source, explanation
  mutate(
    category   = "Technology Demand",
    source     = "IEA World Energy Outlook",
    explanation = case_when(
      variable =="2035" ~ 
        glue("Global energy demand in 2035"),
      variable =="growth" ~ 
        glue("Modelled demand growth, 2022-2035, IEA States Policies Scenario"),
      variable =="demand_size" ~ 
        glue("Index of global energy demand in 2035"),
      variable =="demand_growth_index" ~ 
        glue("Index of modelled demand growth, 2022-2035, IEA States Policies Scenario"),
      variable =="demand_index" ~ 
        glue("Mean of growth and size indices"),
      TRUE ~ variable
    )
  ) %>%
  mutate(variable=ifelse(variable=="demand","Overall Global Demand",variable))


neo<-bnef_neo %>%
  filter(Scenario=="ETS",
         Indicator=="Primary energy consumption",
         !Region %in% c("Global",
                        "MENAT"),
         !grepl("Other|Rest|Africa",Region),
         Sector=="All sectors",
         Fuel.type %in% techs) %>%
  mutate(Region=ifelse(Region=="US","United States",
                       ifelse(Region=="UK","United Kingdom",Region))) %>%
  mutate_at(vars(X2000:X2050),as.numeric) %>%
  mutate(growth=(`X2035`-`X2024`)/X2024) %>%
  group_by(Fuel.type) %>%
  mutate(demand_size_index=median_scurve(`X2035`),
         demand_growth_index=median_scurve(growth)) %>%
  rename("tech"="Fuel.type") %>%
  rowwise() %>%
  mutate(
    demand_index = (demand_size_index + 2 * demand_growth_index) / 3
  ) %>%
  ungroup() %>%
  mutate(demand_index=median_scurve(demand_index)) %>%
  mutate(Upstream="Upstream",
         Midstream="Midstream",
         Downstream="Downstream") %>%
  pivot_longer(cols=c(Upstream:Downstream),names_to="supply_chain",values_to="supply_chain2") %>%
  select(Region,tech,supply_chain,`X2024`,`X2035`,`growth`,demand_size_index,demand_growth_index,demand_index) %>% 
  pivot_longer(
    cols      = c(`X2024`:demand_index),
    names_to  = "variable",
    values_to = "value"
  ) %>%
  
  # 3) classify raw vs index
  mutate(
    data_type   = if_else(str_ends(variable, "_index"), "index", "raw"),
    variable= str_remove(variable, "_index$")
  ) %>%
  
  # 4) attach category, source, explanation
  transmute(
    Country = Region,
    tech,
    supply_chain,
    category   = "Technology Demand",
    variable,
    data_type,
    value,
    source     = "BNEF New Energy Outlook",
    explanation = case_when(
      variable =="X2035" ~ 
        glue("Energy demand (PJ) in 2035"),
      variable =="growth" ~ 
        glue("Modelled demand growth, 2024-2035, BNEF ETS Scenario"),
      variable =="demand_size" ~ 
        glue("Index of energy demand in 2035"),
      variable =="demand_growth_index" ~ 
        glue("Index of modelled demand growth, 2024-2035, BNEF ETS Scenario"),
      variable =="demand_index" ~ 
        glue("Mean of growth and size indices"),
      TRUE ~ variable
    )
  ) %>%
  mutate(variable=ifelse(variable=="demand","Overall Demand",variable))


#BCG US Serviceable Market---------------------
bcg<-read_excel(paste0(raw_data,"Market Size for Technology and Supply Chain.xlsx"),1) %>%
  rename(tech="Energy source") %>%
  pivot_longer(cols=c(Upstream,Midstream,Downstream),names_to="supply_chain",values_to="SAM") %>%
  mutate(sam_index=median_scurve(SAM)) %>%
  mutate(Country="United States") %>%
  pivot_longer(
    cols      = c(SAM,sam_index),
    names_to  = "variable",
    values_to = "value"
  ) %>%
  
  # 3) classify raw vs index
  mutate(
    data_type   = if_else(str_ends(variable, "_index"), "index", "raw"),
    variable= str_remove(variable, "_index$")
  ) %>%
  
  # 4) attach category, source, explanation
  mutate(
    category   = "Technology Demand",
    source     = "BCG",
    explanation = case_when(
      variable =="SAM" ~ 
        glue("Serviceable Addressable Market, 2020-2050"),
      variable =="sam" ~ 
        glue("Serviceable Addressable Market, 2020-2050 (index)"),
      TRUE ~ variable
    )
  ) %>%
  mutate(variable=ifelse(variable=="sam","Overall Addressable Market",variable))

#Economic Opportunity Index-----------------
econ_opp_indices<- rbind(trade_tidy,production_tidy %>%
                           mutate(Country=ifelse(Country=="US","United States",Country)))  %>%
  mutate(variable=ifelse(variable=="feas" & data_type=="index","Overall Feasibility",variable)) %>%
  rbind(neo) %>%
  rbind(lcoe_bnef) %>%
  rbind(investment_clean %>%
          rename(Country=country)) %>%
  rbind(trade_tidy %>%
          select(Country,tech,supply_chain) %>%
          left_join(iea_weo,by=c("tech","supply_chain"))) %>%
  rbind(overcapacity_tidy) %>%
  rbind(energy_consumption_growth) %>% mutate(variable=ifelse(variable=="Energy consumption per capita growth","Overall Energy consumption growth",variable)) %>%
  rbind(cleantech_clean %>%
          mutate(variable=ifelse(variable=="Market Share","Overall Market Share",variable))) %>%
  rbind(criticalmineral_supply %>%
          rename(Country=country) %>%
          mutate(variable = "Overall Mineral Supply")) %>%
  rbind(ev_man %>%
          mutate(variable=ifelse(variable=="ev_Midstream","Overall Production",variable))) %>%
  rbind(iea_cost_clean) %>%
  #rbind(bcg) %>%
  #mutate(variable=ifelse(variable=="Overall Addressable Market","Overall Demand",variable)) %>%
  rbind(neo_cap %>%
          ungroup() %>%
          mutate(variable=ifelse(variable=="elec_growth_index",
                                 "Overall Production",variable))) %>%
  mutate(Pillar="Economic Opportunity") %>%
  filter(Country != "Korea, Dem. People's Rep.") %>%
  mutate(Country=ifelse(grepl("Korea",Country),"South Korea",Country),
         tech=ifelse(tech=="Oil & Gas","Geothermal",tech)) %>%
  filter(supply_chain %in% c("Downstream",
                             "Midstream",
                             "Upstream")) 

opportunity_weights <- c(
  "Overall Trade Index"   = 3,
  "Overall Feasibility" = 4,
  "Overall Production"    = 4,
  "Overall Demand"        = 5,
  "Overall Overcapacity"  = 3,
  "Overall Market Share" = 3,
  "Overall Mineral Supply" = 3,
  "Overall input cost index"=3,
  "Overall Energy consumption growth" = 3,
  "Overall Investment"    = 3,
  "Overall Global Demand" = 3
)

# 2) compute weighted index
econ_opp_index <- econ_opp_indices %>%
  filter(
    data_type == "index",
    tech %in% techs,
    grepl("Overall",variable)    # same names as your weights
  )%>%
  distinct(Country,tech,supply_chain,Pillar,value,variable) %>%
  group_by(Country, tech, supply_chain, Pillar) %>%
  summarize(
    econ_opp_index = weighted.mean(
      x = value,
      w = opportunity_weights[variable],        # lookup weight by variable name
      na.rm = TRUE
    ),
    .groups = "drop"
  ) %>%
  transmute(
    Country,tech,
    supply_chain,
    Pillar,
    category="Energy Opportunity",
    variable="Overall Economic Opportunity Index",
    data_type="index",
    value=econ_opp_index,
    source="Author calculation",
    explanation="Mean across economic opportunity category indices"
  ) %>%
  rbind(econ_opp_indices) %>%
  distinct()

us_opportunity <- econ_opp_index %>%
  filter(Country=="United States",
         variable=="Overall Economic Opportunity Index",
         data_type=="index") %>%
  select(tech,supply_chain,value) %>%
  mutate(value=median_scurve(value)) %>%
  pivot_wider(names_from=supply_chain,values_from=value)

#Final File

security_opp_indices<-rbind(energy_security_index,econ_opp_index)
write.csv(security_opp_indices,"Downloads/Energy_Security_Econ_Opportunity_Indices.csv")

#Energy Strategy Scatterplot-----------------------
scatter_index<-rbind(energy_security_index %>%
                       filter(variable=="Overall Energy Security Index"),
                     econ_opp_index %>% 
                       filter(variable=="Overall Economic Opportunity Index")) %>%
  mutate(tech=ifelse(tech=="Green Hydrogen","Hydrogen",tech),industry=paste0(tech,": ",supply_chain)) %>%
  filter(industry != "Geothermal: Midstream")

us_scatter_index <- scatter_index %>%
  filter(Country=="South Korea")%>%
  select(industry,tech,supply_chain,variable,value) %>%
  #group_by(variable) %>%
  #mutate(value=median_scurve(value)) %>%
  pivot_wider(names_from=variable,values_from=value)

write.csv(us_scatter_index %>%
            mutate( `Overall Energy Security Index`=1- `Overall Energy Security Index`),"Downloads/us_scatter.csv")
View(us_scatter_index %>% ungroup() %>% summarize(security=mean((1- `Overall Energy Security Index`),na.rm=T),
                                                  opportunity=mean(`Overall Economic Opportunity Index`,na.rm=T)))


#------------------Bilateral Relationships-----------------
#US export Size & Growth------------------------------
country_codes <- read_delim(
  "https://www.census.gov/foreign-trade/schedules/c/country.txt",
  delim      = "|",
  skip       = 4,                            # drop the title & separator lines
  col_names  = c("Code","Name","ISO"),       # assign sensible names
  trim_ws    = TRUE,                         # strip leading/trailing spaces
  comment    = "-"                           # ignore the dashed-line row
) %>%
  # coerce types
  mutate(
    Code = as.integer(Code),
    ISO  = as.character(ISO),
    Name = str_squish(Name)
  )


energy_exports_hs<-unique(energy_codes$code)
energy_exports_hs_10<-unique(sectors$code_10)

export_data_all <- data.frame()

for (code in energy_exports_hs) {
  result <- tryCatch({
    getCensus(
      name = "timeseries/intltrade/exports/hs",
      vintage = NULL, 
      vars = c("E_COMMODITY", "CTY_CODE", "CTY_NAME", "ALL_VAL_MO"), 
      time = "from 2015 to 2025",
      E_COMMODITY = code  # Single HS code
    )
  }, error = function(e) {
    message("Error fetching data for code: ", code)
    message("Error: ", e)
    return(NULL)
  })
  
  if (!is.null(result)) {
    export_data_all <- bind_rows(export_data_all, result)  # Append results
  }
}


export_data_all_10 <- data.frame()

for (code in energy_exports_hs_10) {
  result <- tryCatch({
    getCensus(
      name = "timeseries/intltrade/exports/hs",
      vintage = NULL, 
      vars = c("E_COMMODITY", "CTY_CODE", "CTY_NAME", "ALL_VAL_MO"), 
      time = "from 2020 to 2025",
      E_COMMODITY = code  # Single HS code
    )
  }, error = function(e) {
    message("Error fetching data for code: ", code)
    message("Error: ", e)
    return(NULL)
  })
  
  if (!is.null(result)) {
    export_data_all <- bind_rows(export_data_all, result)  # Append results
  }
}


export_growth <- export_data_all %>%
  mutate(value=as.numeric(ALL_VAL_MO)) %>%
  left_join(energy_codes,by=c("E_COMMODITY"="code")) %>%
  left_join(country_codes %>%
              mutate(Code=as.character(Code)) %>%
              select(-Name),by=c("CTY_CODE"="Code")) %>%
  mutate(country = str_to_sentence(CTY_NAME)) %>%
  distinct(ISO,country,industry,E_COMMODITY,product,time,value) 

export_growth2 <- export_growth %>%
  group_by(ISO,country,industry,time) %>%
  summarize(across(c(value),sum,na.rm=T))

export_oil_gas_coal <- export_data_all %>%
  mutate(value=as.numeric(ALL_VAL_MO)) %>%
  left_join(energy_codes,by=c("E_COMMODITY"="code")) %>%
  left_join(country_codes %>%
              mutate(Code=as.character(Code)) %>%
              select(-Name),by=c("CTY_CODE"="Code")) %>%
  mutate(country = str_to_sentence(CTY_NAME)) %>%
  distinct(ISO,country,industry,E_COMMODITY,time,value) %>%
  mutate(year=substr(time,1,4)) %>%
  filter(country=="Total for all countries") %>%
  group_by(ISO,country,industry,time) %>%
  summarize(across(c(value),sum,na.rm=T)) %>%
  mutate(value=value/1000000000)

export_sector_wide <- export_oil_gas_coal %>%
  group_by(industry) %>%
  mutate(export_ma = rollmean(value, k = 12, align = "right", fill = NA)) %>%
  mutate(exp_index_15 = 100*export_ma/export_ma[time=="2016-01"]) %>%
  select(industry,time,exp_index_15) %>%
  pivot_wider(names_from="industry",values_from="exp_index_15") %>%
  write.csv("Downloads/export_sector.csv")

export_growth3 <- export_growth2 %>%
  arrange(time) %>%
  group_by(country,industry) %>%
  mutate(
    export_12mma = rollmean(value, k = 12, align = "right", fill = NA),
    base_2020 = ifelse(any(time == "2020-01"), 
                       export_12mma[time == "2021-01"], 
                       NA),
    export_index_20 = ifelse(!is.na(base_2020) & base_2020 > 0, 
                             100 * export_12mma / base_2020, 
                             NA)
  ) %>%
  ungroup() 

export_growth_allies <- export_growth3 %>%
  filter(time=="2025-05") %>%
  select(-base_2020)%>%
  #group_by(industry) %>%
  mutate(us_export_index=median_scurve(export_12mma),
         us_export_growth_index = median_scurve(export_index_20)) %>%
  rowwise() %>%
  mutate(us_trade_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  group_by() %>%
  mutate(us_trade_index = median_scurve(us_trade_index)) %>%
  filter(!is.na(ISO),
         !is.na(industry)) %>%
  mutate(industry=str_replace_all(industry,"Natural Gas","Gas")) %>%
  arrange(desc(us_trade_index))


export_growth_allies2 <- export_growth_allies %>%
  left_join(econ_opp_index %>%
              ungroup() %>%
              filter(Country=="United States",
                     variable=="Overall Economic Opportunity Index") %>%
              mutate(industry=paste(tech,supply_chain),
                     econ_opp_index=value) %>%
              select(industry,econ_opp_index),by=c("industry")) %>%
  mutate(us_opp_index=(us_trade_index+econ_opp_index)/2) 

#filter(ISO %in% allies$iso2c)

#US Imports---------------------

energy_imports_hs<-unique(energy_codes$code)

import_data_all <- data.frame()

for (code in energy_exports_hs) {
  result <- tryCatch({
    getCensus(
      name = "timeseries/intltrade/imports/hs",
      vintage = NULL, 
      vars = c("I_COMMODITY", "CTY_CODE", "CTY_NAME", "GEN_VAL_MO"), 
      time = "from 2020 to 2025",
      I_COMMODITY = code  # Single HS code
    )
  }, error = function(e) {
    message("Error fetching data for code: ", code)
    message("Error: ", e)
    return(NULL)
  })
  
  if (!is.null(result)) {
    import_data_all <- bind_rows(import_data_all, result)  # Append results
  }
}


import_growth <- import_data_all %>%
  mutate(value=as.numeric(GEN_VAL_MO)) %>%
  left_join(energy_codes,by=c("I_COMMODITY"="code")) %>%
  left_join(country_codes %>%
              mutate(Code=as.character(Code)) %>%
              select(-Name),by=c("CTY_CODE"="Code")) %>%
  mutate(country = str_to_sentence(CTY_NAME)) %>%
  distinct(ISO,country,industry,I_COMMODITY,product,time,value) 

importgrowth<-import_growth %>%
  group_by(ISO,country,industry,time) %>%
  summarize(across(c(value),sum,na.rm=T))


import_growth2 <- importgrowth %>%
  arrange(time) %>%
  group_by(country,industry) %>%
  mutate(
    import_12mma = rollmean(value, k = 12, align = "right", fill = NA),
    base_2020 = ifelse(any(time == "2020-01"), 
                       import_12mma[time == "2021-01"], 
                       NA),
    import_index_20 = ifelse(!is.na(base_2020) & base_2020 > 0, 
                             100 * import_12mma / base_2020, 
                             NA)
  ) %>%
  ungroup() 

import_growth_allies <- import_growth2 %>%
  filter(time=="2025-02") %>%
  select(-base_2020)%>%
  #group_by(industry) %>%
  mutate(us_import_index=median_scurve(import_12mma),
         us_import_growth_index = median_scurve(import_index_20)) %>%
  rowwise() %>%
  mutate(us_trade_index = mean(c_across(ends_with("_index")), na.rm = TRUE)) %>%
  group_by() %>%
  mutate(us_trade_index = median_scurve(us_trade_index)) %>%
  #filter(ISO %in% allies$iso2c) %>%
  mutate(industry=str_replace_all(industry,"Natural Gas","Gas"))

#US Outbound Investment-----------------------
library(dplyr)
library(tidyr)
library(stringr)
library(readr)
library(janitor)

outbound <- read.csv(
  file.path(raw_data, "us_outbound_15_24.csv"),
  skip = 4,
  na.strings = c("n.s.", "(D)", "(*)")   # add "(*)"
) %>%
  slice(-1) %>%                       # drop the '2015' header row
  rename(country = X) %>%
  clean_names() %>%                   # All.Industries.Total.1 -> all_industries_total_1
  mutate(across(
    -country,
    ~ readr::parse_number(as.character(.x),
                          na = c("n.s.", "(D)", "(*)"))   # add "(*)"
  )) %>%
  pivot_longer(
    -country,
    names_to = "industry",
    values_to = "value",
    # strip trailing _1, _2, ... so all years collapse under one industry
    names_transform = list(industry = ~ str_remove(.x, "_\\d+$"))
  ) %>%
  group_by(country, industry) %>%
  summarise(total = sum(value, na.rm = TRUE), .groups = "drop") %>%
  pivot_wider(names_from = industry, values_from = total) %>%
  mutate(
    downstream_size = rowSums(across(c(
      utilities,
      information,
      wholesale_trade,
      professional_scientific_and_technical_services
    )), na.rm = TRUE),
    midstream_size  = coalesce(total_manufacturing, 0),
    upstream_size   = coalesce(mining, 0)
  ) %>%
  mutate(country = str_trim(country)) %>%
  select(country, downstream_size, midstream_size, upstream_size) %>%
  filter(!str_detect(country, "(?i)All|Other|Asia|Middle|EU|nonzero|Caribbean")) %>%
  left_join(gdp, by = "country") %>%
  mutate(
    GDP = GDP / 1e6,
    downstream_share = downstream_size / GDP,
    midstream_share  = midstream_size  / GDP,
    upstream_share   = upstream_size   / GDP,
    
    downstream_size_index = median_scurve(downstream_size),
    midstream_size_index  = median_scurve(midstream_size),
    upstream_size_index   = median_scurve(upstream_size),
    
    downstream_share_index = median_scurve(downstream_share),
    midstream_share_index  = median_scurve(midstream_share),
    upstream_share_index   = median_scurve(upstream_share)
  ) %>%
  pivot_longer(
    cols = matches("(?i)^(downstream|midstream|upstream)_(size|share)(?:_(index))?$"),
    names_to      = c("supply_chain", "metric", "type"),
    names_pattern = "(?i)^(downstream|midstream|upstream)_(size|share)(?:_(index))?$",
    values_to     = "value"
  ) %>%
  mutate(
    supply_chain = stringr::str_to_sentence(tolower(supply_chain)),  # Downstream/Midstream/Upstream
    type         = dplyr::coalesce(type, "raw")                      # "index" or "raw"
  )


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


outbound_chart <- outbound %>%
  rename(Country=country) %>%
  filter(!grepl("Europ",Country),
         !grepl("America",Country),
         !grepl("Africa",Country),
         metric=="size",
         type=="index",
         year=="2023") %>%
  select(Country,supply_chain,value) %>%
  pivot_wider(names_from="supply_chain",values_from="value") %>%
  rowwise() %>%
  mutate(total=sum(c_across(c(Upstream,Midstream,Downstream)))) %>%
  ungroup() %>%
  arrange(desc(total)) %>%
  slice_max(order_by=total,n=15) %>%
  select(Country:Upstream) %>%
  pivot_longer(cols=c(2:4),names_to="supply_chain",values_to="value") %>%
  mutate(value=value*1000000) %>%
  pivot_wider(names_from="Country",values_from="value") %>%
  write.csv("Downloads/invest.csv")


#Energy Agreements-----------------
bilat_energy<-read_excel(paste0(raw_data,"clean_energy_foreign_policy_heatmap.xlsx")) %>%
  pivot_longer(cols=c(2:13),names_to="tech",values_to="value") %>%
  mutate(value=replace_na(value, 0)) %>%
  group_by(tech) %>%
  mutate(bilat_energy_agreement=ifelse(value==1,0.8,
                                       ifelse(value==2,1,0))) 


#Climate Prioritization----------------------------
#IPCC Median GHG Intensity estimates

min_max_index <- function(x) {
  # 1) get only the finite values
  finite_x <- x[is.finite(x)]
  min_x    <- min(finite_x, na.rm = TRUE)
  max_x    <- max(finite_x, na.rm = TRUE)
  
  # 2) clamp Inf to the finite extremes
  x2 <- x
  x2[x ==  Inf] <- max_x
  x2[x == -Inf] <- min_x
  
  # 3) standard min-max scale
  (x2 - min_x) / (max_x - min_x)
}

tech_ghg<-read.csv(paste0(raw_data,"ipcc_ghg_intensity.csv")) %>%
  mutate(Tech=recode(
    Tech,
    "Coal (pulverised)" ="Coal" ,
    "Natural-gas combined cycle" = "Gas" ,
    "Oil-fired steam" = "Oil",
    "Solar PV - utility scale" = "Solar",
    "Nuclear (light-water)" = "Nuclear",
    "Wind - onshore" = "Wind"
  )) %>%
  mutate(ghg_index=min_max_index(-ghg_intensity))


#Climate Action Tracker Score
cat<-read.csv(paste0(raw_data,"CAT_country ratings data.csv")) %>%
  mutate(climate_policy_index = case_when(
    Overall.rating == "Critically insufficient"   ~ 0.25,
    Overall.rating == "Highly insufficient"   ~ 0.5,
    Overall.rating == "Insufficient"   ~ 0.75,
    Overall.rating == "Almost Sufficient"   ~ 1
  )) 

#Total US Friendshore Index------------------------

us_friendshore_index<- import_growth_allies %>%
  mutate(country=str_to_title(country)) %>%
  mutate(country=recode(
    country,
    "Korea, south" = "South Korea",
    "Vietnam" = "Viet Nam",
    "Turkey"          = "Turkiye",
    "United kingdom"  = "United Kingdom",
    "Curacao"         = "Cura�ao",
    "Saudi arabia"         = "Saudi Arabia"
  )) %>%
  left_join(energy_security_index %>%
              ungroup() %>%
              filter(Country=="United States",
                     variable=="Overall Energy Security Index") %>%
              mutate(industry=paste(tech,supply_chain),
                     energy_security_index=1-value) %>%
              select(tech,supply_chain,industry,energy_security_index),by=c("industry")) %>%
  left_join(econ_opp_index %>%
              ungroup() %>%
              filter(Country=="United States",
                     variable=="Overall Economic Opportunity Index") %>%
              mutate(industry=paste(tech,supply_chain),
                     econ_opp_index=1-value) %>%
              select(industry,econ_opp_index),by=c("industry")) %>%
  inner_join(econ_opp_index %>%
               mutate(Country=str_to_title(Country)) %>%
               ungroup() %>%
               filter(variable=="Overall Economic Opportunity Index") %>%
               mutate(industry=paste(tech,supply_chain),
                      econ_opp_index2=value) %>%
               select(Country,industry,econ_opp_index2),by=c("country"="Country","industry")) %>%
  left_join(outbound_indexed %>%
              mutate(Country=str_to_title(Country)) %>%
              mutate(Country=recode(
                Country,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Cura�ao",
                "Saudi arabia"         = "Saudi Arabia"
              )),
            by=c("country"="Country","supply_chain")) %>%
  left_join(bilat_energy %>%
              mutate(Country=str_to_title(Country)) %>%
              mutate(Country=recode(
                Country,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Cura�ao",
                "Saudi arabia"         = "Saudi Arabia"
              )),
            by=c("country"="Country","tech")) %>%
  filter(ISO %in% country_info$iso2c,
         !country %in% c("China","Russia"),
         tech %in% techs) %>%
  #climate adjustment
  left_join(tech_ghg,by=c("tech"="Tech")) %>%
  left_join(cat %>%
              mutate(Country=recode(
                Country,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Cura�ao",
                "Saudi arabia"         = "Saudi Arabia"
              )),
            by=c("country"="Country")) %>%
  mutate(
    ghg_index            = replace_na(ghg_index,
                                      median(tech_ghg$ghg_index, na.rm = TRUE)),
    climate_policy_index = replace_na(climate_policy_index,
                                      median(cat$climate_policy_index, na.rm = TRUE))
  ) %>%
  rowwise() %>% 
  mutate(
    us_friendshore_index_unadj = {                 # braces let you run many lines
      vals <- c(us_trade_index,
                energy_security_index,
                econ_opp_index,
                econ_opp_index2,
                bilat_energy_agreement,
                outbound_inv_index)
      wts  <- c(1, 1,1, 1,1, 1)
      keep <- !is.na(vals) & !is.na(wts)
      weighted.mean(vals[keep], wts[keep])   # returns a single number
    }
  ) %>% 
  mutate(penalty    = (1 - ghg_index) * climate_policy_index,
         us_friendshore_index=us_friendshore_index_unadj-0.2*penalty) %>%
  ungroup() %>% 
  mutate(us_friendshore_index=median_scurve(us_friendshore_index)) %>%
  #filter(!grepl("Geothermal", tech)) %>% 
  arrange(desc(us_friendshore_index))

us_friendshore_1<-us_friendshore_index %>%
  group_by(country) %>%
  slice_max(order_by=us_friendshore_index,n=1)

us_friendshore_index_total<-us_friendshore_index %>%
  group_by(country) %>%
  summarize(total_us_friendshore_index=mean(us_friendshore_index,na.rm=T)) %>%
  mutate(total_us_friendshore_index=median_scurve(total_us_friendshore_index)) %>%
  arrange(desc(total_us_friendshore_index))


friendshore_scatter<-us_friendshore_index %>%
  ungroup() %>%
  mutate(country_industry=paste0(country,": ",industry))%>%
  select(country_industry,energy_security_index,econ_opp_index2) %>%
  mutate(energy_security_index=median_scurve(energy_security_index))

#Total US Export Opportunity index---------------------
us_opportunity_index <- export_growth_allies %>%
  separate(
    col    = industry,
    into   = c("tech", "supply_chain"),
    sep    = " (?=Downstream|Midstream|Upstream$)"
  ) %>%
  mutate(country=str_to_title(country)) %>%
  mutate(country=recode(
    country,
    "Korea, south" = "South Korea",
    "Vietnam" = "Viet Nam",
    "Turkey"          = "Turkiye",
    "United kingdom"  = "United Kingdom",
    "Curacao"         = "Cura�ao",
    "Saudi arabia"         = "Saudi Arabia",
    "Vietnam" = "Viet Name",
    "United arab emirates" = "United Arab Emirates"
  )) %>%
  left_join(econ_opp_index %>%
              ungroup() %>%
              filter(Country=="United States",
                     variable=="Overall Economic Opportunity Index") %>%
              mutate(industry=paste(tech,supply_chain),
                     econ_opp_index=value) %>%
              select(tech,supply_chain,econ_opp_index) %>%
              mutate(econ_opp_index=median_scurve(econ_opp_index)),by=c("tech","supply_chain")) %>%
  inner_join(energy_security_index %>%
               mutate(Country=str_to_title(Country)) %>%
               mutate(Country=recode(
                 Country,
                 "Korea, south" = "South Korea",
                 "Vietnam" = "Viet Nam",
                 "Turkey"          = "Turkiye",
                 "United kingdom"  = "United Kingdom",
                 "Curacao"         = "Cura�ao",
                 "Saudi arabia"         = "Saudi Arabia",
                 "Vietnam" = "Viet Name",
                 "United arab emirates" = "United Arab Emirates"
               )) %>%
               ungroup() %>%
               filter(variable=="Overall Energy Security Index") %>%
               mutate(industry=paste(tech,supply_chain),
                      energy_security_index=1-value) %>%
               select(Country,tech,supply_chain,industry,energy_security_index),by=c("country"="Country","tech","supply_chain")) %>%
  left_join(bilat_energy %>%
              mutate(Country=str_to_title(Country)) %>%
              mutate(Country=recode(
                Country,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Cura�ao",
                "Saudi arabia"         = "Saudi Arabia",
                "Vietnam" = "Viet Name",
                "United arab emirates" = "United Arab Emirates"
              )),by=c("country"="Country","tech")) %>%
  left_join(tech_ghg,by=c("tech"="Tech")) %>%
  left_join(cat %>%
              mutate(Country=str_to_title(Country)) %>%
              mutate(Country=recode(
                Country,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Cura�ao",
                "Saudi arabia"         = "Saudi Arabia",
                "Vietnam" = "Viet Name",
                "United arab emirates" = "United Arab Emirates"
              )) ,by=c("country"="Country")) %>%
  mutate(
    ghg_index            = replace_na(ghg_index,
                                      median(tech_ghg$ghg_index, na.rm = TRUE)),
    climate_policy_index = replace_na(climate_policy_index,
                                      median(cat$climate_policy_index, na.rm = TRUE))
  ) %>%
  rowwise() %>%
  mutate(
    us_opportunity_index = {                 # braces let you run many lines
      vals <- c(us_trade_index,econ_opp_index,energy_security_index)
      wts  <- c(2, 2,0.5)
      keep <- !is.na(vals) & !is.na(wts)
      weighted.mean(vals[keep], wts[keep])   # returns a single number
    }
  ) %>%
  mutate(penalty    = (1 - ghg_index) * climate_policy_index,
         us_opportunity_index=us_opportunity_index-0.25*penalty) %>%
  filter(ISO %in% country_info$iso2c,
         !country %in% c("China","Russia"),
         tech %in% techs,
         tech != "Geothermal") %>%
  
  ungroup() %>%
  mutate(us_opportunity_index = median_scurve(us_opportunity_index)) %>%
  arrange(desc(us_opportunity_index))

write.csv(us_opportunity_index %>%
            group_by(country) %>%
            slice_max(order_by=us_opportunity_index,n=1) %>%
            mutate(country_industry=paste0(country,": ",tech," ",supply_chain)) %>%
            select(ISO,country,country_industry,tech),"Downloads/us_opportunities.csv")

us_opp_index_total<-us_opportunity_index %>%
  group_by(country) %>%
  summarize(total_opportunity_index=mean(us_opportunity_index,na.rm=T)) %>%
  mutate(total_opportunity_index=median_scurve(total_opportunity_index)) %>%
  arrange(desc(total_opportunity_index))


#Development Indicators------------------------
#World Bank Doing Business
wb_db<-read.csv(paste0(raw_data,"wb_doingbusiness.csv")) %>%
  filter(Series.Code=="IC.BUS.EASE.DFRN.XQ.DB1719") %>%
  rename(doingbusiness=5) %>%
  mutate(doingbusiness=as.numeric(doingbusiness)) %>%
  select(1,2,5) %>%
  mutate(doingbusiness_index=median_scurve(doingbusiness)) %>%
  pivot_longer(
    cols      = c(doingbusiness,doingbusiness_index),      
    names_to  = "variable",
    values_to = "value"
  ) %>%
  mutate(
    data_type = if_else(str_ends(variable, "_index"), "index", "raw"),
    variable    = str_remove(variable, "_index$")   # drop the suffix
  ) 


#World Bank Development Indicators
wb_wdi<-read.csv(paste0(raw_data,"wb_wdi.csv")) %>%
  select(1:7) %>%
  pivot_longer(cols=c(5:7),names_to="year",values_to="value") %>%
  mutate(value=as.numeric(value)) %>%
  group_by(Country.Name,Country.Code,Series.Name,Series.Code) %>%
  summarize(value=mean(value,na.rm=T)) %>%
  group_by(Series.Name,Series.Code) %>%
  mutate(index=median_scurve(value)) %>%
  mutate(index = ifelse(Series.Code %in% c("FD.RES.LIQU.AS.ZS",
                                           "FB.BNK.CAPA.ZS",
                                           "FR.INR.DPST",
                                           "EG.IMP.CONS.ZS",
                                           "DT.DOD.DECT.GN.ZS",
                                           "IC.ELC.OUTG.ZS",
                                           "DC.DAC.USAL.CD",
                                           "DT.ODA.ODAT.PC.ZS",
                                           "TM.TAX.MRCH.WM.AR.ZS",
                                           "TM.TAX.MANF.WM.AR.ZS",
                                           "DT.TDS.DECT.GN.ZS"),1-index,index))

development_index <- country_info %>%
  filter(income != "High income") %>%
  distinct(iso3c,country,income) %>% 
  left_join(wb_wdi %>%
              mutate(Country.Name=str_to_title(Country.Name)) %>%
              mutate(Country.Name=recode(
                Country.Name,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Cura�ao",
                "Saudi arabia"         = "Saudi Arabia"
              )) %>%
              group_by(Country.Name,Country.Code) %>%
              summarize(development_index=mean(index,na.rm=T)) %>%
              filter(!is.na(development_index)) %>%
              ungroup() %>%
              mutate(development_index=median_scurve(development_index)),
            by=c("iso3c"="Country.Code","country"="Country.Name")) %>%
  left_join(wb_db %>%
              mutate(Country.Name=str_to_title(Country.Name)) %>%
              mutate(Country.Name=recode(
                Country.Name,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Cura�ao",
                "Saudi arabia"         = "Saudi Arabia"
              ))%>%
              filter(data_type=="index")%>%
              select(,-variable,-data_type) %>%
              rename(doingbusiness_index=value),by=c("iso3c"="Country.Code","country"="Country.Name")) %>%
  mutate(investment_enviro_index = (development_index+doingbusiness_index)/2) %>%
  filter(!is.na(investment_enviro_index)) %>%
  ungroup() %>%
  mutate(investment_enviro_index=median_scurve(investment_enviro_index)) %>%
  left_join(wb_wdi %>%
              mutate(Country.Name=str_to_title(Country.Name)) %>%
              mutate(Country.Name=recode(
                Country.Name,
                "Korea, south" = "South Korea",
                "Vietnam" = "Viet Nam",
                "Turkey"          = "Turkiye",
                "United kingdom"  = "United Kingdom",
                "Curacao"         = "Cura�ao",
                "Saudi arabia"         = "Saudi Arabia"
              )) %>%
              filter(Series.Code=="EG.USE.PCAP.KG.OE") %>%
              select(Country.Name,index) %>%
              rename(energy_use_index=index),by=c("country"="Country.Name"))

#Bilateral Development Assistance
oecd_1215<-read.csv(paste0(raw_data,"oecd_1215.csv"))
oecd_1518<-read.csv(paste0(raw_data,"oecd_1518.csv"))
oecd_1823<-read.csv(paste0(raw_data,"oecd_1823.csv"))
oecd<-rbind(oecd_1215,oecd_1518,oecd_1823) %>%
  distinct()
oecd<-oecd %>%
  group_by(Donor,RECIPIENT,Recipient,Sector,Measure) %>%
  summarize(aid=sum(OBS_VALUE,na.rm=T))

oecd_totalenergy<-oecd %>%
  filter(Sector=="Energy",
         Recipient != "Afghanistan") %>%
  group_by(Donor,Recipient,RECIPIENT) %>%
  summarize(aid=sum(aid,na.rm=T)) %>%
  left_join(gdp_data %>% 
              filter(year=="2023") %>%
              select(iso3c,gdp=NY.GDP.MKTP.CD),
            by=c("RECIPIENT"="iso3c")) %>%
  mutate(aid_gdp=aid/gdp) %>%
  arrange(desc(aid_gdp)) %>%
  ungroup() %>%
  mutate(aid_index=median_scurve(aid),
         aidgdp_index=median_scurve(aid_gdp))

oecd_sector<-oecd %>%
  filter(Sector != "Energy") %>%
  mutate(Sector=ifelse(grepl("Nuclear",Sector),"Nuclear",
                       ifelse(grepl("Solar",Sector),"Solar",
                              ifelse(grepl("Wind",Sector),"Wind",
                                     ifelse(grepl("Geothermal",Sector),"Geothermal",
                                            ifelse(grepl("Coal",Sector),"Coal",Sector)))))) %>%
  mutate(Sector=recode(
    Sector,
    "Energy distribution"="Electric Grid",
    "Energy generation, non-renewable sources" ="Fossil Generation",
    "Energy generation, renewable sources" ="Renewable Generation",
    "Electric power transmission and distribution (centralised grids)"="Electric Grid",
    "Energy generation, renewable sources - multiple technologies"="Renewable Generation",
    "Natural gas-fired electric power plants" ="Gas",
    "Electric power transmission and distribution (isolated mini-grids)"="Electric Grid",
    "Hydro-electric power plants" ="Renewable Generation"
  )) %>%
  group_by(Donor,RECIPIENT,Recipient,Sector) %>%
  summarize(aid=sum(aid,na.rm=T))

oecd_sector_clean <- oecd_sector %>% 
  ## 1.  Create a comma-separated tech string
  mutate(
    tech = case_when(
      str_detect(Sector, regex("^Fossil Generation$", TRUE)) ~ "Coal,Gas",
      str_detect(Sector, regex("^Renewable Generation$", TRUE)) ~ "Solar,Wind,Batteries",
      str_detect(Sector, regex("^Electric Grid$", TRUE)) ~ "Electric Grid",
      str_detect(Sector, regex("non[- ]renewable",  TRUE))             ~ "Coal,Gas",
      str_detect(Sector, regex("^Solar$", TRUE))            ~ "Solar",
      str_detect(Sector, regex("^Wind$", TRUE))             ~ "Wind",
      str_detect(Sector, regex("^Nuclear$", TRUE))          ~ "Nuclear",
      str_detect(Sector, regex("^Gas$", TRUE))              ~ "Gas",
      str_detect(Sector, regex("^Coal$", TRUE))             ~ "Coal",
      TRUE                                                  ~ NA
    )
  ) %>% 
  ## 2.  Duplicate rows wherever tech contains commas
  separate_rows(tech, sep = ",") %>% 
  ## 3.  Optionally re-factor to match your `techs` vector
  mutate(tech = factor(tech, levels = techs),
         supply_chain="Downstream") %>%
  left_join(gdp_data %>% 
              filter(year=="2023") %>%
              select(iso3c,gdp=NY.GDP.MKTP.CD),
            by=c("RECIPIENT"="iso3c")) %>%
  mutate(aid_gdp=aid/gdp) %>%
  arrange(desc(aid_gdp)) %>%
  ungroup() %>%
  mutate(aid_index=median_scurve(aid),
         aidgdp_index=median_scurve(aid_gdp))

eme_iso<-country_info %>%
  filter(income != "High income") %>%
  distinct(iso3c,country) 

library(dplyr)
library(purrr)
library(readr)
library(httr)
library(glue)

# -------------------------------------------------------------------
# 1. Make bigger chunks  (e.g. 40 ISO codes at a time) --------------
# -------------------------------------------------------------------
iso_vec    <- eme_iso %>% pull(iso3c)
chunk_size <- 132
iso_chunks <- split(iso_vec, ceiling(seq_along(iso_vec) / chunk_size))

# -------------------------------------------------------------------
# 2. Helper to fetch ONE chunk  -------------------------------------
# -------------------------------------------------------------------
fetch_chunk <- function(recipients) {
  
  recips <- paste(recipients, collapse = "+")  # AFG+ALB+ARG+.
  
  url <- glue(
    "https://sdmx.oecd.org/dcd-public/rest/data/",
    "OECD.DCD.FSD,DSD_CRS@DF_CRS,1.4/USA.{recips}.",
    "32262+32261+322+321+230+1000.100._T._T.D.Q._T..",
    "?startPeriod=2014",
    "&dimensionAtObservation=AllDimensions",
    "&format=csvfilewithlabels"
  )
  
  tmp <- tempfile(fileext = ".csv")
  
  RETRY(
    "GET", url,
    user_agent("my-oecd-scraper/0.1 (lachlan@example.com)"),
    write_disk(tmp, overwrite = TRUE),
    # **NO** pause_base / pause_cap ??? RETRY obeys server's Retry-After
    times = 6,               # one initial + 5 retries
    terminate_on = c(404)
  )
  
  # skip the two metadata rows, keep everything as character
  read_csv(
    tmp,
    col_names = FALSE,
    skip = 1,
    col_types = cols(.default = "c"),
    show_col_types = FALSE
  )
}

# -------------------------------------------------------------------
# 3. Loop over chunks, throttle between them ------------------------
# -------------------------------------------------------------------
all_oecd <- map_dfr(iso_chunks, function(chunk) {
  dat <- fetch_chunk(chunk)
  Sys.sleep(12)               # **throttle** ??? one call every 6s ??? 10/min
  dat
}, .progress = TRUE)

# -------------------------------------------------------------------
# 4. Quick checks ----------------------------------------------------
# -------------------------------------------------------------------
glimpse(all_oecd)                             # should now be BIGGER than 219
unique(all_oecd$X8)[1:20]                     # first 20 recipient names

names(all_oecd)[c(5,7,8,10,27,29)] <- c("DonorISO","RecipientISO","Recipient","Sector","Year","Value")
all_oecd <- all_oecd %>%
  select(c(5,7,8,10,27,29))

oecd_supplychain <- all_oecd %>%
  filter(!Sector %in%  c("All sectors",
                         "Coal",
                         "Oil and gas (Upstream)")) %>%
  mutate(supply_chain = ifelse(Sector=="Energy","Downstream",
                               ifelse(Sector=="Industry","Midstream",
                                      ifelse(Sector=="Mineral resources and mining","Upstream","Upstream"))),
         Value=as.numeric(Value)) %>%
  group_by(DonorISO,RecipientISO,Recipient,supply_chain) %>%
  summarize(aid=sum(Value,na.rm=T)) %>%
  left_join(gdp_data %>% 
              filter(year=="2023") %>%
              select(iso3c,gdp=NY.GDP.MKTP.CD),
            by=c("RecipientISO"="iso3c")) %>%
  mutate(aid_gdp=aid/gdp) %>%
  arrange(desc(aid_gdp)) %>%
  ungroup() %>%
  mutate(aid_index=median_scurve(aid),
         aidgdp_index=median_scurve(aid_gdp))


oecd_all <- all_oecd %>%
  mutate(Value=as.numeric(Value)) %>%
  filter(Sector == "All sectors") %>%
  group_by(DonorISO,RecipientISO,Recipient) %>%
  summarize(aid=sum(Value,na.rm=T)) %>%
  left_join(gdp_data %>% 
              filter(year=="2023") %>%
              select(iso3c,gdp=NY.GDP.MKTP.CD),
            by=c("RecipientISO"="iso3c")) %>%
  mutate(aid_gdp=aid/gdp) %>%
  arrange(desc(aid_gdp)) %>%
  ungroup() %>%
  mutate(aid_index=median_scurve(aid),
         aidgdp_index=median_scurve(aid_gdp))

#Total Development Potential Index
energy_security_clean <-
  energy_security_index %>%
  mutate(Country = recode(Country,
                          "Turkey"          = "T�rkiye",
                          "United kingdom"  = "United Kingdom",
                          "Curacao"         = "Cura�ao",
                          "Vietnam"         = "Viet Nam")) %>%
  filter(variable == "Overall Energy Security Index") %>%
  transmute(country = Country,
            tech,
            supply_chain,
            energy_security = 1 - value)

econ_opp_clean<-
  econ_opp_index %>%
  mutate(Country = recode(str_to_title(Country),
                          "Turkey"         = "Turkiye",
                          "Saudi arabia"   = "Saudi Arabia",
                          "Curacao"        = "Cura�ao",
                          "United kingdom" = "United Kingdom",
                          "Vietnam"        = "Viet Nam")) %>%
  filter(variable == "Overall Economic Opportunity Index") %>%
  transmute(country = Country,
            tech,
            supply_chain,
            economic_opportunity = value)

us_opp_index <- econ_opp_index %>%
  filter(Country == "United States",
         variable == "Overall Economic Opportunity Index") %>%
  transmute(tech, supply_chain,
            us_economic_opportunity = value)

library(purrr)

tables <- list(
  energy_security_clean,            # already pre-processed
  # already pre-processed
  econ_opp_clean,                # already pre-processed
  oecd_sector_clean%>%
    mutate(country=Recipient))

dev_potential_index <- reduce(tables, full_join, by = c("country", "tech", "supply_chain")) %>%
  left_join(us_opp_index,by=c("tech","supply_chain")) %>%
  left_join(oecd_supplychain,by=c("country"="Recipient","supply_chain")) %>%
  left_join(development_index %>% mutate(development_index = 1 - development_index, 
                                         energy_use_index  = 1 - energy_use_index),
            by=c("country")) %>%
  filter(country %in% eme_iso$country) %>%
  rowwise() %>%
  mutate(
    dev_potential_index = {                 # braces let you run many lines
      vals <- c(development_index,
                doingbusiness_index,
                investment_enviro_index,
                energy_security,
                economic_opportunity,
                us_economic_opportunity,
                aid_index.x,
                aidgdp_index.x,
                aid_index.y,
                aidgdp_index.y,
                energy_use_index)
      wts  <- c(1, 1,1, 1,1.5, 1.5,1.5,1.5,1.5,2,1)
      keep <- !is.na(vals) & !is.na(wts)
      weighted.mean(vals[keep], wts[keep])   # returns a single number
    }
  ) %>%
  ungroup() %>%
  select(-Country) %>%
  arrange(desc(dev_potential_index)) 

dev_potential_index_total<- dev_potential_index %>%
  
  group_by(country) %>%
  slice_max(dev_potential_index,n=5) %>%
  summarize(total_opportunity_index=mean(dev_potential_index,na.rm=T)) %>%
  mutate(total_opportunity_index=median_scurve(total_opportunity_index)) %>%
  arrange(desc(total_opportunity_index))


#Top 10 of Each library(dplyr)
country_flags<-read.csv("Downloads/Country flag lookup table - Sheet1.csv")
compacts <- bind_cols(
  # 1) Safer (friend-shore) block
  us_friendshore_index %>%
    ungroup() %>%
    mutate(ci = paste0(country,": ",industry)) %>%
    left_join(country_flags,by=c("country"="Country")) %>%
    select(
      code_1=Code,
      "Safer (Friendshoring)"       = ci,
      Safer_index = us_friendshore_index
    ) %>%
    slice_max(Safer_index, n = 100),
  
  # 2) Prosperous (opportunity) block
  us_opportunity_index %>%
    ungroup() %>%
    mutate(ci = paste0(country,": ",tech," ",supply_chain),
           country=ifelse(country=="Turkey","T�rkiye",
                          ifelse(country=="United kingdom","United Kingdom",
                                 ifelse(country=="Curacao","Cura�ao",
                                        ifelse(country=="Vietnam","Viet Nam",country))))) %>%
    left_join(country_flags,by=c("country"="Country")) %>%
    select(
      code_2=Code,
      "Prosperous (Exports)"       = ci,
      Prosperous_index = us_opportunity_index
    ) %>%
    slice_max(Prosperous_index, n = 100),
  
  # 3) Stronger (development) block
  dev_potential_index %>%
    ungroup() %>%
    mutate(ci = paste0(country,": ",tech," ",supply_chain)) %>%
    left_join(country_flags,by=c("country"="Country")) %>%
    select(
      code_3=Code,
      "Stronger (Development)"       = ci,
      Stronger_index = dev_potential_index
    ) %>%
    slice_max(Stronger_index, n = 100)
) %>%
  # finally, put the columns in the order you want:
  select(
    code_1,`Safer (Friendshoring)`, Safer_index,
    code_3,`Stronger (Development)` , Stronger_index,
    code_2,`Prosperous (Exports)`, Prosperous_index
  )

write.csv(compacts,"Downloads/compacts.csv")

# Partnership Strength Index-------------------
strategy_tbl <- us_friendshore_index  %>%
  mutate(us_friendshore_index=median_scurve(us_friendshore_index)) %>%
  group_by(country) %>%
  slice_max(us_friendshore_index, n = 5) %>%
  summarize(us_friendshore_index=mean(us_friendshore_index,na.rm=T)) %>%
  select(country,
         us_friendshore_index) %>%
  left_join(us_opportunity_index   %>% 
              mutate(us_opportunity_index=median_scurve(us_opportunity_index)) %>%
              group_by(country) %>%
              slice_max(us_opportunity_index, n = 5) %>%
              summarize(us_opportunity_index=mean(us_opportunity_index,na.rm=T)) %>%
              select(country,
                     us_opportunity_index), 
            by = "country") |>
  left_join(dev_potential_index     %>%
              ungroup() %>%
              mutate(dev_potential_index=median_scurve(dev_potential_index)) %>%
              group_by(country) %>%
              slice_max(dev_potential_index,n=5) %>%
              left_join(gdp %>%
                          filter(year=="2024") %>%
                          select(iso3c,GDP),by=c("iso3c")) %>%
              summarize(dev_potential_index=weighted.mean(dev_potential_index,w=GDP,na.rm=T)) %>%
              select(country,
                     dev_potential_index), 
            by = "country") %>%
  rowwise() %>%
  mutate(
    psi = weighted.mean(
      c_across(c(us_friendshore_index, us_opportunity_index, dev_potential_index)),
      c(0.4, 0.4, 0.2),
      na.rm = TRUE
    )
  ) %>%
  ungroup() %>%
  arrange(desc(psi)) %>% 
  filter(country %in% all_countries,
         country != "Guyana")

compacts <- bind_cols(
  strategy_tbl %>%
    slice_max(psi, n = 20) %>%
    left_join(country_flags,by=c("country"="Country")) %>%
    select(
      code_0=Code,
      "Energy Partnership"       = country,
      Partnership_index = psi
    ),
  strategy_tbl %>%
    slice_max(us_friendshore_index, n = 20) %>%
    left_join(country_flags,by=c("country"="Country")) %>%
    select(
      code_1=Code,
      "Safer (Friendshoring)"       = country,
      Safer_index = us_friendshore_index
    ),
  strategy_tbl %>%
    slice_max(us_opportunity_index, n = 20) %>%
    left_join(country_flags,by=c("country"="Country")) %>%
    select(
      code_2=Code,
      "Prosperous (Exports)"       = country,
      Prosperous_index = us_opportunity_index
    ),
  strategy_tbl %>%
    slice_max(dev_potential_index, n = 20) %>%
    left_join(country_flags,by=c("country"="Country")) %>%
    select(
      code_3=Code,
      "Stronger (Development)"       = country,
      Stronger_index = dev_potential_index
    )) %>%
  select(
    code_0, `Energy Partnership`,Partnership_index,
    code_1,`Safer (Friendshoring)`, Safer_index,
    code_3,`Stronger (Development)` , Stronger_index,
    code_2,`Prosperous (Exports)`, Prosperous_index
  )

write.csv(compacts,"Downloads/compacts.csv")


#Regional PSI
region_strategy <- strategy_tbl %>%
  mutate(iso3c=countrycode(country,"country.name","iso3c")) %>%
  left_join(world %>%
              select(iso_a3,subregion),by=c("iso3c"="iso_a3")) %>%
  left_join(gdp %>%
              filter(year=="2024") %>%
              select(iso3c,GDP),by=c("iso3c")) %>%
  filter(!is.na(GDP)) %>%
  # 2) group & collapse
  group_by(subregion) %>%
  summarise(
    across(
      us_friendshore_index:psi,
      ~ weighted.mean(.x, w = GDP, na.rm = TRUE)
    ),
    .groups = "drop"
  ) %>%
  arrange(desc(psi))


##Environmental Impact Indices----------------------------------
clean_xwalk  <- read_excel(paste0(raw_data, "clean_industry_naics_test.xlsx")) %>%
  rename(industry = clean_industry,
         supply_chain = Production_Phase,
         X6.Digit.Code    = X6_Digit_Code)

fossil_xwalk <- read_excel(file.path(raw_data, "fossil_industry_naics.xlsx"), sheet = 1)

all_xwalk    <- bind_rows(fossil_xwalk, clean_xwalk) %>%
  mutate(supply_chain=case_when(
    supply_chain=="Input" ~ "Upstream",
    supply_chain=="Midstream" ~ "Midstream",
    supply_chain %in% c("Operations",
                        "Transport_Handling",
                        "Design_Engineering","Construction") ~ "Downstream"
  ))

# 3.  eGRID emissions  --------------------------------------------------
techs <- c("Electric Vehicles","Nuclear","Coal","Batteries","Green Hydrogen",
           "Wind","Oil","Solar","Gas","Geothermal")

egrid <- paste0(raw_data, "egrid2023_data_metric_rev2.xlsx") %>%
  read_excel(sheet = 4, skip = 1) %>%
  select(PLFUELCT, PLNOXAN, PLSO2AN) %>%
  rename(industry          = PLFUELCT,
         `Nitrogen Oxides` = PLNOXAN,
         `Sulfur Dioxide`  = PLSO2AN) %>%
  mutate(industry = recode(industry,
                           "WIND" = "Wind", "SOLAR" = "Solar", "GEOTHERMAL" = "Geothermal",
                           "NUCLEAR" = "Nuclear", "OIL" = "Oil", "COAL" = "Coal",
                           "GAS" = "Gas", "OTHF" = "Green Hydrogen"),
         supply_chain = "Operations") %>%
  filter(industry %in% techs)

# ───────────────────────────────────────────────────────────────────────
# 4.  NEI 2022 pollutants  ---------------------------------------------
target_pollutants <- c("Nitrogen Oxides", "Sulfur Dioxide",
                       "PM2.5 Primary (Filt + Cond)",
                       "PM10 Primary (Filt + Cond)",
                       "Volatile Organic Compounds")
NEI<- paste0(raw_data, "2022-NEI-Co-pollutant data.xlsx") %>%
  readxl::read_xlsx()
NEI_onroad <- paste0(raw_data, "2022-NEI-Co-pollutant-data-onroad.xlsx") %>%
  readxl::read_xlsx()%>% mutate(industry = 'Oil', supply_chain = 'Downstream')##load this to account for downstream oil emissions

nei_wide <- NEI %>%
  left_join(all_xwalk, by = c("NAICS Description" = "naics_desc")) %>%
  mutate(industry = recode(industry, `Wind Energy` = "Wind")) %>%
  filter(industry %in% techs,
         Pollutant %in% target_pollutants) %>%
  transmute(industry, supply_chain, `EIS Facility ID`,
            Pollutant,
            tons = as.numeric(`2022 Final Emis. (tons)`)) %>%
  group_by(industry, supply_chain, `EIS Facility ID`, Pollutant) %>%
  summarise(tons = sum(tons, na.rm = TRUE), .groups = "drop") %>%
  pivot_wider(names_from = Pollutant,
              values_from = tons,
              values_fill = 0)

NEI_onroad_wide <- NEI_onroad %>% 
  mutate(Pollutant = recode(Pollutant, NOx = "Nitrogen Oxides",
                            VOC = "Volatile Organic Compounds",
                            `PM10-PRI` = "PM10 Primary (Filt + Cond)",
                            `PM25-PRI` = "PM2.5 Primary (Filt + Cond)",
                            SO2 = "Sulfur Dioxide")) %>%
  transmute(industry, supply_chain,Pollutant,
            tons = as.numeric(`2022 Final Emis. (tons)`)) %>%
  group_by(industry, supply_chain, Pollutant) %>%
  summarise(tons = sum(tons, na.rm = TRUE), .groups = "drop") %>%
  pivot_wider(names_from = Pollutant,
              values_from = tons,
              values_fill = 0)

# ───────────────────────────────────────────────────────────────────────
# 5.  Align eGRID & NEI columns  ---------------------------------------
pollutant_cols <- setdiff(names(nei_wide),
                          c("industry", "supply_chain", "EIS Facility ID"))

egrid_aligned <- egrid %>%
  bind_cols(
    # add missing pollutant columns filled with 0
    as_tibble(matrix(0,
                     nrow = nrow(egrid),
                     ncol = length(setdiff(pollutant_cols, names(egrid)))),
              .name_repair = ~ setdiff(pollutant_cols, names(egrid)))
  )

combined <- bind_rows(
  nei_wide  %>% select(industry, supply_chain, all_of(pollutant_cols)),
  egrid_aligned
)
combined <- combined %>%
  bind_rows(NEI_onroad_wide)

# ───────────────────────────────────────────────────────────────────────
# 6.  EPA Benefit-Per-Ton factors ($/short-ton, 2023$) ------------------
damage_wt <- c(
  `PM2.5 Primary (Filt + Cond)` = 113000,
  `Sulfur Dioxide`              =  57000,
  `Nitrogen Oxides`             =   7710,
  `Volatile Organic Compounds`  =  12600,
  `PM10 Primary (Filt + Cond)`  = 113000 * 0.33
)

# ───────────────────────────────────────────────────────────────────────
# 7.  $-damage PER facility (row)  -------------------------------------
combined_dmg <- combined %>%
  mutate(across(all_of(names(damage_wt)), as.numeric)) %>%
  mutate(public_health_damage =
           rowSums(across(all_of(names(damage_wt))) *
                     damage_wt[names(damage_wt)], na.rm = TRUE))

# ───────────────────────────────────────────────────────────────────────
# 8-A.  Total damage by industry × phase  -------------------------------
industry_phase <- combined_dmg %>%
  group_by(industry, supply_chain) %>%
  summarise(damage = mean(public_health_damage, na.rm = TRUE),
            damage_tot=sum(public_health_damage,na.rm=T),
            .groups = "drop") %>%
  mutate(damage_index_0_1 = rescale(damage, to = c(0, 1)),
         damagetot_index_0_1 = rescale(damage_tot, to = c(0, 1))) %>%
  arrange(desc(damage_index_0_1))

# 8-B.  Total damage by industry (all phases)  --------------------------
industry_total <- combined_dmg %>%
  group_by(industry) %>%
  summarise(damage = mean(public_health_damage, na.rm = TRUE),
            .groups = "drop") %>%
  mutate(index_0_1 = rescale(damage, to = c(0, 1))) %>%
  arrange(desc(index_0_1))

# ───────────────────────────────────────────────────────────────────────
# 9.  Write outputs  ----------------------------------------------------
write_csv(industry_phase,
          file.path(out_dir, "Health_damage_by_industry_phase.csv"))
write_csv(industry_total,
          file.path(out_dir, "Health_damage_by_industry.csv"))

#PART TWO:CLIMATE INDEX___________________________________________________
#10. Load CO2 data from 2020 NEI-----------------------------------------------------

NEI_CO2<- readxl::read_xlsx(paste0(raw_data,"2020 Facility-Level Data for Point  Emissions (70f61977-e493-46d2-b03d-b62a6e683153).xlsx"))

nei_wide_CO2 <- NEI_CO2 %>%
  left_join(all_xwalk, by = c("NAICS" = "naics_desc")) %>%
  mutate(industry = recode(industry, `Wind Energy` = "Wind")) %>%
  filter(industry %in% techs) %>%
  transmute(industry, supply_chain, `EIS Facility ID`,
            Pollutant,
            tons = as.numeric(`Emissions Tons`)) %>%
  group_by(industry, supply_chain, `EIS Facility ID`, Pollutant) %>%
  summarise(tons = sum(tons, na.rm = TRUE), .groups = "drop") %>%
  pivot_wider(names_from = Pollutant,
              values_from = tons,
              values_fill = 0)

#11. Load CO2 data from egrid-----------------------
egrid_CO2 <- paste0(raw_data, "egrid2023_data_metric_rev2.xlsx") %>%
  readxl::read_xlsx(sheet = 4, skip = 1) %>%
  select(PLFUELCT, PLCO2AN, PLCH4AN, PLN2OAN) %>%
  rename(industry          = PLFUELCT,
         `Carbon Dioxide` = PLCO2AN,
         `Methane`  = PLCH4AN,
         `Nitrous Oxide` = PLN2OAN) %>%
  mutate(industry = recode(industry,
                           "WIND" = "Wind", "SOLAR" = "Solar", "GEOTHERMAL" = "Geothermal",
                           "NUCLEAR" = "Nuclear", "OIL" = "Oil", "COAL" = "Coal",
                           "GAS" = "Gas", "OTHF" = "Green Hydrogen"),
         supply_chain = "Downstream") %>%
  filter(industry %in% techs)

#12. Combine both sources-------------------------------------------
pollutant_cols <- setdiff(names(nei_wide_CO2),
                          c("industry", "supply_chain", "EIS Facility ID"))

egrid_CO2_aligned <- egrid_CO2 %>%
  bind_cols(
    # add missing pollutant columns filled with 0
    as_tibble(matrix(0,
                     nrow = nrow(egrid_CO2),
                     ncol = length(setdiff(pollutant_cols, names(egrid_CO2)))),
              .name_repair = ~ setdiff(pollutant_cols, names(egrid_CO2)))
  )

combined_CO2 <- bind_rows(
  nei_wide_CO2  %>% select(industry, supply_chain, all_of(pollutant_cols)),
  egrid_CO2_aligned
)

#Emissions by fuel type and econ sector 

ems_fuel<-read.csv(paste0(raw_data,"ghg_ems_fossil_combustion_fuel.csv")) %>%
  mutate(supply_chain=case_when(
    Sector %in% c("Residential","Transportation") ~ "Downstream"
  ),
  co2e=as.numeric(X2022)*1000000) %>%
  select(industry=Fuel,supply_chain,co2e) %>%
  group_by(industry,supply_chain) %>%
  summarize(`Carbon Dioxide`=sum(co2e,na.rm=T)) %>%
  filter(!is.na(supply_chain)) %>%
  mutate(Methane =NA,
         `Nitrous Oxide`=NA,
         `Sulfur Hexafluoride`=NA)

combined_CO2 <- bind_rows(combined_CO2,ems_fuel)

## 13. group by industry and phase, add a total CO2e column, and rescale on 0-1 scale----------

facility_totals <- combined_CO2 %>% 
  rowwise() %>% 
  mutate(total_co2e = sum(c_across(where(is.numeric)), na.rm = TRUE)) %>% 
  ungroup()

# ----------------------------------------------------------------------
# 13 A: average total CO₂-e by INDUSTRY × PRODUCTION PHASE
industry_phase_CO2 <- facility_totals %>% 
  group_by(industry, supply_chain) %>% 
  summarise(
    avg_total_co2e = mean(total_co2e, na.rm = TRUE),
    total_co2e=sum(total_co2e, na.rm = TRUE),
    .groups = "drop"
  ) %>% 
  mutate(co2_index_0_1 = rescale(avg_total_co2e, to = c(0, 1)),
         co2_index_tot = rescale(total_co2e, to = c(0, 1))) %>% 
  arrange(desc(co2_index_0_1))

# ----------------------------------------------------------------------


enviro_index<-industry_phase %>%
  left_join(industry_phase_CO2,by=c("industry","supply_chain")) %>%
  filter(!is.na(industry)) %>%
  rowwise() %>% 
  mutate(
    enviro_index = mean(
      c_across(c(co2_index_0_1, damage_index_0_1)), 
      na.rm = TRUE
    ),
    enviro_index_tot = mean(
      c_across(c(co2_index_tot, damagetot_index_0_1)), 
      na.rm = TRUE
    )
  ) %>%
  arrange(desc(enviro_index_tot))

write.csv(enviro_index %>%
            rename(tech=industry) %>%
            mutate(industry=paste0(tech,": ",supply_chain)),"Downloads/enviro_index.csv")

# OPTION B: average total CO₂-e by INDUSTRY only
industry_CO2 <- facility_totals %>% 
  group_by(industry) %>% 
  summarise(
    total_co2e = sum(total_co2e, na.rm = TRUE),
    .groups = "drop"
  ) %>% 
  mutate(index_0_1 = rescale(total_co2e, to = c(0, 1))) %>% 
  arrange(desc(index_0_1))



enviro_industry_index <- industry_total %>%
  left_join(industry_CO2,by=c("industry"))%>%
  filter(!is.na(industry))%>%
  rowwise() %>% 
  mutate(
    enviro_index = mean(
      c_across(c(index_0_1.x, index_0_1.y)), 
      na.rm = TRUE
    ))

write.csv(enviro_industry_index %>%
            select(industry,index_0_1.x, index_0_1.y),"Downloads/enviro_industry.csv")

tech_table<-left_join(us_scatter_index %>%
                        ungroup() %>%
                        transmute(tech,supply_chain,`Overall Economic Opportunity Index`,
                                  `Overall Energy Security Index`=1-`Overall Energy Security Index`),
                      enviro_index %>%
                        select(industry,supply_chain,co2_index_tot),
                      by=c("tech"="industry",
                           "supply_chain")) %>%
  mutate(enviro_index=1-co2_index_tot) %>%
  rowwise() %>% 
  mutate(
    total_index = mean(
      c_across(c(`Overall Energy Security Index`, `Overall Economic Opportunity Index`,co2_index_tot)), 
      na.rm = TRUE
    )) 

%>%
  write.csv("Downloads/tech_table.csv")


# Install if needed
install.packages("plotly")
library(plotly)

# Sample data inside a cube (0-1 range)
set.seed(123)
tech_table<-as.data.frame(tech_table %>%
                            mutate(industry=paste0(tech,": ",supply_chain)))

library(plotly)


library(plotly)
library(dplyr)
library(htmlwidgets)

# Prepare your palette
rmi_palette <- c("#0BD0D9", "#0989B1", "#003A61", "#FFCA08", "#F8931D", "#548538", "#7F7F7F",
                 "#E63946", "#F4A261", "#2A9D8F", "#264653", "#E76F51", "#8D99AE", "#9C6644",
                 "#6A4C93", "#F77F00", "#80B918", "#005F73", "#9D4EDD", "#EF233C")

# Transform Z-axis to log scale

# Start base plot
p <- plot_ly(
  tech_table,
  x = ~`Overall Energy Security Index`,
  y = ~`Overall Economic Opportunity Index`,
  z = ~enviro_index,
  type = "scatter3d",
  mode = "markers+text",
  text = ~industry,
  textposition = "top center",
  hoverinfo = "text",
  color = ~tech,
  colors = rmi_palette,
  marker = list(size = 4),
  textfont = list(size = 9)
)

# Helper to add dividing planes
library(plotly)
library(dplyr)

# RMI palette
rmi_palette <- c("#0BD0D9", "#0989B1", "#003A61", "#FFCA08", "#F8931D", "#548538", "#7F7F7F",
                 "#E63946", "#F4A261", "#2A9D8F", "#264653", "#E76F51", "#8D99AE", "#9C6644",
                 "#6A4C93", "#F77F00", "#80B918", "#005F73", "#9D4EDD", "#EF233C")

library(plotly)

# Define RMI palette (first 8 colors)
rmi_palette <- c("#0BD0D9", "#0989B1", "#003A61", "#FFCA08", 
                 "#F8931D", "#548538", "#7F7F7F", "#E63946")

# Function to add one mini cube at given corner coords
add_cube <- function(p, xmin, xmax, ymin, ymax, zmin, zmax, color) {
  # Vertices of the cube (8 corners)
  x <- c(xmin, xmax, xmax, xmin, xmin, xmax, xmax, xmin)
  y <- c(ymin, ymin, ymax, ymax, ymin, ymin, ymax, ymax)
  z <- c(zmin, zmin, zmin, zmin, zmax, zmax, zmax, zmax)
  
  # Faces defined by vertex indices (0-based)
  faces <- list(
    c(0,1,2,3),  # bottom
    c(4,5,6,7),  # top
    c(0,1,5,4),  # front
    c(2,3,7,6),  # back
    c(1,2,6,5),  # right
    c(0,3,7,4)   # left
  )
  
  # Flatten faces to triangles
  i <- c(); j <- c(); k <- c()
  for (f in faces) {
    i <- c(i, f[1], f[1])
    j <- c(j, f[2], f[3])
    k <- c(k, f[3], f[4])
  }
  
  p %>% add_trace(
    type = "mesh3d",
    x = x, y = y, z = z,
    i = i, j = j, k = k,
    facecolor = rep(color, length(i)),
    opacity = 0.025,
    showlegend = FALSE
  )
}

# Start empty plot
p <- plot_ly()

# Add 8 cubes, looping through x/y/z splits
cube_idx <- 1
for (xi in 0:1) {
  for (yi in 0:1) {
    for (zi in 0:1) {
      xmin <- xi * 0.5
      xmax <- xmin + 0.5
      ymin <- yi * 0.5
      ymax <- ymin + 0.5
      zmin <- zi * 0.5
      zmax <- zmin + 0.5
      p <- add_cube(p, xmin, xmax, ymin, ymax, zmin, zmax, rmi_palette[cube_idx])
      cube_idx <- cube_idx + 1
    }
  }
}

# Add your data points
p <- p %>% add_trace(
  data = tech_table,
  x = ~`Overall Energy Security Index`,
  y = ~`Overall Economic Opportunity Index`,
  z = ~enviro_index,
  type = "scatter3d",
  mode = "markers+text",
  text = ~industry,
  textposition = "top center",
  hoverinfo = "text",
  color = ~tech,
  colors = rmi_palette,
  marker = list(size = 4),
  textfont = list(size = 9)
)

# Final layout
p <- p %>% layout(scene = list(
  xaxis = list(title = "Energy Security", range = c(0, 1)),
  yaxis = list(title = "Economic Opportunity", range = c(0, 1)),
  zaxis = list(title = "Environmental Stewardship", range = c(0, 1)),
  aspectmode = "cube"
))

p

# Final layout
p <- p %>%
  layout(scene = list(
    xaxis = list(title = "Energy Security", range = c(0, 1)),
    yaxis = list(title = "Economic Opportunity", range = c(0, 1)),
    zaxis = list(title = "Environmental Index", range = c(0,1)),
    aspectmode = "cube"
  ))

# Save or display
# saveWidget(p, "cube_plot.html", selfcontained = TRUE)
p

library(htmlwidgets)

# your plot object

# save to file
saveWidget(p, "Downloads/cube_plot.html", selfcontained = TRUE)


library(plotly)
library(dplyr)

# Define color palette
rmi_palette <- c("#0BD0D9", "#0989B1", "#003A61", "#FFCA08", 
                 "#F8931D", "#548538", "#7F7F7F", "#E63946",
                 "#F4A261", "#2A9D8F", "#264653", "#E76F51", 
                 "#8D99AE", "#9C6644", "#6A4C93", "#F77F00", 
                 "#80B918", "#005F73", "#9D4EDD", "#EF233C")

# Optional: map each tech to a color
tech_levels <- unique(tech_table$tech)
tech_colors <- setNames(rmi_palette[1:length(tech_levels)], tech_levels)
tech_table$enviro_index=1-tech_table$enviro_index

# Bar width
bar_width <- 0.02

# Start empty plot
p <- plot_ly()

# Loop over each row to draw bars
for (i in 1:nrow(tech_table)) {
  row <- tech_table[i, ]
  x0 <- row$`Overall Energy Security Index`
  y0 <- row$`Overall Economic Opportunity Index`
  z0 <- 0
  z1 <- row$enviro_index
  color <- tech_colors[[as.character(row$tech)]]
  
  # Define bar corners
  x <- c(x0 - bar_width, x0 + bar_width, x0 + bar_width, x0 - bar_width,
         x0 - bar_width, x0 + bar_width, x0 + bar_width, x0 - bar_width)
  y <- c(y0 - bar_width, y0 - bar_width, y0 + bar_width, y0 + bar_width,
         y0 - bar_width, y0 - bar_width, y0 + bar_width, y0 + bar_width)
  z <- c(rep(z0, 4), rep(z1, 4))
  
  # Define faces (6 cube faces, split into 2 triangles each)
  faces <- list(
    c(0,1,2,3),  # bottom
    c(4,5,6,7),  # top
    c(0,1,5,4),  # front
    c(2,3,7,6),  # back
    c(1,2,6,5),  # right
    c(0,3,7,4)   # left
  )
  i <- c(); j <- c(); k <- c()
  for (f in faces) {
    i <- c(i, f[1], f[1])
    j <- c(j, f[2], f[3])
    k <- c(k, f[3], f[4])
  }
  
  # Add the mesh bar
  p <- add_trace(p,
                 type = "mesh3d",
                 x = x, y = y, z = z,
                 i = i, j = j, k = k,
                 facecolor = rep(color, length(i)),
                 opacity = 0.85,
                 hoverinfo = "text",
                 text = paste0("<b>", row$industry, "</b><br>Tech: ", row$tech, "<br>Enviro Index: ", signif(z1, 3)),
                 showlegend = FALSE
  )
  
  # Add the label at top of bar
  p <- add_trace(p,
                 type = "scatter3d",
                 mode = "text",
                 x = ~x0, y = ~y0, z = ~z0,
                 text = ~row$industry,
                 textposition = "top center",
                 showlegend = FALSE,
                 hoverinfo = "none",
                 textfont = list(size = 9, color = "black")
  )
}

# Final layout
p <- layout(p,
            scene = list(
              xaxis = list(title = "Energy Security", range = c(0, 1)),
              yaxis = list(title = "Economic Opportunity", range = c(0, 1)),
              zaxis = list(title = "Environmental Stewardship", range = c(0, 1)),
              aspectmode = "cube"
            )
)

p



# 9.  Write outputs  ----------------------------------------------------
write_csv(industry_phase_CO2,
          file.path(out_dir, "CO2e_emissions_by_industry_phase.csv"))
write_csv(industry_CO2,
          file.path(out_dir, "CO2e_emissions_by_industry.csv"))





##CHARTS-------------------------


#Master Energy Agreements
library(dplyr)
library(stringr)

# helper to collapse the top-5 industries for *any* index table ------------
top5_by_country <- function(.data, score_col, tech_col = "tech",
                            chain_col = "supply_chain", out_name) {
  
  .data %>% 
    ungroup() %>%                                # safety
    mutate(industry = str_c(.data[[tech_col]], " ", .data[[chain_col]])) %>% 
    group_by(country) %>% 
    slice_max({{ score_col }}, n = 5, with_ties = FALSE) %>% 
    summarise(!!out_name := str_c(industry, collapse = "; "), .groups = "drop")
}

# 1) make three small country-level tables ---------------------------------
friendshore_top  <- top5_by_country(us_friendshore_index,
                                    score_col = us_friendshore_index,
                                    out_name  = "friendshore_top5")

opportunity_top  <- us_opportunity_index %>%                # fix country names first
  mutate(country = recode(country,
                          "Turkey"          = "T�rkiye",
                          "United kingdom"  = "United Kingdom",
                          "Curacao"         = "Cura�ao",
                          "Vietnam"         = "Viet Nam")) %>% 
  top5_by_country(score_col = us_opportunity_index,
                  out_name  = "opportunity_top5")

devpot_top       <- top5_by_country(dev_potential_index,
                                    score_col = dev_potential_index,
                                    out_name  = "devpot_top5")

# 2) join them to the strategy table ---------------------------------------
meas <- strategy_tbl %>% 
  select(country, psi) %>% 
  arrange(desc(psi)) %>% 
  left_join(friendshore_top, by = "country") %>% 
  left_join(opportunity_top, by = "country") %>% 
  left_join(devpot_top,   by = "country")

library(dplyr)
library(countrycode)
library(readr)

meas_export <- meas %>%                       # <- the tibble you built earlier
  mutate(
    iso_a3 = countrycode(country, "country.name", "iso3c"),
    
    # turn any list-column into ONE quoted string, with a separator Datawrapper
    # will not treat as a column delimiter - "|" (pipe) is a safe choice
    across(ends_with("_top5"),
           ~ if(is.list(.x)) sapply(.x, paste, collapse = " | ") else .x)
  ) %>%
  select(iso_a3, country,psi, friendshore_top5, opportunity_top5, devpot_top5)

write_csv(meas_export %>%
            slice_max(psi,n=15), "Downloads/meas_datawrapper.csv", na = "")

meas_split <- meas_export %>%                               
  mutate(psi=median_scurve(psi)) %>%
  
  mutate(across(ends_with("_top5"), ~ str_squish(.x))) %>%  # trim stray spaces
  separate_wider_delim(                                     # friend-shore
    cols  = friendshore_top5,
    delim = ";",
    names = paste0("friendshore_", 1:5),
    too_few = "align_start"            # keeps order 1-5 even if fewer items
  ) %>%
  separate_wider_delim(                                     # opportunity
    cols  = opportunity_top5,
    delim = ";",
    names = paste0("opportunity_", 1:5),
    too_few = "align_start"
  ) %>%
  separate_wider_delim(                                     # dev-potential
    cols  = devpot_top5,
    delim = ";",
    names = paste0("devpot_", 1:5),
    too_few = "align_start"
  ) %>%
  select(-ends_with("_top5"))   


write_csv(meas_split, "Downloads/meas_split_datawrapper.csv", na = "")

write.csv(meas_split %>%
            left_join(country_flags,by=c("country"="Country")) %>%
            slice_max(psi,n=12),"Downloads/meas_split_datawrapper12.csv")

library(dplyr)
library(readr)
library(stringr)

# 1.  Collapse to wide (if you haven't already) ----------------------------
meas_wide <- meas_long %>%                 # your long table
  group_by(country, psi) %>%               # keep psi once
  summarise(
    friendshore_top5  = paste(industry[metric=="friendshore_top5"],  collapse = " | "),
    opportunity_top5  = paste(industry[metric=="opportunity_top5"],  collapse = " | "),
    devpot_top5       = paste(industry[metric=="devpot_top5"],       collapse = " | "),
    .groups = "drop"
  )

# 2.  Replace commas or line breaks *inside* the lists ---------------------
meas_wide <- meas_wide %>% 
  mutate(across(ends_with("_top5"),
                ~ str_replace_all(.x, ",", " � ")))     # any safe character


write.csv(meas_wide,"Downloads/meas_wide.csv")



energy_security_sub_chart<-energy_security_index %>%
  filter(category %in% c(
    "Foreign Dependency",
    "Energy Imports",
    "Reserves",
    "Trade",
    "critmin_trade",
    "Energy Access",
    "Consumption"
  ))


ei_total <- ei %>%
  filter(Var=="elect_twh",
         Year %in% c("2019","2024")) %>%
  select(Country,Year,Value) %>%
  pivot_wider(names_from=Year,values_from=Value) %>%
  rename(
    "tot_2019"="2019",
    "tot_2024"="2024"
  )

imports_renewable <- production_growth %>%
  filter(tech %in% c("Solar","Wind"),
         supply_chain=="Downstream") %>%
  group_by(Country) %>%
  summarize(change_5yr_abs=sum(change_5yr_abs,na.rm=T),
            X2024=sum(`2024`,na.rm=T),
            X2019=sum(`2019`,na.rm=T)) %>%
  ungroup() %>%
  left_join(ei_total,by=c("Country")) %>%
  mutate(renshare_18=X2019/tot_2019,
         renshare_23=X2024/tot_2024) %>%
  mutate(change_5yr=(renshare_23-renshare_18)) %>%
  left_join(imports_clean %>%
              filter(data_type=="raw",
                     supply_chain=="Upstream") %>%
              group_by(Country) %>%
              summarize(surplus_deficit=sum(value,na.rm=T)),
            by=c("Country")) %>%
  left_join(cat %>%
              select(Country,climate_policy_index) %>%
              mutate(Country=recode(
                Country,
                "UAE" = "United Arab Emirates",
                "USA"="United States",
                "Viet Nam"="Vietnam",
                "T�rkiye"="Turkiye"
              )) %>%
              mutate(EU=ifelse(Country=="EU",1,NA)) %>%
              left_join(ei %>%
                          select(Country,EU) %>%
                          distinct(),
                        by="EU") %>%
              mutate(Country=ifelse(is.na(EU),Country.x,Country.y)),
            by=c("Country")) %>%
  distinct(Country,change_5yr,surplus_deficit,climate_policy_index)

imports_ren_clean <- imports_renewable %>% 
  dplyr::filter(
    !is.na(change_5yr), 
    !is.na(surplus_deficit), 
    !is.na(climate_policy_index)
  )

# 2. Fit the OLS model
model <- lm(
  change_5yr ~ surplus_deficit + climate_policy_index,
  data = imports_ren_clean
)

# 3. Inspect the results
summary(model)

ggplot(imports_renewable)+geom_point(aes(y=change_5yr,x=surplus_deficit))+theme_minimal()




#Energy Aid Since 2015
oecd_sector_plot <- oecd_sector %>% 
  mutate(aid = replace_na(aid, 0)) %>% 
  pivot_wider(names_from = Sector,
              values_from = aid,
              values_fill = 0) %>%   
  # keeps later sums clean
  rowwise() %>%                            # Donor / Recipient become grouping vars
  mutate(energy_total = sum(c_across(everything()),   # all *data* cols
                            na.rm = TRUE)) %>% 
  left_join(oecd_totalenergy %>%
              transmute(Recipient,
                        total_aid=aid),by=c("Recipient")) %>%
  mutate(Other=total_aid-energy_total) %>%
  ungroup() %>%
  arrange(desc(total_aid)) %>%
  slice_max(order_by=energy_total,n=15) 
write.csv(oecd_sector_plot,"Downloads/energy_aid.csv")


#Emissions out to 2050
emissions <- bnef_neo %>%
  filter(Indicator=="Emissions")


#US Energy Exports to Countries by CAT Rating
export_cat <- export_growth_allies %>%
  filter(time=="2025-05") %>%
  left_join(cat %>%
              mutate(
                iso_a2 = countrycode(Country, "country.name", "iso2c")),
            by=c("ISO"="iso_a2")) %>%
  filter(!is.na(Country)) %>%
  group_by(Country,Overall.rating,climate_policy_index) %>%
  summarize_at(vars(export_12mma),sum,na.rm=T) %>%
  ungroup() %>%
  arrange(desc(climate_policy_index)) %>%
  select(Country,Overall.rating, export_12mma) %>%
  pivot_wider(names_from="Overall.rating",values_from="export_12mma")

write.csv(export_cat,"Downloads/export_cat.csv")
