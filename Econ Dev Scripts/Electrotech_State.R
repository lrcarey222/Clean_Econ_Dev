#Electrotech Index


options(tigris_use_cache = TRUE)

#Policy Intent------------------

#Econ Dev Incentives----------------

gjf<- read.csv(paste0(raw_data,"Good Jobs First/gjf_complete.csv"))

gjf_statetotal_1924<-gjf %>%
  filter(Year>2019) %>%
  group_by(Location) %>%
  summarize_at(vars(subs_m),sum,na.rm=T) %>%
  arrange(desc(subs_m)) %>%
  ungroup() %>%
  inner_join(state_gdp, by=c("Location"="GeoName")) %>%
  mutate(incent_gdp=subs_m/X2022*100,
         incent_gdp_rank = rank(-subs_m/X2022))


gjf_electrotech <- gjf %>%
  filter(Major.Industry.of.Parent %in% c(
    "miscellaneous energy products and systems",
    "industrial equipment",
    "electrical and electronic equipment",
    "motor vehicles",
    "information technology",
    "utilities and power generation",
    "automotive parts",
    "telecommunications"
  ),
  Year > 2020,
  !grepl("media",Specific.Industry.of.Parent),
  Specific.Industry.of.Parent != "computers") %>%
  arrange(desc(subs_m))

top_electrotech <- gjf_electrotech %>%
  slice_max(order_by=subs_m, n= 25)
write.csv(top_electrotech,"Downloads/top_electrotech.csv")

electrotech_state<-gjf_electrotech %>%
  group_by(Location,Specific.Industry.of.Parent) %>% 
  summarize(subs_m=sum(subs_m,na.rm=T)) %>%
  ungroup() %>%
  inner_join(state_gdp, by=c("Location"="GeoName")) %>%
  mutate(incent_gdp=subs_m/X2022*100,
         incent_gdp_rank = rank(-subs_m/X2022))

electrotech_state_tot <- gjf_electrotech %>%
  group_by(Location) %>% 
  summarize(subs_m=sum(subs_m,na.rm=T)) %>%
  ungroup() %>%
  inner_join(state_gdp, by=c("Location"="GeoName")) %>%
  mutate(incent_gdp=subs_m/X2022*100,
         incent_gdp_rank = rank(-subs_m/X2022))

electrostate_wide <- electrotech_state %>%
  select(Location,Specific.Industry.of.Parent,subs_m) %>%
  group_by(Location) %>%
  slice_max(order_by=subs_m,n=1) %>%
  arrange(desc(subs_m)) %>%
  filter(Location %in% c("New York","Michigan","Georgia","South Carolina","Ohio","North Carolina","Kansas","Tennessee","Indiana","Texas")) %>%
  mutate(Specific.Industry.of.Parent=str_to_title(Specific.Industry.of.Parent)) %>%
  pivot_wider(names_from=Location,values_from=subs_m) %>%
  write.csv("Downloads/electro_incents.csv")

gjf_electrotech_programs <- gjf_electrotech %>%
  group_by(Location, Program.Name,Awarding.Agency,Type.of.Subsidy) %>% 
  summarize(subs_m=sum(subs_m,na.rm=T)) %>%
  filter(Program.Name != "multiple")

gjf_all_programs <- gjf %>%
  filter(Year>2020) %>%
  group_by(Location, Program.Name,Awarding.Agency,Type.of.Subsidy) %>% 
  summarize(subs_m=sum(subs_m,na.rm=T)) %>%
  arrange(desc(subs_m))
  

#SPOT Index Score
spot<-spot

#DSIRE Count
dsire_count<-dsire_inc %>%
  group_by(state) %>%
  summarise(n = n(), .groups = "drop")

#Climate/Clean ENergy/Manufacturing Incentive Policies------------------------------------------

dev_pol <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/dbo_Program.csv")

keywords<- c("renewable","solar","wind",
             "electric","vehicle", "battery","batteries","storage",
             "EV","transportation","manufacturing","job",
             "jobs","research","R&D","innovation",
             "datacenter","data center","drone","semiconductor",
             "motor")
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

#Energy Legislation
climate_leg<-read.csv("OneDrive - RMI/Documents/Data/Raw Data/climate_leg.csv")

leg_index <- climate_leg %>%
  filter(
    bill_type_1 %in% c(
      "Renewables &amp; Energy Efficiency" ,
      "Green Banks",
      "Utilities and the Grid",
      "Electric Vehicles",
      "100% Clean Energy and Zero Emissions Targets",
      "Emerging Energy Technologies"
    ) |
      bill_type_2 %in% c(
        "Renewables &amp; Energy Efficiency" ,
        "Green Banks",
        "Utilities and the Grid",
        "Electric Vehicles",
        "100% Clean Energy and Zero Emissions Targets",
        "Emerging Energy Technologies"
      ),
    issue_type_1 == "Energy" | issue_type_2 == "Energy"
  ) %>%
  mutate(status_index = case_when(
    status_type == "679" ~ 1,
    status_type == "680" ~ 2,
    status_type == "681" ~ 3,
    status_type == "682" ~ 15,
    status_type == "683" ~ 5,
    status_type == "723" ~ 4,
    TRUE ~ NA_real_   # fallback for anything else
  )) %>%
  group_by(statename) %>%
  summarize(leg_index=sum(status_index,na.rm=T))

#CPCN Requirements-------------
cpcn<-read.csv(paste0(raw_data,"CPCN_Requirements_and_Enactment_Years_by_State_GPT.csv"))

cpcn <- cpcn %>%
  mutate(cpcn=ifelse(CPCN.Required.for.Utility.Scale.Projects=="No",0.75,0.25))


#QuantGov Regulatory Count-----------

regdata<-read.csv(paste0(raw_data,"Regdata_subnational.csv")) %>%
  filter(Country=="United States") %>%
  left_join(pop %>% filter(geo=="State") %>% select(geo_name,pop),by=c("Jurisdiction"="geo_name")) %>%
  group_by(Country) %>%     # or Year.of.Period.Code
  mutate(
    Restrictions_num = parse_number(Restrictions),
    Words_num        = parse_number(Words),
    Restrictions01   = rescale(Restrictions_num, to = c(1, 0)),
    Words01          = rescale(Words_num,        to = c(1, 0)),
    Restrictions02   = rescale(Restrictions_num/pop, to = c(1, 0)),
    Words02          = rescale(Words_num/pop,        to = c(1, 0)),
    RW_avg01         = rowMeans(across(c(Restrictions01, Words01,Restrictions02, Words02)), na.rm = TRUE)
  ) %>%
  ungroup()

#Solar Ordinances--------
solar_ordinance <- read.csv(paste0(raw_data,"Solar Ordinances.csv")) %>%
  filter(State != "") %>%
  group_by(State) %>%
  summarize(count=n()) %>%
  ungroup() %>%
  mutate(ordinance=rescale(count,to=c(1,0))) %>%
  arrange(desc(count))

#State SEPA
sepa<-read.csv(paste0(raw_data,"state_sepa.csv")) %>%
  mutate(has_sepa=rescale(has_sepa,to=c(1,0)))

#Regulatory Friction------
reg_friction <-states_simple %>%
  rename("State"="full") %>%
  left_join(cpcn %>% select(State,cpcn),by=c("State")) %>%
  left_join(regdata %>% select(Jurisdiction,RW_avg01),by=c("State"="Jurisdiction")) %>%
  left_join(solar_ordinance %>% select(State,ordinance),by=c("State")) %>%
  left_join(sepa,by=c("State"="state")) %>%
  mutate(across(
    where(is.numeric), 
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(ease_index = rowMeans(across(where(is.numeric)), na.rm = TRUE)) %>%
  arrange(desc(ease_index))
write.csv(reg_friction,"Downloads/reg_friction.csv")

reg_friction_plot <- reg_friction %>%
  left_join(deployment_index,by=c("abbr"="State"))

ggplot(data=reg_friction_plot,aes(x=ease_index,y=deployment_index))+geom_point()+theme_minimal()

#Policy Intent Index-----------------------

policy_intent <-states_simple %>%
  rename("State"="full") %>%
  left_join(electrotech_state_tot %>%
  select(Location,incent_gdp),by=c("State"="Location")) %>%
  mutate(incent_gdp = coalesce(incent_gdp, 0)) %>%
  left_join(spot %>%
              rename(spot_electricity_score=Policy_Index),by=c("State")) %>%
  left_join(dsire_count %>%
              rename(dsire_policy_count=n),by=c("State"="state")) %>%
  left_join(climate_dev_pol_sum %>%
              rename(economic_development_policy_count=Program_Name),by=c("State")) %>%
  left_join(leg_index %>%
              rename(electrotech_legislation_index=leg_index),by=c("State"="statename")) %>%
  mutate(electrotech_legislation_index = coalesce(electrotech_legislation_index, 0)) %>%
   mutate(across(
    where(is.numeric), 
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(intent_index = rowMeans(across(where(is.numeric)), na.rm = TRUE)) %>%
  arrange(desc(intent_index))


#Economic Capabilities------------


#Employment - Location Quotients, Employment Change------------------------------------------

library(dplyr)
library(purrr)

# Your FIPS map (kept as-is)
state_fips <- c(
  "AL"="01","AK"="02","AZ"="04","AR"="05","CA"="06","CO"="08","CT"="09",
  "DE"="10","FL"="12","GA"="13","HI"="15","ID"="16","IL"="17","IN"="18",
  "IA"="19","KS"="20","KY"="21","LA"="22","ME"="23","MD"="24","MA"="25",
  "MI"="26","MN"="27","MS"="28","MO"="29","MT"="30","NE"="31","NV"="32",
  "NH"="33","NJ"="34","NM"="35","NY"="36","NC"="37","ND"="38","OH"="39",
  "OK"="40","OR"="41","PA"="42","RI"="44","SC"="45","SD"="46","TN"="47",
  "TX"="48","UT"="49","VT"="50","VA"="51","WA"="53","WV"="54","WI"="55",
  "WY"="56"
)

# Years you want (edit as needed)
years <- c(2020:2025)

# Helper: area code for a state's statewide QCEW = <state_fips> + "000" (e.g., "19000" for IA)
state_area_code <- function(fips) paste0(fips, "000")

# Wrapper that returns NULL on error (so we can drop it)
fetch_area_year <- purrr::possibly(
  function(area, y) {
    blsQCEW("Area", year = as.character(y), quarter = "a", area = area)
  },
  otherwise = NULL
)

# Iterate over states × years and bind
all_states_qcew <- imap_dfr( # iterates over named vector (names = state abbr)
  state_fips,
  function(fips, abbr) {
    area <- state_area_code(fips)
    # be a little polite to the API if needed:
    Sys.sleep(0.2)
    
    map_dfr(years, function(y) {
      res <- fetch_area_year(area, y)
      if (is.null(res)) {
        # if a call fails, return an empty tibble
        tibble()
      } else {
        res %>%
          mutate(
            state_abbr = abbr,
            state_fips = fips,
            area_code  = area,
            year       = y
          )
      }
    })
  }
)

# Peek
dplyr::glimpse(all_states_qcew)

electric_man <- c(
  "334210",
  "334220",
  "334290",
  "335912",
  "335110",
  "335121",
  "335122",
  "335129",
  "335311",
  "335312",
  "335313",
  "335314",
  "335921",
  "335929",
  "335931",
  "335932",
  "335991",
  "335999",
  "335911"
)

state_elec_man <- all_states_qcew %>%
  filter(industry_code %in% electric_man) %>%
  select(state_abbr,industry_code,annual_avg_emplvl,avg_annual_pay,lq_annual_avg_emplvl ) %>%
  mutate(naics=as.numeric(industry_code)) %>%
  left_join(naics2022 %>%
              select(`2022 NAICS Code`,
                     `2022 NAICS Title`),by=c("naics"="2022 NAICS Code")) 

base <- all_states_qcew %>%
  filter(own_code == 5, qtr == "A")          # all ownerships, annual rows only

# 2) State totals (all industries = industry_code "10")
state_totals <- base %>%
  filter(industry_code == "10") %>%
  select(state_abbr, year, state_total_empl = annual_avg_emplvl)

# 3) State employment in your composite industry (sum across your NAICS)
state_bundle <- base %>%
  filter(nchar(industry_code) == 6, industry_code %in% electric_man) %>%
  group_by(state_abbr, year) %>%
  summarise(state_bundle_empl = sum(annual_avg_emplvl, na.rm = TRUE), .groups = "drop")

# 4) U.S. totals derived by summing states (close to official; fine unless heavy suppression)
us_bundle <- state_bundle %>%
  group_by(year) %>%
  summarise(us_bundle_empl = sum(state_bundle_empl, na.rm = TRUE), .groups = "drop")

us_total <- state_totals %>%
  group_by(year) %>%
  summarise(us_total_empl = sum(state_total_empl, na.rm = TRUE), .groups = "drop")

# 5) Assemble and compute the composite LQ
bundle_lq <- state_bundle %>%
  left_join(state_totals, by = c("state_abbr","year")) %>%
  left_join(us_bundle,     by = "year") %>%
  left_join(us_total,      by = "year") %>%
  mutate(
    state_share = state_bundle_empl / state_total_empl,
    us_share    = us_bundle_empl    / us_total_empl,
    LQ_bundle   = ifelse(state_total_empl > 0 & us_share > 0, state_share / us_share, NA_real_)
  ) %>%
  select(state_abbr, year, state_bundle_empl, state_total_empl, LQ_bundle) %>%
  arrange(year, state_abbr)

bundle_lq

state_growth <- bundle_lq %>%
  select(state_abbr,year,state_bundle_empl) %>%
  pivot_wider(names_from="year",values_from="state_bundle_empl")%>%
  mutate(growth_2224=`2024`/`2022`-1,
         growth_2024=`2024`/`2020`-1)

#GDP Growth


#Quarterly GDP by Industry----------------------------
url <- "https://apps.bea.gov/regional/zip/SQGDP.zip"
temp_zip <- tempfile(fileext = ".zip")
download(url, temp_zip, mode = "wb")
temp_dir <- tempdir()
unzip(temp_zip, exdir = temp_dir)
files <- list.files(temp_dir, full.names = TRUE)

gdp_ind_q <- read.csv(files[grepl("SQGDP9__ALL_AREAS_2005_2025.csv", files)], stringsAsFactors = FALSE)

gdp_ind <- gdp_ind_q %>%
  mutate(across(matches("^X\\d{4}\\.Q[1-4]$"),
                ~ suppressWarnings(as.numeric(gsub(",", "", .))))) %>%
  filter(IndustryClassification=="321,327-339"|
           Description=="All industry total ") %>%
  mutate(gdp_growth_1yr = X2025.Q1/X2024.Q1-1,
         gdp_growth_5yr = X2025.Q1/X2020.Q1-1,
         gdp_growth_10yr = X2025.Q1/X2015.Q1-1) %>%
  select(GeoName,IndustryClassification,LineCode,X2025.Q1,gdp_growth_1yr,gdp_growth_5yr,gdp_growth_10yr)

gdp_manshare = gdp_ind %>%
  select(GeoName,LineCode,X2025.Q1) %>%
  pivot_wider(names_from="LineCode",values_from="X2025.Q1") %>%
  mutate(man_share=`13`/)

gdp_man_index <- gdp_ind %>%
  filter(IndustryClassification=="321,327-339") %>%
  left_join(gdp_manshare %>%
              select(GeoName,man_share),by=c("GeoName")) %>%
  inner_join(states_simple %>%
               select(full),by=c("GeoName"="full")) %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(gdp_index = rowMeans(across(where(is.numeric)), na.rm = TRUE))


#Feasibility------------------------------
#feas<-read.csv(paste0(acre_data,/Clean-growth-project/raw/ClimateandEconomicJusticeTool/feasibility_geo.csv'))

feas_state<-feas %>%
  filter(geo=="State",
         aggregation_level==4)%>%
  filter(industry_code %in% electric_man) %>%
  group_by(geo_name) %>%
  summarize(across(c(industry_feas_perc), 
                   weighted.mean, 
                   w = .data$pci, 
                   na.rm = TRUE))


#Economic Dynamism--------
url <- "https://eig.org/state-dynamism-2025/assets/Downloadable-Data-EIG-Index-of-State-Dynamism-2022.xlsx"

tf <- tempfile(fileext = ".xlsx")
download.file(url, tf, mode = "wb")  # mode="wb" is important on Windows

# See available sheets
excel_sheets(tf)
# -> pick the sheet you want by name or index

dynamism <- read_excel(tf, sheet = 1)  # or sheet = "Index 2022" (example)
dynamism <- janitor::clean_names(dynamism) %>%
  filter(year=="2022")

metrics <- c("combined_score",
  "core_startup_rate_percent",
  "share_of_workers_at_firms_5_years_old_percent",
  "unique_inventors_per_1_000_residents",
  "housing_permits_per_1_000_residents",
  "reallocation_rate",
  "growth_in_total_firms_percent"
)

dyn0 <- dynamism %>%
  filter(state_abbreviation != "DC") %>%
  select(state_abbreviation, all_of(metrics))

# Build a tidy top-10 per metric, rank within each metric, then pivot wide
top10_league <- dyn0 %>%
  pivot_longer(cols = all_of(metrics), names_to = "metric", values_to = "value") %>%
  group_by(metric) %>%
  arrange(desc(value), .by_group = TRUE) %>%
  mutate(rank = row_number()) %>%
  slice_head(n = 5) %>%
  ungroup() %>%
  mutate(label = str_glue("{state_abbreviation} ({round(value, 2)})")) %>%
  select(metric, rank, label) %>%
  pivot_wider(names_from = metric, values_from = label) %>%
  arrange(rank)

names(top10_league) <- names(top10_league) |>
  str_replace_all("_", " ") |>
  str_to_title()

write.csv(top10_league,"Downloads/dynamism.csv")

#Econ Capabilities Index-----------------------------------
econ_index <- bundle_lq %>%
  select(state_abbr,LQ_bundle) %>%
  rename(electrotech_employment_specialization=LQ_bundle) %>%
  left_join(states_simple %>%
              select(abbr,full),by=c("state_abbr"="abbr")) %>%
  left_join(gdp_man_index %>%
              select(GeoName,gdp_index) %>%
              rename(electrotech_GDP_growth_index=gdp_index),by=c("full"="GeoName")) %>%
  left_join(feas_state %>%
              rename(electrotech_feasibility_index=industry_feas_perc),by=c("full"="geo_name")) %>%
  left_join(dynamism %>%
              select(state_abbreviation,combined_score) %>%
              rename(economic_dynamism=combined_score),by=c("state_abbr"="state_abbreviation")) %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(econ_index = rowMeans(across(where(is.numeric)), na.rm = TRUE))


#Infrastructure & Technical Potential--------------------------
#NREL SUpply Curve (NB calculated in All Geos)--------------
supplycurve_state<- supplycurve_geo %>%
  filter(geo=="State") %>%
  select(1:8)

scale01 <- function(v) {
  v <- replace(v, !is.finite(v), NA_real_)   # turn Inf/-Inf to NA
  rng <- range(v, na.rm = TRUE)
  den <- diff(rng)
  if (!is.finite(den) || den == 0) return(rep(0, length(v)))
  (v - rng[1]) / den
}
# (optional) columns to exclude from the index
exclude <- c("fips","state","year")

num_cols <- names(dplyr::select(supplycurve_state, where(is.numeric))) |> setdiff(exclude)
bad_cols <- num_cols[1:3]                     # first three numeric columns (adjust)
good_cols <- setdiff(num_cols, bad_cols)

renpotential_state <- supplycurve_state %>%
  mutate(
    # good-direction metrics: higher = better ??? scale 0..1
    across(all_of(good_cols),
           ~ rescale(replace(.x, !is.finite(.x), NA_real_), to = c(0, 1), na.rm = TRUE)),
    # bad-direction metrics: higher = worse ??? reverse scale 1..0
    across(all_of(bad_cols),
           ~ rescale(replace(.x, !is.finite(.x), NA_real_), to = c(1, 0), na.rm = TRUE))
  ) %>%
  mutate(
    ren_index = rowMeans(pick(all_of(num_cols)), na.rm = TRUE)
  )


#EV Charging Stations, by State------------
url <-'https://afdc.energy.gov/files/docs/historical-station-counts.xlsx?year=2024'
dest_file <- tempfile(fileext = ".xlsx")
download.file(url, destfile = dest_file, mode = "wb")
data <- read_excel(dest_file,skip=2, sheet=2)
ev_stations_state <- data %>%
  rename("State"="...1",
         "total_chargers"="...10") %>%
  select(State,total_chargers) %>%
  filter(!is.na(State)) %>%
  left_join(socioecon %>% filter(quarter=="2023-Q4") %>% select(StateName,population), by=c("State"="StateName")) %>%
  mutate(ev_stations_cap = total_chargers/population) %>%
  filter(!is.na(ev_stations_cap))


#Interconnection Queue----------------
url <-'https://emp.lbl.gov/sites/default/files/2025-08/LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx'
dest_file <- tempfile(fileext = ".xlsx")
download.file(url, destfile = dest_file, mode = "wb")
data <- read_excel(dest_file,skip=1, sheet=6)

queue <- data %>%
  filter(q_status != "withdrawn") %>%
  group_by(state,q_status) %>%
  summarize(mw=sum(mw1,na.rm=T)) %>%
  pivot_wider(names_from="q_status",values_from="mw") %>%
  mutate(q_share=`active`/`operational`)


#Monthly Industrial Electricity Prices---------------
url <- 'https://www.eia.gov/electricity/data/eia861m/xls/sales_revenue.xlsx'
destination_folder<-'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/States Data/'
file_path <- paste0(destination_folder, "eia_sales.xlsx")
downloaded_content <- GET(url, write_disk(file_path, overwrite = TRUE))

#EIA Sales
eia_sales <- read_excel(file_path, sheet = 1,skip=2)

#State Totals Industrial Prices
ind_price_m <- eia_sales %>%
  mutate(ind_price_m=`Cents/kWh...16`) %>%
  select(State,Year,Month,ind_price_m)

ind_price <- ind_price_m %>%
  group_by(State,Year) %>%
  summarize(ind_price=mean(ind_price_m,na.rm=T)) %>%
  filter(Year %in% c("2014","2019","2024")) %>%
  pivot_wider(names_from=Year,values_from=ind_price) %>%
  mutate(ind_price_10yr=(`2024`/`2014`-1)*100,
         ind_price_5yr=(`2024`/`2019`-1)*100,
         ind_price_cents_kwh=`2024`) %>%
  select(State,ind_price_10yr,ind_price_5yr,ind_price_cents_kwh) %>%
  ungroup() %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(price_index = 1-rowMeans(across(where(is.numeric)), na.rm = TRUE))

ind_price_yr <- ind_price_m %>%
  group_by(State,Year) %>%
  summarize(ind_price=mean(ind_price_m,na.rm=T)) %>%
  filter(State %in% target_states) %>%
  pivot_wider(names_from="State",values_from="ind_price")

write.csv(ind_price_yr,"Downloads/ind_price_yr.csv")

#CNBC Doing Business Infrastructure Rank--------------------------
cnbc <- read.csv("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/cnbc_bus_rankings.csv")
colnames(cnbc)[4]<-"infrastructure"
colnames(cnbc)[2]<-"state"
cnbc<-cnbc %>%
  select(state,infrastructure)

#Infrastructure Index----------------------

# 1) Decide polarity (edit these as you see fit)
positive <- c("renewable_potential", "ev_stations_cap", "electricity_price")   # higher = better
negative <- c("interconnection_queue","cnbc_rank")         # higher = worse

# (Optional) weights; omit if you want a simple average
weights <- c(renewable_potential = 0.2, ev_stations_cap = 0.2, interconnection_queue = 0.2, electricity_price = 0.2,cnbc_rank=0.2)

# 2) Build the table (your joins), then scale by polarity and score
infrastructure_index <- renpotential_state %>%
  select(geo_name, ren_index) %>%
  rename(renewable_potential=ren_index) %>%
  left_join(states_simple %>% select(abbr, full),             by = c("geo_name" = "full")) %>%
  left_join(ev_stations_state %>% select(State, ev_stations_cap), by = c("geo_name" = "State")) %>%
  left_join(queue %>% select(state, q_share) %>%
              rename(interconnection_queue=q_share),                  by = c("abbr" = "state")) %>%
  left_join(ind_price %>% select(State, price_index) %>%
              rename(electricity_price=price_index),          by = c("abbr" = "State")) %>%
  left_join(cnbc,          by = c("geo_name" = "state")) %>%
  # Clean infinities
  mutate(across(all_of(c(positive, negative)),
                ~ replace(.x, !is.finite(.x), NA_real_))) %>%
  # Scale: positives 0???1, negatives 1???0
  mutate(across(all_of(positive), ~ rescale(.x, to = c(0, 1), na.rm = TRUE))) %>%
  mutate(across(all_of(negative), ~ rescale(.x, to = c(1, 0), na.rm = TRUE))) %>%
  # 3a) Unweighted index (simple average)
  mutate(infra_index = rowMeans(pick(all_of(c(positive, negative))), na.rm = TRUE)) %>%
  # 3b) Weighted index (drops if you don't want weights)
  mutate(
    infra_index_w = {
      m <- as.matrix(pick(all_of(names(weights))))
      num <- rowSums(t(t(m) * weights), na.rm = TRUE)
      den <- rowSums(t(t(!is.na(m)) * weights))
      num / den
    }
  )



#Investment & Deployment Outcomes--------------------------
#Electricity Capacity----------------------------
states_gen <- op_gen %>%
  filter(Status=="(OP) Operating") %>%
  group_by(`Plant State`) %>%
  summarize_at(vars(`Nameplate Capacity (MW)`),sum,na.rm=T) %>%
  left_join(census_divisions,by=c("Plant State"="State.Code"))


states_rengen <- op_gen %>%
  group_by(`Plant State`,`Operating Year`,Technology) %>%
  summarize_at(vars(`Nameplate Capacity (MW)`),sum,na.rm=T) %>%
  left_join(census_divisions,by=c("Plant State"="State.Code")) %>%filter(Status=="(OP) Operating") %>%
  filter(Technology %in% c("Conventional Hydroelectric",
                                                     "Onshore Wind Turbine",
                                                     "Batteries",
                                                     "Nuclear",
                                                     "Solar Photovoltaic",
                                                     "Solar Thermal with Energy Storage",
                                                     "Hydroelectric Pumped Storage",
                                                     "Geothermal",
                                                     "Solar Thermal without Energy Storage",
                                                     "Offshore Wind Turbine")) %>%
  
  group_by(Division,State, `Operating Year`) %>%
  summarize(`Nameplate Capacity (MW)` = sum(`Nameplate Capacity (MW)`, na.rm = TRUE)) %>%
  complete(`Operating Year` = 2013:2025, fill = list(`Nameplate Capacity (MW)` = 0)) %>%
  mutate(Year = make_date(`Operating Year`)) %>%
  mutate(cum_cap = cumsum(`Nameplate Capacity (MW)`)) %>%
  group_by(Division,State) %>%
  mutate(cap_index_22 = 100*cum_cap/cum_cap[Year=="2022-01-01"]) %>%
  mutate(rengrowth_22_25 = cum_cap - cum_cap[Year=="2022-01-01"]) 

states_rengen_total <- op_gen %>%
  group_by(`Plant State`,`Operating Year`) %>%
  filter(Status=="(OP) Operating") %>%
  summarize_at(vars(`Nameplate Capacity (MW)`),sum,na.rm=T) %>%
  complete(`Operating Year` = 1900:2025, fill = list(`Nameplate Capacity (MW)` = 0)) %>%
  mutate(Year = make_date(`Operating Year`)) %>%
  mutate(cum_cap = cumsum(`Nameplate Capacity (MW)`)) %>%
  ungroup() %>%
  filter(State %in% top_states$State,
         `Operating Year`>2012) %>%
  arrange(`Operating Year`) %>%

top_states <- states_rengen %>%
  filter(`Operating Year`==2025) %>%
  ungroup() %>%
  slice_max(order_by=rengrowth_22_25,n=10) 

rengen_chart <- states_rengen %>%
  ungroup() %>%
  filter(State %in% top_states$State,
         `Operating Year`>2012) %>%
  arrange(`Operating Year`) %>%
  select(State,cum_cap,`Operating Year`) %>%
  pivot_wider(names_from=State,values_from=cum_cap)
write.csv(rengen_chart,"Downloads/rengen_chart.csv")

states_rengen <- states_rengen %>%
  filter(`Operating Year`==2025) %>%
  ungroup() %>%
  select(State,cum_cap,cap_index_22,rengrowth_22_25) %>%
  left_join(states_gen,by=c("State")) %>%
  mutate(ren_share=cum_cap/`Nameplate Capacity (MW)`*100)

states_rengen_index <- states_rengen %>%
  ungroup() %>%
  select(State,cum_cap,cap_index_22,rengrowth_22_25,ren_share) %>%
    mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(capacity_index = rowMeans(across(where(is.numeric)), na.rm = TRUE))

#Datacenters------------------------
BNEF_DATA_CENTER_LOCATIONS <- read_excel(paste0(raw_data,"BNEF/2025-08-08 - Global Data Center Live IT Capacity Database.xlsx"), sheet = "Data Centers", skip = 7) %>%
  #Filter for rows where "Market" is "US"
  filter(`Market` == "US")


# Keep only rows with coordinates
BNEF_POINTS <- BNEF_DATA_CENTER_LOCATIONS %>%
  filter(!is.na(Latitude), !is.na(Longitude)) %>%
  # keep original lon/lat columns for convenience; add geometry
  st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326, remove = FALSE)

library(tigris)

options(tigris_use_cache = TRUE)

# 1) States polygons (50 + DC; drop territories if you like)
states <- tigris::states(cb = TRUE, year = 2023, class = "sf") %>%
  filter(!STUSPS %in% c("PR","VI","GU","MP","AS")) %>%
  st_transform(4326) %>%                         # match your points CRS
  select(STATEFP, STUSPS, STATE = NAME) # keep only useful cols

# 2) (You already have this) Points in EPSG:4326
# BNEF_POINTS <- BNEF_DATA_CENTER_LOCATIONS %>%
#   filter(!is.na(Latitude), !is.na(Longitude)) %>%
#   st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326, remove = FALSE)

# 3) Spatial point-in-polygon join
BNEF_WITH_STATE <- BNEF_POINTS %>%
  st_join(states, join = st_within, left = TRUE)

datacenters<- BNEF_WITH_STATE %>%
  st_drop_geometry() %>%
  filter(Date=="2025-03-31") %>%
  group_by(STATE) %>%
  summarize(headline_mw=sum(`Headline Capacity (MW)`,na.rm=T),
            construction_mw=sum(`Under Construction Capacity (MW)`,na.rm=T),
            committed_mw=sum(`Committed Capacity (MW)`,na.rm=T)) %>%
  left_join(states_gen,by=c("STATE"="State")) %>%
  mutate(datacenter_share=headline_mw/`Nameplate Capacity (MW)`) %>%
  select(STATE,headline_mw,construction_mw,committed_mw,datacenter_share)

datacenter_index<-datacenters %>%
  ungroup() %>%
  select(STATE,committed_mw,datacenter_share) %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(datacenter_index = rowMeans(across(where(is.numeric)), na.rm = TRUE))
  
  
#Manufacturing Investment----------------------------
investment<- read.csv('OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q2_2025/quarterly_actual_investment.csv',skip=5)
socioeconomics_data_path <- 'OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q2_2025/socioeconomics.csv'
socioecon <- read.csv(socioeconomics_data_path, skip=5)

investment_state <- investment %>%
  filter(Technology %in% c("Batteries","Solar","Nuclear","Zero Emission Vehicles")) %>%
  mutate(
    tech = if_else(Segment == "Manufacturing",
                   paste(Technology, Segment),
                   Technology)) %>%
  group_by(State,tech) %>%
  summarize(inv=sum(Estimated_Actual_Quarterly_Expenditure,na.rm=T )) %>%
  left_join(socioecon %>%
              filter(quarter=="2025-Q2"),by=c("State"="State"))%>%
  mutate(inv_gdp=inv/real_gdp) %>%
  select(State,tech,inv_gdp) %>%
  pivot_wider(names_from="tech",values_from="inv_gdp") %>%
  #mutate(Manufacturing=ifelse(is.na(Manufacturing),0,Manufacturing)) %>%
  ungroup() %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(investment_index = rowMeans(across(where(is.numeric)), na.rm = TRUE)) 


# ---- 7.1 Semiconductor Manufacturing Investment ----------------------------
library(leaflet)
  #MORE HERE: https://www.semiconductors.org/chip-supply-chain-investments/ 
SEMICONDUCTOR_MANUFACTURING_INVESTMENT <- read.csv(paste0(raw_data,"semiconductor_man.csv"))

library(readr)
semi_man <- SEMICONDUCTOR_MANUFACTURING_INVESTMENT %>%
  mutate(project_size_usd = as.numeric(gsub("[\\$,]", "", trimws(`Project.Size....`)))) %>%
  group_by(State) %>%
  summarize(project_size_usd=sum(project_size_usd,na.rm=T)/1000000) %>%
  left_join(socioecon %>%
              filter(quarter=="2025-Q2") %>%
              select(State,real_gdp),by=c("State")) %>%
  mutate(semi_gdp=project_size_usd/real_gdp)

#EV Registrations by State
#Check for latest data here: https://afdc.energy.gov/data/categories/maps-data-categories?sort=most+recent
url <- 'https://afdc.energy.gov/files/u/data/data_source/10962/10962-ev-registration-counts-by-state_9-06-24.xlsx?12518e7893'
dest_file <- tempfile(fileext = ".xlsx")
download.file(url, destfile = dest_file, mode = "wb")
data <- read_excel(dest_file,skip=2)
evs_state <- data %>%
  rename(ev_reg = "Registration Count") %>%
  select(State,ev_reg) %>%
  left_join(pop %>%
              filter(geo=="State") %>%
              select(geo_name,pop),by=c("State"="geo_name")) %>%
  mutate(ev_cap = ev_reg/pop)


#Combined Deployment Index----------------------
deployment_index<-investment_state %>%
  left_join(states_simple %>%
              select(full,abbr),by=c("State"="abbr")) %>%
  select(State,full,investment_index) %>%
  rename(clean_tech_investment=investment_index) %>%
  left_join(datacenter_index %>%
              select(STATE,datacenter_index),by=c("full"="STATE")) %>%
  left_join(states_rengen_index %>%
              select(State,capacity_index) %>%
              rename(electric_capacity_growth=capacity_index),by=c("full"="State")) %>%
  left_join(semi_man %>%
              select(State,semi_gdp) %>%
              rename(semiconductor_investment=semi_gdp),by=c("State")) %>%
  left_join(evs_state %>%
              select(State,ev_cap) %>%
              rename(evs_per_capita=ev_cap),by=c("full"="State")) %>%
  ungroup() %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(deployment_index = rowMeans(across(where(is.numeric)), na.rm = TRUE)) 

write.csv(deployment_index,"Downloads/deployment.csv")


#ElectroTech Index -----------------------
weights <- c(deployment_index = 0.4, infra_index = 0.15, econ_index = 0.15, intent_index = 0.2,cluster_index=0.2,ease_index=0.2)

electrotech <- deployment_index %>%
  select(State,full,deployment_index) %>%
  left_join(infrastructure_index %>%
              select(geo_name,infra_index),by=c("full"="geo_name")) %>%
  left_join(econ_index %>%
              select(state_abbr,econ_index),by=c("State"="state_abbr")) %>%
  left_join(policy_intent %>%
              select(State,intent_index),by=c("full"="State")) %>%
  left_join(reg_friction %>% select(State,ease_index),by=c("full"="State")) %>%
  left_join(cluster_state %>%select(state,cluster_index),by=c("State"="state")) %>%
  mutate(cluster_index = coalesce(cluster_index, 0)) %>%
  ungroup() %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(electrotech_index = rowSums(across(where(is.numeric)), na.rm = TRUE)) %>%
  mutate(
    electrotech_index_w = {
      m <- as.matrix(pick(all_of(names(weights))))
      num <- rowSums(t(t(m) * weights), na.rm = TRUE)
      den <- rowSums(t(t(!is.na(m)) * weights))
      num / den
    }
  ) %>%
  mutate(electrotech_index_w=rescale(electrotech_index_w,to=c(0,1))) %>%
  arrange(desc(electrotech_index_w)) %>%
  mutate(electro_high = dense_rank(desc(electrotech_index_w)) <= 10,
         electro_high_name=ifelse(electrotech_index_w>0.6,State,"")) %>%
  arrange(desc(electrotech_index_w))

write.csv(electrotech %>%
            arrange(desc(electrotech_index)),"Downloads/electrotech.csv")


df <- electrotech %>%
  select(State, full, intent_index, deployment_index, econ_index, infra_index) %>%
  mutate(across(c(intent_index, deployment_index, econ_index, infra_index),
                ~ pmin(pmax(.x, 0), 1))) %>%  # clamp to [0,1] just in case
  drop_na(intent_index, deployment_index)

# 1) Medians (use quantiles instead if you prefer 60/40 splits)
m_intent  <- median(df$intent_index, na.rm = TRUE)
m_deploy  <- median(df$deployment_index, na.rm = TRUE)

# (Optional) quantile thresholds:
# hi_intent <- quantile(df$intent_index, 0.6, na.rm = TRUE)
# hi_deploy <- quantile(df$deployment_index, 0.6, na.rm = TRUE)

# 2) Archetypes by medians
electro_df <- df %>%
  mutate(
    archetype = case_when(
      intent_index  >= m_intent & deployment_index >= m_deploy ~ "Builders (High Intent . High Deployment)",
      intent_index  >= m_intent & deployment_index <  m_deploy ~ "Declarers (High Intent . Low Deployment)",
      intent_index  <  m_intent & deployment_index >= m_deploy ~ "Doers (Low Intent . High Deployment)",
      TRUE ~ "Draggers (Low Intent . Low Deployment)"
    ) %>% factor(levels = c(
      "Builders (High Intent . High Deployment)",
      "Declarers (High Intent . Low Deployment)",
      "Doers (Low Intent . High Deployment)",
      "Draggers (Low Intent . Low Deployment)"
    )),
    # Gap metric for labeling: who is over-/under-performing execution vs. policy?
    rank_intent  = dense_rank(desc(intent_index)),
    rank_deploy  = dense_rank(desc(deployment_index)),
    gap_deploy_vs_intent = rank_deploy - rank_intent,             # >0 = executes better than it declares
    abs_gap = abs(gap_deploy_vs_intent)
  ) %>%
  left_join(states_simple %>%
              mutate(region=str_to_sentence(region)),by=c("State"="abbr"))

write.csv(electro_df,"Downloads/electro_df.csv")

electrotech_div <- electrotech %>%
  left_join(census_divisions,by=c("State"="State.Code")) %>%
  group_by(Division) %>%
  summarize(electrotech_index=mean(electrotech_index,na.rm=T))

elec_price<- electrotech %>%
  left_join(ind_price,by=c("State"="State"))
# select only numeric columns
num_vars <- elec_price %>%
  dplyr::select(where(is.numeric))


# Multi-sheet Excel export of Electrotech indices & components
# ------------------------------------------------------------
# Requires: openxlsx, dplyr, stringr (and your previously built data frames)
# Tabs written:
#  - Policy_Intent
#  - Deployment
#  - Economic_Capabilities
#  - Infrastructure
#  - Electrotech_Combined

library(openxlsx)
library(dplyr)
library(stringr)

# Helper: put likely state-ID columns first, index columns last
reorder_cols <- function(df, index_cols = character(),
                         id_candidates = c("State","state","STATE",
                                           "Location","full","GeoName",
                                           "state_abbr","state_fips",
                                           "abbr","geo_name")) {
  existing_ids   <- intersect(id_candidates, names(df))
  existing_index <- intersect(index_cols, names(df))
  middle         <- setdiff(names(df), c(existing_ids, existing_index))
  df[, c(existing_ids, middle, existing_index), drop = FALSE]
}

# Helper: add a sheet if the object exists
add_index_sheet <- function(wb, obj_name, sheet_name, index_cols) {
  if (!exists(obj_name, inherits = TRUE)) {
    message(sprintf("Skipping %s - object '%s' not found.", sheet_name, obj_name))
    return(invisible(NULL))
  }
  df <- get(obj_name, inherits = TRUE)
  # Ensure it's a data.frame (tibble ok)
  df <- as.data.frame(df)
  
  # Reorder columns (IDs first, then inputs, then index columns)
  df_out <- reorder_cols(df, index_cols = index_cols)
  
  addWorksheet(wb, sheet_name)
  writeData(wb, sheet = sheet_name, x = df_out)
  
  # Freeze header and first row
  freezePane(wb, sheet = sheet_name, firstRow = TRUE)
}

# Create workbook
wb <- createWorkbook()

# 1) Policy Intent tab
#    Common index column name: intent_index
add_index_sheet(wb,
                obj_name  = "policy_intent",
                sheet_name = "Policy_Intent",
                index_cols = c("intent_index"))

# 1) Regulatory Ease tab
#    Common index column name: intent_index
add_index_sheet(wb,
                obj_name  = "reg_friction",
                sheet_name = "Regulatory_Ease",
                index_cols = c("ease_index"))

# 2) Deployment tab
#    Common index column name: deployment_index
add_index_sheet(wb,
                obj_name  = "deployment_index",
                sheet_name = "Deployment",
                index_cols = c("deployment_index"))

# 3) Economic Capabilities tab
#    Common index column name: econ_index
add_index_sheet(wb,
                obj_name  = "econ_index",
                sheet_name = "Economic_Capabilities",
                index_cols = c("econ_index"))

# 4) Infrastructure tab
#    Common index columns: infra_index (and, if present, infra_index_w)
infra_idx_cols <- c("infra_index","infra_index_w")
add_index_sheet(wb,
                obj_name  = "infrastructure_index",
                sheet_name = "Infrastructure",
                index_cols = infra_idx_cols)

# 4) Infrastructure tab
#    Common index columns: infra_index (and, if present, infra_index_w)
add_index_sheet(wb,
                obj_name  = "cluster_state",
                sheet_name = "Top Cluster",
                index_cols = c("cluster_index"))

# 5) Electrotech (combined) tab
#    Common index columns: electrotech_index and electrotech_index_w
add_index_sheet(wb,
                obj_name  = "electrotech",
                sheet_name = "Electrotech_Combined",
                index_cols = c("electrotech_index","electrotech_index_w",
                               # keep the sub-indexes grouped at the end if present
                               "deployment_index","infra_index","econ_index","intent_index","cluster_index"))

# (Optional) a README tab with brief notes pulled from the column names
if (exists("electrotech", inherits = TRUE)) {
  notes <- data.frame(
    Sheet = c("Policy_Intent","Deployment","Economic_Capabilities","Infrastructure","Electrotech_Combined"),
    Index_Column = c("intent_index","deployment_index","econ_index","infra_index (+ infra_index_w)","electrotech_index (+ electrotech_index_w)"),
    Notes = c(
      "Built from incentives-to-GDP, SPOT, active dev-policy counts, and energy/clean-tech legislation progress (all 0-1).",
      "Built from investment/GDP, renewable capacity growth/share, data-center IT MW, semiconductor inv/GDP, EVs per capita (0-1).",
      "Built from composite LQs, manufacturing GDP index, feasibility, and state dynamism (0-1).",
      "Built from renewable potential, EV charging per capita, interconnection queue health (rev.), industrial price index (inv.), and CNBC infra (rev.).",
      "Weighted blend: deployment (0.3), infrastructure (0.2), economic capabilities (0.3), policy intent (0.2)."
    ),
    stringsAsFactors = FALSE
  )
  addWorksheet(wb, "README")
  writeData(wb, "README", notes)
  freezePane(wb, "README", firstRow = TRUE)
}

# Save workbook
out_path <- file.path(getwd(), "Downloads/Electrotech_Index_Tables.xlsx")
saveWorkbook(wb, out_path, overwrite = TRUE)

message(sprintf("Wrote Excel to: %s", out_path))




#ELectrotech v Growth
econ_risk<-read.csv("Downloads/state_business_cycle_status.csv")

electrotech_econ <- electrotech %>%
  select(State,full,electrotech_index,electrotech_index_w) %>%
  left_join(econ_risk,by=c("full"="State"))


ggplot(data=electrotech_econ,aes(x=Risk,y=electrotech_index,size=`Share.of.U.S..GDP...`))+geom_point()+theme_minimal()



#ELectrotech Facilities Chart------------------------------
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

elec_fac<-op_gen %>%
  filter(Technology %in% c("Batteries",
                           "Solar Photovoltaic"),
         `Operating Year`>2021) %>%
  filter(Status=="(OP) Operating") %>%
  mutate(unit="MW",
         cat=ifelse(Technology=="Batteries","Electricity Storage","Solar Generation")) %>%
  select(name=`Entity Name`,size=`Nameplate Capacity (MW)`,cat,tech=Technology,Latitude,Longitude,unit)


facilities_cim_electro <- facilities %>%
  filter(
    Technology %in% c("Batteries","Solar","Zero Emission Vehicles"),
    Investment_Status != "C",
    Segment == "Manufacturing"
  ) %>%
  mutate(
    Announcement_Date = na_if(trimws(Announcement_Date), ""),
    # handle both 4/18/23 and 2023-04-18
    date = parse_date_time(Announcement_Date, orders = c("mdy","ymd"), quiet = TRUE) |> as_date(),
    Technology=ifelse(Technology=="Zero Emission Vehicles","Electric Vehicle",Technology)
  ) %>%
  filter(
    !is.na(date),
    date > as.Date("2022-01-01"),
    Investment_Reported_Flag == TRUE   # <- no quotes
  ) %>%
  mutate(
    tech = if_else(Segment == "Manufacturing",
                   paste(Subcategory, Technology, Segment),
                   Technology),
    unit = "USD",
    cat=paste(Technology,Segment)
  ) %>%
  select(name = Company, tech, cat, size = Estimated_Total_Facility_CAPEX,
         Latitude, Longitude, unit)


datacenter_fac <-BNEF_POINTS %>%
  mutate(tech=paste(`Facility Category`,"Datacenter"),
         cat="Datacenter",
         unit="MW") %>%
  filter(Date=="2025-03-31",
         grepl("2022|2023|2024|2025",`First Live`)) %>%
  st_drop_geometry() %>%
  select(name=Company,tech,cat,size=`Headline Capacity (MW)`,Latitude,Longitude,unit) %>%
  distinct(name,cat,tech,size,Latitude,Longitude,unit)
  
  semi_fac <- SEMICONDUCTOR_MANUFACTURING_INVESTMENT %>%
    mutate(project_size_usd = as.numeric(gsub("[\\$,]", "", trimws(`Project.Size....`)))/1000000,
           tech=paste("Semiconductor",Category),
           cat="Semiconductor Manufacturing",
           unit="USD") %>%
    select(name=Company,cat,tech,size=project_size_usd,Latitude=LAT,Longitude=LON,unit) 
  
  library(tidygeocoder)
  drones_fac<-read.csv("Downloads/us_drone_facility_announcements_2022_2025.csv")
  
  drones_fac <- drones_fac %>%
    mutate(county_state = paste(County, State, sep = ", ")) %>%
    geocode(address = county_state, method = "osm", lat = Latitude, long = Longitude) %>%
    mutate(city_state = paste(City, State, sep = ", ")) %>%
    geocode(address = city_state, method = "osm", lat = Latitude, long = Longitude)
  
  
  drones_fac2 <- drones_fac %>%
    mutate(cat="Drone Manufacturing",
           tech=Facility.Project,
           unit="USD",
           Latitude=ifelse(is.na(`Latitude...13`),`Latitude...16`,`Latitude...13`),
           Longitude=ifelse(is.na(`Longitude...14`),`Longitude...17`,`Longitude...14`)) %>%
    select(name=Company,cat,tech,size=Investment_USD_millions,Longitude,Latitude,unit)

  
electrotech_fac<-rbind(facilities_cim_electro,datacenter_fac) %>%
  rbind(semi_fac) %>%
  rbind(elec_fac) %>%
  rbind(drones_fac2) %>%
  #mutate(size=ifelse(is.na(size),10,size)) %>%
  group_by(cat) %>%
  mutate(size_perc=rescale(size,to=c(0,1))) %>%
  ungroup() %>%
  mutate(size=ifelse(unit=="MW",size*0.66,size))


write.csv(electrotech_fac,"Downloads/electrotech_fac.csv")


#By State
electrotech_fac <- bind_rows(facilities_cim_electro, datacenter_fac, semi_fac, elec_fac) %>%
  filter(!is.na(Longitude), !is.na(Latitude)) %>%
  st_as_sf(coords = c("Longitude","Latitude"), crs = 4326, remove = FALSE)

# 2) Ensure states is sf and in the same CRS
# (If you created it with tigris/usaboundaries it's already sf; just transform)
states <- st_transform(states, 4326)

# 3) Spatial join: attach state attrs to each point
# Use st_intersects (robust for boundary points); st_within also works.
electrotech_fac <- st_join(
  electrotech_fac,
  states %>% select(state_abbr = STUSPS, STATE),  # adjust to your column names
  join = st_intersects,
  left = TRUE
)

# 4) If you want a plain data.frame again:
electrotech_fac_df <- st_drop_geometry(electrotech_fac) %>%
  group_by(state_abbr,STATE,cat,unit) %>%
  summarize(size=sum(size,na.rm=T))

state_electro_wide<-electrotech_fac_df %>%
  filter(state_abbr %in% target_states) %>%
  ungroup() %>%
  select(state_abbr,cat,size) %>%
  pivot_wider(names_from="cat",values_from="size")

write.csv(state_electro_wide,"Downloads/state_electro.csv")  


#Economic Areas Facilities
electrotech_fac <- bind_rows(facilities_cim_electro, datacenter_fac, semi_fac, elec_fac) %>%
  filter(!is.na(Longitude), !is.na(Latitude)) %>%
  st_as_sf(coords = c("Longitude","Latitude"), crs = 4326, remove = FALSE)

# 2) Ensure states is sf and in the same CRS
# (If you created it with tigris/usaboundaries it's already sf; just transform)
pea <- st_read("Downloads/FCC_PEAs_Website/FCC_PEAs_website.shp", quiet = TRUE)

# Fix any geometry issues (common with multipart shapes)
if (any(!st_is_valid(pea))) {
  pea <- st_make_valid(pea)
}

pea <- st_transform(pea, 4326)

# 3) Spatial join: attach state attrs to each point
# Use st_intersects (robust for boundary points); st_within also works.
electrotech_fac <- st_join(
  electrotech_fac,
  pea %>% select(PEA_Name),  # adjust to your column names
  join = st_intersects,
  left = TRUE
)

# 4) If you want a plain data.frame again:
electrotech_fac_ea <- st_drop_geometry(electrotech_fac) %>%
  group_by(PEA_Name,cat,tech,unit) %>%
  summarize(size=sum(size,na.rm=T)) %>%
  left_join(cluster_index,by=c("PEA_Name"="economic_area"))
write.csv(electrotech_fac_ea,"Downloads/ea_electro.csv")  

state_electro_wide<-electrotech_fac_df %>%
  filter(state_abbr %in% target_states) %>%
  ungroup() %>%
  select(state_abbr,cat,size) %>%
  pivot_wider(names_from="cat",values_from="size")

write.csv(state_electro_wide,"Downloads/state_electro.csv")  



#County Employment
# Packages
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(tibble)
library(tidycensus)   # for fips_codes table (no API key needed to access this data frame)
library(blsQCEW)      # provides blsQCEW()

## -----------------------------
## 0) Define your 6-digit set and collapse to 4-digit
## -----------------------------
electric_man_6d <- c(
  "513322","513340","513390","515210",
  "517210","517211","517212","517410","517910","517919",
  "334210","334220","334290","335912",
  "221112","221111","221113","221114","221115","221116","221117","221118","221119","221121",
  "335110","335121","335122","335129",
  "335311","335312","335313","335314",
  "335921","335929","335931","335932",
  "335991","335999","335911"
)

electric_man_4d <- unique(str_sub(electric_man_6d, 1, 4))
# Expect: c("3342", "3351", "3353", "3359")

## -----------------------------
## 1) County area codes (stateFIPS + countyFIPS)
## -----------------------------
counties <- tidycensus::fips_codes %>%
  transmute(
    state_abbr = state,
    state_name,
    state_fips = state_code,         # 2-digit string
    county_name = county,
    county_fips = county_code,       # 3-digit string
    area_code  = paste0(state_code, county_code)
  ) %>%
  distinct(area_code, .keep_all = TRUE)

## -----------------------------
## 2) Find the latest available (year, quarter)
##    Try in descending order (edit the range if you want)
## -----------------------------
years_try <- 2025:2020
quarters_try <- c("4","3","2","1")

detect_latest_yq <- function(sample_area = "01001", years = years_try, quarters = quarters_try) {
  candidates <- expand_grid(year = years, quarter = quarters) %>%
    arrange(desc(year), desc(quarter))
  for (i in seq_len(nrow(candidates))) {
    y <- as.character(candidates$year[i])
    q <- candidates$quarter[i]
    res <- try(blsQCEW("Area", year = y, quarter = q, area = sample_area), silent = TRUE)
    if (!inherits(res, "try-error") && is.data.frame(res) && nrow(res) > 0) {
      return(list(year = y, quarter = q))
    }
  }
  stop("Could not detect a valid (year, quarter) from the QCEW API. Try expanding years_try.")
}

latest <- detect_latest_yq(sample_area = "01001")  # Autauga County, AL as a probe
latest
# $year
# [1] "2025"
# $quarter
# [1] "2"  # (example; will vary by what's available)

## -----------------------------
## 3) Helper to fetch one county's data (latest Y/Q), safely
## -----------------------------
fetch_county_qtr <- purrr::possibly(
  function(area_code, y, q) {
    blsQCEW("Area", year = y, quarter = q, area = area_code)
  },
  otherwise = NULL
)

## -----------------------------
## 4) Pull all counties for that latest quarter
##    (This is a lot of counties; you can parallelize with {furrr} if desired.)
## -----------------------------
all_cty_qtr <- map_dfr(
  counties$area_code,
  function(ac) {
    Sys.sleep(0.03)  # be polite; adjust if needed
    df <- fetch_county_qtr(ac, latest$year, latest$quarter)
    if (is.null(df) || nrow(df) == 0) return(tibble())
    df %>% mutate(area_code = ac)
  }
)

## -----------------------------
## 5) Keep only 4-digit NAICS in your electric_man set
##    and compute the *latest month* employment
## -----------------------------
county_elec_4d <- all_cty_qtr %>%
  # QCEW returns a mix of totals and different NAICS levels.
  # Keep only 4-digit NAICS rows and match to your set:
  filter(own_code==5,nchar(industry_code) == 4, industry_code %in% electric_man_4d) %>%
  # latest month within the quarter (prefers month3, then month2, then month1)
  mutate(
    latest_month_emplvl = coalesce(month3_emplvl, month2_emplvl, month1_emplvl),
    latest_month = case_when(
      !is.na(month3_emplvl) ~ 3L,
      !is.na(month2_emplvl) ~ 2L,
      !is.na(month1_emplvl) ~ 1L,
      TRUE ~ NA_integer_
    )
  ) %>%
  # Bring back readable county/state columns
  left_join(counties, by = "area_code") %>%
  left_join(NAICS_2017,by=c("industry_code"="2017.NAICS.US.Code")) %>%
  select(
    state_abbr, state_name, state_fips, county_name, county_fips, area_code,
    year, qtr, industry_code, NAICS_name,lq_avg_wkly_wage,lq_month3_emplvl,
    month1_emplvl, month2_emplvl, month3_emplvl,
    latest_month, latest_month_emplvl
  ) %>%
  arrange(state_fips, county_fips, industry_code) %>%
  mutate(fips=as.numeric(area_code)) %>%
  left_join(pea,by=c("fips")) 

dplyr::glimpse(county_elec_4d)

EA_elecman<-county_elec_4d %>%
  mutate(fips=as.numeric(area_code)) %>%
  #left_join(pea,by=c("fips")) %>%
  group_by(FCC_PEA_Name) %>%
  summarize(emp=sum(latest_month_emplvl,na.rm=T))

EA_all<-all_cty_qtr %>%
  # QCEW returns a mix of totals and different NAICS levels.
  # Keep only 4-digit NAICS rows and match to your set:
  filter(own_code==5,industry_code=="10") %>%
  # latest month within the quarter (prefers month3, then month2, then month1)
  mutate(
    latest_month_emplvl = coalesce(month3_emplvl, month2_emplvl, month1_emplvl),
    latest_month = case_when(
      !is.na(month3_emplvl) ~ 3L,
      !is.na(month2_emplvl) ~ 2L,
      !is.na(month1_emplvl) ~ 1L,
      TRUE ~ NA_integer_
    )
  ) %>%
  # Bring back readable county/state columns
  left_join(counties, by = "area_code") %>%
  mutate(fips=as.numeric(area_code)) %>%
  left_join(pea,by=c("fips")) %>%
  group_by(FCC_PEA_Name) %>%
  summarize(emp=sum(latest_month_emplvl,na.rm=T))

EA_elecman<-EA_elecman %>%
  left_join(EA_all,by=c("FCC_PEA_Name")) %>%
  mutate(elecman_share=emp.x/emp.y*100)


#2022
all_cty_qtr_22 <- map_dfr(
  counties$area_code,
  function(ac) {
    Sys.sleep(0.03)  # be polite; adjust if needed
    df <- fetch_county_qtr(ac, "2022", "1")
    if (is.null(df) || nrow(df) == 0) return(tibble())
    df %>% mutate(area_code = ac)
  }
)

county_elec_4d22 <- all_cty_qtr_22 %>%
  # QCEW returns a mix of totals and different NAICS levels.
  # Keep only 4-digit NAICS rows and match to your set:
  filter(own_code==5,nchar(industry_code) == 4, industry_code %in% electric_man_4d) %>%
  # latest month within the quarter (prefers month3, then month2, then month1)
  mutate(
    latest_month_emplvl = coalesce(month3_emplvl, month2_emplvl, month1_emplvl),
    latest_month = case_when(
      !is.na(month3_emplvl) ~ 3L,
      !is.na(month2_emplvl) ~ 2L,
      !is.na(month1_emplvl) ~ 1L,
      TRUE ~ NA_integer_
    )
  ) %>%
  # Bring back readable county/state columns
  left_join(counties, by = "area_code") %>%
  select(
    state_abbr, state_name, state_fips, county_name, county_fips, area_code,
    year, qtr, industry_code, 
    month1_emplvl, month2_emplvl, month3_emplvl,
    latest_month, latest_month_emplvl
  ) %>%
  arrange(state_fips, county_fips, industry_code)

dplyr::glimpse(county_elec_4d)

EA_elecman22<-county_elec_4d22 %>%
  mutate(fips=as.numeric(area_code)) %>%
  left_join(pea,by=c("fips")) %>%
  group_by(FCC_PEA_Name) %>%
  summarize(emp_22=sum(latest_month_emplvl,na.rm=T))

EA_elecman<-EA_elecman %>%
  left_join(EA_elecman22,by=c("FCC_PEA_Name")) %>%
  mutate(growth=emp.x/emp_22-1,
         growth_pop=(emp.x-emp_22)/emp.y)
write.csv(EA_elecman,"Downloads/EA_elec.csv")


EA_elecman_top <- EA_elecman %>%
  arrange(desc(elecman_share)) %>%
  slice_max(elecman_share,n=25) %>%
  arrange(desc(growth))


#Clusters-------------------------

#Employment Growth
EA_elecman

#Feasibility
feas_EA<-feas %>%
  filter(geo=="Economic Area",
         aggregation_level==4)%>%
  filter(industry_code %in% electric_man_6d) %>%
  group_by(geo_name) %>%
  summarize(across(c(industry_feas_perc), 
                   weighted.mean, 
                   w = .data$pci, 
                   na.rm = TRUE))

#Electricity Capacity

op_gen_clean <- op_gen %>% 
  filter(!is.na(Latitude) & !is.na(Longitude))
op_gen_sf <- st_as_sf(op_gen_clean, coords = c("Longitude", "Latitude"), crs = 4326)
counties_sf <- st_as_sf(us_counties, coords = c("Longitude", "Latitude"), crs = 4326)
op_gen_sf <- st_transform(op_gen_sf, crs = st_crs(us_counties))

op_gen_with_county <- st_join(op_gen_sf, counties_sf)

op_gen_geo <- op_gen_with_county %>%
  st_drop_geometry() %>%
  filter(Status=="(OP) Operating") %>%
  mutate(fips = as.numeric(fips),
         unique_id=paste0(as.character(`Entity ID`),as.character(`Generator ID`))) %>%
  select(unique_id,`Plant State`, Technology, `Operating Year`, `Nameplate Capacity (MW)`, fips) %>%
  left_join(
    op_gen_cd %>%
      select(`Plant State`,cd_119, Technology, `Operating Year`, `Nameplate Capacity (MW)`),
    by = c("Plant State", "Technology", "Operating Year", "Nameplate Capacity (MW)")
  ) %>%
  left_join(
    geo %>%
      select(-cd_119,-percent_district) %>%
      distinct(),
    by = "fips"
  )  %>%
  distinct()


EA_rengen <- op_gen_geo %>%
  group_by(PEA,`Operating Year`,Technology) %>%
  summarize_at(vars(`Nameplate Capacity (MW)`),sum,na.rm=T) %>%
  filter(Technology %in% c("Conventional Hydroelectric",
                           "Onshore Wind Turbine",
                           "Batteries",
                           "Nuclear",
                           "Solar Photovoltaic",
                           "Solar Thermal with Energy Storage",
                           "Hydroelectric Pumped Storage",
                           "Geothermal",
                           "Solar Thermal without Energy Storage",
                           "Offshore Wind Turbine")) %>%
  
  group_by(PEA, `Operating Year`) %>%
  summarize(`Nameplate Capacity (MW)` = sum(`Nameplate Capacity (MW)`, na.rm = TRUE)) %>%
  complete(`Operating Year` = 2013:2025, fill = list(`Nameplate Capacity (MW)` = 0)) %>%
  mutate(Year = make_date(`Operating Year`)) %>%
  mutate(cum_cap = cumsum(`Nameplate Capacity (MW)`)) %>%
  group_by(PEA) %>%
  mutate(cap_index_22 = 100*cum_cap/cum_cap[Year=="2022-01-01"]) %>%
  mutate(rengrowth_22_25 = cum_cap - cum_cap[Year=="2022-01-01"]) %>%
  filter(`Operating Year`==2025) %>%
  ungroup() %>%
  select(PEA,cum_cap,cap_index_22,rengrowth_22_25) 

#Datacenters
BNEF_POINTS <- BNEF_DATA_CENTER_LOCATIONS %>%
  filter(!is.na(Latitude), !is.na(Longitude)) %>%
  # keep original lon/lat columns for convenience; add geometry
  st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326, remove = FALSE)

counties <- tigris::counties(cb = TRUE, year = 2023, class = "sf") %>%
  st_transform(4326) 

BNEF_WITH_COUNTY <- BNEF_POINTS %>%
  st_join(counties, join = st_within, left = TRUE)

datacenter_ea <- BNEF_WITH_COUNTY %>%
  mutate(fips=as.numeric(GEOID)) %>%
  left_join(pea,by=c("fips")) %>%
  st_drop_geometry() %>%
  filter(Date=="2025-03-31") %>%
  group_by(FCC_PEA_Name) %>%
  summarize(datacenter_mw=sum(`Headline Capacity (MW)`,na.rm=T),
            datacenter_count=n(), .groups = "drop") %>%
  distinct()

#Electricity Prices
utility<-read_excel(paste0(raw_data,"egrid2023_data_metric_rev2.xlsx"),sheet=4,skip=1)

utility<-utility %>% 
  distinct(OPRNAME,BANAME,FIPSST,FIPSCNTY) %>% 
  mutate(fips=as.numeric(paste0(FIPSST,FIPSCNTY))) %>%
  left_join(geo,by=c("fips")) 

utility_ind_price<-read_excel(paste0(raw_data,"table_8.xlsx"),skip=2)

utility_ind_price2 <- utility_ind_price %>%
  fuzzy_left_join(utility,by=c("Entity"="OPRNAME"),
                  match_fun = ~ stringdist::stringdist(.x, .y, method = "jw") < 0.1)

utility_ind_price2 <- utility_ind_price2 %>%
  mutate(`Average Price (cents/kWh)`=as.numeric(`Average Price (cents/kWh)`)) %>%
  filter(!is.na(`Customers (Count)`),
         !is.na(`Average Price (cents/kWh)`)) %>%
  distinct(PEA,`Average Price (cents/kWh)`,`Sales (Megawatthours)`) %>%
  group_by(PEA) %>%
  summarize(price=weighted.mean(`Average Price (cents/kWh)`,w=`Sales (Megawatthours)`,na.rm=T, groups = "drop")
  )

#Clean Investment Monitor
facilities_cim_electro_EA <- facilities %>%
  filter(
    Technology %in% c("Batteries","Solar","Zero Emission Vehicles"),
    Investment_Status != "C",
    Segment == "Manufacturing"
  ) %>%
  mutate(
    Announcement_Date = na_if(trimws(Announcement_Date), ""),
    # handle both 4/18/23 and 2023-04-18
    date = parse_date_time(Announcement_Date, orders = c("mdy","ymd"), quiet = TRUE) |> as_date(),
    Technology=ifelse(Technology=="Zero Emission Vehicles","Electric Vehicle",Technology)
  ) %>%
  filter(
    !is.na(date),
    date > as.Date("2022-01-01"),
    Investment_Reported_Flag == TRUE   # <- no quotes
  ) %>%
  left_join(pea,by=c("county_2020_geoid"="fips")) %>%
  group_by(FCC_PEA_Name,Technology) %>%
  summarize(inv=sum(Estimated_Total_Facility_CAPEX,na.rm=T)) %>%
  pivot_wider(names_from="Technology",values_from="inv",values_fill = 0)

#Semiconductors
semi_clean <- SEMICONDUCTOR_MANUFACTURING_INVESTMENT %>% 
  filter(!is.na(LAT) & !is.na(LON))
semi_sf <- st_as_sf(semi_clean, coords = c("LON", "LAT"), crs = 4326)
counties_sf <- st_as_sf(us_counties, coords = c("Longitude", "Latitude"), crs = 4326)
semi_sf <- st_transform(semi_sf, crs = st_crs(us_counties))

semi_with_county <- st_join(semi_sf, counties_sf)

semi_man <- semi_with_county %>%
  mutate(project_size_usd = as.numeric(gsub("[\\$,]", "", trimws(`Project.Size....`))),
         fips=as.numeric(fips)) %>%
  left_join(pea,by=c("fips")) %>%
  st_drop_geometry() %>%
  group_by(FCC_PEA_Name) %>%
  summarize(project_size_usd=sum(project_size_usd,na.rm=T)/1000000) 


#Cluster Index 
cluster<-EA_elecman %>%
  left_join(feas_EA,by=c("FCC_PEA_Name"="geo_name")) %>%
  left_join(EA_rengen,by=c("FCC_PEA_Name"="PEA")) %>%
  left_join(datacenter_ea,by=c("FCC_PEA_Name")) %>%
  left_join(utility_ind_price2,by=c("FCC_PEA_Name"="PEA")) %>%
  left_join(semi_man,by=c("FCC_PEA_Name")) %>%
  left_join(facilities_cim_electro_EA,by=c("FCC_PEA_Name")) %>%
  rename(economic_area=FCC_PEA_Name,
         electrostack_manufacturing_workforce_share=elecman_share,
         workforce_growth_2225=growth_pop,
         industry_feasibility=industry_feas_perc,
         clean_electric_capacity=cum_cap,
         clean_electric_capacity_growth=rengrowth_22_25,
         industrial_electricity_price=price,
         semiconductor_manufacturing=project_size_usd,
         battery_manufacturing=Batteries,
         solar_manufacturing=Solar,
         EV_manufacturing=`Electric Vehicle`) %>%
  select(economic_area,
         electrostack_manufacturing_workforce_share,
         workforce_growth_2225,
         industry_feasibility,
         clean_electric_capacity,
         clean_electric_capacity_growth,
         industrial_electricity_price,
         datacenter_mw,
         datacenter_count,
         semiconductor_manufacturing,
         battery_manufacturing,
         solar_manufacturing,
         EV_manufacturing)
write.csv(cluster,"Downloads/cluster.csv")



# 1) Identify the five "anchor" vars and the rest
anchor_vars <- c("datacenter_mw",
                 "semiconductor_manufacturing",
                 "battery_manufacturing",
                 "solar_manufacturing",
                 "EV_manufacturing")

positive <- c("electrostack_manufacturing_workforce_share", "workforce_growth_2225","industry_feasibility",
              "clean_electric_capacity_growth", anchor_vars)   # higher = better
negative <- c("industrial_electricity_price")                 # higher = worse

# All non-anchor inputs to average directly
base_cols <- c(setdiff(positive, anchor_vars), negative)
base_n    <- length(base_cols)  # for the mean denominator

cluster_index <- cluster %>%
  # Clean infinities and NAs before scaling
  mutate(across(all_of(c(positive, negative)), ~ replace(.x, !is.finite(.x), NA_real_))) %>%
  mutate(across(all_of(c(positive, negative)), ~ tidyr::replace_na(.x, 0))) %>%
  # Scale: positives 0???1, negatives 1???0
  mutate(across(all_of(positive), ~ rescale(.x, to = c(0, 1), na.rm = TRUE))) %>%
  mutate(across(all_of(negative), ~ rescale(.x, to = c(1, 0), na.rm = TRUE))) %>%
  # Row-wise: take the max across the five anchors
  rowwise() %>%
  mutate(max_anchor = max(c_across(all_of(anchor_vars)), na.rm = TRUE)) %>%
  ungroup() %>%
  # Mean = (sum of all base cols + max(anchor set)) / (num base cols + 1)
  mutate(
    base_sum      = rowSums(across(all_of(base_cols)), na.rm = TRUE),
    cluster_index = (base_sum + max_anchor) / (base_n + 1),
    cluster_index = rescale(cluster_index, to = c(0, 1), na.rm = TRUE)
  ) %>%
  arrange(desc(cluster_index)) %>%
  mutate(cluster_top = ifelse(cluster_index > 0.5, economic_area, "")) %>%
  select(-base_sum) %>%  # cleanup 
  arrange(desc(cluster_index)) %>%
  mutate(cluster_top=ifelse(cluster_index>0.5,economic_area,""))

write.csv(cluster_index,"Downloads/cluster_index.csv")

cluster_state <- cluster_index %>%
  mutate(state = str_sub(as.character(economic_area), -2)) %>%
  group_by(state) %>%
  slice_max(order_by=cluster_index,n=1) %>%
  select(state,cluster_index,economic_area,positive,negative) %>%
  arrange(desc(cluster_index))





#Workforce-----------------------------
# OEWS programmatic access via official ZIP "text files" (not the BLS API)
# Libraries
required <- c("httr", "readr", "readxl", "janitor", "dplyr", "stringr", "tidyr", "purrr")
invisible(lapply(setdiff(required, rownames(installed.packages())), install.packages))
lapply(required, library, character.only = TRUE)

# =========================
# 1) Setup & helpers
# =========================
library(dplyr); library(readr); library(readxl); library(purrr); library(janitor); library(stringr); library(tidyr)

dl_dir <- "C:/Users/LCarey/Downloads"
st_zip <- file.path(dl_dir, "oesm24st.zip")   # May-2024 STATE OEWS

stopifnot(file.exists(st_zip))

# --- Helpers: robust to missing columns --------------------------------------
to_numeric <- function(x) {
  if (is.numeric(x)) return(x)
  x <- gsub(",", "", x)
  x[trimws(x) %in% c("*","**","#","-","")] <- NA
  suppressWarnings(as.numeric(x))
}

ensure_cols <- function(df, cols) {
  for (cc in cols) if (!cc %in% names(df)) df[[cc]] <- NA_real_
  df
}

normalize_oews_cols <- function(df) {
  df <- janitor::clean_names(df)
  ren <- c(
    "area"="area","area_title"="area_title",
    "occ_code"="occ_code","occupation_code"="occ_code",
    "occ_title"="occ_title","occupation_title"="occ_title",
    "tot_emp"="tot_emp","total_employment"="tot_emp","emp_prse"="emp_prse",
    "h_mean"="h_mean","a_mean"="a_mean","h_median"="h_median","a_median"="a_median",
    "h_p10"="h_p10","h_p25"="h_p25","h_p75"="h_p75","h_p90"="h_p90",
    "a_p10"="a_p10","a_p25"="a_p25","a_p75"="a_p75","a_p90"="a_p90"
  )
  i <- intersect(names(df), names(ren))
  names(df)[match(i, names(df))] <- unname(ren[i])
  df %>%
    dplyr::select(dplyr::any_of(c(
      "area","area_title","occ_code","occ_title","tot_emp","emp_prse",
      "h_mean","a_mean","h_median","a_median",
      "h_p10","h_p25","h_p75","h_p90","a_p10","a_p25","a_p75","a_p90"
    )))
}

read_oews_zip_full <- function(zip_path) {
  files <- unzip(zip_path, list = TRUE)$Name
  data_files <- files[!grepl("readme|doc|note|\\.pdf$|\\.txt$", files, ignore.case = TRUE)]
  pick <- c(grep("\\.csv$",  data_files, value=TRUE, ignore.case=TRUE),
            grep("\\.xlsx$", data_files, value=TRUE, ignore.case=TRUE),
            grep("\\.xls$",  data_files, value=TRUE, ignore.case=TRUE))
  stopifnot(length(pick) > 0)
  
  dfs <- purrr::map(pick, function(f){
    ext <- tools::file_ext(f); tmpdir <- tempdir()
    unzip(zip_path, files=f, exdir=tmpdir, overwrite=TRUE)
    path <- file.path(tmpdir, f)
    switch(tolower(ext),
           "csv"  = suppressMessages(readr::read_csv(path, show_col_types = FALSE)),
           "xlsx" = readxl::read_excel(path),
           "xls"  = readxl::read_excel(path)
    )
  })
  
  out <- dplyr::bind_rows(dfs) %>%
    normalize_oews_cols()
  
  # Guarantee all numeric targets exist, then convert
  numeric_targets <- c("tot_emp","h_mean","a_mean","h_median","a_median",
                       "h_p10","h_p25","h_p75","h_p90","a_p10","a_p25","a_p75","a_p90")
  out %>%
    ensure_cols(numeric_targets) %>%
    dplyr::mutate(
      dplyr::across(dplyr::all_of(numeric_targets), to_numeric),
      occ_code = as.character(occ_code),
      area_title = as.character(area_title),
      area = as.character(area)
    )
}

# =========================
# 2) Load OEWS (with totals)
# =========================
oews_st_full <- read_oews_zip_full(st_zip)

# Pull statewide totals row (00-0000) for each state
state_totals <- oews_st_full %>%
  filter(occ_code == "00-0000") %>%
  transmute(State = area_title,
            total_emp_state = tot_emp,
            state_h_med = h_median)

# =========================
# 3) Define job families (SOC lists)
# =========================
SOCs_MAKE   <- c("49-9041","49-9071","51-4041","17-2112","17-2071","17-3024","17-3026","17-3029")
SOCs_BUILD  <- c("47-2111","49-9051","47-2231","47-2061","47-2152","47-2073")
SOCs_RUN    <- c("49-2095","49-9081","17-2072","49-1011","17-3023")
SOCs_ENABLE <- c("17-2112","13-1081","13-1151","13-1071","19-5011","17-2141")

families <- tibble::tibble(
  Family = c("MAKE","BUILD","RUN","ENABLE"),
  SOCs   = list(SOCs_MAKE, SOCs_BUILD, SOCs_RUN, SOCs_ENABLE)
)

# =========================
# 4) Summarize by family & state
# =========================
family_summaries <- families %>%
  mutate(data = purrr::map(SOCs, ~ oews_st_full %>% filter(occ_code %in% .x))) %>%
  select(Family, data) %>%
  tidyr::unnest(data) %>%
  group_by(Family, State = area_title) %>%
  summarize(
    family_emp          = sum(tot_emp, na.rm = TRUE),
    family_h_med_weight = wtd_mean(h_median, tot_emp),  # employment-weighted avg of occupation medians
    .groups = "drop"
  ) %>%
  left_join(state_totals, by = "State") %>%
  mutate(
    density_per_10k   = ifelse(!is.na(total_emp_state) & total_emp_state > 0,
                               1e4 * family_emp / total_emp_state, NA_real_),
    wage_premium_vs_state = ifelse(!is.na(family_h_med_weight) & !is.na(state_h_med) & state_h_med > 0,
                                   (family_h_med_weight / state_h_med) - 1, NA_real_)
  ) %>%
  select(State, Family,
         Family_Employment = family_emp,
         Density_per_10k = density_per_10k,
         Family_Hourly_Median_weighted = family_h_med_weight,
         Statewide_Hourly_Median = state_h_med,
         Wage_Premium_vs_State = wage_premium_vs_state) %>%
  arrange(State, Family)

# =========================
# 5) Nice formatting + optional export
# =========================
# Display a preview
family_summaries %>% 
  mutate(
    Density_per_10k = round(Density_per_10k, 2),
    Family_Hourly_Median_weighted = round(Family_Hourly_Median_weighted, 2),
    Statewide_Hourly_Median = round(Statewide_Hourly_Median, 2),
    Wage_Premium_vs_State = scales::percent(Wage_Premium_vs_State, accuracy = 0.1)
  ) %>%
  arrange(desc(Density_per_10k)) %>%
  head(20)

library(dplyr)
library(tidyr)
library(openxlsx)

# Keep only what we need, then pivot wide
wide_tbl <- family_summaries %>%
  select(State,
         Family,
         Density_per_10k,
         Wage_Premium_vs_State) %>%
  mutate(Family = toupper(Family)) %>%
  pivot_wider(
    names_from = Family,
    values_from = c(Density_per_10k, Wage_Premium_vs_State),
    names_glue = "{Family}_{.value}"
  ) %>%
  # Order columns to match your mockup
  select(
    State,
    MAKE_Density_per_10k, MAKE_Wage_Premium_vs_State,
    BUILD_Density_per_10k, BUILD_Wage_Premium_vs_State,
    RUN_Density_per_10k,   RUN_Wage_Premium_vs_State,
    ENABLE_Density_per_10k, ENABLE_Wage_Premium_vs_State
  ) %>%
  arrange(State)

# Optional: round density now; keep premium as decimal for Excel % format
wide_tbl <- wide_tbl %>%
  mutate(across(ends_with("Density_per_10k"), ~round(., 2)))

target_states<- as.data.frame(target_states) %>%
  left_join(states_simple,by=c("target_states"="abbr")) %>%
  select(target_states,full)

wide_target <- wide_tbl %>%
  filter(State %in% target_states$full)

library(dplyr)
library(tidyr)
library(openxlsx)

library(dplyr)
library(tidyr)

# 1) Statewide totals (denominator for state shares)
state_totals <- oews_st_full %>%
  filter(occ_code == "00-0000") %>%
  transmute(State = area_title, total_emp_state = tot_emp)

# 2) U.S. total employment (sum of states) for the same file
us_total_emp <- sum(state_totals$total_emp_state, na.rm = TRUE)

# 3) U.S. family employment (sum across all states by family) - CORRECTLY grouped
families <- tibble::tibble(
  Family = c("MAKE","BUILD","RUN","ENABLE"),
  SOCs   = list(SOCs_MAKE, SOCs_BUILD, SOCs_RUN, SOCs_ENABLE)
)

us_family <- families %>%
  mutate(data = purrr::map(SOCs, ~ oews_st_full %>% filter(occ_code %in% .x))) %>%
  select(Family, data) %>%
  unnest(data) %>%
  group_by(Family) %>%
  summarize(family_emp_us = sum(tot_emp, na.rm = TRUE), .groups = "drop") %>%
  mutate(us_share = family_emp_us / us_total_emp)

# 4) Rebuild (or reuse) your earlier 'family_summaries' with Family_Employment
#    If you already have 'family_summaries' from earlier code, skip to step 5.
family_summaries <- families %>%
  mutate(data = purrr::map(SOCs, ~ oews_st_full %>% filter(occ_code %in% .x))) %>%
  select(Family, data) %>%
  unnest(data) %>%
  group_by(Family, State = area_title) %>%
  summarize(
    Family_Employment = sum(tot_emp, na.rm = TRUE),
    Family_Hourly_Median_weighted = {
      ok <- !is.na(h_median) & !is.na(tot_emp)
      if (any(ok)) sum(h_median[ok] * tot_emp[ok]) / sum(tot_emp[ok]) else NA_real_
    },
    .groups = "drop"
  ) %>%
  left_join(state_totals, by = "State") %>%
  left_join(
    oews_st_full %>% filter(occ_code == "00-0000") %>%
      transmute(State = area_title, Statewide_Hourly_Median = h_median),
    by = "State"
  ) %>%
  mutate(
    Density_per_10k = 1e4 * Family_Employment / total_emp_state,
    Wage_Premium_vs_State = (Family_Hourly_Median_weighted / Statewide_Hourly_Median) - 1
  )

# 5) Compute LQ + Tightness properly
family_with_lq <- family_summaries %>%
  left_join(us_family %>% select(Family, us_share), by = "Family") %>%
  mutate(
    state_share = Family_Employment / total_emp_state,
    LQ = state_share / us_share,
    Tightness = case_when(
      !is.na(LQ) & LQ >= 1.10 ~ "Specialized",
      !is.na(LQ) & LQ <= 0.90 ~ "Thin bench",
      !is.na(LQ)             ~ "Balanced",
      TRUE                   ~ NA_character_
    )
  )

# Quick sanity check: you should now see a mix of Surplus/Balanced/Shortage
table(family_with_lq$Family, family_with_lq$Tightness, useNA = "ifany")
summary(family_with_lq$LQ)

library(openxlsx)

wide_tbl <- family_with_lq %>%
  select(State, Family, Density_per_10k, Wage_Premium_vs_State, LQ, Tightness) %>%
  mutate(Family = toupper(Family)) %>%
  pivot_wider(
    names_from = Family,
    values_from = c(Density_per_10k, Wage_Premium_vs_State, LQ, Tightness),
    names_glue = "{Family}_{.value}"
  ) %>%
  select(
    State,
    MAKE_Density_per_10k, MAKE_Wage_Premium_vs_State, MAKE_LQ, MAKE_Tightness,
    BUILD_Density_per_10k, BUILD_Wage_Premium_vs_State, BUILD_LQ, BUILD_Tightness,
    RUN_Density_per_10k, RUN_Wage_Premium_vs_State, RUN_LQ, RUN_Tightness,
    ENABLE_Density_per_10k, ENABLE_Wage_Premium_vs_State, ENABLE_LQ, ENABLE_Tightness
  ) %>%
  mutate(
    across(ends_with("Density_per_10k"), ~round(., 2)),
    across(ends_with("_LQ"), ~round(., 2))
  ) %>%
  arrange(State)

out_path <- file.path("C:/Users/LCarey/Downloads", "oews_four_families_state_table_with_LQ.xlsx")
wb <- createWorkbook(); addWorksheet(wb, "Four Families")
writeData(wb, "Four Families", wide_tbl, startRow = 3, headerStyle = createStyle(textDecoration = "bold"))
writeData(wb, "Four Families",
          x = t(c("State","Make","Make","Make","Make","Build","Build","Build","Build",
                  "Run","Run","Run","Run","Enable","Enable","Enable","Enable")),
          startRow = 1, startCol = 1, colNames = FALSE)
writeData(wb, "Four Families",
          x = t(c("", "Density","Wage Premium","LQ","Tightness",
                  "Density","Wage Premium","LQ","Tightness",
                  "Density","Wage Premium","LQ","Tightness",
                  "Density","Wage Premium","LQ","Tightness")),
          startRow = 2, startCol = 1, colNames = FALSE)

mergeCells(wb, "Four Families", cols = 2:5, rows = 1)
mergeCells(wb, "Four Families", cols = 6:9, rows = 1)
mergeCells(wb, "Four Families", cols = 10:13, rows = 1)
mergeCells(wb, "Four Families", cols = 14:17, rows = 1)
mergeCells(wb, "Four Families", cols = 1, rows = 1:2)

hdr <- createStyle(textDecoration="bold", halign="center", valign="center", border="Bottom")
addStyle(wb, "Four Families", hdr, rows = 1:2, cols = 1:ncol(wide_tbl), gridExpand = TRUE)
setRowHeights(wb, "Four Families", rows = 1:2, heights = 18)
setColWidths(wb, "Four Families", cols = 1, widths = 18)
setColWidths(wb, "Four Families", cols = 2:ncol(wide_tbl), widths = 14)

dens_cols <- grep("Density_per_10k$", names(wide_tbl))
prem_cols <- grep("Wage_Premium_vs_State$", names(wide_tbl))
lq_cols   <- grep("_LQ$", names(wide_tbl))
addStyle(wb, "Four Families", createStyle(numFmt="0.00"), rows=4:(nrow(wide_tbl)+3), cols=dens_cols, gridExpand=TRUE)
addStyle(wb, "Four Families", createStyle(numFmt="0.0%"), rows=4:(nrow(wide_tbl)+3), cols=prem_cols, gridExpand=TRUE)
addStyle(wb, "Four Families", createStyle(numFmt="0.00"), rows=4:(nrow(wide_tbl)+3), cols=lq_cols, gridExpand=TRUE)

freezePane(wb, "Four Families", firstActiveRow = 4, firstActiveCol = 2)
saveWorkbook(wb, out_path, overwrite = TRUE)
out_path

# =========================
# Metro clusters (OEWS MA)
# =========================
library(dplyr); library(tidyr); library(purrr); library(stringr)
library(readr); library(readxl); library(janitor); library(openxlsx)

# ---- Inputs ----
dl_dir <- "C:/Users/LCarey/Downloads"
ma_zip <- file.path(dl_dir, "oesm24ma.zip")   # May-2024 metro/nonmetro
stopifnot(file.exists(ma_zip))

# ---- Helpers you already used on state-level ----
to_numeric <- function(x) {
  if (is.numeric(x)) return(x)
  x <- gsub(",", "", x)
  x[trimws(x) %in% c("*","**","#","-","")] <- NA
  suppressWarnings(as.numeric(x))
}
ensure_cols <- function(df, cols) { for (cc in cols) if (!cc %in% names(df)) df[[cc]] <- NA_real_; df }

normalize_oews_cols <- function(df) {
  df <- janitor::clean_names(df)
  ren <- c(
    "area"="area","area_title"="area_title",
    "occ_code"="occ_code","occupation_code"="occ_code",
    "occ_title"="occ_title","occupation_title"="occ_title",
    "tot_emp"="tot_emp","total_employment"="tot_emp","emp_prse"="emp_prse",
    "h_mean"="h_mean","a_mean"="a_mean","h_median"="h_median","a_median"="a_median",
    "h_p10"="h_p10","h_p25"="h_p25","h_p75"="h_p75","h_p90"="h_p90",
    "a_p10"="a_p10","a_p25"="a_p25","a_p75"="a_p75","a_p90"="a_p90"
  )
  i <- intersect(names(df), names(ren))
  names(df)[match(i, names(df))] <- unname(ren[i])
  df %>% select(any_of(c(
    "area","area_title","occ_code","occ_title","tot_emp","emp_prse",
    "h_mean","a_mean","h_median","a_median",
    "h_p10","h_p25","h_p75","h_p90","a_p10","a_p25","a_p75","a_p90"
  )))
}

# --- Drop-in replacement: robust OEWS ZIP reader for MA/ST files -------------
# --- Drop-in replacement: robust OEWS ZIP reader for MA/ST files -------------
read_oews_zip_full <- function(zip_path) {
  stopifnot(file.exists(zip_path))
  
  # Helper: read any file as all-character first
  read_any_as_char <- function(path, ext) {
    if (tolower(ext) == "csv") {
      suppressMessages(
        readr::read_csv(path, col_types = readr::cols(.default = readr::col_character()))
      )
    } else if (tolower(ext) %in% c("xlsx","xls")) {
      # readxl: 'text' forces all columns to character
      readxl::read_excel(path, col_types = "text")
    } else {
      stop("Unsupported file type: ", ext)
    }
  }
  
  # Files in the ZIP
  files <- unzip(zip_path, list = TRUE)$Name
  data_files <- files[!grepl("readme|doc|note|\\.pdf$|\\.txt$", files, ignore.case = TRUE)]
  
  # Prefer CSV first (most complete), then XLSX/XLS
  pick <- c(grep("\\.csv$",  data_files, value = TRUE, ignore.case = TRUE),
            grep("\\.xlsx$", data_files, value = TRUE, ignore.case = TRUE),
            grep("\\.xls$",  data_files, value = TRUE, ignore.case = TRUE))
  stopifnot(length(pick) > 0)
  
  dfs <- purrr::map(pick, function(f){
    ext <- tools::file_ext(f)
    tmpdir <- tempdir()
    unzip(zip_path, files = f, exdir = tmpdir, overwrite = TRUE)
    path <- file.path(tmpdir, f)
    
    # 1) Read as character to avoid type clashes
    raw <- read_any_as_char(path, ext)
    
    # 2) Clean & normalize column names
    df <- janitor::clean_names(raw)
    
    # Map common variants -> standard set
    rename_map <- c(
      "area"="area","area_title"="area_title",
      "occ_code"="occ_code","occupation_code"="occ_code","occ"="occ_code",
      "occ_title"="occ_title","occupation_title"="occ_title","occ_title_m2024_dl"="occ_title",
      "tot_emp"="tot_emp","total_employment"="tot_emp","emp_tot"="tot_emp","employment"="tot_emp",
      "emp_prse"="emp_prse","employment_rse"="emp_prse",
      "h_mean"="h_mean","a_mean"="a_mean","mean_hourly_wage"="h_mean","mean_annual_wage"="a_mean",
      "h_median"="h_median","a_median"="a_median",
      "median_hourly_wage"="h_median","median_annual_wage"="a_median",
      "h_p10"="h_p10","h_p25"="h_p25","h_p75"="h_p75","h_p90"="h_p90",
      "a_p10"="a_p10","a_p25"="a_p25","a_p75"="a_p75","a_p90"="a_p90"
    )
    hit <- intersect(names(df), names(rename_map))
    names(df)[match(hit, names(df))] <- unname(rename_map[hit])
    
    # 3) Keep only the useful core (others are ignored safely)
    keep <- c("area","area_title","occ_code","occ_title","tot_emp","emp_prse",
              "h_mean","a_mean","h_median","a_median",
              "h_p10","h_p25","h_p75","h_p90","a_p10","a_p25","a_p75","a_p90")
    df <- dplyr::select(df, dplyr::any_of(keep))
    
    # 4) Guarantee all targets exist (as character for now)
    for (cc in keep) if (!cc %in% names(df)) df[[cc]] <- NA_character_
    
    df
  })
  
  # Bind AFTER standardization
  out <- dplyr::bind_rows(dfs)
  
  # 5) Convert numerics safely
  to_numeric <- function(x) {
    if (is.numeric(x)) return(x)
    x <- gsub(",", "", x)
    x[trimws(x) %in% c("*","**","#","-","")] <- NA
    suppressWarnings(as.numeric(x))
  }
  numeric_targets <- c("tot_emp","h_mean","a_mean","h_median","a_median",
                       "h_p10","h_p25","h_p75","h_p90","a_p10","a_p25","a_p75","a_p90")
  
  out %>%
    dplyr::mutate(
      dplyr::across(dplyr::all_of(numeric_targets), to_numeric),
      occ_code   = as.character(occ_code),
      area_title = as.character(area_title),
      area       = as.character(area)
    )
}


# ---- Load OEWS MA (metros + nonmetros) ----
oews_ma_full <- read_oews_zip_full(ma_zip)

# Keep METRO areas only (drop "Nonmetropolitan")
oews_metro <- oews_ma_full %>%
  filter(!grepl("Nonmetropolitan", area_title, ignore.case = TRUE))

# ---- SOC families (same as before; edit if needed) ----
SOCs_MAKE   <- c("49-9041","49-9071","51-4041","17-2112","17-2071","17-3024","17-3026","17-3029")
SOCs_BUILD  <- c("47-2111","49-9051","47-2231","47-2061","47-2152","47-2073")
SOCs_RUN    <- c("49-2095","49-9081","17-2072","49-1011","17-3023")
SOCs_ENABLE <- c("17-2112","13-1081","13-1151","13-1071","19-5011","17-2141")

families <- tibble::tibble(
  Family = c("MAKE","BUILD","RUN","ENABLE"),
  SOCs   = list(SOCs_MAKE, SOCs_BUILD, SOCs_RUN, SOCs_ENABLE)
)

# ---- Metro totals & U.S. denominators ----
metro_totals <- oews_metro %>%
  filter(occ_code == "00-0000") %>%
  transmute(Metro = area_title, total_emp_metro = tot_emp, metro_h_med = h_median)

us_total_emp_metro <- sum(metro_totals$total_emp_metro, na.rm = TRUE)

us_family_metro <- families %>%
  mutate(data = map(SOCs, ~ oews_metro %>% filter(occ_code %in% .x))) %>%
  select(Family, data) %>% unnest(data) %>%
  group_by(Family) %>%
  summarize(family_emp_us = sum(tot_emp, na.rm = TRUE), .groups = "drop") %>%
  mutate(us_share = family_emp_us / us_total_emp_metro)

# ---- Metro family summaries ----
metro_family <- families %>%
  mutate(data = map(SOCs, ~ oews_metro %>% filter(occ_code %in% .x))) %>%
  select(Family, data) %>%
  unnest(data) %>%
  group_by(Family, Metro = area_title) %>%
  summarize(
    Family_Employment = sum(tot_emp, na.rm = TRUE),
    Family_Hourly_Median_weighted = wtd_mean(h_median, tot_emp),
    .groups = "drop"
  ) %>%
  left_join(metro_totals, by = "Metro") %>%
  mutate(
    Density_per_10k = 1e4 * Family_Employment / total_emp_metro,
    Wage_Premium_vs_Metro = (Family_Hourly_Median_weighted / metro_h_med) - 1
  ) %>%
  left_join(us_family_metro %>% select(Family, us_share), by = "Family") %>%
  mutate(
    metro_share = Family_Employment / total_emp_metro,
    LQ = metro_share / us_share,
    Tightness = case_when(
      !is.na(LQ) & LQ >= 1.10 ~ "Surplus / Specialized",
      !is.na(LQ) & LQ <= 0.90 ~ "Shortage / Thin bench",
      !is.na(LQ)             ~ "Balanced",
      TRUE                   ~ NA_character_
    )
  )

library(dplyr); library(stringr); library(tidyr); library(openxlsx)

# Your list
regions <- c(
  "Rome, GA",
  "Frankfort, KY","Lafayette, LA","Albuquerque, NM",
  "Greensboro, NC","Tulsa, OK",
  "Greenville, SC",
  "Nashville, TN",
  "Dallas, TX","Richmond, VA"
)

# 1) Build a robust metro index (first city token + state)
metro_index <- metro_family %>%
  distinct(Metro = Metro) %>%
  mutate(
    StateAbbr = str_extract(Metro, "[A-Z]{2}$"),
    CityFull  = str_extract(Metro, "^[^,]+"),            # e.g., "Atlanta-Sandy Springs-Roswell"
    CityFirst = str_split_fixed(CityFull, "-", 2)[,1],   # take first token: "Atlanta"
    CityKey   = str_to_lower(str_remove_all(CityFirst, "[\\s\\.]")) # normalize "La Grange" -> "lagrange"
  )

# 2) Normalize your target list the same way
targets <- tibble(Region = regions) %>%
  separate(Region, into = c("City","StateAbbr"), sep = ",\\s*") %>%
  mutate(CityKey = str_to_lower(str_remove_all(City, "[\\s\\.]")))

# 3) Find the OEWS Metro names that match your list
metros_keep <- metro_index %>%
  inner_join(targets, by = c("StateAbbr","CityKey")) %>%
  pull(Metro) %>%
  unique()

# Quick check of matches (optional)
setdiff(targets$City, metro_index %>% filter(Metro %in% metros_keep) %>% pull(CityFirst))

# 4) Filter the metro family summary to only those metros
metro_selected <- metro_family %>%
  filter(Metro %in% metros_keep) %>%
  arrange(Family, Metro)

# 5) Export a compact Excel (one sheet per family)
out_path <- file.path("C:/Users/LCarey/Downloads", "oews_metro_SELECTED_clusters.xlsx")
wb <- createWorkbook()

for (fam in c("MAKE","BUILD","RUN","ENABLE")) {
  df <- metro_selected %>%
    filter(Family == fam) %>%
    transmute(
      Region = Metro,
      Density_per_10k = round(Density_per_10k, 2),
      Wage_Premium_vs_Metro = Wage_Premium_vs_Metro,
      LQ = round(LQ, 2),
      Tightness
    ) %>%
    arrange(Region)
  
  addWorksheet(wb, fam)
  writeData(wb, fam, df, headerStyle = createStyle(textDecoration = "bold"))
  
  # Formats
  dens_col <- which(names(df) == "Density_per_10k")
  prem_col <- which(names(df) == "Wage_Premium_vs_Metro")
  lq_col   <- which(names(df) == "LQ")
  addStyle(wb, fam, createStyle(numFmt = "0.00"),
           rows = 2:(nrow(df)+1), cols = dens_col, gridExpand = TRUE)
  addStyle(wb, fam, createStyle(numFmt = "0.0%"),
           rows = 2:(nrow(df)+1), cols = prem_col, gridExpand = TRUE)
  addStyle(wb, fam, createStyle(numFmt = "0.00"),
           rows = 2:(nrow(df)+1), cols = lq_col, gridExpand = TRUE)
  
  # Optional traffic-light fill for Tightness
  tcol <- which(names(df) == "Tightness")
  rule_surplus  <- createStyle(fgFill = "#D8F5D1")
  rule_balanced <- createStyle(fgFill = "#FFF6BF")
  rule_short    <- createStyle(fgFill = "#FAD2D2")
  conditionalFormatting(wb, fam, cols = tcol, rows = 2:(nrow(df)+1),
                        type = "contains", rule = "Surplus", style = rule_surplus)
  conditionalFormatting(wb, fam, cols = tcol, rows = 2:(nrow(df)+1),
                        type = "contains", rule = "Balanced", style = rule_balanced)
  conditionalFormatting(wb, fam, cols = tcol, rows = 2:(nrow(df)+1),
                        type = "contains", rule = "Shortage", style = rule_short)
  
  setColWidths(wb, fam, cols = 1, widths = 36)
  setColWidths(wb, fam, cols = 2:ncol(df), widths = 14)
  freezePane(wb, fam, firstActiveRow = 2, firstActiveCol = 2)
}

saveWorkbook(wb, out_path, overwrite = TRUE)
out_path

# ----- Scorecard for metro_selected (metros) ----------------------------------
library(dplyr)
library(tidyr)
library(stringr)
library(gt)
library(gtExtras)   # for gt_plt_bar; if missing, install.packages("gtExtras")
library(scales)
library(glue)
library(rlang)

# --- 1) Prep: add Job Quality label; keep LQ; remove Density ------------------
score_df <- metro_selected %>%
  mutate(
    Region   = Metro,
    Family   = toupper(Family),
    Premium  = Wage_Premium_vs_Metro,        # numeric (decimal)
    LQ       = round(LQ, 2),
    Tightness = case_when(
      LQ >= 1.10 ~ "Specialized",
      LQ <= 0.90 ~ "Shortage",
      TRUE       ~ "Balanced"
    ),
    # Job Quality traffic light from wage premium:
    # > +5% = Good, -2%..+5% = Fair, < -2% = Weak
    JobQuality = case_when(
      Premium > 0.33 ~ "Great",
      Premium >  0.1 ~ "Good",
      Premium >= -0.02 & Premium <= 0.1 ~ "Fair",
      Premium <  -0.02 ~ "Weak",
      TRUE ~ NA_character_
    )
  ) %>%
  select(State, Region, Family, Premium, JobQuality, LQ, Tightness) %>%
  pivot_wider(
    names_from  = Family,
    values_from = c(Premium, JobQuality, LQ, Tightness),
    names_glue  = "{Family}_{.value}"
  ) %>%
  arrange(State, Region)

prem_cols   <- names(score_df)[grepl("^MAKE|^BUILD|^RUN|^ENABLE", names(score_df)) & grepl("_Premium$",   names(score_df))]
qual_cols   <- names(score_df)[grepl("^MAKE|^BUILD|^RUN|^ENABLE", names(score_df)) & grepl("_JobQuality$",names(score_df))]
lq_cols     <- names(score_df)[grepl("^MAKE|^BUILD|^RUN|^ENABLE", names(score_df)) & grepl("_LQ$",       names(score_df))]
tight_cols  <- names(score_df)[grepl("^MAKE|^BUILD|^RUN|^ENABLE", names(score_df)) & grepl("_Tightness$", names(score_df))]

# color helpers
tight_col <- function(x) dplyr::case_when(
  x == "Specialized"  ~ "#4CAF50",  # green
  x == "Balanced" ~ "#FFC107",  # amber
  x == "Shortage" ~ "#F44336",  # red
  TRUE ~ "#BDBDBD"
)
quality_col <- function(x) dplyr::case_when(
  x == "Great" ~"#0BD0D9", 
  x == "Good" ~ "#4CAF50",      # green
  x == "Fair" ~ "#FFC107",      # amber
  x == "Weak" ~ "#F44336",      # red
  TRUE ~ "#BDBDBD"
)

# --- 3) Add SOC note per family ----------------------------------------------
soc_titles <- c(
  # MAKE
  "49-9041"="Industrial Machinery Mechanics",
  "49-9071"="Maintenance and Repair Workers, General",
  "51-4041"="Machinists",
  "17-2112"="Industrial Engineers",
  "17-2071"="Electrical Engineers",
  "17-3024"="Electro-Mechanical & Mechatronics Technologists/Technicians",
  "17-3026"="Industrial Engineering Technologists/Technicians",
  "17-3029"="Engineering Technologists & Technicians, Except Drafters, All Other",
  # BUILD
  "47-2111"="Electricians",
  "49-9051"="Electrical Power-Line Installers & Repairers",
  "47-2231"="Solar Photovoltaic Installers",
  "47-2061"="Construction Laborers",
  "47-2152"="Plumbers, Pipefitters, and Steamfitters",
  "47-2073"="Operating Engineers & Other Construction Equipment Operators",
  # RUN
  "49-2095"="Electrical & Electronics Repairers, Powerhouse, Substation, and Relay",
  "49-9081"="Wind Turbine Service Technicians",
  "17-2072"="Electronics Engineers, Except Computer",
  "49-1011"="First-Line Supervisors of Mechanics, Installers, and Repairers",
  "17-3023"="Electrical & Electronic Engineering Technologists/Technicians",
  # ENABLE
  "13-1081"="Logisticians",
  "13-1151"="Training & Development Specialists",
  "13-1071"="Human Resources Specialists",
  "19-5011"="Occupational Health & Safety Specialists",
  "17-2141"="Mechanical Engineers"
)

soc_names <- function(codes, source_df = NULL) {
  codes <- as.character(codes)
  if (!is.null(source_df) && all(c("occ_code","occ_title") %in% names(source_df))) {
    from_oews <- source_df %>%
      dplyr::filter(occ_code %in% codes) %>%
      dplyr::distinct(occ_code, occ_title)
    # Merge with fallback to ensure full coverage
    out <- dplyr::tibble(occ_code = codes) %>%
      dplyr::left_join(from_oews, by = "occ_code") %>%
      dplyr::mutate(occ_title = dplyr::coalesce(occ_title, soc_titles[occ_code]))
    return(out$occ_title)
  } else {
    return(unname(soc_titles[codes]))
  }
}

# 3) Build note strings with NAMES instead of codes (uses your SOC vectors)
make_names   <- soc_names(SOCs_MAKE,   source_df = oews_ma_full)
build_names  <- soc_names(SOCs_BUILD,  source_df = oews_ma_full)
run_names    <- soc_names(SOCs_RUN,    source_df = oews_ma_full)
enable_names <- soc_names(SOCs_ENABLE, source_df = oews_ma_full)

make_note   <- paste0("MAKE: ",   paste(make_names,   collapse = "; "))
build_note  <- paste0("BUILD: ",  paste(build_names,  collapse = "; "))
run_note    <- paste0("RUN: ",    paste(run_names,    collapse = "; "))
enable_note <- paste0("ENABLE: ", paste(enable_names, collapse = "; "))


library(gt)
library(glue)

# Build GT with spanners; no Density, LQ as number
gt_tbl <- score_df %>%
  gt(rowname_col = "Region", groupname_col = "State") %>%
  tab_spanner("Make",   columns = c(MAKE_Premium,  MAKE_JobQuality,  MAKE_LQ,  MAKE_Tightness)) %>%
  tab_spanner("Build",  columns = c(BUILD_Premium, BUILD_JobQuality, BUILD_LQ, BUILD_Tightness)) %>%
  tab_spanner("Run",    columns = c(RUN_Premium,   RUN_JobQuality,   RUN_LQ,   RUN_Tightness)) %>%
  tab_spanner("Enable", columns = c(ENABLE_Premium,ENABLE_JobQuality,ENABLE_LQ,ENABLE_Tightness)) %>%
  
  # Wage premium as % (numeric value shown)
  fmt_percent(columns = all_of(prem_cols), decimals = 1) %>%
  
  # JobQuality and Tightness as colored pills
  text_transform(
    locations = cells_body(columns = all_of(qual_cols)),
    fn = function(x) {
      mapply(function(lbl, col) {
        html(glue(
          "<span style='background:{col};color:white;padding:2px 6px;
                  border-radius:10px;font-size:85%'>{lbl}</span>"
        ))
      }, x, quality_col(x), SIMPLIFY = FALSE)
    }
  ) %>%
  text_transform(
    locations = cells_body(columns = all_of(tight_cols)),
    fn = function(x) {
      mapply(function(lbl, col) {
        html(glue(
          "<span style='background:{col};color:white;padding:2px 6px;
                  border-radius:10px;font-size:85%'>{lbl}</span>"
        ))
      }, x, tight_col(x), SIMPLIFY = FALSE)
    }
  ) %>%
  
  # Format LQ numerically (2 decimals) and relabel as "Specialization"
  fmt_number(columns = all_of(lq_cols), decimals = 2) %>%
  cols_label(
    MAKE_Premium      = "Wage prem.",   MAKE_JobQuality  = "Job quality",
    MAKE_LQ           = "Specialization", MAKE_Tightness = "Tightness",
    BUILD_Premium     = "Wage prem.",   BUILD_JobQuality = "Job quality",
    BUILD_LQ          = "Specialization", BUILD_Tightness= "Tightness",
    RUN_Premium       = "Wage prem.",   RUN_JobQuality   = "Job quality",
    RUN_LQ            = "Specialization", RUN_Tightness  = "Tightness",
    ENABLE_Premium    = "Wage prem.",   ENABLE_JobQuality= "Job quality",
    ENABLE_LQ         = "Specialization", ENABLE_Tightness = "Tightness"
  ) %>%
  tab_options(table.font.size = px(12), data_row.padding = px(3)) %>%
  tab_source_note(
    md(paste0(
      "**Notes.** *Job quality* thresholds (vs metro median): **Great** > 33%; **Good** > +10%; **Fair** ???2% to +5%; **Weak** < ???2%. ",
      "**Specialization** = metro employment share for the family ÷ U.S. share (LQ).  ",
      "**Occupations included** - ",
      make_note, " | ", build_note, " | ", run_note, " | ", enable_note, "."
    ))
  )
gt_tbl <- gt_tbl %>%
  # Bold the State names (row group labels)
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_row_groups()
  ) %>%
  # Bold the four family spanners: Make, Build, Run, Enable
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_column_spanners(spanners = c("Make","Build","Run","Enable"))
  )

gt_tbl
# ---- Export the scorecard to your folder ----
library(gt)
# If you don't have webshot2 installed for PNG, uncomment:
# install.packages("webshot2")

out_dir <- "C:/Users/LCarey/Downloads"
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

html_path <- file.path(out_dir, "metro_scorecard.html")
png_path  <- file.path(out_dir, "metro_scorecard.png")
pdf_path  <- file.path(out_dir, "metro_scorecard.pdf")

# Save HTML (no extra dependencies)
gtsave(gt_tbl, html_path)

# Save PNG (uses webshot2/Chromium under the hood)
gtsave(gt_tbl, png_path, expand = 5)  # expand adds padding around the table

# Optional: Save PDF (also via headless Chromium)
gtsave(gt_tbl, pdf_path, expand = 5)

cat("Saved:\n", png_path)


#Greatest Shortage by Cluster
# =========================
# "Greatest Shortage" by Family & Metro
# =========================
library(dplyr); library(tidyr); library(stringr); library(purrr); library(readr)
library(openxlsx); library(scales)

# Inputs assumed present from earlier steps:
# - oews_metro: OEWS metro rows (incl. occ_code, occ_title, tot_emp, h_median, area_title)
# - metro_selected: your filtered per-family metro summary (we'll take its Metro list)
# - SOCs_MAKE, SOCs_BUILD, SOCs_RUN, SOCs_ENABLE

# 0) Keep only the metros you care about
metros_keep <- unique(metro_selected$Metro)

# 1) Metro totals (employment & median) for wage premium and shares
metro_totals <- oews_metro %>%
  filter(occ_code == "00-0000") %>%
  transmute(Metro = area_title,
            total_emp_metro = tot_emp,
            metro_h_med = h_median)

# 2) U.S. totals across metros (denominator for SOC-level LQ)
us_total_emp_metro <- sum(metro_totals$total_emp_metro, na.rm = TRUE)

# 3) U.S. share by SOC (across all metros)
us_soc_share <- oews_metro %>%
  filter(occ_code != "00-0000") %>%
  group_by(occ_code) %>%
  summarize(us_soc_emp = sum(tot_emp, na.rm = TRUE), .groups = "drop") %>%
  mutate(us_soc_share = us_soc_emp / us_total_emp_metro)

# 4) Build a named list of families -> SOCs
families <- list(
  MAKE   = SOCs_MAKE,
  BUILD  = SOCs_BUILD,
  RUN    = SOCs_RUN,
  ENABLE = SOCs_ENABLE
)

# helper to tag a row by which family it belongs to (first match wins)
which_family <- function(code) {
  for (nm in names(families)) if (code %in% families[[nm]]) return(nm)
  NA_character_
}

# 5) SOC-level metrics for only your metros, limited to SOCs we care about
soc_metrics <- oews_metro %>%
  filter(area_title %in% metros_keep, occ_code != "00-0000") %>%
  mutate(Family = vapply(occ_code, which_family, FUN.VALUE = character(1))) %>%
  filter(!is.na(Family)) %>%
  left_join(metro_totals, by = c("area_title" = "Metro")) %>%
  left_join(us_soc_share %>% select(occ_code, us_soc_share), by = "occ_code") %>%
  mutate(
    metro_share_soc = ifelse(total_emp_metro > 0, tot_emp / total_emp_metro, NA_real_),
    LQ_soc          = metro_share_soc / us_soc_share,                     # specialization at SOC level
    wage_premium    = (h_median / metro_h_med) - 1,                        # vs metro median
    JobQuality      = case_when(                                          # traffic light from premium
      wage_premium >  0.05 ~ "Good",
      wage_premium >= -0.02 & wage_premium <= 0.05 ~ "Fair",
      wage_premium <  -0.02 ~ "Weak",
      TRUE ~ NA_character_
    )
  ) %>%
  transmute(
    StateAbbr = str_extract(area_title, "[A-Z]{2}$"),
    Metro     = area_title,
    Family,
    occ_code, occ_title,
    LQ_soc,
    wage_premium,
    JobQuality
  )

# 6) For each Metro x Family, pick the occupation with the LOWEST LQ (greatest shortage)
greatest_shortage <- soc_metrics %>%
  group_by(Metro, Family) %>%
  slice_min(order_by = LQ_soc, with_ties = FALSE, na_rm = TRUE) %>%
  ungroup() %>%
  # attach State names for display (using your earlier mapping if present)
  mutate(
    State = dplyr::case_when(
      StateAbbr %in% state.abb ~ state.name[match(StateAbbr, state.abb)],
      TRUE ~ NA_character_
    )
  ) %>%
  select(
    State, Metro, Family,
    Occupation = occ_title,
    Specialization = LQ_soc,
    `Wage premium` = wage_premium,
    `Job quality` = JobQuality
  ) %>%
  arrange(State, Metro, Family) %>%
  # pretty formats for quick viewing (keep raw for file export)
  mutate(
    Specialization_fmt = number(Specialization, accuracy = 0.01),
    Wage_premium_fmt   = percent(`Wage premium`, accuracy = 0.1)
  )

# Peek
head(greatest_shortage, 12)
# View(greatest_shortage)

# 7) Export to Excel (one compact sheet)
out_dir  <- "C:/Users/LCarey/Downloads"
out_path <- file.path(out_dir, "metro_greatest_shortage_by_family.xlsx")

wb <- openxlsx::createWorkbook()
openxlsx::addWorksheet(wb, "Greatest shortage")

export_tbl <- greatest_shortage %>%
  select(State, Metro, Family, Occupation,
         Specialization, `Wage premium`, `Job quality`)

openxlsx::writeData(wb, "Greatest shortage", export_tbl,
                    headerStyle = openxlsx::createStyle(textDecoration = "bold"))

# Formats: 2-dec number for LQ; percent for wage prem.
lq_col   <- which(names(export_tbl) == "Specialization")
prem_col <- which(names(export_tbl) == "Wage premium")
openxlsx::addStyle(wb, "Greatest shortage",
                   openxlsx::createStyle(numFmt = "0.00"),
                   rows = 2:(nrow(export_tbl)+1), cols = lq_col, gridExpand = TRUE)
openxlsx::addStyle(wb, "Greatest shortage",
                   openxlsx::createStyle(numFmt = "0.0%"),
                   rows = 2:(nrow(export_tbl)+1), cols = prem_col, gridExpand = TRUE)

openxlsx::setColWidths(wb, "Greatest shortage", cols = 1, widths = 16)
openxlsx::setColWidths(wb, "Greatest shortage", cols = 2, widths = 40)
openxlsx::setColWidths(wb, "Greatest shortage", cols = 3, widths = 10)
openxlsx::setColWidths(wb, "Greatest shortage", cols = 4, widths = 44)
openxlsx::setColWidths(wb, "Greatest shortage", cols = 5:7, widths = 14)

openxlsx::freezePane(wb, "Greatest shortage", firstActiveRow = 2, firstActiveCol = 3)
openxlsx::saveWorkbook(wb, out_path, overwrite = TRUE)

cat("Saved:", out_path, "\n")


shortage_wide <- greatest_shortage %>%
  select(State,Metro,Family,Occupation) %>%
  pivot_wider(names_from="Family",values_from="Occupation") %>%
  write.csv("Downloads/shortages.csv")
