#Electrotech Index



#Policy Intent

#Econ Dev Incentives----------------

gjf<- read.csv("OneDrive - RMI/Regional Investment Strategies/Great Lakes Investment Strategy/Great Lakes Overview/Econ Development/gjf_complete.csv")

gjf_statetotal_1924<-gjf %>%
  filter(Year>2019) %>%
  group_by(region,Location) %>%
  summarize_at(vars(subs_m),sum,na.rm=T) %>%
  arrange(desc(subs_m)) %>%
  ungroup() %>%
  inner_join(state_gdp, by=c("Location"="GeoName")) %>%
  mutate(incent_gdp=subs_m/X2022*100,
         incent_gdp_rank = rank(-subs_m/X2022))


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

#Policy Intent Index-----------------------

policy_intent <-gjf_statetotal_1924 %>%
  select(Location,incent_gdp) %>%
  left_join(spot,by=c("Location"="State")) %>%
  left_join(dsire_count %>%
              rename(dsire=n),by=c("Location"="state")) %>%
  left_join(climate_dev_pol_sum,by=c("Location"="State")) %>%
  left_join(leg_index,by=c("Location"="statename")) %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(intent_index = rowMeans(across(where(is.numeric)), na.rm = TRUE))


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
years <- c(2024)

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


#Economic Dynamism
url <- "https://eig.org/state-dynamism-2025/assets/Downloadable-Data-EIG-Index-of-State-Dynamism-2022.xlsx"

tf <- tempfile(fileext = ".xlsx")
download.file(url, tf, mode = "wb")  # mode="wb" is important on Windows

# See available sheets
excel_sheets(tf)
# -> pick the sheet you want by name or index

dynamism <- read_excel(tf, sheet = 1)  # or sheet = "Index 2022" (example)
dynamism <- janitor::clean_names(dynamism) %>%
  filter(year=="2022")

#Econ Capabilities Index-----------------------------------
econ_index <- bundle_lq %>%
  select(state_abbr,LQ_bundle) %>%
  left_join(states_simple %>%
              select(abbr,full),by=c("state_abbr"="abbr")) %>%
  left_join(gdp_man_index %>%
              select(GeoName,gdp_index),by=c("full"="GeoName")) %>%
  left_join(feas_state,by=c("full"="geo_name")) %>%
  left_join(dynamism %>%
              select(state_abbreviation,combined_score),by=c("state_abbr"="state_abbreviation")) %>%
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
#Infrastructure Index----------------------

# 1) Decide polarity (edit these as you see fit)
positive <- c("ren_index", "ev_stations_cap", "price_index")   # higher = better
negative <- c("q_share")         # higher = worse

# (Optional) weights; omit if you want a simple average
weights <- c(ren_index = 0.25, ev_stations_cap = 0.25, q_share = 0.25, price_index = 0.25)

# 2) Build the table (your joins), then scale by polarity and score
infrastructure_index <- renpotential_state %>%
  select(geo_name, ren_index) %>%
  left_join(states_simple %>% select(abbr, full),             by = c("geo_name" = "full")) %>%
  left_join(ev_stations_state %>% select(State, ev_stations_cap), by = c("geo_name" = "State")) %>%
  left_join(queue %>% select(state, q_share),                  by = c("abbr" = "state")) %>%
  left_join(ind_price %>% select(State, price_index),          by = c("abbr" = "State")) %>%
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
  left_join(census_divisions,by=c("Plant State"="State.Code")) %>%
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
  mutate(rengrowth_22_25 = cum_cap - cum_cap[Year=="2022-01-01"]) %>%
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
  filter(Technology %in% c("Batteries","Storage","Solar","Wind","Critical Minerals","Heat Pumps","Nuclear","Zero Emission Vehicles")) %>%
  group_by(State,Segment) %>%
  summarize(inv=sum(Estimated_Actual_Quarterly_Expenditure,na.rm=T )) %>%
  left_join(socioecon %>%
              filter(quarter=="2025-Q2"),by=c("State"="State"))%>%
  mutate(inv_gdp=inv/real_gdp) %>%
  select(State,Segment,inv_gdp) %>%
  pivot_wider(names_from="Segment",values_from="inv_gdp") %>%
  mutate(Manufacturing=ifelse(is.na(Manufacturing),0,Manufacturing)) %>%
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
  left_join(datacenter_index %>%
              select(STATE,datacenter_index),by=c("full"="STATE")) %>%
  left_join(states_rengen_index %>%
              select(State,capacity_index),by=c("full"="State")) %>%
  left_join(semi_man %>%
              select(State,semi_gdp),by=c("State")) %>%
  left_join(evs_state %>%
              select(State,ev_cap),by=c("full"="State")) %>%
  ungroup() %>%
  mutate(across(
    where(is.numeric),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(deployment_index = rowMeans(across(where(is.numeric)), na.rm = TRUE)) 




#ElectroTech Index -----------------------
weights <- c(deployment_index = 0.3, infra_index = 0.2, econ_index = 0.3, intent_index = 0.2)

electrotech <- deployment_index %>%
  select(State,full,deployment_index) %>%
  left_join(infrastructure_index %>%
              select(geo_name,infra_index),by=c("full"="geo_name")) %>%
  left_join(econ_index %>%
              select(state_abbr,econ_index),by=c("State"="state_abbr")) %>%
  left_join(policy_intent %>%
              select(Location,intent_index),by=c("full"="Location")) %>%
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
  arrange(desc(electrotech_index))

write.csv(electrotech %>%
            arrange(desc(electrotech_index)) %>%
            slice_max(electrotech_index,n=20),"Downloads/electrotech.csv")

electrotech_div <- electrotech %>%
  left_join(census_divisions,by=c("State"="State.Code")) %>%
  group_by(Division) %>%
  summarize(electrotech_index=mean(electrotech_index,na.rm=T))

elec_price<- electrotech %>%
  left_join(ind_price,by=c("State"="State"))
# select only numeric columns
num_vars <- elec_price %>%
  dplyr::select(where(is.numeric))

# compute correlation matrix
cor_matrix <- cor(num_vars, use = "pairwise.complete.obs", method = "pearson")

# view
cor_matrix


#ELectrotech Facilities Chart


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

  
electrotech_fac<-rbind(facilities_cim_electro,datacenter_fac) %>%
  rbind(semi_fac) %>%
  rbind(elec_fac) %>%
  group_by(unit) %>%
  mutate(across(
    c(size),
    ~ (. - min(.[!is.infinite(.)], na.rm = TRUE)) / 
      (max(.[!is.infinite(.)] - min(.[!is.infinite(.)], na.rm = TRUE), na.rm = TRUE))
  )) %>%
  ungroup() %>%
  mutate(size=ifelse(unit=="MW",size*0.66,size))


write.csv(electrotech_fac,"Downloads/electrotech.csv")


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
