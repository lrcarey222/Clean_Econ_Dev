# 0) Libraries & global options   #
# ------------------------------- #
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(purrr)
  library(readr)
  library(writexl)
  library(countrycode)
  library(scales)
})

options(dplyr.summarise.inform = FALSE)

set_primary_comtrade_key('2940653b9bbe4671b3f7fde2846d14be')

# -------------------------------------- #
# 1) Utilities & normalizers (one place) #
# -------------------------------------- #

# Robust 0-1 S-curve scaling used across the pipeline.
# Falls back gracefully if your custom median_scurve() isn't defined.
safe_scurve <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  if (!length(x) || all(is.na(x))) return(rep(NA_real_, length(x)))
  if (exists("median_scurve")) return(median_scurve(x))
  # Conservative fallback: rank-based 0-1
  rescale(rank(x, na.last = "keep", ties.method = "average"), to = c(0, 1))
}

# A single place to normalize country names that appear in your sources
norm_country <- function(x) {
  x <- stringr::str_squish(x)
  dplyr::recode(
    x,
    "United kingdom" = "United Kingdom",
    "Korea, south"   = "South Korea",
    "Korea, Republic of" = "South Korea",
    "Vietnam"        = "Viet Nam",
    "Turkey"         = "Turkiye",
    "T rkiye, Republic of" = "Turkiye",
    "Czechia"        = "Czech Republic",
    "Curacao"        = "Cura ao",
    "Saudi arabia"   = "Saudi Arabia",
    "Iran, Islamic Rep." = "Iran",
    "Iran, Islamic Republic of" = "Iran",
    .default = x
  )
}

# ISO3 -> Country helper, backed by your country_info table when possible
iso_to_country <- function(iso_vec) {
  lookup <- country_info %>%
    mutate(country = norm_country(country)) %>%
    distinct(iso3c, country)
  tibble(iso3c = iso_vec) %>%
    left_join(lookup, by = "iso3c") %>%
    transmute(country = country %||% countrycode(iso3c, "iso3c", "country.name")) %>%
    pull(country)
}

# Guard: ensure required objects exist
required_objs <- c("res","subcat","econ_opp_index","energy_security_index",
                   "tech_ghg","cat","country_info","allies")
missing <- setdiff(required_objs, ls())
if (length(missing)) stop("Missing required objects: ", paste(missing, collapse = ", "))

ally_iso <- unique(allies$iso3c)


gdp_data<-WDI(indicator = "NY.GDP.MKTP.CD", start = 2007, end = 2024) 
gdp <- gdp_data %>%           # iso3c, year, NY.GDP.MKTP.CD
  rename(GDP = NY.GDP.MKTP.CD)
gdp_2024 <- gdp %>%
  filter(year=="2024")

country_info <- WDI_data$country %>%
  filter(region!="Aggregates") %>%
  mutate(country=ifelse(country=="Russian Federation","Russia",country),
         country=ifelse(iso3c=="KOR","South Korea",country),
         country=ifelse(iso3c=="COD","Democratic Republic of Congo",country))

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

ref_area <- allies$iso3c

#Overall Indices--------------------------------
scatter_index<-read.csv("OneDrive - RMI/New Energy Industrial Strategy - Documents/Research/Data/scatter_index.csv")

ally_scatter <- scatter_index %>%
  filter(Country %in% allies$country) %>%
  select(Country,tech,supply_chain,category,value) %>%
  pivot_wider(names_from="category",values_from ="value") %>%
  mutate(ci = paste0(Country,": ",tech, " ",supply_chain))

write.csv(ally_scatter %>% 
            filter(tech=="Batteries",supply_chain %in% c("Midstream","Upstream")) %>%
            mutate(`Energy Security`= 1 - `Energy Security`),
          "Downloads/ally_batt_mid.csv")

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

#Trade Data - Comtrade--------------------------------------------

#All Energy Trade
country_info_iso <- country_info %>%
  filter(!iso3c %in% c("ASM", "CHI", "GUM", "IMN", "LIE", "MAF", "MCO", "PRI", "XKX"))

subcat<-read.csv(paste0(raw_data,"hts_codes_categories_bolstered_final.csv")) %>%
  mutate(code=as.character(HS6))

# 1) Clean & prep the HS6 codes
codes <- subcat$code %>%
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

split_vec <- function(x, chunk_size) {
  if (length(x) == 0) return(list(character()))
  split(x, ceiling(seq_along(x) / chunk_size))
}

safe_ct <- purrr::possibly(ct_get_data, otherwise = NULL)

# reporter / partner / code inputs you already have
reporters <- allies$iso3c
partners  <- country_info_iso$iso3c
codes     <- code_chunks                 # from your split_by_nchar(...)
years     <- c(2020,2024)
dirs      <- c("export","import")

# ALSO chunk the partner list to keep rows per call below 100k
# (tune chunk_size if you still hit the cap; larger == fewer calls, smaller == safer)
partner_chunks <- split_vec(partners, chunk_size = 50)

# Cartesian product: one reporter × one year × one flow × one code-chunk × one partner-chunk
grid <- tidyr::expand_grid(
  rep  = reporters,
  yr   = years,
  dir  = dirs,
  cc   = codes,
  pch  = partner_chunks
)

pb <- progress_bar$new(
  format = "  :current/:total [:bar] :percent | ETA: :eta | rep=:rep yr=:yr dir=:dir partners=:pn codes=:cn",
  total  = nrow(grid),
  clear  = FALSE, width = 90
)

# Run the queries
res_list <- purrr::pmap(
  grid,
  function(rep, yr, dir, cc, pch) {
    pb$tick(tokens = list(rep = rep, yr = yr, dir = dir,
                          pn = length(pch), cn = length(cc)))
    Sys.sleep(0.5)  # increase if rate-limited
    out <- safe_ct(
      reporter       = rep,
      partner        = pch,
      commodity_code = cc,
      start_date     = yr,
      end_date       = yr,
      flow_direction = dir
    )
    if (is.null(out)) return(NULL)
    
    # Tag the direction if the API payload doesn't include it
    if (!"flow_direction" %in% names(out)) {
      out <- dplyr::mutate(out, flow_direction = dir)
    }
    # (Optional) if there's a 'trade_flow' column, keep only the requested flow
    if ("trade_flow" %in% names(out)) {
      out <- dplyr::filter(out, tolower(trade_flow) == dir)
    }
    
    # Stamp keys to help debugging / dedup if needed
    dplyr::mutate(out, reporter_req = rep, year_req = yr)
  }
)

# Bind and dedupe
res <- dplyr::bind_rows(res_list) %>% dplyr::distinct()
write.csv(res,paste0(raw_data,"allied_comtrade_energy_data.csv"))
res <-read.csv(paste0(raw_data,"allied_comtrade_energy_data.csv"))

#All Allies
library(dplyr)
library(tidyr)
library(stringr)

# Keep only columns we need; standardize names; limit to target years
res_tech <- res %>%
  left_join(subcat, by = c("cmd_code" = "code"), relationship = "many-to-many") %>%
  rename(tech = Technology, supply_chain = `Value.Chain`) %>%
  mutate(
    primary_value  = suppressWarnings(as.numeric(primary_value)),
    flow_direction = tolower(flow_direction),
    period         = suppressWarnings(as.integer(period))
  ) %>%
  filter(!is.na(period), period %in% 2020:2024) %>%
  select(reporter_iso, partner_iso, period, flow_direction, tech, supply_chain, primary_value)




#Investment Data - IMF Direct Investment Position------------------------
fdi_all <-read.csv(paste0(raw_data,"imf_dip.csv"))

fdi_allies <- fdi_all %>%
  mutate(COUNTRY=recode(
    COUNTRY,
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


outbound <- fdi_allies %>%
  left_join(country_info %>% select(country,iso3c),by=c("COUNTRY"="country")) %>%
  left_join(country_info %>% select(country,iso3c),by=c("COUNTERPART_COUNTRY"="country")) %>%
  filter(!is.na(iso3c.x),
         !is.na(iso3c.y)) %>%
  ungroup() %>%
  left_join(gdp_2024, by = c("iso3c.x" = "iso3c")) %>%
  left_join(gdp_2024, by = c("iso3c.y" = "iso3c")) %>%
  filter(!is.na(GDP.x)) %>%
  filter(!is.na(GDP.y)) %>%
  mutate(GDP_cp = GDP.y / 1e6, 
         GDP = GDP.x / 1e6, 
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


#Export Opportunity index---------------------
# -------------------------------------------- #
# Dyad time series -> levels & growth       #
# -------------------------------------------- #

build_dyad_series <- function(res_tech, flow = c("export","import"), years = 2020:2024) {
  flow <- match.arg(flow)
  res_tech %>%
    filter(flow_direction == flow, !is.na(tech), !is.na(supply_chain)) %>%
    group_by(reporter_iso, partner_iso, tech, supply_chain, period) %>%
    summarise(val = sum(primary_value, na.rm = TRUE), .groups = "drop_last") %>%
    complete(period = c(min(years), max(years)), fill = list(val = 0)) %>%
    ungroup() %>%
    pivot_wider(names_from = period, values_from = val, names_prefix = "y", values_fill = 0) %>%
    transmute(
      reporter_iso, partner_iso, tech, supply_chain,
      level_last  = .data[[paste0("y", max(years))]],
      level_first = .data[[paste0("y", min(years))]],
      growth      = dplyr::if_else(level_first > 0, (level_last / level_first) - 1, NA_real_)
    )
}
# ------------------------------------------------ #
# Export-side trade indices (relative scaling)  #
# ------------------------------------------------ #

ds_export <- build_dyad_series(res_tech, flow = "export", years = 2020:2024)

trade_indices_all <- ds_export %>%
  group_by(tech, supply_chain) %>%
  mutate(
    export_index        = safe_scurve(level_last),     # level in last year
    export_growth_index = safe_scurve(growth),         # growth from first -> last
    # NA-safe weighted blend: 2 * level + 1 * growth, then re-scaled in (tech, chain)
    ti_num = 2 * export_index + 1 * export_growth_index,
    ti_den = 2*(!is.na(export_index)) + 1*(!is.na(export_growth_index)),
    trade_index_raw = if_else(ti_den > 0, ti_num / ti_den, NA_real_)
  ) %>%
  mutate(trade_index = safe_scurve(trade_index_raw)) %>%
  ungroup() %>%
  select(reporter_iso, partner_iso, tech, supply_chain, trade_index)


# ------------------------------------------------------------ #
# 5) Partner & exporter modifiers for Opportunity calculation  #
# ------------------------------------------------------------ #

# Exporter-side economic opportunity (by exporter ISO, tech, chain)
econ_opp_iso <- econ_opp_index %>%
  mutate(Country = norm_country(Country),
         exporter_iso = countrycode(Country, "country.name", "iso3c",
                                    custom_match = c("Viet Nam"="VNM","Turkiye"="TUR","South Korea"="KOR",
                                                     "Cura ao"="CUW","Laos"="LAO","Czech Republic"="CZE"))) %>%
  filter(grepl("Overall\\s*Economic\\s*Opportunity", variable, ignore.case = TRUE)) %>%
  transmute(exporter_iso, tech, supply_chain, econ_opp_raw = value)

# Partner-side energy security (invert to "need/attractiveness")
energy_sec_iso <- energy_security_index %>%
  mutate(Country = norm_country(Country),
         partner_iso = countrycode(Country, "country.name", "iso3c",
                                   custom_match = c("Viet Nam"="VNM","Turkiye"="TUR","South Korea"="KOR",
                                                    "Cura ao"="CUW","Laos"="LAO","Czech Republic"="CZE"))) %>%
  filter(grepl("Overall\\s*Energy\\s*Security", variable, ignore.case = TRUE)) %>%
  transmute(partner_iso, tech, supply_chain, energy_sec_raw = 1 - value)

# Tech-level GHG and partner climate policy
ghg_iso <- tech_ghg %>% rename(tech = !!(names(tech_ghg)[names(tech_ghg) %in% c("Tech","tech")][1])) %>%
  transmute(tech, ghg_index = as.numeric(ghg_index))

policy_iso <- cat %>%
  mutate(Country = norm_country(str_to_title(Country %||% "")),
         iso3c   = countrycode(Country, "country.name", "iso3c",
                               custom_match = c("Viet Nam"="VNM","Turkiye"="TUR","South Korea"="KOR",
                                                "Cura ao"="CUW","Laos"="LAO","Czech Republic"="CZE"))) %>%
  transmute(iso3c, climate_policy_index)

default_ghg    <- if (nrow(ghg_iso))    median(ghg_iso$ghg_index, na.rm = TRUE) else 0.5
default_policy <- if (nrow(policy_iso)) median(policy_iso$climate_policy_index, na.rm = TRUE) else 0.5

# -------------------------------------- #
# 6) Opportunity index (global, dyads)   #
# -------------------------------------- #

opportunity_all <- trade_indices_all %>%
  left_join(econ_opp_iso,  by = c("reporter_iso" = "exporter_iso", "tech","supply_chain")) %>%
  left_join(energy_sec_iso, by = c("partner_iso","tech","supply_chain")) %>%
  left_join(ghg_iso,        by = "tech") %>%
  left_join(policy_iso,     by = c("partner_iso" = "iso3c")) %>%
  filter(!is.na(econ_opp_raw), !is.na(energy_sec_raw)) %>%
  group_by(tech, supply_chain) %>%
  mutate(
    econ_opp_index        = safe_scurve(econ_opp_raw),
    energy_security_index = safe_scurve(energy_sec_raw),
    ghg_index             = coalesce(ghg_index, default_ghg),
    climate_policy_index  = coalesce(climate_policy_index, default_policy)
  ) %>%
  ungroup() %>%
  mutate(
    # Weighted blend: 2*trade + 2*econ_opp + 0.5*energy_security
    o_num = 2*trade_index + 2*econ_opp_index + 0.5*energy_security_index,
    o_den = 2*(!is.na(trade_index)) + 2*(!is.na(econ_opp_index)) + 0.5*(!is.na(energy_security_index)),
    opportunity_raw = if_else(o_den > 0, o_num / o_den, NA_real_),
    penalty = (1 - ghg_index) * climate_policy_index * 0.20,
    opportunity_index_raw = pmax(0, opportunity_raw - penalty)
  ) %>%
  group_by(tech, supply_chain) %>%
  mutate(opportunity_index = safe_scurve(opportunity_index_raw)) %>%
  ungroup() %>%
  select(reporter_iso, partner_iso, tech, supply_chain,
         trade_index, econ_opp_index, energy_security_index,
         ghg_index, climate_policy_index, penalty,
         opportunity_raw, opportunity_index_raw, opportunity_index)



#Total Friendshore Index------------------------
# ------------------------------------------------------------ #
# 7) Import-side + friend-shoring (global, dyads)              #
# ------------------------------------------------------------ #

ds_import <- build_dyad_series(res_tech, flow = "import", years = 2020:2024)

import_idx_all <- ds_import %>%
  group_by(tech, supply_chain) %>%
  mutate(
    import_index      = safe_scurve(level_last),
    import_growth_idx = safe_scurve(growth),
    imp_num = 2*import_index + 1*import_growth_idx,
    imp_den = 2*(!is.na(import_index)) + 1*(!is.na(import_growth_idx)),
    imp_trade_index   = if_else(imp_den > 0, imp_num / imp_den, NA_real_),
    imp_trade_index_z = safe_scurve(imp_trade_index)
  ) %>%
  ungroup() %>%
  transmute(reporter_iso, partner_iso, tech, supply_chain,
            imp_trade_index = imp_trade_index_z)

# importer energy-security "need", partner opportunity, optional outbound FDI ties
es_iso <- energy_security_index %>%
  mutate(Country = norm_country(Country),
         iso3c   = countrycode(Country, "country.name", "iso3c",
                               custom_match = c("Viet Nam"="VNM","Turkiye"="TUR","South Korea"="KOR",
                                                "Cura ao"="CUW","Laos"="LAO","Czech Republic"="CZE"))) %>%
  filter(grepl("Overall\\s*Energy\\s*Security", variable, ignore.case = TRUE)) %>%
  transmute(reporter_iso = iso3c, tech, supply_chain, es_need = 1 - value)

eo_partner_iso <- econ_opp_index %>%
  mutate(Country = norm_country(Country),
         iso3c   = countrycode(Country, "country.name", "iso3c",
                               custom_match = c("Viet Nam"="VNM","Turkiye"="TUR","South Korea"="KOR",
                                                "Cura ao"="CUW","Laos"="LAO","Czech Republic"="CZE"))) %>%
  filter(grepl("Overall\\s*Economic\\s*Opportunity", variable, ignore.case = TRUE)) %>%
  transmute(partner_iso = iso3c, tech, supply_chain, eo_partner = value)

# Outbound FDI ties if available; else fallback to 0
outbound_edges <- if (exists("outbound") &&
                      all(c("COUNTRY","COUNTERPART_COUNTRY","outbound_index") %in% names(outbound))) {
  outbound %>%
    left_join(select(country_info, iso3c, country), by = c("COUNTRY"="country")) %>%
    rename(reporter_iso = iso3c) %>%
    left_join(rename(select(country_info, iso3c, country), partner_iso = iso3c),
              by = c("COUNTERPART_COUNTRY"="country")) %>%
    select(reporter_iso, partner_iso, outbound_index) %>%
    distinct()
} else {
  tibble(reporter_iso = character(), partner_iso = character(), outbound_index = numeric())
}

friendshore_all <- import_idx_all %>%
  left_join(es_iso,         by = c("tech","supply_chain","reporter_iso")) %>%
  left_join(eo_partner_iso, by = c("tech","supply_chain","partner_iso")) %>%
  left_join(outbound_edges, by = c("reporter_iso","partner_iso")) %>%
  left_join(rename(policy_iso, reporter_iso = iso3c, cpi_r = climate_policy_index), by = "reporter_iso") %>%
  left_join(rename(policy_iso, partner_iso  = iso3c, cpi_p = climate_policy_index), by = "partner_iso") %>%
  left_join(ghg_iso, by = "tech") %>%
  mutate(
    cpi_r    = coalesce(cpi_r, default_policy),
    cpi_p    = coalesce(cpi_p, default_policy),
    gh_entry = coalesce(ghg_index, default_ghg),
    outbound_index = coalesce(outbound_index, 0)
  ) %>%
  mutate(
    # Your global variant: 1*(imports) + 1*(ES need) + 1*(EO partner) + 1*(FDI ties)
    fsi_raw   = imp_trade_index + es_need + eo_partner + outbound_index,
    penalty_r = (1 - gh_entry) * cpi_r * 0.10,
    penalty_p = (1 - gh_entry) * cpi_p * 0.10,
    fsi_adj   = pmax(0, fsi_raw - (penalty_r + penalty_p))
  ) %>%
  group_by(tech, supply_chain) %>%
  mutate(friendshore_index = safe_scurve(fsi_adj)) %>%
  ungroup() %>%
  select(reporter_iso, partner_iso, tech, supply_chain,
         imp_trade_index, es_need, eo_partner, outbound_index,
         penalty_r, penalty_p, fsi_raw, fsi_adj, friendshore_index)

# ---------------------------------------------------------------- #
# 8) Build tidy sheets in the 'security_opp_indices' style schema  #
#     (Country, tech, supply_chain, Pillar, category, variable,    #
#      data_type, value, source, explanation)                      #
# ---------------------------------------------------------------- #

# NOTE: set top_n_dyads once here for consistency with your indices:
top_n_dyads <- 3

# --- 1) Build richer EXPORTS dyad table -----------------------------------
exports_dyads <- ds_export %>%
  # attach export-side indices from earlier step
  left_join(
    trade_indices_all %>%
      group_by(reporter_iso, partner_iso, tech, supply_chain) %>%
      summarise(trade_index = max(trade_index, na.rm = TRUE), .groups = "drop"),
    by = c("reporter_iso","partner_iso","tech","supply_chain")
  ) %>%
  # (re)compute subcomponents for clarity
  group_by(tech, supply_chain) %>%
  mutate(
    export_index        = safe_scurve(level_last),   # last-year exports level
    export_growth_index = safe_scurve(growth),       # growth 2020->2024
    # same recipe you used to build trade_index_raw before scaling
    ti_num = 2*export_index + 1*export_growth_index,
    ti_den = 2*(!is.na(export_index)) + 1*(!is.na(export_growth_index)),
    trade_index_raw = if_else(ti_den > 0, ti_num / ti_den, NA_real_)
  ) %>%
  ungroup() %>%
  mutate(
    Reporter = iso_to_country(reporter_iso),
    Partner  = iso_to_country(partner_iso)
  )

# Tidy/long with metadata
exports_meta <- tribble(
  ~variable,              ~source,                          ~explanation,                                                                ~data_type,
  "level_first",          "UN Comtrade (exports)",          "Exporter->partner value in first year (e.g., 2020).",                        "value",
  "level_last",           "UN Comtrade (exports)",          "Exporter->partner value in last year (e.g., 2024).",                         "value",
  "growth",               "UN Comtrade (exports) + author", "Percent change last/first ??? 1.",                                            "rate",
  "export_index",         "UN Comtrade (exports) + author", "Rescaled last-year exports level within tech × chain.",                      "index",
  "export_growth_index",  "UN Comtrade (exports) + author", "Rescaled export growth within tech × chain.",                                "index",
  "trade_index_raw",      "Author calculation",             "2×level_index + 1×growth_index (pre-rescale).",                              "composite",
  "trade_index",          "Author calculation",             "Export-side composite rescaled within tech × chain.",                        "index"
)

Exports_Dyads <- exports_dyads %>%
  pivot_longer(
    cols = c(level_first, level_last, growth, export_index, export_growth_index, trade_index_raw, trade_index),
    names_to = "variable", values_to = "value"
  ) %>%
  left_join(exports_meta, by = "variable") %>%
  mutate(
    Country    = iso_to_country(partner_iso),
    Pillar     = "Trade (Exports)",
    category   = "Exports",
    source     = coalesce(source, "Author calculation"),
    explanation= coalesce(explanation, "Derived in pipeline."),
    data_type  = coalesce(data_type, "value")
  ) %>%
  select(Reporter, Partner, reporter_iso, partner_iso,
         Country, tech, supply_chain, Pillar, category, variable, data_type, value, source, explanation) %>%
  arrange(tech, supply_chain, Reporter, Partner, variable)

# Country-aggregated (average of the same top-N dyads used by OPPORTUNITY index)
Exports_ByCountry <- opportunity_all %>%
  filter(!is.na(opportunity_index)) %>%
  group_by(partner_iso, tech, supply_chain) %>%
  slice_max(order_by = opportunity_index, n = top_n_dyads, with_ties = FALSE) %>%
  ungroup() %>%
  select(reporter_iso, partner_iso, tech, supply_chain) %>%
  inner_join(exports_dyads, by = c("reporter_iso","partner_iso","tech","supply_chain")) %>%
  group_by(partner_iso, tech, supply_chain) %>%
  summarise(across(c(level_first, level_last, growth, export_index, export_growth_index, trade_index_raw, trade_index),
                   ~mean(.x, na.rm = TRUE)),
            .groups = "drop") %>%
  mutate(Country = iso_to_country(partner_iso)) %>%
  pivot_longer(cols = c(level_first, level_last, growth, export_index, export_growth_index, trade_index_raw, trade_index),
               names_to = "variable", values_to = "value") %>%
  left_join(exports_meta, by = "variable") %>%
  mutate(
    Pillar    = "Trade (Exports)",
    category  = "Exports (Top Dyads Avg)",
    source    = coalesce(source, "Author calculation"),
    explanation = coalesce(explanation, "Average over top dyads that define the Opportunity index."),
    data_type = coalesce(data_type, "value")
  ) %>%
  select(Country, tech, supply_chain, Pillar, category, variable, data_type, value, source, explanation) %>%
  arrange(Country, tech, supply_chain, variable)

# --- 2) Build richer IMPORTS dyad table -----------------------------------
imports_dyads <- ds_import %>%
  left_join(import_idx_all, by = c("reporter_iso","partner_iso","tech","supply_chain")) %>%
  group_by(tech, supply_chain) %>%
  mutate(
    import_index      = safe_scurve(level_last),
    import_growth_idx = safe_scurve(growth),
    imp_num = 2*import_index + 1*import_growth_idx,
    imp_den = 2*(!is.na(import_index)) + 1*(!is.na(import_growth_idx)),
    imp_trade_index_raw = if_else(imp_den > 0, imp_num / imp_den, NA_real_)
  ) %>%
  ungroup() %>%
  mutate(
    Reporter = iso_to_country(reporter_iso),
    Partner  = iso_to_country(partner_iso)
  )

imports_meta <- tribble(
  ~variable,               ~source,                           ~explanation,                                                                ~data_type,
  "level_first",           "UN Comtrade (imports)",           "Importer<-partner value in first year (e.g., 2020).",                        "value",
  "level_last",            "UN Comtrade (imports)",           "Importer<-partner value in last year (e.g., 2024).",                         "value",
  "growth",                "UN Comtrade (imports) + author",  "Percent change last/first ??? 1.",                                            "rate",
  "import_index",          "UN Comtrade (imports) + author",  "Rescaled last-year imports level within tech × chain.",                      "index",
  "import_growth_idx",     "UN Comtrade (imports) + author",  "Rescaled import growth within tech × chain.",                                "index",
  "imp_trade_index_raw",   "Author calculation",              "2×level_index + 1×growth_index (pre-rescale).",                              "composite",
  "imp_trade_index",       "Author calculation",              "Import-side composite rescaled within tech × chain.",                        "index"
)

Imports_Dyads <- imports_dyads %>%
  pivot_longer(
    cols = c(level_first, level_last, growth, import_index, import_growth_idx, imp_trade_index_raw, imp_trade_index),
    names_to = "variable", values_to = "value"
  ) %>%
  left_join(imports_meta, by = "variable") %>%
  mutate(
    Country     = iso_to_country(partner_iso),
    Pillar      = "Trade (Imports)",
    category    = "Imports",
    source      = coalesce(source, "Author calculation"),
    explanation = coalesce(explanation, "Derived in pipeline."),
    data_type   = coalesce(data_type, "value")
  ) %>%
  select(Reporter, Partner, reporter_iso, partner_iso,
         Country, tech, supply_chain, Pillar, category, variable, data_type, value, source, explanation) %>%
  arrange(tech, supply_chain, Reporter, Partner, variable)

# Country-aggregated (average of the same top-N dyads used by FRIENDSHORE index)
Imports_ByCountry <- friendshore_all %>%
  filter(!is.na(friendshore_index)) %>%
  group_by(partner_iso, tech, supply_chain) %>%
  slice_max(order_by = friendshore_index, n = top_n_dyads, with_ties = FALSE) %>%
  ungroup() %>%
  select(reporter_iso, partner_iso, tech, supply_chain) %>%
  inner_join(imports_dyads, by = c("reporter_iso","partner_iso","tech","supply_chain")) %>%
  group_by(partner_iso, tech, supply_chain) %>%
  summarise(across(c(level_first, level_last, growth, import_index, import_growth_idx, imp_trade_index_raw, imp_trade_index),
                   ~mean(.x, na.rm = TRUE)),
            .groups = "drop") %>%
  mutate(Country = iso_to_country(partner_iso)) %>%
  pivot_longer(cols = c(level_first, level_last, growth, import_index, import_growth_idx, imp_trade_index_raw, imp_trade_index),
               names_to = "variable", values_to = "value") %>%
  left_join(imports_meta, by = "variable") %>%
  mutate(
    Pillar    = "Trade (Imports)",
    category  = "Imports (Top Dyads Avg)",
    source    = coalesce(source, "Author calculation"),
    explanation = coalesce(explanation, "Average over top dyads that define the Friendshore index."),
    data_type = coalesce(data_type, "value")
  ) %>%
  select(Country, tech, supply_chain, Pillar, category, variable, data_type, value, source, explanation) %>%
  arrange(Country, tech, supply_chain, variable)

# --- 3) Build richer OUTBOUND dyad table -----------------------------------
# If you have extra columns in `outbound` (e.g., flows), merge them safely
outbound_dyads <- outbound_edges %>%
  mutate(
    Reporter = iso_to_country(reporter_iso),
    Partner  = iso_to_country(partner_iso)
  )

# If you have a reporter/partner policy you might attach penalties again (optional)
# Example: none added here-keep it as a clean network/ties sheet.

outbound_meta <- tribble(
  ~variable,         ~source,                     ~explanation,                                                ~data_type,
  "outbound_index",  "Your outbound FDI ties tbl","Normalized outbound FDI/network ties to partner (0-1).",   "index"
)

Outbound_Dyads <- outbound_dyads %>%
  ungroup() %>%
  pivot_longer(cols = c(outbound_index), names_to = "variable", values_to = "value") %>%
  left_join(outbound_meta, by = "variable") %>%
  mutate(
    Country     = iso_to_country(partner_iso),
    Pillar      = "Outbound Investment",
    category    = "Outbound Ties",
    source      = coalesce(source, "Author calculation"),
    explanation = coalesce(explanation, "Derived in pipeline."),
    data_type   = coalesce(data_type, "index")
  ) %>%
  select(Reporter, Partner, reporter_iso, partner_iso,
         Country,Pillar, category, variable, data_type, value, source, explanation) %>%
  arrange(Reporter, Partner, variable)

# Country-aggregated (align with FRIENDSHORE selection or choose top dyads by outbound_index)
Outbound_ByCountry <- friendshore_all %>%
  filter(!is.na(friendshore_index)) %>%
  group_by(partner_iso) %>%
  slice_max(order_by = friendshore_index, n = top_n_dyads, with_ties = FALSE) %>%
  ungroup() %>%
  select(reporter_iso, partner_iso) %>%
  inner_join(outbound_dyads, by = c("reporter_iso","partner_iso")) %>%
  group_by(partner_iso) %>%
  summarise(outbound_index = mean(outbound_index, na.rm = TRUE), .groups = "drop") %>%
  mutate(Country = iso_to_country(partner_iso)) %>%
  pivot_longer(cols = c(outbound_index), names_to = "variable", values_to = "value") %>%
  left_join(outbound_meta, by = "variable") %>%
  mutate(
    Pillar    = "Outbound Investment",
    category  = "Outbound Ties (Top Dyads Avg)",
    source    = coalesce(source, "Author calculation"),
    explanation = coalesce(explanation, "Average over top dyads that define the Friendshore index."),
    data_type = coalesce(data_type, "index")
  ) %>%
  select(Country, Pillar, category, variable, data_type, value, source, explanation) %>%
  arrange(Country, variable)



# 1) Variable-level metadata -----------------------------------------------
opp_meta <- tribble(
  ~variable,               ~source,                         ~explanation,                                                                 ~data_type,
  "trade_index",           "UN Comtrade (exports) + author","Export-side composite of last-year level and 4-5y growth, rescaled within tech × chain.", "index",
  "econ_opp_index",        "Your econ_opp_index table",      "Exporter economic opportunity (already scaled 0-1 in your pipeline).",               "index",
  "energy_security_index", "Your energy_security_index tbl", "Partner energy-security 'need' (higher = more need), scaled 0-1.",                   "index",
  "ghg_index",             "Tech GHG lookup (tech_ghg)",     "Technology-level lifecycle GHG factor (higher = cleaner).",                          "index",
  "climate_policy_index",  "Policy/CAT lookup (cat)",        "Partner climate-policy stringency proxy, scaled 0-1.",                               "index",
  "penalty",               "Author calculation",             "(1???GHG) × climate_policy × 0.20 applied to opportunity.",                           "composite",
  "opportunity_raw",       "Author calculation",             "Pre-penalty weighted blend: 2×trade + 2×econ_opp + 0.5×energy_security.",           "composite",
  "opportunity_index_raw", "Author calculation",             "Post-penalty value before within-tech×chain rescale.",                               "composite",
  "opportunity_index",     "Author calculation",             "Final opportunity index rescaled within tech × chain.",                              "index"
)

fsi_meta <- tribble(
  ~variable,            ~source,                          ~explanation,                                                                 ~data_type,
  "imp_trade_index",    "UN Comtrade (imports) + author", "Import-side composite of last-year level and 4-5y growth, rescaled within tech × chain.", "index",
  "es_need",            "Your energy_security_index tbl", "Importer energy-security 'need' (higher = more need), scaled 0-1.",                      "index",
  "eo_partner",         "Your econ_opp_index table",      "Partner economic opportunity (scaled 0-1).",                                           "index",
  "outbound_index",     "Your outbound FDI ties table",   "Normalized outbound FDI/network ties to partner (0 if unavailable).",                   "index",
  "penalty_r",          "Author calculation",             "Reporter-side climate penalty: (1???GHG) × reporter_policy × 0.10.",                      "composite",
  "penalty_p",          "Author calculation",             "Partner-side climate penalty: (1???GHG) × partner_policy × 0.10.",                        "composite",
  "fsi_raw",            "Author calculation",             "Pre-penalty composite: imports + ES need + EO partner + outbound.",                     "composite",
  "fsi_adj",            "Author calculation",             "Post-penalty value before within-tech×chain rescale.",                                  "composite",
  "friendshore_index",  "Author calculation",             "Final friendshore index rescaled within tech × chain.",                                 "index"
)

# 2) Helper that injects metadata per variable ------------------------------
build_inputs_sheet <- function(df, index_col, vars, pillar_name, category_name, meta_tbl, n_top = 3) {
  stopifnot(all(c("partner_iso","tech","supply_chain", index_col) %in% names(df)))
  # pick same top-N dyads as the final index
  top_dyads <- df %>%
    filter(!is.na(.data[[index_col]])) %>%
    group_by(partner_iso, tech, supply_chain) %>%
    slice_max(order_by = .data[[index_col]], n = n_top, with_ties = FALSE) %>%
    ungroup()
  
  # average each requested input across those dyads
  agg <- top_dyads %>%
    group_by(partner_iso, tech, supply_chain) %>%
    summarise(across(all_of(vars), ~mean(.x, na.rm = TRUE)), .groups = "drop") %>%
    mutate(Country = iso_to_country(partner_iso))
  
  # tidy + attach variable-level metadata (with sensible defaults)
  agg %>%
    pivot_longer(cols = all_of(vars), names_to = "variable", values_to = "value") %>%
    left_join(meta_tbl, by = "variable") %>%
    mutate(
      Pillar      = pillar_name,
      category    = category_name,
      data_type   = coalesce(data_type, "index"),
      source      = coalesce(source, "Author calculation"),
      explanation = coalesce(explanation, "Derived in pipeline; see code comments.")
    ) %>%
    select(Country, tech, supply_chain, Pillar, category, variable, data_type, value, source, explanation) %>%
    arrange(Country, tech, supply_chain, variable)
}

# Opportunity inputs sheet (per-variable source/explanation)
opp_vars <- c("trade_index","econ_opp_index","energy_security_index",
              "ghg_index","climate_policy_index","penalty",
              "opportunity_raw","opportunity_index_raw","opportunity_index")

opp_inputs_sheet <- build_inputs_sheet(
  opportunity_all,
  index_col   = "opportunity_index",
  vars        = opp_vars,
  pillar_name = "Opportunity",
  category_name = "Opportunity Inputs",
  meta_tbl    = opp_meta,
  n_top       = 3
)

# Friendshoring inputs sheet (per-variable source/explanation)
fsi_vars <- c("imp_trade_index","es_need","eo_partner","outbound_index",
              "penalty_r","penalty_p","fsi_raw","fsi_adj","friendshore_index")

fsi_inputs_sheet <- build_inputs_sheet(
  friendshore_all %>% filter(!partner_iso %in% c("CHN","RUS")),
  index_col   = "friendshore_index",
  vars        = fsi_vars,
  pillar_name = "Friendshoring",
  category_name = "Friendshoring Inputs",
  meta_tbl    = fsi_meta,
  n_top       = 3
)

# Make sure no lingering dplyr groups interfere
security_opp_indices <- security_opp_indices %>% dplyr::ungroup()

# Columns we want, in order
pillar_cols <- c(
  "Country", "tech", "supply_chain",
  "Pillar", "category", "variable", "data_type",
  "value", "source", "explanation"
)

# Defensive select (in case extra cols exist)
select_pillar_cols <- function(df) {
  missing <- setdiff(pillar_cols, names(df))
  if (length(missing)) {
    stop("security_opp_indices is missing columns: ",
         paste(missing, collapse = ", "))
  }
  df %>% dplyr::select(dplyr::all_of(pillar_cols))
}

energy_security_sheet <- security_opp_indices %>%
  dplyr::filter(Pillar == "Energy Security") %>%
  select_pillar_cols() %>%
  dplyr::arrange(Country, tech, supply_chain)

economic_opportunity_sheet <- security_opp_indices %>%
  dplyr::filter(Pillar == "Economic Opportunity") %>%
  select_pillar_cols() %>%
  dplyr::arrange(Country, tech, supply_chain)

# Build README as a single text column (Excel will wrap lines)
readme_tbl <- tibble(
  README = c(
    "README - Opportunity & Friendshoring Indices Workbook",
    "",
    "Purpose",
    "This workbook summarizes composite indices to assess clean-energy trade, investment, and partner opportunity across technologies and value-chain stages.",
    "It integrates export/import flows, economic opportunity, energy security, emissions intensity, climate-policy alignment, and outbound ties.",
    "",
    "Sheet Overview",
    ". Opportunity_Index - Final country-level Opportunity Index (avg of top-3 export dyads per Country×Tech×Supply Chain).",
    ". Friendshore_Index - Final country-level Friendshore Index (avg of top-3 import dyads per Country×Tech×Supply Chain).",
    ". Opportunity_Inputs - Inputs to Opportunity (trade, econ_opp, energy_security, ghg, policy, penalties, raw/adj), averaged over same top-3 dyads.",
    ". Friendshoring_Inputs - Inputs to Friendshoring (imports, ES need, EO partner, outbound, penalties, raw/adj), averaged over same top-3 dyads.",
    ". Exports_Dyads / Exports_ByCountry - Export levels, growth, and indices at dyad level and averaged by partner country.",
    ". Imports_Dyads / Imports_ByCountry - Import levels, growth, and indices at dyad level and averaged by partner country.",
    ". Outbound_Dyads / Outbound_ByCountry - Outbound investment/FDI ties (normalized 0-1) at dyad level and averaged by partner country.",
    "",
    "Method Summary",
    "Data sources: UN Comtrade (2020-2024), econ_opp_index, energy_security_index, tech_ghg, climate policy (CAT), outbound ties.",
    "Rescaling: median-S-curve to 0-1 within Technology × Supply Chain groups.",
    "Aggregation: country-level indices = mean of the top 3 dyads per Country×Tech×Supply Chain (prevents outliers, rewards consistency).",
    "",
    "Weighting (pre-scaling)",
    "Opportunity = 2×Trade + 2×Exporter EconOpp + 0.5×Partner EnergySec ??? 0.2×(1???GHG)×Policy.",
    "Friendshoring = Imports + ES Need + EO Partner + Outbound ??? [0.1×(1???GHG)×Policy_reporter + 0.1×(1???GHG)×Policy_partner].",
    "",
    "Variable Conventions",
    "trade_index / imp_trade_index: export/import composite (level + growth, 0-1).",
    "econ_opp_index / eo_partner: exporter/partner opportunity (0-1).",
    "energy_security_index / es_need: partner/importer vulnerability (0-1).",
    "ghg_index: tech cleanliness (0-1, higher = cleaner).  climate_policy_index: policy strength (0-1).",
    "outbound_index: outbound ties (0-1).  penalty / penalty_r / penalty_p: climate-related deductions.",
    " *_raw / *_adj: pre-scaling composites; *_index: final normalized 0-1.",
    "",
    "Intended Use",
    "Identify priority partner countries by technology and value-chain stage; compare export competitiveness vs. friendshoring feasibility;",
    "support industrial strategy where economic, climate, and trade objectives align.",
    "",
    "Citation",
    "Author calculation based on UN Comtrade, ILO/World Bank (as applicable), Climate Action Tracker, and RMI/NEIS datasets (2020-2024)."
  )
)

# Write as FIRST tab with the rest of your sheets
# (Assumes you already created: op_sheet, fsi_sheet, opp_inputs_sheet, fsi_inputs_sheet,
#  Exports_Dyads, Exports_ByCountry, Imports_Dyads, Imports_ByCountry, Outbound_Dyads, Outbound_ByCountry)



# ----------------------
# Write all four sheets
# ----------------------
out_xlsx <- file.path(getwd(), "OneDrive - RMI/New Energy Industrial Strategy - Documents/NEIS Center Asia Trips/Research/Benchmark_Indices.xlsx")
writexl::write_xlsx(
  list(
    README              = readme_tbl,
    Opportunity_Index   = op_sheet,
    Friendshore_Index   = fsi_sheet,
    Opportunity_Inputs  = opp_inputs_sheet,
    Friendshoring_Inputs= fsi_inputs_sheet,
    Economic_Opportunity=economic_opportunity_sheet,
    Energy_Security = energy_security_sheet,
    Exports_Dyads       = Exports_Dyads,
    Imports_Dyads       = Imports_Dyads,
    Outbound_Dyads      = Outbound_Dyads
  ),
  path = out_xlsx
)
message("Wrote Excel workbook with indices + inputs: ", out_xlsx)

#Top 10 of Each library(dplyr)
country_flags<-read.csv("Downloads/Country flag lookup table - Sheet1.csv")
friendshore_ten <- 
  # 1) Safer (friend-shore) block
  friendshore_index %>%
  ungroup() %>%
  mutate(ci = paste0(country.y,": ",industry)) %>%
  left_join(country_flags %>%
              mutate(Country=recode(
                Country,"United States of America"="United States"
              )),by=c("country.y"="Country")) %>%
  select(
    code_1=Code,
    "Friendshoring"       = ci,
    friendshore_index) %>%
  slice_max(friendshore_index, n = 10)
write.csv(friendshore_ten,"Downloads/friendshore.csv")


# Charts-----------------------

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



#Investment Chord Diagram
library(dplyr)
library(tidyr)
library(circlize)
library(RColorBrewer)
library(scales)

ally_set  <- unique(allies$country)   # names must match fdi_allies$COUNTRY strings
value_col <- "total"                  # or "X2023", etc.

# Keep only ally???ally flows (drop self-loops)
flows <- fdi_allies %>%
  filter(COUNTRY %in% ally_set,
         COUNTERPART_COUNTRY %in% ally_set,
         DI_DIRECTION == "Outward") %>%     # drop this line to include both directions
  transmute(from = COUNTRY,
            to   = COUNTERPART_COUNTRY,
            value = .data[[value_col]]) %>%
  group_by(from, to) %>%
  summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
  filter(value > 0, from != to)

# Use allies order for the ring (so it only shows allies)
nodes <- intersect(ally_set, unique(c(flows$from, flows$to)))
pal   <- setNames(colorRampPalette(brewer.pal(8, "Set3"))(length(nodes)), nodes)

circos.clear()
circos.par(gap.after = rep(3, length(nodes)))

chordDiagram(
  x               = flows,
  order           = nodes,
  grid.col        = pal,
  transparency    = 0.25,
  directional     = 1,                # arrows show direction (from ??? to)
  direction.type  = c("arrows","diffHeight"),
  link.arr.type   = "big.arrow",
  link.sort       = TRUE,
  col             = adjustcolor(pal[flows$from], alpha.f = 0.6),
  link.lwd        = rescale(flows$value, to = c(1, 8)),
  annotationTrack = "grid",
  preAllocateTracks = list(track.height = 0.08)
)

# Labels
circos.trackPlotRegion(track.index = 1, bg.border = NA, panel.fun = function(x, y) {
  sector = get.cell.meta.data("sector.index")
  xlim   = get.cell.meta.data("xlim")
  ylim   = get.cell.meta.data("ylim")
  circos.text(mean(xlim), ylim[1] + 0.1, sector, facing = "clockwise",
              niceFacing = TRUE, adj = c(0, 0.5), cex = 0.7)
})


#EV Midstream Chord Diagram
# install.packages(c("circlize","dplyr","tidyr","RColorBrewer","scales"))

ally_iso <- unique(allies$iso3c)
ally_iso<-c(ally_iso,"CHN")

# --- Filter to allies + EV Midstream and latest year ---
dat <- res_tech %>%
  filter(Technology == "Solar",
         Value.Chain == "Midstream",
         reporter_iso %in% ally_iso,
         partner_iso  %in% ally_iso) %>%
  mutate(period_num = suppressWarnings(as.integer(period))) %>%
  filter(!is.na(period_num))

yr <- max(dat$period_num, na.rm = TRUE)

# Sum trade balance per pair in that year
pairs <- dat %>%
  filter(period_num == yr) %>%
  group_by(reporter_iso, partner_iso) %>%
  summarise(value = sum(trade_balance, na.rm = TRUE), .groups = "drop") %>%
  filter(reporter_iso != partner_iso)

# Positive balance: reporter -> partner; Negative: partner -> reporter (abs)
pos <- pairs %>% filter(value > 0) %>%
  transmute(from = reporter_iso, to = partner_iso, value = value)
neg <- pairs %>% filter(value < 0) %>%
  transmute(from = partner_iso,  to = reporter_iso, value = abs(value))

flows <- bind_rows(pos, neg) %>%
  group_by(from, to) %>%
  summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
  filter(value > 0)

# --- (Optional) keep top nodes; bucket others as "Other" for readability ---
k <- 18  # tweak (or set to length(unique(...)) to keep all)
node_totals <- bind_rows(
  flows %>% summarise(t = sum(value), .by = from) %>% rename(node = from),
  flows %>% summarise(t = sum(value), .by = to)   %>% rename(node = to)
) %>% group_by(node) %>% summarise(t = sum(t), .groups = "drop") %>% arrange(desc(t))
keep <- head(node_totals$node, min(k, nrow(node_totals)))

flows2 <- flows %>%
  mutate(from = if_else(from %in% keep, from, "Other"),
         to   = if_else(to   %in% keep, to,   "Other")) %>%
  group_by(from, to) %>% summarise(value = sum(value), .groups = "drop") %>%
  filter(from != to)

# --- Plot ---
nodes <- sort(unique(c(flows2$from, flows2$to)))
pal   <- setNames(colorRampPalette(brewer.pal(8, "Set3"))(length(nodes)), nodes)
link_lwd <- rescale(flows2$value, to = c(1, 8))
link_col <- adjustcolor(pal[flows2$from], alpha.f = 0.6)

circos.clear()
circos.par(gap.after = rep(3, length(nodes)))
chordDiagram(
  x               = flows2,
  order           = nodes,
  grid.col        = pal,
  transparency    = 0.25,
  directional     = 1,
  direction.type  = c("arrows", "diffHeight"),
  link.arr.type   = "big.arrow",
  link.sort       = TRUE,
  link.decreasing = FALSE,
  link.lwd        = link_lwd,
  col             = link_col,
  annotationTrack = "grid",
  preAllocateTracks = list(track.height = 0.08)
)

circos.trackPlotRegion(track.index = 1, bg.border = NA, panel.fun = function(x, y) {
  sector <- get.cell.meta.data("sector.index")
  xlim   <- get.cell.meta.data("xlim")
  ylim   <- get.cell.meta.data("ylim")
  circos.text(mean(xlim), ylim[1] + 0.1, sector, facing = "clockwise",
              niceFacing = TRUE, adj = c(0, 0.5), cex = 0.7)
})

title(main = sprintf("Allies-only Battery Upstream trade-balance flows (%s)", yr))

#Opportunity Index Version
# install.packages(c("circlize","dplyr","RColorBrewer","scales"))
library(dplyr)
library(circlize)
library(RColorBrewer)
library(scales)

plot_opportunity_chord_topN_capped_highlight <- function(opportunity_all, allies,
                                                         tech_sel    = "Electric Vehicles",
                                                         chain_sel   = "Midstream",
                                                         extra_iso   = NULL,     # e.g. "CHN"
                                                         top_overall = 20,       # total dyads to plot
                                                         per_exporter_cap = 5,   # max per exporter
                                                         ally_alpha  = 0.90,     # alpha when partner is ally
                                                         other_alpha = 0.20,     # alpha otherwise
                                                         max_nodes   = NA,       # optional cap on distinct node labels
                                                         label_cex   = 0.75) {
  
  # Allies set for highlighting (and for restricting reporters)
  ally_iso <- unique(allies$iso3c)
  if (!is.null(extra_iso)) ally_iso <- union(ally_iso, extra_iso)
  
  # 1) Build candidate dyads: reporters must be allies; partners can be anyone
  flows <- opportunity_all %>%
    filter(tech == !!tech_sel,
           supply_chain == !!chain_sel,
           reporter_iso %in% !!ally_iso,
           !is.na(opportunity_index),
           opportunity_index > 0) %>%
    group_by(reporter_iso, partner_iso) %>%
    summarise(value = max(opportunity_index, na.rm = TRUE), .groups = "drop") %>%
    transmute(from = reporter_iso,
              to   = partner_iso,
              value = as.numeric(value),
              partner_is_ally = to %in% !!ally_iso)
  
  if (nrow(flows) == 0L) stop("No flows after filtering; check tech/supply_chain or data.")
  
  # 2) Global ranking, cap per exporter, then take top N overall
  flows_sel <- flows %>%
    arrange(desc(value), from, to) %>%        # global order by value
    group_by(from) %>%                         # per-exporter cap in that global order
    mutate(rank_within_from = row_number()) %>%
    ungroup() %>%
    filter(rank_within_from <= per_exporter_cap) %>%
    select(from, to, value, partner_is_ally) %>%
    slice_head(n = top_overall)
  
  if (nrow(flows_sel) == 0L) stop("Nothing left after applying per-exporter cap.")
  
  # 3) Optional: bucket long tail nodes (but don't drop allies)
  if (!is.null(max_nodes) && is.finite(max_nodes)) {
    node_totals <- bind_rows(
      flows_sel %>% summarise(w = sum(value), .by = from) %>% rename(node = from),
      flows_sel %>% summarise(w = sum(value), .by = to)   %>% rename(node = to)
    ) %>% group_by(node) %>% summarise(w = sum(w), .groups = "drop") %>% arrange(desc(w))
    
    keep_nodes <- unique(c(ally_iso, head(node_totals$node, min(max_nodes, nrow(node_totals)))))
    
    flows_sel <- flows_sel %>%
      mutate(from = if_else(from %in% keep_nodes, from, "Other"),
             to   = if_else(to   %in% keep_nodes, to,   "Other"),
             partner_is_ally = to %in% ally_iso) %>%
      group_by(from, to, partner_is_ally) %>%
      summarise(value = sum(value), .groups = "drop") %>%
      filter(from != to)
  }
  
  # Ensure only one numeric column goes to chordDiagram (controls ribbon width)
  plt_df <- flows_sel %>% select(from, to, value)
  
  # 4) Colors with per-link alpha (bright for ally targets, faint otherwise)
  nodes   <- sort(unique(c(flows_sel$from, flows_sel$to)))
  pal     <- setNames(colorRampPalette(brewer.pal(8, "Set3"))(length(nodes)), nodes)
  base    <- pal[match(flows_sel$from, names(pal))]
  # replace any NAs from matching with a neutral color
  base[is.na(base)] <- "#999999"
  #alpha_vec <- ifelse(flows_sel$partner_isanggih <- NULL; FALSE, FALSE) # placeholder to avoid R CMD check
  alpha_vec <- ifelse(flows_sel$partner_is_ally, ally_alpha, other_alpha)
  # clamp alpha to [0,1] and build RGBA colors robustly
  alpha_vec <- pmin(pmax(alpha_vec, 0), 1)
  link_cols <- scales::alpha(base, alpha = alpha_vec)
  
  # 5) Draw the mini-chord
  circos.clear()
  circos.par(gap.after = rep(3, length(nodes)))
  chordDiagram(
    x               = as.data.frame(plt_df),   # only from, to, value
    order           = nodes,
    grid.col        = pal,
    col             = link_cols,               # per-link colors with alpha
    transparency    = 0,                       # we already encoded alpha in colors
    directional     = 1,
    direction.type  = c("arrows","diffHeight"),
    link.arr.type   = "big.arrow",
    link.sort       = TRUE,
    link.decreasing = FALSE,
    annotationTrack = "grid",
    preAllocateTracks = list(track.height = 0.08)
  )
  
  # Labels (ISO3 by default)
  circos.trackPlotRegion(track.index = 1, bg.border = NA, panel.fun = function(x, y) {
    sec  <- get.cell.meta.data("sector.index")
    xlim <- get.cell.meta.data("xlim")
    ylim <- get.cell.meta.data("ylim")
    circos.text(mean(xlim), ylim[1] + 0.1, sec, facing = "clockwise",
                niceFacing = TRUE, adj = c(0, 0.5), cex = label_cex)
  })
  
  title(main = sprintf("Top %d Export Partnerships - %s %s",
                       top_overall, tech_sel, chain_sel))
}




# Example (no bucketing; keep exactly the top 20 dyads, include China)
plot_opportunity_chord_topN_capped_highlight(
  # or whatever you name it
  opportunity_all, allies,
  tech_sel = "Electric Vehicles", chain_sel = "Midstream",
  top_overall = 25, per_exporter_cap = 10,
  # highlight allies instead of filtering
  extra_iso = NULL,            # or "CHN" to include China among partners
  ally_alpha = 0.9, other_alpha = 0.1,
  max_nodes = NA               # keep exactly the selected dyads
)

