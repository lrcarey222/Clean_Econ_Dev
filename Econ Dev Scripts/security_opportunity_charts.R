library(comtradr)
library(WDI)
library(progress)
set_primary_comtrade_key('2940653b9bbe4671b3f7fde2846d14be')

gdp_data<-WDI(indicator = "NY.GDP.MKTP.CD", start = 2007, end = 2024) 
gdp <- gdp_data %>%           # iso3c, year, NY.GDP.MKTP.CD
  rename(GDP = NY.GDP.MKTP.CD)
gdp_2024 <- gdp %>%
  filter(year=="2024")
##Clean Technology Partnerships-----------------

#Country Cleanup-----------------------------------
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

#Country of interest

coi <- "Japan"
coi_iso3 <- pull(country_info %>% filter(country==coi) %>% distinct(iso3c))

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

res_tech <- res %>%
  # 1) Join; keep the many-to-many if that's expected
  left_join(subcat, by = c("cmd_code" = "code"), relationship = "many-to-many") %>%
  # 2) Clean types + standardize flow label
  mutate(
    primary_value  = as.numeric(primary_value),
    flow_direction = tolower(flow_direction)
  ) %>%
  # 3) Collapse duplicates BEFORE pivot: one value per key x flow
  group_by(
    reporter_iso, partner_iso, partner_desc, period,
    Technology, Value.Chain, Sub.Sector, flow_direction
    # (Optionally drop cmd_desc here to avoid duplicates from multiple descriptions)
  ) %>%
  summarise(primary_value = sum(primary_value, na.rm = TRUE), .groups = "drop") %>%
  # 4) Pivot to export/import; fill missing with 0 (or use NA_real_ if preferred)
  pivot_wider(
    names_from  = flow_direction,
    values_from = primary_value,
    values_fill = 0
  ) %>%
  left_join(gdp %>% filter(year=="2024") %>%
              select(iso3c,GDP),by=c("reporter_iso"="iso3c")) %>%
    # 5) Compute balance
    mutate(trade_balance = export - import,
           trade_balance_share=trade_balance/(export+import)*100,
           trade_balance_gdpshare=trade_balance/GDP*100) %>%
    arrange(desc(trade_balance_gdpshare))


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
# Build exporter???partner trade indices for one country --------------------
compute_trade_exports_for <- function(coi_iso3) {
  res %>%
    filter(reporter_iso == coi_iso3,
           flow_direction == "export",
           !is.na(period)) %>%
    mutate(period = as.integer(period)) %>%
    filter(dplyr::between(period, 2020, 2024)) %>%
    left_join(subcat, by = c("cmd_code" = "code"), relationship = "many-to-many") %>%
    distinct(reporter_iso, partner_iso, partner_desc, period,
             Technology, Value.Chain, Sub.Sector, cmd_desc, primary_value) %>%
    group_by(reporter_iso, partner_iso, Technology, Value.Chain, period) %>%
    summarise(exports = sum(suppressWarnings(as.numeric(primary_value)), na.rm = TRUE),
              .groups = "drop_last") %>%
    # Ensure both years exist per group (missing -> 0) BEFORE pivot
    complete(period = c(2020L, 2024L), fill = list(exports = 0)) %>%
    ungroup() %>%
    pivot_wider(
      names_from  = period,
      values_from = exports,
      names_prefix = "y",          # y2020, y2024
      values_fill = 0,
      values_fn   = sum
    ) %>%
    mutate(
      export_growth = dplyr::if_else(y2020 > 0, y2024 / y2020 - 1, NA_real_),
      exports       = y2024,
      export_index        = median_scurve(exports),
      export_growth_index = median_scurve(export_growth),
      trade_index_raw = {
        w_exp <- 2; w_gro <- 1
        num <- w_exp * export_index + w_gro * export_growth_index
        den <- w_exp * (!is.na(export_index)) + w_gro * (!is.na(export_growth_index))
        dplyr::if_else(den > 0, num / den, NA_real_)
      },
      trade_index = median_scurve(trade_index_raw)
    ) %>%
    filter(!is.na(trade_index)) %>%
    select(reporter_iso, partner_iso, Technology, Value.Chain, trade_index)
}

# Build opportunity index for one country --------------------------------
# Uses: trade_index (exporter???partner), exporter's econ_opp_index per (tech, chain),
# partner's energy_security_index per (tech, chain), plus GHG & climate_policy penalty.

compute_opportunity_for <- function(trade_tbl, coi_iso3, coi_name) {
  # Split tech/supply chain already present-no need to use a combined "industry" field
  # Map partner ISO3 ??? Country name (for joins that use country names)
  trade_tbl %>%
    left_join(all_countries, by = c("partner_iso" = "ISO3166_alpha3")) %>%
    # Exporter's economic opportunity index for this country and (tech, chain)
    left_join(
      econ_opp_index %>%
        ungroup() %>%
        filter(Country == coi_name, variable == "Overall Economic Opportunity Index") %>%
        transmute(tech, supply_chain, econ_opp_index = median_scurve(value)),
      by = c("Technology" = "tech", "Value.Chain" = "supply_chain")
    ) %>%
    # Partner's energy security index (1 - value as in your code)
    inner_join(
      energy_security_index %>%
        ungroup() %>%
        filter(variable == "Overall Energy Security Index") %>%
        transmute(Country, tech, supply_chain,
                  energy_security_index = 1 - value),
      by = c("Country" = "Country",
             "Technology" = "tech",
             "Value.Chain" = "supply_chain")
    ) %>%
    # Climate & GHG
    left_join(tech_ghg %>% rename(Technology = Tech), by = "Technology") %>%
    left_join(
      cat %>%
        mutate(Country = str_to_title(Country)) %>%
        mutate(
          Country = recode(
            Country,
            "Korea, south" = "South Korea",
            "Vietnam"      = "Viet Nam",
            "Turkey"       = "Turkiye",
            "United kingdom" = "United Kingdom",
            "Curacao"      = "Curaçao",
            "Saudi arabia" = "Saudi Arabia",
            "United arab emirates" = "United Arab Emirates"
          )
        ),
      by = c("Country" = "Country")
    ) %>%
    mutate(
      ghg_index            = replace_na(ghg_index,            median(tech_ghg$ghg_index, na.rm = TRUE)),
      climate_policy_index = replace_na(climate_policy_index, median(cat$climate_policy_index, na.rm = TRUE))
    ) %>%
    rowwise() %>%
    mutate(
      opportunity_index_raw = {
        vals <- c(trade_index, econ_opp_index, energy_security_index)
        wts  <- c(2, 2, 0.5)
        keep <- !is.na(vals) & !is.na(wts)
        if (any(keep)) weighted.mean(vals[keep], wts[keep]) else NA_real_
      },
      penalty = (1 - ghg_index) * climate_policy_index,
      opportunity_index_adj = opportunity_index_raw - 0.2 * penalty
    ) %>%
    ungroup() %>%
    mutate(
      opportunity_index = median_scurve(opportunity_index_adj)
    ) %>%
    transmute(
      reporter_iso,
      partner_iso,
      tech = Technology,
      supply_chain = Value.Chain,
      trade_index,
      econ_opp_index,
      energy_security_index,
      ghg_index,
      penalty,
      climate_policy_index,
      opportunity_index
    )
}

# Driver: run for ALL allies and bind ------------------------------------

ally_params <- allies %>% distinct(iso3c, country) %>% arrange(iso3c)

allies_indices <- pmap_dfr(
  ally_params,
  function(iso3c, country) {
    # 1) Export/trade index for this exporter
    t_exports <- compute_trade_exports_for(iso3c)
    
    if (nrow(t_exports) == 0) return(tibble())  # skip if no data
    
    # 2) Opportunity index for this exporter (uses exporter name + partner indices)
    opp <- compute_opportunity_for(t_exports, iso3c, country)
    
    # 3) Stamp the exporter for clarity
    opp %>%
      mutate(exporter_iso = iso3c, exporter_country = country) %>%
      relocate(exporter_iso, exporter_country)
  }
)


# Result: one tidy dataset with all exporters in `allies`
# Columns:
# exporter_iso, exporter_country, reporter_iso (=exporter), partner_iso,
# tech, supply_chain, trade_index, econ_opp_index, energy_security_index,
# ghg_index, climate_policy_index, opportunity_index
glimpse(allies_indices)

#All Allies Export Index---------------

# Safety wrapper around your sigmoid/median scaling
safe_median_scurve <- function(x) {
  if (length(x) == 0 || all(is.na(x))) return(rep(NA_real_, length(x)))
  median_scurve(x)
}

ally_iso <- unique(allies$iso3c)

# 1) Build global dyad-year export table for ALL allies (no per-reporter filter)
build_dyad_exports_all <- function(res, subcat, years = 2020:2024) {
  res %>%
    filter(flow_direction == "export",
           !is.na(period),
           reporter_iso %in% ally_iso) %>%
    mutate(period = as.integer(period)) %>%
    filter(period %in% years) %>%
    left_join(subcat, by = c("cmd_code" = "code"), relationship = "many-to-many") %>%
    # collapse to one number per (reporter, partner, tech, chain, year)
    group_by(reporter_iso, partner_iso, Technology, Value.Chain, period) %>%
    summarise(exports = sum(suppressWarnings(as.numeric(primary_value)), na.rm = TRUE),
              .groups = "drop_last") %>%
    # ensure both endpoints exist in each group before pivoting
    complete(period = c(min(years), max(years)), fill = list(exports = 0)) %>%
    ungroup() %>%
    pivot_wider(
      names_from  = period,
      values_from = exports,
      names_prefix = "y",
      values_fill = 0,
      values_fn   = sum
    ) %>%
    rename(y_first = !!sym(paste0("y", min(years))),
           y_last  = !!sym(paste0("y", max(years)))) %>%
    mutate(
      export_growth = if_else(y_first > 0, (y_last / y_first) - 1, NA_real_),
      exports       = y_last
    )
}

dyads_all <- build_dyad_exports_all(res, subcat, years = 2020:2024)

# 2) Trade indices RELATIVE within each (tech × chain) across ALL reporters/partners
trade_indices_all <- dyads_all %>%
  group_by(Technology, Value.Chain) %>%
  mutate(
    # choose one or both normalizations; here we keep your sigmoid style:
    export_index        = safe_median_scurve(exports),
    export_growth_index = safe_median_scurve(export_growth),
    
    # Weighted (exports x2 + growth x1), NA-safe
    trade_index_raw = {
      w_exp <- 2; w_gro <- 1
      num <- w_exp * export_index + w_gro * export_growth_index
      den <- w_exp * (!is.na(export_index)) + w_gro * (!is.na(export_growth_index))
      if_else(den > 0, num / den, NA_real_)
    },
    
    # Final trade index, rescaled AGAIN within tech×chain to 0-1 (relative)
    trade_index = safe_median_scurve(trade_index_raw)
  ) %>%
  ungroup() %>%
  select(reporter_iso, partner_iso, Technology, Value.Chain,
         exports, export_growth, export_index, export_growth_index, trade_index)

# (Optional) If you prefer percentile-based scaling instead of sigmoid:
# inside the group_by, use:
# export_index        = percent_rank(exports)
# export_growth_index = percent_rank(replace_na(export_growth, -Inf))
# trade_index = scales::rescale(trade_index_raw, to = c(0,1), from = range(trade_index_raw, na.rm=TRUE))

# 3) Opportunity index RELATIVE within each (tech × chain) across ALL dyads
#    - Join exporter name for econ_opp_index
#    - Join partner name for energy_security_index
#    - Compute penalty; rescale within tech×chain at the end.

library(countrycode)
safe_median_scurve <- function(x) {
  if (length(x) == 0 || all(is.na(x))) return(rep(NA_real_, length(x)))
  median_scurve(x)
}
norm_country <- function(x) {
  x <- stringr::str_squish(x)
  dplyr::recode(x,
                "United kingdom" = "United Kingdom",
                "Vietnam" = "Viet Nam",
                "Iran, Islamic Rep." = "Iran",
                "Turkey" = "Turkiye",
                "Korea, south" = "South Korea",
                "United arab emirates" = "United Arab Emirates",
                .default = x
  )
}

# exporter-side economic opportunity (by exporter country, tech, chain)
econ_opp_iso <- econ_opp_index %>%
  mutate(Country = norm_country(Country),
         exporter_iso = countrycode(Country, "country.name", "iso3c",
                                    custom_match = c("Viet Nam"="VNM","Turkiye"="TUR","South Korea"="KOR",
                                                     "Curaçao"="CUW","Laos"="LAO","Czech Republic"="CZE"))) %>%
  filter(grepl("Overall\\s*Economic\\s*Opportunity", variable, ignore.case = TRUE)) %>%
  transmute(exporter_iso,
            Technology = tech,
            `Value.Chain` = supply_chain,
            econ_opp_raw = value)

# partner-side energy security (invert so higher = more attractive)
energy_sec_iso <- energy_security_index %>%
  mutate(Country = norm_country(Country),
         partner_iso = countrycode(Country, "country.name", "iso3c",
                                   custom_match = c("Viet Nam"="VNM","Turkiye"="TUR","South Korea"="KOR",
                                                    "Curaçao"="CUW","Laos"="LAO","Czech Republic"="CZE"))) %>%
  filter(grepl("Overall\\s*Energy\\s*Security", variable, ignore.case = TRUE)) %>%
  transmute(partner_iso,
            Technology = tech,
            `Value.Chain` = supply_chain,
            energy_sec_raw = 1 - value)

# partner-side climate policy index (country-level)
policy_iso <- cat %>%
  mutate(Country = norm_country(stringr::str_to_title(Country)),
         partner_iso = countrycode(Country, "country.name", "iso3c",
                                   custom_match = c("Viet Nam"="VNM","Turkiye"="TUR","South Korea"="KOR",
                                                    "Curaçao"="CUW","Laos"="LAO","Czech Republic"="CZE"))) %>%
  transmute(partner_iso, climate_policy_index)

# tech-level GHG (by Technology)
ghg_iso <- tech_ghg %>% rename(Technology = Tech) %>% select(Technology, ghg_index)

# ---- trade_indices_all must already exist and be relative within (tech, chain)
# columns: reporter_iso, partner_iso, Technology, Value.Chain, trade_index

# ---- build opportunity index (all dyads, ISO-joined)
default_ghg    <- median(ghg_iso$ghg_index, na.rm = TRUE)
default_policy <- median(policy_iso$climate_policy_index, na.rm = TRUE)

opportunity_all <- trade_indices_all %>%
  left_join(econ_opp_iso,  by = c("reporter_iso" = "exporter_iso",
                                  "Technology", "Value.Chain")) %>%
  left_join(energy_sec_iso, by = c("partner_iso",
                                   "Technology", "Value.Chain")) %>%
  left_join(ghg_iso,        by = "Technology") %>%
  left_join(policy_iso,     by = "partner_iso") %>%
  filter(!is.na(econ_opp_raw),
         !is.na(energy_sec_raw)) %>%
  group_by(Technology, Value.Chain) %>%
  mutate(
    econ_opp_index        = safe_median_scurve(econ_opp_raw),
    energy_security_index = safe_median_scurve(energy_sec_raw),
    ghg_index             = coalesce(ghg_index,            default_ghg),
    climate_policy_index  = coalesce(climate_policy_index, default_policy)
  ) %>%
  ungroup() %>%
  mutate(
    # NA-safe weighted mean: (2*T + 2*E + 0.5*S) / (2*1[T] + 2*1[E] + 0.5*1[S])
    num = 2*trade_index + 2*econ_opp_index + 0.5*energy_security_index,
    den = 2*(!is.na(trade_index)) + 2*(!is.na(econ_opp_index)) + 0.5*(!is.na(energy_security_index)),
    opportunity_raw = dplyr::if_else(den > 0, num / den, NA_real_),
    
    penalty = (1 - ghg_index) * climate_policy_index,
    opportunity_index_raw = pmax(0, opportunity_raw - 0.2 * penalty)
  ) %>%
  group_by(Technology, Value.Chain) %>%
  mutate(opportunity_index = safe_median_scurve(opportunity_index_raw)) %>%
  ungroup() %>%
  select(reporter_iso, partner_iso,
         tech = Technology, supply_chain = `Value.Chain`,
         trade_index, econ_opp_index, energy_security_index,
         ghg_index, climate_policy_index,penalty, opportunity_raw,opportunity_index_raw,opportunity_index) %>%
  #filter(partner_iso %in% allies$iso3c) %>%
  arrange(desc(opportunity_index_raw))

#Total Friendshore Index------------------------
library(countrycode)
library(scales)

safe_scurve <- function(x) if (length(x)==0 || all(is.na(x))) rep(NA_real_, length(x)) else median_scurve(x)

norm_country <- function(x) {
  x <- str_squish(x)
  recode(x,
         "United kingdom"="United Kingdom",
         "Vietnam"="Viet Nam",
         "Iran, Islamic Rep."="Iran",
         "Iran, Islamic Republic of"="Iran",
         "Turkey"="Turkiye",
         "Türkiye, Republic of"="Turkiye",
         "Korea, Republic of"="South Korea",
         "Korea, Dem. People's Rep."="Korea, Dem. People's Rep.",
         "United arab republic"="United Arab Republic",
         "United arab emirates"="United Arab Emirates",
         "Czechia"="Czech Republic",
         "Venezuela, RB"="Venezuela",
         .default = x
  )
}

ally_iso <- unique(allies$iso3c)

# ---- 0) Outbound FDI dyads ??? iso3 
outbound_edges <-
  if (exists("outbound") && all(c("COUNTRY","COUNTERPART_COUNTRY","outbound_index") %in% names(outbound))) {
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

# ---- 1) Build import-side dyads and import indices (2× level + 1× growth)
build_dyad_imports_all <- function(res, subcat, years = 2020:2024, restrict_reporters = NULL) {
  res %>%
    filter(flow_direction == "import",
           !is.na(period),
           if (!is.null(restrict_reporters)) reporter_iso %in% restrict_reporters else TRUE) %>%
    mutate(period = as.integer(period)) %>%
    filter(period %in% years) %>%
    left_join(subcat, by = c("cmd_code"="code"), relationship = "many-to-many") %>%
    group_by(reporter_iso, partner_iso, Technology, `Value.Chain`, period) %>%
    summarise(imports = sum(suppressWarnings(as.numeric(primary_value)), na.rm = TRUE),
              .groups = "drop_last") %>%
    complete(period = c(min(years), max(years)), fill = list(imports = 0)) %>%
    ungroup() %>%
    pivot_wider(names_from = period, values_from = imports, names_prefix = "y", values_fill = 0) %>%
    mutate(import_2024   = .data[[paste0("y", max(years))]],
           import_0      = .data[[paste0("y", min(years))]],
           import_growth = if_else(is.finite(import_0) & import_0 > 0,
                                   (import_2024 / import_0) - 1, NA_real_))
}

imports_all <- build_dyad_imports_all(res, subcat, years = 2020:2024, restrict_reporters = NULL)

import_idx_all <- imports_all %>%
  group_by(Technology, `Value.Chain`) %>%
  mutate(
    import_index      = safe_scurve(as.numeric(import_2024)),
    import_growth_idx = safe_scurve(as.numeric(import_growth)),
    imp_trd_num       = 2 * import_index + coalesce(import_growth_idx, 0),
    imp_trd_den       = 2 * as.numeric(!is.na(import_index)) + 1 * as.numeric(!is.na(import_growth_idx)),
    imp_trade_index   = if_else(imp_trd_den > 0, imp_trd_num / imp_trd_den, NA_real_),
    imp_trade_index_z = safe_scurve(imp_trade_index)
  ) %>%
  ungroup() %>%
  transmute(
    reporter_iso, partner_iso,
    tech = Technology, supply_chain = `Value.Chain`,
    import_2024, import_growth,
    import_index, import_growth_idx,
    imp_trade_index,            # raw weighted mean (2× level + 1× growth)
    imp_trade_index_z           # 0-1 rescaled within tech×chain
  )

# ---- 2) Country/sector modifiers (ES need for importer, EO for partner, CPI, GHG)
es_iso <- energy_security_index %>%
  mutate(Country = norm_country(Country),
         iso3c   = countrycode(Country, "country.name", "iso3c",
                               custom_match = c("Viet Nam"="VNM","Turkiye"="TUR",
                                                "South Korea"="KOR","Curaçao"="CUW","Laos"="LAO",
                                                "Czech Republic"="CZE"))) %>%
  filter(grepl("Overall\\s*Energy\\s*Security", variable, ignore.case = TRUE)) %>%
  transmute(reporter_iso = iso3c, tech = tech, supply_chain = supply_chain,
            es_need = 1 - value)

eo_partner_iso <- econ_opp_index %>%
  mutate(Country = norm_country(Country),
         iso3c   = countrycode(Country, "country.name", "iso3c",
                               custom_match = c("Viet Nam"="VNM","Turkiye"="TUR",
                                                "South Korea"="KOR","Curaçao"="CUW","Laos"="LAO",
                                                "Czech Republic"="CZE"))) %>%
  filter(grepl("Overall\\s*Economic\\s*Opportunity", variable, ignore.case = TRUE)) %>%
  transmute(partner_iso = iso3c, tech = tech, supply_chain = supply_chain,
            eo_partner = value)

policy_iso <- cat %>%
  mutate(Country = norm_country(str_to_title(Country %||% "")),
         iso3c   = countrycode(Country, "country.name", "iso3c",
                               custom_match = c("Viet Nam"="VNM","Turkiye"="TUR",
                                                "South Korea"="KOR","Curaçao"="CUW","Laos"="LAO",
                                                "Czech Republic"="CZE"))) %>%
  transmute(iso3c, climate_policy = climate_policy_index)

# Robustly pick tech column for GHG table
ghg_tech_col <- if ("Tech" %in% names(tech_ghg)) "Tech" else if ("tech" %in% names(tech_ghg)) "tech" else stop("`tech_ghg` must have column 'Tech' or 'tech'.")
ghg_iso <- tech_ghg %>% transmute(tech = .data[[ghg_tech_col]], ghg_index = as.numeric(ghg_index))

# Defaults if some countries/sectors are missing
default_ghg <- if (nrow(ghg_iso)) median(ghg_iso$line %||% ghg_iso$ghg_index, na.rm = TRUE) else 0.5
default_cpi <- if (nrow(policy_iso)) median(policy_iso$climate_policy,         na.rm = TRUE) else 0.5

# ---- 3) Friend-Shore Index (relative to ALL countries; scale within tech×chain) 
friendshore_all <- import_idx_all %>%
  left_join(es_iso,        by = c("tech","supply_chain","reporter_iso")) %>%
  left_join(eo_partner_iso, by = c("tech","supply_chain","partner_iso")) %>%
  left_join(outbound_edges, by = c("reporter_iso","partner_iso")) %>%
  left_join(rename(policy_iso, reporter_iso = iso3c, cpi_r = climate_policy), by = "reporter_iso") %>%
  left_join(rename(policy_iso, partner_iso  = iso3c, cpi_p = climate_policy),  by = "partner_iso") %>%
left_join(ghg_iso,       by = "tech") %>%
  mutate(cpi_r          = coalesce(cpi_r, default_cpi),
    cpi_p          = coalesce(cpi_p, default_cpi),
    gh_entry       = coalesce(ghg_index, default_ghg)
  ) %>%
  mutate(
    fsi_raw = imp_trade_index +
      es_need + eo_partner + outbound_index,
    penalty_r = (1 - gh_entry) * cpi_r * 0.10,
    penalty_p = (1 - gh_entry) * cpi_p * 0.10,
    fsi_adj   = pmax(0, fsi_raw - (penalty_r + penalty_p))
  ) %>%
  group_by(tech, supply_chain) %>%
  mutate(friendshore_index = safe_scurve(fsi_adj)) %>%
  ungroup()

# ---- 4) Friend-Shore Index per-exporter (scale within each reporter×tech×chain) 
friendshore_by_exporter <- import_idx_all %>%
  semi_join(tibble(reporter_iso = ally_iso), by = "reporter_iso") %>%
  left_join(es_iso,        by = c("tech","supply_chain","reporter_iso")) %>%
  left_join(eo_partner_iso, by = c("tech","supply_chain","partner_iso")) %>%
  left_join(outbound_edges, by = c("reporter_iso","partner_iso")) %>%
  left_join(rename(policy_iso, reporter_iso = iso3c, cpi_r = climate_policy), by = "reporter_iso") %>%
  left_join(rename(policy_iso, partner_iso  = iso3c, cpi_p = climate_policy),  by = "partner_iso") %>%
left_join(ghg_iso,       by = "tech") %>%
  mutate(cpi_r          = coalesce(cpi_r, default_cpi),
    cpi_p          = coalesce(cpi_p, default_cpi),
    gh_entry       = coalesce(ghg_index, default_ghg),
    fsi_raw        = 2*imp_trade_index +
      2*es_need + 2*eo_partner + 2*outbound_index,
    penalty_r      = (1 - gh_entry) * cpi_r * 0.10,
    penalty_p      = (1 - gh_entry) * cpi_p * 0.10,
    fsi_adj        = pmax(0, fsi_raw - (penalty_r + penalty_p))
  ) %>%
group_by(reporter_iso) %>%
  mutate(friendshore_index = safe_scurve(fsi_adj)) %>%
  ungroup() %>%
  left_join(select(country_info, iso3c, country), by = c("reporter_iso" = "iso3c")) %>%
              rename(exporter_country = country) %>%
              select(reporter_iso, exporter_country, partner_iso,
                     tech, supply_chain,
                     imp_trade_index, es_need, eo_partner, outbound_index,
                     penalty_r, penalty_p, fsi_raw, fsi_adj, friendshore_index)
            


res_list <- purrr::cross_df(list(
  win = year_windows,
  cc  = code_chunks
)) %>%
  dplyr::mutate(
    out = purrr::pmap(
      list(win, cc), \(win, cc) {
        Sys.sleep(0.4)
        safe_ct(
          reporter       = allies$iso3c,
          partner        = country_info_iso$iso3c,
          commodity_code = cc,
          start_date     = win[1],
          end_date       = win[2],
          flow_direction = "import"
        )
      }
    )
  ) %>%
  dplyr::pull(out)

energy_imports<-res_list %>%
  discard(is.null) %>%
  dplyr::bind_rows()

imports <- res %>%
  filter(reporter_iso==coi_iso,
         flow_direction=="import") %>%
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
  slice_max(order_by=friendshore_index,n=3) %>%
  summarize(total_friendshore_index=mean(friendshore_index,na.rm=T)) %>%
  ungroup() %>%
  #mutate(total_friendshore_index=median_scurve(total_friendshore_index)) %>%
  arrange(desc(total_friendshore_index))


friendshore_scatter<-friendshore_index %>%
  ungroup() %>%
  mutate(country_industry=paste0(country.y,": ",industry))%>%
  select(country_industry,energy_security_index,econ_opp_index2) %>%
  mutate(energy_security_index=median_scurve(energy_security_index))


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
  tech_sel = "Electric Grid", chain_sel = "Midstream",
  top_overall = 10, per_exporter_cap = 5,
  # highlight allies instead of filtering
  extra_iso = NULL,            # or "CHN" to include China among partners
  ally_alpha = 0.9, other_alpha = 0.1,
  max_nodes = NA               # keep exactly the selected dyads
)

