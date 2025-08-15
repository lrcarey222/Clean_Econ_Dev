# =========================
# CLEAN INVESTMENT MONITOR (CIM) x GOOD JOBS FIRST (GJF)
# Top-100 facility geocoding + automatic subsidy matching + single wide export
#
# UPDATES (Aug 2025):
#  - Top 100 facilities by CAPEX (announced since Aug 2020)
#  - Automatic matching (no human approvals); keep only Medium & High confidence
#  - Final export ONLY: top_75_facilities_subsidy_matches_ACCEPTED_WIDE_geocoded.csv
#    (filename preserved for downstream compatibility)
#  - Each matched subsidy row in the wide export carries EVERY original column
#    from GJF_SUBSIDY_DATA (including links in "Source of Data"), plus match signals
# =========================

suppressPackageStartupMessages({
  library(tidyverse)   # dplyr, tidyr, readr, purrr, tibble, stringr
  library(stringdist)  # stringsim()
  library(geosphere)   # distHaversine()
  library(lubridate)
  library(readxl)
  library(tigris)      # counties(), CBSA/CSA, fips_codes
  library(sf)
  library(httr)
  library(jsonlite)
})

options(
  tigris_use_cache = TRUE,
  tigris_class     = "sf",
  readr.show_col_types = FALSE
)
sf::sf_use_s2(FALSE)  # planar ops to reduce s2 warnings

CRS_GEO  <- 4326   # WGS84 (lon/lat)
CRS_PROJ <- 5070   # NAD83 / Conus Albers (area-preserving for polygon tests)

# ==============================================================================
# DEBUGGING HELPERS
# ==============================================================================
debug_print <- function(msg, obj = NULL) {
  cat(paste0("\n[DEBUG] ", msg, "\n"))
  if (!is.null(obj)) {
    if (is.data.frame(obj) || inherits(obj, "tbl_df")) {
      cat("  Dimensions:  ", nrow(obj), "  x   ", ncol(obj), "\n")
      cat("  Columns:   ", paste(names(obj), collapse=", "), "\n")
      print(utils::head(obj, 3))
    } else {
      print(obj)
    }
  }
}

timed <- function(label, expr) {
  t0 <- Sys.time()
  on.exit(cat(sprintf("[TIMER] %s lookup: %.2fs\n", label, as.numeric(difftime(Sys.time(), t0, units="secs")))))
  force(expr)
}

# ==============================================================================
# GENERAL HELPERS
# ==============================================================================
topk_by <- function(df, by, k, group_vars = NULL) {
  if (!is.null(group_vars) && length(group_vars) > 0) {
    df %>%
      group_by(across(all_of(group_vars))) %>%
      slice_max(order_by = {{ by }}, n = k, with_ties = FALSE, na_rm = TRUE) %>%
      ungroup()
  } else {
    df %>% slice_max(order_by = {{ by }}, n = k, with_ties = FALSE, na_rm = TRUE)
  }
}

valid_lonlat <- function(lon, lat) {
  suppressWarnings({
    lon <- as.numeric(lon); lat <- as.numeric(lat)
    !is.na(lon) & !is.na(lat) & lon >= -180 & lon <= 180 & lat >= -90 & lat <= 90
  })
}

nzchr <- function(x) ifelse(is.na(x) | trimws(x) == "", NA_character_, x)

`%||%` <- function(a, b) {
  if (is.null(a) || length(a) == 0) return(b)
  if (length(a) > 1) return(a)
  if (is.na(a)) return(b)
  if (is.character(a) && a == "") return(b)
  a
}

fmt_usd <- function(x) {
  if (is.null(x) || length(x) == 0) return("NA")
  if (all(is.na(x))) return("NA")
  tryCatch({
    paste0("$", format(round(as.numeric(x), 0), big.mark = ",", trim = TRUE, scientific = FALSE))
  }, error = function(e) "NA")
}

parse_date_safely <- function(x) {
  x_chr <- as.character(x)
  out <- suppressWarnings(lubridate::parse_date_time(
    x_chr,
    orders = c("mdy","mdY","ymd","Ymd","dmy","b d Y","B d Y","m/d/y","m/d/Y"),
    tz = "UTC"
  ))
  as.Date(out)
}

# Ensure missing character columns exist to avoid "object not found" in mutate()
ensure_chr_cols <- function(df, cols) {
  for (nm in cols) if (!nm %in% names(df)) df[[nm]] <- NA_character_
  df
}

# ==============================================================================
# NAME NORMALIZATION & TOKENIZATION
# ==============================================================================
.corp_suffix_re   <- "(?i)\\b(incorporated|inc|corporation|corp|company|co|limited|ltd|llc|l\\.l\\.c\\.|plc|lp|llp|gmbh|s\\.a\\.?|s\\.p\\.a\\.?|nv|ag)\\b"
.state_tail_re    <- "(?i)\\b(usa|u\\.s\\.|us|america|north america|na)\\b"
.common_noise_re  <- "(?i)\\b(holdings?|international|intl|associates?|assoc|group|industr(y|ies)|manufacturing|mfg|solutions?|global|america(ns)?)\\b"
.dba_re           <- "(?i)\\b(d/b/a|dba|formerly known as|fka|aka|a/k/a)\\b"

normalize_company_name <- function(x) {
  x <- tolower(dplyr::coalesce(x, ""))
  x <- gsub("\\([^\\)]*\\)", " ", x)
  x <- gsub(.dba_re, " ", x, perl = TRUE)
  x <- gsub("[&/+,._-]", " ", x)
  x <- gsub("[0-9]+", " ", x)
  x <- gsub(.corp_suffix_re,  " ", x, perl = TRUE)
  x <- gsub(.state_tail_re,   " ", x, perl = TRUE)
  x <- gsub(.common_noise_re, " ", x, perl = TRUE)
  x <- gsub("\\b(the|an|a)\\b", " ", x)
  x <- gsub("[^a-z\\s]", " ", x)
  stringr::str_squish(x)
}

tokenize <- function(x) {
  if (is.null(x) || all(is.na(x))) return(character(0))
  x <- tolower(dplyr::coalesce(x, ""))
  toks <- unlist(strsplit(x, "\\s+"))
  toks[nchar(toks) >= 2]
}

token_jaccard <- function(a, b) {
  if (length(a) == 0 || length(b) == 0) return(0)
  inter <- length(intersect(a, b))
  union <- length(union(a, b))
  if (union == 0) 0 else inter / union
}

acronym_from_tokens <- function(toks) {
  if (length(toks) == 0) return("")
  paste0(substr(toks, 1, 1), collapse = "")
}

stringsim_vec <- function(a, b, method = "jw", ...) {
  a <- dplyr::coalesce(a, ""); b <- dplyr::coalesce(b, "")
  stopifnot(length(a) == length(b))
  empty <- (a == "" | b == "")
  out <- numeric(length(a))
  if (any(!empty)) {
    out[!empty] <- stringdist::stringsim(a[!empty], b[!empty], method = method, ...)
  }
  out
}

# ==============================================================================
# COUNTY NORMALIZATION & LOOKUP
# ==============================================================================
normalize_county_name <- function(x) {
  y <- tolower(dplyr::coalesce(x, ""))
  y <- gsub("\\b(city and county of|city and borough of|city of|county of)\\b", "", y)
  y <- gsub("\\b(county|parish|borough|municipio|census area|city and borough|municipality)\\b", "", y)
  y <- gsub("\\b(st\\.?|ste\\.?|saint)\\b", "saint", y)
  y <- gsub("[^a-z\\s]", " ", y)
  stringr::str_squish(y)
}

get_state_county_lookup <- function(year = 2020) {
  timed("tigris::counties", {
    ct <- try(tigris::counties(cb = TRUE, year = year, progress_bar = FALSE), silent = TRUE)
    if (inherits(ct, "try-error")) stop("Failed to download counties.")
    if (is.na(sf::st_crs(ct))) sf::st_crs(ct) <- CRS_GEO
    
    st_map <- tigris::fips_codes %>%
      distinct(state_code, state_abbr) %>%
      mutate(state_code = stringr::str_pad(as.character(state_code), 2, pad = "0"))
    
    ct %>%
      sf::st_transform(CRS_GEO) %>%
      mutate(
        county_norm = normalize_county_name(NAME),
        STATEFP = stringr::str_pad(as.character(STATEFP), 2, pad = "0")
      ) %>%
      left_join(st_map, by = c("STATEFP" = "state_code")) %>%
      mutate(
        county_centroid = suppressWarnings(sf::st_point_on_surface(geometry)),
        county_lon = suppressWarnings(sf::st_coordinates(county_centroid)[,1]),
        county_lat = suppressWarnings(sf::st_coordinates(county_centroid)[,2]),
        state = dplyr::coalesce(state_abbr, NA_character_)
      ) %>%
      sf::st_drop_geometry() %>%
      select(state, county_name = NAME, county_norm, county_fips = GEOID, county_lon, county_lat)
  })
}

# ==============================================================================
# PROGRAM KEYWORDS & ALIASES
# ==============================================================================
tech_synonyms <- function(tech, subcat = NULL, sector = NULL) {
  tech <- tolower(dplyr::coalesce(tech, ""))
  subcat <- tolower(dplyr::coalesce(subcat, ""))
  sector <- tolower(dplyr::coalesce(sector, ""))
  base <- c()
  if (stringr::str_detect(tech, "batter")) base <- c(base, "battery","batteries","cell","cells","lithium","anode","cathode","module","pack","gigafactory")
  if (stringr::str_detect(tech, "vehicle") || stringr::str_detect(subcat, "vehicle")) base <- c(base, "ev","electric","automotive","auto","vehicle","assembly","plant")
  if (stringr::str_detect(tech, "semiconductor") || stringr::str_detect(sector, "micro") || stringr::str_detect(sector, "chip")) base <- c(base, "semiconductor","chip","fab","foundry","wafer","microelectronics")
  if (stringr::str_detect(tech, "solar") || stringr::str_detect(subcat, "pv")) base <- c(base, "solar","pv","photovoltaic","module","panel")
  if (stringr::str_detect(tech, "wind")) base <- c(base, "wind","turbine","blade","nacelle","tower","offshore")
  if (stringr::str_detect(tech, "hydrogen") || stringr::str_detect(subcat, "electrolyzer") || stringr::str_detect(subcat, "ammonia")) base <- c(base, "hydrogen","electrolyzer","ammonia","h2","fuel","fuelcell")
  if (stringr::str_detect(sector, "steel") || stringr::str_detect(subcat, "steel")) base <- c(base, "steel","mill","blast","arc","furnace")
  unique(base)
}

CALIB <- list(
  w_jw   = 0.62, w_cos  = 0.23, w_jacc = 0.15,
  geo_weight_county = 0.25, geo_weight_cbsa = 0.10,
  acronym_bonus_with_county = 0.25, acronym_bonus_with_cbsa = 0.15, acronym_bonus_plain = 0.10,
  kw_per_token = 0.02, kw_per_synonym = 0.04, kw_cap = 0.12, learned_kw_per = 0.01,
  temporal_bonus = c(close=0.20, good=0.10, ok=0.05, far=0.00, bad=-0.10),
  year_window_hard = 7, year_window_soft = 5,
  distance_prefilter_km = 200,
  distance_bonus_km   = c(25, 50, 100),
  distance_bonus_vals = c(0.15, 0.10, 0.05),
  ratio_bonus = c(lo = 0.0001, hi = 0.25, bonus = 0.05),
  gov_recipient_penalty = -0.05,
  jw_hard_min = 0.85, def_jw_min = 0.96, high_total_min = 0.90, med_total_min = 0.80, low_total_min = 0.66
)

LEARNED_PROGRAM_KW <- c(
  "megadeal","multiple","abatement","industrial","exemption","property",
  "rebate","reimbursement","business","investment","customized","fasttrack",
  "kentucky","bluegrass","skills","corporation","grant-in-aid","site","assistance","invest"
)

ALIAS_TABLE <- tibble::tibble(
  variant   = c("blueoval sk","envision aesc","equinor wind","ford motor","general motors",
                "gotion","hyundai motor","lg energy","linde","panasonic","redwood materials",
                "rivian","scout motors","toyota","ultium cells"),
  canonical = c("blueovalsk","envision aesc","equinor wind","ford motor","general motors",
                "gotion","hyundai motor lges","lg energy","linde","panasonic","redwood materials",
                "rivian","scout motors","toyota","ultium cells")
)
apply_alias_norm <- function(norm_vec) {
  repl <- ALIAS_TABLE$canonical[match(norm_vec, ALIAS_TABLE$variant)]
  ifelse(is.na(repl), norm_vec, repl)
}

debug_print("Embedded CALIB (from snapshots)", CALIB)
debug_print("Embedded alias table", ALIAS_TABLE)
debug_print("Embedded learned program keywords", head(LEARNED_PROGRAM_KW, 10))

# ==============================================================================
# LOAD CIM DATA
# ==============================================================================
cat("\n================================================================================\n")
cat("LOADING CIM DATA\n")
cat("================================================================================\n")

CIM_DATA <- read_csv(
  "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/clean_investment_monitor_q1_2025/extended_data/manufacturing_energy_and_industry_facility_metadata.csv",
  skip = 5
) %>% distinct(unique_id, .keep_all = TRUE)

debug_print("CIM_DATA loaded", CIM_DATA)

CIM_DATA_IRA_TIMELINE <- CIM_DATA %>%
  mutate(
    Announcement_Date = parse_date_safely(Announcement_Date),
    Announced_Post_IRA          = if_else(!is.na(Announcement_Date) & Announcement_Date >= as.Date("2022-08-16"), "Yes", "No"),
    Announced_Since_2022        = if_else(!is.na(Announcement_Date) & Announcement_Date >= as.Date("2022-01-01"), "Yes", "No"),
    Announced_Since_August_2020 = if_else(!is.na(Announcement_Date) & Announcement_Date >= as.Date("2020-08-01"), "Yes", "No")
  )

debug_print("CIM_DATA_IRA_TIMELINE created", CIM_DATA_IRA_TIMELINE)
glimpse(CIM_DATA_IRA_TIMELINE)

# ==============================================================================
# TOP 5 BY STATE (kept for reference; not exported)
# ==============================================================================
cat("\n================================================================================\n")
cat("CREATING TOP 5 BY STATE DATA FRAMES\n")
cat("================================================================================\n")

base_cols <- c(
  "State","Estimated_Total_Facility_CAPEX","unique_id","Decarb_Sector","Company",
  "Technology","Subcategory","Current_Facility_Status","Announcement_Date",
  "Announced_Post_IRA","Announced_Since_2022","Announced_Since_August_2020"
)

top_5_by_state_all <- CIM_DATA_IRA_TIMELINE %>%
  topk_by(Estimated_Total_Facility_CAPEX, 5, group_vars = "State") %>%
  arrange(State, desc(Estimated_Total_Facility_CAPEX)) %>%
  select(all_of(base_cols))

top_5_by_state_post_ira <- CIM_DATA_IRA_TIMELINE %>%
  filter(Announced_Post_IRA == "Yes",
         !Current_Facility_Status %in% c("Retired","Canceled prior to operation")) %>%
  topk_by(Estimated_Total_Facility_CAPEX, 5, group_vars = "State") %>%
  arrange(State, desc(Estimated_Total_Facility_CAPEX)) %>%
  select(all_of(base_cols))

top_5_by_state_since_2022 <- CIM_DATA_IRA_TIMELINE %>%
  filter(Announced_Since_2022 == "Yes",
         !Current_Facility_Status %in% c("Retired","Canceled prior to operation")) %>%
  topk_by(Estimated_Total_Facility_CAPEX, 5, group_vars = "State") %>%
  arrange(State, desc(Estimated_Total_Facility_CAPEX)) %>%
  select(all_of(base_cols))

top_5_by_state_since_aug_2020 <- CIM_DATA_IRA_TIMELINE %>%
  filter(Announced_Since_August_2020 == "Yes",
         !Current_Facility_Status %in% c("Retired","Canceled prior to operation")) %>%
  topk_by(Estimated_Total_Facility_CAPEX, 5, group_vars = "State") %>%
  arrange(State, desc(Estimated_Total_Facility_CAPEX)) %>%
  select(all_of(base_cols))

# ==============================================================================
# TOP 100 ACROSS ALL STATES (since Aug 2020)
# ==============================================================================
cat("\n================================================================================\n")
cat("CREATING TOP 100 FACILITIES ACROSS ALL STATES\n")
cat("================================================================================\n")

top_100_cols <- c(base_cols, "LatLon_Valid","Latitude","Longitude","Address")

top_100_all_states_since_aug_2020 <- CIM_DATA_IRA_TIMELINE %>%
  filter(Announced_Since_August_2020 == "Yes",
         !Current_Facility_Status %in% c("Retired","Canceled prior to operation")) %>%
  topk_by(Estimated_Total_Facility_CAPEX, 100) %>%
  arrange(desc(Estimated_Total_Facility_CAPEX)) %>%
  select(all_of(top_100_cols))

debug_print("top_100_all_states_since_aug_2020 created", top_100_all_states_since_aug_2020)
glimpse(top_100_all_states_since_aug_2020)

# ==============================================================================
# GEOCODING TOP 100 FOR COUNTY, CBSA, CSA, OPPORTUNITY ZONES
# ==============================================================================
cat("\n================================================================================\n")
cat("GEOCODING TOP 100 FACILITIES FOR COUNTY, CBSA, CSA, AND OPPORTUNITY ZONES\n")
cat("================================================================================\n\n")

geocode_with_coordinates <- function(fac_sf_proj, counties_proj, cbsa_proj, csa_proj) {
  suppressMessages({
    counties_proj <- sf::st_make_valid(counties_proj)
    cbsa_proj     <- sf::st_make_valid(cbsa_proj)
    csa_proj      <- sf::st_make_valid(csa_proj)
    
    f_cnty <- sf::st_join(fac_sf_proj, counties_proj[, c("GEOID","NAME","geometry")], join = sf::st_within, left = TRUE) %>%
      rename(county_fips = GEOID, county_name = NAME)
    f_cbsa <- sf::st_join(f_cnty, cbsa_proj[, c("GEOID","NAME","geometry")], join = sf::st_within, left = TRUE) %>%
      rename(cbsa_code = GEOID, cbsa_name = NAME)
    f_csa  <- sf::st_join(f_cbsa, csa_proj[,  c("GEOID","NAME","geometry")], join = sf::st_within, left = TRUE) %>%
      rename(csa_code = GEOID, csa_name = NAME)
    
    sf::st_drop_geometry(f_csa)
  })
}

get_opportunity_zones <- function() {
  cat("Downloading opportunity zone boundaries from ArcGIS...\n")
  base_url <- "https://services5.arcgis.com/JIM0C93ugEcJ7iyY/ArcGIS/rest/services/oz_shapefile_final_version/FeatureServer/0/query"
  all_features <- list(); offset <- 0; record_count <- 2000
  
  repeat {
    params <- list(
      where = "oz_lgbl = 'OZ eligible'",
      outFields = "*",
      f = "geojson",
      returnGeometry = "true",
      resultOffset = offset,
      resultRecordCount = record_count
    )
    resp <- try(httr::GET(base_url, query = params), silent = TRUE)
    if (inherits(resp, "try-error") || is.null(resp) || is.na(httr::status_code(resp))) {
      cat("  Warning: ArcGIS request failed; skipping OZ overlay.\n")
      return(NULL)
    }
    if (httr::status_code(resp) == 200) {
      txt <- httr::content(resp, as = "text", encoding = "UTF-8")
      oz_batch <- try(sf::st_read(txt, quiet = TRUE), silent = TRUE)
      if (inherits(oz_batch, "try-error")) {
        cat("  Warning: Could not parse OZ geojson; skipping OZ overlay.\n")
        return(NULL)
      }
      if (nrow(oz_batch) == 0) break
      all_features[[length(all_features) + 1]] <- oz_batch
      cat(sprintf("  Downloaded %d features (batch %d)\n", nrow(oz_batch), length(all_features)))
      if (nrow(oz_batch) < record_count) break
      offset <- offset + record_count
    } else {
      cat("  Warning: ArcGIS status", httr::status_code(resp), "- skipping OZ overlay.\n")
      return(NULL)
    }
  }
  
  if (length(all_features) == 0) return(NULL)
  oz <- do.call(rbind, all_features)
  sf::st_make_valid(oz)
}

cat("Step 1: Geocoding top 100 facilities with coordinates...\n")
facilities_with_coords <- top_100_all_states_since_aug_2020 %>%
  mutate(
    Longitude = suppressWarnings(as.numeric(Longitude)),
    Latitude  = suppressWarnings(as.numeric(Latitude)),
    LatLon_Valid = dplyr::coalesce(LatLon_Valid, FALSE)
  ) %>%
  filter(LatLon_Valid | valid_lonlat(Longitude, Latitude))

cat("Facilities with valid coordinates:", nrow(facilities_with_coords), "\n")

if (nrow(facilities_with_coords) > 0) {
  counties_sf <- try(tigris::counties(cb = TRUE, year = 2020, progress_bar = FALSE), silent = TRUE)
  cbsa_sf     <- try(tigris::core_based_statistical_areas(cb = TRUE, year = 2020, progress_bar = FALSE), silent = TRUE)
  csa_sf      <- try(tigris::combined_statistical_areas(cb = TRUE, year = 2020, progress_bar = FALSE), silent = TRUE)
  
  if (!(inherits(counties_sf,"try-error") | inherits(cbsa_sf,"try-error") | inherits(csa_sf,"try-error"))) {
    if (is.na(sf::st_crs(counties_sf))) sf::st_crs(counties_sf) <- CRS_GEO
    if (is.na(sf::st_crs(cbsa_sf)))     sf::st_crs(cbsa_sf)     <- CRS_GEO
    if (is.na(sf::st_crs(csa_sf)))      sf::st_crs(csa_sf)      <- CRS_GEO
    
    facilities_sf   <- sf::st_as_sf(facilities_with_coords, coords = c("Longitude","Latitude"), crs = CRS_GEO, remove = FALSE)
    facilities_proj <- sf::st_transform(facilities_sf, CRS_PROJ)
    counties_proj   <- sf::st_transform(counties_sf, CRS_PROJ)
    cbsa_proj       <- sf::st_transform(cbsa_sf, CRS_PROJ)
    csa_proj        <- sf::st_transform(csa_sf, CRS_PROJ)
    
    n_chunks <- max(1, min(10, nrow(facilities_proj)))
    chunk_sz <- ceiling(nrow(facilities_proj) / n_chunks)
    out_list <- vector("list", n_chunks)
    
    cat("Processing geocoding...\n")
    pb <- txtProgressBar(min = 0, max = n_chunks, style = 3)
    for (i in seq_len(n_chunks)) {
      s <- (i - 1) * chunk_sz + 1
      e <- min(i * chunk_sz, nrow(facilities_proj))
      if (s <= nrow(facilities_proj)) {
        out_list[[i]] <- geocode_with_coordinates(facilities_proj[s:e, ], counties_proj, cbsa_proj, csa_proj)
      }
      setTxtProgressBar(pb, i)
    }
    close(pb)
    
    geocoded_data <- bind_rows(out_list) %>% distinct(unique_id, .keep_all = TRUE)
    
    top_100_all_states_since_aug_2020 <- top_100_all_states_since_aug_2020 %>%
      left_join(geocoded_data %>%
                  select(unique_id, county_fips, county_name, cbsa_code, cbsa_name, csa_code, csa_name),
                by = "unique_id")
    cat("\nGeocoding with coordinates complete.\n")
    
    cat("\nStep 2: Checking for opportunity zone locations...\n")
    oz_boundaries <- get_opportunity_zones()
    if (!is.null(oz_boundaries)) {
      oz_proj <- try(sf::st_transform(oz_boundaries, CRS_PROJ), silent = TRUE)
      if (!inherits(oz_proj, "try-error")) {
        n_chunks <- max(1, min(10, nrow(facilities_proj)))
        chunk_sz <- ceiling(nrow(facilities_proj) / n_chunks)
        oz_list <- vector("list", n_chunks)
        
        cat("Checking opportunity zone overlap...\n")
        pb <- txtProgressBar(min = 0, max = n_chunks, style = 3)
        for (i in seq_len(n_chunks)) {
          s <- (i - 1) * chunk_sz + 1
          e <- min(i * chunk_sz, nrow(facilities_proj))
          if (s <= nrow(facilities_proj)) {
            chunk <- facilities_proj[s:e, ]
            select_cols <- intersect(c("GEOID","geometry"), names(oz_proj))
            if (length(select_cols) == 0) select_cols <- "geometry"
            oz_overlap <- suppressMessages(sf::st_join(chunk, oz_proj[, select_cols, drop = FALSE], join = sf::st_within, left = TRUE))
            res <- oz_overlap %>%
              sf::st_drop_geometry() %>%
              mutate(oz_census_tract = if ("GEOID" %in% names(oz_overlap)) as.character(GEOID) else NA_character_,
                     in_opportunity_zone = if ("GEOID" %in% names(oz_overlap)) !is.na(GEOID) else NA) %>%
              select(unique_id, oz_census_tract, in_opportunity_zone)
            oz_list[[i]] <- res
          }
          setTxtProgressBar(pb, i)
        }
        close(pb)
        
        oz_data <- bind_rows(oz_list) %>%
          group_by(unique_id) %>%
          summarise(
            in_opportunity_zone = any(dplyr::coalesce(in_opportunity_zone, FALSE)),
            oz_census_tract     = paste0(unique(na.omit(oz_census_tract)), collapse = ";"),
            .groups = "drop"
          )
        
        top_100_all_states_since_aug_2020 <- top_100_all_states_since_aug_2020 %>%
          left_join(oz_data, by = "unique_id")
        
        cat("\nOpportunity zone geocoding complete.\n")
      } else {
        cat("  Warning: Could not transform OZ CRS; skipping OZ overlay.\n")
      }
    } else {
      top_100_all_states_since_aug_2020 <- top_100_all_states_since_aug_2020 %>%
        mutate(in_opportunity_zone = NA, oz_census_tract = NA_character_)
    }
  } else {
    cat("Boundary downloads failed; skipping geocoding joins and OZ overlay.\n")
  }
  
} else {
  top_100_all_states_since_aug_2020 <- top_100_all_states_since_aug_2020 %>%
    mutate(
      county_fips = NA_character_,
      county_name = NA_character_,
      cbsa_code   = NA_character_,
      cbsa_name   = NA_character_,
      csa_code    = NA_character_,
      csa_name    = NA_character_,
      in_opportunity_zone = NA,
      oz_census_tract     = NA_character_
    )
}

geocoding_summary <- top_100_all_states_since_aug_2020 %>%
  mutate(has_coords = valid_lonlat(Longitude, Latitude) | dplyr::coalesce(LatLon_Valid, FALSE)) %>%
  summarise(
    total_facilities    = n(),
    with_coordinates    = sum(has_coords, na.rm = TRUE),
    with_county         = sum(!is.na(county_name)),
    with_cbsa           = sum(!is.na(cbsa_name)),
    with_csa            = sum(!is.na(csa_name)),
    in_opportunity_zone = sum(dplyr::coalesce(in_opportunity_zone, FALSE), na.rm = TRUE)
  )
cat("\nGeocoding Summary for Top 100 Facilities:\n"); print(geocoding_summary)

# ==============================================================================
# GOOD JOBS FIRST (GJF) DATA
# ==============================================================================
cat("\n================================================================================\n")
cat("GOOD JOBS FIRST DATA PROCESSING\n")
cat("================================================================================\n\n")

.state_name_to_abbr <- setNames(c(state.abb, "DC", "PR"),
                                c(state.name, "District of Columbia", "Puerto Rico"))

process_gjf_data_complete <- function(march_2025_file, new_data_folder) {
  cat("STEP 1: LOADING AND COMBINING DATA\n")
  cat("==================================\n")
  cat("Reading March 2025 GJF complete dataset...\n")
  
  GJF_MARCH_2025 <- read_csv(march_2025_file, guess_max = 10000)
  names(GJF_MARCH_2025) <- gsub("\\.", " ", names(GJF_MARCH_2025))
  GJF_MARCH_2025_char <- GJF_MARCH_2025 %>% mutate(across(everything(), as.character))
  # Add a stable row id for full original record carry-through
  GJF_MARCH_2025_char <- GJF_MARCH_2025_char %>% mutate(gjf_row_id = row_number())
  cat("March 2025 data:", nrow(GJF_MARCH_2025_char), "rows,", ncol(GJF_MARCH_2025_char), "columns\n\n")
  
  csv_files <- list.files(new_data_folder, pattern = "\\.csv$", full.names = TRUE, ignore.case = TRUE)
  cat("Found", length(csv_files), "CSV files in new folder\n")
  cat("Reading and processing new CSV files...\n")
  pb <- txtProgressBar(min = 0, max = length(csv_files), style = 3)
  all_gjf_data <- purrr::map_dfr(seq_along(csv_files), ~ {
    setTxtProgressBar(pb, .x)
    read_csv(csv_files[.x], col_types = cols(.default = "c")) %>%
      filter(rowSums(is.na(.) | . == "") < ncol(.))
  }) %>% distinct()
  close(pb)
  names(all_gjf_data) <- gsub("\\.", " ", names(all_gjf_data))
  all_gjf_data <- all_gjf_data %>% mutate(gjf_row_id = row_number() + nrow(GJF_MARCH_2025_char))
  cat("\nNew data:", nrow(all_gjf_data), "rows,", ncol(all_gjf_data), "columns\n\n")
  
  cat("Aligning columns (union across sources)...\n")
  all_cols <- union(names(GJF_MARCH_2025_char), names(all_gjf_data))
  GJF_MARCH_2025_aligned <- GJF_MARCH_2025_char %>% select(any_of(all_cols))
  all_gjf_data_aligned   <- all_gjf_data %>% select(any_of(all_cols))
  
  cat("Combining datasets...\n")
  combined_GJF_data <- bind_rows(GJF_MARCH_2025_aligned, all_gjf_data_aligned)
  
  # Ensure critical columns exist as character to avoid downstream errors
  required_cols <- c("State","Location","Company","Parent Company","Zip","City","County",
                     "Program Name","Type of Subsidy","Year","Subsidy Value",
                     "Number of Jobs or Training Slots","Investment Data","Source of Data")
  GJF_SUBSIDY_DATA <- ensure_chr_cols(combined_GJF_data, required_cols)
  
  cat("\nSTEP 2: ADDING DERIVED COLUMNS\n")
  cat("==============================\n")
  
  # Precompute patterns for state parsing
  two_letter_pat <- "\\b(AL|AK|AS|AZ|AR|CA|CO|CT|DE|DC|FL|GA|GU|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|MP|OH|OK|OR|PA|PR|RI|SC|SD|TN|TX|UT|VT|VI|VA|WA|WI|WV)\\b"
  long_names_pat <- paste0("\\b(", paste(names(.state_name_to_abbr), collapse="|"), ")\\b")
  
  GJF_SUBSIDY_DATA <- GJF_SUBSIDY_DATA %>%
    mutate(
      Location = dplyr::coalesce(Location, ""),
      State_raw = nzchr(str_trim(State)),
      State_2   = ifelse(!is.na(State_raw) & str_detect(State_raw, "^[A-Za-z]{2}$"),
                         toupper(State_raw), NA_character_),
      State_from_loc_two  = toupper(str_extract(Location, two_letter_pat)),
      State_long          = str_extract(Location, long_names_pat),
      State_from_loc_long = unname(.state_name_to_abbr[match(State_long, names(.state_name_to_abbr))]),
      state = dplyr::coalesce(State_2, State_from_loc_two, State_from_loc_long),
      Company = nzchr(str_trim(Company)),
      `Parent Company` = nzchr(str_trim(`Parent Company`)),
      company_parent = case_when(
        !is.na(Company) & !is.na(`Parent Company`) ~ paste0(Company, " | ", `Parent Company`),
        !is.na(Company) &  is.na(`Parent Company`) ~ Company,
        is.na(Company)  & !is.na(`Parent Company`) ~ `Parent Company`,
        TRUE ~ NA_character_
      )
    ) %>%
    select(-State_raw, -State_2, -State_long, -State_from_loc_two, -State_from_loc_long)
  cat("Added state abbreviations and Company; Parent column\n")
  
  cat("\nSTEP 3: CLEANING ZIP CODES\n")
  cat("==========================\n")
  GJF_SUBSIDY_DATA <- GJF_SUBSIDY_DATA %>%
    mutate(
      Zip_numeric = str_extract(as.character(Zip), "^\\d+"),
      Zip_clean = case_when(
        is.na(Zip_numeric) ~ NA_character_,
        nchar(Zip_numeric) >= 5 ~ substr(Zip_numeric, 1, 5),
        nchar(Zip_numeric) > 0 ~ str_pad(Zip_numeric, 5, "left", "0"),
        TRUE ~ NA_character_
      ),
      Zip_clean = if_else(str_detect(Zip_clean, "^\\d{5}$"), Zip_clean, NA_character_),
      Zip = Zip_clean
    ) %>%
    select(-Zip_numeric, -Zip_clean)
  
  cat("\nSTEP 4: CONVERTING NUMERIC COLUMNS\n")
  cat("==================================\n")
  GJF_SUBSIDY_DATA <- GJF_SUBSIDY_DATA %>%
    mutate(
      Year = suppressWarnings(as.integer(Year)),
      `Subsidy Value` = suppressWarnings(as.numeric(str_replace_all(`Subsidy Value`, "[^0-9.-]", ""))),
      `Number of Jobs or Training Slots` = suppressWarnings(as.integer(`Number of Jobs or Training Slots`)),
      `Investment Data` = suppressWarnings(as.numeric(str_replace_all(`Investment Data`, "[^0-9.-]", "")))
    )
  
  # Ensure gjf_row_id exists and is unique
  if (!"gjf_row_id" %in% names(GJF_SUBSIDY_DATA)) {
    GJF_SUBSIDY_DATA <- GJF_SUBSIDY_DATA %>% mutate(gjf_row_id = row_number())
  }
  
  cat("\n=== FINAL SUMMARY ===\n")
  cat("Total rows: ", format(nrow(GJF_SUBSIDY_DATA), big.mark = ","), "\n")
  cat("Total subsidy value: $ ", format(sum(GJF_SUBSIDY_DATA$`Subsidy Value`, na.rm = TRUE), big.mark = ",", scientific = FALSE), "\n")
  
  GJF_SUBSIDY_DATA
}

GJF_SUBSIDY_DATA <- process_gjf_data_complete(
  march_2025_file = "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/Good Jobs First/gjf_complete.csv",
  new_data_folder = "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/Good Jobs First/Good Jobs First Downloads 22 July 2025"
)

# ==============================================================================
# SUBSIDY MATCHING (REFINED + CALIBRATED) — AUTOMATIC
# ==============================================================================
cat("\n================================================================================\n")
cat("SUBSIDY MATCHING FOR TOP 100 FACILITIES SINCE AUG 2020 (REFINED + CALIBRATED)\n")
cat("================================================================================\n\n")

# Enhanced facilities
top_100_enhanced <- top_100_all_states_since_aug_2020 %>%
  mutate(
    capex_amount_estimated = suppressWarnings(as.numeric(Estimated_Total_Facility_CAPEX)) * 1e6,
    announcement_date = Announcement_Date,
    announcement_year = lubridate::year(Announcement_Date),
    facility_company_raw = Company,
    fac_company_norm = apply_alias_norm(normalize_company_name(Company)),
    company_primary = stringr::str_extract(Company, "^[^|]+") %>% str_trim(),
    fac_company_primary_norm = apply_alias_norm(normalize_company_name(company_primary)),
    fac_company_tokens = purrr::map(fac_company_norm, tokenize),
    fac_company_acronym = purrr::map_chr(fac_company_tokens, acronym_from_tokens),
    fac_company_primary_tokens = purrr::map(fac_company_primary_norm, tokenize),
    fac_company_primary_acronym = purrr::map_chr(fac_company_primary_tokens, acronym_from_tokens),
    facility_kw_raw = paste(
      tolower(dplyr::coalesce(Decarb_Sector,"")),
      tolower(dplyr::coalesce(Technology,"")),
      tolower(dplyr::coalesce(Subcategory,"")), sep = " "
    ),
    facility_kw_tokens = purrr::map(facility_kw_raw, tokenize),
    facility_syn_tokens = purrr::map2(Technology, Subcategory, ~ tech_synonyms(.x, .y, sector = NA_character_)),
    fac_lon = suppressWarnings(as.numeric(Longitude)),
    fac_lat = suppressWarnings(as.numeric(Latitude)),
    facility_county_norm = normalize_county_name(county_name),
    facility_county_fips = county_fips
  ) %>%
  filter(!is.na(announcement_year)) %>%
  distinct(unique_id, .keep_all = TRUE)

debug_print("top_100_enhanced created", top_100_enhanced)

# County centroids for distances
state_county_lookup <- tryCatch(
  get_state_county_lookup(year = 2020),
  error = function(e) { cat("[ERROR] get_state_county_lookup failed: ", conditionMessage(e), "\n"); NULL }
)

# Build subsidies_enhanced (use gjf_row_id as stable key)
gov_recipient_re <- "(?i)\\b(authority|board|commission|county|city|town|township|village|borough|parish|district|municipal|redevelopment|finance\\s+authority|industrial\\s+development|port\\s+authority|economic\\s+development|state\\b|dept\\.|department|treasurer|trustee|ida\\b|edc\\b|idb\\b)\\b"

subsidies_core <- GJF_SUBSIDY_DATA %>%
  filter(!is.na(company_parent), !is.na(state), !is.na(`Subsidy Value`), !is.na(Year)) %>%
  transmute(
    subsidy_id = gjf_row_id,   # stable id
    state,
    subsidy_company_raw = nzchr(Company),
    subsidy_parent_raw  = nzchr(`Parent Company`),
    sub_company_norm    = apply_alias_norm(normalize_company_name(Company)),
    sub_parent_norm     = apply_alias_norm(normalize_company_name(`Parent Company`)),
    subsidy_year        = as.integer(Year),
    subsidy_value       = `Subsidy Value`,
    program_name        = tolower(`Program Name`),
    subsidy_type_raw    = tolower(`Type of Subsidy`),
    subsidy_city        = nzchr(City),
    subsidy_county      = nzchr(County),
    subsidy_zip         = nzchr(Zip)
  ) %>%
  mutate(
    sub_company_tokens  = purrr::map(sub_company_norm, tokenize),
    sub_parent_tokens   = purrr::map(sub_parent_norm, tokenize),
    sub_company_acronym = purrr::map_chr(sub_company_tokens, acronym_from_tokens),
    sub_parent_acronym  = purrr::map_chr(sub_parent_tokens, acronym_from_tokens),
    sub_text_tokens     = purrr::map(paste(program_name, subsidy_type_raw, sep = " "), tokenize),
    recipient_is_gov    = if_else(stringr::str_detect(dplyr::coalesce(subsidy_company_raw, ""), gov_recipient_re), TRUE, FALSE),
    subsidy_county_norm = normalize_county_name(subsidy_county)
  )

if (!is.null(state_county_lookup) && nrow(state_county_lookup) > 0) {
  subsidies_enhanced <- subsidies_core %>%
    left_join(
      state_county_lookup %>% select(state, county_norm, county_fips, county_lon, county_lat),
      by = c("state" = "state", "subsidy_county_norm" = "county_norm")
    ) %>%
    rename(subsidy_county_fips = county_fips, subsidy_county_lon = county_lon, subsidy_county_lat = county_lat)
} else {
  cat("[WARN] County lookup unavailable; distance features will be skipped.\n")
  subsidies_enhanced <- subsidies_core %>%
    mutate(subsidy_county_fips = NA_character_, subsidy_county_lon = NA_real_, subsidy_county_lat = NA_real_)
}

# Assertions & debug
stopifnot(is.data.frame(subsidies_enhanced))
stopifnot(all(c("state","subsidy_id") %in% names(subsidies_enhanced)))
alias_hits_company <- sum(GJF_SUBSIDY_DATA$Company %>% normalize_company_name() %in% ALIAS_TABLE$variant, na.rm = TRUE)
alias_hits_parent  <- sum(GJF_SUBSIDY_DATA$`Parent Company` %>% normalize_company_name() %in% ALIAS_TABLE$variant, na.rm = TRUE)
cat(sprintf("\n[DEBUG] Alias hits (company): %d  | (parent): %d\n", alias_hits_company, alias_hits_parent))
cat("--- 1. Prepared facility & subsidy tables (aliasing, gov flags, centroids) ---\n")
cat("Facilities: ", nrow(top_100_enhanced), " | Subsidy rows: ", nrow(subsidies_enhanced), "\n", sep="")

# ==============================================================================
# SCORING FUNCTION
# ==============================================================================
score_pairs <- function(df, calib = CALIB) {
  df %>%
    mutate(
      # Name similarities
      jw_fac_vs_subco   = stringsim_vec(fac_company_norm,        sub_company_norm,   method = "jw"),
      jw_fac_vs_parent  = stringsim_vec(fac_company_norm,        sub_parent_norm,    method = "jw"),
      jw_prim_vs_subco  = stringsim_vec(fac_company_primary_norm,sub_company_norm,   method = "jw"),
      jw_prim_vs_parent = stringsim_vec(fac_company_primary_norm,sub_parent_norm,    method = "jw"),
      jw_name_max = pmax(jw_fac_vs_subco, jw_fac_vs_parent, jw_prim_vs_subco, jw_prim_vs_parent, na.rm = TRUE),
      
      cos_fac_vs_subco   = stringsim_vec(fac_company_norm,        sub_company_norm,   method = "cosine", q = 3),
      cos_fac_vs_parent  = stringsim_vec(fac_company_norm,        sub_parent_norm,    method = "cosine", q = 3),
      cos_prim_vs_subco  = stringsim_vec(fac_company_primary_norm,sub_company_norm,   method = "cosine", q = 3),
      cos_prim_vs_parent = stringsim_vec(fac_company_primary_norm,sub_parent_norm,    method = "cosine", q = 3),
      cos_name_max = pmax(cos_fac_vs_subco, cos_fac_vs_parent, cos_prim_vs_subco, cos_prim_vs_parent, na.rm = TRUE),
      
      jacc_fac_vs_subco   = purrr::map2_dbl(fac_company_tokens,          sub_company_tokens,  token_jaccard),
      jacc_fac_vs_parent  = purrr::map2_dbl(fac_company_tokens,          sub_parent_tokens,   token_jaccard),
      jacc_prim_vs_subco  = purrr::map2_dbl(fac_company_primary_tokens,  sub_company_tokens,  token_jaccard),
      jacc_prim_vs_parent = purrr::map2_dbl(fac_company_primary_tokens,  sub_parent_tokens,   token_jaccard),
      jacc_name_max = pmax(jacc_fac_vs_subco, jacc_fac_vs_parent, jacc_prim_vs_subco, jacc_prim_vs_parent, na.rm = TRUE),
      
      acronym_hit = pmax(
        as.integer(fac_company_acronym        == sub_company_acronym & fac_company_acronym        != ""),
        as.integer(fac_company_acronym        == sub_parent_acronym  & fac_company_acronym        != ""),
        as.integer(fac_company_primary_acronym== sub_company_acronym & fac_company_primary_acronym!= ""),
        as.integer(fac_company_primary_acronym== sub_parent_acronym  & fac_company_primary_acronym!= ""),
        na.rm = TRUE
      ),
      
      same_state = as.integer(State == state),
      
      # FIPS-first county match
      county_match_fips = as.integer(!is.na(facility_county_fips) & !is.na(subsidy_county_fips) &
                                       facility_county_fips == subsidy_county_fips),
      county_match_name = as.integer(!is.na(facility_county_norm) & !is.na(subsidy_county_norm) &
                                       facility_county_norm == subsidy_county_norm & same_state == 1),
      county_match = pmax(county_match_fips, county_match_name, na.rm = TRUE),
      cbsa_evidence = as.integer(!is.na(facility_cbsa) & ( (!is.na(subsidy_city) & same_state == 1) | !is.na(subsidy_zip) )),
      geo_bonus = calib$geo_weight_county * county_match + calib$geo_weight_cbsa * cbsa_evidence,
      
      # Distance (county centroid vs facility lat/lon)
      distance_km = ifelse(
        !is.na(fac_lon) & !is.na(fac_lat) & !is.na(subsidy_county_lon) & !is.na(subsidy_county_lat),
        geosphere::distHaversine(cbind(fac_lon, fac_lat), cbind(subsidy_county_lon, subsidy_county_lat)) / 1000,
        NA_real_
      ),
      distance_bonus = case_when(
        is.na(distance_km) ~ 0,
        county_match == 1 ~ 0,
        distance_km <= calib$distance_bonus_km[1] ~ calib$distance_bonus_vals[1],
        distance_km <= calib$distance_bonus_km[2] ~ calib$distance_bonus_vals[2],
        distance_km <= calib$distance_bonus_km[3] ~ calib$distance_bonus_vals[3],
        TRUE ~ 0
      ),
      
      # Temporal
      year_diff = subsidy_year - announcement_year,
      temporal_bonus = case_when(
        dplyr::between(year_diff, -1,  2) ~ calib$temporal_bonus["close"],
        dplyr::between(year_diff, -2,  3) ~ calib$temporal_bonus["good"],
        dplyr::between(year_diff, -3,  4) ~ calib$temporal_bonus["ok"],
        dplyr::between(year_diff, -4,  5) ~ calib$temporal_bonus["far"],
        TRUE ~ calib$temporal_bonus["bad"]
      ) %>% as.numeric(),
      
      # Keywords (tech tokens + learned)
      kw_overlap     = purrr::map2_int(facility_kw_tokens, sub_text_tokens, ~ length(intersect(.x, .y))),
      syn_overlap    = purrr::map2_int(facility_syn_tokens, sub_text_tokens, ~ length(intersect(.x, .y))),
      learned_kw_hits= purrr::map_int(sub_text_tokens, ~ length(intersect(.x, LEARNED_PROGRAM_KW))),
      kw_bonus = pmin(calib$kw_cap,
                      calib$kw_per_token * kw_overlap +
                        calib$kw_per_synonym * syn_overlap +
                        calib$learned_kw_per * learned_kw_hits),
      
      # Name weighting & bonuses/penalties
      name_base = calib$w_jw * jw_name_max + calib$w_cos * cos_name_max + calib$w_jacc * jacc_name_max,
      acronym_bonus = case_when(
        acronym_hit >= 1 & county_match == 1 ~ calib$acronym_bonus_with_county,
        acronym_hit >= 1 & cbsa_evidence == 1 ~ calib$acronym_bonus_with_cbsa,
        acronym_hit >= 1 ~ calib$acronym_bonus_plain,
        TRUE ~ 0
      ),
      sub_capex_ratio = subsidy_value / capex_amount_estimated,
      ratio_bonus = case_when(
        is.na(sub_capex_ratio) ~ 0,
        sub_capex_ratio >= calib$ratio_bonus["lo"] & sub_capex_ratio <= calib$ratio_bonus["hi"] ~ calib$ratio_bonus["bonus"],
        TRUE ~ 0
      ) %>% as.numeric(),
      gov_penalty = ifelse(recipient_is_gov & jw_name_max < 0.92 & acronym_hit == 0, calib$gov_recipient_penalty, 0),
      
      total_score = pmin(1, pmax(0, name_base + geo_bonus + distance_bonus + temporal_bonus + kw_bonus + acronym_bonus + ratio_bonus + gov_penalty)),
      
      # Filters
      suspicious_timing      = abs(year_diff) > calib$year_window_soft,
      weak_name              = (jw_name_max < calib$jw_hard_min) & !(acronym_hit == 1 & cos_name_max >= 0.65),
      acronym_timing_penalty = (acronym_hit == 1 & abs(year_diff) > 4)
    ) %>%
    filter(!suspicious_timing, !weak_name, !acronym_timing_penalty) %>%
    mutate(
      confidence_level = case_when(
        jw_name_max >= calib$def_jw_min & county_match == 1 & dplyr::between(year_diff, -2, 3) ~ "Definite",
        total_score >= calib$high_total_min & dplyr::between(year_diff, -3, 4)                ~ "High",
        total_score >= calib$med_total_min  & dplyr::between(year_diff, -4, 5)                ~ "Medium",
        total_score >= calib$low_total_min                                                    ~ "Low",
        TRUE                                                                                  ~ "Very Low"
      ),
      match_reason = paste0(
        "name[jw=", sprintf("%.2f", jw_name_max),
        ", cos=", sprintf("%.2f", cos_name_max),
        ", jacc=", sprintf("%.2f", jacc_name_max), "]; ",
        "geo[county_fips=", ifelse(!is.na(facility_county_fips) & !is.na(subsidy_county_fips) & facility_county_fips == subsidy_county_fips, 1, 0),
        ", county_name=", county_match_name,
        ", cbsa=", cbsa_evidence, ", dist_km=", ifelse(is.na(distance_km), "NA", sprintf("%.1f", distance_km)), "]; ",
        "time=", sprintf("%+d", year_diff), "; ",
        "acr=", acronym_hit, "; ",
        "kw=", kw_overlap, "; syn=", syn_overlap, "; learned=", learned_kw_hits, "; ",
        "ratio=", sprintf("%.3f", sub_capex_ratio), "; gov_pen=", sprintf("%.2f", gov_penalty)
      )
    )
}

cat("\n--- 2. Pairing & scoring by state (with distance + FIPS prefilter) ---\n")

match_state_top100_refined <- function(state_code, facilities_df, subsidies_df, calib = CALIB) {
  cat(paste0("Processing state: ", state_code, "\n"))
  if (!is.data.frame(facilities_df)) stop("facilities_df is not a data.frame; class=", paste(class(facilities_df), collapse=","))
  if (!is.data.frame(subsidies_df))  stop("subsidies_df is not a data.frame; class=", paste(class(subsidies_df), collapse=","))
  
  f <- facilities_df %>% filter(State == state_code)
  s <- subsidies_df  %>% filter(state  == state_code)
  
  cat("  Facilities: ", nrow(f), " | Subsidies: ", nrow(s), "\n", sep = "")
  if (nrow(f) == 0 || nrow(s) == 0) {
    cat("  No facilities or subsidies. Skipping.\n")
    return(NULL)
  }
  
  f <- f %>%
    mutate(f_init = substr(fac_company_norm, 1, 1),
           fp_init = substr(fac_company_primary_norm, 1, 1))
  
  s <- s %>%
    mutate(sc_init = substr(sub_company_norm, 1, 1),
           sp_init = substr(sub_parent_norm,  1, 1))
  
  pairs_pre <- tidyr::expand_grid(unique_id = f$unique_id, subsidy_id = s$subsidy_id) %>%
    left_join(f %>%
                transmute(
                  unique_id, State,
                  facility_company_raw, fac_company_norm, fac_company_tokens, fac_company_acronym,
                  fac_company_primary_norm, fac_company_primary_tokens, fac_company_primary_acronym,
                  facility_kw_tokens, facility_syn_tokens,
                  announcement_date, announcement_year, capex_amount_estimated,
                  Current_Facility_Status, facility_county = county_name, facility_county_norm, facility_county_fips,
                  facility_cbsa = cbsa_name, facility_csa = csa_name, in_opportunity_zone,
                  f_init, fp_init, fac_lon, fac_lat,
                  Technology, Subcategory, Decarb_Sector
                ),
              by = "unique_id") %>%
    left_join(s %>%
                select(subsidy_id, subsidy_company_raw, subsidy_parent_raw, sub_company_norm,
                       sub_parent_norm, sub_company_tokens, sub_parent_tokens,
                       sub_company_acronym, sub_parent_acronym, subsidy_year, subsidy_value,
                       program_name, subsidy_type_raw, subsidy_city, subsidy_county, subsidy_zip, sub_text_tokens,
                       sc_init, sp_init, recipient_is_gov,
                       subsidy_county_norm, subsidy_county_fips, subsidy_county_lon, subsidy_county_lat, state),
              by = "subsidy_id") %>%
    mutate(
      year_diff_pref = subsidy_year - announcement_year,
      init_agree = (f_init == sc_init) | (f_init == sp_init) | (fp_init == sc_init) | (fp_init == sp_init),
      dist_pref_km = ifelse(
        !is.na(fac_lon) & !is.na(fac_lat) & !is.na(subsidy_county_lon) & !is.na(subsidy_county_lat),
        geosphere::distHaversine(cbind(fac_lon, fac_lat), cbind(subsidy_county_lon, subsidy_county_lat)) / 1000,
        NA_real_
      )
    )
  cat(sprintf("  Candidate pairs (expanded): %s\n", format(nrow(pairs_pre), big.mark=",")))
  
  pairs <- pairs_pre %>%
    filter(
      !is.na(announcement_year), !is.na(subsidy_year),
      abs(year_diff_pref) <= calib$year_window_hard,
      (
        init_agree |
          (!is.na(facility_county_fips) & !is.na(subsidy_county_fips) & facility_county_fips == subsidy_county_fips) |
          (is.na(facility_county_fips) & !is.na(facility_county_norm) & !is.na(subsidy_county_norm) &
             facility_county_norm == subsidy_county_norm) |
          (!is.na(dist_pref_km) & dist_pref_km <= calib$distance_prefilter_km) |
          (fac_company_acronym != "" & (fac_company_acronym == sub_company_acronym | fac_company_acronym == sub_parent_acronym)) |
          (fac_company_primary_acronym != "" & (fac_company_primary_acronym == sub_company_acronym | fac_company_primary_acronym == sub_parent_acronym))
      )
    ) %>%
    select(-year_diff_pref, -init_agree, -dist_pref_km)
  cat(sprintf("  Candidate pairs after prefilter: %s\n", format(nrow(pairs), big.mark=",")))
  
  if (nrow(pairs) == 0) {
    cat("  No candidate pairs after prefilter.\n")
    return(NULL)
  }
  
  scored <- score_pairs(pairs, calib = calib)
  cat(sprintf("  Retained after scoring/filters: %s\n", format(nrow(scored), big.mark=",")))
  
  if (nrow(scored) == 0) {
    cat("  No matches after scoring/filters.\n")
    return(NULL)
  }
  
  best <- scored %>%
    group_by(unique_id, subsidy_year) %>%
    slice_max(order_by = total_score, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  cat("  Score quantiles (total_score):\n")
  print(quantile(best$total_score, probs = c(.1,.5,.8,.9,.95,1), na.rm = TRUE))
  best
}

states_to_process <- sort(unique(top_100_enhanced$State))
cat("States to process: ", length(states_to_process), "\n", sep="")

match_list <- vector("list", length(states_to_process))
names(match_list) <- states_to_process

for (st in states_to_process) {
  tryCatch({
    mm <- match_state_top100_refined(st, top_100_enhanced, subsidies_enhanced, calib = CALIB)
    if (!is.null(mm) && nrow(mm) > 0) match_list[[st]] <- mm
  }, error = function(e) {
    cat(paste0("  ERROR processing state ", st, ": ", conditionMessage(e), "\n"))
  })
}

# Detailed long candidates
top_100_subsidy_candidates <- bind_rows(match_list)
cat("\nTotal matches retained: ", nrow(top_100_subsidy_candidates), "\n", sep="")
debug_print("top_100_subsidy_candidates created", top_100_subsidy_candidates)

# ==============================================================================
# MERGE ORIGINAL GJF COLUMNS + KEEP ONLY MEDIUM & HIGH CONFIDENCE
# ==============================================================================
if (!is.data.frame(top_100_subsidy_candidates) || nrow(top_100_subsidy_candidates) == 0) {
  stop("No subsidy candidates found after scoring/filters.")
}

# Attach ALL original GJF columns (by stable subsidy_id = gjf_row_id)
gjf_original_cols <- names(GJF_SUBSIDY_DATA)

matches_with_gjf <- top_100_subsidy_candidates %>%
  left_join(GJF_SUBSIDY_DATA %>% select(all_of(c("gjf_row_id", gjf_original_cols))),
            by = c("subsidy_id" = "gjf_row_id"))

# Filter to Medium & High confidence only (as requested)
matches_medium_high <- matches_with_gjf %>%
  filter(confidence_level %in% c("High","Medium"))

cat("Matches kept (Medium/High): ", nrow(matches_medium_high), "\n", sep="")

# ==============================================================================
# WIDE EXPORT (ONLY) — includes ALL original GJF columns + key match signals
# ==============================================================================
rank_matches_by_facility <- function(matches_long) {
  if (is.null(matches_long) || !is.data.frame(matches_long) || nrow(matches_long) == 0) return(tibble())
  matches_long %>%
    arrange(unique_id, desc(total_score), desc(`Subsidy Value`), subsidy_year) %>%
    group_by(unique_id) %>%
    mutate(match_rank = row_number(),
           match_count = n()) %>%
    ungroup()
}

widen_matches_by_rank <- function(ranked_long, facilities_df, prefix = "subsidy_match",
                                  extra_signals = c("confidence_level","total_score",
                                                    "jw_name_max","cos_name_max","jacc_name_max",
                                                    "county_match","cbsa_evidence","distance_km","year_diff",
                                                    "kw_overlap","syn_overlap","match_reason"),
                                  gjf_cols = gjf_original_cols) {
  base <- facilities_df %>%
    distinct(unique_id, .keep_all = TRUE) %>%
    select(
      unique_id, State, Company, facility_company_raw,
      announcement_date, announcement_year, capex_amount_estimated,
      facility_county = county_name, facility_county_fips = county_fips, facility_cbsa = cbsa_name,
      facility_csa = csa_name, in_opportunity_zone,
      Longitude, Latitude, Address, Technology, Subcategory, Decarb_Sector
    )
  
  if (is.null(ranked_long) || !is.data.frame(ranked_long) || nrow(ranked_long) == 0) {
    base$match_count <- 0L
    return(base)
  }
  
  counts <- ranked_long %>% count(unique_id, name = "match_count")
  base <- base %>% left_join(counts, by = "unique_id") %>%
    mutate(match_count = dplyr::coalesce(match_count, 0L))
  
  # For each rank, attach every original GJF column + signals
  info_cols <- unique(c(extra_signals, gjf_cols))
  
  max_k <- ranked_long %>% summarise(m = max(match_rank, na.rm = TRUE)) %>% pull(m)
  if (is.infinite(max_k) || is.na(max_k)) max_k <- 0L
  cat(sprintf("Max matches per facility to widen: %d\n", max_k))
  
  if (max_k > 0) {
    for (k in seq_len(max_k)) {
      chunk <- ranked_long %>%
        filter(match_rank == k) %>%
        select(unique_id, all_of(info_cols))
      new_names <- paste0(prefix, "_", k, "_", info_cols)
      names(chunk) <- c("unique_id", new_names)
      base <- base %>% left_join(chunk, by = "unique_id")
    }
  }
  base
}

intermediate_long_mh <- rank_matches_by_facility(matches_medium_high)

final_wide <- widen_matches_by_rank(
  ranked_long  = intermediate_long_mh,
  facilities_df = top_100_enhanced,
  prefix = "subsidy_match",
  extra_signals = c("confidence_level","total_score",
                    "jw_name_max","cos_name_max","jacc_name_max",
                    "county_match","cbsa_evidence","distance_km","year_diff",
                    "kw_overlap","syn_overlap","match_reason"),
  gjf_cols = gjf_original_cols
)

# === ONLY EXPORT ===
readr::write_csv(final_wide, "top_75_facilities_subsidy_matches_ACCEPTED_WIDE_geocoded.csv")
cat("\n[EXPORT] Wrote: top_75_facilities_subsidy_matches_ACCEPTED_WIDE_geocoded.csv\n")

# ==============================================================================
# FINAL DEBUG SUMMARY
# ==============================================================================
cat("\n================================================================================\n")
cat("GEOCODING + AUTOMATIC MATCHING COMPLETE (TOP 100 + MEDIUM/HIGH ONLY)\n")
cat("================================================================================\n")
cat("Total facilities processed: ", nrow(top_100_enhanced), "\n", sep="")
cat("Total subsidy candidates: ", nrow(top_100_subsidy_candidates), "\n", sep="")
cat("Total Medium/High matches: ", nrow(matches_medium_high), "\n", sep="")
cat("Final wide columns: ", ncol(final_wide), "  | rows: ", nrow(final_wide), "\n", sep="")
cat("Only file produced: top_75_facilities_subsidy_matches_ACCEPTED_WIDE_geocoded.csv\n")
