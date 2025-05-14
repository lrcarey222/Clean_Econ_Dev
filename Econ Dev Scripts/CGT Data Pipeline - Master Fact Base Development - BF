###############################################################################
##  MASTER R SCRIPT (with Parallel Processing)
##  With Time-Stamped Logging, County/State/CBSA Data, NREL, Property Taxes,
##  Geocoding Data, NSF Patent Data, Geopackages, Coal Power Plant CSV,
##  Additional EIA Spatial Layers (ArcGIS), plus updated logic to explicitly
##  build geographies (excluding US territories), embed county presence flags,
##  add state names/abbrevs to counties, merge CBSA columns into counties,
##  reintroduce progress updates, and now incorporate additional speed-ups:
##
##     - Use data.table::fread() for large CSV files, then convert to tibble.
##     - Parallelize EIG sheet reading, Geopackage loading, ArcGIS fetches,
##       county presence flag computation.
##     - Chunked and parallelized county-CBSA spatial join.
##     - Enhanced logging & progress updates.
##     - Wrapped network calls in tryCatch for improved robustness.
##     - Minor code refinements and consistent dbg() usage.
##
##  Organized by:
##     1) Logging & Setup (all packages loaded up front)
##     2) Core Geographies + Crosswalks (explicitly created)
##     3) County-Level Data
##     4) State-Level Data
##     5) CBSA-Level Data
##     6) Geocoding-Related Data (EIA 860M, CIM Facilities, etc.)
##     7) NSF Patent Data (COUNTY & STATE) from swbinv-3 / swbinv-1
##     8) Other Additional Data Loads (Geopackages, Coal CSV)
##     9) Additional EIA Spatial Layers
##    10) Extra Data: CBS News 2024 Election Data
##    11) Close sinks
###############################################################################

# ------------------------------------------------------------------------------
# 1) LOGGING & SETUP
# ------------------------------------------------------------------------------
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_file  <- file.path(getwd(), paste0("consolidated_output_", timestamp, ".log"))
log_con   <- file(log_file, open = "wt")

sink(log_con, type = "output", split = TRUE) # Capture R output
sink(log_con, type = "message")              # Capture messages, warnings, errors

message(paste("[Debug] Logging started. Writing to:", log_file))

DEBUG_MODE <- TRUE
now <- \( ) format(Sys.time(), "%Y-%m-%d %H:%M:%S")

msg <- \(lvl = "INFO", txt) {
  log_message_str <- glue::glue("{now()} [{lvl}] {txt}")
  message(log_message_str)
}

dbg <- \(obj, lbl = deparse(substitute(obj))) {
  if (DEBUG_MODE) {
    msg("DEBUG", paste0("Inspecting: ", lbl, " (class: ", paste(class(obj), collapse=", "), ")"))
    obj_dim <- try(dim(obj), silent = TRUE)
    if (!inherits(obj_dim, "try-error") && !is.null(obj_dim)) {
      msg("DEBUG", paste0("  Dimensions: ", paste(obj_dim, collapse=" x ")))
    } else {
      obj_len <- try(length(obj), silent = TRUE)
      if (!inherits(obj_len, "try-error")) {
        msg("DEBUG", paste0("  Length: ", obj_len))
      }
    }
    if (is.data.frame(obj) || inherits(obj, "sf") || is.matrix(obj)) {
      msg("DEBUG", "  Glimpse of the data frame:")
      dplyr::glimpse(obj)
    } else if (is.list(obj)) {
      msg("DEBUG", "  Structure (top level):")
      str(obj, max.level = 1, list.len = 5)
    } else if (is.vector(obj)) {
      msg("DEBUG", "  First few elements:")
      print(head(obj, 6))
    } else {
      msg("DEBUG", "  Object printout:")
      print(obj)
    }
    msg("DEBUG", paste0("Finished inspecting: ", lbl))
  }
  invisible(obj)
}

msg("INFO","Starting script setup...")

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(tidyr)
  library(readxl)
  library(writexl)
  library(rvest)
  library(janitor)
  library(tibble)
  library(glue)
  library(httr)
  library(jsonlite)
  library(tidycensus)
  library(tigris)
  library(sf)
  library(ggplot2)
  library(data.table)  # For fast fread
  library(arrow)
  library(furrr)
  library(future)
  library(crayon)
  library(cli)
  library(esri2sf)
  library(readr) # For read_tsv / read_csv
})

# Additional speed optimizations
options(timeout = 600)                         # Increase download timeouts
Sys.setenv(VROOM_CONNECTION_SIZE = "131072")   # For faster reading
options(tigris_use_cache = TRUE, tigris_class = "sf")
sf_use_s2(TRUE) # Using S2 geometry engine for global operations
options(warn = 1, sf_max_bool_epsg_define = 100000) # Suppress some sf warnings for bool ops

msg("INFO","Libraries loaded and options set.")
msg("INFO","Setting up parallel plan (furrr/future)...")
# Use min of 6 or available cores, ensure at least 1 worker.
num_workers <- max(1, min(6, future::availableCores(omit = 1))) 
plan(multisession, workers = num_workers)
msg("INFO", sprintf("Parallel plan activated with multisession workers: %d", future::nbrOfWorkers()))

msg("INFO","Attempting to locate OneDrive path...")
candidate_paths <- c(
  file.path(Sys.getenv(ifelse(.Platform$OS.type=="windows","USERPROFILE","HOME")),
            "Library","CloudStorage","OneDrive-RMI"),
  file.path(Sys.getenv(ifelse(.Platform$OS.type=="windows","USERPROFILE","HOME")),
            "OneDrive - RMI"),
  file.path(Sys.getenv("USERPROFILE"), "OneDrive - RMI"),
  file.path("~", "Library", "CloudStorage", "OneDrive-RMI"),
  "~" # Fallback to home directory if others not found
)
base_onedrive_path <- NA_character_
for (p in candidate_paths) {
  abs_p <- path.expand(p)
  if (dir.exists(abs_p)) {
    base_onedrive_path <- abs_p
    break
  }
}

if (is.na(base_onedrive_path)) {
  stop("Could not find a suitable OneDrive or home directory. Please update script or check OneDrive paths.")
} else {
  msg("INFO", sprintf("Found or using base path: %s", base_onedrive_path))
}

DATA_FOLDER <- file.path(
  base_onedrive_path,
  "US Program - Documents",
  "6_Projects",
  "Clean Regional Economic Development",
  "ACRE",
  "Data"
)
if(!dir.exists(DATA_FOLDER)){
  dir.create(DATA_FOLDER, recursive = TRUE)
  msg("INFO", sprintf("Created DATA_FOLDER: %s", DATA_FOLDER))
}
RAW_DATA_FOLDER <- file.path(DATA_FOLDER, "Raw Data")
if(!dir.exists(RAW_DATA_FOLDER)){
  dir.create(RAW_DATA_FOLDER, recursive = TRUE)
  msg("INFO", sprintf("Created RAW_DATA_FOLDER: %s", RAW_DATA_FOLDER))
}
msg("INFO", sprintf("Using DATA_FOLDER: %s", DATA_FOLDER))
msg("INFO","Script setup completed.")

# ------------------------------------------------------------------------------
# 2) CORE GEOGRAPHIES + CROSSWALKS
# ------------------------------------------------------------------------------
msg("INFO","Starting explicit creation of geographies and merges...")

CRS_USE <- 4326
EXCLUDE_TERRITORIES <- c("60","66","69","72","78", "VI", "GU", "AS", "MP", "PR")
msg("INFO",paste("Territory FIPS/Abbrs excluded:", paste(EXCLUDE_TERRITORIES, collapse=", ")))

msg("INFO","Creating STATE_GEODATA from tigris::states() for year=2023...")
STATE_GEODATA <- tigris::states(year=2023, cb=TRUE) %>%
  st_transform(CRS_USE) %>%
  clean_names() %>%
  rename_with(~ toupper(.x), everything()) %>%
  mutate(GEO_ID = as.character(GEOID),
         is_territory = ifelse(
           "STATEFP" %in% names(.), STATEFP %in% EXCLUDE_TERRITORIES, FALSE
         )) %>%
  filter(!is_territory) %>%
  select(-is_territory, -GEOID)
dbg(STATE_GEODATA, "STATE_GEODATA (post-exclusion)")

msg("INFO","Creating COUNTY_GEODATA from tigris::counties() for year=2023...")
temp_counties <- tigris::counties(year=2023, cb=TRUE) %>%
  st_transform(CRS_USE) %>%
  clean_names() %>%
  rename_with(~ toupper(.x), everything()) %>%
  mutate(GEO_ID = as.character(GEOID),
         is_territory = ifelse(
           "STATEFP" %in% names(.), STATEFP %in% EXCLUDE_TERRITORIES, FALSE
         )) %>%
  filter(!is_territory) %>%
  select(-is_territory)
dbg(temp_counties, "temp_counties (base for COUNTY_GEODATA)")

msg("PROGRESS","Now computing presence flags for each YEAR in c(2018,2020,2023) in parallel...")
YEARS <- c(2018, 2020, 2023)

compute_presence_flags <- function(y, exclude_fips) {
  tmp <- tigris::counties(year = y, cb = TRUE) %>%
    st_drop_geometry() %>%
    clean_names() %>%
    rename_with(~ toupper(.x), everything()) %>%
    mutate(GEO_ID = as.character(GEOID),
           is_territory = ifelse(
             "STATEFP" %in% names(.), STATEFP %in% exclude_fips, FALSE
           )) %>%
    filter(!is_territory) %>%
    select(GEO_ID) %>%
    mutate(!!paste0("PRESENT_", y) := TRUE)
  tmp
}

all_flag_frames <- future_map(
  YEARS,
  ~ compute_presence_flags(.x, exclude_fips = EXCLUDE_TERRITORIES),
  .options = furrr_options(
    seed = TRUE,
    packages = c("tigris", "sf", "dplyr", "stringr", "janitor", "glue", "tidyr")
  )
)

msg("DEBUG","Combining presence flags with full_join over all years...")
msg("PROGRESS","Merging all presence flag data frames together...")

combined_flags <- Reduce(function(a, b) {
  full_join(a, b, by="GEO_ID")
}, all_flag_frames)

msg("DEBUG","Replacing NA with FALSE in presence flag columns...")
combined_flags <- combined_flags %>%
  mutate(across(starts_with("PRESENT_"), ~ replace_na(.x, FALSE))) %>%
  select(GEO_ID, starts_with("PRESENT_"))
dbg(combined_flags, "combined_flags (GEO_ID and PRESENT_ flags)")

msg("INFO","Merging presence flags into COUNTY_GEODATA...")
COUNTY_GEODATA <- temp_counties %>%
  left_join(combined_flags, by="GEO_ID")
dbg(COUNTY_GEODATA, "COUNTY_GEODATA (with presence flags)")

msg("INFO","Adding state name/abbr from STATE_GEODATA...")
state_lookup <- STATE_GEODATA %>%
  st_drop_geometry() %>%
  # We'll rename the "NAME" column to "STATE_NAME", if it exists
  rename(STATE_NAME = NAME, STATE_ABBR = STUSPS) %>%
  select(any_of(c("STATEFP","STATE_ABBR","STATE_NAME")))

COUNTY_GEODATA <- COUNTY_GEODATA %>%
  left_join(state_lookup, by="STATEFP") 

# Safely relocate if these columns exist
COUNTY_GEODATA <- COUNTY_GEODATA %>%
  relocate(any_of(c("STATE_ABBR","STATE_NAME")), .after="STATEFP")

dbg(COUNTY_GEODATA, "COUNTY_GEODATA (with state name/abbr)")

msg("INFO","Creating CBSA_GEODATA from tigris::core_based_statistical_areas() year=2023...")
CBSA_GEODATA <- tigris::core_based_statistical_areas(year=2023, cb=TRUE) %>%
  st_transform(CRS_USE) %>%
  clean_names() %>%
  rename_with(~ toupper(.x), everything()) %>%
  mutate(GEO_ID = as.character(GEOID),
         is_territory = ifelse(str_sub(GEOID, 1, 2) %in% EXCLUDE_TERRITORIES, TRUE, FALSE)) %>%
  filter(!is_territory) %>%
  select(-is_territory)
dbg(CBSA_GEODATA, "CBSA_GEODATA (post-exclusion)")

msg("INFO","Performing st_join for CBSA into COUNTY...")

current_county_data_for_join <- COUNTY_GEODATA

n_rows_total <- nrow(current_county_data_for_join)
n_workers_plan <- future::nbrOfWorkers()
if (is.infinite(n_workers_plan)) n_workers_plan <- future::availableCores()
if (n_workers_plan == 0) n_workers_plan <- 1

desired_rows_per_chunk <- 500
max_chunks_by_rows <- floor(n_rows_total / desired_rows_per_chunk)
actual_num_chunks <- max(1, min(n_workers_plan, max_chunks_by_rows, n_rows_total))

process_joined_chunk <- function(joined_df_chunk) {
  result <- joined_df_chunk %>%
    mutate(
      CBSA_GEOID_NEW = GEO_ID.y,
      CBSA_NAMELSAD_NEW = NAMELSAD.y,
      CBSA_AREA_SQMI_NEW = ALAND.y / 2.589988110336
    ) %>%
    select(
      COUNTY_GEOID    = GEO_ID.x,
      COUNTY_NAME     = NAME.x,
      COUNTY_NAMELSAD = NAMELSAD.x,
      
      GEOID           = GEOID.x,
      MTFCC           = MTFCC.x,
      CSAFP           = CSAFP.x,
      CBSAFP          = CBSAFP.x,
      ALAND           = ALAND.x,
      AWATER          = AWATER.x,
      INTPTLAT        = INTPTLAT.x,
      INTPTLON        = INTPTLON.x,
      
      STATEFP, COUNTYFP, COUNTYNS, AFFGEOID, LSADC, CLASSFP, METDIVFP, FUNCSTAT,
      geometry,
      starts_with("PRESENT_"), 
      any_of(c("STATE_ABBR","STATE_NAME")),
      
      CBSA_GEOID = CBSA_GEOID_NEW,
      CBSA_NAMELSAD = CBSA_NAMELSAD_NEW,
      CBSA_AREA_SQMI = CBSA_AREA_SQMI_NEW
    )
  return(result)
}

if (actual_num_chunks <= 1 || n_rows_total < 1000) {
  msg("INFO", "Performing st_join for CBSA into COUNTY without chunking (data size or workers limit)...")
  temp_join_combined <- st_join(current_county_data_for_join, CBSA_GEODATA, left=TRUE, largest=TRUE)
  COUNTY_GEODATA_updated <- process_joined_chunk(temp_join_combined)
} else {
  msg("INFO", paste0("Performing st_join for CBSA into COUNTY with chunking (", actual_num_chunks, " chunks)..."))
  
  county_data_split <- current_county_data_for_join %>%
    mutate(chunk_id_temp = base::rep(1:actual_num_chunks, length.out = n_rows_total)) %>%
    dplyr::group_by(chunk_id_temp) %>%
    dplyr::group_split(.keep = FALSE)
  
  # IMPORTANT: no repeated .options argument
  list_of_processed_chunks <- future_map(
    county_data_split,
    function(county_chunk, cbsa_full_data) {
      joined_chunk <- st_join(county_chunk, cbsa_full_data, left = TRUE, largest = TRUE)
      process_joined_chunk(joined_chunk)
    },
    cbsa_full_data = CBSA_GEODATA,
    .options = furrr_options(
      seed = TRUE,
      packages = c("sf", "dplyr", "stringr", "tidyr")
    )
  )
  
  COUNTY_GEODATA_updated <- bind_rows(list_of_processed_chunks)
}

COUNTY_GEODATA <- COUNTY_GEODATA_updated
dbg(COUNTY_GEODATA, "COUNTY_GEODATA (final, incl. CBSA columns, potentially chunked join)")

msg("INFO","Creating CD119_GEODATA from ArcGIS REST service...")
CD119_GEODATA <- tryCatch({
  esri2sf::esri2sf("https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/USA_119th_Congressional_Districts_no_territories/FeatureServer/0") %>%
    st_transform(CRS_USE) %>%
    clean_names() %>%
    rename_with(~ toupper(.x), everything()) %>%
    mutate(GEO_ID = as.character(DISTRICTID),
           stf = substr(GEO_ID, 1, 2),
           is_territory = stf %in% EXCLUDE_TERRITORIES) %>%
    filter(!is_territory) %>%
    select(-stf, -is_territory)
}, error=function(e){
  warning("CD119_GEODATA fetch failed: ", e$message)
  NULL
})
if (!is.null(CD119_GEODATA)) dbg(CD119_GEODATA, "CD119_GEODATA") else msg("WARN", "CD119_GEODATA is NULL due to fetch error.")

msg("INFO","Creating EDD_GEODATA from ArcGIS REST service...")
EDD_GEODATA <- tryCatch({
  esri2sf::esri2sf("https://services8.arcgis.com/iSa23B1d0nMdPKnp/ArcGIS/rest/services/EDD_Layer_October_2024/FeatureServer/0") %>%
    st_transform(CRS_USE) %>%
    clean_names() %>%
    rename_with(~ toupper(.x), everything()) %>%
    mutate(GEO_ID = as.character(FID))
}, error=function(e){
  warning("EDD_GEODATA fetch failed: ", e$message)
  NULL
})
if (!is.null(EDD_GEODATA)) dbg(EDD_GEODATA, "EDD_GEODATA") else msg("WARN", "EDD_GEODATA is NULL due to fetch error.")

msg("INFO","Creating AIANNHA_GEODATA from tigris::native_areas(year=2023)...")
AIANNHA_GEODATA <- tigris::native_areas(year=2023, cb=TRUE) %>%
  st_transform(CRS_USE) %>%
  clean_names() %>%
  rename_with(~ toupper(.x), everything()) %>%
  mutate(GEO_ID = as.character(GEOID))
# Not all tigris native_areas have STATEFP, so filter only if present
if ("STATEFP" %in% names(AIANNHA_GEODATA)) {
  AIANNHA_GEODATA <- AIANNHA_GEODATA %>%
    mutate(is_territory = STATEFP %in% EXCLUDE_TERRITORIES) %>%
    filter(!is_territory) %>%
    select(-is_territory)
}
dbg(AIANNHA_GEODATA, "AIANNHA_GEODATA")

msg("INFO","Core Geographies creation/merges completed.")

# ------------------------------------------------------------------------------
# 3) COUNTY-LEVEL DATA
# ------------------------------------------------------------------------------
msg("INFO","Starting County-Level Data processing...")

msg("INFO","Loading HEADWATERS_COUNTY_DATA (Excel)...")
hdw <- file.path(RAW_DATA_FOLDER,"HE_Rural_Capacity_Index_Feb_2025_Download_Data.xlsx")
HEADWATERS_COUNTY_DATA <- tibble()
if (file.exists(hdw)) {
  headwaters_county_data <- readxl::read_excel(hdw, sheet="County")
  HEADWATERS_COUNTY_DATA <- headwaters_county_data %>%
    mutate(FIPS = str_pad(as.character(FIPS), 5, pad="0")) %>%
    rename(COUNTY_GEOID = FIPS) %>%
    select(-any_of("GEOIDFQ"))
} else {
  msg("WARN", paste("HEADWATERS_COUNTY_DATA file not found:", hdw))
}
dbg(HEADWATERS_COUNTY_DATA)

msg("INFO","Loading EDCI_COUNTY_INDICATORS_2024 (Excel)...")
edci_file <- file.path(RAW_DATA_FOLDER,"Economic_Development_Capacity_Index_Snapshot_August_2024.xlsx")
EDCI_COUNTY_INDICATORS_2024 <- tibble()
if (file.exists(edci_file)) {
  rename_indicator_columns <- function(df, dictionary, suffix_pattern, suffix_label) {
    ig <- c("county_fips","county","state_fips","state","area_land","area_water",
            "latitude","longitude","total_population","total_households")
    old <- names(df)
    new <- sapply(old, function(col) {
      if (col %in% ig || !str_ends(col, suffix_pattern)) {
        col
      } else {
        ind <- str_remove(col, paste0(suffix_pattern,"$"))
        mr  <- dictionary %>% filter(indicator == ind)
        if (nrow(mr) == 1 && !is.na(mr$definition[1])) {
          paste0(mr$definition[1], suffix_label)
        } else col
      }
    }, USE.NAMES=FALSE)
    setNames(df, make.unique(new, sep=" "))
  }
  {
    edci_dict <- read_excel(edci_file, sheet="data_dictionary") %>%
      select(indicator, definition) %>%
      distinct() %>%
      arrange(indicator)
    dbg(edci_dict, "edci_dict")
    
    edci_data_ind <- read_excel(edci_file, sheet="indicators") %>%
      rename_indicator_columns(edci_dict,"_unit_value"," Value")
    dbg(edci_data_ind, "edci_data_ind (post rename)")
    
    edci_combined_data <- edci_data_ind
    dbg(edci_combined_data, "edci_combined_data (effectively edci_data_ind)")
    
    EDCI_COUNTY_INDICATORS_2024 <- edci_combined_data %>%
      select(-any_of(c("area_land", "area_water", "latitude", "longitude")))
  }
} else {
  msg("WARN", paste("EDCI_COUNTY_INDICATORS_2024 file not found:", edci_file))
}
dbg(EDCI_COUNTY_INDICATORS_2024)

msg("INFO","Loading STATSAMERICA_INNOVATION_COUNTIES (CSV => fread => as_tibble)...")
st_cnty <- file.path(RAW_DATA_FOLDER,"StatsAmerica_Innovation_Intelligence",
                     "statsamerica_innovation_intelligence_2023_measures_counties_pivoted.csv")
STATSAMERICA_INNOVATION_COUNTIES <- tryCatch({
  data.table::fread(st_cnty, showProgress=FALSE) %>%
    as_tibble()
}, error=function(e){
  warning("Error reading STATSAMERICA_INNOVATION_COUNTIES CSV: ", e$message)
  tibble()
})
dbg(STATSAMERICA_INNOVATION_COUNTIES)

msg("INFO","Loading NREL_TECHNICAL_POTENTIAL_COUNTY (CSV => fread => as_tibble)...")
nrel_county <- file.path(RAW_DATA_FOLDER,"NREL SLOPE","techpot_baseline","techpot_baseline_county.csv")
NREL_TECHNICAL_POTENTIAL_COUNTY <- tryCatch({
  data.table::fread(nrel_county, showProgress=FALSE) %>%
    as_tibble()
}, error=function(e){
  warning("Error reading NREL county CSV: ", e$message)
  tibble()
})
dbg(NREL_TECHNICAL_POTENTIAL_COUNTY)

msg("INFO","Pivoting NREL_TECHNICAL_POTENTIAL_COUNTY...")
if (nrow(NREL_TECHNICAL_POTENTIAL_COUNTY) > 0) {
  NREL_TECHNICAL_POTENTIAL_COUNTY_PIVOTED <- NREL_TECHNICAL_POTENTIAL_COUNTY %>%
    select(`County Name`, `State Name`, Technology,
           `Technical Generation Potential - MWh MWh`) %>%
    pivot_wider(
      id_cols = c(`County Name`, `State Name`),
      names_from = Technology,
      values_from = `Technical Generation Potential - MWh MWh`,
      names_prefix = "Technical Generation Potential - MWh: "
    )
} else {
  NREL_TECHNICAL_POTENTIAL_COUNTY_PIVOTED <- tibble(`County Name`=character(), `State Name`=character())
}
dbg(NREL_TECHNICAL_POTENTIAL_COUNTY_PIVOTED)

msg("INFO","Loading CGT_COUNTY_NAICS6D (CSV => fread => as_tibble)...")
cgt_cnty <- file.path(DATA_FOLDER,"CGT_county_data","modified_CGT_files","CGT_NAICS6D_COUNTY_LONG.csv")
CGT_COUNTY_NAICS6D <- tryCatch({
  data.table::fread(cgt_cnty, showProgress=FALSE) %>%
    as_tibble()
}, error=function(e){
  warning("Error reading CGT_COUNTY_NAICS6D: ", e$message)
  tibble()
})
dbg(CGT_COUNTY_NAICS6D)

msg("INFO","Loading CZ_COMMUTING_ZONE_DATA via direct URL => fread => as_tibble...")
cz_tmp <- tempfile(fileext = ".csv")
CZ_COMMUTING_ZONE_DATA <- tryCatch({
  download.file("https://datawrapper.dwcdn.net/signH/1/dataset.csv", cz_tmp, quiet=TRUE, mode="wb")
  cz_data <- data.table::fread(cz_tmp, showProgress=FALSE) %>%
    as_tibble()
  
  cz_data %>%
    select(countyfips, ctyname2000, imp) %>%
    mutate(countyfips = ifelse(nchar(as.character(countyfips))==4,
                               paste0("0", countyfips),
                               as.character(countyfips))) %>%
    rename(`County GEOID (2020)` = countyfips,
           `County Name (2020)`  = ctyname2000,
           `Commuting-Zone Import Penetration Change (2000-2012)` = imp)
}, error = function(e) {
  warning("Could not download or process CZ_COMMUTING_ZONE_DATA: ", e$message)
  tibble()
}, finally = {
  if (file.exists(cz_tmp)) unlink(cz_tmp)
})
dbg(CZ_COMMUTING_ZONE_DATA)

msg("INFO","Processing COUNTY_CDFI_NMTC_ELIGIBILITY...")
true_vals <- c(1,"1","Y","Yes","YES","T","TRUE",TRUE)
nmtc_path <- file.path(RAW_DATA_FOLDER,"CDFI and NMTC Eligibility Mapping",
                       "NMTC_2016-2020_ACS_LIC_12-23-2024b(2016-2020).csv")
cdfi_path <- file.path(RAW_DATA_FOLDER,"CDFI and NMTC Eligibility Mapping",
                       "CDFI_Investment_Areas_ACS_2016_2020(DATA).csv")

nmtc_processed <- tibble()
if(file.exists(nmtc_path)){
  nmtc_raw <- data.table::fread(nmtc_path, showProgress=FALSE) %>% as_tibble() %>% clean_names()
  tract_geoid_col_name <- grep("tract.*geoid", names(nmtc_raw), ignore.case=TRUE, value=TRUE)[1]
  if(!is.na(tract_geoid_col_name) && nzchar(tract_geoid_col_name)){
    nmtc_processed <- nmtc_raw %>%
      filter(!state_name %in% c("Puerto Rico", EXCLUDE_TERRITORIES)) %>%
      mutate(county_geoid = substr(.data[[tract_geoid_col_name]], 1, 5),
             nmtc_any = if_any(matches("qualify.*(poverty|median_family)"), ~ .x %in% true_vals)) %>%
      select(county_geoid, nmtc_any)
  } else {
    warning("Could not find tract geoid column in NMTC data.")
  }
} else {
  warning("NMTC file not found: ", nmtc_path)
}
dbg(nmtc_processed, "nmtc_processed")

cdfi_processed <- tibble()
if(file.exists(cdfi_path)){
  cdfi_raw <- data.table::fread(cdfi_path, showProgress=FALSE) %>% as_tibble() %>% clean_names()
  if("ct2020" %in% names(cdfi_raw) && "ia2020" %in% names(cdfi_raw) && "statename2020" %in% names(cdfi_raw)) {
    cdfi_processed <- cdfi_raw %>%
      mutate(county_geoid = substr(ct2020, 1, 5),
             cdfi_flag    = ia2020 %in% true_vals) %>%
      filter(!statename2020 %in% c("Puerto Rico", EXCLUDE_TERRITORIES)) %>%
      select(county_geoid, cdfi_flag)
  } else {
    warning("CDFI data missing required columns: ct2020, ia2020, or statename2020.")
  }
} else {
  warning("CDFI file not found: ", cdfi_path)
}
dbg(cdfi_processed, "cdfi_processed")

if(nrow(nmtc_processed) > 0 && nrow(cdfi_processed) > 0) {
  COUNTY_CDFI_NMTC_ELIGIBILITY <- full_join(
    nmtc_processed %>% group_by(county_geoid) %>%
      summarise(NMTC_PCT = round(100*mean(nmtc_any,na.rm=TRUE),2), .groups="drop"),
    cdfi_processed %>% group_by(county_geoid) %>%
      summarise(CDFI_PCT = round(100*mean(cdfi_flag,na.rm=TRUE),2), .groups="drop"),
    by="county_geoid"
  ) %>%
    rename(COUNTY_GEOID = county_geoid,
           `Percent Tracts Eligible (2016-2020 ACS): New Markets Tax Credit (NMTC)` = NMTC_PCT,
           `Community Development Financial Institution (CDFI) Investment Area: Percent Tracts Qualifying (2016-2020 ACS)` = CDFI_PCT)
} else if (nrow(nmtc_processed) > 0) {
  COUNTY_CDFI_NMTC_ELIGIBILITY <- nmtc_processed %>% group_by(county_geoid) %>%
    summarise(NMTC_PCT = round(100*mean(nmtc_any,na.rm=TRUE),2), .groups="drop") %>%
    rename(COUNTY_GEOID = county_geoid,
           `Percent Tracts Eligible (2016-2020 ACS): New Markets Tax Credit (NMTC)` = NMTC_PCT) %>%
    mutate(`Community Development Financial Institution (CDFI) Investment Area: Percent Tracts Qualifying (2016-2020 ACS)` = NA_real_)
} else if (nrow(cdfi_processed) > 0) {
  COUNTY_CDFI_NMTC_ELIGIBILITY <- cdfi_processed %>% group_by(county_geoid) %>%
    summarise(CDFI_PCT = round(100*mean(cdfi_flag,na.rm=TRUE),2), .groups="drop") %>%
    rename(COUNTY_GEOID = county_geoid,
           `Community Development Financial Institution (CDFI) Investment Area: Percent Tracts Qualifying (2016-2020 ACS)` = CDFI_PCT) %>%
    mutate(`Percent Tracts Eligible (2016-2020 ACS): New Markets Tax Credit (NMTC)` = NA_real_)
} else {
  COUNTY_CDFI_NMTC_ELIGIBILITY <- tibble(COUNTY_GEOID=character())
}
dbg(COUNTY_CDFI_NMTC_ELIGIBILITY)

msg("INFO","Loading COUNTY_PROPERTY_VALUES (CSV => fread => as_tibble)...")
cpv <- file.path(RAW_DATA_FOLDER,"county_property_values.csv")
COUNTY_PROPERTY_VALUES <- tryCatch({
  data.table::fread(cpv, showProgress=FALSE) %>%
    as_tibble()
}, error=function(e){
  warning("Error reading county_property_values: ", e$message)
  tibble()
})
dbg(COUNTY_PROPERTY_VALUES)

msg("INFO","Loading MEDIAN_PROPERTY_TAXES_BY_COUNTY_2023 from datawrapper => read_csv ...")
MEDIAN_PROPERTY_TAXES_BY_COUNTY_2023 <- {
  chart_id <- "xQ5ws"
  base_url <- sprintf("https://datawrapper.dwcdn.net/%s/", chart_id)
  data_df <- tibble()
  html_txt <- tryCatch(
    httr::GET(base_url, httr::timeout(30)) |> httr::content(as="text", encoding="UTF-8"),
    error = function(e) {
      warning("Could not retrieve HTML for property taxes datawrapper page: ", e$message)
      NA_character_
    }
  )
  if(!is.na(html_txt) && nzchar(html_txt)) {
    edition_match <- stringr::str_match(html_txt, sprintf("/%s/(\\d+)/dataset\\.csv", chart_id))
    if (is.na(edition_match[1,2])) {
      edition_match <- stringr::str_match(html_txt, sprintf("/%s/(\\d+)/", chart_id))
    }
    
    if (!is.na(edition_match[1,2])) {
      edition <- edition_match[1,2]
      csv_url <- sprintf("https://datawrapper.dwcdn.net/%s/%s/dataset.csv", chart_id, edition)
      data_df <- tryCatch({
        readr::read_csv(csv_url, show_col_types=FALSE)
      }, error = function(e_csv) {
        warning("Could not read property tax CSV from datawrapper: ", e_csv$message)
        tibble()
      })
    } else {
      warning("Could not detect edition number for Datawrapper chart ‘", chart_id, "’.")
    }
  }
  data_df
}

if (nrow(MEDIAN_PROPERTY_TAXES_BY_COUNTY_2023) > 0) {
  MEDIAN_PROPERTY_TAXES_BY_COUNTY_2023 <- MEDIAN_PROPERTY_TAXES_BY_COUNTY_2023 %>%
    rename(
      `County GEOID` = any_of(c("geoid", "GEOID")),
      `County Name` = any_of(c("name", "NAME")),
      `Median Housing Value (2022)` = any_of("med_housing_value_22"),
      `Median Property Tax Paid (2022)` = any_of("med_prop_tax_paid_22"),
      `Effective Property Tax Rate (2022)` = any_of("effective_prop_tax_rate_22"),
      `Median Housing Value (2023)` = any_of("med_housing_value_23"),
      `Median Property Tax Paid (2023)` = any_of("med_prop_tax_paid_23"),
      `Effective Property Tax Rate (2023)` = any_of("effective_prop_tax_rate_23"),
      `Percent Change in Housing Value (2022-2023)` = any_of("ch_22_23"),
      `Real Percent Change in Housing Value (2022-2023)` = any_of("ch_22_23_real")
    )
}
dbg(MEDIAN_PROPERTY_TAXES_BY_COUNTY_2023)
msg("INFO","County-Level Data processing completed.")

# ------------------------------------------------------------------------------
# 4) STATE-LEVEL DATA
# ------------------------------------------------------------------------------
msg("INFO","Starting State-Level Data processing...")

msg("INFO","Loading NREL_TECHNICAL_POTENTIAL_STATE_WIDE (CSV => fread => as_tibble)...")
nrel_state_file <- file.path(RAW_DATA_FOLDER,"NREL SLOPE","techpot_baseline","techpot_baseline_state.csv")
NREL_TECHPOT_STATE_RAW <- tryCatch({
  data.table::fread(nrel_state_file, showProgress=FALSE) %>%
    as_tibble()
}, error=function(e){
  warning("Could not read NREL state CSV: ", e$message)
  tibble()
})
dbg(NREL_TECHPOT_STATE_RAW)

if (nrow(NREL_TECHPOT_STATE_RAW) > 0) {
  NREL_TECHNICAL_POTENTIAL_STATE_WIDE <- NREL_TECHPOT_STATE_RAW %>%
    select(-any_of("Geography ID")) %>%
    pivot_wider(
      id_cols = c(`State Name`),
      names_from = Technology,
      values_from = `Technical Generation Potential - MWh MWh`,
      names_prefix = "Technical Generation Potential - MWh: "
    )
  all_possible_states <- c(datasets::state.name, "District of Columbia")
  missing_states <- setdiff(all_possible_states, NREL_TECHNICAL_POTENTIAL_STATE_WIDE$`State Name`)
  if(length(missing_states)>0){
    to_add <- tibble(`State Name` = missing_states)
    NREL_TECHNICAL_POTENTIAL_STATE_WIDE <- bind_rows(NREL_TECHNICAL_POTENTIAL_STATE_WIDE, to_add)
  }
} else {
  NREL_TECHNICAL_POTENTIAL_STATE_WIDE <- tibble(`State Name` = character())
}
dbg(NREL_TECHNICAL_POTENTIAL_STATE_WIDE)

msg("INFO","Loading STATSAMERICA_INNOVATION_STATES (CSV => fread => as_tibble)...")
st_st <- file.path(RAW_DATA_FOLDER,"StatsAmerica_Innovation_Intelligence",
                   "statsamerica_innovation_intelligence_2023_measures_states_pivoted.csv")
STATSAMERICA_INNOVATION_STATES <- tryCatch({
  data.table::fread(st_st, showProgress=FALSE) %>%
    as_tibble()
}, error=function(e){
  warning("Error reading STATSAMERICA_INNOVATION_STATES: ", e$message)
  tibble()
})
dbg(STATSAMERICA_INNOVATION_STATES)

msg("INFO","Loading XCHANGE_STATE_CLIMATE_POLICY (Excel)...")
clim_file <- file.path(RAW_DATA_FOLDER,"State Climate Policy Dashboard - Full Download (1.28.25) 2.xlsx")
XCHANGE_STATE_CLIMATE_POLICY <- tibble(State = character())
if (file.exists(clim_file)) {
  shs <- c("Cross-Sector","Electricity","Buildings and Efficiency","Transportation",
           "Natural and Working Lands","Industry, Materials, and Waste ")
  
  actual_sheet_names <- tryCatch(readxl::excel_sheets(clim_file), error = function(e) NULL)
  
  if (!is.null(actual_sheet_names)) {
    shs_to_read <- intersect(trimws(shs), trimws(actual_sheet_names))
    missing_shs <- setdiff(trimws(shs), shs_to_read)
    if(length(missing_shs) > 0) {
      msg("WARN", paste("Following sheets not found in XCHANGE_STATE_CLIMATE_POLICY Excel:", paste(missing_shs, collapse=", ")))
    }
    
    if(length(shs_to_read) > 0) {
      plong <- purrr::map_dfr(shs_to_read, ~ read_excel(clim_file, sheet=.x) %>%
                                select(any_of(c("State", "Policy", "Policy Status"))))
      dbg(plong, "XCHANGE_STATE_CLIMATE_POLICY_long")
      if (nrow(plong) > 0 && all(c("State", "Policy", "Policy Status") %in% names(plong))) {
        XCHANGE_STATE_CLIMATE_POLICY <- plong %>%
          tidyr::pivot_wider(id_cols=State, names_from=Policy, values_from=`Policy Status`) %>%
          mutate(across(-State, ~ factor(.x, levels=c("not-enacted","enacted","in-progress","partially-enacted"))))
      } else {
        msg("WARN", "XCHANGE_STATE_CLIMATE_POLICY long format data is empty or missing required columns.")
      }
    } else {
      msg("WARN", "No valid sheets found to read for XCHANGE_STATE_CLIMATE_POLICY.")
    }
  } else {
    msg("WARN", paste("Could not read sheet names from XCHANGE_STATE_CLIMATE_POLICY Excel:", clim_file))
  }
} else {
  msg("WARN", paste("XCHANGE_STATE_CLIMATE_POLICY Excel file not found:", clim_file))
}
dbg(XCHANGE_STATE_CLIMATE_POLICY)

msg("INFO","Loading CGT_STATE_INDUSTRY_COMPLEXITY_2022 (CSV => fread => as_tibble)...")
cgt_state_path <- file.path(DATA_FOLDER,"CGT_county_data","cbsa_state_upscale_attempt",
                            "final_state_industry_employment_complexity_estimates_2022_pivoted.csv")
CGT_STATE_INDUSTRY_COMPLEXITY_2022 <- tryCatch({
  data.table::fread(cgt_state_path, showProgress=FALSE) %>%
    as_tibble()
}, error=function(e){
  warning("Could not read CGT_STATE_INDUSTRY_COMPLEXITY_2022: ", e$message)
  tibble()
})
dbg(CGT_STATE_INDUSTRY_COMPLEXITY_2022)

msg("INFO","Loading CNBC_2024_TOP_STATES (web HTML parse => table => mutate)...")
url_cnbc <- "https://www.cnbc.com/2024/07/11/americas-top-states-for-business-full-rankings.html"
CNBC_2024_TOP_STATES <- tryCatch({
  page_html <- read_html(url_cnbc)
  html_table_node <- html_element(page_html, "table")
  
  if (is.na(html_table_node)) {
    warning("No table found on CNBC page.")
    tibble(State=character())
  } else {
    df_cnbc <- html_table_node %>%
      html_table(fill=TRUE) %>%
      rename_with(~ gsub("[^A-Za-z0-9_]+","_",.x)) %>%
      mutate(across(everything(), trimws))
    
    if (!"State" %in% names(df_cnbc)) {
      if (length(names(df_cnbc)) > 0 && any(df_cnbc[[1]] %in% datasets::state.name)) {
        names(df_cnbc)[1] <- "State"
      } else {
        warning("CNBC table parsed but 'State' column not found or auto-detected.")
        return(tibble(State=character()))
      }
    }
    
    rc <- setdiff(names(df_cnbc),"State")
    n_cnbc <- nrow(df_cnbc)
    
    if (n_cnbc > 0 && length(rc) > 0) {
      df_cnbc <- df_cnbc %>%
        mutate(across(all_of(rc), ~ suppressWarnings(as.integer(as.character(.))))) %>%
        mutate(across(all_of(rc), ~ ifelse(is.na(.x), NA_integer_,
                                           as.integer(round((n_cnbc - .x + 1)/n_cnbc * 100))))) %>%
        rename_with(~paste0("CNBC 2024 Top States for Business: ",
                            str_to_title(str_replace_all(str_trim(.x),"_"," ")),
                            " Score"), all_of(rc)) %>%
        relocate(State, .before=everything())
    } else if (n_cnbc == 0) {
      warning("CNBC table parsed but was empty.")
      df_cnbc <- tibble(State=character())
    }
    df_cnbc
  }
}, error=function(e){
  warning("Could not parse CNBC table: ", e$message)
  tibble(State=character())
})
dbg(CNBC_2024_TOP_STATES)

msg("INFO","Parallelizing reading of EIG_STATE_DYNAMISM_2021 from multiple sheets ...")
url_eig <- "https://eig.org/state-dynamism-2023/assets/Downloadable-Data-EIG-Index-of-State-Dynamism-2021.xlsx"
eig_local <- file.path(tempdir(), "dynamism.xlsx")

if(!file.exists(eig_local)){
  tryCatch({
    download.file(url_eig, eig_local, mode="wb", quiet=TRUE)
    msg("DEBUG","EIG dynamism.xlsx downloaded.")
  }, error=function(e) {
    warning("Could not download EIG xlsx: ", e$message)
    if(file.exists(eig_local)) unlink(eig_local)
  })
} else {
  msg("DEBUG",glue::glue("EIG dynamism.xlsx already in tempdir(): {eig_local}"))
}

EIG_STATE_DYNAMISM_2021 <- tibble(State=character())

if (file.exists(eig_local)) {
  target_sheets <- c("ISD Scores","Core Startup Rate","Jobs in Young Firms","Worker churn",
                     "Labor Force Participation","Housing Permits per capita","Migration",
                     "Inventors per capita")
  
  available_sheets_eig <- tryCatch(readxl::excel_sheets(eig_local), error = function(e) NULL)
  if (!is.null(available_sheets_eig)) {
    sheets_to_process_eig <- intersect(target_sheets, available_sheets_eig)
    missing_sheets_eig <- setdiff(target_sheets, sheets_to_process_eig)
    if (length(missing_sheets_eig) > 0) {
      msg("WARN", paste("EIG sheets not found:", paste(missing_sheets_eig, collapse = ", ")))
    }
    
    process_eig_sheet <- function(sheet_name, file_path_param) {
      df <- tryCatch({
        read_excel(file_path_param, sheet=sheet_name)
      }, error=function(e){
        tibble()
      })
      
      required_cols <- c("State","1992","2021")
      if(nrow(df)>0 && all(required_cols %in% names(df))) {
        df <- df %>%
          select(all_of(required_cols)) %>%
          filter(!is.na(State) & nzchar(trimws(as.character(State)))) %>%
          mutate(
            `1992` = as.numeric(as.character(`1992`)),
            `2021` = as.numeric(as.character(`2021`)),
            CHANGE = `2021` - `1992`,
            PCT_CHANGE = if_else(`1992` == 0 | is.na(`1992`), NA_real_, (CHANGE/`1992`)*100)
          ) %>%
          rename_with(
            ~paste0(
              if(sheet_name=="ISD Scores") "Index of State Dynamism (EIG)" else sheet_name,
              if(sheet_name=="ISD Scores") " Overall State Score: 2021"
              else ", 2021, Index of State Dynamism (EIG)"
            ),
            .cols=`2021`
          ) %>%
          rename_with(
            ~paste0(
              if(sheet_name=="ISD Scores")
                "Index of State Dynamism (EIG), Change in Overall State Score, 1992-2021"
              else paste0(sheet_name,", Change in Overall State Score, 1992-2021, Index of State Dynamism (EIG)")
            ),
            .cols=CHANGE
          ) %>%
          rename_with(
            ~paste0(
              if(sheet_name=="ISD Scores")
                "Index of State Dynamism (EIG), Percent Change in Overall State Score, 1992-2021"
              else paste0(sheet_name,", Percent Change in Overall State Score, 1992-2021, Index of State Dynamism (EIG)")
            ),
            .cols=PCT_CHANGE
          ) %>%
          select(-`1992`)
      } else {
        df <- tibble(State=character())
      }
      df
    }
    
    if (length(sheets_to_process_eig) > 0) {
      EIG_STATE_DYNAMISM_LIST <- future_map(sheets_to_process_eig, process_eig_sheet,
                                            file_path_param = eig_local,
                                            .options = furrr_options(
                                              seed=TRUE,
                                              packages=c("readxl", "dplyr", "tidyr", "stringr")
                                            ))
      
      EIG_STATE_DYNAMISM_LIST_VALID <- Filter(function(x) !is.null(x) && nrow(x) > 0 && "State" %in% names(x), EIG_STATE_DYNAMISM_LIST)
      
      if(length(EIG_STATE_DYNAMISM_LIST_VALID) > 0) {
        EIG_STATE_DYNAMISM_2021 <- Reduce(function(x,y) full_join(x,y, by="State"), EIG_STATE_DYNAMISM_LIST_VALID)
      } else {
        msg("WARN", "No valid EIG sheets were processed successfully.")
      }
    } else {
      msg("WARN", "No EIG sheets to process after checking availability.")
    }
  } else {
    msg("WARN", "Could not read sheet names from local EIG Excel file.")
  }
} else {
  msg("WARN", "Local EIG Excel file does not exist. Skipping EIG data processing.")
}
dbg(EIG_STATE_DYNAMISM_2021, "EIG_STATE_DYNAMISM_2021")

msg("INFO","State-Level Data processing completed.")

# ------------------------------------------------------------------------------
# 5) CBSA-LEVEL DATA
# ------------------------------------------------------------------------------
msg("INFO","Starting CBSA-Level Data processing...")

msg("INFO","Loading STATSAMERICA_INNOVATION_METROS (CSV => fread => as_tibble)...")
st_m <- file.path(RAW_DATA_FOLDER,"StatsAmerica_Innovation_Intelligence",
                  "Innovation Intelligence - Measures - Metros.csv")
STATSAMERICA_INNOVATION_METROS_RAW <- tryCatch({
  data.table::fread(st_m, showProgress=FALSE) %>%
    as_tibble()
}, error=function(e){
  warning("Could not read STATSAMERICA_INNOVATION_METROS: ", e$message)
  tibble()
})
dbg(STATSAMERICA_INNOVATION_METROS_RAW)

if(nrow(STATSAMERICA_INNOVATION_METROS_RAW) > 0 &&
   all(c("time_id", "geo_id", "description", "Code Description", "Measure Value") %in% names(STATSAMERICA_INNOVATION_METROS_RAW))) {
  STATSAMERICA_INNOVATION_METROS <- STATSAMERICA_INNOVATION_METROS_RAW %>%
    rename(AREA_NAME=description) %>%
    filter(time_id==2023) %>%
    pivot_wider(
      id_cols=c(geo_id, AREA_NAME),
      names_from=`Code Description`,
      values_from=`Measure Value`
    )
} else {
  msg("WARN", "STATSAMERICA_INNOVATION_METROS_RAW is empty or missing required columns for pivoting.")
  STATSAMERICA_INNOVATION_METROS <- tibble(geo_id=character(), AREA_NAME=character())
}
dbg(STATSAMERICA_INNOVATION_METROS)

msg("INFO","Loading CGT_CBSA_INDUSTRY_COMPLEXITY_2022 (CSV => fread => as_tibble)...")
cgt_cbsa_path <- file.path(DATA_FOLDER,"CGT_county_data","cbsa_state_upscale_attempt",
                           "final_cbsa_industry_employment_complexity_estimates_2022_pivoted.csv")
CGT_CBSA_INDUSTRY_COMPLEXITY_2022 <- tryCatch({
  data.table::fread(cgt_cbsa_path, showProgress=FALSE) %>%
    as_tibble()
}, error=function(e){
  warning("Could not read CGT_CBSA_INDUSTRY_COMPLEXITY_2022: ", e$message)
  tibble()
})
dbg(CGT_CBSA_INDUSTRY_COMPLEXITY_2022)

msg("INFO","CBSA-Level Data processing completed.")

# ------------------------------------------------------------------------------
# 6) GEOCODING-RELATED DATA
# ------------------------------------------------------------------------------
msg("INFO","Starting Geocoding-Related Data processing...")

msg("INFO","Loading SEMICONDUCTOR_MANUFACTURING_INVESTMENT (Datawrapper) ...")
SEMICONDUCTOR_MANUFACTURING_INVESTMENT <- {
  chart_id <- "u1vJC"
  base_url <- sprintf("https://datawrapper.dwcdn.net/%s/", chart_id)
  df <- tibble()
  html_txt_semi <- tryCatch({
    httr::GET(base_url, httr::timeout(30)) |> httr::content(as="text", encoding="UTF-8")
  }, error=function(e){
    warning("Could not retrieve HTML for semiconductor Datawrapper page: ", e$message)
    NA_character_
  })
  
  if(!is.na(html_txt_semi) && nzchar(html_txt_semi)){
    edition_semi <- stringr::str_match(html_txt_semi, sprintf("/%s/(\\d+)/dataset\\.csv", chart_id))[,2]
    if (is.na(edition_semi)) {
      edition_semi <- stringr::str_match(html_txt_semi, sprintf("/%s/(\\d+)/", chart_id))[,2]
    }
    
    if (!is.na(edition_semi)){
      csv_url_semi <- sprintf("https://datawrapper.dwcdn.net/%s/%s/dataset.csv", chart_id, edition_semi)
      df <- tryCatch({
        data.table::fread(csv_url_semi, showProgress=FALSE) %>% as_tibble()
      }, error=function(e_csv){
        warning("Could not read SEMICONDUCTOR_MANUFACTURING_INVESTMENT CSV from Datawrapper: ", e_csv$message)
        tibble()
      })
    } else {
      warning("Could not detect edition number for semiconductor Datawrapper chart '", chart_id, "'.")
    }
  }
  
  if(nrow(df) > 0 && "Project Size ($)" %in% names(df)){
    df <- df %>% mutate(
      `Project Size ($)` = as.numeric(gsub("[\\$,]", "", `Project Size ($)`))
    )
  }
  df
}
dbg(SEMICONDUCTOR_MANUFACTURING_INVESTMENT)

msg("INFO","Loading CLEAN_INVESTMENT_MONITOR_FACILITIES (CSV => fread => as_tibble, skip=5)...")
CLEAN_INVESTMENT_MONITOR_FACILITIES <- {
  cim_file <- file.path(RAW_DATA_FOLDER, "clean_investment_monitor_q4_2024",
                        "manufacturing_energy_and_industry_facility_metadata.csv")
  df <- tibble()
  if(!file.exists(cim_file)){
    warning("CIM Facilities CSV not found at: ", cim_file)
  } else {
    df_raw <- tryCatch(
      data.table::fread(cim_file, skip=5, showProgress=FALSE, check.names = TRUE) %>% as_tibble(),
      error = function(e) {
        warning("Error reading CIM Facilities CSV: ", e$message)
        NULL
      }
    )
    if (!is.null(df_raw) && nrow(df_raw) > 0) {
      df <- df_raw %>%
        mutate(
          Announcement_Date   = suppressWarnings(as.Date(Announcement_Date, format="%Y-%m-%d")),
          Construction_Start  = suppressWarnings(as.Date(Construction_Start, format="%Y-%m-%d")),
          Construction_End    = suppressWarnings(as.Date(Construction_End, format="%Y-%m-%d")),
          Announcement_Year   = ifelse(!is.na(Announcement_Date), format(Announcement_Date, "%Y"), NA_character_),
          Announcement_Quarter= ifelse(!is.na(Announcement_Date), 
                                       paste0(format(Announcement_Date, "%Y"), "-Q",
                                              ceiling(as.numeric(format(Announcement_Date, "%m"))/3)), 
                                       NA_character_),
          Investment_Announced_Post_IRA = ifelse(!is.na(Announcement_Date), 
                                                 Announcement_Date >= as.Date("2022-08-16"), 
                                                 NA)
        )
      
      if("LatLon_Valid" %in% names(df)){
        if (!is.logical(df$LatLon_Valid)) {
          df$LatLon_Valid <- toupper(as.character(df$LatLon_Valid))
          df$LatLon_Valid <- ifelse(df$LatLon_Valid == "NA", NA, df$LatLon_Valid == "TRUE")
        }
        bad_cnt <- sum(df$LatLon_Valid==FALSE, na.rm=TRUE)
        if(bad_cnt>0){
          msg("DEBUG", glue::glue("Filtering out {bad_cnt} rows from CIM with LatLon_Valid==FALSE."))
          df <- df %>% filter(is.na(LatLon_Valid) | LatLon_Valid==TRUE)
        }
      }
      
      keep_cols <- c("unique_id","Segment","Company","Facility_ID","Technology","Subcategory","Decarb_Sector",
                     "Project_Type","Current_Facility_Status","State","Address","Latitude","Longitude",
                     "Investment_Reported_Flag","Estimated_Total_Facility_CAPEX","Announcement_Date",
                     "Production_Date","Construction_Start","Construction_End","Announcement_Year",
                     "county_2020_geoid","Announcement_Quarter","Investment_Announced_Post_IRA")
      df <- df %>% select(any_of(keep_cols))
      
      if("Estimated_Total_Facility_CAPEX" %in% names(df) && is.numeric(df$Estimated_Total_Facility_CAPEX)){
        df <- df %>% mutate(Estimated_Total_Facility_CAPEX = Estimated_Total_Facility_CAPEX * 1e6)
      }
    } else if (is.null(df_raw)) {
      # Already warned
    } else {
      msg("WARN", "CIM facilities data was empty after reading.")
    }
  }
  df
}
dbg(CLEAN_INVESTMENT_MONITOR_FACILITIES)

msg("INFO","Setting up EIA_860M helper functions...")
EIA_860M_SETUP <- {
  TERR <- EXCLUDE_TERRITORIES
  eia_path <- file.path(RAW_DATA_FOLDER, "EIA_860M_March_2025.xlsx")
  
  read_eia <- function(sheet_name, file_path_param) {
    if (!file.exists(file_path_param)) {
      warning(paste("EIA Excel file not found at:", file_path_param, "Cannot read sheet:", sheet_name))
      return(tibble())
    }
    available_sheets <- tryCatch(readxl::excel_sheets(file_path_param), error = function(e) NULL)
    if (is.null(available_sheets) || !sheet_name %in% available_sheets) {
      warning(paste("Sheet", sheet_name, "not found in EIA Excel file:", file_path_param))
      return(tibble())
    }
    readxl::read_excel(file_path_param, sheet=sheet_name, skip=2)
  }
  
  tech_bucket <- function(tech_vector) {
    tech_vector <- as.character(tech_vector)
    tech_vector <- str_trim(tech_vector)
    case_when(
      is.na(tech_vector)                                                            ~ NA_character_,
      str_detect(tech_vector, regex("Steam Coal|Integrated Gasification", ignore_case=TRUE)) ~ "Coal",
      str_detect(tech_vector, regex("Petroleum|Coke", ignore_case=TRUE))                   ~ "Petroleum",
      str_detect(tech_vector, regex("^(Natural Gas|Other Natural Gas)|Compressed Air", ignore_case=TRUE)) ~ "Fossil Methane",
      str_detect(tech_vector, regex("Wind Turbine", ignore_case=TRUE))                     ~ "Wind",
      str_detect(tech_vector, regex("^Solar |Solar Thermal", ignore_case=TRUE))            ~ "Solar",
      str_detect(tech_vector, regex("Geothermal", ignore_case=TRUE))                       ~ "Geothermal",
      str_detect(tech_vector, regex("Biomass|Solid Waste", ignore_case=TRUE))              ~ "Biomass",
      str_detect(tech_vector, regex("Landfill Gas", ignore_case=TRUE))                     ~ "Landfill Methane",
      str_detect(tech_vector, regex("Flywheel", ignore_case=TRUE))                         ~ "Flywheel Storage",
      str_detect(tech_vector, regex("Batteries?", ignore_case=TRUE))                       ~ "Battery Storage",
      str_detect(tech_vector, regex("Pumped Storage", ignore_case=TRUE))                   ~ "Hydroelectric Pumped Storage",
      str_detect(tech_vector, regex("Nuclear", ignore_case=TRUE))                          ~ "Nuclear",
      TRUE                                                                          ~ "Other"
    )
  }
  
  add_bucket <- function(df) {
    if (nrow(df) > 0 && "Technology" %in% names(df)) {
      df <- df %>% mutate(Technology_Category = tech_bucket(Technology))
    } else if (!"Technology" %in% names(df) && nrow(df) > 0) {
      warning("Column 'Technology' not found in EIA data for bucketing.")
      df <- df %>% mutate(Technology_Category = NA_character_)
    }
    df
  }
  
  classify_clean_fossil <- function(tech_vector) {
    tech_vector <- as.character(tech_vector)
    tech_vector <- str_trim(tech_vector)
    case_when(
      is.na(tech_vector) ~ NA_character_,
      tech_vector %in% c("Natural Gas Steam Turbine", "Natural Gas Fired Combined Cycle",
                         "Natural Gas Internal Combustion Engine", "Natural Gas Fired Combustion Turbine",
                         "Conventional Steam Coal", "Petroleum Liquids") ~ "Fossil",
      tech_vector %in% c("Conventional Hydroelectric", "Onshore Wind Turbine", "Offshore Wind Turbine", 
                         "Batteries", "Solar Photovoltaic", "Solar Thermal with Energy Storage",
                         "Hydroelectric Pumped Storage", "Geothermal", "Wood/Wood Waste Biomass",
                         "Nuclear") ~ "Clean",
      str_detect(tech_vector, regex("Coal|Petroleum|Natural Gas", ignore_case=TRUE)) ~ "Fossil",
      str_detect(tech_vector, regex("Solar|Wind|Hydro|Nuclear|Biomass|Geothermal|Batteries", ignore_case=TRUE)) ~ "Clean",
      TRUE ~ "Other"
    )
  }
  
  add_clean_fossil <- function(df) {
    if (nrow(df) > 0 && "Technology" %in% names(df)) {
      df <- df %>% mutate(Clean_Fossil = classify_clean_fossil(Technology))
    } else if (!"Technology" %in% names(df) && nrow(df) > 0) {
      warning("Column 'Technology' not found in EIA data for clean/fossil classification.")
      df <- df %>% mutate(Clean_Fossil = NA_character_)
    }
    df
  }
  
  list(
    TERR=TERR,
    eia_path=eia_path,
    read_eia=read_eia, 
    add_bucket=add_bucket, 
    add_clean_fossil=add_clean_fossil
  )
}
msg("INFO","EIA_860M helper functions defined.")

msg("INFO","Loading EIA_860M_OPERATING, PLANNED, RETIRED, CANCELED...")
EIA_860M_OPERATING <- {
  env <- EIA_860M_SETUP
  env$read_eia("Operating", env$eia_path) %>% env$add_bucket() %>% env$add_clean_fossil()
}
dbg(EIA_860M_OPERATING)

EIA_860M_PLANNED <- {
  env <- EIA_860M_SETUP
  env$read_eia("Planned", env$eia_path) %>% env$add_bucket() %>% env$add_clean_fossil()
}
dbg(EIA_860M_PLANNED)

EIA_860M_RETIRED <- {
  env <- EIA_860M_SETUP
  env$read_eia("Retired", env$eia_path) %>% env$add_bucket() %>% env$add_clean_fossil()
}
dbg(EIA_860M_RETIRED)

EIA_860M_CANCELED <- {
  env <- EIA_860M_SETUP
  tryCatch({
    env$read_eia("Canceled or Postponed", env$eia_path) %>%
      env$add_bucket() %>%
      env$add_clean_fossil()
  }, error=function(e){
    warning("Could not read or process 'Canceled or Postponed' EIA sheet: ", e$message)
    tibble(
      `Entity ID`=numeric(), `Entity Name`=character(), `Plant ID`=numeric(),
      `Plant Name`=character(), `Plant State`=character(), County=character(),
      `Nameplate Capacity (MW)`=numeric(), Technology=character(),
      Latitude=numeric(), Longitude=numeric(),
      Technology_Category=character(), Clean_Fossil=character()
    )
  })
}
dbg(EIA_860M_CANCELED)
msg("INFO","Geocoding-Related Data processing completed.")

# ------------------------------------------------------------------------------
# 7) NSF PATENT DATA
# ------------------------------------------------------------------------------
msg("INFO","Starting NSF Patent Data processing...")

SWBINV_3_FILE <- file.path(RAW_DATA_FOLDER, "Invention, Knowledge Transfer, and Innovation (NSF)", "swbinv-3.xlsx")
SWBINV_1_FILE <- file.path(RAW_DATA_FOLDER, "Invention, Knowledge Transfer, and Innovation (NSF)", "swbinv-1.xlsx")

INDEX_3 <- tibble()
INDEX_1 <- tibble()
NSF_PATENT_DATA_COUNTY <- tibble()
NSF_PATENT_DATA_STATE <- tibble()

if(file.exists(SWBINV_3_FILE)) {
  INDEX_3 <- read_excel(SWBINV_3_FILE, sheet="Index")
  dbg(INDEX_3, "INDEX_3_NSF")
} else {
  warning("NSF swbinv-3.xlsx not found at: ", SWBINV_3_FILE)
}

if(file.exists(SWBINV_1_FILE)) {
  INDEX_1 <- read_excel(SWBINV_1_FILE, sheet="Index")
  dbg(INDEX_1, "INDEX_1_NSF")
} else {
  warning("NSF swbinv-1.xlsx not found at: ", SWBINV_1_FILE)
}

if (nrow(INDEX_3) > 0 || nrow(INDEX_1) > 0) {
  
  STATE_FIPS_LOOKUP <- tidycensus::fips_codes %>%
    distinct(state, state_code, state_name) %>%
    mutate(state_abbr_upper = toupper(state),
           state_name_upper = str_to_upper(state_name))
  dbg(STATE_FIPS_LOOKUP, "STATE_FIPS_LOOKUP_for_NSF")
  
  county_code_to_geoid <- function(code_vector, fips_lookup_param){
    geoids <- rep(NA_character_, length(code_vector))
    m <- str_match(code_vector, "^us-([a-zA-Z]{2})-(\\d{3})$")
    valid_format_indices <- which(!is.na(m[,1]))
    if(length(valid_format_indices) == 0) return(geoids)
    
    st_post_abbrs_input <- toupper(m[valid_format_indices, 2])
    cty_fips_parts <- m[valid_format_indices, 3]
    matched_st_fips <- fips_lookup_param$state_code[match(st_post_abbrs_input, fips_lookup_param$state_abbr_upper)]
    valid_fips_indices_within_subset <- which(!is.na(matched_st_fips))
    original_indices_to_update <- valid_format_indices[valid_fips_indices_within_subset]
    geoids[original_indices_to_update] <- paste0(
      matched_st_fips[valid_fips_indices_within_subset], 
      cty_fips_parts[valid_fips_indices_within_subset]
    )
    geoids
  }
  
  extract_bits_swbinv3 <- function(title_str){
    innov <- str_extract(title_str, "(?<=\\bin\\s)(.+?)(?=,\\s*by)") %>% str_to_title()
    yrs   <- str_extract(title_str, "\\d{4}(–|\\s*to\\s*)\\d{2,4}$")
    list(innov=innov, years=yrs)
  }
  extract_bits_swbinv1 <- function(title_str){
    cat1 <- str_match(title_str, "inventors in ([^,:]+)")[,2]
    if(is.na(cat1)){
      alt <- str_match(title_str, "inventors ([^,:]+)")[,2]
      cat1 <- if(!is.na(alt) && nzchar(trimws(alt))) alt else NA_character_
    }
    cat1 <- ifelse(is.na(cat1) | str_trim(cat1)=="","All Technologies",str_to_title(cat1))
    yrs  <- str_extract(title_str, "\\d{4}(–|\\s*to\\s*)\\d{2,4}$")
    list(innov=cat1, years=yrs)
  }
  
  pad_years_df_list <- function(df_list, all_years_vector){
    purrr::map(df_list, function(df){
      if (is.null(df) || nrow(df) == 0) return(df)
      existing_year_cols <- intersect(all_years_vector, names(df))
      missing_year_cols <- setdiff(all_years_vector, names(df))
      for(mc in missing_year_cols){ df[[mc]] <- NA_real_ }
      id_cols <- setdiff(names(df), all_years_vector)
      df <- df %>% select(any_of(id_cols), all_of(all_years_vector))
      df
    })
  }
  
  COUNTY_MASTER_3 <- tibble()
  STATE_MASTER_3 <- tibble()
  if (nrow(INDEX_3) > 0) {
    is_county_3 <- str_detect(INDEX_3$`Table Title`, "U\\.S\\.\\s+county")
    is_state_3  <- str_detect(INDEX_3$`Table Title`, "\\bby\\s+state\\b") 
    COUNTY_SHEETS_3 <- INDEX_3$`Table Number`[which(is_county_3)]
    STATE_SHEETS_3  <- INDEX_3$`Table Number`[which(is_state_3)]
    
    process_sheet_swbinv3 <- function(sheet_nm_param, level_type_param, idx3_param, fips_lkp_param, file_path_3_param){
      title <- idx3_param %>% filter(`Table Number`==sheet_nm_param) %>% pull(`Table Title`)
      if (length(title) == 0) return(tibble())
      bits <- extract_bits_swbinv3(title)
      df   <- tryCatch(read_excel(file_path_3_param, sheet=sheet_nm_param, skip=3), error = function(e) tibble())
      if (nrow(df) == 0) return(tibble())
      
      ycols <- names(df)[str_detect(names(df), "^\\d{4}$") & 
                           sapply(df, function(x) is.numeric(x) || all(grepl("^\\d+(\\.\\d+)?$", na.omit(as.character(x)))))]
      if(length(ycols) == 0) {
        potential_ycols <- names(df)[str_detect(names(df), "^\\d{4}$")]
        for(col_name in potential_ycols) {
          if(is.character(df[[col_name]])) {
            suppressWarnings(df[[col_name]] <- as.numeric(df[[col_name]]))
          }
        }
        ycols <- names(df)[str_detect(names(df), "^\\d{4}$") & sapply(df, is.numeric)]
        if(length(ycols) == 0) return(tibble())
      }
      
      df <- df %>%
        mutate(across(all_of(ycols), as.numeric)) %>%
        mutate(`All-Years Total`=rowSums(across(all_of(ycols)), na.rm=TRUE),
               Metric="USPTO Utility Patents Granted",
               `Innovation Category`=bits$innov,
               `Years Available`=bits$years, .before=1)
      
      if(level_type_param=="county" && "County code" %in% names(df)){
        df <- df %>%
          mutate(`County GEOID`=county_code_to_geoid(`County code`, fips_lkp_param)) %>%
          relocate(`County GEOID`, .after=`All-Years Total`) %>%
          select(-`County code`)
      } else if (level_type_param=="state" && !"State" %in% names(df) && length(names(df)) > 0) {
        if (names(df)[1] != "Metric" && !names(df)[1] %in% ycols) names(df)[1] <- "State"
      }
      df
    }
    
    msg("PROGRESS","Parallelizing swbinv-3 for COUNTY sheets ...")
    county_list_3 <- future_map(COUNTY_SHEETS_3, ~process_sheet_swbinv3(.x, "county", INDEX_3, STATE_FIPS_LOOKUP, SWBINV_3_FILE),
                                .options = furrr_options(seed=TRUE, packages=c("readxl", "dplyr", "stringr", "purrr", "tidyr")))
    dbg(county_list_3, "county_list_3_raw")
    
    msg("PROGRESS","Parallelizing swbinv-3 for STATE sheets ...")
    state_list_3 <- future_map(STATE_SHEETS_3, ~process_sheet_swbinv3(.x, "state", INDEX_3, STATE_FIPS_LOOKUP, SWBINV_3_FILE),
                               .options = furrr_options(seed=TRUE, packages=c("readxl", "dplyr", "stringr", "purrr", "tidyr")))
    dbg(state_list_3, "state_list_3_raw")
    
    county_list_3 <- Filter(function(x) !is.null(x) && nrow(x) > 0, county_list_3)
    state_list_3 <- Filter(function(x) !is.null(x) && nrow(x) > 0, state_list_3)
    
    all_years_3_vec <- unique(c(unlist(purrr::map(county_list_3, names)),
                                unlist(purrr::map(state_list_3, names)))) %>%
      purrr::keep(~ str_detect(.x, "^\\d{4}$")) %>% sort()
    
    if (length(all_years_3_vec) > 0) {
      county_list_3 <- pad_years_df_list(county_list_3, all_years_3_vec)
      state_list_3  <- pad_years_df_list(state_list_3,  all_years_3_vec)
    }
    
    COUNTY_MASTER_3 <- if(length(county_list_3) > 0) bind_rows(county_list_3) else tibble()
    STATE_MASTER_3  <- if(length(state_list_3) > 0) bind_rows(state_list_3) else tibble()
    
    meta_county <- c("Metric","Innovation Category","Years Available","All-Years Total","County GEOID","County","State")
    meta_state  <- c("Metric","Innovation Category","Years Available","All-Years Total","State")
    
    if (nrow(COUNTY_MASTER_3) > 0 && length(all_years_3_vec) > 0) {
      COUNTY_MASTER_3 <- COUNTY_MASTER_3 %>%
        select(any_of(meta_county), all_of(all_years_3_vec))
    }
    if (nrow(STATE_MASTER_3) > 0 && length(all_years_3_vec) > 0) {
      STATE_MASTER_3  <- STATE_MASTER_3 %>%
        select(any_of(meta_state), all_of(all_years_3_vec))
    }
  }
  
  LOOKUP_1 <- tibble()
  COUNTY_MASTER_1 <- tibble()
  if (nrow(INDEX_1) > 0 && "Table SWBINV1-1" %in% INDEX_1$`Table Name` && file.exists(SWBINV_1_FILE)) {
    LOOKUP_1 <- tryCatch(read_excel(SWBINV_1_FILE,"Table SWBINV1-1",skip=3), error = function(e) tibble())
    if (nrow(LOOKUP_1) > 0 && all(c("County code","County","State") %in% names(LOOKUP_1))) {
      LOOKUP_1 <- LOOKUP_1 %>% select(`County code`,County,State)
    } else {
      LOOKUP_1 <- tibble()
      warning("NSF swbinv-1 'Table SWBINV1-1' for lookup is invalid or missing columns.")
    }
  }
  dbg(LOOKUP_1, "LOOKUP_1_NSF")
  
  if (nrow(INDEX_1) > 0 && nrow(LOOKUP_1) > 0 && file.exists(SWBINV_1_FILE)) { 
    PROC_SHEETS_1 <- setdiff(INDEX_1$`Table Name`,"Table SWBINV1-1") 
    
    load_enrich_swbinv1 <- function(sh_name_param, idx1_param, lkp1_param, fips_lkp_param, file_path_1_param){
      ttl  <- idx1_param$`Table Title`[match(sh_name_param, idx1_param$`Table Name`)]
      if (length(ttl) == 0) return(tibble())
      bits <- extract_bits_swbinv1(ttl)
      df   <- tryCatch(read_excel(file_path_1_param, sh_name_param, skip=3), error = function(e) tibble())
      if (nrow(df) == 0) return(tibble())
      
      fc <- names(df)[1]
      if(fc!="County code" && !"County code" %in% names(df)){ 
        df <- rename(df, `County code`=all_of(fc)) 
      } else if (!"County code" %in% names(df)) {
        return(tibble()) 
      }
      
      df <- df %>%
        left_join(lkp1_param, by="County code") %>%
        relocate(any_of(c("County","State")), .after=`County code`) %>%
        mutate(`County GEOID`=county_code_to_geoid(`County code`, fips_lkp_param),
               Metric="USPTO Utility Patents Granted",
               `Innovation Category`=bits$innov,
               `Years Available`=bits$years,.before=1)
      
      yrs_found <- names(df) %>% purrr::keep(~ str_detect(.x,"^\\d{4}$"))
      valid_yrs_found <- c()
      if (length(yrs_found) > 0) {
        for(yr_col_name in yrs_found) {
          if(is.character(df[[yr_col_name]])) {
            suppressWarnings(df[[yr_col_name]] <- as.numeric(df[[yr_col_name]]))
          }
          if(is.numeric(df[[yr_col_name]])) {
            valid_yrs_found <- c(valid_yrs_found, yr_col_name)
          }
        }
      }
      if (length(valid_yrs_found) == 0) return(df)
      
      df <- df %>%
        mutate(across(all_of(valid_yrs_found), as.numeric)) %>%
        mutate(`All-Years Total`=rowSums(across(all_of(valid_yrs_found)),na.rm=TRUE),.after=`Years Available`)
      df
    }
    
    msg("PROGRESS","Parallelizing swbinv-1 for COUNTY sheets ...")
    tbls_1 <- future_map(PROC_SHEETS_1, ~load_enrich_swbinv1(.x, INDEX_1, LOOKUP_1, STATE_FIPS_LOOKUP, SWBINV_1_FILE),
                         .options = furrr_options(seed=TRUE, packages=c("readxl", "dplyr", "stringr", "purrr", "tidyr")))
    dbg(tbls_1, "tbls_1_raw")
    
    tbls_1 <- Filter(function(x) !is.null(x) && nrow(x) > 0, tbls_1)
    
    all_years_1_vec <- unique(unlist(purrr::map(tbls_1,names))) %>%
      purrr::keep(~ str_detect(.x,"^\\d{4}$")) %>% sort()
    
    if (length(all_years_1_vec) > 0) {
      tbls_1 <- pad_years_df_list(tbls_1, all_years_1_vec)
    }
    
    COUNTY_MASTER_1 <- if(length(tbls_1) > 0) bind_rows(tbls_1) else tibble()
    
    if (nrow(COUNTY_MASTER_1) > 0 && length(all_years_1_vec) > 0) {
      COUNTY_MASTER_1 <- COUNTY_MASTER_1 %>%
        select(any_of(c("Metric","Innovation Category","Years Available","All-Years Total",
                        "County GEOID","County","State")), all_of(all_years_1_vec))
    }
  }
  
  final_years_vec_county <- union(names(COUNTY_MASTER_3), names(COUNTY_MASTER_1)) %>%
    purrr::keep(~ str_detect(.x,"^\\d{4}$")) %>% sort()
  
  if (length(final_years_vec_county) > 0) {
    COUNTY_MASTER_3 <- pad_years_df_list(list(COUNTY_MASTER_3), final_years_vec_county)[[1]]
    COUNTY_MASTER_1 <- pad_years_df_list(list(COUNTY_MASTER_1), final_years_vec_county)[[1]]
  }
  
  meta_final_county <- c("Metric","Innovation Category","Years Available",
                         "All-Years Total","County GEOID","County","State")
  
  if (nrow(COUNTY_MASTER_3) > 0 && ("State" %in% names(COUNTY_MASTER_3) || "County" %in% names(COUNTY_MASTER_3))) {
    COUNTY_MASTER_3 <- COUNTY_MASTER_3 %>%
      select(any_of(meta_final_county), all_of(final_years_vec_county))
  }
  if (nrow(COUNTY_MASTER_1) > 0 && ("State" %in% names(COUNTY_MASTER_1) || "County" %in% names(COUNTY_MASTER_1))) {
    COUNTY_MASTER_1 <- COUNTY_MASTER_1 %>%
      select(any_of(meta_final_county), all_of(final_years_vec_county))
  }
  
  if (nrow(COUNTY_MASTER_3) > 0 && nrow(COUNTY_MASTER_1) > 0) {
    NSF_PATENT_DATA_COUNTY <- bind_rows(COUNTY_MASTER_3, COUNTY_MASTER_1)
  } else if (nrow(COUNTY_MASTER_3) > 0) {
    NSF_PATENT_DATA_COUNTY <- COUNTY_MASTER_3
  } else if (nrow(COUNTY_MASTER_1) > 0) {
    NSF_PATENT_DATA_COUNTY <- COUNTY_MASTER_1
  }
  
  NSF_PATENT_DATA_STATE  <- STATE_MASTER_3
}

dbg(NSF_PATENT_DATA_COUNTY, "NSF_PATENT_DATA_COUNTY")
dbg(NSF_PATENT_DATA_STATE, "NSF_PATENT_DATA_STATE")

msg("INFO","NSF Patent Data processing completed.")

# ------------------------------------------------------------------------------
# 8) OTHER ADDITIONAL Data LOADS (Geopackages, Coal CSV)
# ------------------------------------------------------------------------------
msg("INFO","Loading geopackages in parallel...")

geopkg_folder <- file.path(RAW_DATA_FOLDER,"Geopackages")
geopkgs_to_load <- c(
  "2024_COAL_CLOSURE_ENERGY_COMMUNITIES.gpkg",
  "2024_FFE_ENERGY_COMMUNITIES_MSA_NONMSA.gpkg",
  "2024_FFE_MSA_NONMSA_NOT_ENERGY_COMMUNITIES.gpkg",
  "Bureau_of_Land_Management.gpkg",
  "Bureau_of_Reclamation.gpkg",
  "Department_of_Defense.gpkg",
  "DOT_AlternativeFuelCorridors.gpkg",
  "DOT_ArmyCorps_PortsStatisticalArea.gpkg",
  "EIA_NERC_Regions_EIA.gpkg",
  "FHWA_NationalHighwaySystem.gpkg",
  "Fish_and_Wildlife_Service.gpkg",
  "Forest_Service.gpkg",
  "National_Park_Service.gpkg",
  "OpportunityZones.gpkg",
  "PrincipalPorts.gpkg",
  "NTAD_North_American_Rail_Network_Lines.gpkg",
  "Protected Areas Database of the United States (PAD-US) 3.0.gpkg"
)

load_geopackage_in_parallel <- function(gpkg_filename, base_folder_param) {
  gpkg_path <- file.path(base_folder_param, gpkg_filename)
  gpkg_r_name <- make.names(tools::file_path_sans_ext(gpkg_filename))
  
  if(!file.exists(gpkg_path)){
    return(list(name=gpkg_r_name, data=NULL, error_msg=paste("File not found:", gpkg_path)))
  }
  
  gpkg_data <- NULL
  error_message <- NA_character_
  tryCatch({
    gpkg_data <- st_read(gpkg_path, quiet=TRUE)
  }, error=function(e){
    error_message <<- e$message
    gpkg_data <<- NULL
  })
  list(name=gpkg_r_name, data=gpkg_data, error_msg=error_message)
}

if (dir.exists(geopkg_folder)) {
  geopkg_results <- future_map(
    geopkgs_to_load, 
    load_geopackage_in_parallel,
    base_folder_param = geopkg_folder,
    .options = furrr_options(seed=TRUE, packages="sf")
  )
  
  for(res in geopkg_results){
    if(!is.null(res$data)){
      assign(res$name, res$data, envir=.GlobalEnv)
      dbg(get(res$name, envir=.GlobalEnv), res$name)
    } else {
      msg("WARN", paste0("Failed to load geopackage '", res$name, "'. ",
                         if(!is.na(res$error_msg)) paste("Error:",res$error_msg) else "Data is NULL."))
    }
  }
} else {
  msg("WARN", paste("Geopackage folder not found:", geopkg_folder, "Skipping geopackage loading."))
}

msg("INFO","Loading Coal Power Plant Reinvestment CSV => fread => as_tibble...")
coal_file <- file.path(RAW_DATA_FOLDER,"IWG","NETL Coal Power Plant Reinvestment Visualization Tool",
                       "Coal_Retiring_Beta_plants_retirement_2015_2030.csv")
COAL_POWER_PLANT_REINVESTMENT_CSV <- tibble()
if(file.exists(coal_file)){
  COAL_POWER_PLANT_REINVESTMENT_CSV <- tryCatch({
    data.table::fread(coal_file, showProgress=FALSE) %>%
      as_tibble()
  }, error=function(e){
    warning("Error reading coal csv: ", e$message)
    tibble()
  })
} else {
  warning("Coal CSV not found: ", coal_file)
}
dbg(COAL_POWER_PLANT_REINVESTMENT_CSV)

msg("INFO","Loading processed Geopackage CSVs => fread => as_tibble ...")
PROCESSED_GPKG_FOLDER <- file.path(RAW_DATA_FOLDER, "Geopackages","Processed")

PETROLEUM_TERMINALS_PATH <- file.path(PROCESSED_GPKG_FOLDER, "Petroleum_Terminals_Processed.csv")
PETROLEUM_TERMINALS <- tibble()
if(file.exists(PETROLEUM_TERMINALS_PATH)){
  PETROLEUM_TERMINALS <- tryCatch({
    data.table::fread(PETROLEUM_TERMINALS_PATH, showProgress=FALSE) %>% as_tibble()
  }, error=function(e){ warning("Error reading Petroleum_Terminals: ", e$message); tibble()})
} else { warning("File not found: ", PETROLEUM_TERMINALS_PATH) }
dbg(PETROLEUM_TERMINALS)

BROWNFIELD_SITES_PATH <- file.path(PROCESSED_GPKG_FOLDER, "Brownfield_Sites.csv")
BROWNFIELD_SITES <- tibble()
if(file.exists(BROWNFIELD_SITES_PATH)){
  BROWNFIELD_SITES <- tryCatch({
    data.table::fread(BROWNFIELD_SITES_PATH, showProgress=FALSE) %>% as_tibble()
  }, error=function(e){ warning("Error reading Brownfield_Sites: ", e$message); tibble()})
} else { warning("File not found: ", BROWNFIELD_SITES_PATH) }
dbg(BROWNFIELD_SITES)

CRITICAL_MINERALS_PATH <- file.path(PROCESSED_GPKG_FOLDER, "CRITICAL_MINERAL_MINES_DEPOSITS_GLOBAL_NETL.csv")
CRITICAL_MINERAL_MINES_DEPOSITS_GLOBAL_NETL <- tibble()
if(file.exists(CRITICAL_MINERALS_PATH)){
  CRITICAL_MINERAL_MINES_DEPOSITS_GLOBAL_NETL <- tryCatch({
    data.table::fread(CRITICAL_MINERALS_PATH, showProgress=FALSE) %>%
      as_tibble() %>%
      filter(tolower(location) %in% c("united states of america", "usa", "united states"))
  }, error=function(e){ warning("Error reading CRITICAL_MINERAL csv: ", e$message); tibble()})
} else { warning("File not found: ", CRITICAL_MINERALS_PATH) }
dbg(CRITICAL_MINERAL_MINES_DEPOSITS_GLOBAL_NETL)

msg("INFO","Loading FEMA National Risk Index data => CSV => fread => as_tibble ...")
fema_base_path <- file.path(RAW_DATA_FOLDER, "FEMA_NationalRiskIndex_April2025Download")
NRI_COUNTIES_PATH <- file.path(fema_base_path, "NRI_Table_Counties.csv")
NRI_DICT_PATH <- file.path(fema_base_path, "NRIDataDictionary.csv")

NATIONAL_RISK_INDEX_2023 <- tibble()
NATIONAL_RISK_INDEX_DATA_DICTIONARY <- tibble()
NATIONAL_RISK_INDEX_2023_RAW <- tibble()

if(file.exists(NRI_COUNTIES_PATH) && file.exists(NRI_DICT_PATH)){
  NATIONAL_RISK_INDEX_2023_RAW <- tryCatch({
    data.table::fread(NRI_COUNTIES_PATH, showProgress=FALSE) %>% as_tibble()
  }, error=function(e){ warning("Could not read NRI_COUNTIES CSV: ", e$message); tibble()})
  
  NATIONAL_RISK_INDEX_DATA_DICTIONARY <- tryCatch({
    data.table::fread(NRI_DICT_PATH, showProgress=FALSE, quote="") %>% as_tibble()
  }, error=function(e){ warning("Could not read NRIDataDictionary: ", e$message); tibble()})
  
  dbg(NATIONAL_RISK_INDEX_2023_RAW, "NATIONAL_RISK_INDEX_2023_raw_names")
  dbg(NATIONAL_RISK_INDEX_DATA_DICTIONARY, "NATIONAL_RISK_INDEX_DATA_DICTIONARY")
  
  if(nrow(NATIONAL_RISK_INDEX_2023_RAW)>0 && nrow(NATIONAL_RISK_INDEX_DATA_DICTIONARY)>0 &&
     all(c("Field Name", "Field Alias") %in% names(NATIONAL_RISK_INDEX_DATA_DICTIONARY))){
    msg("INFO","Renaming columns in NATIONAL_RISK_INDEX_2023 using dictionary...")
    
    current_names <- names(NATIONAL_RISK_INDEX_2023_RAW)
    name_map <- NATIONAL_RISK_INDEX_DATA_DICTIONARY %>% 
      filter(!is.na(`Field Alias`) & nzchar(trimws(`Field Alias`))) %>%
      distinct(`Field Name`, .keep_all = TRUE) %>%
      pull(`Field Alias`, name = `Field Name`)
    
    new_names <- current_names
    for(i in seq_along(current_names)){
      original_name <- current_names[i]
      if (original_name %in% names(name_map)) {
        new_names[i] <- name_map[[original_name]]
      }
    }
    
    NATIONAL_RISK_INDEX_2023 <- NATIONAL_RISK_INDEX_2023_RAW
    names(NATIONAL_RISK_INDEX_2023) <- make.unique(new_names, sep="_")
    dbg(NATIONAL_RISK_INDEX_2023, "NATIONAL_RISK_INDEX_2023_renamed")
  } else {
    msg("WARN", "FEMA NRI data or dictionary is empty/invalid, or dictionary missing key columns. Skipping renaming.")
    NATIONAL_RISK_INDEX_2023 <- NATIONAL_RISK_INDEX_2023_RAW
  }
} else {
  warning(paste0("FEMA NRI files not found. Checked: \n", 
                 "  Counties: ", NRI_COUNTIES_PATH, "\n",
                 "  Dictionary: ", NRI_DICT_PATH))
}
msg("INFO","Other Additional Data Loads processing completed.")

# ------------------------------------------------------------------------------
# 9) ADDITIONAL EIA SPATIAL LAYERS (ArcGIS)
# ------------------------------------------------------------------------------
msg("INFO","Parallel fetching ArcGIS layers for EIA references...")

arcgis_layers <- list(
  list(
    base_url_val="https://services7.arcgis.com/FGr1D95XCGALKXqM/ArcGIS/rest/services/Renewable_Diesel_and_Other_Biofuels/FeatureServer/",
    layer_id_val=245,
    label_val="renewable_biofuel_plants_sf"
  ),
  list(
    base_url_val="https://services7.arcgis.com/FGr1D95XCGALKXqM/ArcGIS/rest/services/Balancing_Authorities/FeatureServer/",
    layer_id_val=255,
    label_val="balancing_authorities_sf"
  ),
  list(
    base_url_val="https://services7.arcgis.com/FGr1D95XCGALKXqM/ArcGIS/rest/services/NERC_Regions_EIA/FeatureServer/",
    layer_id_val=0,
    label_val="nerc_regions_sf"
  )
)

fetch_layer_as_sf <- function(base_url_param, layer_id_param, crs_out_param=4326, label_param="unknown_layer") {
  layer_url    <- paste0(base_url_param, layer_id_param)
  metadata_url <- paste0(layer_url, "?f=pjson")
  
  meta_json_content <- NULL
  sf_obj <- NULL
  
  tryCatch({
    meta_response <- httr::GET(metadata_url, httr::timeout(30))
    httr::stop_for_status(meta_response, task = paste("fetch metadata for", label_param))
    meta_json_content <- httr::content(meta_response, as="text", encoding="UTF-8") %>%
      jsonlite::fromJSON()
  }, error=function(e){
    return(NULL)
  })
  
  if(is.null(meta_json_content) || !is.list(meta_json_content) || is.null(meta_json_content$fields)){
    return(NULL)
  }
  
  alias_map <- stats::setNames(meta_json_content$fields$alias, meta_json_content$fields$name)
  
  tryCatch({
    sf_obj <- esri2sf::esri2sf(layer_url, where="1=1", outFields="*", crs=crs_out_param, progress = FALSE)
  }, error=function(e){
    return(NULL)
  })
  
  if(is.null(sf_obj) || nrow(sf_obj) == 0) {
    return(NULL)
  }
  
  geom_col_name <- attr(sf_obj,"sf_column")
  attr_cols_present <- setdiff(names(sf_obj),geom_col_name)
  valid_aliases_map_subset <- alias_map[names(alias_map) %in% attr_cols_present]
  
  names_to_change <- names(valid_aliases_map_subset)
  new_column_names_for_change <- valid_aliases_map_subset
  final_new_column_names <- names(sf_obj)
  name_indices_to_update <- match(names_to_change, final_new_column_names)
  
  if (length(names_to_change) > 0 && length(name_indices_to_update) == length(names_to_change)) {
    final_new_column_names[name_indices_to_update] <- make.unique(as.character(new_column_names_for_change), sep="_")
    names(sf_obj) <- final_new_column_names
  }
  
  sf_obj
}

arcgis_results <- future_pmap(
  list(
    base_url_param = sapply(arcgis_layers, `[[`, "base_url_val"),
    layer_id_param = sapply(arcgis_layers, `[[`, "layer_id_val"),
    label_param    = sapply(arcgis_layers, `[[`, "label_val")
  ),
  fetch_layer_as_sf,
  crs_out_param = CRS_USE,
  .options = furrr_options(
    seed=TRUE,
    packages=c("httr","jsonlite","esri2sf","sf", "glue", "stats", "dplyr", "stringr")
  )
)

default_empty_sf <- st_sf(geometry = st_sfc(crs = CRS_USE))

renewable_biofuel_plants_sf <- if(length(arcgis_results) >= 1 && !is.null(arcgis_results[[1]]) && inherits(arcgis_results[[1]], "sf")) arcgis_results[[1]] else default_empty_sf
balancing_authorities_sf    <- if(length(arcgis_results) >= 2 && !is.null(arcgis_results[[2]]) && inherits(arcgis_results[[2]], "sf")) arcgis_results[[2]] else default_empty_sf
nerc_regions_sf             <- if(length(arcgis_results) >= 3 && !is.null(arcgis_results[[3]]) && inherits(arcgis_results[[3]], "sf")) arcgis_results[[3]] else default_empty_sf

cat("\n── Renewable Biofuel Plants (first 6 rows if data exists) ─\n")
if (nrow(renewable_biofuel_plants_sf) > 0) print(head(renewable_biofuel_plants_sf)) else cat("No data or empty sf object.\n")
if (inherits(renewable_biofuel_plants_sf, "sf")) glimpse(st_drop_geometry(renewable_biofuel_plants_sf))

cat("\n── Balancing Authorities (first 6 rows if data exists) ─\n")
if (nrow(balancing_authorities_sf) > 0) print(head(balancing_authorities_sf)) else cat("No data or empty sf object.\n")
if (inherits(balancing_authorities_sf, "sf")) glimpse(st_drop_geometry(balancing_authorities_sf))

cat("\n── NERC Reliability Regions (first 6 rows if data exists) ─\n")
if (nrow(nerc_regions_sf) > 0) print(head(nerc_regions_sf)) else cat("No data or empty sf object.\n")
if (inherits(nerc_regions_sf, "sf")) glimpse(st_drop_geometry(nerc_regions_sf))

# ------------------------------------------------------------------------------
# 10) EXTRA DATA: CBS NEWS 2024 Election Data
# ------------------------------------------------------------------------------
msg("INFO","Loading CBS News 2024 Election Data from GitHub raw ...")
CBS_2024_ELECTION_DATA <- tryCatch({
  data.table::fread("https://raw.githubusercontent.com/cbs-news-data/election-2024-maps/refs/heads/master/output/all_counties_clean_2024.csv",
                    showProgress=FALSE) %>%
    as_tibble()
}, error=function(e){
  warning("Could not read CBS_2024_ELECTION_DATA: ", e$message)
  tibble()
})
dbg(CBS_2024_ELECTION_DATA)

# ------------------------------------------------------------------------------
# 11) CLOSE SINKS AND FINISH
# ------------------------------------------------------------------------------
msg("INFO","Script execution nearing completion. Stopping parallel workers and closing log sinks.")

plan(sequential) # Revert to sequential plan to close out workers

sink(type="message")
sink(type="output")
if(isOpen(log_con)) close(log_con)

cat(sprintf("%s [INFO] Log saved to: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), log_file))
cat(sprintf("%s [INFO] Full script execution completed.\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))

if(exists("eig_local", inherits = FALSE) && file.exists(eig_local)) {
  unlink(eig_local)
  cat(sprintf("%s [DEBUG] Removed temporary EIG Excel file: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), eig_local))
}
