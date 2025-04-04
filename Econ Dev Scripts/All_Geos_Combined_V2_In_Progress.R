#Section 1: SETUP, LIBRARIES, INSTALLATION CHECKS, AND HELPER FUNCTIONS------------------------

###############################################################################
# 0) SETUP, LIBRARIES, INSTALLATION CHECKS, AND HELPER FUNCTIONS
###############################################################################

# Added glmnet for LASSO
required_pkgs <- c(
  "callr", "tigris", "sf", "rvest", "dplyr", "stringr", "purrr", "tibble",
  "tidycensus", "readxl", "httr", "tidyr", "readr", "jsonlite", "units",
  "parallel", "writexl", "lubridate", "xml2", "knitr", "ggplot2",
  "skimr", "naniar", "FactoMineR", "caret", "DT", "data.table", "futile.logger",
  "pROC", "glmnet"
)

# Check for missing packages
to_install <- required_pkgs[!(required_pkgs %in% installed.packages()[, "Package"])]
if(length(to_install) > 0) {
  cat("Installing missing packages:\n")
  print(to_install)
  install.packages(to_install, repos = "https://cloud.r-project.org")
}

# Load all packages
invisible(lapply(required_pkgs, library, character.only = TRUE))

# ----------------------------------------------------------------
# ADDITIONAL DEBUGGING STATEMENT:
# We can't call debug_log yet until we define it (see below).
# So for any immediate logging, we can do this:
cat("All required packages are installed and loaded.\n")
# ----------------------------------------------------------------

# Add futile.logger to required packages (redundant but safe)
# Added glmnet again for safety
required_pkgs <- c(
  "callr", "tigris", "sf", "rvest", "dplyr", "stringr", "purrr", "tibble",
  "tidycensus", "readxl", "httr", "tidyr", "readr", "jsonlite", "units",
  "parallel", "writexl", "lubridate", "xml2", "knitr", "ggplot2",
  "skimr", "naniar", "FactoMineR", "caret", "DT", "data.table",
  "futile.logger", "pROC", "glmnet"
)

# Check again for any newly added missing packages
to_install <- required_pkgs[!(required_pkgs %in% installed.packages()[, "Package"])]
if(length(to_install) > 0) {
  cat("Installing missing packages:\n")
  print(to_install)
  install.packages(to_install, repos = "https://cloud.r-project.org")
}

# Load packages (again, safe if already loaded)
invisible(lapply(required_pkgs, library, character.only = TRUE))

# ----------------------------------------------------------------
# Now we can define debug_log function
# ----------------------------------------------------------------

# Setup logging with futile.logger
timestamp <- format(lubridate::with_tz(Sys.time(), tzone = "America/New_York"), "%Y%m%d_%H%M%S")
log_file_name <- paste0("county_data_development_script_log_", timestamp, ".txt")
flog.appender(appender.tee(log_file_name))
flog.threshold(DEBUG)  # Set to capture all log levels
flog.info("=== Script started at %s ===", timestamp)

# Replace debug_log function with futile.logger wrapper
debug_log <- function(message, level = "INFO") {
  switch(level,
         "DEBUG" = flog.debug(message),
         "INFO"  = flog.info(message),
         "WARN"  = flog.warn(message),
         "ERROR" = flog.error(message),
         flog.info(message))  # Default case
}

debug_log(sprintf("Logging initialized. Log file: %s", log_file_name), "DEBUG")

options(max.print = 10000)
options(tigris_use_cache = TRUE)  # Enable tigris caching

# Function to both log and print glimpse output
log_glimpse <- function(data, label = "Data glimpse", level = "INFO") {
  # Capture the glimpse output
  output <- capture.output(glimpse(data))
  
  # Print to console
  cat(paste0("\n", label, ":\n"))
  cat(paste(output, collapse = "\n"))
  cat("\n")
  
  # Also log it
  switch(level,
         "DEBUG" = flog.debug("%s:\n%s", label, paste(output, collapse = "\n")),
         "INFO"  = flog.info("%s:\n%s", label, paste(output, collapse = "\n")),
         "WARN"  = flog.warn("%s:\n%s", label, paste(output, collapse = "\n")),
         "ERROR" = flog.error("%s:\n%s", label, paste(output, collapse = "\n")),
         flog.info("%s:\n%s", label, paste(output, collapse = "\n")))  # Default case
}

read_timeout_secs <- 600  # Timeout for reading in subprocess

# Safe download in subprocess
safe_download_subproc <- function(url, destfile, timeout = read_timeout_secs, ...) {
  debug_log(sprintf("Attempting download (timeout=%ds): %s => %s", timeout, url, destfile), "DEBUG")
  result <- callr::r(
    function(u, f, ...) {
      library(httr)
      resp <- GET(u, write_disk(f, overwrite = TRUE), ...)
      if(http_error(resp)) {
        stop(sprintf("HTTP error %s for URL '%s'.", status_code(resp), u))
      }
      return("Download successful")
    },
    args = list(u = url, f = destfile, ...),
    timeout = timeout
  )
  debug_log(result, "INFO")
}

# Safe read Excel via subprocess
safe_read_excel_subproc <- function(path, sheet = NULL, timeout = read_timeout_secs, ...) {
  debug_log(sprintf("Reading Excel file: %s (sheet='%s')", path, sheet), "DEBUG")
  if(!file.exists(path)) {
    stop(sprintf("File not found: %s", path))
  }
  result <- callr::r(
    function(p, s, ...) {
      library(readxl)
      read_excel(p, sheet = s, ...)
    },
    args = list(p = path, s = sheet, ...),
    timeout = timeout
  )
  debug_log(sprintf("Finished reading Excel file: %s", path), "DEBUG")
  return(result)
}

# Safe read CSV via subprocess
safe_read_csv_subproc <- function(path, timeout = read_timeout_secs, show_col_types = FALSE, ...) {
  debug_log(sprintf("Reading CSV file: %s", path), "DEBUG")
  if(!file.exists(path)) {
    stop(sprintf("File not found: %s", path))
  }
  # FIXED HERE: the inner function argument must also be `show_col_types`
  result <- callr::r(
    function(p, show_col_types, ...) {
      library(readr)
      # Pass 'show_col_types' correctly to read_csv:
      read_csv(p, show_col_types = show_col_types, ...)
    },
    # Also pass show_col_types by that exact name
    args = list(p = path, show_col_types = show_col_types, ...),
    timeout = timeout
  )
  debug_log(sprintf("Finished reading CSV file: %s", path), "DEBUG")
  return(result)
}

# Safe read a spatial file via subprocess (example if needed)
safe_st_read_subproc <- function(dsn, layer = NULL, timeout = read_timeout_secs, ...) {
  debug_log(sprintf("Reading spatial file: %s", dsn), "DEBUG")
  if(!grepl("^https?://", dsn) && !file.exists(dsn)) {
    stop(sprintf("File not found: %s", dsn))
  }
  result <- callr::r(
    function(ds, lyr, ...) {
      library(sf)
      if(!is.null(lyr)) {
        st_read(ds, layer = lyr, ...)
      } else {
        st_read(ds, ...)
      }
    },
    args = list(ds = dsn, lyr = layer, ...),
    timeout = timeout
  )
  debug_log(sprintf("Finished reading spatial file: %s", dsn), "DEBUG")
  return(result)
}

# A safe left join with debug logs
safe_left_join <- function(x, y, by = NULL, y_name = "unknown_df", join_details = "",
                           allow_empty_y = TRUE) {
  n_before <- nrow(x)
  debug_log(sprintf("Performing safe_left_join with '%s'. Rows in 'x'=%d, 'y'=%d.",
                    y_name, n_before, nrow(y)), "DEBUG")
  if(!allow_empty_y && nrow(y) == 0) {
    stop(sprintf("Data frame '%s' is empty but allow_empty_y=FALSE", y_name))
  }
  out <- dplyr::left_join(x, y, by = by)
  n_after <- nrow(out)
  if(n_after != n_before) {
    debug_log(
      sprintf("Row count changed from %d to %d after merging with '%s' (%s).",
              n_before, n_after, y_name, join_details),
      if(n_after > n_before) "WARN" else "INFO"
    )
  } else {
    debug_log(sprintf("Row count remained %d after merging with '%s'.", n_after, y_name), "DEBUG")
  }
  return(out)
}

debug_log("Completed Section 1: Setup and Helper Functions", "INFO")

#Section 2: OS DETECTION AND BASE-PATH SETUP------------------------

###############################################################################
# OS DETECTION AND BASE-PATH SETUP
###############################################################################

os_type <- Sys.info()[["sysname"]]

# Attempt to automatically detect the username on Windows, if that's the OS
if(grepl("windows", tolower(os_type))) {
  user_name <- Sys.getenv("USERNAME")  # Typically works on Windows
  if (nzchar(user_name)) {
    base_path <- file.path(
      "C:",
      "Users",
      user_name,
      "OneDrive - RMI",
      "Documents - US Program",
      "6_Projects",
      "Clean Regional Economic Development",
      "ACRE",
      "Data"
    )
  } else {
    # fallback if we couldn't detect the username
    debug_log("Could not detect Windows username. Using default fallback path.", "WARN")
    base_path <- file.path(
      "C:",
      "Users",
      "YourUser",  # fallback
      "OneDrive - RMI",
      "Documents - US Program",
      "6_Projects",
      "Clean Regional Economic Development",
      "ACRE",
      "Data"
    )
  }
} else if(grepl("darwin", tolower(os_type))) {
  # Mac (Darwin)
  base_path <- "~/Library/CloudStorage/OneDrive-RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data"
} else {
  # Other (Linux, etc.)
  # Adjust as needed if you have the data stored elsewhere
  base_path <- "~/Library/CloudStorage/OneDrive-RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data"
}

debug_log(paste("OS detected as:", os_type), "INFO")
debug_log(paste("Base path set to:", base_path), "DEBUG")
debug_log("Completed Section 2: OS detection and base-path setup complete.", "INFO")


#Section 3: DEFINE FILE PATHS------------------------

###############################################################################
# DEFINE FILE PATHS
###############################################################################

NERDE_data_file <- file.path(base_path, "Raw Data", "NERDE Snapshot_August_2023.xlsx")
EDCI_scores_file <- file.path(base_path, "Raw Data", "Economic_Development_Capacity_Index_Snapshot_August_2024.xlsx")
cnbc_data_file <- file.path(base_path, "Raw Data", "CNBC_State_Business_Friendliness_Rankings_2024.csv")
cgt_county_file <- file.path(base_path, "CGT_county_data", "modified_CGT_files", "cgt_county_data_sep_29_2024.csv")
us_counties_major_roads_file <- file.path(base_path, "Raw Data", "us_counties_major_roads.csv")
county_property_values_file <- file.path(base_path, "Raw Data", "county_property_values.csv")
wellesley_facilities_data_file <- file.path(base_path, "Raw Data", "wellesley_big_green_machine", "The-Big-Green-Machine-Dataset-March-2025.xlsx")
statsamerica_zip_url <- "https://www.statsamerica.org/downloads/Innovation-Intelligence.zip"
eia_sales_url <- "https://www.eia.gov/electricity/data/eia861m/xls/sales_revenue.xlsx"
state_corporate_taxes_file <- file.path(base_path, "Raw Data", "2025 State Corporate Income Tax Rates   Brackets.csv") # Note potential double space
state_capacity_data_file <- file.path(base_path, "Raw Data", "sc_data.csv")
MESC_portfolio_file <- file.path(base_path, "Raw Data", "MESC_Awards_Portfolio_10_January_2025.csv")
investment_data_shared_file <- file.path(base_path, "Raw Data", "Investment Data - SHARED.xlsx")
nist_chips_award_file <- file.path(base_path, "Raw Data", "nist_chips_award_data_as_of_january_23_2025.csv")
cim_data_file <- file.path(base_path, "Raw Data", "clean_investment_monitor_q4_2024", "manufacturing_energy_and_industry_facility_metadata.csv")
xchange_dashboard_file <- file.path(base_path, "Raw Data", "State Climate Policy Dashboard - Full Download (1.28.25) 2.xlsx")

# Energy Communities base path (to be loaded upfront)
# Updated path to avoid potential issues with ~ on all OS, using explicit home expansion if needed
home_dir <- Sys.getenv("HOME")
if (home_dir == "") {
  # Fallback for non-standard environments if HOME is not set
  home_dir <- path.expand("~")
}
energy_comms_base_path <- file.path(
  home_dir, # More robust way to get home directory
  "Library",
  "CloudStorage",
  "OneDrive-RMI",
  "Documents - US Program",
  "6_Projects",
  "Clean Regional Economic Development",
  "ACRE",
  "Data",
  "Energy_Communities_Dot_Gov_Map_Data",
  "crawler_run_2025-02-25_194350"
)
# Ensure the expanded path is used if necessary
energy_comms_base_path <- path.expand(energy_comms_base_path)

paths_list <- list(
  NERDE_data_file = NERDE_data_file,
  EDCI_scores_file = EDCI_scores_file,
  cnbc_data_file = cnbc_data_file,
  cgt_county_file = cgt_county_file,
  us_counties_major_roads_file = us_counties_major_roads_file,
  county_property_values_file = county_property_values_file,
  wellesley_facilities_data_file = wellesley_facilities_data_file,
  statsamerica_zip_url = statsamerica_zip_url,
  eia_sales_url = eia_sales_url,
  state_corporate_taxes_file = state_corporate_taxes_file,
  state_capacity_data_file = state_capacity_data_file,
  MESC_portfolio_file = MESC_portfolio_file,
  investment_data_shared_file = investment_data_shared_file,
  nist_chips_award_file = nist_chips_award_file,
  cim_data_file = cim_data_file,
  xchange_dashboard_file = xchange_dashboard_file
)

# We'll also define the list of energy community .gpkg files:
energy_comm_files <- c(
  "Eligiblefor48C_EC_April2024Update.gpkg",
  "Eligiblefor48C_April2024Update.gpkg",
  "Disadvantaged_Communities_DACs_.gpkg",
  "2024_Coal_Closure_Energy_Communities.gpkg",
  "2024_MSAs_Non-MSAs_that_are_Energy_Communities.gpkg",
  "2024_MSAs_Non-MSAs_that_only_meet_the_Fossil_Fuel_Employment_Threshold.gpkg",
  "Top75_MicroStatAreas_byFossilEnergy_EmpRank.gpkg",
  "Top70_MicroStatAreas_byCoalEmpRank.gpkg",
  "Top_70_U.S._Coal_Communities.gpkg"
)

debug_log("File paths defined successfully.", "DEBUG")

# Verify that all local files exist, and URLs are reachable
verify_paths <- function(paths_list) {
  for(nm in names(paths_list)) {
    path_val <- paths_list[[nm]]
    if(grepl("^https?://", path_val, ignore.case = TRUE)) {
      debug_log(sprintf("Checking URL: %s => %s", nm, path_val), "DEBUG")
      res <- tryCatch({
        httr::HEAD(path_val, config = httr::timeout(10)) # Increased timeout slightly
      }, error = function(e) e)
      if(inherits(res, "error") || httr::http_error(res)) {
        warning(sprintf("URL check failed for %s: %s. Error: %s", nm, path_val, ifelse(inherits(res, "error"), res$message, httr::http_status(res)$reason)))
        flog.warn("URL check failed for %s: %s. Error: %s", nm, path_val, ifelse(inherits(res, "error"), res$message, httr::http_status(res)$reason))
        # Changed from stop() to warning() to allow script to potentially continue if a URL is temporarily down
      } else {
        debug_log(sprintf("OK URL: %s (Status: %d)", nm, httr::status_code(res)), "INFO")
      }
    } else {
      # Ensure path expansion for local files as well, especially if using ~
      path_val <- path.expand(path_val)
      debug_log(sprintf("Checking local file: %s => %s", nm, path_val), "DEBUG")
      if(!file.exists(path_val)) {
        # Changed from stop() to warning()
        warning(sprintf("File not found: %s (%s)", nm, path_val))
        flog.warn("File not found: %s (%s)", nm, path_val)
      } else {
        debug_log(sprintf("OK file: %s", nm), "INFO")
      }
    }
  }
  # Verify Energy Community Base Path
  debug_log(sprintf("Checking Energy Community Base Path: %s", energy_comms_base_path), "DEBUG")
  if (!dir.exists(energy_comms_base_path)) {
    warning(sprintf("Energy Community base path not found: %s", energy_comms_base_path))
    flog.warn("Energy Community base path not found: %s", energy_comms_base_path)
  } else {
    debug_log("OK Energy Community Base Path.", "INFO")
  }
  
  message <- "Path verification check complete. Check warnings for issues."
  cat(message, "\n")
  flog.info(message)
}

# Verify all paths
verify_paths(paths_list)
debug_log("verify_paths completed.", "INFO") # Removed 'successfully' as it now issues warnings instead of stopping

debug_log("Completed Section 3: Define File Paths", "INFO")

#Section 4: BUILD UNIQUE COUNTIES DATAFRAME (unique_county_geoids)------------------------

###############################################################################
# 1) BUILD UNIQUE COUNTIES DATAFRAME (unique_county_geoids)
###############################################################################

section_header <- "\n\n=== SECTION 4: Build unique_county_geoids ===" # Updated section number
cat(section_header)
flog.info(section_header)

# Safe wrap tigris calls
check_is_sf <- function(obj, which_data, year) {
  if(!inherits(obj, "sf")) {
    stop(sprintf("%s() for year %s did not return an sf object.", which_data, year))
  }
  obj
}

safe_states <- function(year) {
  debug_log(sprintf("Fetching states for year %s", year), "DEBUG")
  obj <- states(year = year)
  check_is_sf(obj, "states", year)
}
safe_counties <- function(year) {
  debug_log(sprintf("Fetching counties for year %s", year), "DEBUG")
  obj <- counties(year = year)
  check_is_sf(obj, "counties", year)
}
safe_cbsa <- function(year) {
  debug_log(sprintf("Fetching CBSAs for year %s", year), "DEBUG")
  obj <- core_based_statistical_areas(year = year) # Use correct function name
  check_is_sf(obj, "cbsa", year)
}
safe_csa <- function(year) {
  debug_log(sprintf("Fetching CSAs for year %s", year), "DEBUG")
  obj <- combined_statistical_areas(year = year) # Use correct function name
  check_is_sf(obj, "csa", year)
}


get_tigris_geos <- function(year) {
  list(
    states = safe_states(year),
    counties = safe_counties(year),
    cbsa = safe_cbsa(year),
    csa = safe_csa(year)
  )
}

get_tigris_geos_2022 <- function() {
  # For 2022, tigris might not have CBSA/CSA readily available in the standard package
  # Fetching separately if needed, but function assumes counties and states are primary
  debug_log("Fetching TIGRIS data for 2022 (states, counties)", "DEBUG")
  list(
    states = safe_states(2022),
    counties = safe_counties(2022)
    # Note: CBSA/CSA might need different handling or source for 2022 if tigris::core_based_statistical_areas fails
  )
}

debug_log("Loading TIGRIS data for 2018, 2020, 2022, 2023", "INFO")
df_2018 <- get_tigris_geos(2018)
df_2020 <- get_tigris_geos(2020)
df_2022 <- get_tigris_geos_2022() # Uses the specific 2022 function
df_2023 <- get_tigris_geos(2023)
debug_log("Finished loading TIGRIS data for all specified years.", "INFO")

log_glimpse(df_2018$states, "Glimpse of df_2018 (states)")
log_glimpse(df_2018$counties, "Glimpse of df_2018 (counties)")

log_glimpse(df_2020$states, "Glimpse of df_2020 (states)")
log_glimpse(df_2020$counties, "Glimpse of df_2020 (counties)")

log_glimpse(df_2022$states, "Glimpse of df_2022 (states)")
log_glimpse(df_2022$counties, "Glimpse of df_2022 (counties)")

log_glimpse(df_2023$states, "Glimpse of df_2023 (states)")
log_glimpse(df_2023$counties, "Glimpse of df_2023 (counties)")
log_glimpse(df_2023$cbsa, "Glimpse of df_2023 (cbsa)")
log_glimpse(df_2023$csa, "Glimpse of df_2023 (csa)")

make_county_wide_df <- function(tigris_list, year_label) {
  debug_log(sprintf("Creating county-wide df for year %s", year_label), "DEBUG")
  
  states_sf <- tigris_list$states
  counties_sf <- tigris_list$counties
  has_cbsa_csa <- all(c("cbsa", "csa") %in% names(tigris_list))
  
  # Prepare states data first
  states_df <- states_sf %>%
    st_drop_geometry() %>%
    select(STATEFP, STUSPS, state_name = NAME)
  
  # Prepare base counties data
  counties_base <- counties_sf %>%
    left_join(states_df, by = "STATEFP") %>%
    transmute(
      `State Name` = state_name,
      `State FIPS` = STATEFP,
      State_Abbreviation = STUSPS,
      `County NAME` = NAME,
      `County NAMELSAD` = NAMELSAD,
      `County GEOID` = GEOID,
      geometry = geometry
    )
  
  if(has_cbsa_csa) {
    cbsa_sf <- tigris_list$cbsa
    csa_sf <- tigris_list$csa
    
    # Prepare CBSA data (ensure relevant columns exist)
    cbsa_df <- cbsa_sf %>%
      st_drop_geometry() %>%
      select(cbsa_geoid = GEOID, cbsa_namelsad = NAMELSAD) # Assuming these names in tigris output
    
    # Prepare CSA data (ensure relevant columns exist)
    csa_df <- csa_sf %>%
      st_drop_geometry() %>%
      select(csa_geoid = GEOID, csa_namelsad = NAMELSAD) # Assuming these names in tigris output
    
    # Check if CBSAFP and CSAFP exist in counties_sf for joining
    county_colnames <- names(counties_sf)
    cbsafp_col <- if("CBSAFP" %in% county_colnames) "CBSAFP" else if ("GEOID" %in% names(cbsa_sf)) "GEOID" else NULL # Heuristic check
    csafp_col <- if("CSAFP" %in% county_colnames) "CSAFP" else if ("GEOID" %in% names(csa_sf)) "GEOID" else NULL # Heuristic check
    
    if (is.null(cbsafp_col) || is.null(csafp_col)) {
      debug_log(sprintf("Warning: CBSAFP or CSAFP column missing in counties_sf for year %s. Cannot join CBSA/CSA.", year_label), "WARN")
      # Join only states if CBSA/CSA keys are missing
      counties_joined <- counties_base %>%
        mutate(
          `CBSA NAMELSAD` = NA_character_,
          `CBSA GEOID` = NA_character_,
          `CSA NAMELSAD` = NA_character_,
          `CSA GEOID` = NA_character_
        )
    } else {
      # Join all if keys exist
      # Need to get CBSAFP and CSAFP from original counties_sf first
      counties_with_keys <- counties_sf %>%
        st_drop_geometry() %>%
        select(GEOID, CBSAFP, CSAFP)
      
      counties_joined <- counties_base %>%
        left_join(counties_with_keys, by = c("County GEOID" = "GEOID")) %>%
        left_join(cbsa_df, by = c("CBSAFP" = "cbsa_geoid")) %>%
        left_join(csa_df, by = c("CSAFP" = "csa_geoid")) %>%
        transmute(
          `State Name`, `State FIPS`, State_Abbreviation, `County NAME`,
          `County NAMELSAD`, `County GEOID`, geometry,
          `CBSA NAMELSAD` = cbsa_namelsad,
          `CBSA GEOID` = CBSAFP, # Use the original key column
          `CSA NAMELSAD` = csa_namelsad,
          `CSA GEOID` = CSAFP # Use the original key column
        )
    }
    
  } else {
    # If no CBSA/CSA data provided (like for 2022 function)
    debug_log(sprintf("No CBSA/CSA data provided for year %s. Setting related columns to NA.", year_label), "INFO")
    counties_joined <- counties_base %>%
      mutate(
        `CBSA NAMELSAD` = NA_character_,
        `CBSA GEOID` = NA_character_,
        `CSA NAMELSAD` = NA_character_,
        `CSA GEOID` = NA_character_
      )
  }
  
  message <- paste("Glimpse of counties for year", year_label, ":")
  cat(message, "\n")
  flog.info(message)
  log_glimpse(counties_joined, sprintf("Counties for year %s", year_label))
  debug_log(sprintf("Finished creating county-wide df for year %s with %d rows.",
                    year_label, nrow(counties_joined)), "DEBUG")
  return(counties_joined)
}

counties_2018_final <- make_county_wide_df(df_2018, "2018")
debug_log(sprintf("Counties data for 2018 has %d rows.", nrow(counties_2018_final)), "DEBUG")
counties_2020_final <- make_county_wide_df(df_2020, "2020")
debug_log(sprintf("Counties data for 2020 has %d rows.", nrow(counties_2020_final)), "DEBUG")
counties_2022_final <- make_county_wide_df(df_2022, "2022")
debug_log(sprintf("Counties data for 2022 has %d rows.", nrow(counties_2022_final)), "DEBUG")
counties_2023_final <- make_county_wide_df(df_2023, "2023")
debug_log(sprintf("Counties data for 2023 has %d rows.", nrow(counties_2023_final)), "DEBUG")

all_counties <- rbind(
  counties_2018_final %>% mutate(Vintage = "2018"),
  counties_2020_final %>% mutate(Vintage = "2020"),
  counties_2022_final %>% mutate(Vintage = "2022"),
  counties_2023_final %>% mutate(Vintage = "2023")
)
debug_log(sprintf("Combined all_counties has %d rows.", nrow(all_counties)), "DEBUG")
log_glimpse(all_counties, "Glimpse of all_counties (combined across vintages)")

# Create an attribute table for all counties
all_counties_attrib <- all_counties %>%
  st_drop_geometry() %>%
  select(`State Name`, `State FIPS`, State_Abbreviation,
         `County GEOID`, `County NAME`, `County NAMELSAD`, Vintage) %>%
  distinct()
log_glimpse(all_counties_attrib, "Glimpse of all_counties_attrib")

# Exclude territories
territory_fips <- c("60", "66", "69", "72", "78") # FIPS codes for territories like AS, GU, MP, PR, VI
unique_county_geoids_pre_filter <- all_counties_attrib %>%
  group_by(`County GEOID`) %>%
  reframe(
    StateNames = list(sort(unique(`State Name`))),
    StateFIPSs = list(sort(unique(`State FIPS`))),
    StateAbbrevs = list(sort(unique(State_Abbreviation))),
    CountyNAMEs = list(sort(unique(`County NAME`))),
    CountyNAMELSADs = list(sort(unique(`County NAMELSAD`))),
    Vintages = list(sort(unique(Vintage)))
  ) %>%
  mutate(
    `State Name` = sapply(StateNames, paste, collapse = "; "),
    `State FIPS` = sapply(StateFIPSs, paste, collapse = "; "),
    State_Abbreviation = sapply(StateAbbrevs, paste, collapse = "; "),
    `County NAME` = sapply(CountyNAMEs, paste, collapse = "; "),
    `County NAMELSAD` = sapply(CountyNAMELSADs, paste, collapse = "; "),
    `Vintage(s)` = sapply(Vintages, function(x) paste(x, collapse = ", "))
  )

# Apply territory filter using State FIPS
unique_county_geoids <- unique_county_geoids_pre_filter %>%
  filter(!sapply(StateFIPSs, function(fips_list) any(fips_list %in% territory_fips)))

debug_log(sprintf("unique_county_geoids constructed with %d rows after territory filtering.", nrow(unique_county_geoids)), "DEBUG")
log_glimpse(unique_county_geoids, "Glimpse of unique_county_geoids (before attaching geometry)")

# Attach geometry from the most recent vintage available for each county
all_counties_num <- all_counties %>% mutate(VintageYear = as.numeric(Vintage))

most_recent_info <- all_counties_num %>%
  group_by(`County GEOID`) %>%
  # Ensure filtering happens before slice_max if necessary, although group_by should handle it
  slice_max(order_by = VintageYear, n = 1, with_ties = FALSE) %>% # Ensure only one row per county
  ungroup() %>%
  select(`County GEOID`, geometry, county_geometry_vintage_year = VintageYear,
         County_NAME_Latest = `County NAME`, State_Name_Latest = `State Name`)

debug_log("Extracted most recent geometry information.", "DEBUG")

unique_county_geoids <- safe_left_join(unique_county_geoids, most_recent_info,
                                       by = "County GEOID", y_name = "most_recent_info")

# Convert to sf object, handling potential errors if geometry is missing for some
# Ensure geometry column exists before converting
if ("geometry" %in% names(unique_county_geoids)) {
  # Check for empty or invalid geometries before converting
  valid_geom_indices <- !sapply(st_geometry(unique_county_geoids), is.null) & !st_is_empty(unique_county_geoids)
  if(sum(!valid_geom_indices) > 0) {
    debug_log(sprintf("Warning: %d rows have missing or empty geometry.", sum(!valid_geom_indices)), "WARN")
    # Option: filter out rows with bad geometry, or proceed and handle errors
    # unique_county_geoids <- unique_county_geoids[valid_geom_indices, ]
  }
  
  unique_county_geoids <- st_as_sf(unique_county_geoids, sf_column_name = "geometry", crs = 4269) # Use NAD83 as common CRS
  debug_log(sprintf("unique_county_geoids converted to sf. Row count: %d",
                    nrow(unique_county_geoids)), "DEBUG")
} else {
  debug_log("Error: 'geometry' column not found after joining most_recent_info. Cannot convert to sf.", "ERROR")
  # Handle error - perhaps stop execution or proceed without spatial capabilities
}

log_glimpse(unique_county_geoids, "Glimpse of unique_county_geoids (after attaching geometry)")

# Create "County, State" using the most recent names
unique_county_geoids <- unique_county_geoids %>%
  mutate(`County, State` = paste0(County_NAME_Latest, ", ", State_Name_Latest))

# Attach most recent CBSA/CSA info
most_recent_cbsa_csa <- all_counties_num %>%
  st_drop_geometry() %>%
  group_by(`County GEOID`) %>%
  slice_max(order_by = VintageYear, n = 1, with_ties = FALSE) %>% # Get the single most recent record
  ungroup() %>%
  select(`County GEOID`, `CBSA NAMELSAD`, `CBSA GEOID`, `CSA NAMELSAD`, `CSA GEOID`) %>%
  distinct() # Ensure uniqueness after selection

unique_county_geoids <- safe_left_join(unique_county_geoids, most_recent_cbsa_csa,
                                       by = "County GEOID", y_name = "most_recent_cbsa_csa") %>%
  mutate(
    # Convert to logical rather than relying on NA checks which might fail if CBSA/CSA GEOID is ""
    `Part of Metro/Micro Area -- Yes/No` = !is.na(`CBSA GEOID`) & `CBSA GEOID` != "",
    `Metro/Micro Area Type` = case_when(
      is.na(`CBSA GEOID`) | `CBSA GEOID` == "" ~ "NA",
      !is.na(`CBSA NAMELSAD`) & str_detect(`CBSA NAMELSAD`, "Metropolitan Statistical Area") ~ "Metropolitan Area",
      !is.na(`CBSA NAMELSAD`) & str_detect(`CBSA NAMELSAD`, "Micropolitan Statistical Area") ~ "Micropolitan Area",
      TRUE ~ "NA" # Fallback
    ),
    `Metro/Micro Area Name` = `CBSA NAMELSAD`,
    `Metro/Micro Area GEOID` = `CBSA GEOID`,
    # Convert to logical
    `Part of Combined Statistical Area (CSA)` = !is.na(`CSA GEOID`) & `CSA GEOID` != "",
    `CSA Name (If Applicable)` = `CSA NAMELSAD`,
    `CSA GEOID (If Applicable)` = `CSA GEOID`
  ) %>%
  # Select final desired columns, removing intermediate ones if necessary
  select(-`CBSA NAMELSAD`, -`CBSA GEOID`, -`CSA NAMELSAD`, -`CSA GEOID`) # Drop intermediate columns


log_glimpse(unique_county_geoids, "Glimpse of unique_county_geoids (final version with geometry and CBSA/CSA)")
debug_log("Completed Section 4: unique_county_geoids built.", "INFO")

#Section 5: LOAD & SCRAPE EXTERNAL DATA (NO MERGING)------------------------

###############################################################################
# 2) LOAD & SCRAPE EXTERNAL DATA (NO MERGING)
###############################################################################

section_header <- "\n=== SECTION 5: Load & Scrape External Data (No Merging) ===" # Updated section number
cat(section_header)
flog.info(section_header)
debug_log("Starting Section 5: External data load and scrape", "DEBUG")

# Load NERDE Data
if(file.exists(NERDE_data_file)) {
  NERDE_data <- safe_read_excel_subproc(NERDE_data_file, sheet = "Data")
  debug_log(sprintf("NERDE_data loaded with %d rows.", nrow(NERDE_data)), "DEBUG")
  log_glimpse(NERDE_data, "Glimpse of NERDE_data")
} else {
  debug_log(sprintf("NERDE_data_file not found: %s. Skipping load.", NERDE_data_file), "WARN")
  NERDE_data <- NULL # Set to NULL if file doesn't exist
}

# Load EDCI Scores
if(file.exists(EDCI_scores_file)) {
  EDCI_scores <- safe_read_excel_subproc(EDCI_scores_file, sheet = "scores")
  debug_log(sprintf("EDCI_scores loaded with %d rows.", nrow(EDCI_scores)), "DEBUG")
  log_glimpse(EDCI_scores, "Glimpse of EDCI_scores")
} else {
  debug_log(sprintf("EDCI_scores_file not found: %s. Skipping load.", EDCI_scores_file), "WARN")
  EDCI_scores <- NULL
}

# Load CNBC State Rankings
if(file.exists(cnbc_data_file)) {
  CNBC_state_rankings_2024 <- safe_read_csv_subproc(cnbc_data_file)
  debug_log(sprintf("CNBC_state_rankings_2024 loaded with %d rows.", nrow(CNBC_state_rankings_2024)), "DEBUG")
  log_glimpse(CNBC_state_rankings_2024, "Glimpse of CNBC_state_rankings_2024")
} else {
  debug_log(sprintf("cnbc_data_file not found: %s. Skipping load.", cnbc_data_file), "WARN")
  CNBC_state_rankings_2024 <- NULL
}

# Load CGT County Data
if(file.exists(cgt_county_file)) {
  cgt_county <- safe_read_csv_subproc(cgt_county_file)
  debug_log(sprintf("cgt_county loaded with %d rows.", nrow(cgt_county)), "DEBUG")
  log_glimpse(cgt_county, "Glimpse of cgt_county")
} else {
  debug_log(sprintf("cgt_county_file not found: %s. Skipping load.", cgt_county_file), "WARN")
  cgt_county <- NULL
}

# Load US Counties Major Roads
if(file.exists(us_counties_major_roads_file)) {
  us_counties_major_roads <- safe_read_csv_subproc(us_counties_major_roads_file)
  debug_log(sprintf("us_counties_major_roads loaded with %d rows.", nrow(us_counties_major_roads)), "DEBUG")
  log_glimpse(us_counties_major_roads, "Glimpse of us_counties_major_roads")
} else {
  debug_log(sprintf("us_counties_major_roads_file not found: %s. Skipping load.", us_counties_major_roads_file), "WARN")
  us_counties_major_roads <- NULL
}

# Load County Property Values
if(file.exists(county_property_values_file)) {
  county_property_values <- safe_read_csv_subproc(county_property_values_file)
  debug_log(sprintf("county_property_values loaded with %d rows.", nrow(county_property_values)), "DEBUG")
  log_glimpse(county_property_values, "Glimpse of county_property_values")
} else {
  debug_log(sprintf("county_property_values_file not found: %s. Skipping load.", county_property_values_file), "WARN")
  county_property_values <- NULL
}

# Load Wellesley Facilities Data
if(file.exists(wellesley_facilities_data_file)) {
  # Be specific about sheet name, check if it exists
  wellesley_sheet_name <- "Dataset-3-19-25" # As per original script
  wellesley_sheets <- tryCatch(excel_sheets(wellesley_facilities_data_file), error = function(e) NULL)
  if (!is.null(wellesley_sheets) && wellesley_sheet_name %in% wellesley_sheets) {
    wellesley_facilities_data <- safe_read_excel_subproc(wellesley_facilities_data_file, sheet = wellesley_sheet_name)
    debug_log(sprintf("wellesley_facilities_data loaded from sheet '%s' with %d rows.", wellesley_sheet_name, nrow(wellesley_facilities_data)), "DEBUG")
    log_glimpse(wellesley_facilities_data, "Glimpse of wellesley_facilities_data")
  } else {
    debug_log(sprintf("Sheet '%s' not found in wellesley_facilities_data_file: %s. Skipping load.", wellesley_sheet_name, wellesley_facilities_data_file), "WARN")
    wellesley_facilities_data <- NULL
  }
} else {
  debug_log(sprintf("wellesley_facilities_data_file not found: %s. Skipping load.", wellesley_facilities_data_file), "WARN")
  wellesley_facilities_data <- NULL
}


# Download and Load StatsAmerica Data
temp_zip <- tempfile(fileext = ".zip")
download_success <- tryCatch({
  safe_download_subproc(statsamerica_zip_url, temp_zip, mode = "wb")
  TRUE
}, error = function(e) {
  debug_log(sprintf("Failed to download StatsAmerica zip file from %s: %s", statsamerica_zip_url, e$message), "ERROR")
  FALSE
})

index_data <- NULL
measures_data <- NULL
if(download_success && file.exists(temp_zip)) {
  debug_log("Downloaded StatsAmerica zip file.", "DEBUG")
  unzip_dir <- tempdir()
  unzip_success <- tryCatch({
    unzip(temp_zip, exdir = unzip_dir)
    TRUE
  }, error = function(e) {
    debug_log(sprintf("Failed to unzip StatsAmerica file %s: %s", temp_zip, e$message), "ERROR")
    FALSE
  })
  
  if (unzip_success) {
    index_file <- file.path(unzip_dir, "Innovation Intelligence - Index Values - States and Counties.csv")
    measures_file <- file.path(unzip_dir, "Innovation Intelligence - Measures - States and Counties.csv")
    
    if(file.exists(index_file)) {
      index_data <- safe_read_csv_subproc(index_file, show_col_types = FALSE)
      debug_log(sprintf("index_data loaded with %d rows.", nrow(index_data)), "DEBUG")
      log_glimpse(index_data, "Glimpse of index_data (StatsAmerica)")
    } else {
      debug_log(sprintf("StatsAmerica index file not found after unzipping: %s", index_file), "WARN")
    }
    
    if(file.exists(measures_file)) {
      measures_data <- safe_read_csv_subproc(measures_file, show_col_types = FALSE)
      debug_log(sprintf("measures_data loaded with %d rows.", nrow(measures_data)), "DEBUG")
      log_glimpse(measures_data, "Glimpse of measures_data (StatsAmerica)")
    } else {
      debug_log(sprintf("StatsAmerica measures file not found after unzipping: %s", measures_file), "WARN")
    }
  }
  # Clean up downloaded zip file
  unlink(temp_zip)
} else if (!download_success) {
  debug_log("StatsAmerica data skipped due to download failure.", "WARN")
}

# Download and Load EIA Sales Data
eia_sales_temp <- tempfile(fileext = ".xlsx")
download_success_eia <- tryCatch({
  safe_download_subproc(eia_sales_url, eia_sales_temp, mode = "wb")
  TRUE
}, error = function(e) {
  debug_log(sprintf("Failed to download EIA sales data from %s: %s", eia_sales_url, e$message), "ERROR")
  FALSE
})

eia_sales <- NULL
if (download_success_eia && file.exists(eia_sales_temp)) {
  debug_log("Downloaded EIA sales data.", "DEBUG")
  eia_sheets <- tryCatch(excel_sheets(eia_sales_temp), error = function(e) NULL)
  if (!is.null(eia_sheets) && length(eia_sheets) > 0) {
    # Assuming the relevant data is on the first sheet, skip first 2 rows as per original script
    eia_sales <- tryCatch({
      read_excel(eia_sales_temp, sheet = 1, skip = 2)
    }, error = function(e) {
      debug_log(sprintf("Error reading EIA sales Excel file %s: %s", eia_sales_temp, e$message), "ERROR")
      NULL # Return NULL on error
    })
    
    if (!is.null(eia_sales)) {
      debug_log(sprintf("eia_sales loaded with %d rows.", nrow(eia_sales)), "DEBUG")
      log_glimpse(eia_sales, "Glimpse of eia_sales")
    }
  } else {
    debug_log(sprintf("Could not read sheets from EIA sales file: %s", eia_sales_temp), "WARN")
  }
  # Clean up downloaded file
  unlink(eia_sales_temp)
} else if (!download_success_eia) {
  debug_log("EIA sales data skipped due to download failure.", "WARN")
}


# Load State Corporate Taxes
if(file.exists(state_corporate_taxes_file)) {
  state_corporate_taxes <- safe_read_csv_subproc(state_corporate_taxes_file)
  debug_log(sprintf("state_corporate_taxes loaded with %d rows.", nrow(state_corporate_taxes)), "DEBUG")
  log_glimpse(state_corporate_taxes, "Glimpse of state_corporate_taxes")
} else {
  debug_log(sprintf("state_corporate_taxes_file not found: %s. Skipping load.", state_corporate_taxes_file), "WARN")
  state_corporate_taxes <- NULL
}

# Load State Capacity Data
if(file.exists(state_capacity_data_file)) {
  state_capacity <- safe_read_csv_subproc(state_capacity_data_file)
  debug_log(sprintf("state_capacity loaded with %d rows.", nrow(state_capacity)), "DEBUG")
  log_glimpse(state_capacity, "Glimpse of state_capacity")
} else {
  debug_log(sprintf("state_capacity_data_file not found: %s. Skipping load.", state_capacity_data_file), "WARN")
  state_capacity <- NULL
}

# Load MESC Portfolio Data
if(file.exists(MESC_portfolio_file)) {
  MESC_portfolio <- safe_read_csv_subproc(MESC_portfolio_file)
  debug_log(sprintf("MESC_portfolio loaded with %d rows.", nrow(MESC_portfolio)), "DEBUG")
  log_glimpse(MESC_portfolio, "Glimpse of MESC_portfolio (loaded in Section 5)")
} else {
  debug_log(sprintf("MESC_portfolio_file not found: %s. Skipping load.", MESC_portfolio_file), "WARN")
  MESC_portfolio <- NULL
}

# Load Investment Data Shared (not used later in original script, but loaded here)
if(file.exists(investment_data_shared_file)) {
  # Determine if it's multi-sheet, read first sheet if unsure
  investment_sheets <- tryCatch(excel_sheets(investment_data_shared_file), error = function(e) NULL)
  if (!is.null(investment_sheets) && length(investment_sheets) > 0) {
    investment_data_shared <- safe_read_excel_subproc(investment_data_shared_file, sheet = investment_sheets[1]) # Read first sheet
    debug_log(sprintf("investment_data_shared loaded from sheet '%s' with %d rows.", investment_sheets[1], nrow(investment_data_shared)), "DEBUG")
    log_glimpse(investment_data_shared, "Glimpse of investment_data_shared")
  } else {
    debug_log(sprintf("Could not read sheets from investment_data_shared_file: %s. Skipping load.", investment_data_shared_file), "WARN")
    investment_data_shared <- NULL
  }
} else {
  debug_log(sprintf("investment_data_shared_file not found: %s. Skipping load.", investment_data_shared_file), "WARN")
  investment_data_shared <- NULL
}

# Load NIST CHIPS Award Data
if(file.exists(nist_chips_award_file)) {
  nist_chips_award_data <- safe_read_csv_subproc(nist_chips_award_file)
  debug_log(sprintf("nist_chips_award_data loaded with %d rows.", nrow(nist_chips_award_data)), "DEBUG")
  log_glimpse(nist_chips_award_data, "Glimpse of nist_chips_award_data (loaded in Section 5)")
} else {
  debug_log(sprintf("nist_chips_award_file not found: %s. Skipping load.", nist_chips_award_file), "WARN")
  nist_chips_award_data <- NULL
}

# Load CIM Data
if(file.exists(cim_data_file)) {
  # Using data.table::fread directly as in original, wrapped in tryCatch
  cim_data <- tryCatch({
    data.table::fread(cim_data_file, skip = 5)
  }, error = function(e) {
    debug_log(sprintf("Error loading CIM data with fread from %s: %s", cim_data_file, e$message), "ERROR")
    NULL
  })
  if (!is.null(cim_data)) {
    debug_log(sprintf("cim_data loaded with %d rows.", nrow(cim_data)), "DEBUG")
    log_glimpse(cim_data, "Glimpse of cim_data (loaded in Section 5)")
  }
} else {
  debug_log(sprintf("cim_data_file not found: %s. Skipping load.", cim_data_file), "WARN")
  cim_data <- NULL
}

# Load XChange Dashboard File (only load sheets here, process later)
if(file.exists(xchange_dashboard_file)) {
  xchange_sheets <- tryCatch(excel_sheets(xchange_dashboard_file), error = function(e) NULL)
  if (!is.null(xchange_sheets)) {
    debug_log(sprintf("XChange dashboard file (%s) found with sheets: %s", xchange_dashboard_file, paste(xchange_sheets, collapse=", ")), "DEBUG")
    # Actual reading deferred to section 10 (original script section 7)
  } else {
    debug_log(sprintf("Could not read sheets from XChange dashboard file: %s. Processing might fail later.", xchange_dashboard_file), "WARN")
  }
} else {
  debug_log(sprintf("xchange_dashboard_file not found: %s. Skipping related processing.", xchange_dashboard_file), "WARN")
  xchange_sheets <- NULL # Indicate file not available
}


debug_log("Completed Section 5: External data loading checks complete. Check warnings for missing files.", "INFO")

# Add checks to ensure essential dataframes were loaded before proceeding
stopifnot("unique_county_geoids must be loaded to proceed" = exists("unique_county_geoids") && !is.null(unique_county_geoids) && nrow(unique_county_geoids) > 0)
# Add other essential data checks if needed, e.g., for investment data
# stopifnot("wellesley_facilities_data must be loaded" = exists("wellesley_facilities_data") && !is.null(wellesley_facilities_data))
# stopifnot("MESC_portfolio must be loaded" = exists("MESC_portfolio") && !is.null(MESC_portfolio))
# stopifnot("nist_chips_award_data must be loaded" = exists("nist_chips_award_data") && !is.null(nist_chips_award_data))
# stopifnot("cim_data must be loaded" = exists("cim_data") && !is.null(cim_data))
# stopifnot("cgt_county must be loaded" = exists("cgt_county") && !is.null(cgt_county))

#Section 6: PROCESS INVESTMENT DATA SOURCES------------------------

###############################################################################
# 3) PROCESS INVESTMENT DATA SOURCES (WELLESLEY, MESC, NIST CHIPS, CIM)
###############################################################################

section_header <- "\n=== SECTION 6: Process Investment Data Sources ===" # Updated section number
cat(section_header)
flog.info(section_header)
debug_log("Starting Section 6: Investment data processing", "DEBUG")

# Initialize placeholder dataframes for results in case input data is missing
counties_wellesley <- unique_county_geoids %>% select(`County GEOID`, geometry) # Start with base geos
counties_mesc <- unique_county_geoids %>% select(`County GEOID`, geometry)
county_chips_award_data <- unique_county_geoids %>% st_drop_geometry() %>% select(`County GEOID`, `County, State`) %>% mutate(CHIPS_Funding_Amount_January_2025 = 0.0) # Default to 0
counties_cim_final <- unique_county_geoids %>% select(`County GEOID`, geometry)

#-----------------------------------------------------------------------------
# 6A) Wellesley Investment Data
#-----------------------------------------------------------------------------
section_header_6a <- "\n=== SECTION 6A: Wellesley Investment Data ===\n"
cat(section_header_6a)
flog.info(section_header_6a)

if (exists("wellesley_facilities_data") && !is.null(wellesley_facilities_data) && nrow(wellesley_facilities_data) > 0) {
  debug_log("Starting Wellesley data processing", "DEBUG")
  
  minerals_products <- c("Cobalt","Graphite","Lithium","Manganese Sulfate","Manganese, Zinc","Nickel","Rare Earths","Silicon","Quartz")
  
  wellesley_processed <- wellesley_facilities_data %>%
    mutate(
      # Standardize Mfg Product names
      `Mfg Product` = case_when(
        Sector=="Solar" & `Mfg Product`=="Cells" ~ "Solar Cells",
        Sector=="Solar" & `Mfg Product`=="Silicon" ~ "Polysilicon", # Corrected typo 'Silion' to 'Silicon' if present
        # Add more standardizations if needed
        TRUE ~ `Mfg Product` # Keep original otherwise
      ),
      # Re-categorize Sector based on Mfg Product if necessary
      Sector = case_when(
        Sector=="EVs" & `Mfg Product`=="Battery Cell Components" ~ "Batteries",
        `Mfg Product` %in% minerals_products ~ "Minerals",
        # Add more re-categorizations if needed
        TRUE ~ Sector # Keep original otherwise
      )
    ) %>%
    # Filter out facilities that are not operational or planned
    filter(
      !(`Operating Status` %in% c("Cancelled","Sold","Closed","Rumored", "On Hold")), # Added On Hold based on potential values
      Country=="USA" # Ensure only US facilities
    ) %>%
    # Convert coordinates to numeric, handling potential errors
    mutate(
      Latitude_num = suppressWarnings(as.numeric(Latitude)),
      Longitude_num = suppressWarnings(as.numeric(Longitude))
    )
  debug_log(sprintf("Wellesley data transformed; rows after initial filtering: %d", nrow(wellesley_processed)), "DEBUG")
  log_glimpse(wellesley_processed, "Glimpse of wellesley_processed (after transformation)")
  
  # Validate coordinates
  invalid_coords <- wellesley_processed %>%
    filter(
      is.na(Latitude_num) | is.na(Longitude_num) |
        Latitude_num < 18 | Latitude_num > 72 | # Reasonable Lat bounds for US
        Longitude_num < -170 | Longitude_num > -65 # Reasonable Lon bounds for US (excluding territories far out)
    )
  if(nrow(invalid_coords) > 0) {
    debug_log(sprintf("Invalid or potentially out-of-bounds Wellesley coords discovered: %d rows. These will be excluded from spatial join.", nrow(invalid_coords)), "WARN")
    # Optionally print/log details of invalid coordinates
    # log_glimpse(invalid_coords, "Invalid Wellesley Coordinates Sample", level="WARN")
  }
  
  wellesley_valid <- wellesley_processed %>%
    filter(
      !is.na(Latitude_num), !is.na(Longitude_num),
      Latitude_num >= 18, Latitude_num <= 72, # Apply reasonable US bounds
      Longitude_num >= -170, Longitude_num <= -65
    )
  debug_log(sprintf("Valid Wellesley facilities count for spatial join: %d", nrow(wellesley_valid)), "DEBUG")
  log_glimpse(wellesley_valid, "Glimpse of wellesley_valid (valid coordinates only)")
  
  # Prepare for spatial join
  counties_for_wellesley <- unique_county_geoids %>% st_transform(4269) # Ensure consistent CRS (NAD83)
  debug_log("Transformed unique_county_geoids to CRS 4269 for Wellesley join.", "DEBUG")
  
  if (nrow(wellesley_valid) > 0) {
    facilities_sf <- st_as_sf(
      wellesley_valid,
      coords = c("Longitude_num","Latitude_num"),
      crs = st_crs(counties_for_wellesley), # Use county CRS
      remove = FALSE # Keep original coordinate columns if needed
    )
    debug_log(sprintf("Converted wellesley_valid to spatial sf object with %d features.", nrow(facilities_sf)), "DEBUG")
    log_glimpse(facilities_sf, "Glimpse of facilities_sf (Wellesley as sf)")
    
    # Convert "Capital Investment \r\n($ million)" to numeric, handling various formats
    capex_col_name <- names(facilities_sf)[grepl("Capital Investment", names(facilities_sf)) & grepl("million", names(facilities_sf))]
    if (length(capex_col_name) == 1) {
      debug_log(sprintf("Using capex column: '%s'", capex_col_name), "DEBUG")
      facilities_sf <- facilities_sf %>%
        mutate(
          capex_raw = !!sym(capex_col_name),
          # Clean string: remove $, commas, whitespace, convert ranges/text to NA
          capex_cleaned = str_replace_all(as.character(capex_raw), "[$,]", ""),
          capex_cleaned = trimws(capex_cleaned),
          # Handle ranges (e.g., "100-200") by taking the lower bound or average, or NA
          # This example takes NA for non-numeric looking strings after cleaning
          capex_numeric = suppressWarnings(as.numeric(capex_cleaned)),
          # Flag investments missing a valid numeric capex
          capex_missing_flag = if_else(
            is.na(capex_numeric) | capex_numeric <= 0 | capex_raw %in% c("","?","TBD", "Undisclosed", NA), # Consider 0 as missing for aggregation? User decision. Added more text NAs.
            1L, 0L
          ),
          # Set NA capex_numeric for those flagged as missing before aggregation
          capex_numeric = if_else(capex_missing_flag == 1L, NA_real_, capex_numeric)
        )
      debug_log("Converted Wellesley capex amounts to numeric, handling various formats.", "DEBUG")
      log_glimpse(facilities_sf %>% select(capex_raw, capex_cleaned, capex_numeric, capex_missing_flag), "Glimpse of Wellesley Capex Cleaning")
    } else {
      debug_log("Wellesley Capex column not found or ambiguous. Proceeding without capex aggregation.", "WARN")
      facilities_sf <- facilities_sf %>% mutate(capex_numeric = NA_real_, capex_missing_flag = 1L) # Add dummy columns if not found
    }
    
    
    # Perform spatial join (which facilities fall within which counties)
    # Use st_within for points in polygons
    facilities_by_county <- st_join(facilities_sf, counties_for_wellesley, join=st_within) %>%
      filter(!is.na(`County GEOID`)) # Ensure join was successful
    
    if (nrow(facilities_by_county) == 0) {
      debug_log("No Wellesley facilities were successfully joined to counties.", "WARN")
      # Create empty summary dataframes to avoid errors later
      facilities_summary <- data.frame(`County GEOID`=character(), Investments_Lacking_Capex_Amount_Wellesley=integer(), Total_Capex_In_County_Wellesley_Data=numeric(), stringsAsFactors=FALSE)
      sector_summary <- data.frame(`County GEOID`=character(), stringsAsFactors=FALSE)
      sector_product_summary <- data.frame(`County GEOID`=character(), stringsAsFactors=FALSE)
    } else {
      debug_log(sprintf("Wellesley facilities spatially joined to counties; joined rows: %d", nrow(facilities_by_county)), "DEBUG")
      log_glimpse(facilities_by_county %>% st_drop_geometry() %>% head(), "Glimpse of facilities_by_county (head)")
      
      # Summarize investment presence and capex at county level
      facilities_summary <- facilities_by_county %>%
        st_drop_geometry() %>% # Drop geometry for faster aggregation
        group_by(`County GEOID`) %>%
        summarize(
          Investment_Presence_Wellesley_Data_Calc = TRUE, # If it's in this group, presence is true
          Investments_Lacking_Capex_Amount_Wellesley = sum(capex_missing_flag, na.rm=TRUE),
          Total_Capex_In_County_Wellesley_Data = sum(capex_numeric, na.rm=TRUE), # Sums only valid numeric capex
          Total_Facility_Count_Wellesley = n(), # Count facilities
          .groups="drop"
        )
      debug_log("Summarized Wellesley facilities capex and counts by county.", "DEBUG")
      log_glimpse(facilities_summary, "Glimpse of facilities_summary")
      
      # Summarize by Sector
      sector_summary <- facilities_by_county %>%
        st_drop_geometry() %>%
        filter(!is.na(Sector) & Sector != "") %>% # Filter out missing/empty sectors
        group_by(`County GEOID`, Sector) %>%
        summarize(
          capex_sector = sum(capex_numeric, na.rm=TRUE),
          count_sector = n(),
          .groups="drop"
        ) %>%
        tidyr::pivot_wider(
          id_cols = `County GEOID`,
          names_from = Sector,
          values_from = c(capex_sector, count_sector),
          names_glue = "Wellesley_{Sector}_{.value}", # Create unique names
          values_fill = list(capex_sector = 0, count_sector = 0) # Fill missing combinations with 0
        )
      debug_log("Created sector summary (capex and count) for Wellesley data.", "DEBUG")
      log_glimpse(sector_summary, "Glimpse of sector_summary")
      
      # Summarize by Sector and Product (may create many columns)
      sector_product_summary <- facilities_by_county %>%
        st_drop_geometry() %>%
        filter(!is.na(Sector) & Sector != "" & !is.na(`Mfg Product`) & `Mfg Product` != "") %>%
        mutate(Mfg_Product_Clean = str_replace_all(`Mfg Product`, "[^A-Za-z0-9_]", "_")) %>% # Clean product names for column names
        group_by(`County GEOID`, Sector, Mfg_Product_Clean) %>%
        summarize(
          capex_sector_product = sum(capex_numeric, na.rm=TRUE),
          count_sector_product = n(),
          .groups="drop"
        ) %>%
        pivot_wider(
          id_cols = `County GEOID`,
          names_from = c(Sector, Mfg_Product_Clean),
          values_from = c(capex_sector_product, count_sector_product),
          names_glue = "Wellesley_{Sector}_{Mfg_Product_Clean}_{.value}",
          values_fill = list(capex_sector_product = 0, count_sector_product = 0)
        )
      debug_log("Created sector and product summary (capex and count) for Wellesley data.", "DEBUG")
      log_glimpse(sector_product_summary %>% select(1:min(6, ncol(.))), "Glimpse of sector_product_summary (first few cols)") # Glimpse only first few cols due to potentially large size
    }
    
    # Merge summaries back to the base county dataframe
    counties_wellesley <- counties_for_wellesley %>% # Start with the correctly projected counties
      safe_left_join(facilities_summary, by="County GEOID", y_name="facilities_summary") %>%
      safe_left_join(sector_summary, by="County GEOID", y_name="sector_summary") %>%
      safe_left_join(sector_product_summary, by="County GEOID", y_name="sector_product_summary")
    
    # Clean up NAs introduced by joins - replace with FALSE for presence and 0 for counts/sums
    wellesley_cols <- setdiff(names(counties_wellesley), c("County GEOID", "geometry")) # Get all newly added columns
    for(col_nm in wellesley_cols) {
      if (grepl("_Presence_", col_nm, ignore.case = TRUE) || is.logical(counties_wellesley[[col_nm]])) {
        counties_wellesley[[col_nm]] <- replace_na(counties_wellesley[[col_nm]], FALSE)
      } else if (is.numeric(counties_wellesley[[col_nm]])) {
        counties_wellesley[[col_nm]] <- replace_na(counties_wellesley[[col_nm]], 0)
      } else if (is.integer(counties_wellesley[[col_nm]])) {
        counties_wellesley[[col_nm]] <- replace_na(counties_wellesley[[col_nm]], 0L)
      }
    }
    # Ensure the primary presence flag is correct
    if("Investment_Presence_Wellesley_Data_Calc" %in% names(counties_wellesley)) {
      names(counties_wellesley)[names(counties_wellesley) == "Investment_Presence_Wellesley_Data_Calc"] <- "Investment_Presence_Wellesley_Data"
    } else {
      counties_wellesley$Investment_Presence_Wellesley_Data <- FALSE # Add if somehow missing
    }
    
    
    # Create logical "presence" columns for each sector/product count > 0
    count_cols <- grep("_count", names(counties_wellesley), value=TRUE)
    for(colnm in count_cols) {
      # Create a YesNo name, avoiding potential conflicts if _YesNo already exists
      yesno_name <- sub("_count$", "_Presence", colnm) # More direct replacement
      if (!yesno_name %in% names(counties_wellesley)) { # Only add if it doesn't exist
        counties_wellesley[[yesno_name]] <- counties_wellesley[[colnm]] > 0
        debug_log(sprintf("Created logical column '%s' from count column '%s'.", yesno_name, colnm), "DEBUG")
      }
    }
    
    log_glimpse(counties_wellesley, "Glimpse of counties_wellesley (final for this section)")
    
  } else {
    debug_log("No valid Wellesley facilities found after filtering and coordinate checks. Skipping spatial join and aggregation.", "WARN")
    # Ensure base dataframe has expected columns, filled with defaults
    counties_wellesley$Investment_Presence_Wellesley_Data <- FALSE
    counties_wellesley$Investments_Lacking_Capex_Amount_Wellesley <- 0L
    counties_wellesley$Total_Capex_In_County_Wellesley_Data <- 0.0
    counties_wellesley$Total_Facility_Count_Wellesley <- 0L
  }
  
} else {
  debug_log("Wellesley facilities data not loaded or empty. Skipping Wellesley processing.", "WARN")
  # Ensure base dataframe exists with default columns if data was skipped
  counties_wellesley$Investment_Presence_Wellesley_Data <- FALSE
  counties_wellesley$Investments_Lacking_Capex_Amount_Wellesley <- 0L
  counties_wellesley$Total_Capex_In_County_Wellesley_Data <- 0.0
  counties_wellesley$Total_Facility_Count_Wellesley <- 0L
}
debug_log("Finished Wellesley data processing step.", "DEBUG")


#-----------------------------------------------------------------------------
# 6B) MESC Awards Data
#-----------------------------------------------------------------------------
section_header_6b <- "\n=== SECTION 6B: MESC Awards Data ===\n"
cat(section_header_6b)
flog.info(section_header_6b)

if (exists("MESC_portfolio") && !is.null(MESC_portfolio) && nrow(MESC_portfolio) > 0) {
  debug_log("Starting MESC data processing", "DEBUG")
  # Use the MESC_portfolio loaded in Section 5
  log_glimpse(MESC_portfolio, "Glimpse of MESC_portfolio (start of 6B)")
  
  unique_categories <- unique(MESC_portfolio$OverarchingCategory)
  message_cats <- "Unique OverarchingCategory values in MESC data:"
  cat(message_cats, "\n")
  flog.info("%s %s", message_cats, paste(unique_categories, collapse=", "))
  print(unique_categories)
  
  # Convert FederalShare from character to numeric
  # Handle potential non-standard numeric characters
  mesc_processed <- MESC_portfolio %>%
    mutate(
      FederalShare_cleaned = str_replace_all(FederalShare, "[\\$,]", ""), # Remove $ and commas
      FederalShare_num = suppressWarnings(as.numeric(FederalShare_cleaned)),
      FederalShare_missing_flag = is.na(FederalShare_num)
    )
  debug_log("Converted FederalShare to numeric in MESC_portfolio.", "DEBUG")
  log_glimpse(mesc_processed %>% select(FederalShare, FederalShare_cleaned, FederalShare_num, FederalShare_missing_flag), "Glimpse of MESC FederalShare Cleaning")
  
  # Check lat/lng validity
  invalid_coords_mesc <- mesc_processed %>%
    filter(is.na(lat) | is.na(lng) | lat < 18 | lat > 72 | lng < -170 | lng > -65) # Use US bounds
  
  if(nrow(invalid_coords_mesc) > 0) {
    debug_log(sprintf("Invalid or out-of-bounds MESC facility coords discovered: %d rows. Excluding from spatial join.", nrow(invalid_coords_mesc)), "WARN")
    # log_glimpse(invalid_coords_mesc, "Invalid MESC Coordinates Sample", level="WARN")
  }
  
  valid_mesc <- mesc_processed %>%
    filter(!is.na(lat) & !is.na(lng) &
             lat >= 18 & lat <= 72 &
             lng >= -170 & lng <= -65)
  debug_log(sprintf("MESC valid facilities count for spatial join: %d", nrow(valid_mesc)), "DEBUG")
  log_glimpse(valid_mesc, "Glimpse of valid_mesc (valid coords only)")
  
  # Prepare for spatial join
  counties_for_mesc <- unique_county_geoids %>% st_transform(4269) # Use NAD83
  
  if (nrow(valid_mesc) > 0) {
    mesc_sf <- st_as_sf(valid_mesc, coords = c("lng", "lat"), crs = 4269, remove = FALSE) # Assume WGS84/NAD83 input
    debug_log(sprintf("Converted valid MESC data to spatial sf object with %d features.", nrow(mesc_sf)), "DEBUG")
    log_glimpse(mesc_sf, "Glimpse of mesc_sf")
    
    mesc_by_county <- st_join(mesc_sf, counties_for_mesc, join = st_within) %>%
      filter(!is.na(`County GEOID`))
    
    if (nrow(mesc_by_county) > 0) {
      debug_log(sprintf("MESC spatial join completed; result rows: %d", nrow(mesc_by_county)), "DEBUG")
      log_glimpse(mesc_by_county %>% st_drop_geometry() %>% head(), "Glimpse of mesc_by_county (head)")
      
      # Summarize MESC awards by county
      mesc_summary <- mesc_by_county %>%
        st_drop_geometry() %>%
        group_by(`County GEOID`) %>%
        summarize(
          MESC_Award_Presence_Calc = TRUE,
          MESC_Award_Total_Quantity = n(),
          MESC_Award_Quantity_Manufacturing_Capacity = sum(OverarchingCategory == "Manufacturing Capacity", na.rm = TRUE),
          MESC_Award_Quantity_Workforce_Deployment = sum(OverarchingCategory == "Workforce Deployment", na.rm = TRUE),
          # Add other categories if needed, e.g., "Supply Chain"
          MESC_Award_Quantity_Supply_Chain = sum(OverarchingCategory == "Supply Chain", na.rm = TRUE),
          MESC_Award_Quantity_Other_Category = sum(!OverarchingCategory %in% c("Manufacturing Capacity", "Workforce Deployment", "Supply Chain"), na.rm = TRUE),
          MESC_All_Awards_Total_Federal_Investment = sum(FederalShare_num, na.rm = TRUE),
          MESC_Workforce_Awards_Total_Federal_Investment = sum(ifelse(OverarchingCategory == "Workforce Deployment", FederalShare_num, 0), na.rm = TRUE),
          MESC_Manufacturing_Capacity_Awards_Total_Federal_Investment = sum(ifelse(OverarchingCategory == "Manufacturing Capacity", FederalShare_num, 0), na.rm = TRUE),
          MESC_Supply_Chain_Awards_Total_Federal_Investment = sum(ifelse(OverarchingCategory == "Supply Chain", FederalShare_num, 0), na.rm = TRUE),
          MESC_Other_Category_Awards_Total_Federal_Investment = sum(ifelse(!OverarchingCategory %in% c("Manufacturing Capacity", "Workforce Deployment", "Supply Chain"), FederalShare_num, 0), na.rm = TRUE),
          .groups = "drop"
        )
      debug_log("Summarized MESC awards by county.", "DEBUG")
      log_glimpse(mesc_summary, "Glimpse of mesc_summary")
      
      # Merge summary back
      counties_mesc <- safe_left_join(
        counties_for_mesc, # Start with base county geos
        mesc_summary,
        by = "County GEOID",
        y_name = "mesc_summary"
      )
    } else {
      debug_log("No MESC facilities were successfully joined to counties.", "WARN")
      # If join results in zero rows, still create the columns with default values
      counties_mesc <- counties_for_mesc %>%
        mutate(
          MESC_Award_Presence_Calc = FALSE,
          MESC_Award_Total_Quantity = 0L,
          MESC_Award_Quantity_Manufacturing_Capacity = 0L,
          MESC_Award_Quantity_Workforce_Deployment = 0L,
          MESC_Award_Quantity_Supply_Chain = 0L,
          MESC_Award_Quantity_Other_Category = 0L,
          MESC_All_Awards_Total_Federal_Investment = 0.0,
          MESC_Workforce_Awards_Total_Federal_Investment = 0.0,
          MESC_Manufacturing_Capacity_Awards_Total_Federal_Investment = 0.0,
          MESC_Supply_Chain_Awards_Total_Federal_Investment = 0.0,
          MESC_Other_Category_Awards_Total_Federal_Investment = 0.0
        )
    }
    
    # Clean up NAs from join and ensure correct types
    mesc_cols <- setdiff(names(counties_mesc), c("County GEOID", "geometry"))
    for(col_nm in mesc_cols) {
      if (grepl("_Presence_", col_nm, ignore.case = TRUE) || is.logical(counties_mesc[[col_nm]])) {
        counties_mesc[[col_nm]] <- replace_na(counties_mesc[[col_nm]], FALSE)
      } else if (is.numeric(counties_mesc[[col_nm]])) {
        counties_mesc[[col_nm]] <- replace_na(counties_mesc[[col_nm]], 0)
      } else if (is.integer(counties_mesc[[col_nm]])) {
        counties_mesc[[col_nm]] <- replace_na(counties_mesc[[col_nm]], 0L)
      }
    }
    # Rename presence flag
    if ("MESC_Award_Presence_Calc" %in% names(counties_mesc)) {
      names(counties_mesc)[names(counties_mesc) == "MESC_Award_Presence_Calc"] <- "MESC_Award_Presence"
    } else {
      counties_mesc$MESC_Award_Presence <- FALSE
    }
    
    
    message_mesc_final <- "=== 'counties_mesc' final version with geometry + MESC Award info. ==="
    cat(message_mesc_final, "\n")
    flog.info(message_mesc_final)
    log_glimpse(counties_mesc, "Glimpse of counties_mesc (final for this section)")
    
  } else {
    debug_log("No valid MESC facilities found after filtering. Skipping spatial join and aggregation.", "WARN")
    # Create default columns on the base county dataframe
    counties_mesc <- counties_for_mesc %>%
      mutate(
        MESC_Award_Presence = FALSE,
        MESC_Award_Total_Quantity = 0L,
        MESC_Award_Quantity_Manufacturing_Capacity = 0L,
        MESC_Award_Quantity_Workforce_Deployment = 0L,
        MESC_Award_Quantity_Supply_Chain = 0L,
        MESC_Award_Quantity_Other_Category = 0L,
        MESC_All_Awards_Total_Federal_Investment = 0.0,
        MESC_Workforce_Awards_Total_Federal_Investment = 0.0,
        MESC_Manufacturing_Capacity_Awards_Total_Federal_Investment = 0.0,
        MESC_Supply_Chain_Awards_Total_Federal_Investment = 0.0,
        MESC_Other_Category_Awards_Total_Federal_Investment = 0.0
      )
  }
  
} else {
  debug_log("MESC portfolio data not loaded or empty. Skipping MESC processing.", "WARN")
  # Create default columns on the base county dataframe
  counties_mesc <- unique_county_geoids %>% select(`County GEOID`, geometry) %>% # Start with base geos
    mutate(
      MESC_Award_Presence = FALSE,
      MESC_Award_Total_Quantity = 0L,
      MESC_Award_Quantity_Manufacturing_Capacity = 0L,
      MESC_Award_Quantity_Workforce_Deployment = 0L,
      MESC_Award_Quantity_Supply_Chain = 0L,
      MESC_Award_Quantity_Other_Category = 0L,
      MESC_All_Awards_Total_Federal_Investment = 0.0,
      MESC_Workforce_Awards_Total_Federal_Investment = 0.0,
      MESC_Manufacturing_Capacity_Awards_Total_Federal_Investment = 0.0,
      MESC_Supply_Chain_Awards_Total_Federal_Investment = 0.0,
      MESC_Other_Category_Awards_Total_Federal_Investment = 0.0
    )
}
debug_log("Finished MESC data processing step.", "DEBUG")


#-----------------------------------------------------------------------------
# 6C) NIST CHIPS Awards Data
#-----------------------------------------------------------------------------
section_header_6c <- "\n=== SECTION 6C: NIST CHIPS Awards Data ===\n"
cat(section_header_6c)
flog.info(section_header_6c)

if (exists("nist_chips_award_data") && !is.null(nist_chips_award_data) && nrow(nist_chips_award_data) > 0) {
  debug_log("Starting NIST CHIPS data processing", "DEBUG")
  # Use nist_chips_award_data loaded in Section 5
  chips_raw <- nist_chips_award_data
  log_glimpse(chips_raw, "Glimpse of chips_raw (start of 6C)")
  
  # Check required columns
  required_cols_chips <- c("County, State", "amount_millions")
  if (!all(required_cols_chips %in% names(chips_raw))) {
    debug_log(sprintf("NIST CHIPS data is missing required columns (%s). Skipping processing.", paste(required_cols_chips[!required_cols_chips %in% names(chips_raw)], collapse=", ")), "ERROR")
    # Keep default county_chips_award_data initialized earlier
  } else {
    chips_processed <- chips_raw %>%
      mutate(
        # Ensure amount is numeric, default NA to 0
        amount_millions_num = suppressWarnings(as.numeric(amount_millions)),
        amount_millions_num = replace_na(amount_millions_num, 0),
        # Clean County, State field if necessary (e.g., extra whitespace)
        `County, State` = trimws(`County, State`)
      ) %>%
      filter(!is.na(`County, State`) & `County, State` != "") # Remove rows with missing county identifier
    
    debug_log("Processed CHIPS data: cleaned amounts and county names.", "DEBUG")
    log_glimpse(chips_processed, "Glimpse of chips_processed")
    
    # Aggregate funding by "County, State"
    chips_aggregated_by_county_state <- chips_processed %>%
      group_by(`County, State`) %>%
      summarize(
        CHIPS_Funding_Amount_January_2025_calc = sum(amount_millions_num, na.rm = TRUE) * 1e6, # Convert millions to dollars
        CHIPS_Award_Count = n(), # Count number of awards per county
        .groups = "drop"
      )
    debug_log("Aggregated NIST CHIPS funding data by 'County, State'.", "DEBUG")
    log_glimpse(chips_aggregated_by_county_state, "Glimpse of chips_aggregated_by_county_state")
    
    # Join aggregated data back to the unique county list using "County, State"
    # Start with the base county list (no geometry needed here)
    county_chips_award_data_temp <- unique_county_geoids %>%
      st_drop_geometry() %>%
      select(`County GEOID`, `County, State`) %>%
      left_join(
        chips_aggregated_by_county_state,
        by = "County, State"
      ) %>%
      mutate(
        # Replace NAs from non-matching counties with 0
        CHIPS_Funding_Amount_January_2025 = replace_na(CHIPS_Funding_Amount_January_2025_calc, 0),
        CHIPS_Award_Count = replace_na(CHIPS_Award_Count, 0L),
        # Create presence flag
        CHIPS_Award_Presence = (CHIPS_Award_Count > 0)
      ) %>%
      select(`County GEOID`, `County, State`, CHIPS_Funding_Amount_January_2025, CHIPS_Award_Count, CHIPS_Award_Presence) # Select final columns
    
    # Check for counties listed in CHIPS data but not found in unique_county_geoids
    missing_counties <- anti_join(chips_aggregated_by_county_state, county_chips_award_data_temp, by = "County, State")
    if(nrow(missing_counties) > 0) {
      debug_log(sprintf("Warning: %d 'County, State' entries from CHIPS data were not found in unique_county_geoids.", nrow(missing_counties)), "WARN")
      # flog.warn("Missing CHIPS counties: %s", paste(missing_counties$`County, State`, collapse="; "))
    }
    
    # Overwrite the placeholder dataframe
    county_chips_award_data <- county_chips_award_data_temp
    log_glimpse(county_chips_award_data, "Glimpse of county_chips_award_data (final)")
  }
  
} else {
  debug_log("NIST CHIPS award data not loaded or empty. Skipping CHIPS processing. Funding will be 0.", "WARN")
  # Ensure default columns exist if data skipped
  county_chips_award_data <- county_chips_award_data %>%
    mutate(CHIPS_Award_Count = 0L, CHIPS_Award_Presence = FALSE)
}
debug_log("Finished NIST CHIPS data processing step.", "DEBUG")


#-----------------------------------------------------------------------------
# 6D) Clean Investment Monitor (CIM) Data
#-----------------------------------------------------------------------------
section_header_6d <- "\n=== SECTION 6D: Clean Investment Monitor (CIM) Data ===\n"
cat(section_header_6d)
flog.info(section_header_6d)

if (exists("cim_data") && !is.null(cim_data) && nrow(cim_data) > 0) {
  debug_log("Starting CIM data processing", "DEBUG")
  # Use cim_data loaded in Section 5 (should be a data.table)
  cim_raw <- data.table::as.data.table(cim_data) # Ensure it's a data.table
  log_glimpse(cim_raw, "Glimpse of cim_raw (start of 6D)")
  
  # Data Cleaning and Transformation
  # Convert date columns safely
  date_cols <- c("Announcement_Date", "Construction_Start", "Construction_End")
  for (col in date_cols) {
    if (col %in% names(cim_raw)) {
      # Try different formats if needed, handle failures gracefully
      cim_raw[, (col) := as.Date(get(col), format="%Y-%m-%d")] # Assume YYYY-MM-DD format
      debug_log(sprintf("Converted CIM column '%s' to Date.", col), "DEBUG")
    } else {
      debug_log(sprintf("CIM Date column '%s' not found.", col), "WARN")
    }
  }
  
  # Convert CAPEX, ensuring it exists and handling potential issues
  capex_col_cim <- "Estimated_Total_Facility_CAPEX"
  if (capex_col_cim %in% names(cim_raw)) {
    cim_raw[, (capex_col_cim) := suppressWarnings(as.numeric(get(capex_col_cim)))]
    cim_raw[, (capex_col_cim) := ifelse(is.na(get(capex_col_cim)), 0, get(capex_col_cim) * 1e6)] # Convert millions to dollars, NA becomes 0
    debug_log(sprintf("Converted CIM column '%s' to numeric dollars.", capex_col_cim), "DEBUG")
  } else {
    debug_log(sprintf("CIM CAPEX column '%s' not found. Setting to 0.", capex_col_cim), "WARN")
    cim_raw[, (capex_col_cim) := 0.0] # Create column if missing
  }
  
  
  # Filter out non-operational statuses
  status_col <- "Current_Facility_Status"
  exclude_statuses <- c("Canceled prior to operation", "Retired", "Cancelled", "On hold", "Rumored") # Expanded list
  if (status_col %in% names(cim_raw)) {
    cim_filtered <- cim_raw[!(get(status_col) %in% exclude_statuses), ]
    debug_log(sprintf("cim_filtered: %d rows remain after filtering statuses (%s).", nrow(cim_filtered), paste(exclude_statuses, collapse=", ")), "DEBUG")
  } else {
    debug_log(sprintf("CIM Status column '%s' not found. No status filtering applied.", status_col), "WARN")
    cim_filtered <- cim_raw # Use unfiltered data if status column is missing
  }
  
  log_glimpse(cim_filtered, "Glimpse of cim_filtered (after status filter)")
  
  # Add Post-IRA flag (ensure Announcement_Date exists and is Date type)
  ira_date <- as.Date("2022-08-16")
  if ("Announcement_Date" %in% names(cim_filtered) && inherits(cim_filtered$Announcement_Date, "Date")) {
    cim_filtered[, Investment_Announced_Post_IRA := (Announcement_Date >= ira_date & !is.na(Announcement_Date))]
    debug_log("Labeled CIM investments as pre/post IRA based on Announcement_Date.", "DEBUG")
  } else {
    debug_log("CIM Announcement_Date column missing or not Date type. Cannot create Post-IRA flag.", "WARN")
    cim_filtered[, Investment_Announced_Post_IRA := NA] # Add placeholder
  }
  
  
  # Prepare for aggregation by County GEOID
  # Ensure county geoid column exists and format it
  geoid_col_cim <- "county_2020_geoid" # As used in original script
  if (!geoid_col_cim %in% names(cim_filtered)) {
    debug_log(sprintf("CIM GEOID column '%s' not found. Cannot aggregate by county.", geoid_col_cim), "ERROR")
    # Skip aggregation, counties_cim_final will remain the base unique_county_geoids
  } else {
    cim_filtered[, `County GEOID` := sprintf("%05d", as.integer(get(geoid_col_cim)))] # Format GEOID
    debug_log("Formatted CIM County GEOID.", "DEBUG")
    
    # Initialize aggregation dataframe
    cim_aggregated <- unique_county_geoids %>%
      st_drop_geometry() %>%
      select(`County GEOID`) # Start with all unique county GEOIDs
    
    # Aggregate by Decarb_Sector (ensure column exists)
    sector_col_cim <- "Decarb_Sector"
    if (!sector_col_cim %in% names(cim_filtered)) {
      debug_log(sprintf("CIM Sector column '%s' not found. Cannot aggregate by sector.", sector_col_cim), "WARN")
      # Potentially perform an overall aggregation instead?
      # For now, skip sector aggregation if column missing.
    } else {
      # Identify unique valid sectors for aggregation
      decarb_sectors <- unique(na.omit(cim_filtered[[sector_col_cim]]))
      decarb_sectors <- decarb_sectors[decarb_sectors != ""] # Remove empty strings
      debug_log(sprintf("Identified CIM decarb sectors for aggregation: %s", paste(decarb_sectors, collapse = ", ")), "DEBUG")
      
      if (length(decarb_sectors) > 0) {
        for(s in decarb_sectors) {
          # Create clean & informative column names
          s_clean <- make.names(s) # Use make.names for safety
          col_presence <- paste0("CIM_Presence_", s_clean)
          col_total <- paste0("CIM_Total_Capex_", s_clean)
          col_pre <- paste0("CIM_PreIRA_Capex_", s_clean)
          col_post <- paste0("CIM_PostIRA_Capex_", s_clean)
          col_count <- paste0("CIM_Facility_Count_", s_clean)
          
          # Aggregate for the current sector 's'
          # Ensure necessary columns exist for aggregation
          agg_cols_needed <- c("County GEOID", capex_col_cim, "Investment_Announced_Post_IRA")
          if (!all(agg_cols_needed %in% names(cim_filtered))) {
            debug_log(sprintf("Skipping aggregation for sector '%s' due to missing columns (%s).", s, paste(agg_cols_needed[!agg_cols_needed %in% names(cim_filtered)], collapse=",")), "WARN")
            next # Skip to next sector
          }
          
          subset_s <- cim_filtered[get(sector_col_cim) == s, ]
          agg_s <- subset_s[, .(
            total_capex = sum(get(capex_col_cim), na.rm=TRUE),
            # Handle NA in Post_IRA flag if it occurred
            pre_ira = sum(ifelse(is.na(Investment_Announced_Post_IRA) | !Investment_Announced_Post_IRA, get(capex_col_cim), 0), na.rm=TRUE),
            post_ira = sum(ifelse(is.na(Investment_Announced_Post_IRA) | Investment_Announced_Post_IRA, get(capex_col_cim), 0), na.rm=TRUE),
            facility_count = .N # Count rows in this group
          ), by = `County GEOID`]
          
          # Perform left join onto the main aggregation table
          cim_aggregated <- merge(cim_aggregated, agg_s, by = "County GEOID", all.x = TRUE)
          
          # Rename the newly merged columns and set presence flag
          # Check if columns were actually added before renaming/mutating
          if ("total_capex" %in% names(cim_aggregated)) {
            setnames(cim_aggregated, "total_capex", col_total)
            setnames(cim_aggregated, "pre_ira", col_pre)
            setnames(cim_aggregated, "post_ira", col_post)
            setnames(cim_aggregated, "facility_count", col_count)
            
            # Add presence column based on the count column
            cim_aggregated[, (col_presence) := get(col_count) > 0]
            # Fill NAs introduced by the merge with 0 for numeric/integer and FALSE for logical
            cim_aggregated[is.na(get(col_total)), (col_total) := 0]
            cim_aggregated[is.na(get(col_pre)), (col_pre) := 0]
            cim_aggregated[is.na(get(col_post)), (col_post) := 0]
            cim_aggregated[is.na(get(col_count)), (col_count) := 0L]
            cim_aggregated[is.na(get(col_presence)), (col_presence) := FALSE]
            
          } else {
            debug_log(sprintf("Columns not added for sector '%s', potentially due to empty subset or join issue.", s), "WARN")
          }
          
          debug_log(sprintf("Processed CIM decarb sector '%s'", s), "DEBUG")
        }
      } else {
        debug_log("No valid CIM decarb sectors found for aggregation.", "WARN")
      }
    } # End sector aggregation
    
    # Merge aggregated CIM data back to the main county sf object
    counties_cim_final <- safe_left_join(
      unique_county_geoids, # Start with the base sf object
      cim_aggregated,       # Join the aggregated data.table
      by = "County GEOID",
      y_name = "cim_aggregated"
    )
    
    # Final NA fill for any columns that might still be NA (e.g., counties with no CIM data at all)
    cim_agg_cols <- setdiff(names(cim_aggregated), "County GEOID")
    for(col in cim_agg_cols) {
      if (col %in% names(counties_cim_final)) {
        if (is.logical(counties_cim_final[[col]])) {
          counties_cim_final[[col]][is.na(counties_cim_final[[col]])] <- FALSE
        } else if (is.numeric(counties_cim_final[[col]])) {
          counties_cim_final[[col]][is.na(counties_cim_final[[col]])] <- 0
        } else if (is.integer(counties_cim_final[[col]])) {
          counties_cim_final[[col]][is.na(counties_cim_final[[col]])] <- 0L
        }
      }
    }
    
    debug_log("Merged CIM aggregated data into counties_cim_final.", "DEBUG")
    log_glimpse(counties_cim_final, "Glimpse of counties_cim_final")
    
  } # End if GEOID column exists
  
} else {
  debug_log("CIM data not loaded or empty. Skipping CIM processing.", "WARN")
  # counties_cim_final remains the base unique_county_geoids with no CIM columns
}
debug_log("Finished CIM data processing step.", "DEBUG")

debug_log("Completed Section 6: Process Investment Data Sources", "INFO")

#Section 7: CGT DATA PROCESSING------------------------

###############################################################################
# 4) CGT DATA (BROADER + MORE RIGOROUS)
###############################################################################

section_header <- "\n=== SECTION 7: CGT Data Processing ===" # Updated section number
cat(section_header)
flog.info(section_header)
debug_log("Starting Section 7: CGT data processing", "DEBUG")

# Initialize placeholder
counties_cgt <- unique_county_geoids %>% select(`County GEOID`, geometry)

if (exists("cgt_county") && !is.null(cgt_county) && nrow(cgt_county) > 0) {
  # Use cgt_county loaded in Section 5
  log_glimpse(cgt_county, "Glimpse of cgt_county (start of Section 7)")
  
  # Check for county_geoid column and handle missing values
  if(!("county_geoid" %in% names(cgt_county))) {
    debug_log("CGT data is missing 'county_geoid' column. Skipping CGT processing.", "ERROR")
    # Add default columns to counties_cgt
    counties_cgt$County_Data_Available_CGT <- FALSE
    counties_cgt$county_economic_complexity_index <- NA_real_
    counties_cgt$county_complexity_outlook_index <- NA_real_
    counties_cgt$diversity <- NA_real_
    
  } else {
    cgt_county_processed <- cgt_county %>%
      filter(!is.na(county_geoid) & county_geoid != "") %>%
      mutate(county_geoid = sprintf("%05d", as.integer(county_geoid))) # Format GEOID
    
    n_removed <- nrow(cgt_county) - nrow(cgt_county_processed)
    if(n_removed > 0) {
      debug_log(sprintf("Removed %d rows from cgt_county with missing or empty county_geoid.", n_removed), "WARN")
    } else {
      debug_log("No missing county_geoid in cgt_county.", "DEBUG")
    }
    
    # Coverage: Check which counties have any data in the CGT file
    cgt_coverage <- cgt_county_processed %>%
      distinct(county_geoid) %>%
      mutate(County_Data_Available_CGT = TRUE)
    
    counties_cgt_join <- safe_left_join(
      unique_county_geoids %>% select(`County GEOID`, geometry), # Start with base geos
      cgt_coverage,
      by = c("County GEOID" = "county_geoid"),
      y_name = "cgt_coverage"
    ) %>%
      mutate(
        County_Data_Available_CGT = replace_na(County_Data_Available_CGT, FALSE)
      )
    debug_log("Merged CGT coverage data into counties_cgt_join.", "DEBUG")
    log_glimpse(counties_cgt_join, "Glimpse of counties_cgt_join (after coverage join)")
    
    # Baseline ECI, COI, Diversity
    # Ensure columns exist before selecting
    baseline_cols <- c("county_geoid", "eci", "coi", "diversity")
    if (all(baseline_cols %in% names(cgt_county_processed))) {
      cgt_baseline <- cgt_county_processed %>%
        select(all_of(baseline_cols)) %>%
        mutate(
          # Replace Inf with NA, ensure numeric
          eci = as.numeric(if_else(is.infinite(eci), NA_real_, eci)),
          coi = as.numeric(if_else(is.infinite(coi), NA_real_, coi)),
          diversity = as.numeric(if_else(is.infinite(diversity), NA_real_, diversity))
        ) %>%
        distinct(county_geoid, .keep_all = TRUE) %>% # Keep only one record per county for baseline stats
        rename(
          county_economic_complexity_index = eci,
          county_complexity_outlook_index = coi,
          # Keep 'diversity' as is, or rename if desired
          # county_diversity = diversity
        )
      debug_log("Constructed CGT baseline data (ECI, COI, Diversity).", "DEBUG")
      log_glimpse(cgt_baseline, "Glimpse of cgt_baseline")
    } else {
      debug_log("Missing one or more baseline CGT columns (eci, coi, diversity). Skipping baseline creation.", "WARN")
      # Create empty baseline to avoid errors later
      cgt_baseline <- data.frame(county_geoid = character(0),
                                 county_economic_complexity_index=numeric(0),
                                 county_complexity_outlook_index=numeric(0),
                                 diversity=numeric(0),
                                 stringsAsFactors = FALSE)
    }
    
    
    # Pivot RCA and Density for all industries
    pivot_cols <- c("county_geoid", "industry_desc", "industry_code", "rca", "density")
    if (all(pivot_cols %in% names(cgt_county_processed))) {
      cgt_long <- cgt_county_processed %>%
        select(all_of(pivot_cols)) %>%
        filter(!is.na(industry_desc) & industry_desc != "" & !is.na(industry_code)) %>% # Ensure industry identifiers are valid
        mutate(
          # county_geoid already formatted
          rca = as.numeric(if_else(is.infinite(rca), NA_real_, rca)),
          density = as.numeric(if_else(is.infinite(density), NA_real_, density)),
          # Create clean, relatively short, unique column names for pivoting
          # Using make.names for robustness, then shortening if needed
          industry_desc_clean = make.names(paste(industry_desc, industry_code, sep="_"), unique = FALSE),
          # Optional: shorten if names are excessively long
          # industry_desc_clean = str_sub(industry_desc_clean, 1, 60) # Example limit
        ) %>%
        # Check for duplicate clean names - needs handling if happens
        # distinct(county_geoid, industry_desc_clean, industry_code, rca, density)
        select(county_geoid, industry_desc_clean, rca, density) %>%
        distinct() # Keep distinct combinations
      
      debug_log("Prepared CGT long format data for pivoting.", "DEBUG")
      log_glimpse(cgt_long %>% head(), "Glimpse of cgt_long (head)")
      
      # Check for and handle potential duplicate industry_desc_clean within a county
      duplicates_check <- cgt_long %>%
        group_by(county_geoid, industry_desc_clean) %>%
        filter(n() > 1)
      if (nrow(duplicates_check) > 0) {
        debug_log(sprintf("Warning: Found %d instances where industry_desc_clean is duplicated within a county. Pivoting might average/take first.", nrow(duplicates_check)), "WARN")
        # Decide on handling: average, take first, etc. pivot_wider defaults often take the first.
        # For safety, let's average before pivoting wide
        cgt_long <- cgt_long %>%
          group_by(county_geoid, industry_desc_clean) %>%
          summarize(
            rca = mean(rca, na.rm = TRUE),
            density = mean(density, na.rm = TRUE),
            .groups = "drop"
          )
        debug_log("Averaged RCA/Density for duplicated clean industry names within counties.", "INFO")
      }
      
      
      # Pivot RCA and Density separately, then join
      cgt_rca_wide <- cgt_long %>%
        select(county_geoid, industry_desc_clean, value = rca) %>%
        filter(!is.na(value)) %>% # Remove rows where RCA is NA before pivoting
        pivot_wider(
          id_cols = county_geoid,
          names_from = industry_desc_clean,
          values_from = value,
          names_prefix = "rca_",
          values_fill = NA_real_ # Explicitly fill non-present values with NA
        )
      
      cgt_density_wide <- cgt_long %>%
        select(county_geoid, industry_desc_clean, value = density) %>%
        filter(!is.na(value)) %>% # Remove rows where Density is NA
        pivot_wider(
          id_cols = county_geoid,
          names_from = industry_desc_clean,
          values_from = value,
          names_prefix = "density_",
          values_fill = NA_real_ # Explicitly fill non-present values with NA
        )
      
      debug_log("Pivoted CGT RCA and Density data to wide format separately.", "DEBUG")
      log_glimpse(cgt_rca_wide %>% select(1:min(5, ncol(.))), "Glimpse of cgt_rca_wide (first few cols)")
      log_glimpse(cgt_density_wide %>% select(1:min(5, ncol(.))), "Glimpse of cgt_density_wide (first few cols)")
      
      # Join the wide tables
      cgt_pivoted <- safe_left_join(cgt_rca_wide, cgt_density_wide,
                                    by = "county_geoid",
                                    y_name = "cgt_density_wide",
                                    join_details = "Join pivoted RCA and Density")
      
      # Sort columns alphabetically after county_geoid for consistency
      pivoted_cols_sorted <- sort(setdiff(names(cgt_pivoted), "county_geoid"))
      cgt_pivoted <- cgt_pivoted %>%
        select(county_geoid, all_of(pivoted_cols_sorted))
      
      debug_log("Joined pivoted CGT RCA and Density data.", "DEBUG")
      
    } else {
      debug_log("Missing one or more CGT columns for pivoting (industry_desc, industry_code, rca, density). Skipping pivot.", "WARN")
      # Create empty pivot table
      cgt_pivoted <- data.frame(county_geoid = character(0), stringsAsFactors = FALSE)
    }
    
    
    # Join Baseline and Pivoted Data
    # Need to handle cases where one or both might be empty
    if (nrow(cgt_baseline) > 0 && nrow(cgt_pivoted) > 0) {
      cgt_final_processed <- safe_left_join(cgt_baseline, cgt_pivoted,
                                            by = "county_geoid",
                                            y_name = "cgt_pivoted",
                                            join_details = "Join CGT baseline and pivoted industries")
    } else if (nrow(cgt_baseline) > 0) {
      cgt_final_processed <- cgt_baseline # Use only baseline if pivot failed/empty
      debug_log("Using only CGT baseline data as pivoted data is missing/empty.", "INFO")
    } else if (nrow(cgt_pivoted) > 0) {
      cgt_final_processed <- cgt_pivoted # Use only pivoted if baseline failed/empty
      debug_log("Using only CGT pivoted data as baseline data is missing/empty.", "INFO")
    } else {
      debug_log("Both CGT baseline and pivoted data are missing/empty.", "WARN")
      cgt_final_processed <- data.frame(county_geoid = character(0), stringsAsFactors = FALSE) # Empty dataframe
    }
    
    log_glimpse(cgt_final_processed %>% select(1:min(5,ncol(.))), "Glimpse of cgt_final_processed (baseline + pivoted, first few cols)")
    
    # Final merge into counties_cgt
    if (nrow(cgt_final_processed) > 0 && "county_geoid" %in% names(cgt_final_processed)) {
      counties_cgt <- safe_left_join(counties_cgt_join, # Start with the one having coverage flag
                                     cgt_final_processed,
                                     by = c("County GEOID" = "county_geoid"),
                                     y_name = "cgt_final_processed",
                                     join_details = "Add CGT baseline + pivot")
      debug_log("Final CGT data merged into counties_cgt.", "DEBUG")
    } else {
      counties_cgt <- counties_cgt_join # Keep the coverage flag version if no processed data
      debug_log("No processed CGT data to merge. Keeping coverage flag only.", "WARN")
      # Ensure baseline columns exist even if empty, to prevent errors later if expected
      if (!"county_economic_complexity_index" %in% names(counties_cgt)) counties_cgt$county_economic_complexity_index <- NA_real_
      if (!"county_complexity_outlook_index" %in% names(counties_cgt)) counties_cgt$county_complexity_outlook_index <- NA_real_
      if (!"diversity" %in% names(counties_cgt)) counties_cgt$diversity <- NA_real_
    }
  }
  
} else {
  debug_log("CGT County data not loaded or empty. Skipping CGT processing.", "WARN")
  # Add default columns to the initial placeholder
  counties_cgt$County_Data_Available_CGT <- FALSE
  counties_cgt$county_economic_complexity_index <- NA_real_
  counties_cgt$county_complexity_outlook_index <- NA_real_
  counties_cgt$diversity <- NA_real_
}

log_glimpse(counties_cgt, "Glimpse of counties_cgt (final for this section)")
debug_log("Completed Section 7: CGT Data Processing", "INFO")

#Section 8: HIGHWAYS, ENERGY COMMUNITIES, PROPERTY VALUES------------------------

###############################################################################
# 5) HIGHWAYS, ENERGY COMMUNITIES, PROPERTY VALUES
###############################################################################

section_header <- "\n=== SECTION 8: Highways, Energy Communities, Property Values ===" # Updated section number
cat(section_header)
flog.info(section_header)

#-----------------------------------------------------------------------------
# 8A) Highways Data
#-----------------------------------------------------------------------------
section_header_8a <- "\n=== SECTION 8A: Highways Data ===\n"
cat(section_header_8a)
flog.info(section_header_8a)
debug_log("Starting highways data creation", "DEBUG")

# Initialize placeholder
counties_highways <- unique_county_geoids %>% select(`County GEOID`, geometry)

if (exists("us_counties_major_roads") && !is.null(us_counties_major_roads) && nrow(us_counties_major_roads) > 0) {
  highways_raw <- us_counties_major_roads
  log_glimpse(highways_raw, "Glimpse of us_counties_major_roads (start of 8A)")
  
  # Check for GEOID column
  if (!"GEOID" %in% names(highways_raw)) {
    debug_log("Highways data missing 'GEOID' column. Skipping highway processing.", "ERROR")
    counties_highways$County_Has_Major_Road <- FALSE # Default value
  } else {
    highways_df <- highways_raw %>%
      mutate(`County GEOID` = sprintf("%05d", as.integer(GEOID))) %>% # Format GEOID
      filter(!is.na(`County GEOID`)) %>%
      # Assuming presence of a row for a county means it has a major road
      group_by(`County GEOID`) %>%
      summarize(County_Has_Major_Road_Calc = TRUE, .groups = "drop") # Create TRUE flag if county appears
    
    debug_log(sprintf("Highways presence dataframe created with %d unique counties.", nrow(highways_df)), "DEBUG")
    log_glimpse(highways_df, "Glimpse of highways_df")
    
    # Join back to the base county list
    counties_highways <- safe_left_join(
      unique_county_geoids %>% select(`County GEOID`, geometry), # Base geos
      highways_df,
      by = "County GEOID",
      y_name = "highways_df"
    ) %>%
      mutate(
        # Replace NA for counties not in highways_df with FALSE
        County_Has_Major_Road = replace_na(County_Has_Major_Road_Calc, FALSE)
      ) %>%
      select(-County_Has_Major_Road_Calc) # Remove intermediate column
    
    debug_log("Merged highways presence data into counties_highways.", "DEBUG")
  }
  
} else {
  debug_log("US Counties Major Roads data not loaded or empty. Skipping highway processing.", "WARN")
  counties_highways$County_Has_Major_Road <- FALSE # Default value
}

log_glimpse(counties_highways, "Glimpse of counties_highways (final for this section)")
debug_log("Finished highways data creation", "INFO")


#-----------------------------------------------------------------------------
# 8B) Energy Communities Data
#-----------------------------------------------------------------------------
section_header_8b <- "\n=== SECTION 8B: Energy Communities Data ===\n"
cat(section_header_8b)
flog.info(section_header_8b)
debug_log("Starting processing for Energy Communities", "DEBUG")

# Initialize placeholder
counties_energy_communities <- unique_county_geoids %>% select(`County GEOID`, geometry)

# Check if the base path exists
if (!exists("energy_comms_base_path") || !dir.exists(energy_comms_base_path)) {
  debug_log(sprintf("Energy Communities base path not found or not accessible: %s. Skipping Energy Community processing.", energy_comms_base_path), "WARN")
  # Add placeholder columns maybe? Or just skip. Let's skip for now.
} else {
  # Use the energy_comm_files list defined in Section 3
  
  process_energy_community_file <- function(ec_file_name, base_path, counties_sf) {
    ec_full_path <- file.path(base_path, ec_file_name)
    debug_log(sprintf("Processing energy community file: %s", ec_full_path), "INFO")
    
    # Create a safe column name from the filename
    col_nm_base <- gsub("\\.gpkg$", "", ec_file_name)
    col_nm_base <- gsub("[^A-Za-z0-9_]+", "_", col_nm_base) # Replace non-alphanumeric with underscore
    col_nm_base <- gsub("_+", "_", col_nm_base) # Collapse multiple underscores
    col_nm_base <- gsub("^_|_$", "", col_nm_base) # Trim leading/trailing underscores
    new_col_nm <- paste0("EC_", col_nm_base, "_Overlap") # Prefix with EC_
    
    # Default result in case of error
    default_result <- list(overlap = rep(FALSE, nrow(counties_sf)), column_name = new_col_nm)
    
    if(!file.exists(ec_full_path)) {
      debug_log(sprintf("Energy Community file not found: %s. Returning FALSE for all counties.", ec_full_path), "WARN")
      return(default_result)
    }
    
    # Read the spatial file safely
    ec_sf <- tryCatch({
      # Use the safe subprocess reader if needed, or direct st_read
      # Using direct st_read here for simplicity, assuming files are generally well-behaved
      sf_obj <- sf::st_read(ec_full_path, quiet = TRUE)
      # Validate and repair geometry if needed
      sf_obj <- sf::st_make_valid(sf_obj)
      # Remove empty geometries which can cause issues
      sf_obj <- sf_obj[!sf::st_is_empty(sf_obj), , drop = FALSE]
      sf_obj
    }, error = function(e) {
      debug_log(sprintf("Error reading or validating '%s': %s. Returning FALSE.", ec_file_name, e$message), "ERROR")
      NULL # Indicate failure
    })
    
    if (is.null(ec_sf) || nrow(ec_sf) == 0) {
      debug_log(sprintf("No valid features read from '%s'. Returning FALSE.", ec_file_name), "WARN")
      return(default_result)
    }
    
    log_glimpse(ec_sf, sprintf("Glimpse of ec_sf (%s)", ec_file_name))
    debug_log(sprintf("Read file '%s' with %d valid features.", ec_file_name, nrow(ec_sf)), "DEBUG")
    
    # Ensure CRS match - transform EC layer to match counties if necessary
    target_crs <- sf::st_crs(counties_sf)
    if (sf::st_crs(ec_sf) != target_crs) {
      debug_log(sprintf("Transforming CRS for '%s' to match counties (EPSG:%s).", ec_file_name, target_crs$epsg), "DEBUG")
      ec_sf <- tryCatch({
        sf::st_transform(ec_sf, target_crs)
      }, error = function(e) {
        debug_log(sprintf("Error transforming CRS for '%s': %s. Cannot calculate overlap.", ec_file_name, e$message), "ERROR")
        NULL # Indicate failure
      })
      if (is.null(ec_sf)) return(default_result)
    }
    
    # Calculate overlaps using st_intersects (more efficient for presence/absence)
    # Use tryCatch for the intersection calculation as well
    overlap_indices <- tryCatch({
      sf::st_intersects(counties_sf, ec_sf) # Returns a list of indices
    }, error = function(e) {
      debug_log(sprintf("Error during st_intersects for '%s': %s. Attempting centroid check.", ec_file_name, e$message), "WARN")
      # Fallback: check if county centroid is within any EC polygon
      county_centroids <- sf::st_centroid(counties_sf)
      tryCatch({
        sf::st_intersects(county_centroids, ec_sf)
      }, error = function(e2) {
        debug_log(sprintf("Centroid intersection also failed for '%s': %s. Returning FALSE.", ec_file_name, e2$message), "ERROR")
        NULL # Indicate complete failure
      })
    })
    
    if (is.null(overlap_indices)) {
      return(default_result) # Return default if intersection failed
    }
    
    # Convert the list of indices to a logical vector (TRUE if list element has length > 0)
    overlap_logical <- lengths(overlap_indices) > 0
    
    n_true <- sum(overlap_logical, na.rm=TRUE)
    debug_log(sprintf("Count of counties overlapped with '%s': %d", ec_file_name, n_true), "DEBUG")
    
    list(
      overlap = overlap_logical,
      column_name = new_col_nm
    )
  }
  
  # Process each energy community file
  ec_results_list <- list()
  if (exists("energy_comm_files") && length(energy_comm_files) > 0) {
    for(ec_file in energy_comm_files) {
      # Check if column already exists (e.g., from a previous run segment)
      temp_col_nm <- paste0("EC_", gsub("\\.gpkg$", "", gsub("[^A-Za-z0-9_]+", "_", gsub("_+", "_", gsub("^_|_$", "", ec_file)))), "_Overlap")
      if(temp_col_nm %in% names(counties_energy_communities)) {
        debug_log(sprintf("Column '%s' already exists, skipping file '%s'", temp_col_nm, ec_file), "INFO")
        next
      }
      
      result <- process_energy_community_file(ec_file, energy_comms_base_path, counties_energy_communities)
      # Store result using the calculated column name as the list key
      ec_results_list[[result$column_name]] <- result$overlap
      debug_log(sprintf("Processed '%s', added data for column '%s'.", ec_file, result$column_name), "INFO")
    }
  } else {
    debug_log("Variable 'energy_comm_files' not found or empty. No EC files to process.", "WARN")
  }
  
  # Add the results to the main dataframe
  if (length(ec_results_list) > 0) {
    for (col_name in names(ec_results_list)) {
      # Ensure the length matches the dataframe nrow before assigning
      if (length(ec_results_list[[col_name]]) == nrow(counties_energy_communities)) {
        counties_energy_communities[[col_name]] <- ec_results_list[[col_name]]
      } else {
        debug_log(sprintf("Length mismatch for column '%s'. Expected %d, got %d. Filling with FALSE.", col_name, nrow(counties_energy_communities), length(ec_results_list[[col_name]])), "WARN")
        counties_energy_communities[[col_name]] <- FALSE # Fallback
      }
    }
  }
  
  ec_cols_added <- names(ec_results_list)
  debug_log(sprintf("Added %d energy community overlap columns: %s",
                    length(ec_cols_added), paste(ec_cols_added, collapse=", ")), "INFO")
  
  # Final check for NAs in added columns (shouldn't happen with default FALSE)
  for(col in ec_cols_added) {
    if (col %in% names(counties_energy_communities)) {
      na_count <- sum(is.na(counties_energy_communities[[col]]))
      if(na_count > 0) {
        debug_log(sprintf("Fixing %d NA values in column '%s' (should not occur).", na_count, col), "WARN")
        counties_energy_communities[[col]][is.na(counties_energy_communities[[col]])] <- FALSE
      }
    }
  }
} # End check for energy_comms_base_path existence

log_glimpse(counties_energy_communities, "Glimpse of counties_energy_communities (after processing)")
debug_log("Finished Energy Communities processing step.", "INFO")


#-----------------------------------------------------------------------------
# 8C) Property Values
#-----------------------------------------------------------------------------
section_header_8c <- "\n=== SECTION 8C: Property Values ===\n"
cat(section_header_8c)
flog.info(section_header_8c)
debug_log("Starting property values data processing", "DEBUG")

# Initialize placeholder
counties_property_values <- unique_county_geoids %>% select(`County GEOID`, geometry)

if (exists("county_property_values") && !is.null(county_property_values) && nrow(county_property_values) > 0) {
  prop_values_raw <- county_property_values
  log_glimpse(prop_values_raw, "Glimpse of county_property_values (start of 8C)")
  
  # Check required columns
  prop_req_cols <- c("GEOID", "PropertyValueUSD") # Assuming these are the key columns
  if (!all(prop_req_cols %in% names(prop_values_raw))) {
    debug_log(sprintf("Property values data missing required columns (%s). Skipping property value processing.", paste(prop_req_cols[!prop_req_cols %in% names(prop_values_raw)], collapse=", ")), "ERROR")
    counties_property_values$PropertyValueUSD <- NA_real_ # Default value
  } else {
    prop_values_processed <- prop_values_raw %>%
      mutate(
        `County GEOID` = sprintf("%05d", as.integer(GEOID)), # Format GEOID
        PropertyValueUSD_num = suppressWarnings(as.numeric(PropertyValueUSD)) # Ensure numeric
      ) %>%
      filter(!is.na(`County GEOID`)) %>%
      select(`County GEOID`, PropertyValueUSD = PropertyValueUSD_num) %>% # Rename for clarity
      # Handle potential duplicates for a GEOID - take the first, last, mean, or error?
      # Taking the mean here if duplicates exist
      group_by(`County GEOID`) %>%
      summarize(PropertyValueUSD = mean(PropertyValueUSD, na.rm = TRUE), .groups = "drop")
    
    debug_log(sprintf("Processed property values data has %d unique counties.", nrow(prop_values_processed)), "DEBUG")
    log_glimpse(prop_values_processed, "Glimpse of prop_values_processed")
    
    # Join back to the base county list
    counties_property_values <- safe_left_join(
      unique_county_geoids %>% select(`County GEOID`, geometry), # Base geos
      prop_values_processed,
      by = "County GEOID",
      y_name = "prop_values_processed"
    )
    # NAs will remain for counties not in the property value file, which is expected.
    debug_log("Merged property values data into counties_property_values.", "DEBUG")
  }
  
} else {
  debug_log("County property values data not loaded or empty. Skipping property value processing.", "WARN")
  counties_property_values$PropertyValueUSD <- NA_real_ # Default value
}

log_glimpse(counties_property_values, "Glimpse of counties_property_values (final for this section)")
debug_log("Finished property values data processing", "INFO")

debug_log("Completed Section 8: Highways, Energy Communities, Property Values", "INFO")

#Section 9: STATE-LEVEL DATA PROCESSING (EIA, Corporate Tax)------------------------

###############################################################################
# 6) STATE-LEVEL DATA (EIA, Corporate Tax)
###############################################################################
section_header <- "\n=== SECTION 9: State-Level Data Processing (EIA, Corporate Tax) ===" # Updated section number
cat(section_header)
flog.info(section_header)
debug_log("Starting Section 9: state-level data processing and preparation", "DEBUG")

# Initialize placeholders for results
counties_eia <- unique_county_geoids %>% st_drop_geometry() %>% select(`County GEOID`) %>% mutate(Industrial_Electricity_Price_2025 = NA_real_)
counties_corp_taxes <- unique_county_geoids %>% st_drop_geometry() %>% select(`County GEOID`) %>% mutate(state_corporate_tax_rate = NA_real_)

#-----------------------------------------------------------------------------
# 9A) Process EIA Industrial Electricity Price Data
#-----------------------------------------------------------------------------
section_header_9a <- "\n=== SECTION 9A: Process EIA Data ===\n"
cat(section_header_9a)
flog.info(section_header_9a)

eia_state_processed <- NULL # Initialize
if (exists("eia_sales") && !is.null(eia_sales) && nrow(eia_sales) > 0) {
  log_glimpse(eia_sales, "Glimpse of raw eia_sales data (start of 9A)")
  
  # Identify correct columns - names might change over time
  # Original script used 'Year' and 'Thousand Dollars...5' for Industrial Sales
  # Let's try to find them more robustly
  year_col <- names(eia_sales)[tolower(names(eia_sales)) == "year"]
  state_col <- names(eia_sales)[tolower(names(eia_sales)) == "state"]
  # Price column is tricky. Original 'Thousand Dollars...5' implies 5th column with that pattern?
  # Let's look for columns containing "industrial" and "cents/kWh" or similar for price, or revenue/sales for calculation.
  # The original 'Thousand Dollars...5' might relate to Revenue. Let's assume it's Industrial Revenue for now.
  # We need Sales too if calculating price = Revenue / Sales.
  # Looking at EIA 861M documentation suggests direct price columns exist (e.g., "Industrial Price (cents/kWh)")
  # Let's search for potential price columns.
  price_col_candidates <- names(eia_sales)[grepl("industrial", names(eia_sales), ignore.case = TRUE) &
                                             (grepl("price", names(eia_sales), ignore.case = TRUE) |
                                                grepl("cents", names(eia_sales), ignore.case = TRUE))]
  
  # Or try the column index approach if names are unstable (less robust)
  # price_col_index_guess <- 5 # Based on original script's '...5'
  
  # Selecting the price column requires inspecting the actual loaded 'eia_sales' file.
  # Assuming a column named like 'Industrial Price (cents/kWh)' exists:
  price_col <- price_col_candidates[1] # Take the first match for now
  # If original script's "...5" truly meant revenue, that needs different handling.
  # Let's assume for now we found a direct price column.
  
  target_year_eia <- 2025 # As per original script context (may need update based on data)
  
  if (length(year_col) == 1 && length(state_col) == 1 && length(price_col) == 1) {
    debug_log(sprintf("Using EIA columns: Year='%s', State='%s', Price='%s'", year_col, state_col, price_col), "INFO")
    
    eia_state_processed <- eia_sales %>%
      select(
        Year = all_of(year_col),
        State = all_of(state_col),
        Price_raw = all_of(price_col)
      ) %>%
      filter(Year == target_year_eia) %>%
      mutate(
        # Assuming price is in cents/kWh, convert to $/kWh? Or keep as cents?
        # Original script used 'Thousand Dollars...5' - if that was revenue, this is wrong.
        # Assuming Price_raw is cents/kWh here. Let's keep it as cents/kWh for now.
        Industrial_Electricity_Price_Cents_kWh_2025 = suppressWarnings(as.numeric(Price_raw)),
        State = trimws(State) # Clean state abbreviation
      ) %>%
      # Handle cases where a state might appear multiple times (e.g., regions) - average? take first?
      group_by(State) %>%
      summarize(Industrial_Electricity_Price_Cents_kWh_2025 = mean(Industrial_Electricity_Price_Cents_kWh_2025, na.rm = TRUE), .groups = "drop") %>%
      filter(!is.na(State) & State != "" & !is.na(Industrial_Electricity_Price_Cents_kWh_2025)) %>%
      select(State, Industrial_Electricity_Price_Cents_kWh_2025) # Final columns
    
    debug_log(sprintf("Processed EIA state electricity price data for %d states for year %d.", nrow(eia_state_processed), target_year_eia), "DEBUG")
    log_glimpse(eia_state_processed, "Glimpse of processed eia_state data")
    
  } else {
    debug_log(sprintf("Could not find required columns in EIA data (Year: %s, State: %s, Price: %s found?). Skipping EIA processing.",
                      length(year_col)==1, length(state_col)==1, length(price_col)==1), "WARN")
  }
  
} else {
  debug_log("EIA sales data not loaded or empty. Skipping EIA processing.", "WARN")
}


#-----------------------------------------------------------------------------
# 9B) Process State Corporate Tax Data
#-----------------------------------------------------------------------------
section_header_9b <- "\n=== SECTION 9B: Process State Corporate Tax Data ===\n"
cat(section_header_9b)
flog.info(section_header_9b)

state_corp_processed <- NULL # Initialize
if (exists("state_corporate_taxes") && !is.null(state_corporate_taxes) && nrow(state_corporate_taxes) > 0) {
  log_glimpse(state_corporate_taxes, "Glimpse of raw state_corporate_taxes data (start of 9B)")
  
  # Check for required columns
  tax_req_cols <- c("State", "Rates")
  if (!all(tax_req_cols %in% names(state_corporate_taxes))) {
    debug_log(sprintf("State corporate tax data missing required columns (%s). Skipping tax processing.", paste(tax_req_cols[!tax_req_cols %in% names(state_corporate_taxes)], collapse=", ")), "ERROR")
  } else {
    # States with no corporate income tax (source: Tax Foundation or similar, based on file context '2025')
    # Note: Ohio has CAT, Texas has Margin Tax, WA has B&O - these might be considered '0' income tax depending on definition.
    # Nevada, South Dakota, Wyoming have no state corp income tax or gross receipts tax.
    # Using list from original script for consistency. Verify this list if source changes.
    no_corporate_income_tax_states <- c("Nevada", "Ohio", "South Dakota", "Texas", "Washington", "Wyoming")
    
    state_corp_processed <- state_corporate_taxes %>%
      mutate(
        State = trimws(State),
        # Clean the 'Rates' column - remove %, handle non-numeric entries, ranges etc.
        Rates_cleaned = str_remove(Rates, "%"),
        # Take the highest value if it's a range like "X% - Y%"
        Rates_cleaned = sapply(Rates_cleaned, function(x) {
          if (grepl("-", x)) {
            parts <- str_split(x, "-", simplify = TRUE)
            # Take the maximum rate in the bracket/range
            max(suppressWarnings(as.numeric(trimws(parts))), na.rm = TRUE)
          } else {
            suppressWarnings(as.numeric(trimws(x)))
          }
        }),
        # Convert to numeric percentage (e.g., 5.0 for 5%)
        Rates_numeric = suppressWarnings(as.numeric(Rates_cleaned))
      ) %>%
      filter(!is.na(State) & State != "") %>%
      # Group by state and take the MAX rate listed (handles bracketed systems simply)
      group_by(State) %>%
      summarize(
        max_rate = if(all(is.na(Rates_numeric))) NA_real_ else max(Rates_numeric, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      # Apply the zero rate for states with no *income* tax
      mutate(
        state_corporate_tax_rate_pct_2025 = case_when(
          State %in% no_corporate_income_tax_states ~ 0.0,
          is.infinite(max_rate) ~ NA_real_, # Handle Inf if max returned it
          TRUE ~ max_rate
        ),
        # Handle states potentially missing from the file (assign NA)
        state_corporate_tax_rate_pct_2025 = ifelse(is.na(state_corporate_tax_rate_pct_2025), NA_real_, state_corporate_tax_rate_pct_2025)
      ) %>%
      select(State, state_corporate_tax_rate_pct_2025) # Final columns
    
    debug_log(sprintf("Processed state corporate tax data for %d states.", nrow(state_corp_processed)), "DEBUG")
    log_glimpse(state_corp_processed, "Glimpse of processed state_corp_processed data")
    
    # Check for states potentially missed
    all_us_states <- unique(unique_county_geoids$State_Name_Latest) # Get unique states from our county list
    missing_tax_states <- setdiff(all_us_states, state_corp_processed$State)
    if (length(missing_tax_states) > 0) {
      debug_log(sprintf("Warning: The following %d states from unique_county_geoids are missing from the corporate tax file: %s", length(missing_tax_states), paste(missing_tax_states, collapse=", ")), "WARN")
    }
  }
  
} else {
  debug_log("State corporate taxes data not loaded or empty. Skipping tax processing.", "WARN")
}


#-----------------------------------------------------------------------------
# 9C) Prepare State Info and Join State-Level Data
#-----------------------------------------------------------------------------
section_header_9c <- "\n=== SECTION 9C: Join State-Level Data to Counties ===\n"
cat(section_header_9c)
flog.info(section_header_9c)

# Create state info dataframe for joining (using most recent state name/abbr)
state_info <- unique_county_geoids %>%
  st_drop_geometry() %>%
  select(`County GEOID`, State_Name_Latest, State_Abbreviation_Latest = State_Abbreviation) %>% # Assuming State_Abbreviation is the most recent
  distinct() # Ensure one row per County GEOID

# Correct state abbreviations if needed (e.g., ensure uppercase)
state_info <- state_info %>% mutate(State_Abbreviation_Latest = toupper(State_Abbreviation_Latest))

debug_log(sprintf("State info prepared for joining with %d unique counties.", nrow(state_info)), "DEBUG")
log_glimpse(state_info, "Glimpse of state_info (for joining state-level data)")


# Join EIA data using state abbreviations (ensure case and format match)
if (!is.null(eia_state_processed)) {
  eia_state_processed <- eia_state_processed %>% mutate(State = toupper(trimws(State))) # Match formatting
  
  counties_eia_joined <- safe_left_join(
    state_info %>% select(`County GEOID`, State_Abbreviation_Latest), # Only need GEOID and Key
    eia_state_processed,
    by = c("State_Abbreviation_Latest" = "State"),
    y_name = "eia_state_processed"
  ) %>%
    select(`County GEOID`, Industrial_Electricity_Price_Cents_kWh_2025) # Keep GEOID and value
  
  # Overwrite placeholder
  counties_eia <- counties_eia_joined
  debug_log("Joined EIA state data to county list.", "DEBUG")
  log_glimpse(counties_eia, "Glimpse of counties_eia (joined)")
  
  na_eia_count <- sum(is.na(counties_eia$Industrial_Electricity_Price_Cents_kWh_2025))
  if (na_eia_count > 0) {
    debug_log(sprintf("%d counties have NA for EIA electricity price after join.", na_eia_count), "INFO")
  }
  
} else {
  debug_log("Skipping EIA join as processed EIA data is NULL.", "WARN")
}


# Join corporate tax data using state names (ensure case and format match)
if (!is.null(state_corp_processed)) {
  state_corp_processed <- state_corp_processed %>% mutate(State = trimws(State)) # Match formatting
  
  counties_corp_taxes_joined <- safe_left_join(
    state_info %>% select(`County GEOID`, State_Name_Latest), # Only need GEOID and Key
    state_corp_processed,
    by = c("State_Name_Latest" = "State"),
    y_name = "state_corp_processed"
  ) %>%
    select(`County GEOID`, state_corporate_tax_rate_pct_2025) # Keep GEOID and value
  
  # Overwrite placeholder
  counties_corp_taxes <- counties_corp_taxes_joined
  debug_log("Joined corporate tax data to county list.", "DEBUG")
  log_glimpse(counties_corp_taxes, "Glimpse of counties_corp_taxes (joined)")
  
  na_tax_count <- sum(is.na(counties_corp_taxes$state_corporate_tax_rate_pct_2025))
  if (na_tax_count > 0) {
    debug_log(sprintf("%d counties have NA for corporate tax rate after join.", na_tax_count), "INFO")
  }
  
} else {
  debug_log("Skipping corporate tax join as processed tax data is NULL.", "WARN")
}


# Final check of the resulting dataframes
log_glimpse(counties_eia, "Final glimpse of counties_eia")
log_glimpse(counties_corp_taxes, "Final glimpse of counties_corp_taxes")
debug_log("Completed Section 9: State-Level Data Processing", "INFO")

#Section 10: XCHANGE STATE CLIMATE POLICY DATA PROCESSING------------------------

###############################################################################
# 7) XCHANGE STATE CLIMATE POLICY APPROACH (WITH LOGICAL CONVERSION)
###############################################################################

section_header <- "\n=== SECTION 10: XChange State Climate Policy Data Processing ===" # Updated section number
cat(section_header)
flog.info(section_header)
debug_log("Starting Section 10: XChange dashboard data processing with logical conversion", "DEBUG")

# Initialize placeholder
xchange_by_county <- unique_county_geoids %>% select(`County GEOID`, geometry, `State Name` = State_Name_Latest) # Need State Name for join

# Check if file exists (based on check in Section 5)
if (exists("xchange_dashboard_file") && !is.null(xchange_dashboard_file) && file.exists(xchange_dashboard_file)) {
  
  xlsx_path <- xchange_dashboard_file
  debug_log(sprintf("Processing XChange file: %s", xlsx_path), "INFO")
  
  # Load data dictionary (optional, for context)
  readme_sheet_name <- "README"
  available_sheets_readme <- tryCatch(excel_sheets(path = xlsx_path), error = function(e) NULL)
  if (!is.null(available_sheets_readme) && readme_sheet_name %in% available_sheets_readme) {
    data_dictionary <- tryCatch({
      read_excel(path = xlsx_path, sheet = readme_sheet_name, skip = 4) %>%
        fill(`Policy Area`, .direction = "down") %>% # Assuming column names haven't changed
        fill(`Policy Category`, .direction = "down")
    }, error = function(e) {
      debug_log(sprintf("Error reading README sheet from XChange file: %s", e$message), "WARN")
      NULL
    })
    if (!is.null(data_dictionary)) {
      debug_log("Data dictionary loaded from XChange dashboard file.", "DEBUG")
      # log_glimpse(data_dictionary, "Glimpse of XChange Data Dictionary")
    }
  } else {
    debug_log("README sheet not found in XChange file.", "INFO")
  }
  
  
  # Load policy sheets safely
  all_sheets <- available_sheets_readme # Use sheets already fetched
  debug_log(sprintf("All sheets found in xchange_dashboard_file: %s", paste(all_sheets, collapse=", ")), "DEBUG")
  
  load_xchange_sheet <- function(sheet_name, path) {
    if (sheet_name %in% all_sheets) {
      debug_log(sprintf("Reading XChange sheet: '%s'", sheet_name), "DEBUG")
      tryCatch({
        read_excel(path, sheet = sheet_name)
      }, error = function(e) {
        debug_log(sprintf("Error reading sheet '%s' from %s: %s", sheet_name, path, e$message), "ERROR")
        NULL # Return NULL on error
      })
    } else {
      debug_log(sprintf("Sheet '%s' not found in XChange file.", sheet_name), "WARN")
      NULL # Return NULL if sheet doesn't exist
    }
  }
  
  # Load core sheets
  cross_sector <- load_xchange_sheet("Cross-Sector", xlsx_path)
  electricity <- load_xchange_sheet("Electricity", xlsx_path)
  buildings_efficiency <- load_xchange_sheet("Buildings and Efficiency", xlsx_path)
  transportation <- load_xchange_sheet("Transportation", xlsx_path)
  natural_working_lands <- load_xchange_sheet("Natural and Working Lands", xlsx_path)
  
  # Find and load industry sheet dynamically
  imw_pattern <- "^Industry.*Materials.*Waste" # More flexible pattern
  imw_match <- grep(imw_pattern, all_sheets, value = TRUE, ignore.case = TRUE)
  industry_materials_waste <- NULL
  if(length(imw_match) == 1) {
    industry_materials_waste <- load_xchange_sheet(imw_match[1], xlsx_path)
    if (!is.null(industry_materials_waste)) {
      debug_log(sprintf("Loaded Industry/Materials/Waste sheet: '%s'", imw_match[1]), "INFO")
    }
  } else if (length(imw_match) > 1) {
    debug_log(sprintf("Multiple sheets match pattern '%s': %s. Cannot determine correct Industry sheet.", imw_pattern, paste(imw_match, collapse=", ")), "WARN")
  } else {
    debug_log(sprintf("No sheet found matching pattern '%s'.", imw_pattern), "WARN")
  }
  
  # Glimpse loaded sheets (if they loaded correctly)
  if(!is.null(cross_sector)) log_glimpse(cross_sector, "Glimpse of cross_sector")
  if(!is.null(electricity)) log_glimpse(electricity, "Glimpse of electricity")
  if(!is.null(buildings_efficiency)) log_glimpse(buildings_efficiency, "Glimpse of buildings_efficiency")
  if(!is.null(transportation)) log_glimpse(transportation, "Glimpse of transportation")
  if(!is.null(natural_working_lands)) log_glimpse(natural_working_lands, "Glimpse of natural_working_lands")
  if(!is.null(industry_materials_waste)) log_glimpse(industry_materials_waste, "Glimpse of industry_materials_waste")
  
  
  # --- Optional Coverage Checks (can be commented out for speed) ---
  # check_coverage <- function(df, sheet_name) {
  #   if(is.null(df) || nrow(df) == 0) {
  #        flog.info("Skipping coverage check for '%s' as it's empty or NULL.", sheet_name)
  #        return()
  #   }
  #   # ... (rest of the check_coverage function from original script) ...
  # }
  # check_coverage(cross_sector, "Cross-Sector")
  # check_coverage(electricity, "Electricity")
  # check_coverage(buildings_efficiency, "Buildings and Efficiency")
  # check_coverage(transportation, "Transportation")
  # check_coverage(natural_working_lands, "Natural and Working Lands")
  # check_coverage(industry_materials_waste, imw_match[1]) # Use matched name
  # debug_log("Completed coverage checks for loaded XChange policy sheets.", "DEBUG")
  # --- End Optional Coverage Checks ---
  
  
  # Combine sheets into long format
  make_policy_df <- function(df, sheet_label) {
    if(is.null(df) || nrow(df) == 0) return(NULL) # Skip if sheet failed to load
    
    # Ensure required columns 'State', 'Policy', 'Policy Status' exist
    req_cols_policy <- c("State", "Policy", "Policy Status")
    if (!all(req_cols_policy %in% names(df))) {
      debug_log(sprintf("Sheet '%s' is missing required columns (%s). Skipping.", sheet_label, paste(req_cols_policy[!req_cols_policy %in% names(df)], collapse=", ")), "WARN")
      return(NULL)
    }
    
    # Handle 'Year Enacted' column existence and type
    if(!("Year Enacted" %in% names(df))) {
      df$`Year Enacted` <- NA_character_
    } else {
      # Ensure it's character, handle potential non-numeric values gracefully
      df$`Year Enacted` <- as.character(df$`Year Enacted`)
      # Optional: Clean 'Year Enacted' further if needed (e.g., extract only 4-digit years)
    }
    
    df %>%
      mutate(
        `Policy Category` = sheet_label,
        State = trimws(State), # Clean State names
        Policy = trimws(Policy), # Clean Policy names
        `Policy Status` = tolower(trimws(`Policy Status`)) # Standardize status to lower case
      ) %>%
      select(State, `Policy Category`, Policy, `Policy Status`, `Year Enacted`) %>%
      filter(!is.na(State) & State != "" & !is.na(Policy) & Policy != "") # Ensure key identifiers are present
  }
  
  # Create list of loaded dataframes and their labels
  policy_dfs_list <- list(
    "Cross-Sector" = cross_sector,
    "Electricity" = electricity,
    "Buildings and Efficiency" = buildings_efficiency,
    "Transportation" = transportation,
    "Natural and Working Lands" = natural_working_lands,
    "Industry, Materials, and Waste" = industry_materials_waste # Use generic label
  )
  
  # Apply function and bind rows
  xchange_state_climate_policy_data <- bind_rows(
    lapply(names(policy_dfs_list), function(label) make_policy_df(policy_dfs_list[[label]], label))
  )
  
  if (nrow(xchange_state_climate_policy_data) > 0) {
    debug_log(sprintf("Combined XChange policy data has %d rows.", nrow(xchange_state_climate_policy_data)), "DEBUG")
    log_glimpse(xchange_state_climate_policy_data, "Glimpse of combined xchange_state_climate_policy_data")
    
    # Check unique policy statuses found after cleaning
    unique_policy_statuses <- unique(xchange_state_climate_policy_data$`Policy Status`)
    status_message <- "Unique standardized 'Policy Status' values across all sheets:"
    cat("\n", status_message, "\n")
    output <- capture.output(print(sort(unique_policy_statuses)))
    flog.info("%s\n%s", status_message, paste(output, collapse="\n"))
    print(sort(unique_policy_statuses))
    
    # --- Create Pivoted Versions ---
    # Strict: Only 'enacted' counts as TRUE
    strict_statuses <- c("enacted")
    xchange_state_climate_policy_data_strict <- xchange_state_climate_policy_data %>%
      mutate(
        logical_value = `Policy Status` %in% strict_statuses
      )
    
    # Inclusive: 'enacted', 'partially-enacted', 'in-progress' count as TRUE
    # Update this list based on observed unique_policy_statuses if needed
    inclusive_statuses <- c("enacted", "partially enacted", "partially-enacted", "in progress", "in-progress")
    xchange_state_climate_policy_data_inclusive <- xchange_state_climate_policy_data %>%
      mutate(
        logical_value = `Policy Status` %in% inclusive_statuses
      )
    debug_log("Created strict and inclusive logical interpretations of policy status.", "DEBUG")
    
    # Pivot each version - handle potential duplicate Policy names within a State
    pivot_xchange <- function(policy_df, value_col, id_cols = "State", names_from_col = "Policy") {
      # Check for duplicates before pivoting
      dups_check <- policy_df %>%
        group_by(!!sym(id_cols), !!sym(names_from_col)) %>%
        filter(n() > 1)
      if (nrow(dups_check) > 0) {
        debug_log(sprintf("Found %d duplicates for (%s, %s) combination. Taking the first value for pivoting.", nrow(dups_check), id_cols, names_from_col), "WARN")
        # Option: Summarize before pivoting if needed (e.g., for logical, TRUE wins?)
        # policy_df <- policy_df %>% group_by(!!sym(id_cols), !!sym(names_from_col)) %>% summarize(!!sym(value_col) := first(!!sym(value_col)), .groups="drop") # Or max for logical?
        policy_df <- policy_df %>% distinct(!!sym(id_cols), !!sym(names_from_col), .keep_all = TRUE) # Keep first occurrence
      }
      
      policy_df %>%
        select(all_of(c(id_cols, names_from_col, value_col))) %>%
        pivot_wider(
          id_cols = all_of(id_cols),
          names_from = all_of(names_from_col),
          values_from = all_of(value_col),
          # Fill missing state/policy combos. Logical should be FALSE, Character should be "not found" or NA
          values_fill = if(is.logical(policy_df[[value_col]])) FALSE else if(is.character(policy_df[[value_col]])) NA_character_ else NA
        )
    }
    
    pivoted_xchange_data_strict <- pivot_xchange(xchange_state_climate_policy_data_strict, "logical_value")
    pivoted_xchange_data_inclusive <- pivot_xchange(xchange_state_climate_policy_data_inclusive, "logical_value")
    pivoted_xchange_data_original <- pivot_xchange(xchange_state_climate_policy_data, "Policy Status") # Use original status strings
    
    debug_log("Pivoted XChange policy data for strict, inclusive, and original versions.", "DEBUG")
    
    # Rename columns with prefixes - use make.names for safety
    rename_pivot_cols <- function(df, prefix) {
      old_names <- names(df)
      new_names <- sapply(old_names, function(nm) {
        if (nm == "State") {
          return("State")
        } else {
          safe_policy_name <- make.names(nm) # Make policy name safe
          return(paste0(prefix, safe_policy_name))
        }
      })
      setNames(df, new_names)
    }
    
    pivoted_xchange_data_strict <- rename_pivot_cols(pivoted_xchange_data_strict, "XChange_Strict_")
    pivoted_xchange_data_inclusive <- rename_pivot_cols(pivoted_xchange_data_inclusive, "XChange_Inclusive_")
    pivoted_xchange_data_original <- rename_pivot_cols(pivoted_xchange_data_original, "XChange_Original_Status_")
    
    log_glimpse(pivoted_xchange_data_strict %>% select(1:min(5, ncol(.))), "Glimpse Strict Pivot (First 5)")
    log_glimpse(pivoted_xchange_data_inclusive %>% select(1:min(5, ncol(.))), "Glimpse Inclusive Pivot (First 5)")
    log_glimpse(pivoted_xchange_data_original %>% select(1:min(5, ncol(.))), "Glimpse Original Status Pivot (First 5)")
    
    # Prepare county-to-state mapping (already done in initialization of xchange_by_county)
    xchange_county_state_map <- unique_county_geoids %>%
      st_drop_geometry() %>%
      select(`County GEOID`, `State Name` = State_Name_Latest) %>%
      distinct()
    debug_log(sprintf("Prepared county-to-state map with %d rows.", nrow(xchange_county_state_map)), "DEBUG")
    
    # Join pivoted data to county map
    xchange_by_county_joined <- xchange_county_state_map %>%
      safe_left_join(pivoted_xchange_data_strict, by = c("State Name" = "State"), y_name = "strict_pivot") %>%
      safe_left_join(pivoted_xchange_data_inclusive, by = c("State Name" = "State"), y_name = "inclusive_pivot") %>%
      safe_left_join(pivoted_xchange_data_original, by = c("State Name" = "State"), y_name = "original_pivot")
    
    debug_log("Merged all versions of pivoted XChange policy data into county-level data.", "DEBUG")
    log_glimpse(xchange_by_county_joined, "Glimpse of xchange_by_county_joined (after joining all policy data versions)")
    
    # Re-attach geometry using County GEOID
    xchange_by_county <- safe_left_join(
      unique_county_geoids %>% select(`County GEOID`, geometry), # Base sf object
      xchange_by_county_joined,
      by = "County GEOID",
      y_name = "xchange_by_county_joined_data"
    ) %>% distinct(`County GEOID`, .keep_all = TRUE) # Ensure unique counties
    
    debug_log("Re-attached geometry to xchange_by_county.", "DEBUG")
    
    # Final Summary Log
    strict_cols <- grep("^XChange_Strict_", names(xchange_by_county), value = TRUE)
    inclusive_cols <- grep("^XChange_Inclusive_", names(xchange_by_county), value = TRUE)
    original_cols <- grep("^XChange_Original_Status_", names(xchange_by_county), value = TRUE)
    message_summ <- "\nXChange Climate Policy Data Summary in final output:"
    cat(message_summ, "\n")
    flog.info(message_summ)
    col_summary_orig <- sprintf("- %d original string value columns (%s*)", length(original_cols), "XChange_Original_Status_")
    cat(col_summary_orig, "\n"); flog.info(col_summary_orig)
    col_summary_strict <- sprintf("- %d strict logical columns (%s*)", length(strict_cols), "XChange_Strict_")
    cat(col_summary_strict, "\n"); flog.info(col_summary_strict)
    col_summary_incl <- sprintf("- %d inclusive logical columns (%s*)", length(inclusive_cols), "XChange_Inclusive_")
    cat(col_summary_incl, "\n"); flog.info(col_summary_incl)
    
  } else {
    debug_log("No data combined from XChange policy sheets. Skipping pivot and merge.", "WARN")
    # xchange_by_county remains the initialized placeholder
  }
  
} else {
  debug_log("XChange dashboard file not found or not specified. Skipping XChange processing.", "WARN")
  # xchange_by_county remains the initialized placeholder
}

log_glimpse(xchange_by_county, "Glimpse of xchange_by_county (final for this section)")
debug_log("Completed Section 10: XChange dashboard data processing", "INFO")

#Section 11: MERGE ALL PROCESSED DATASETS------------------------

###############################################################################
# 8) MERGE EVERYTHING INTO final_big_merged
###############################################################################

section_header <- "\n=== SECTION 11: Merge All Processed Datasets ===" # Updated section number
cat(section_header)
flog.info(section_header)
debug_log("Starting Section 11: Final merge of all county-level data", "DEBUG")

# Create a list of dataframes to merge. Check if they exist first.
big_tables_list <- list()

if (exists("counties_wellesley") && !is.null(counties_wellesley)) big_tables_list$wellesley <- counties_wellesley else debug_log("counties_wellesley not found for merge.", "WARN")
if (exists("counties_mesc") && !is.null(counties_mesc)) big_tables_list$mesc <- counties_mesc else debug_log("counties_mesc not found for merge.", "WARN")
# county_chips_award_data is not spatial, needs County GEOID
if (exists("county_chips_award_data") && !is.null(county_chips_award_data)) big_tables_list$chips <- county_chips_award_data else debug_log("county_chips_award_data not found for merge.", "WARN")
if (exists("counties_cim_final") && !is.null(counties_cim_final)) big_tables_list$cim <- counties_cim_final else debug_log("counties_cim_final not found for merge.", "WARN")
if (exists("counties_cgt") && !is.null(counties_cgt)) big_tables_list$cgt <- counties_cgt else debug_log("counties_cgt not found for merge.", "WARN")
if (exists("counties_highways") && !is.null(counties_highways)) big_tables_list$highways <- counties_highways else debug_log("counties_highways not found for merge.", "WARN")
if (exists("counties_energy_communities") && !is.null(counties_energy_communities)) big_tables_list$energy_comm <- counties_energy_communities else debug_log("counties_energy_communities not found for merge.", "WARN")
if (exists("counties_property_values") && !is.null(counties_property_values)) big_tables_list$prop_values <- counties_property_values else debug_log("counties_property_values not found for merge.", "WARN")
# Select only County GEOID and the specific variable from state-level files
if (exists("counties_eia") && !is.null(counties_eia) && "Industrial_Electricity_Price_Cents_kWh_2025" %in% names(counties_eia)) {
  big_tables_list$eia <- counties_eia %>% select(`County GEOID`, Industrial_Electricity_Price_Cents_kWh_2025)
} else debug_log("counties_eia or its price column not found for merge.", "WARN")
if (exists("counties_corp_taxes") && !is.null(counties_corp_taxes) && "state_corporate_tax_rate_pct_2025" %in% names(counties_corp_taxes)) {
  big_tables_list$corp_tax <- counties_corp_taxes %>% select(`County GEOID`, state_corporate_tax_rate_pct_2025)
} else debug_log("counties_corp_taxes or its rate column not found for merge.", "WARN")
if (exists("xchange_by_county") && !is.null(xchange_by_county)) big_tables_list$xchange <- xchange_by_county else debug_log("xchange_by_county not found for merge.", "WARN")

# Add EDCI and NERDE if they exist and have GEOID (assuming processing adds GEOID)
# Note: Original script didn't explicitly merge NERDE/EDCI here, they might be used differently.
# Merging them here requires preprocessing them to have a 'County GEOID' column.
# Example placeholder merge (requires preprocessing NERDE/EDCI)
# if (exists("EDCI_processed") && !is.null(EDCI_processed)) big_tables_list$edci <- EDCI_processed else debug_log("EDCI_processed not found for merge.", "WARN")
# if (exists("NERDE_processed") && !is.null(NERDE_processed)) big_tables_list$nerde <- NERDE_processed else debug_log("NERDE_processed not found for merge.", "WARN")


# Initialize the final dataframe with the unique county geometries and attributes
final_big_merged <- unique_county_geoids
debug_log(sprintf("Initialized final_big_merged with %d rows and %d columns.", nrow(final_big_merged), ncol(final_big_merged)), "DEBUG")


# Helper function to safely join, adding only new columns
# Handles both sf and non-sf dataframes in 'new_df_obj'
safe_join_new_columns <- function(main_sf, new_df_obj, join_key = "County GEOID", table_name = "unknown") {
  debug_log(sprintf("Attempting to merge table '%s' onto main_sf by '%s'", table_name, join_key), "DEBUG")
  
  # Ensure the new object is a dataframe and drop geometry if it's sf
  if (inherits(new_df_obj, "sf")) {
    new_df <- st_drop_geometry(new_df_obj)
  } else if (is.data.frame(new_df_obj)) {
    new_df <- new_df_obj
  } else {
    debug_log(sprintf("Table '%s' is not a dataframe or sf object. Skipping merge.", table_name), "WARN")
    return(main_sf) # Return original sf object
  }
  
  # Check if join key exists in the new dataframe
  if (!join_key %in% names(new_df)) {
    debug_log(sprintf("Join key '%s' not found in table '%s'. Skipping merge.", join_key, table_name), "WARN")
    return(main_sf)
  }
  
  # Identify columns to merge (key + columns not already in main_sf)
  cols_in_main <- names(main_sf)
  cols_to_add <- setdiff(names(new_df), cols_in_main)
  
  if (length(cols_to_add) == 0) {
    debug_log(sprintf("No new columns found in table '%s' to merge.", table_name), "INFO")
    return(main_sf) # Return original sf object
  }
  
  debug_log(sprintf("Merging %d new columns from '%s': %s",
                    length(cols_to_add), table_name, paste(cols_to_add, collapse=", ")), "DEBUG")
  
  # Select only the key and new columns for the join
  new_df_subset <- new_df %>% select(all_of(join_key), all_of(cols_to_add))
  
  # Perform the safe left join
  main_sf_updated <- safe_left_join(
    main_sf,
    new_df_subset,
    by = join_key,
    y_name = table_name,
    join_details = paste("Add new columns from", table_name)
  )
  
  return(main_sf_updated)
}


# Iteratively merge each table in the list
if (length(big_tables_list) > 0) {
  for(i in seq_along(big_tables_list)) {
    table_name <- names(big_tables_list)[i]
    current_table <- big_tables_list[[i]]
    
    if (is.null(current_table) || nrow(current_table) == 0) {
      debug_log(sprintf("Table '%s' is NULL or empty. Skipping merge.", table_name), "INFO")
      next # Skip to the next table
    }
    
    final_big_merged <- safe_join_new_columns(
      main_sf = final_big_merged,
      new_df_obj = current_table,
      join_key = "County GEOID",
      table_name = table_name
    )
    debug_log(sprintf("Merged table '%s'. Current dimensions: %d rows, %d columns.",
                      table_name, nrow(final_big_merged), ncol(final_big_merged)), "DEBUG")
  }
} else {
  debug_log("List of tables to merge ('big_tables_list') is empty. No merging performed.", "WARN")
}


debug_log(sprintf("Final merged data ('final_big_merged') has %d rows and %d columns.",
                  nrow(final_big_merged), ncol(final_big_merged)), "INFO")
log_glimpse(final_big_merged, "Glimpse of final_big_merged")

# Optional: Check for columns that are all NA after merge
all_na_cols <- names(final_big_merged)[sapply(final_big_merged, function(x) all(is.na(x)))]
if(length(all_na_cols) > 0) {
  debug_log(sprintf("Warning: The following %d columns are entirely NA after merging: %s",
                    length(all_na_cols), paste(all_na_cols, collapse=", ")), "WARN")
}

debug_log("Completed Section 11: Merge All Processed Datasets", "INFO")

#Section 12: IDENTIFY NON-NUMERIC COLUMNS------------------------

###############################################################################
# 9) LIST ALL NON-NUMERIC COLUMNS IN final_big_merged
###############################################################################

section_header <- "\n=== SECTION 12: Identify Non-Numeric Columns ===" # Updated section number
cat(section_header)
flog.info(section_header)
debug_log("Starting Section 12: Identifying non-numeric columns in final_big_merged", "DEBUG")

# Check if final_big_merged exists
if (!exists("final_big_merged") || is.null(final_big_merged)) {
  debug_log("final_big_merged dataframe not found. Skipping non-numeric column identification.", "ERROR")
  non_numeric_col_names <- character(0) # Empty list
} else {
  # Drop geometry for type checking
  if (inherits(final_big_merged, "sf")) {
    final_big_merged_no_geo <- st_drop_geometry(final_big_merged)
  } else {
    final_big_merged_no_geo <- final_big_merged # Assume it's already non-spatial if not sf
  }
  
  # Identify non-numeric columns (excluding geometry which was dropped)
  # Consider logicals as potentially non-numeric for modeling inputs depending on method
  # Let's list non-numeric OR non-logical for clarity, as factors/characters are the main concern
  non_numeric_or_char_flags <- sapply(final_big_merged_no_geo, function(col) {
    !is.numeric(col) && !is.logical(col) # Focus on character, factor, list, etc.
  })
  
  non_numeric_col_names <- names(final_big_merged_no_geo)[non_numeric_or_char_flags]
  
  message_nn <- "Non-numeric, non-logical columns in final_big_merged (excluding geometry):"
  cat("\n", message_nn, "\n")
  flog.info(message_nn)
  
  if (length(non_numeric_col_names) > 0) {
    for (col_name in non_numeric_col_names) {
      # Get class, handling potential multiple classes (e.g., POSIXct)
      col_class <- class(final_big_merged_no_geo[[col_name]])[1]
      col_info <- sprintf("- %s (class: %s)", col_name, col_class)
      cat(col_info, "\n")
      flog.info(col_info)
    }
  } else {
    msg_none <- "No non-numeric, non-logical columns found (excluding geometry)."
    cat(msg_none, "\n")
    flog.info(msg_none)
  }
  
  summary_nn <- sprintf("\nTotal non-numeric, non-logical columns: %d out of %d total columns (excluding geometry). Check listed columns for potential issues before modeling.",
                        length(non_numeric_col_names),
                        ncol(final_big_merged_no_geo))
  cat(summary_nn, "\n")
  flog.info(summary_nn)
  
  debug_log(sprintf("Found %d non-numeric, non-logical columns in final_big_merged.",
                    length(non_numeric_col_names)), "INFO")
}

debug_log("Completed Section 12: Identify Non-Numeric Columns", "INFO")

#Section 13: CLASSIFY AND PREPARE COLUMNS FOR MODELING------------------------

###############################################################################
# 10) CLASSIFY AND SEPARATE COLUMNS
###############################################################################

section_header <- "\n=== SECTION 13: Classify and Prepare Columns for Modeling ===" # Updated section number
cat(section_header)
flog.info(section_header)
debug_log("Starting Section 13: Classify and separate columns", "DEBUG")

# Check if final_big_merged exists
if (!exists("final_big_merged") || is.null(final_big_merged)) {
  stop("final_big_merged dataframe not found. Cannot proceed with column classification.")
}

#-----------------------------------------------------------------------------
# 13A) Define Columns to Exclude from Analysis
#-----------------------------------------------------------------------------
# These are identifiers, raw list columns produced during aggregation, geometry,
# highly redundant columns, or columns explicitly not wanted as predictors or outcomes.
exclude_from_analysis <- c(
  # Identifiers / Geography
  "County GEOID",
  "geometry",
  "State FIPS",         # Use State_FIPS if needed, this might be list column
  "County NAME",        # Use County_NAME_Latest if needed
  "County NAMELSAD",    # Use County_NAMELSAD_Latest if needed
  "State Name",         # Use State_Name_Latest
  "State_Abbreviation", # Use State_Abbreviation_Latest
  "County, State",      # Composite identifier
  "Metro/Micro Area Name", # Text descriptions
  "Metro/Micro Area GEOID",# CBSA identifier
  "CSA Name (If Applicable)", # Text descriptions
  "CSA GEOID (If Applicable)", # CSA identifier
  "County_NAME_Latest", # Keep for reference, exclude from model matrix
  "State_Name_Latest",  # Keep for reference, exclude from model matrix
  "State_Abbreviation_Latest", # Derived, keep for reference
  
  # List columns created during unique_county_geoids aggregation
  "StateNames",
  "StateFIPSs",
  "StateAbbrevs",
  "CountyNAMEs",
  "CountyNAMELSADs",
  "Vintages",
  
  # Metadata / Redundant Info
  "Vintage(s)",                 # Text summary of vintages
  "county_geometry_vintage_year", # Metadata about geometry source year
  "NAME",                       # Often county name, redundant with County_NAME_Latest
  
  # Columns specifically excluded based on original script / interpretation
  "Investments_Lacking_Capex_Amount_Wellesley", # Metadata about data quality
  # Original script excluded these - check if still relevant
  # "Total_YesNo_Wellesley", # Seems redundant if other presence flags exist
  # "County_Data_Available_CGT", # Metadata flags
  # "County_Data_Available_Highways" # Metadata flags
  # Also exclude the "Original Status" XChange columns if they exist
  grep("^XChange_Original_Status_", names(final_big_merged), value = TRUE)
  
  # Add any other columns known to be unsuitable for modeling here
)
# Ensure exclude list only contains columns actually present
exclude_from_analysis <- intersect(exclude_from_analysis, names(final_big_merged))
debug_log(sprintf("Defined %d columns to exclude from modeling analysis.", length(exclude_from_analysis)), "DEBUG")
flog.debug("Columns to exclude: %s", paste(exclude_from_analysis, collapse=", "))

#-----------------------------------------------------------------------------
# 13B) Identify Potential Dependent (Outcome) Variables for LASSO
#-----------------------------------------------------------------------------
# Based on original script: focus on binary presence flags from Wellesley and CIM
# LASSO requires a single outcome per model run. This list identifies candidates to iterate over.
# We need BINARY (logical TRUE/FALSE or 0/1) outcomes for binomial LASSO.

potential_dependent_candidates <- c(
  # Wellesley presence flags (overall, by sector, by product)
  grep("Wellesley.*_Presence$", names(final_big_merged), value = TRUE, ignore.case = TRUE),
  # CIM presence flags (by sector)
  grep("^CIM_Presence_", names(final_big_merged), value = TRUE, ignore.case = TRUE),
  # MESC presence flag
  "MESC_Award_Presence",
  # CHIPS presence flag
  "CHIPS_Award_Presence"
  # Add other binary investment/outcome flags if relevant
)

# Filter this list to keep only columns that are actually LOGICAL in the dataframe
potential_dependent_candidates <- intersect(potential_dependent_candidates, names(final_big_merged))
actual_logical_dependents <- potential_dependent_candidates[
  sapply(final_big_merged[potential_dependent_candidates], is.logical)
]

if(length(actual_logical_dependents) < length(potential_dependent_candidates)){
  debug_log(sprintf("Warning: Some potential dependent variables were not logical: %s",
                    paste(setdiff(potential_dependent_candidates, actual_logical_dependents), collapse=", ")), "WARN")
}

dependent_cols_for_lasso <- actual_logical_dependents
debug_log(sprintf("Identified %d logical columns as potential dependent variables for LASSO iterations.", length(dependent_cols_for_lasso)), "DEBUG")
flog.debug("Potential LASSO dependent vars: %s", paste(dependent_cols_for_lasso, collapse=", "))


#-----------------------------------------------------------------------------
# 13C) Identify Candidate Independent (Predictor) Variables
#-----------------------------------------------------------------------------
all_col_names <- names(final_big_merged)

# Start with all columns, remove excluded ones, remove potential dependent ones
independent_candidates <- setdiff(all_col_names, c(exclude_from_analysis, potential_dependent_candidates)) # Remove potential deps too

debug_log(sprintf("Identified %d columns as initial 'independent candidates' after exclusions.",
                  length(independent_candidates)), "DEBUG")

#-----------------------------------------------------------------------------
# 13D) Classify Independent Candidates by Type and Filter
#-----------------------------------------------------------------------------
# Need to handle different data types before creating model matrix for LASSO

# Drop geometry if still present in candidates (should be in exclude_from_analysis)
if ("geometry" %in% independent_candidates) {
  independent_candidates <- setdiff(independent_candidates, "geometry")
  debug_log("Removed 'geometry' from independent candidates list.", "DEBUG")
}

# Get types for remaining candidates
candidate_types <- sapply(final_big_merged[independent_candidates], function(c) class(c)[1])

# Identify columns unsuitable for direct use in glmnet matrix (list, character, factor without encoding)
list_vars <- independent_candidates[candidate_types == "list"]
character_vars <- independent_candidates[candidate_types == "character"]
factor_vars <- independent_candidates[candidate_types == "factor"]
# Add other complex types if they exist (e.g., POSIXct)
date_vars <- independent_candidates[sapply(final_big_merged[independent_candidates], inherits, "Date")]
datetime_vars <- independent_candidates[sapply(final_big_merged[independent_candidates], inherits, "POSIXt")]


# Log unsuitable types found
if(length(list_vars) > 0) flog.warn("List columns found among independent candidates (will be excluded): %s", paste(list_vars, collapse=", "))
if(length(character_vars) > 0) flog.warn("Character columns found among independent candidates (will be excluded unless encoded): %s", paste(character_vars, collapse=", "))
if(length(factor_vars) > 0) flog.warn("Factor columns found among independent candidates (will be excluded unless encoded): %s", paste(factor_vars, collapse=", "))
if(length(date_vars) > 0) flog.warn("Date columns found among independent candidates (will be excluded): %s", paste(date_vars, collapse=", "))
if(length(datetime_vars) > 0) flog.warn("DateTime columns found among independent candidates (will be excluded): %s", paste(datetime_vars, collapse=", "))

# Define the set of predictors suitable for LASSO (numeric, integer, logical)
# These can be directly used or easily converted for the model matrix.
numeric_vars <- independent_candidates[candidate_types %in% c("numeric", "integer", "units")] # Include units if they can be treated as numeric
logical_vars <- independent_candidates[candidate_types == "logical"]

# Handle 'units' columns - convert them to numeric
unit_vars <- independent_candidates[candidate_types == "units"]
if(length(unit_vars) > 0) {
  debug_log(sprintf("Found %d 'units' columns. Converting to numeric: %s", length(unit_vars), paste(unit_vars, collapse=", ")), "INFO")
  # Create a temporary df to modify
  final_big_merged_temp_units <- final_big_merged # Avoid modifying original in place yet
  for(uv in unit_vars) {
    final_big_merged_temp_units[[uv]] <- as.numeric(final_big_merged_temp_units[[uv]])
  }
  # Note: Need to use this modified df in the LASSO section or apply conversion there.
  # Let's store the names of numeric/logical predictors based on the original df for now.
}


independent_variables_for_lasso <- union(numeric_vars, logical_vars)

debug_log(sprintf("Selected %d numeric/logical columns as independent variables for LASSO.",
                  length(independent_variables_for_lasso)), "INFO")


# Log counts by suitable type
final_numeric_vars <- intersect(independent_variables_for_lasso, names(final_big_merged)[sapply(final_big_merged, is.numeric)])
final_logical_vars <- intersect(independent_variables_for_lasso, names(final_big_merged)[sapply(final_big_merged, is.logical)])
final_integer_vars <- intersect(independent_variables_for_lasso, names(final_big_merged)[sapply(final_big_merged, is.integer)])
final_units_vars   <- intersect(independent_variables_for_lasso, names(final_big_merged)[sapply(final_big_merged, inherits, "units")])


debug_log("=== Final Independent Variable Classification (Suitable for LASSO Matrix) ===", "INFO")
debug_log(sprintf("Numeric (non-int, non-units): %d", length(setdiff(final_numeric_vars, c(final_integer_vars, final_units_vars)))), "INFO")
debug_log(sprintf("Integer: %d", length(final_integer_vars)), "INFO")
debug_log(sprintf("Logical: %d", length(final_logical_vars)), "INFO")
debug_log(sprintf("Units (will be converted): %d", length(final_units_vars)), "INFO")


#-----------------------------------------------------------------------------
# 13E) Create Safe Variable Names (Optional but recommended)
#-----------------------------------------------------------------------------
# `glmnet` works with matrices, but safe names are useful if formulas are used or for interpreting coefficients.
make_safe_varname <- function(varname) {
  # Replace non-alphanumeric (except underscore, period) with underscore
  safe_name <- gsub("[^a-zA-Z0-9_.]", "_", varname)
  # Replace multiple underscores with single underscore
  safe_name <- gsub("_+", "_", safe_name)
  # Remove leading/trailing underscores
  safe_name <- gsub("^_|_$", "", safe_name)
  # Prepend 'X' if starts with a number or period
  if(grepl("^[0-9.]", safe_name)) {
    safe_name <- paste0("X", safe_name)
  }
  # Handle potential R keywords (e.g., 'if', 'for') - less common with long names
  # if (safe_name %in% c("if", "else", ...)) safe_name <- paste0(safe_name, "_var")
  # Ensure name is not empty
  if (safe_name == "") return("unnamed_var")
  safe_name
}

# Create mapping for potential dependent and selected independent variables
all_vars_for_mapping <- c(dependent_cols_for_lasso, independent_variables_for_lasso)
# Ensure unique names before applying function
all_vars_for_mapping <- unique(all_vars_for_mapping)

var_name_map <- setNames(sapply(all_vars_for_mapping, make_safe_varname), all_vars_for_mapping)

# Check which names were changed
changed_map <- var_name_map[var_name_map != names(var_name_map)]
if(length(changed_map) > 0) {
  debug_log(sprintf("%d variable names changed for safety:", length(changed_map)), "INFO")
  # Log first few changes for example
  for (i in 1:min(5, length(changed_map))) {
    flog.debug("  %s --> %s", names(changed_map)[i], changed_map[i])
  }
  if(length(changed_map) > 5) flog.debug("  ... (and %d more)", length(changed_map)-5)
} else {
  debug_log("No variable names needed changing for safety.", "INFO")
}

# Store the map for potential use later? Not strictly needed for glmnet matrix approach.
# assign("safe_var_name_map", var_name_map, envir = .GlobalEnv)

debug_log("Completed Section 13: Classify and Prepare Columns for Modeling", "INFO")

#Section 14: LASSO REGRESSION ANALYSIS------------------------

###############################################################################
# 11) LASSO REGRESSION ANALYSIS
###############################################################################

section_header <- "\n=== SECTION 14: LASSO Regression Analysis ===" # Updated section number
cat(section_header)
flog.info(section_header)
debug_log("Starting Section 14: LASSO regression analysis", "DEBUG")

# Check for required objects
required_objects <- c("final_big_merged", "dependent_cols_for_lasso", "independent_variables_for_lasso")
missing_objects <- required_objects[!sapply(required_objects, exists)]
if (length(missing_objects) > 0) {
  stop(sprintf("Missing required objects for LASSO analysis: %s. Cannot proceed.",
               paste(missing_objects, collapse=", ")))
}
if (length(dependent_cols_for_lasso) == 0 || length(independent_variables_for_lasso) == 0) {
  stop("Dependent or independent variable lists are empty. Cannot proceed with LASSO.")
}

#-----------------------------------------------------------------------------
# 14A) Prepare Data Matrix for glmnet
#-----------------------------------------------------------------------------
section_header_14a <- "\n=== SECTION 14A: Prepare Data Matrix ===\n"
cat(section_header_14a)
flog.info(section_header_14a)

# Select only relevant columns (potential outcomes and predictors)
cols_for_model <- unique(c(dependent_cols_for_lasso, independent_variables_for_lasso))
model_data_raw <- final_big_merged %>%
  st_drop_geometry() %>%
  select(all_of(cols_for_model))

debug_log(sprintf("Selected %d columns for the initial model dataframe.", ncol(model_data_raw)), "DEBUG")

# Handle 'units' columns by converting to numeric
unit_cols_in_model <- intersect(colnames(model_data_raw), final_units_vars) # Use list from Sec 13
if(length(unit_cols_in_model) > 0) {
  debug_log(sprintf("Converting %d 'units' columns to numeric: %s", length(unit_cols_in_model), paste(unit_cols_in_model, collapse=", ")), "INFO")
  for(uc in unit_cols_in_model) {
    model_data_raw[[uc]] <- as.numeric(model_data_raw[[uc]])
  }
}

# Handle missing values: Option 1: Remove rows with NA in ANY selected column (simpler)
# Option 2: Imputation (e.g., median/mean for numeric, mode/FALSE for logical) - more complex but retains data
# Using Option 1 (complete cases) for consistency with original script's NA handling approach
initial_rows <- nrow(model_data_raw)
model_data_complete <- na.omit(model_data_raw)
final_rows <- nrow(model_data_complete)
rows_removed <- initial_rows - final_rows
if (rows_removed > 0) {
  debug_log(sprintf("Removed %d rows with NA values in selected columns. %d rows remain for analysis.", rows_removed, final_rows), "WARN")
} else {
  debug_log("No rows removed due to NA values.", "INFO")
}

if (final_rows < 10) { # Arbitrary threshold - need sufficient data
  stop(sprintf("Insufficient complete cases (%d) remaining after NA removal. Cannot proceed.", final_rows))
}

# Convert logical predictors and outcomes to numeric (0/1) for glmnet
logical_cols_model <- names(model_data_complete)[sapply(model_data_complete, is.logical)]
if(length(logical_cols_model) > 0) {
  debug_log(sprintf("Converting %d logical columns to numeric (0/1): %s", length(logical_cols_model), paste(logical_cols_model, collapse=", ")), "INFO")
  model_data_numeric <- model_data_complete %>%
    mutate(across(all_of(logical_cols_model), as.numeric))
} else {
  model_data_numeric <- model_data_complete
}

# Separate predictor matrix (x) and outcome vectors (y will be selected in loop)
# Ensure independent variables exist in the final numeric dataframe
final_independent_vars <- intersect(independent_variables_for_lasso, names(model_data_numeric))
if(length(final_independent_vars) == 0) stop("No independent variables remain after processing.")

x_matrix <- as.matrix(model_data_numeric[, final_independent_vars])

# Ensure final dependent variables exist
final_dependent_vars <- intersect(dependent_cols_for_lasso, names(model_data_numeric))
if(length(final_dependent_vars) == 0) stop("No potential dependent variables remain after processing.")


debug_log(sprintf("Prepared model matrix 'x_matrix' with %d rows and %d columns (predictors).",
                  nrow(x_matrix), ncol(x_matrix)), "DEBUG")

#-----------------------------------------------------------------------------
# 14B) Run LASSO Regression Loop
#-----------------------------------------------------------------------------
section_header_14b <- "\n=== SECTION 14B: Run LASSO Loop ===\n"
cat(section_header_14b)
flog.info(section_header_14b)

lasso_results_list <- list() # To store results from each outcome

for (dep_var in final_dependent_vars) {
  
  current_iter_msg <- sprintf("\n--- Running LASSO for Dependent Variable: %s ---", dep_var)
  cat(current_iter_msg, "\n")
  flog.info(current_iter_msg)
  
  # Define outcome vector y (already numeric 0/1)
  y_vector <- model_data_numeric[[dep_var]]
  
  # Check if outcome variable has sufficient variation
  if (length(unique(y_vector)) < 2) {
    msg_skip_y <- sprintf("Skipping '%s': Outcome variable has only one unique value (%s).", dep_var, unique(y_vector)[1])
    cat(msg_skip_y, "\n")
    flog.warn(msg_skip_y)
    next # Skip to next dependent variable
  }
  # Check if outcome is binary (already converted logicals, but check numeric case)
  if (!all(y_vector %in% c(0, 1))) {
    msg_skip_bin <- sprintf("Skipping '%s': Outcome variable is not binary (0/1) after processing.", dep_var)
    cat(msg_skip_bin, "\n")
    flog.warn(msg_skip_bin)
    next # Skip to next dependent variable
  }
  
  n_positive_outcome <- sum(y_vector == 1)
  n_negative_outcome <- sum(y_vector == 0)
  if (min(n_positive_outcome, n_negative_outcome) < 10) { # Check for sufficient cases in each class
    msg_skip_bal <- sprintf("Skipping '%s': Insufficient cases in one class (Positive: %d, Negative: %d). Needs at least 10.", dep_var, n_positive_outcome, n_negative_outcome)
    cat(msg_skip_bal, "\n")
    flog.warn(msg_skip_bal)
    next
  }
  
  debug_log(sprintf("Outcome '%s': %d positive cases, %d negative cases.", dep_var, n_positive_outcome, n_negative_outcome), "DEBUG")
  
  # Run cross-validated LASSO using glmnet
  # Using alpha = 1 for LASSO. alpha = 0 is Ridge, 0 < alpha < 1 is Elastic Net.
  # nfolds=10 is default for cv.glmnet
  set.seed(123) # for reproducibility of CV folds
  cv_lasso_fit <- NULL
  tryCatch({
    # Note: glmnet automatically standardizes predictors by default (standardize=TRUE)
    cv_lasso_fit <- cv.glmnet(x = x_matrix, y = y_vector, alpha = 1, family = "binomial", nfolds = 10)
    debug_log(sprintf("cv.glmnet completed successfully for '%s'.", dep_var), "DEBUG")
    # Optional: Plot the CV curve
    # plot(cv_lasso_fit)
    # title(paste("CV Plot for:", dep_var), line = 2.5)
  }, error = function(e) {
    msg_err_fit <- sprintf("Error during cv.glmnet for '%s': %s", dep_var, e$message)
    cat(msg_err_fit, "\n")
    flog.error(msg_err_fit)
  })
  
  if (is.null(cv_lasso_fit)) {
    msg_fail_fit <- sprintf("Failed to fit LASSO model for '%s'. Skipping results.", dep_var)
    cat(msg_fail_fit, "\n")
    next # Skip to the next dependent variable if fit failed
  }
  
  # Extract results using lambda.min (minimizes CV error)
  # Alternatively use lambda.1se (most parsimonious model within 1 std error of minimum)
  optimal_lambda <- cv_lasso_fit$lambda.min
  lambda_1se <- cv_lasso_fit$lambda.1se
  debug_log(sprintf("Optimal lambda.min for '%s': %f", dep_var, optimal_lambda), "DEBUG")
  debug_log(sprintf("Optimal lambda.1se for '%s': %f", dep_var, lambda_1se), "DEBUG")
  
  # Get coefficients at the optimal lambda (using lambda.min here)
  lasso_coefs <- coef(cv_lasso_fit, s = optimal_lambda)
  
  # Identify non-zero coefficients (excluding intercept)
  selected_vars_indices <- which(lasso_coefs[-1, 1] != 0) # Exclude intercept row
  selected_vars_names <- rownames(lasso_coefs)[-1][selected_vars_indices]
  selected_vars_coefs <- lasso_coefs[-1, 1][selected_vars_indices]
  
  num_selected <- length(selected_vars_names)
  msg_selected <- sprintf("LASSO identified %d non-zero coefficients (predictors) for '%s' at lambda.min = %f",
                          num_selected, dep_var, optimal_lambda)
  cat(msg_selected, "\n")
  flog.info(msg_selected)
  
  # Store results
  results_entry <- list(
    dependent_variable = dep_var,
    n_positive_cases = n_positive_outcome,
    n_negative_cases = n_negative_outcome,
    lambda_min = optimal_lambda,
    lambda_1se = lambda_1se,
    num_predictors_selected = num_selected,
    selected_predictors = if(num_selected > 0) data.frame(Predictor = selected_vars_names, Coefficient = selected_vars_coefs) else data.frame()
  )
  lasso_results_list[[dep_var]] <- results_entry
  
  # Print selected variables and coefficients
  if (num_selected > 0) {
    cat("Selected Predictors and Coefficients:\n")
    print(results_entry$selected_predictors)
    flog.debug("Selected vars for %s:\n%s", dep_var, paste(capture.output(print(results_entry$selected_predictors)), collapse="\n"))
  } else {
    cat("No predictors selected (only intercept in model).\n")
    flog.info("No predictors selected for %s at lambda.min.", dep_var)
  }
  
} # End loop through dependent variables

#-----------------------------------------------------------------------------
# 14C) Summarize LASSO Results
#-----------------------------------------------------------------------------
section_header_14c <- "\n=== SECTION 14C: Summarize LASSO Results ===\n"
cat(section_header_14c)
flog.info(section_header_14c)

if (length(lasso_results_list) > 0) {
  # Example Summary: Count how many times each predictor was selected across all outcomes
  all_selected_predictors <- unlist(sapply(lasso_results_list, function(res) {
    if (nrow(res$selected_predictors) > 0) {
      res$selected_predictors$Predictor
    } else {
      NULL
    }
  }))
  
  if(length(all_selected_predictors) > 0) {
    predictor_selection_counts <- sort(table(all_selected_predictors), decreasing = TRUE)
    predictor_summary_df <- data.frame(
      Predictor = names(predictor_selection_counts),
      Selection_Count = as.integer(predictor_selection_counts)
    )
    
    summary_msg_pred <- "\nSummary: Predictor Selection Frequency Across All LASSO Models Run:"
    cat(summary_msg_pred, "\n")
    flog.info(summary_msg_pred)
    print(predictor_summary_df)
    flog.info("\n%s", paste(capture.output(print(predictor_summary_df)), collapse="\n"))
    
  } else {
    summary_msg_none <- "No predictors were selected across any of the LASSO models run."
    cat(summary_msg_none, "\n")
    flog.info(summary_msg_none)
  }
  
  # Save the results list if needed
  # saveRDS(lasso_results_list, file = file.path(output_dir, "lasso_regression_results.rds"))
  # flog.info("LASSO results list saved to RDS file.")
  
} else {
  summary_msg_fail <- "No LASSO models were successfully run or produced results."
  cat(summary_msg_fail, "\n")
  flog.warn(summary_msg_fail)
}


#-----------------------------------------------------------------------------
# Script Completion
#-----------------------------------------------------------------------------
end_time <- Sys.time()
run_duration <- end_time - start_time

final_msg <- sprintf("\n=== SCRIPT COMPLETE ===\nTotal Run Time: %s\nEnd Time: %s\n",
                     format(run_duration), format(end_time))
cat(final_msg)
flog.info(final_msg)

# Log session info for reproducibility
session_info_out <- capture.output(print(sessionInfo()))
flog.info("Session Information:\n%s", paste(session_info_out, collapse="\n"))

debug_log("Completed Section 14: LASSO Regression Analysis", "INFO")
# --- END OF SCRIPT ---
