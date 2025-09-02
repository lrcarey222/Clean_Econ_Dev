# Optimized R Script for Big Green Machine Data Processing
# Major improvements: vectorized geocoding, error handling, performance optimization

# Load required libraries
library(readxl)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(sf)
library(tigris)

# Set tigris options for caching
options(tigris_use_cache = TRUE)

# File path
path <- "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/The-Big-Green-Machine Dataset-August-2025.xlsx"

# --- Helper Functions ---
bad_tokens <- c("", "?", "NA", "N.A.", "n/a", "N/A", "N.D.", "n.d.")

naify <- function(x) {
  x %>% 
    as.character() %>% 
    str_trim() %>% 
    (\(z) ifelse(z %in% bad_tokens, NA_character_, z))()
}

parse_excel_date <- function(x) {
  x <- naify(x)
  # numeric-like (Excel serials, often with ".0")
  n  <- suppressWarnings(as.numeric(x))
  d1 <- as.Date(n, origin = "1899-12-30")           # Excel serial origin
  # ISO-like text dates
  d2 <- suppressWarnings(ymd(x, quiet = TRUE))
  # US-style text dates
  d3 <- suppressWarnings(mdy(x, quiet = TRUE))
  coalesce(d1, d2, d3)
}

parse_num <- function(x) {
  x <- naify(x)
  suppressWarnings(parse_number(x))
}

# Validate coordinates function
validate_coordinates <- function(lon, lat) {
  valid_lon <- !is.na(lon) & lon >= -180 & lon <= 180
  valid_lat <- !is.na(lat) & lat >= -90 & lat <= 90
  # Additional check for US bounds (rough approximation)
  us_bounds <- !is.na(lon) & !is.na(lat) & 
    lon >= -180 & lon <= -65 & 
    lat >= 15 & lat <= 72
  return(valid_lon & valid_lat & us_bounds)
}

cat("Reading Excel file...\n")

# Read and process the main dataset
BIG_GREEN_MACHINE_USA <- tryCatch({
  read_excel(
    path, sheet = "Dataset", guess_max = 10000, .name_repair = "minimal"
  ) %>%
    # 1) USA-only immediately
    filter(Country == "USA") %>%
    # 2) Keep raw date text for audit
    mutate(
      `Project Announcement Date_raw`     = `Project Announcement Date`,
      `Commercial Production Started_raw` = `Commercial Production Started`
    ) %>%
    # 3) Clean + convert dates
    mutate(
      `Project Announcement Date`     = parse_excel_date(`Project Announcement Date_raw`),
      `Commercial Production Started` = parse_excel_date(`Commercial Production Started_raw`),
      announcement_year               = year(`Project Announcement Date`),
      announcement_quarter            = paste0("Q", quarter(`Project Announcement Date`))
    ) %>%
    # 4) Parse numerics that arrived as text (drop trailing ".0", etc.)
    mutate(
      Longitude = parse_num(Longitude),
      Latitude  = parse_num(Latitude),
      `Capital Investment \n($ million)` = parse_num(`Capital Investment \n($ million)`),
      `Targeted annual production`       = parse_num(`Targeted annual production`),
      `Realized annual production`       = parse_num(`Realized annual production`),
      `Target jobs`                      = parse_num(`Target jobs`),
      `Current jobs`                     = parse_num(`Current jobs`),
      `Target Year`                      = suppressWarnings(as.integer(parse_num(`Target Year`)))
    )
}, error = function(e) {
  stop("Error reading Excel file: ", e$message)
})

cat("Data loaded successfully. Rows:", nrow(BIG_GREEN_MACHINE_USA), "\n")

# Quick sanity checks
glimpse(BIG_GREEN_MACHINE_USA)

# See any raw announcement entries that did NOT parse into dates
unparsed_dates <- BIG_GREEN_MACHINE_USA %>%
  filter(!is.na(`Project Announcement Date_raw`) & is.na(`Project Announcement Date`)) %>%
  distinct(`Project Announcement Date_raw`) %>%
  arrange(`Project Announcement Date_raw`)

cat("Unparsed date entries:", nrow(unparsed_dates), "\n")
if (nrow(unparsed_dates) > 0) {
  print(unparsed_dates, n = min(200, nrow(unparsed_dates)))
}

# --- OPTIMIZED TIGRIS-BASED GEOCODING ---
cat("Loading county data from TIGRIS...\n")

# Get tigris 2024 county-level data with error handling
COUNTY_2024 <- tryCatch({
  tigris::counties(year = 2024, cb = TRUE) %>%
    # Keep NAMELSAD, STATE_NAME, GEOID, and geometry
    select(NAMELSAD, STATE_NAME, GEOID, geometry) %>%
    # Rename columns for clarity
    rename(
      COUNTY_NAME = NAMELSAD,
      COUNTY_GEOID = GEOID
    ) %>%
    # Ensure consistent CRS (WGS84)
    st_transform(crs = 4326)
}, error = function(e) {
  stop("Error loading county data: ", e$message)
})

cat("County data loaded. Counties:", nrow(COUNTY_2024), "\n")
glimpse(COUNTY_2024)

# --- VECTORIZED GEOCODING (MUCH FASTER) ---
cat("Starting vectorized geocoding...\n")

# Create a version of the data with valid coordinates only
coords_data <- BIG_GREEN_MACHINE_USA %>%
  mutate(
    # Validate coordinates first
    coords_valid = validate_coordinates(Longitude, Latitude),
    row_id = row_number()
  )

# Count valid coordinates
valid_coords_count <- sum(coords_data$coords_valid, na.rm = TRUE)
cat("Valid coordinates found:", valid_coords_count, "out of", nrow(coords_data), "\n")

# Perform spatial join only on rows with valid coordinates
if (valid_coords_count > 0) {
  cat("Performing spatial join...\n")
  
  # Create sf object from valid coordinates
  valid_coords_sf <- coords_data %>%
    filter(coords_valid) %>%
    st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326, remove = FALSE)
  
  # Perform spatial join - this is much faster than rowwise operations
  geocoded_results <- tryCatch({
    st_join(valid_coords_sf, COUNTY_2024) %>%
      st_drop_geometry() %>%
      mutate(GEOCODING_SUCCESS = !is.na(COUNTY_NAME)) %>%
      select(row_id, GEOCODING_SUCCESS, COUNTY_NAME, STATE_NAME, COUNTY_GEOID)
  }, error = function(e) {
    cat("Warning: Spatial join failed, creating empty results:", e$message, "\n")
    data.frame(
      row_id = integer(0),
      GEOCODING_SUCCESS = logical(0),
      COUNTY_NAME = character(0),
      STATE_NAME = character(0),
      COUNTY_GEOID = character(0)
    )
  })
  
  # Join results back to original data
  BIG_GREEN_MACHINE_USA_GEOCODED <- coords_data %>%
    left_join(geocoded_results, by = "row_id") %>%
    mutate(
      # Set geocoding success to FALSE for invalid coordinates
      GEOCODING_SUCCESS = ifelse(coords_valid, 
                                 coalesce(GEOCODING_SUCCESS, FALSE), 
                                 FALSE),
      # Set county info to NA for failed geocoding
      COUNTY_NAME = ifelse(GEOCODING_SUCCESS, COUNTY_NAME, NA_character_),
      STATE_NAME = ifelse(GEOCODING_SUCCESS, STATE_NAME, NA_character_),
      COUNTY_GEOID = ifelse(GEOCODING_SUCCESS, COUNTY_GEOID, NA_character_)
    ) %>%
    select(-coords_valid, -row_id)
  
} else {
  cat("No valid coordinates found, skipping geocoding.\n")
  BIG_GREEN_MACHINE_USA_GEOCODED <- coords_data %>%
    mutate(
      GEOCODING_SUCCESS = FALSE,
      COUNTY_NAME = NA_character_,
      STATE_NAME = NA_character_,
      COUNTY_GEOID = NA_character_
    ) %>%
    select(-coords_valid, -row_id)
}

# Geocoding summary
geocoding_summary <- BIG_GREEN_MACHINE_USA_GEOCODED %>%
  summarise(
    total_rows = n(),
    successful_geocoding = sum(GEOCODING_SUCCESS, na.rm = TRUE),
    failed_geocoding = sum(!GEOCODING_SUCCESS, na.rm = TRUE),
    missing_coordinates = sum(is.na(Longitude) | is.na(Latitude)),
    success_rate = round(successful_geocoding / total_rows * 100, 2)
  )

cat("\n--- GEOCODING SUMMARY ---\n")
cat("Total rows:", geocoding_summary$total_rows, "\n")
cat("Successful geocoding:", geocoding_summary$successful_geocoding, "\n")
cat("Failed geocoding:", geocoding_summary$failed_geocoding, "\n")
cat("Missing coordinates:", geocoding_summary$missing_coordinates, "\n")
cat("Success rate:", geocoding_summary$success_rate, "%\n")

# --- DATA CLEANING AND BUSINESS RULES ---
cat("Applying data cleaning and business rules...\n")

# Apply all data cleaning rules in a single mutate operation
BIG_GREEN_MACHINE_USA_GEOCODED <- BIG_GREEN_MACHINE_USA_GEOCODED %>%
  mutate(
    # Add CAPEX column (Capital Investment in actual dollars)
    CAPEX = `Capital Investment \n($ million)` * 1e6,
    
    # Apply name-specific rules first
    `Mfg Product` = case_when(
      Name == "Ionic Mineral Technologies Provo Facility" ~ "Battery Cell Components",
      Name == "Orion Township Facility" ~ "EVs",
      Name == "EcoRecycle" ~ "Recycled Materials",
      `Mfg Product` == "Unsure" ~ "",
      TRUE ~ `Mfg Product`
    ),
    
    # Apply sector-specific rules
    Sector = case_when(
      `Mfg Product` == "Storage Battery" ~ "Batteries",
      str_detect(`Mfg Product`, regex("Battery Cell Components", ignore_case = TRUE)) ~ "Batteries",
      TRUE ~ Sector
    ),
    
    # Apply activity-specific rules
    `Mfg Activity` = case_when(
      Sector == "EVs" & `Mfg Product` == "Rare Earths" & `Mfg Activity` == "Manufacturing" ~ "Materials Processing",
      TRUE ~ `Mfg Activity`
    ),
    
    # Apply product-specific rules (do this last to catch cases created by above rules)
    `Mfg Product` = case_when(
      `Mfg Activity` == "Recycling" & (is.na(`Mfg Product`) | `Mfg Product` == "") ~ "Recycled Materials",
      TRUE ~ `Mfg Product`
    )
  )

cat("Data cleaning completed.\n")

# Final data summary
cat("\n--- FINAL DATA SUMMARY ---\n")
glimpse(BIG_GREEN_MACHINE_USA_GEOCODED)

# Create and display the sector/activity/product matrix
cat("\n--- SECTOR/ACTIVITY/PRODUCT COMBINATIONS ---\n")
sector_activity_product <- BIG_GREEN_MACHINE_USA_GEOCODED %>%
  distinct(Sector, `Mfg Activity`, `Mfg Product`) %>%
  arrange(Sector, `Mfg Activity`, `Mfg Product`)

cat("Total unique combinations:", nrow(sector_activity_product), "\n")
glimpse(sector_activity_product)

# Display unique manufacturing products
cat("\n--- UNIQUE MANUFACTURING PRODUCTS ---\n")
unique_products <- unique(sector_activity_product$`Mfg Product`)
cat("Total unique products:", length(unique_products), "\n")
print(unique_products)

# Optional: Show some examples of successful and failed geocoding
cat("\n--- GEOCODING EXAMPLES ---\n")
cat("Successful geocoding examples:\n")
success_examples <- BIG_GREEN_MACHINE_USA_GEOCODED %>%
  filter(GEOCODING_SUCCESS) %>%
  select(Name, City, `State / Province`, Longitude, Latitude, COUNTY_NAME, STATE_NAME) %>%
  slice_head(n = 5)
print(success_examples)

if (sum(!BIG_GREEN_MACHINE_USA_GEOCODED$GEOCODING_SUCCESS, na.rm = TRUE) > 0) {
  cat("\nFailed geocoding examples:\n")
  failed_examples <- BIG_GREEN_MACHINE_USA_GEOCODED %>%
    filter(!GEOCODING_SUCCESS) %>%
    select(Name, City, `State / Province`, Longitude, Latitude) %>%
    slice_head(n = 5)
  print(failed_examples)
}

# Data quality checks
cat("\n--- DATA QUALITY CHECKS ---\n")
quality_summary <- BIG_GREEN_MACHINE_USA_GEOCODED %>%
  summarise(
    total_rows = n(),
    rows_with_capex = sum(!is.na(CAPEX)),
    rows_with_target_jobs = sum(!is.na(`Target jobs`)),
    rows_with_current_jobs = sum(!is.na(`Current jobs`)),
    rows_with_announcement_date = sum(!is.na(`Project Announcement Date`)),
    rows_with_production_start = sum(!is.na(`Commercial Production Started`)),
    blank_mfg_products = sum(`Mfg Product` == "" | is.na(`Mfg Product`), na.rm = TRUE)
  )

cat("Total rows:", quality_summary$total_rows, "\n")
cat("Rows with CAPEX data:", quality_summary$rows_with_capex, "\n")
cat("Rows with target jobs:", quality_summary$rows_with_target_jobs, "\n")
cat("Rows with current jobs:", quality_summary$rows_with_current_jobs, "\n")
cat("Rows with announcement date:", quality_summary$rows_with_announcement_date, "\n")
cat("Rows with production start date:", quality_summary$rows_with_production_start, "\n")
cat("Rows with blank manufacturing products:", quality_summary$blank_mfg_products, "\n")

cat("\nProject-level data processing completed!\n")
cat("Dataset: BIG_GREEN_MACHINE_USA_GEOCODED\n")
cat("Rows:", nrow(BIG_GREEN_MACHINE_USA_GEOCODED), "Columns:", ncol(BIG_GREEN_MACHINE_USA_GEOCODED), "\n")

# --- COUNTY-LEVEL AGGREGATIONS ---
cat("\n--- CREATING COUNTY-LEVEL AGGREGATIONS ---\n")

# Filter out unwanted operating statuses
cat("Filtering operating statuses...\n")
active_projects <- BIG_GREEN_MACHINE_USA_GEOCODED %>%
  filter(!`Operating Status` %in% c("Rumored", "Closed", "Cancelled", "Paused")) %>%
  # Only include successfully geocoded projects for county aggregation
  filter(GEOCODING_SUCCESS == TRUE)

cat("Active projects after filtering:", nrow(active_projects), "out of", nrow(BIG_GREEN_MACHINE_USA_GEOCODED), "\n")

# Get all US counties (excluding territories) for complete coverage
us_states <- c("Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", 
               "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", 
               "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", 
               "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", 
               "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", 
               "New Hampshire", "New Jersey", "New Mexico", "New York", 
               "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", 
               "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", 
               "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", 
               "West Virginia", "Wisconsin", "Wyoming", "District of Columbia")

all_us_counties <- COUNTY_2024 %>%
  st_drop_geometry() %>%
  filter(STATE_NAME %in% us_states) %>%
  select(STATE_NAME, COUNTY_NAME, COUNTY_GEOID)

cat("Total US counties (excluding territories):", nrow(all_us_counties), "\n")

# Helper function to create plain English column names
make_column_name <- function(prefix, sector = NULL, activity = NULL, product = NULL) {
  parts <- c(prefix)
  if (!is.null(sector) && !is.na(sector) && sector != "") {
    parts <- c(parts, paste(sector, "Sector"))
  }
  if (!is.null(activity) && !is.na(activity) && activity != "") {
    parts <- c(parts, activity)
  }
  if (!is.null(product) && !is.na(product) && product != "") {
    parts <- c(parts, product)
  }
  return(paste(parts, collapse = ", "))
}

# Base county aggregation (all projects combined)
cat("Creating base county aggregation...\n")
county_base <- active_projects %>%
  group_by(STATE_NAME, COUNTY_NAME, COUNTY_GEOID) %>%
  summarise(
    `Facility Presence` = TRUE,
    `Investment Amount` = sum(CAPEX, na.rm = TRUE),
    .groups = "drop"
  )

# Join with all counties to ensure complete coverage
county_aggregation <- all_us_counties %>%
  left_join(county_base, by = c("STATE_NAME", "COUNTY_NAME", "COUNTY_GEOID")) %>%
  mutate(
    `Facility Presence` = coalesce(`Facility Presence`, FALSE),
    `Investment Amount` = coalesce(`Investment Amount`, 0)
  )

cat("Base aggregation created with", nrow(county_aggregation), "counties\n")
cat("Counties with facilities:", sum(county_aggregation$`Facility Presence`), "\n")

# Get unique values for different combinations
unique_sectors <- active_projects %>% 
  filter(!is.na(Sector) & Sector != "") %>% 
  distinct(Sector) %>% 
  pull(Sector) %>% 
  sort()

unique_sector_activity <- active_projects %>% 
  filter(!is.na(Sector) & Sector != "" & !is.na(`Mfg Activity`) & `Mfg Activity` != "") %>% 
  distinct(Sector, `Mfg Activity`) %>% 
  arrange(Sector, `Mfg Activity`)

unique_sector_product <- active_projects %>% 
  filter(!is.na(Sector) & Sector != "" & !is.na(`Mfg Product`) & `Mfg Product` != "") %>% 
  distinct(Sector, `Mfg Product`) %>% 
  arrange(Sector, `Mfg Product`)

unique_sector_activity_product <- active_projects %>% 
  filter(!is.na(Sector) & Sector != "" & 
           !is.na(`Mfg Activity`) & `Mfg Activity` != "" & 
           !is.na(`Mfg Product`) & `Mfg Product` != "") %>% 
  distinct(Sector, `Mfg Activity`, `Mfg Product`) %>% 
  arrange(Sector, `Mfg Activity`, `Mfg Product`)

cat("Unique combinations found:\n")
cat("- Sectors:", length(unique_sectors), "\n")
cat("- Sector-Activity combinations:", nrow(unique_sector_activity), "\n")
cat("- Sector-Product combinations:", nrow(unique_sector_product), "\n")
cat("- Sector-Activity-Product combinations:", nrow(unique_sector_activity_product), "\n")

# Sector-level aggregations
cat("Creating sector-level aggregations...\n")
for (sector in unique_sectors) {
  cat("Processing sector:", sector, "\n")
  
  sector_data <- active_projects %>%
    filter(Sector == sector) %>%
    group_by(STATE_NAME, COUNTY_NAME, COUNTY_GEOID) %>%
    summarise(
      presence = TRUE,
      investment = sum(CAPEX, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Create column names
  presence_col <- make_column_name("Facility Presence", sector)
  investment_col <- make_column_name("Investment Amount", sector)
  
  # Join with main aggregation
  county_aggregation <- county_aggregation %>%
    left_join(sector_data, by = c("STATE_NAME", "COUNTY_NAME", "COUNTY_GEOID")) %>%
    mutate(
      !!presence_col := coalesce(presence, FALSE),
      !!investment_col := coalesce(investment, 0)
    ) %>%
    select(-presence, -investment)
}

# Sector-Activity combinations
cat("Creating sector-activity aggregations...\n")
for (i in 1:nrow(unique_sector_activity)) {
  sector <- unique_sector_activity$Sector[i]
  activity <- unique_sector_activity$`Mfg Activity`[i]
  
  if (i %% 10 == 0) cat("Processing combination", i, "of", nrow(unique_sector_activity), "\n")
  
  combo_data <- active_projects %>%
    filter(Sector == sector & `Mfg Activity` == activity) %>%
    group_by(STATE_NAME, COUNTY_NAME, COUNTY_GEOID) %>%
    summarise(
      presence = TRUE,
      investment = sum(CAPEX, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Create column names
  presence_col <- make_column_name("Facility Presence", sector, activity)
  investment_col <- make_column_name("Investment Amount", sector, activity)
  
  # Join with main aggregation
  county_aggregation <- county_aggregation %>%
    left_join(combo_data, by = c("STATE_NAME", "COUNTY_NAME", "COUNTY_GEOID")) %>%
    mutate(
      !!presence_col := coalesce(presence, FALSE),
      !!investment_col := coalesce(investment, 0)
    ) %>%
    select(-presence, -investment)
}

# Sector-Product combinations
cat("Creating sector-product aggregations...\n")
for (i in 1:nrow(unique_sector_product)) {
  sector <- unique_sector_product$Sector[i]
  product <- unique_sector_product$`Mfg Product`[i]
  
  if (i %% 10 == 0) cat("Processing combination", i, "of", nrow(unique_sector_product), "\n")
  
  combo_data <- active_projects %>%
    filter(Sector == sector & `Mfg Product` == product) %>%
    group_by(STATE_NAME, COUNTY_NAME, COUNTY_GEOID) %>%
    summarise(
      presence = TRUE,
      investment = sum(CAPEX, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Create column names  
  presence_col <- make_column_name("Facility Presence", sector, product = product)
  investment_col <- make_column_name("Investment Amount", sector, product = product)
  
  # Join with main aggregation
  county_aggregation <- county_aggregation %>%
    left_join(combo_data, by = c("STATE_NAME", "COUNTY_NAME", "COUNTY_GEOID")) %>%
    mutate(
      !!presence_col := coalesce(presence, FALSE),
      !!investment_col := coalesce(investment, 0)
    ) %>%
    select(-presence, -investment)
}

# Sector-Activity-Product combinations
cat("Creating sector-activity-product aggregations...\n")
for (i in 1:nrow(unique_sector_activity_product)) {
  sector <- unique_sector_activity_product$Sector[i]
  activity <- unique_sector_activity_product$`Mfg Activity`[i]
  product <- unique_sector_activity_product$`Mfg Product`[i]
  
  if (i %% 10 == 0) cat("Processing combination", i, "of", nrow(unique_sector_activity_product), "\n")
  
  combo_data <- active_projects %>%
    filter(Sector == sector & `Mfg Activity` == activity & `Mfg Product` == product) %>%
    group_by(STATE_NAME, COUNTY_NAME, COUNTY_GEOID) %>%
    summarise(
      presence = TRUE,
      investment = sum(CAPEX, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Create column names
  presence_col <- make_column_name("Facility Presence", sector, activity, product)
  investment_col <- make_column_name("Investment Amount", sector, activity, product)
  
  # Join with main aggregation
  county_aggregation <- county_aggregation %>%
    left_join(combo_data, by = c("STATE_NAME", "COUNTY_NAME", "COUNTY_GEOID")) %>%
    mutate(
      !!presence_col := coalesce(presence, FALSE),
      !!investment_col := coalesce(investment, 0)
    ) %>%
    select(-presence, -investment)
}

cat("\n--- COUNTY AGGREGATION SUMMARY ---\n")
cat("Total counties:", nrow(county_aggregation), "\n")
cat("Total columns:", ncol(county_aggregation), "\n")
cat("Counties with any facilities:", sum(county_aggregation$`Facility Presence`), "\n")
cat("Total investment across all counties: $", 
    formatC(sum(county_aggregation$`Investment Amount`), format = "f", big.mark = ",", digits = 0), "\n")

# Show sample of the aggregated data
cat("\nSample of county aggregation data:\n")
sample_counties <- county_aggregation %>%
  filter(`Facility Presence` == TRUE) %>%
  select(1:min(10, ncol(county_aggregation))) %>%  # Show first 10 columns or all if less
  slice_head(n = 5)
print(sample_counties)

# Show column names for reference
cat("\nColumn names in county aggregation (first 20):\n")
print(head(names(county_aggregation), 20))

glimpse(county_aggregation)

# Clean up large objects to free memory
rm(COUNTY_2024, active_projects)
if (exists("valid_coords_sf")) rm(valid_coords_sf)
gc()

cat("\nScript completed successfully!\n")
cat("Final datasets:\n")
cat("- Project-level: BIG_GREEN_MACHINE_USA_GEOCODED (", nrow(BIG_GREEN_MACHINE_USA_GEOCODED), "rows,", ncol(BIG_GREEN_MACHINE_USA_GEOCODED), "columns)\n")
cat("- County-level: county_aggregation (", nrow(county_aggregation), "rows,", ncol(county_aggregation), "columns)\n")

write_csv(county_aggregation, "wellesley_big_green_machine_county_aggregation_sep_2025.csv")
