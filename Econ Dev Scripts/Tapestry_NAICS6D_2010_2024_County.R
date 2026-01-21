# ==============================================================================
# TAPESTRY EMPLOYMENT DATA PROCESSING PIPELINE - VERSION 2
# ==============================================================================
# 
# This script processes Tapestry employment data and creates a comprehensive
# geographic crosswalk with:
#   - County to CBSA/CSA/Commuting Zone assignments (2024/2020 definitions)
#   - NAICS6-level employment estimates, location quotients, and employment shares
#
# Key features:
#   - CT county -> planning region conversion using LODES employment weights
#   - AK Valdez-Cordova -> Chugach/Copper River conversion
#   - Location Quotient validation for CT CBSA conversions
#   - Commuting zones integrated into main processing
#   - Parallel processing for multi-year data loading
#
# Output (timestamped folder):
#   - RDS file containing all data frames
#   - Individual CSV exports for each data frame
#   - tapestry_naics6 data split by geography level and year
#
# ==============================================================================

# ==============================================================================
# SECTION 0: SETUP AND PACKAGE LOADING
# ==============================================================================

gc()

cat("\n")
cat(strrep("=", 80), "\n")
cat("TAPESTRY EMPLOYMENT DATA PROCESSING PIPELINE - VERSION 2\n")
cat(strrep("=", 80), "\n\n")

cat("[SECTION 0] Loading packages...\n")

pkgs <- c(
  "data.table", "dplyr", "tidyr", "stringr", "stringi", "readr", "readxl",
  "sf", "tigris", "spdep", "Matrix", "RSpectra", "purrr", "httr", "curl",
  "magrittr", "future", "furrr", "knitr", "ggplot2", "lubridate"
)

install_if_missing <- function(p) {
  if (!requireNamespace(p, quietly = TRUE)) {
    cat(sprintf("  Installing: %s\n", p))
    install.packages(p, repos = "https://cloud.r-project.org", quiet = TRUE)
  }
}
invisible(lapply(pkgs, install_if_missing))
suppressPackageStartupMessages(invisible(lapply(pkgs, library, character.only = TRUE)))

options(
  tigris_use_cache = FALSE,
  timeout = 600,
  scipen = 999,
  width = 120,
  pillar.width = 120,
  tibble.width = 120
)

if (identical(.Platform$OS.type, "windows")) {
  options(download.file.method = "libcurl")
}

# Disable S2 spherical geometry for planar operations
sf::sf_use_s2(FALSE)

cat("  Packages loaded successfully.\n\n")

# ------------------------------------------------------------------------------
# Geographic Aggregation Level Codes (used throughout pipeline)
# ------------------------------------------------------------------------------
GEO_AGGREGATION_LEVELS <- tibble::tribble(
  ~geo_aggregation_code, ~geo_aggregation_level,
  1L, "county",
  2L, "state",
  3L, "cbsa",
  4L, "csa",
  5L, "commuting_zone"
)

cat("  Geographic aggregation levels defined:\n")
print(GEO_AGGREGATION_LEVELS)
cat("\n")

# ------------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------------

dbg <- function(fmt, ...) {
  cat(
    format(Sys.time(), "[%H:%M:%S]"),
    tryCatch(sprintf(fmt, ...), error = function(e) paste(fmt, collapse = " ")),
    "\n"
  )
}

glimpse_data <- function(x, label) {
  cat("\n")
  cat(sprintf("Glimpse: %s\n", label))
  dplyr::glimpse(x)
  cat("\n")
}

download_bls_excel <- function(url, destfile, timeout_sec = 120) {
  if (file.exists(destfile)) suppressWarnings(try(unlink(destfile), silent = TRUE))
  ua <- "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  ref <- "https://www.bls.gov/cew/classifications/industry/"
  
  resp <- tryCatch(
    httr::GET(
      url,
      httr::add_headers(
        `User-Agent` = ua,
        `Accept` = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        `Accept-Language` = "en-US,en;q=0.9",
        `Connection` = "keep-alive",
        `Referer` = ref
      ),
      httr::write_disk(destfile, overwrite = TRUE),
      httr::timeout(timeout_sec)
    ),
    error = function(e) e
  )
  
  if (!inherits(resp, "error")) {
    sc <- httr::status_code(resp)
    if (sc == 200 && file.exists(destfile) && file.info(destfile)$size > 0) {
      return(invisible(destfile))
    }
    dbg("    httr download HTTP %s", sc)
  } else {
    dbg("    httr download error: %s", conditionMessage(resp))
  }
  
  dbg("    Falling back to curl::curl_download() ...")
  ok <- tryCatch({
    curl::curl_download(
      url, destfile, mode = "wb", quiet = TRUE,
      handle = curl::new_handle(
        useragent = ua,
        referer = ref,
        httpheader = c(
          "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language: en-US,en;q=0.9"
        )
      )
    )
    TRUE
  }, error = function(e) {
    dbg("    curl_download error: %s", conditionMessage(e))
    FALSE
  })
  
  if (isTRUE(ok) && file.exists(destfile) && !is.na(file.info(destfile)$size) && file.info(destfile)$size > 0) {
    return(invisible(destfile))
  }
  
  stop(paste0(
    "ERROR: Could not download the BLS crosswalk.\n\n",
    "Workaround:\n",
    "  Open this URL in a browser and save the file locally:\n    ",
    url, "\n"
  ))
}

# Helper function to clean CZ names (from ERS format)
clean_cz_name <- function(x) {
  x <- stringr::str_squish(as.character(x))
  m <- stringr::str_match(x, "^(.*),\\s*(.+)$")
  place <- ifelse(is.na(m[,2]), x, m[,2])
  st <- ifelse(is.na(m[,3]), NA_character_, m[,3]) %>%
    stringr::str_replace_all("--", "-") %>%
    stringr::str_replace_all("\\s*-\\s*", "-") %>%
    stringr::str_squish()
  place <- place %>%
    stringr::str_replace(
      stringr::regex("\\s+(city and borough|consolidated government \\(balance\\)|city|town|village|CDP)\\s*$", TRUE),
      ""
    ) %>%
    stringr::str_squish()
  stringr::str_squish(stringr::str_replace(
    ifelse(is.na(st), place, paste0(place, ", ", st)),
    ",\\s+", ", "
  ))
}

cat("  Helper functions defined.\n\n")

# ==============================================================================
# SECTION 1: CONFIGURE PATHS AND CREATE OUTPUT FOLDER
# ==============================================================================

cat("[SECTION 1] Configuring paths and creating output folder...\n")

current_os <- Sys.info()[["sysname"]]
current_user <- Sys.info()[["user"]]
dbg("OS: %s | User: %s", current_os, current_user)

base_drive <- if (current_os == "Windows") {
  file.path("C:/Users", current_user, "OneDrive - RMI")
} else {
  file.path(Sys.getenv("HOME"), "Library", "CloudStorage", "OneDrive-RMI")
}

cred_raw_data_folder <- file.path(
  base_drive,
  "US Program - Documents", "6_Projects", "Clean Regional Economic Development", "ACRE", "Data", "Raw Data"
)

tapestry_base_path <- file.path(
  cred_raw_data_folder,
  "Tapestry_Employment", "contains_naics_999999_county_XX999", "NAICS_6D"
)

dbg("Raw data folder: %s [exists: %s]", cred_raw_data_folder, dir.exists(cred_raw_data_folder))
dbg("Tapestry path: %s [exists: %s]", tapestry_base_path, dir.exists(tapestry_base_path))

if (!dir.exists(cred_raw_data_folder)) stop("ERROR: Raw data folder path not found")
if (!dir.exists(tapestry_base_path)) stop("ERROR: Tapestry data path not found")

# Create timestamped output folder
timestamp_str <- format(Sys.time(), "%Y%m%d_%H%M%S")
output_folder <- file.path(getwd(), paste0("tapestry_output_", timestamp_str))
dir.create(output_folder, showWarnings = FALSE, recursive = TRUE)
dbg("Output folder created: %s", output_folder)

cat("  Paths validated.\n\n")

# ==============================================================================
# SECTION 2: LOAD NAICS HIERARCHY CROSSWALK
# ==============================================================================

cat("[SECTION 2] Loading NAICS hierarchy crosswalk...\n")

naics_hierarchy_xwalk_link <- "https://www.bls.gov/cew/classifications/industry/qcew-naics-hierarchy-crosswalk.xlsx"
naics_hierarchy_xwalk_path <- file.path(tempdir(), "qcew-naics-hierarchy-crosswalk.xlsx")
download_bls_excel(naics_hierarchy_xwalk_link, naics_hierarchy_xwalk_path)
dbg("NAICS hierarchy crosswalk downloaded to: %s", naics_hierarchy_xwalk_path)

naics_hierarchy_sheets <- readxl::excel_sheets(naics_hierarchy_xwalk_path)
dbg("NAICS hierarchy crosswalk sheets: %s", paste(naics_hierarchy_sheets, collapse = ", "))

naics_hierarchy_xwalk <- purrr::map_dfr(
  naics_hierarchy_sheets,
  ~ readxl::read_excel(naics_hierarchy_xwalk_path, sheet = .x) %>%
    dplyr::mutate(sheet_name = .x) %>%
    dplyr::mutate(across(everything(), as.character))
) %>%
  # Rename sheet_name to naics_year
  dplyr::rename(naics_year = sheet_name)

dbg("NAICS hierarchy crosswalk loaded: %d rows, %d columns", nrow(naics_hierarchy_xwalk), ncol(naics_hierarchy_xwalk))
glimpse_data(naics_hierarchy_xwalk, "NAICS hierarchy crosswalk")

# ------------------------------------------------------------------------------
# Create deduplicated NAICS6 to sector mapping
# ------------------------------------------------------------------------------

cat("\n[SECTION 2b] Creating deduplicated NAICS6 to sector mapping...\n")

naics6_to_sector_mapping_raw <- naics_hierarchy_xwalk %>%
  dplyr::select(naics6_code, sector_code, sector_title, naics_year) %>%
  dplyr::filter(!is.na(naics6_code), !is.na(sector_code))

dup_check_before <- naics6_to_sector_mapping_raw %>%
  dplyr::group_by(naics6_code) %>%
  dplyr::summarise(n_mappings = n(), .groups = "drop") %>%
  dplyr::filter(n_mappings > 1)

dbg("NAICS6 codes with multiple mappings (before dedup): %d", nrow(dup_check_before))

naics6_to_sector_mapping <- naics6_to_sector_mapping_raw %>%
  dplyr::distinct(naics6_code, .keep_all = TRUE) %>%
  dplyr::mutate(
    sector_code_for_allocation_mapping = dplyr::if_else(
      naics6_code == "999999",
      "00",
      sector_code
    )
  ) %>%
  dplyr::select(naics6_code, sector_code, sector_title, sector_code_for_allocation_mapping)

dup_check_after <- naics6_to_sector_mapping %>%
  dplyr::group_by(naics6_code) %>%
  dplyr::summarise(n_mappings = n(), .groups = "drop") %>%
  dplyr::filter(n_mappings > 1)

dbg("NAICS6 codes with multiple mappings (after dedup): %d", nrow(dup_check_after))
if (nrow(dup_check_after) == 0) {
  dbg("  SUCCESS: All naics6_code values are now unique")
}

dbg("NAICS6 to sector mapping created: %d rows", nrow(naics6_to_sector_mapping))

# ==============================================================================
# SECTION 3: CT COUNTY -> CBSA ASSIGNMENT (2021 and 2024)
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("SECTION 3: CT COUNTY -> CBSA ASSIGNMENT\n")
cat(strrep("=", 80), "\n\n")

# Helper function for CBSA assignment
assign_county_to_cbsa <- function(counties_sf, cbsa_sf, year_label) {
  target_crs <- 26918  # NAD83 / UTM zone 18N
  
  counties_p <- st_make_valid(st_transform(counties_sf, target_crs))
  cbsa_p <- st_make_valid(st_transform(cbsa_sf, target_crs))
  
  pts <- suppressWarnings(st_point_on_surface(counties_p))
  within_idx <- st_within(pts, cbsa_p)
  
  out <- counties_sf %>%
    st_drop_geometry() %>%
    transmute(
      county_geoid_2021 = county_geoid_2021,
      county_name_2021 = county_name_2021
    )
  
  out[[paste0("county_in_cbsa_", year_label)]] <- FALSE
  out[[paste0("cbsa_", year_label, "_geoid")]] <- NA_character_
  out[[paste0("cbsa_", year_label, "_name")]] <- NA_character_
  
  for (i in seq_len(nrow(counties_p))) {
    hits <- within_idx[[i]]
    if (length(hits) == 0) next
    
    chosen_row <- hits[1]
    
    if (length(hits) > 1) {
      county_i <- counties_p[i, , drop = FALSE]
      cbsas_i <- cbsa_p[hits, , drop = FALSE]
      
      ints <- suppressWarnings(st_intersection(
        county_i %>% select(county_geoid_2021),
        cbsas_i %>% select(cbsa_geoid, cbsa_name)
      ))
      
      if (nrow(ints) > 0) {
        ints$int_area <- st_area(ints)
        
        best_geoid <- ints %>%
          st_drop_geometry() %>%
          group_by(cbsa_geoid) %>%
          summarize(area = sum(as.numeric(int_area)), .groups = "drop") %>%
          arrange(desc(area)) %>%
          slice(1) %>%
          pull(cbsa_geoid)
        
        chosen_row2 <- which(cbsa_p$cbsa_geoid == best_geoid)[1]
        if (!is.na(chosen_row2)) chosen_row <- chosen_row2
      }
    }
    
    out[[paste0("county_in_cbsa_", year_label)]][i] <- TRUE
    out[[paste0("cbsa_", year_label, "_geoid")]][i] <- cbsa_p$cbsa_geoid[chosen_row]
    out[[paste0("cbsa_", year_label, "_name")]][i] <- cbsa_p$cbsa_name[chosen_row]
  }
  
  out
}

# Load CT counties (2021)
ct_counties_2021 <- tigris::counties(state = "CT", year = 2021, cb = FALSE, class = "sf") %>%
  transmute(
    county_geoid_2021 = GEOID,
    county_name_2021 = NAMELSAD,
    geometry = geometry
  )

ct_counties_2021 <- ct_counties_2021 %>% rename(county_geometry_2021 = geometry)
st_geometry(ct_counties_2021) <- "county_geometry_2021"

glimpse_data(ct_counties_2021, "CT Counties 2021 (sf)")

# Load CBSAs for 2021 and 2024
cbsa_2021_all <- tigris::core_based_statistical_areas(year = 2021, cb = FALSE, class = "sf") %>%
  transmute(cbsa_geoid = CBSAFP, cbsa_name = NAMELSAD, geometry = geometry)

cbsa_2024_all <- tigris::core_based_statistical_areas(year = 2024, cb = FALSE, class = "sf") %>%
  transmute(cbsa_geoid = CBSAFP, cbsa_name = NAMELSAD, geometry = geometry)

ct_bbox_ll <- st_as_sfc(st_bbox(ct_counties_2021))

cbsa_2021 <- cbsa_2021_all[st_intersects(cbsa_2021_all, ct_bbox_ll, sparse = FALSE)[, 1], ]
cbsa_2024 <- cbsa_2024_all[st_intersects(cbsa_2024_all, ct_bbox_ll, sparse = FALSE)[, 1], ]

glimpse_data(cbsa_2021, "CBSA 2021 (filtered to CT vicinity)")
glimpse_data(cbsa_2024, "CBSA 2024 (filtered to CT vicinity)")

# Assign counties to CBSAs
assign_2021 <- assign_county_to_cbsa(ct_counties_2021, cbsa_2021, "2021")
assign_2024 <- assign_county_to_cbsa(ct_counties_2021, cbsa_2024, "2024")

# Build final CT county -> CBSA crosswalk
ct_county_cbsa_assignment <- ct_counties_2021 %>%
  left_join(
    assign_2021 %>% select(county_geoid_2021, county_name_2021, county_in_cbsa_2021, cbsa_2021_geoid, cbsa_2021_name),
    by = c("county_geoid_2021", "county_name_2021")
  ) %>%
  left_join(
    assign_2024 %>% select(county_geoid_2021, county_name_2021, county_in_cbsa_2024, cbsa_2024_geoid, cbsa_2024_name),
    by = c("county_geoid_2021", "county_name_2021")
  )

st_geometry(ct_county_cbsa_assignment) <- "county_geometry_2021"
glimpse_data(ct_county_cbsa_assignment, "FINAL: CT County -> CBSA (2021 & 2024)")

old_ct_county_to_new_ct_cbsa <- ct_county_cbsa_assignment

# Verification checks
ct_county_cbsa_2021_check <- tigris::counties(state = "CT", year = 2021, cb = FALSE, class = "sf") %>%
  dplyr::select(GEOID, NAMELSAD, CBSAFP) %>%
  dplyr::mutate(county_in_cbsa_2021 = ifelse(CBSAFP != "", TRUE, FALSE)) %>%
  dplyr::rename(county_geoid_2021 = GEOID, county_name_2021 = NAMELSAD, cbsa_2021_geoid = CBSAFP)

ct_county_cbsa_2024_check <- tigris::counties(state = "CT", year = 2024, cb = FALSE, class = "sf") %>%
  dplyr::select(GEOID, NAMELSAD, CBSAFP) %>%
  dplyr::mutate(county_in_cbsa_2024 = ifelse(CBSAFP != "", TRUE, FALSE)) %>%
  dplyr::rename(county_geoid_2024 = GEOID, county_name_2024 = NAMELSAD, cbsa_2024_geoid = CBSAFP)

glimpse_data(ct_county_cbsa_2024_check, "CT Counties 2024 Check")

# ==============================================================================
# SECTION 4: CT EMPLOYMENT CROSSWALK BY NAICS SECTOR (LODES-BASED)
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("SECTION 4: CT EMPLOYMENT CROSSWALK BY NAICS SECTOR\n")
cat(strrep("=", 80), "\n\n")

# Constants
CT_STATE_ABBR <- "CT"
LODES_YEARS_CT <- 2010:2023
TARGET_CRS_CT <- 5070  # NAD83 / Conus Albers

NAICS_SECTOR_MAP <- tibble::tribble(
  ~cns_code, ~naics_sector, ~sector_name,
  "C000",  "00",    "Total, All Industries",
  "CNS01", "11",    "Agriculture, Forestry, Fishing and Hunting",
  "CNS02", "21",    "Mining, Quarrying, and Oil and Gas Extraction",
  "CNS03", "22",    "Utilities",
  "CNS04", "23",    "Construction",
  "CNS05", "31-33", "Manufacturing",
  "CNS06", "42",    "Wholesale Trade",
  "CNS07", "44-45", "Retail Trade",
  "CNS08", "48-49", "Transportation and Warehousing",
  "CNS09", "51",    "Information",
  "CNS10", "52",    "Finance and Insurance",
  "CNS11", "53",    "Real Estate and Rental and Leasing",
  "CNS12", "54",    "Professional, Scientific, and Technical Services",
  "CNS13", "55",    "Management of Companies and Enterprises",
  "CNS14", "56",    "Admin Support and Waste Mgmt and Remediation",
  "CNS15", "61",    "Educational Services",
  "CNS16", "62",    "Health Care and Social Assistance",
  "CNS17", "71",    "Arts, Entertainment, and Recreation",
  "CNS18", "72",    "Accommodation and Food Services",
  "CNS19", "81",    "Other Services (except Public Administration)",
  "CNS20", "92",    "Public Administration"
)

# Step 1: Download 2020 CT Census Blocks
cat("=== CT STEP 1: Download 2020 CT Census Blocks ===\n")

ct_blocks_2020 <- tigris::blocks(state = CT_STATE_ABBR, year = 2020, class = "sf")
blk_col <- intersect(c("GEOID20", "GEOID10", "GEOID"), names(ct_blocks_2020))[1]
cat("  Block ID column:", blk_col, "\n")

ct_census_blocks <- ct_blocks_2020 %>%
  dplyr::select(all_of(blk_col), geometry) %>%
  dplyr::rename(block_geoid = all_of(blk_col)) %>%
  dplyr::mutate(block_geoid = as.character(block_geoid)) %>%
  sf::st_transform(crs = TARGET_CRS_CT)

cat("  Total blocks:", nrow(ct_census_blocks), "\n")
N_CT_BLOCKS <- nrow(ct_census_blocks)

# Step 2: Download LODES WAC Data
cat("\n=== CT STEP 2: Download LODES WAC Data (2010-2023) ===\n")

read_gz <- function(url) {
  tf <- tempfile(fileext = ".csv.gz")
  curl::curl_download(url, tf, quiet = TRUE)
  readr::read_csv(
    tf,
    show_col_types = FALSE,
    progress = FALSE,
    col_types = readr::cols(w_geocode = readr::col_character(), .default = readr::col_double())
  )
}

wac_base_url_ct <- "https://lehd.ces.census.gov/data/lodes/LODES8/ct/wac/"

wac_all_years_ct <- dplyr::bind_rows(lapply(LODES_YEARS_CT, function(yr) {
  cat("  Downloading year:", yr, "\n")
  f <- sprintf("ct_wac_S000_JT00_%d.csv.gz", yr)
  w <- tryCatch(read_gz(paste0(wac_base_url_ct, f)), error = function(e) NULL)
  if (is.null(w) || !("w_geocode" %in% names(w))) return(NULL)
  
  cols_to_keep <- c("w_geocode", "C000", paste0("CNS", sprintf("%02d", 1:20)))
  cols_present <- intersect(cols_to_keep, names(w))
  
  w %>%
    dplyr::select(all_of(cols_present)) %>%
    dplyr::rename(block_geoid = w_geocode) %>%
    dplyr::mutate(block_geoid = as.character(block_geoid), year = as.integer(yr))
}))

cat("  Total LODES records:", nrow(wac_all_years_ct), "\n")

# Rename columns to NAICS sector codes
cns_rename_map <- setNames(NAICS_SECTOR_MAP$cns_code, NAICS_SECTOR_MAP$naics_sector)
present_cns_codes <- intersect(as.character(cns_rename_map), names(wac_all_years_ct))
cns_rename_map_filtered <- cns_rename_map[cns_rename_map %in% present_cns_codes]

wac_all_years_ct <- wac_all_years_ct %>%
  dplyr::rename(!!!cns_rename_map_filtered)

# Step 3: Download County/Planning Region Shapefiles
cat("\n=== CT STEP 3: Download County Shapefiles ===\n")

connecticut_old_counties <- tigris::counties(state = CT_STATE_ABBR, year = 2020, class = "sf") %>%
  dplyr::select(GEOID, NAMELSAD) %>%
  dplyr::rename(old_county_geoid = GEOID, old_county_name = NAMELSAD) %>%
  sf::st_transform(crs = TARGET_CRS_CT)

connecticut_planning_regions <- tigris::counties(state = CT_STATE_ABBR, year = 2024, class = "sf") %>%
  dplyr::select(GEOID, NAMELSAD) %>%
  dplyr::rename(new_region_geoid = GEOID, new_region_name = NAMELSAD) %>%
  sf::st_transform(crs = TARGET_CRS_CT)

cat("  Old counties (2020):", nrow(connecticut_old_counties), "\n")
cat("  Planning regions (2024):", nrow(connecticut_planning_regions), "\n")

# Step 4: Pre-compute Block Geography Assignments
cat("\n=== CT STEP 4: Pre-compute Block Geography Assignments ===\n")

start_spatial <- Sys.time()

block_centroids_ct <- ct_census_blocks %>%
  suppressWarnings(sf::st_centroid(of_largest_polygon = TRUE))

blocks_to_old_counties <- block_centroids_ct %>%
  sf::st_join(connecticut_old_counties, join = sf::st_within, left = TRUE) %>%
  sf::st_drop_geometry() %>%
  dplyr::select(block_geoid, old_county_geoid)

blocks_to_new_regions <- block_centroids_ct %>%
  sf::st_join(connecticut_planning_regions, join = sf::st_within, left = TRUE) %>%
  sf::st_drop_geometry() %>%
  dplyr::select(block_geoid, new_region_geoid)

block_geography_ct <- blocks_to_old_counties %>%
  dplyr::left_join(blocks_to_new_regions, by = "block_geoid")

spatial_elapsed <- round(difftime(Sys.time(), start_spatial, units = "secs"), 1)
cat("  Spatial pre-computation complete in", spatial_elapsed, "seconds\n")

N_BLOCKS_WITH_GEOGRAPHY_CT <- sum(!is.na(block_geography_ct$old_county_geoid) & !is.na(block_geography_ct$new_region_geoid))
cat("  Blocks with BOTH county and region assignment:", N_BLOCKS_WITH_GEOGRAPHY_CT, "\n")

# Step 5: Create Complete Block-Year Panel
cat("\n=== CT STEP 5: Create Complete Block-Year Panel ===\n")

sector_cols <- NAICS_SECTOR_MAP$naics_sector[NAICS_SECTOR_MAP$naics_sector %in% names(wac_all_years_ct)]

blocks_with_valid_geography_ct <- block_geography_ct %>%
  dplyr::filter(!is.na(old_county_geoid), !is.na(new_region_geoid)) %>%
  dplyr::pull(block_geoid)

complete_block_year_panel_ct <- tidyr::expand_grid(
  block_geoid = blocks_with_valid_geography_ct,
  year = LODES_YEARS_CT
)

wac_complete_ct <- complete_block_year_panel_ct %>%
  dplyr::left_join(
    wac_all_years_ct %>% dplyr::select(block_geoid, year, all_of(sector_cols)),
    by = c("block_geoid", "year")
  )

for (col in sector_cols) {
  wac_complete_ct[[col]][is.na(wac_complete_ct[[col]])] <- 0
}

wac_with_geography_ct <- wac_complete_ct %>%
  dplyr::left_join(block_geography_ct, by = "block_geoid")

cat("  Final panel size:", nrow(wac_with_geography_ct), "\n")

# Step 6: Compute Allocation Factors
cat("\n=== CT STEP 6: Compute Allocation Factors ===\n")

start_agg <- Sys.time()

all_crosswalks_ct <- list()
counter <- 0
total <- length(LODES_YEARS_CT) * length(sector_cols)

for (yr in LODES_YEARS_CT) {
  for (sector in sector_cols) {
    counter <- counter + 1
    
    if (counter %% 50 == 1 || counter == total) {
      pct <- round(100 * counter / total, 1)
      cat(sprintf("\r  [%5.1f%%] Year %d, Sector %-5s (%d/%d)    ", pct, yr, sector, counter, total))
      flush.console()
    }
    
    year_data <- wac_with_geography_ct %>%
      dplyr::filter(year == yr)
    
    xwalk <- year_data %>%
      dplyr::group_by(old_county_geoid, new_region_geoid) %>%
      dplyr::summarise(sector_jobs = sum(.data[[sector]], na.rm = TRUE), .groups = "drop") %>%
      dplyr::group_by(old_county_geoid) %>%
      dplyr::mutate(
        county_sector_total = sum(sector_jobs, na.rm = TRUE),
        afact = dplyr::if_else(county_sector_total > 0, sector_jobs / county_sector_total, NA_real_)
      ) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(year = yr, naics_sector = sector) %>%
      dplyr::select(year, from_geoid = old_county_geoid, to_geoid = new_region_geoid,
                    naics_sector, from_geoid_sector_jobs_total = sector_jobs, afact)
    
    all_crosswalks_ct[[length(all_crosswalks_ct) + 1]] <- xwalk
  }
}

cat("\n")
agg_elapsed <- round(difftime(Sys.time(), start_agg, units = "secs"), 1)
cat("  Aggregation complete in", agg_elapsed, "seconds\n")

ct_employment_crosswalk_by_sector <- dplyr::bind_rows(all_crosswalks_ct) %>%
  dplyr::arrange(year, naics_sector, from_geoid, to_geoid)

cat("  Total rows in CT crosswalk:", nrow(ct_employment_crosswalk_by_sector), "\n")

# Add names
region_names_ct <- connecticut_planning_regions %>%
  sf::st_drop_geometry() %>%
  dplyr::select(new_region_geoid, new_region_name) %>%
  dplyr::rename(to_geoid = new_region_geoid, to_name = new_region_name)

county_names_ct <- connecticut_old_counties %>%
  sf::st_drop_geometry() %>%
  dplyr::select(old_county_geoid, old_county_name) %>%
  dplyr::rename(from_geoid = old_county_geoid, from_name = old_county_name)

ct_employment_crosswalk_by_sector_named <- ct_employment_crosswalk_by_sector %>%
  dplyr::left_join(county_names_ct, by = "from_geoid") %>%
  dplyr::left_join(region_names_ct, by = "to_geoid") %>%
  dplyr::left_join(NAICS_SECTOR_MAP %>% dplyr::select(naics_sector, sector_name), by = "naics_sector") %>%
  dplyr::select(year, from_geoid, from_name, to_geoid, to_name, naics_sector, sector_name, from_geoid_sector_jobs_total, afact)

glimpse_data(ct_employment_crosswalk_by_sector_named, "CT Employment Crosswalk by Sector (Named)")

# ==============================================================================
# SECTION 5: VALDEZ-CORDOVA EMPLOYMENT CROSSWALK BY NAICS SECTOR
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("SECTION 5: VALDEZ-CORDOVA EMPLOYMENT CROSSWALK BY NAICS SECTOR\n")
cat(strrep("=", 80), "\n\n")

# Constants
STATE_ABBR_LODES_AK <- "ak"
STATE_ABBR_TIGRIS_AK <- "AK"
LODES_YEARS_AK <- 2010:2016
TARGET_CRS_AK <- 3338  # Alaska Albers

OLD_COUNTY_FIPS_AK <- "02261"
NEW_REGION_FIPS_AK <- c("02063", "02066")

# Step 1: Download 2020 AK Census Blocks
cat("=== AK STEP 1: Download 2020 AK Census Blocks ===\n")

ak_blocks_2020 <- tigris::blocks(state = STATE_ABBR_TIGRIS_AK, year = 2020, class = "sf")
blk_col_ak <- intersect(c("GEOID20", "GEOID10", "GEOID"), names(ak_blocks_2020))[1]

ak_census_blocks <- ak_blocks_2020 %>%
  dplyr::select(all_of(blk_col_ak), geometry) %>%
  dplyr::rename(block_geoid = all_of(blk_col_ak)) %>%
  dplyr::mutate(block_geoid = as.character(block_geoid)) %>%
  sf::st_transform(crs = TARGET_CRS_AK)

cat("  Total AK blocks:", nrow(ak_census_blocks), "\n")

# Step 2: Download LODES WAC Data
cat("\n=== AK STEP 2: Download LODES WAC Data (2010-2016) ===\n")

wac_base_url_ak <- sprintf("https://lehd.ces.census.gov/data/lodes/LODES8/%s/wac/", STATE_ABBR_LODES_AK)

wac_all_years_ak <- dplyr::bind_rows(lapply(LODES_YEARS_AK, function(yr) {
  cat("  Downloading year:", yr, "\n")
  f <- sprintf("%s_wac_S000_JT00_%d.csv.gz", STATE_ABBR_LODES_AK, yr)
  w <- tryCatch(read_gz(paste0(wac_base_url_ak, f)), error = function(e) NULL)
  if (is.null(w) || !("w_geocode" %in% names(w))) return(NULL)
  
  cols_to_keep <- c("w_geocode", "C000", paste0("CNS", sprintf("%02d", 1:20)))
  cols_present <- intersect(cols_to_keep, names(w))
  
  w %>%
    dplyr::select(all_of(cols_present)) %>%
    dplyr::rename(block_geoid = w_geocode) %>%
    dplyr::mutate(block_geoid = as.character(block_geoid), year = as.integer(yr))
}))

cat("  Total LODES records:", nrow(wac_all_years_ak), "\n")

wac_all_years_ak <- wac_all_years_ak %>%
  dplyr::rename(!!!cns_rename_map_filtered)

# Step 3: Download County/Census Area Shapefiles
cat("\n=== AK STEP 3: Download County Shapefiles ===\n")

valdez_cordova_old <- tigris::counties(state = STATE_ABBR_TIGRIS_AK, year = 2019, class = "sf") %>%
  dplyr::filter(GEOID == OLD_COUNTY_FIPS_AK) %>%
  dplyr::select(GEOID, NAMELSAD) %>%
  dplyr::rename(old_county_geoid = GEOID, old_county_name = NAMELSAD) %>%
  sf::st_transform(crs = TARGET_CRS_AK)

chugach_copper_river_new <- tigris::counties(state = STATE_ABBR_TIGRIS_AK, year = 2024, class = "sf") %>%
  dplyr::filter(GEOID %in% NEW_REGION_FIPS_AK) %>%
  dplyr::select(GEOID, NAMELSAD) %>%
  dplyr::rename(new_region_geoid = GEOID, new_region_name = NAMELSAD) %>%
  sf::st_transform(crs = TARGET_CRS_AK)

cat("  Old county (2019 Valdez-Cordova):", nrow(valdez_cordova_old), "\n")
cat("  New census areas (2024):", nrow(chugach_copper_river_new), "\n")

# Step 4: Pre-compute Block Geography Assignments
cat("\n=== AK STEP 4: Pre-compute Block Geography Assignments ===\n")

start_spatial_ak <- Sys.time()

block_centroids_ak <- ak_census_blocks %>%
  suppressWarnings(sf::st_centroid(of_largest_polygon = TRUE))

blocks_to_old_county_ak <- block_centroids_ak %>%
  sf::st_join(valdez_cordova_old, join = sf::st_within, left = FALSE) %>%
  sf::st_drop_geometry() %>%
  dplyr::select(block_geoid, old_county_geoid)

blocks_to_new_regions_ak <- block_centroids_ak %>%
  sf::st_join(chugach_copper_river_new, join = sf::st_within, left = FALSE) %>%
  sf::st_drop_geometry() %>%
  dplyr::select(block_geoid, new_region_geoid)

block_geography_ak <- blocks_to_old_county_ak %>%
  dplyr::inner_join(blocks_to_new_regions_ak, by = "block_geoid")

spatial_elapsed_ak <- round(difftime(Sys.time(), start_spatial_ak, units = "secs"), 1)
cat("  Spatial pre-computation complete in", spatial_elapsed_ak, "seconds\n")
cat("  Blocks with both old county and new region assignment:", nrow(block_geography_ak), "\n")

N_BLOCKS_IN_VC <- nrow(block_geography_ak)

# Step 5: Create Complete Block-Year Panel
cat("\n=== AK STEP 5: Create Complete Block-Year Panel ===\n")

sector_cols_ak <- NAICS_SECTOR_MAP$naics_sector[NAICS_SECTOR_MAP$naics_sector %in% names(wac_all_years_ak)]

complete_block_year_panel_ak <- tidyr::expand_grid(
  block_geoid = block_geography_ak$block_geoid,
  year = LODES_YEARS_AK
)

wac_complete_ak <- complete_block_year_panel_ak %>%
  dplyr::left_join(
    wac_all_years_ak %>% dplyr::select(block_geoid, year, all_of(sector_cols_ak)),
    by = c("block_geoid", "year")
  )

for (col in sector_cols_ak) {
  wac_complete_ak[[col]][is.na(wac_complete_ak[[col]])] <- 0
}

wac_with_geography_ak <- wac_complete_ak %>%
  dplyr::left_join(block_geography_ak, by = "block_geoid")

cat("  Final panel size:", nrow(wac_with_geography_ak), "\n")

# Step 6: Compute Allocation Factors
cat("\n=== AK STEP 6: Compute Allocation Factors ===\n")

start_agg_ak <- Sys.time()

all_crosswalks_ak <- list()
counter_ak <- 0
total_ak <- length(LODES_YEARS_AK) * length(sector_cols_ak)

for (yr in LODES_YEARS_AK) {
  for (sector in sector_cols_ak) {
    counter_ak <- counter_ak + 1
    
    if (counter_ak %% 20 == 1 || counter_ak == total_ak) {
      pct <- round(100 * counter_ak / total_ak, 1)
      cat(sprintf("\r  [%5.1f%%] Year %d, Sector %-5s (%d/%d)    ", pct, yr, sector, counter_ak, total_ak))
      flush.console()
    }
    
    year_data <- wac_with_geography_ak %>%
      dplyr::filter(year == yr)
    
    xwalk <- year_data %>%
      dplyr::group_by(old_county_geoid, new_region_geoid) %>%
      dplyr::summarise(sector_jobs = sum(.data[[sector]], na.rm = TRUE), .groups = "drop") %>%
      dplyr::group_by(old_county_geoid) %>%
      dplyr::mutate(
        county_sector_total = sum(sector_jobs, na.rm = TRUE),
        afact = dplyr::if_else(county_sector_total > 0, sector_jobs / county_sector_total, NA_real_)
      ) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(year = yr, naics_sector = sector) %>%
      dplyr::select(year, from_geoid = old_county_geoid, to_geoid = new_region_geoid,
                    naics_sector, from_geoid_sector_jobs_total = sector_jobs, afact)
    
    all_crosswalks_ak[[length(all_crosswalks_ak) + 1]] <- xwalk
  }
}

cat("\n")
agg_elapsed_ak <- round(difftime(Sys.time(), start_agg_ak, units = "secs"), 1)
cat("  Aggregation complete in", agg_elapsed_ak, "seconds\n")

valdez_cordova_crosswalk_by_sector <- dplyr::bind_rows(all_crosswalks_ak) %>%
  dplyr::arrange(year, naics_sector, from_geoid, to_geoid)

cat("  Total rows in AK crosswalk:", nrow(valdez_cordova_crosswalk_by_sector), "\n")

# Add names
region_names_ak <- tibble::tribble(
  ~to_geoid, ~to_name,
  "02063", "Chugach Census Area",
  "02066", "Copper River Census Area"
)

valdez_cordova_crosswalk_by_sector_named <- valdez_cordova_crosswalk_by_sector %>%
  dplyr::left_join(region_names_ak, by = "to_geoid") %>%
  dplyr::left_join(NAICS_SECTOR_MAP %>% dplyr::select(naics_sector, sector_name), by = "naics_sector") %>%
  dplyr::mutate(from_name = "Valdez-Cordova Census Area") %>%
  dplyr::select(year, from_geoid, from_name, to_geoid, to_name, naics_sector, sector_name, from_geoid_sector_jobs_total, afact)

glimpse_data(valdez_cordova_crosswalk_by_sector_named, "Valdez-Cordova Employment Crosswalk by Sector (Named)")

# ==============================================================================
# SECTION 6: BUILD BASE COUNTY-STATE-CBSA-CSA-CZ CROSSWALK
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("SECTION 6: BUILD BASE COUNTY-STATE-CBSA-CSA-CZ CROSSWALK\n")
cat(strrep("=", 80), "\n\n")

# Load states data
states_data <- tigris::states(year = 2024) %>%
  sf::st_drop_geometry() %>%
  dplyr::filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>%
  dplyr::transmute(state_fips = STATEFP, state_name = NAME)

# Load commuting zone crosswalk from USDA ERS
cat("[STEP 1] Loading county-to-CZ crosswalk from USDA ERS...\n")

cz_url <- "https://www.ers.usda.gov/media/6968/2020-commuting-zones.csv?v=56155"
cz_raw <- readr::read_csv(cz_url, show_col_types = FALSE)

county_cz_xwalk_2020 <- cz_raw %>%
  dplyr::transmute(
    county_geoid = FIPStxt,
    commuting_zone_2020 = as.integer(CZ2020),
    commuting_zone_2020_name = clean_cz_name(CZName)
  )

cat(sprintf("  County-CZ crosswalk loaded: %d rows\n", nrow(county_cz_xwalk_2020)))
cat(sprintf("  Unique commuting zones: %d\n\n", length(unique(county_cz_xwalk_2020$commuting_zone_2020))))

# Build integrated crosswalk
cat("[STEP 2] Building integrated county crosswalk...\n")

tigris_county_cbsa_csa_cz_state_2024 <- tigris::counties(year = 2024, cb = FALSE) %>%
  dplyr::filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>%
  dplyr::select(STATEFP, COUNTYFP, GEOID, NAMELSAD, CBSAFP, CSAFP, geometry) %>%
  dplyr::rename(
    state_fips = STATEFP,
    county_fips = COUNTYFP,
    county_geoid = GEOID,
    county_name = NAMELSAD,
    cbsa_geoid = CBSAFP,
    csa_geoid = CSAFP
  ) %>%
  dplyr::mutate(
    county_in_cbsa = !is.na(cbsa_geoid) & cbsa_geoid != "",
    county_in_csa = !is.na(csa_geoid) & csa_geoid != ""
  ) %>%
  dplyr::left_join(
    tigris::core_based_statistical_areas(year = 2024) %>%
      sf::st_drop_geometry() %>%
      dplyr::transmute(cbsa_geoid = CBSAFP, cbsa_name = NAMELSAD),
    by = "cbsa_geoid"
  ) %>%
  dplyr::left_join(
    tigris::combined_statistical_areas(year = 2024) %>%
      sf::st_drop_geometry() %>%
      dplyr::transmute(csa_geoid = CSAFP, csa_name = NAMELSAD),
    by = "csa_geoid"
  ) %>%
  dplyr::left_join(states_data, by = "state_fips") %>%
  dplyr::left_join(county_cz_xwalk_2020, by = "county_geoid")

# Check for missing CZ assignments
missing_cz <- tigris_county_cbsa_csa_cz_state_2024 %>%
  sf::st_drop_geometry() %>%
  dplyr::filter(is.na(commuting_zone_2020))

cat(sprintf("  Integrated crosswalk created: %d rows\n", nrow(tigris_county_cbsa_csa_cz_state_2024)))
cat(sprintf("  Counties with CZ assignment: %d (%.1f%%)\n",
            sum(!is.na(tigris_county_cbsa_csa_cz_state_2024$commuting_zone_2020)),
            100 * mean(!is.na(tigris_county_cbsa_csa_cz_state_2024$commuting_zone_2020))))

if (nrow(missing_cz) > 0) {
  cat(sprintf("\n  Warning: %d counties missing CZ assignment\n", nrow(missing_cz)))
}

# Final crosswalk
county_cbsa_csa_cz_state_crosswalk <- tigris_county_cbsa_csa_cz_state_2024

glimpse_data(county_cbsa_csa_cz_state_crosswalk, "Final County-CBSA-CSA-CZ-State Crosswalk")

# ==============================================================================
# SECTION 7: LOAD AND PROCESS TAPESTRY DATA
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("SECTION 7: LOAD AND PROCESS TAPESTRY DATA\n")
cat(strrep("=", 80), "\n\n")

# Set up parallel processing
n_workers_tapestry <- max(1, future::availableCores() - 1)
cat(sprintf("  Setting up parallel processing with %d workers.\n", n_workers_tapestry))
future::plan(future::multisession, workers = n_workers_tapestry)

# ==============================================================================
# Tapestry Data Loading Function
# ==============================================================================

load_tapestry_data <- function(
    year,
    tapestry_base_path_arg,
    ct_xwalk,
    ak_xwalk,
    naics_sector_map,
    old_ct_cbsa,
    new_ct_cbsa
) {
  
  # Initialize
  debug_log <- character(0)
  lq_detail_table <- NULL
  
  dbg <- function(fmt, ...) {
    msg <- tryCatch(sprintf(fmt, ...), error = function(e) paste(fmt, collapse = " "))
    full_msg <- sprintf("[%s][Year %d] %s", format(Sys.time(), "%H:%M:%S"), year, msg)
    message(full_msg)
    debug_log <<- c(debug_log, full_msg)
  }
  
  dbg("=== Starting Tapestry data load ===")
  
  # Geographic conversion constants
  CT_OLD_COUNTIES <- c("09001", "09003", "09005", "09007", "09009", "09011", "09013", "09015")
  CT_NEW_PLANNING_REGIONS <- c("09110", "09120", "09130", "09140", "09150", "09160", "09170", "09180", "09190")
  AK_VALDEZ_CORDOVA <- "02261"
  AK_CHUGACH <- "02063"
  AK_COPPER_RIVER <- "02066"
  
  needs_ct_conversion <- (year >= 2010 && year <= 2023)
  needs_ak_vc_conversion <- (year >= 2010 && year <= 2019)
  
  dbg("Conversion flags: CT=%s, AK_VC=%s", needs_ct_conversion, needs_ak_vc_conversion)
  
  # STEP 1: READ RAW DATA
  tapestry_file <- file.path(tapestry_base_path_arg, sprintf("%d.csv", year))
  dbg("Reading file: %s", tapestry_file)
  
  dt_raw <- readr::read_csv(
    tapestry_file,
    col_types = readr::cols_only(
      year = readr::col_integer(),
      area_fips = readr::col_character(),
      naics_code = readr::col_character(),
      tap_emplvl_est_3 = readr::col_integer()
    ),
    progress = FALSE
  )
  
  dbg("Raw data read: %d rows", nrow(dt_raw))
  
  # STEP 2: BASIC CLEANUP
  dt <- dt_raw %>%
    dplyr::rename(county_geoid = area_fips) %>%
    dplyr::mutate(county_geoid = stringr::str_pad(county_geoid, width = 5, side = "left", pad = "0"))
  
  # STEP 3: ALASKA WADE HAMPTON -> KUSILVAK RECODE
  has_wade_hampton <- any(dt$county_geoid == "02270", na.rm = TRUE)
  has_kusilvak <- any(dt$county_geoid == "02158", na.rm = TRUE)
  
  if (year == 2015) {
    if (has_wade_hampton && has_kusilvak) {
      n_before <- nrow(dt)
      dt <- dt %>% dplyr::filter(county_geoid != "02270")
      dbg("Year 2015: Dropped %d Wade Hampton (02270) rows", n_before - nrow(dt))
    }
  } else {
    if (has_wade_hampton && !has_kusilvak) {
      n_recoded <- sum(dt$county_geoid == "02270", na.rm = TRUE)
      dt <- dt %>% dplyr::mutate(county_geoid = dplyr::if_else(county_geoid == "02270", "02158", county_geoid))
      dbg("Recoded %d rows: Wade Hampton (02270) -> Kusilvak (02158)", n_recoded)
    }
  }
  
  # STEP 4: OTHER GEOID CORRECTIONS
  n_shannon <- sum(dt$county_geoid == "46113", na.rm = TRUE)
  n_bedford <- sum(dt$county_geoid == "51515", na.rm = TRUE)
  
  dt <- dt %>%
    dplyr::mutate(
      county_geoid = dplyr::if_else(county_geoid == "46113", "46102", county_geoid),
      county_geoid = dplyr::if_else(county_geoid == "51515", "51019", county_geoid)
    )
  
  if (n_shannon > 0) dbg("Recoded %d rows: Shannon County (46113) -> Oglala Lakota (46102)", n_shannon)
  if (n_bedford > 0) dbg("Recoded %d rows: Bedford City (51515) -> Bedford County (51019)", n_bedford)
  
  # STEP 5: COLLAPSE DUPLICATES
  n_before_collapse <- nrow(dt)
  dt <- dt %>%
    dplyr::group_by(year, county_geoid, naics_code) %>%
    dplyr::summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(
      state_fips = stringr::str_sub(county_geoid, 1, 2),
      county_fips = stringr::str_sub(county_geoid, 3, 5),
      unknown_undefined_county = (county_fips == "999")
    ) %>%
    dplyr::rename(naics6_code = naics_code, naics6_employment_county = tap_emplvl_est_3)
  
  dbg("After initial collapse: %d rows (was %d)", nrow(dt), n_before_collapse)
  
  # STEP 6: CAPTURE PRE-CONVERSION TOTALS
  pre_state_totals <- dt %>%
    dplyr::group_by(state_fips, naics6_code) %>%
    dplyr::summarise(pre_emp = sum(naics6_employment_county, na.rm = TRUE), .groups = "drop")
  
  pre_national_total <- sum(dt$naics6_employment_county, na.rm = TRUE)
  dbg("Pre-conversion national total employment: %s", format(pre_national_total, big.mark = ","))
  
  pre_ct_data <- NULL
  if (needs_ct_conversion) {
    pre_ct_data <- dt %>% dplyr::filter(state_fips == "09", !unknown_undefined_county)
  }
  
  pre_ak_vc_data <- NULL
  if (needs_ak_vc_conversion) {
    pre_ak_vc_data <- dt %>% dplyr::filter(county_geoid == AK_VALDEZ_CORDOVA)
  }
  
  # STEP 7: CONNECTICUT COUNTY -> PLANNING REGION CONVERSION (2010-2023)
  if (needs_ct_conversion) {
    dbg("=== Starting Connecticut conversion ===")
    
    ct_to_convert <- dt %>% dplyr::filter(county_geoid %in% CT_OLD_COUNTIES, !unknown_undefined_county)
    ct_unknown <- dt %>% dplyr::filter(state_fips == "09", unknown_undefined_county)
    non_ct_rows <- dt %>% dplyr::filter(!(state_fips == "09") | (state_fips == "09" & unknown_undefined_county))
    
    dbg("CT rows to convert: %d | CT unknown: %d | Non-CT rows: %d",
        nrow(ct_to_convert), nrow(ct_unknown), nrow(non_ct_rows))
    
    if (nrow(ct_to_convert) > 0) {
      ct_to_convert <- ct_to_convert %>%
        dplyr::left_join(
          naics_sector_map %>% dplyr::select(naics6_code, sector_code_for_allocation_mapping),
          by = "naics6_code"
        ) %>%
        dplyr::mutate(
          sector_code_for_allocation_mapping = dplyr::if_else(
            is.na(sector_code_for_allocation_mapping), "00", sector_code_for_allocation_mapping
          )
        )
      
      ct_xwalk_year <- ct_xwalk %>%
        dplyr::filter(year == !!year) %>%
        dplyr::select(from_geoid, to_geoid, naics_sector, afact)
      
      ct_xwalk_fallback <- ct_xwalk_year %>%
        dplyr::filter(naics_sector == "00") %>%
        dplyr::select(from_geoid, to_geoid, afact_fallback = afact)
      
      ct_with_factors <- ct_to_convert %>%
        dplyr::left_join(
          ct_xwalk_year,
          by = c("county_geoid" = "from_geoid", "sector_code_for_allocation_mapping" = "naics_sector"),
          relationship = "many-to-many"
        )
      
      n_na_afact <- sum(is.na(ct_with_factors$afact))
      if (n_na_afact > 0) {
        ct_with_factors <- ct_with_factors %>%
          dplyr::left_join(ct_xwalk_fallback, by = c("county_geoid" = "from_geoid", "to_geoid" = "to_geoid")) %>%
          dplyr::mutate(afact = dplyr::if_else(is.na(afact), afact_fallback, afact)) %>%
          dplyr::select(-afact_fallback)
      }
      
      ct_converted <- ct_with_factors %>%
        dplyr::filter(!is.na(afact), !is.na(to_geoid)) %>%
        dplyr::mutate(
          naics6_employment_county = round(naics6_employment_county * afact),
          county_geoid = to_geoid,
          state_fips = stringr::str_sub(to_geoid, 1, 2),
          county_fips = stringr::str_sub(to_geoid, 3, 5)
        ) %>%
        dplyr::select(year, state_fips, county_fips, county_geoid, unknown_undefined_county,
                      naics6_code, naics6_employment_county)
      
      dbg("  CT converted: %d rows created from %d original rows", nrow(ct_converted), nrow(ct_to_convert))
      
      dt <- dplyr::bind_rows(non_ct_rows, ct_unknown, ct_converted)
      dbg("  After CT conversion recombine: %d total rows", nrow(dt))
    }
  }
  
  # STEP 8: VALDEZ-CORDOVA -> CHUGACH/COPPER RIVER CONVERSION (2010-2019)
  if (needs_ak_vc_conversion) {
    dbg("=== Starting Valdez-Cordova conversion ===")
    
    ak_lookup_year <- if (year > 2016) 2016L else year
    if (ak_lookup_year != year) {
      dbg("  Using allocation factors from year %d (no data for %d)", ak_lookup_year, year)
    }
    
    ak_vc_to_convert <- dt %>% dplyr::filter(county_geoid == AK_VALDEZ_CORDOVA)
    non_ak_vc_rows <- dt %>% dplyr::filter(county_geoid != AK_VALDEZ_CORDOVA)
    
    dbg("AK Valdez-Cordova rows to convert: %d | Other rows: %d", nrow(ak_vc_to_convert), nrow(non_ak_vc_rows))
    
    if (nrow(ak_vc_to_convert) > 0) {
      ak_vc_to_convert <- ak_vc_to_convert %>%
        dplyr::left_join(
          naics_sector_map %>% dplyr::select(naics6_code, sector_code_for_allocation_mapping),
          by = "naics6_code"
        ) %>%
        dplyr::mutate(
          sector_code_for_allocation_mapping = dplyr::if_else(
            is.na(sector_code_for_allocation_mapping), "00", sector_code_for_allocation_mapping
          )
        )
      
      ak_xwalk_year <- ak_xwalk %>%
        dplyr::filter(year == ak_lookup_year) %>%
        dplyr::select(from_geoid, to_geoid, naics_sector, afact)
      
      ak_xwalk_fallback <- ak_xwalk_year %>%
        dplyr::filter(naics_sector == "00") %>%
        dplyr::select(from_geoid, to_geoid, afact_fallback = afact)
      
      ak_vc_with_factors <- ak_vc_to_convert %>%
        dplyr::left_join(
          ak_xwalk_year,
          by = c("county_geoid" = "from_geoid", "sector_code_for_allocation_mapping" = "naics_sector"),
          relationship = "many-to-many"
        )
      
      n_na_afact <- sum(is.na(ak_vc_with_factors$afact))
      if (n_na_afact > 0) {
        ak_vc_with_factors <- ak_vc_with_factors %>%
          dplyr::left_join(ak_xwalk_fallback, by = c("county_geoid" = "from_geoid", "to_geoid" = "to_geoid")) %>%
          dplyr::mutate(afact = dplyr::if_else(is.na(afact), afact_fallback, afact)) %>%
          dplyr::select(-afact_fallback)
      }
      
      ak_vc_converted <- ak_vc_with_factors %>%
        dplyr::filter(!is.na(afact), !is.na(to_geoid)) %>%
        dplyr::mutate(
          naics6_employment_county = round(naics6_employment_county * afact),
          county_geoid = to_geoid,
          state_fips = stringr::str_sub(to_geoid, 1, 2),
          county_fips = stringr::str_sub(to_geoid, 3, 5)
        ) %>%
        dplyr::select(year, state_fips, county_fips, county_geoid, unknown_undefined_county,
                      naics6_code, naics6_employment_county)
      
      dbg("  AK converted: %d rows created from %d original rows", nrow(ak_vc_converted), nrow(ak_vc_to_convert))
      
      dt <- dplyr::bind_rows(non_ak_vc_rows, ak_vc_converted)
      dbg("  After AK conversion recombine: %d total rows", nrow(dt))
    }
  }
  
  # STEP 9: COLLAPSE AFTER CONVERSIONS
  n_before_final_collapse <- nrow(dt)
  dt <- dt %>%
    dplyr::group_by(year, county_geoid, naics6_code) %>%
    dplyr::summarise(naics6_employment_county = sum(naics6_employment_county, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(
      state_fips = stringr::str_sub(county_geoid, 1, 2),
      county_fips = stringr::str_sub(county_geoid, 3, 5),
      unknown_undefined_county = (county_fips == "999")
    )
  
  if (nrow(dt) != n_before_final_collapse) {
    dbg("Post-conversion collapse: %d -> %d rows", n_before_final_collapse, nrow(dt))
  }
  
  # STEP 10: VERIFY STATE TOTALS
  post_state_totals <- dt %>%
    dplyr::group_by(state_fips, naics6_code) %>%
    dplyr::summarise(post_emp = sum(naics6_employment_county, na.rm = TRUE), .groups = "drop")
  
  state_comparison <- dplyr::full_join(pre_state_totals, post_state_totals, by = c("state_fips", "naics6_code")) %>%
    dplyr::mutate(
      pre_emp = dplyr::if_else(is.na(pre_emp), 0, pre_emp),
      post_emp = dplyr::if_else(is.na(post_emp), 0, post_emp),
      diff = post_emp - pre_emp
    )
  
  state_discrepancies <- state_comparison %>% dplyr::filter(abs(diff) > 0)
  
  if (nrow(state_discrepancies) > 0) {
    ct_discrepancies <- state_discrepancies %>% dplyr::filter(state_fips == "09")
    ak_discrepancies <- state_discrepancies %>% dplyr::filter(state_fips == "02")
    
    if (nrow(ct_discrepancies) > 0) {
      dbg("  CT state total discrepancies: %d NAICS6 codes affected", nrow(ct_discrepancies))
    }
    if (nrow(ak_discrepancies) > 0) {
      dbg("  AK state total discrepancies: %d NAICS6 codes affected", nrow(ak_discrepancies))
    }
  } else {
    dbg("  All state totals: PRESERVED (no discrepancies)")
  }
  
  # STEP 11: VERIFY NATIONAL TOTALS
  post_national_total <- sum(dt$naics6_employment_county, na.rm = TRUE)
  national_diff <- post_national_total - pre_national_total
  
  dbg("National total validation: Pre=%s, Post=%s, Diff=%d",
      format(pre_national_total, big.mark = ","),
      format(post_national_total, big.mark = ","),
      national_diff)
  
  # STEP 12: CT CBSA VALIDATION USING LOCATION QUOTIENT
  if (needs_ct_conversion && !is.null(pre_ct_data)) {
    dbg("=== CT CBSA Validation using Location Quotient ===")
    
    # Pre-conversion: Aggregate old counties to 2024 CBSAs
    pre_cbsa <- pre_ct_data %>%
      dplyr::left_join(
        old_ct_cbsa %>% sf::st_drop_geometry() %>% dplyr::select(county_geoid_2021, cbsa_2024_geoid),
        by = c("county_geoid" = "county_geoid_2021")
      ) %>%
      dplyr::group_by(cbsa_2024_geoid, naics6_code) %>%
      dplyr::summarise(naics6_emp_cbsa = sum(naics6_employment_county, na.rm = TRUE), .groups = "drop")
    
    pre_cbsa_totals <- pre_cbsa %>%
      dplyr::group_by(cbsa_2024_geoid) %>%
      dplyr::summarise(total_emp_cbsa = sum(naics6_emp_cbsa, na.rm = TRUE), .groups = "drop")
    
    pre_national_by_naics <- pre_state_totals %>%
      dplyr::group_by(naics6_code) %>%
      dplyr::summarise(naics6_emp_national = sum(pre_emp, na.rm = TRUE), .groups = "drop")
    
    pre_total_national <- sum(pre_national_by_naics$naics6_emp_national, na.rm = TRUE)
    
    pre_lq <- pre_cbsa %>%
      dplyr::left_join(pre_cbsa_totals, by = "cbsa_2024_geoid") %>%
      dplyr::left_join(pre_national_by_naics, by = "naics6_code") %>%
      dplyr::mutate(
        share_cbsa = dplyr::if_else(total_emp_cbsa > 0, naics6_emp_cbsa / total_emp_cbsa, NA_real_),
        share_national = naics6_emp_national / pre_total_national,
        pre_lq = dplyr::if_else(!is.na(share_cbsa) & !is.na(share_national) & share_national > 0,
                                share_cbsa / share_national, NA_real_)
      ) %>%
      dplyr::select(cbsa_2024_geoid, naics6_code, pre_emp = naics6_emp_cbsa, pre_lq)
    
    # Post-conversion: Aggregate new planning regions to 2024 CBSAs
    post_ct_data <- dt %>% dplyr::filter(state_fips == "09", !unknown_undefined_county)
    
    post_cbsa <- post_ct_data %>%
      dplyr::left_join(
        new_ct_cbsa %>% sf::st_drop_geometry() %>% dplyr::select(county_geoid_2024, cbsa_2024_geoid),
        by = c("county_geoid" = "county_geoid_2024")
      ) %>%
      dplyr::group_by(cbsa_2024_geoid, naics6_code) %>%
      dplyr::summarise(naics6_emp_cbsa = sum(naics6_employment_county, na.rm = TRUE), .groups = "drop")
    
    post_cbsa_totals <- post_cbsa %>%
      dplyr::group_by(cbsa_2024_geoid) %>%
      dplyr::summarise(total_emp_cbsa = sum(naics6_emp_cbsa, na.rm = TRUE), .groups = "drop")
    
    post_national_by_naics <- dt %>%
      dplyr::group_by(naics6_code) %>%
      dplyr::summarise(naics6_emp_national = sum(naics6_employment_county, na.rm = TRUE), .groups = "drop")
    
    post_total_national <- sum(post_national_by_naics$naics6_emp_national, na.rm = TRUE)
    
    post_lq <- post_cbsa %>%
      dplyr::left_join(post_cbsa_totals, by = "cbsa_2024_geoid") %>%
      dplyr::left_join(post_national_by_naics, by = "naics6_code") %>%
      dplyr::mutate(
        share_cbsa = dplyr::if_else(total_emp_cbsa > 0, naics6_emp_cbsa / total_emp_cbsa, NA_real_),
        share_national = naics6_emp_national / post_total_national,
        post_lq = dplyr::if_else(!is.na(share_cbsa) & !is.na(share_national) & share_national > 0,
                                 share_cbsa / share_national, NA_real_)
      ) %>%
      dplyr::select(cbsa_2024_geoid, naics6_code, post_emp = naics6_emp_cbsa, post_lq)
    
    # Compare
    lq_comparison <- dplyr::full_join(pre_lq, post_lq, by = c("cbsa_2024_geoid", "naics6_code")) %>%
      dplyr::mutate(
        pre_emp = dplyr::if_else(is.na(pre_emp), 0, pre_emp),
        post_emp = dplyr::if_else(is.na(post_emp), 0, post_emp),
        emp_diff = post_emp - pre_emp,
        lq_diff = dplyr::if_else(!is.na(pre_lq) & !is.na(post_lq), post_lq - pre_lq, NA_real_),
        lq_pct_change = dplyr::if_else(!is.na(pre_lq) & pre_lq > 0 & !is.na(post_lq),
                                       100 * (post_lq - pre_lq) / pre_lq, NA_real_)
      )
    
    # Summary
    cbsa_summary <- lq_comparison %>%
      dplyr::group_by(cbsa_2024_geoid) %>%
      dplyr::summarise(
        n_naics6 = dplyr::n(),
        pre_total_emp = sum(pre_emp, na.rm = TRUE),
        post_total_emp = sum(post_emp, na.rm = TRUE),
        emp_diff = sum(emp_diff, na.rm = TRUE),
        mean_lq_diff = mean(lq_diff, na.rm = TRUE),
        .groups = "drop"
      )
    
    dbg("CT CBSA LQ Summary: %d CBSAs validated", nrow(cbsa_summary))
    
    lq_detail_table <- lq_comparison %>%
      dplyr::mutate(year = year) %>%
      dplyr::select(year, cbsa_2024_geoid, naics6_code, pre_emp, post_emp, emp_diff, pre_lq, post_lq, lq_diff, lq_pct_change) %>%
      dplyr::arrange(cbsa_2024_geoid, naics6_code)
  }
  
  # STEP 13: COMPUTE FINAL AGGREGATES
  dbg("Computing final aggregate columns...")
  
  dt <- dt %>%
    dplyr::group_by(state_fips, naics6_code) %>%
    dplyr::mutate(naics6_employment_estimated_state = sum(naics6_employment_county, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(naics6_code) %>%
    dplyr::mutate(naics6_employment_estimated_nation = sum(naics6_employment_county, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(county_geoid) %>%
    dplyr::mutate(total_employment_estimated_county = sum(naics6_employment_county, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(state_fips) %>%
    dplyr::mutate(total_employment_estimated_state = sum(naics6_employment_county, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(total_employment_estimated_nation = sum(naics6_employment_county, na.rm = TRUE)) %>%
    dplyr::select(year, state_fips, county_fips, county_geoid, unknown_undefined_county,
                  naics6_code, naics6_employment_county,
                  naics6_employment_estimated_state, naics6_employment_estimated_nation,
                  total_employment_estimated_county, total_employment_estimated_state, total_employment_estimated_nation)
  
  # STEP 14: FINAL VALIDATION
  unique_counties <- unique(dt$county_geoid)
  n_counties <- length(unique_counties)
  
  if (needs_ct_conversion) {
    remaining_old_ct <- unique_counties[unique_counties %in% CT_OLD_COUNTIES]
    if (length(remaining_old_ct) > 0) {
      warning(sprintf("Year %d: Old CT counties still present: %s", year, paste(remaining_old_ct, collapse = ", ")))
    } else {
      dbg("Verification: No old CT counties remain (conversion successful)")
    }
  }
  
  if (needs_ak_vc_conversion) {
    if (AK_VALDEZ_CORDOVA %in% unique_counties) {
      warning(sprintf("Year %d: Valdez-Cordova (02261) still present!", year))
    } else {
      dbg("Verification: Valdez-Cordova (02261) converted successfully")
    }
  }
  
  dbg("=== Completed: %d rows, %d unique counties ===", nrow(dt), n_counties)
  
  list(
    data = dt,
    lq_table = lq_detail_table,
    debug_log = debug_log,
    year = year
  )
}

# ==============================================================================
# Process all Tapestry years
# ==============================================================================

tapestry_years <- 2010:2024

year_chunks <- split(tapestry_years, ceiling(seq_along(tapestry_years) / 2))
n_chunks <- length(year_chunks)

cat(sprintf("  Loading Tapestry data for years %d-%d in %d chunks...\n",
            min(tapestry_years), max(tapestry_years), n_chunks))

tapestry_load_start <- Sys.time()
tapestry_list <- list()

for (chunk_idx in seq_along(year_chunks)) {
  chunk_years <- year_chunks[[chunk_idx]]
  chunk_start <- Sys.time()
  
  cat(sprintf("\n  [Chunk %d/%d] Processing years: %s\n",
              chunk_idx, n_chunks, paste(chunk_years, collapse = ", ")))
  
  chunk_results <- furrr::future_map(
    .x = chunk_years,
    .f = function(yr) {
      load_tapestry_data(
        year = yr,
        tapestry_base_path_arg = tapestry_base_path,
        ct_xwalk = ct_employment_crosswalk_by_sector_named,
        ak_xwalk = valdez_cordova_crosswalk_by_sector_named,
        naics_sector_map = naics6_to_sector_mapping,
        old_ct_cbsa = old_ct_county_to_new_ct_cbsa,
        new_ct_cbsa = ct_county_cbsa_2024_check
      )
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = FALSE
  )
  
  names(chunk_results) <- paste0("tapestry_", chunk_years)
  tapestry_list <- c(tapestry_list, chunk_results)
  
  chunk_elapsed <- as.numeric(difftime(Sys.time(), chunk_start, units = "secs"))
  cat(sprintf("  [Chunk %d/%d] Completed in %.1f seconds\n", chunk_idx, n_chunks, chunk_elapsed))
}

tapestry_load_end <- Sys.time()
total_elapsed <- as.numeric(difftime(tapestry_load_end, tapestry_load_start, units = "secs"))
cat(sprintf("\n  All Tapestry years loaded in %.2f seconds.\n", total_elapsed))

# Extract results
cat("\n  Extracting and assigning results...\n")
for (yr in tapestry_years) {
  result <- tapestry_list[[paste0("tapestry_", yr)]]
  assign(paste0("tapestry_", yr), result$data, envir = .GlobalEnv)
  
  if (!is.null(result$lq_table)) {
    assign(paste0("ct_cbsa_lq_comparison_", yr), result$lq_table, envir = .GlobalEnv)
  }
}

# ==============================================================================
# SECTION 8: PROCESS TAPESTRY DATA FOR ALL GEOGRAPHIES (INCLUDING CZ)
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("SECTION 8: PROCESS TAPESTRY DATA FOR ALL GEOGRAPHIES\n")
cat(strrep("=", 80), "\n\n")

process_tapestry_year <- function(tapestry_dt, tigris_join_cols) {
  if (!inherits(tapestry_dt, "data.table")) data.table::setDT(tapestry_dt)
  
  tapestry_unknown_undefined <- data.table::copy(tapestry_dt[unknown_undefined_county == TRUE])
  tapestry_unknown_undefined[, `:=`(
    county_in_cbsa = FALSE,
    county_in_csa = FALSE,
    cbsa_geoid = NA_character_,
    csa_geoid = NA_character_,
    commuting_zone_2020 = NA_integer_
  )]
  
  tapestry_real_counties <- data.table::copy(tapestry_dt[unknown_undefined_county == FALSE])
  tapestry_real_counties[
    tigris_join_cols,
    on = "county_geoid",
    `:=`(
      county_in_cbsa = i.county_in_cbsa,
      cbsa_geoid = i.cbsa_geoid,
      county_in_csa = i.county_in_csa,
      csa_geoid = i.csa_geoid,
      commuting_zone_2020 = i.commuting_zone_2020
    )
  ]
  
  tapestry_cbsa_csa_intermediate <- data.table::rbindlist(
    list(tapestry_unknown_undefined, tapestry_real_counties),
    use.names = TRUE, fill = TRUE
  )
  data.table::setorder(tapestry_cbsa_csa_intermediate, year, county_geoid, naics6_code)
  
  tapestry_with_totals <- data.table::copy(tapestry_cbsa_csa_intermediate)
  tapestry_with_totals[, naics6_employment_estimated_cbsa := sum(naics6_employment_county, na.rm = TRUE), by = .(cbsa_geoid, naics6_code)]
  tapestry_with_totals[, naics6_employment_estimated_csa := sum(naics6_employment_county, na.rm = TRUE), by = .(csa_geoid, naics6_code)]
  tapestry_with_totals[, naics6_employment_estimated_cz := sum(naics6_employment_county, na.rm = TRUE), by = .(commuting_zone_2020, naics6_code)]
  tapestry_with_totals[, total_employment_estimated_cbsa := sum(naics6_employment_county, na.rm = TRUE), by = .(cbsa_geoid)]
  tapestry_with_totals[, total_employment_estimated_csa := sum(naics6_employment_county, na.rm = TRUE), by = .(csa_geoid)]
  tapestry_with_totals[, total_employment_estimated_cz := sum(naics6_employment_county, na.rm = TRUE), by = .(commuting_zone_2020)]
  
  tapestry_with_lq <- data.table::copy(tapestry_with_totals[unknown_undefined_county == FALSE])
  tapestry_with_lq[, unknown_undefined_county := NULL]
  tapestry_with_lq[, national_industry_share := naics6_employment_estimated_nation / total_employment_estimated_nation, by = naics6_code]
  tapestry_with_lq[, `:=`(
    naics6_location_quotient_county = (naics6_employment_county / total_employment_estimated_county) / national_industry_share,
    naics6_location_quotient_state = (naics6_employment_estimated_state / total_employment_estimated_state) / national_industry_share,
    naics6_location_quotient_cbsa = (naics6_employment_estimated_cbsa / total_employment_estimated_cbsa) / national_industry_share,
    naics6_location_quotient_csa = (naics6_employment_estimated_csa / total_employment_estimated_csa) / national_industry_share,
    naics6_location_quotient_cz = (naics6_employment_estimated_cz / total_employment_estimated_cz) / national_industry_share,
    # Employment share calculations
    naics6_employment_share_county = naics6_employment_county / total_employment_estimated_county,
    naics6_employment_share_state = naics6_employment_estimated_state / total_employment_estimated_state,
    naics6_employment_share_cbsa = naics6_employment_estimated_cbsa / total_employment_estimated_cbsa,
    naics6_employment_share_csa = naics6_employment_estimated_csa / total_employment_estimated_csa,
    naics6_employment_share_cz = naics6_employment_estimated_cz / total_employment_estimated_cz
  )]
  tapestry_with_lq[, national_industry_share := NULL]
  
  # County (geo_aggregation_code = 1)
  county_with_lq <- tapestry_with_lq[, .(year, county_geoid, naics6_code, naics6_employment_county, 
                                         naics6_location_quotient_county, naics6_employment_share_county)]
  data.table::setorder(county_with_lq, county_geoid, naics6_code)
  data.table::setnames(county_with_lq, 
                       c("county_geoid", "naics6_employment_county", "naics6_location_quotient_county", "naics6_employment_share_county"),
                       c("geoid", "naics6_employment_estimated", "naics6_location_quotient", "naics6_employment_share"))
  county_with_lq[, geo_aggregation_code := 1L]
  
  # State (geo_aggregation_code = 2)
  state_with_lq <- unique(tapestry_with_lq[, .(year, state_fips, naics6_code, naics6_employment_estimated_state, 
                                               naics6_location_quotient_state, naics6_employment_share_state)])
  data.table::setorder(state_with_lq, state_fips, naics6_code)
  data.table::setnames(state_with_lq, 
                       c("state_fips", "naics6_employment_estimated_state", "naics6_location_quotient_state", "naics6_employment_share_state"),
                       c("geoid", "naics6_employment_estimated", "naics6_location_quotient", "naics6_employment_share"))
  state_with_lq[, geo_aggregation_code := 2L]
  
  # CBSA (geo_aggregation_code = 3)
  cbsa_with_lq <- unique(tapestry_with_lq[, .(year, cbsa_geoid, naics6_code, naics6_employment_estimated_cbsa, 
                                              naics6_location_quotient_cbsa, naics6_employment_share_cbsa)])
  data.table::setorder(cbsa_with_lq, cbsa_geoid, naics6_code, na.last = TRUE)
  data.table::setnames(cbsa_with_lq, 
                       c("cbsa_geoid", "naics6_employment_estimated_cbsa", "naics6_location_quotient_cbsa", "naics6_employment_share_cbsa"),
                       c("geoid", "naics6_employment_estimated", "naics6_location_quotient", "naics6_employment_share"))
  cbsa_with_lq[, geo_aggregation_code := 3L]
  
  # CSA (geo_aggregation_code = 4)
  csa_with_lq <- unique(tapestry_with_lq[, .(year, csa_geoid, naics6_code, naics6_employment_estimated_csa, 
                                             naics6_location_quotient_csa, naics6_employment_share_csa)])
  data.table::setorder(csa_with_lq, csa_geoid, naics6_code, na.last = TRUE)
  data.table::setnames(csa_with_lq, 
                       c("csa_geoid", "naics6_employment_estimated_csa", "naics6_location_quotient_csa", "naics6_employment_share_csa"),
                       c("geoid", "naics6_employment_estimated", "naics6_location_quotient", "naics6_employment_share"))
  csa_with_lq[, geo_aggregation_code := 4L]
  
  # Commuting Zone (geo_aggregation_code = 5)
  cz_with_lq <- unique(tapestry_with_lq[, .(year, commuting_zone_2020, naics6_code, naics6_employment_estimated_cz, 
                                            naics6_location_quotient_cz, naics6_employment_share_cz)])
  data.table::setorder(cz_with_lq, commuting_zone_2020, naics6_code, na.last = TRUE)
  # Convert CZ integer to character for consistency with other geoids
  cz_with_lq[, commuting_zone_2020 := as.character(commuting_zone_2020)]
  data.table::setnames(cz_with_lq, 
                       c("commuting_zone_2020", "naics6_employment_estimated_cz", "naics6_location_quotient_cz", "naics6_employment_share_cz"),
                       c("geoid", "naics6_employment_estimated", "naics6_location_quotient", "naics6_employment_share"))
  cz_with_lq[, geo_aggregation_code := 5L]
  
  naics6_all_geos <- data.table::rbindlist(list(county_with_lq, state_with_lq, cbsa_with_lq, csa_with_lq, cz_with_lq), use.names = TRUE)
  data.table::setcolorder(naics6_all_geos, c("year", "geo_aggregation_code", "geoid", "naics6_code", 
                                             "naics6_employment_estimated", "naics6_location_quotient", "naics6_employment_share"))
  data.table::setorder(naics6_all_geos, geo_aggregation_code, geoid, naics6_code, na.last = TRUE)
  
  return(naics6_all_geos)
}

# Prepare tigris join table (now includes commuting_zone_2020)
tigris_join_cols <- if (inherits(county_cbsa_csa_cz_state_crosswalk, "sf")) {
  data.table::as.data.table(sf::st_drop_geometry(county_cbsa_csa_cz_state_crosswalk))[
    , .(county_geoid, county_in_cbsa, cbsa_geoid, county_in_csa, csa_geoid, commuting_zone_2020)
  ]
} else {
  data.table::as.data.table(county_cbsa_csa_cz_state_crosswalk)[
    , .(county_geoid, county_in_cbsa, cbsa_geoid, county_in_csa, csa_geoid, commuting_zone_2020)
  ]
}

# Process all years
years <- 2010:2024

tapestry_naics6_all_geos_list <- lapply(years, function(yr) {
  tapestry_name <- paste0("tapestry_", yr)
  
  if (!exists(tapestry_name)) {
    message(sprintf("Skipping year %d: %s not found", yr, tapestry_name))
    return(NULL)
  }
  
  message(sprintf("Processing year %d...", yr))
  tapestry_dt <- get(tapestry_name)
  process_tapestry_year(tapestry_dt, tigris_join_cols)
})

tapestry_naics6_all_geos_all_years <- data.table::rbindlist(tapestry_naics6_all_geos_list, use.names = TRUE)
data.table::setorder(tapestry_naics6_all_geos_all_years, year, geo_aggregation_code, geoid, naics6_code, na.last = TRUE)

glimpse_data(tapestry_naics6_all_geos_all_years, "tapestry_naics6_all_geos_all_years")
cat("Unique geo_aggregation_codes:", paste(unique(tapestry_naics6_all_geos_all_years$geo_aggregation_code), collapse = ", "), "\n")
cat("Unique years:", paste(unique(tapestry_naics6_all_geos_all_years$year), collapse = ", "), "\n")

# ==============================================================================
# SECTION 9: EXPORT ALL OUTPUTS
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("SECTION 9: EXPORT ALL OUTPUTS\n")
cat(strrep("=", 80), "\n\n")

cat(sprintf("Output folder: %s\n\n", output_folder))

# Create geo_aggregation_levels reference table
geo_aggregation_levels <- GEO_AGGREGATION_LEVELS

# Export geo_aggregation_levels CSV
cat("[1] Exporting geo_aggregation_levels.csv...\n")
readr::write_csv(geo_aggregation_levels, file.path(output_folder, "geo_aggregation_levels.csv"))

# Export NAICS hierarchy crosswalk CSV
cat("[2] Exporting naics_hierarchy_xwalk.csv...\n")
readr::write_csv(naics_hierarchy_xwalk, file.path(output_folder, "naics_hierarchy_xwalk.csv"))

# Export county crosswalk CSV (without geometry)
cat("[3] Exporting county_cbsa_csa_cz_state_crosswalk.csv...\n")
county_crosswalk_no_geom <- county_cbsa_csa_cz_state_crosswalk %>%
  sf::st_drop_geometry()
readr::write_csv(county_crosswalk_no_geom, file.path(output_folder, "county_cbsa_csa_cz_state_crosswalk.csv"))

# Export tapestry data by geography level and year
cat("[4] Exporting tapestry_naics6 CSVs by geography level and year...\n")

# Create lookup for geo level names
geo_level_lookup <- setNames(GEO_AGGREGATION_LEVELS$geo_aggregation_level, GEO_AGGREGATION_LEVELS$geo_aggregation_code)

# Get unique combinations
unique_geo_years <- unique(tapestry_naics6_all_geos_all_years[, .(geo_aggregation_code, year)])

n_files <- nrow(unique_geo_years)
cat(sprintf("  Exporting %d CSV files...\n", n_files))

for (i in seq_len(n_files)) {
  geo_code <- unique_geo_years$geo_aggregation_code[i]
  yr <- unique_geo_years$year[i]
  geo_name <- geo_level_lookup[as.character(geo_code)]
  
  subset_data <- tapestry_naics6_all_geos_all_years[geo_aggregation_code == geo_code & year == yr]
  
  filename <- sprintf("tapestry_naics6_%s_%d.csv", geo_name, yr)
  filepath <- file.path(output_folder, filename)
  
  readr::write_csv(subset_data, filepath)
  
  if (i %% 15 == 0 || i == n_files) {
    cat(sprintf("    [%d/%d] %s\n", i, n_files, filename))
  }
}

cat("  CSV exports complete.\n\n")

# Export main RDS file (timestamped)
cat("[5] Exporting main RDS file...\n")

rds_filename <- sprintf("tapestry_data_employment_lq_%s.rds", timestamp_str)
rds_filepath <- file.path(output_folder, rds_filename)

saveRDS(
  list(
    county_cbsa_csa_cz_state_crosswalk = county_cbsa_csa_cz_state_crosswalk,
    tapestry_naics6_all_geos_all_years = tapestry_naics6_all_geos_all_years,
    naics_hierarchy_xwalk = naics_hierarchy_xwalk,
    geo_aggregation_levels = geo_aggregation_levels
  ),
  file = rds_filepath
)

cat(sprintf("  RDS file exported: %s\n", rds_filepath))
cat(sprintf("  File size: %.2f MB\n", file.info(rds_filepath)$size / 1024^2))

# ==============================================================================
# SECTION 10: SUMMARY AND CLEANUP
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("PIPELINE COMPLETE\n")
cat(strrep("=", 80), "\n\n")

cat("Output folder contents:\n")
output_files <- list.files(output_folder)
cat(sprintf("  Total files: %d\n", length(output_files)))
cat(sprintf("  RDS file: %s\n", rds_filename))
cat(sprintf("  CSV files: %d\n", length(output_files) - 1))

cat("\nData frame summaries:\n")
cat(sprintf("  county_cbsa_csa_cz_state_crosswalk: %d rows\n", nrow(county_cbsa_csa_cz_state_crosswalk)))
cat(sprintf("  tapestry_naics6_all_geos_all_years: %d rows\n", nrow(tapestry_naics6_all_geos_all_years)))
cat(sprintf("  naics_hierarchy_xwalk: %d rows\n", nrow(naics_hierarchy_xwalk)))
cat(sprintf("  geo_aggregation_levels: %d rows\n", nrow(geo_aggregation_levels)))

cat("\nGeography aggregation codes:\n")
print(geo_aggregation_levels)

# Clean up environment
cat("\nCleaning up environment...\n")
objects_to_keep <- c(
  "county_cbsa_csa_cz_state_crosswalk",
  "tapestry_naics6_all_geos_all_years",
  "naics_hierarchy_xwalk",
  "geo_aggregation_levels",
  "output_folder",
  "timestamp_str"
)
rm(list = setdiff(ls(), objects_to_keep))
gc()

cat("Done. Final objects in environment:\n")
print(ls())

cat(sprintf("\nAll outputs saved to: %s\n", output_folder))

#Take all the CSV files in the output folder, loop through each and read, convert to parquet, and export 
csv_files <- list.files(output_folder, pattern = "\\.csv$", full.names = TRUE)
cat(sprintf("\nConverting %d CSV files to Parquet format...\n", length(csv_files)))
library(arrow)
for (csv_file in csv_files) {
  parquet_file <- sub("\\.csv$", ".parquet", csv_file)
  cat(sprintf("  Converting %s to %s...\n", basename(csv_file), basename(parquet_file)))
  
  # Read CSV
  df <- readr::read_csv(csv_file, show_col_types = FALSE)
  
  # Write Parquet
  arrow::write_parquet(df, parquet_file)
}
