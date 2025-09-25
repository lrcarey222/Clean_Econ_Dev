# ============================================================
# County–CBSA–CSA–State–Region/Division Crosswalk (2024)
# Keeps county geometry; adds robust debugging/validation.
# ============================================================

# ----------------------------
# Libraries & global options
# ----------------------------
suppressPackageStartupMessages({
  library(tidyverse)
  library(tidycensus)
  library(tigris)
  library(censusapi)
  library(readxl)
  library(sf)
})

options(tigris_use_cache = TRUE)   # cache shapefiles between runs
sf::sf_use_s2(TRUE)                # modern geodesic operations (we're not doing spatial ops here, but good hygiene)

cat("\n--- Loading TIGER/Line 2024 layers ---------------------------------\n")

# ----------------------------
# Load TIGER/Line (2024)
# ----------------------------
TIGRIS_2024_COUNTY <- tigris::counties(year = 2024, cb = FALSE, class = "sf")
cat("COUNTY rows:", nrow(TIGRIS_2024_COUNTY), "  cols:", ncol(TIGRIS_2024_COUNTY), "\n")
stopifnot(all(c("STATEFP","GEOID","NAMELSAD","CBSAFP","CSAFP","geometry") %in% names(TIGRIS_2024_COUNTY)))
glimpse(TIGRIS_2024_COUNTY, width = 80)

TIGRIS_2024_STATE <- tigris::states(year = 2024, cb = FALSE, class = "sf")
cat("STATE rows:", nrow(TIGRIS_2024_STATE), "  cols:", ncol(TIGRIS_2024_STATE), "\n")
stopifnot(all(c("STATEFP","GEOID","NAME","STUSPS","REGION","DIVISION") %in% names(TIGRIS_2024_STATE)))
glimpse(TIGRIS_2024_STATE, width = 80)

TIGRIS_2024_CBSA <- tigris::core_based_statistical_areas(year = 2024, cb = FALSE, class = "sf")
cat("CBSA rows:", nrow(TIGRIS_2024_CBSA), "  cols:", ncol(TIGRIS_2024_CBSA), "\n")
stopifnot(all(c("GEOID","NAMELSAD","MEMI","geometry") %in% names(TIGRIS_2024_CBSA)))
glimpse(TIGRIS_2024_CBSA, width = 80)

TIGRIS_2024_CSA <- tigris::combined_statistical_areas(year = 2024, cb = FALSE, class = "sf")
cat("CSA rows:", nrow(TIGRIS_2024_CSA), "  cols:", ncol(TIGRIS_2024_CSA), "\n")
stopifnot(all(c("GEOID","NAME","geometry") %in% names(TIGRIS_2024_CSA)))
glimpse(TIGRIS_2024_CSA, width = 80)

TIGRIS_2024_CENSUS_REGION <- tigris::regions(year = 2024)
cat("CENSUS REGION rows:", nrow(TIGRIS_2024_CENSUS_REGION), "  cols:", ncol(TIGRIS_2024_CENSUS_REGION), "\n")
stopifnot(all(c("REGIONCE","GEOID","NAME") %in% names(TIGRIS_2024_CENSUS_REGION)))
glimpse(TIGRIS_2024_CENSUS_REGION, width = 80)

TIGRIS_2024_CENSUS_DIVISION <- tigris::divisions(year = 2024)
cat("CENSUS DIVISION rows:", nrow(TIGRIS_2024_CENSUS_DIVISION), "  cols:", ncol(TIGRIS_2024_CENSUS_DIVISION), "\n")
stopifnot(all(c("DIVISIONCE","GEOID","NAME") %in% names(TIGRIS_2024_CENSUS_DIVISION)))
glimpse(TIGRIS_2024_CENSUS_DIVISION, width = 80)

# (Optional/extra): congressional districts (not used here but loaded per your script)
TIGRIS_2024_CONGRESSIONAL_DISTRICTS <- tigris::congressional_districts(year = 2024, cb = FALSE, class = "sf")
cat("CD rows:", nrow(TIGRIS_2024_CONGRESSIONAL_DISTRICTS), "  cols:", ncol(TIGRIS_2024_CONGRESSIONAL_DISTRICTS), "\n")
glimpse(TIGRIS_2024_CONGRESSIONAL_DISTRICTS, width = 80)

cat("\n--- Building lookup tables (drop geometry) ----------------------------\n")

# ----------------------------
# Lookups (drop geometry)
# ----------------------------
cbsa_lu <- TIGRIS_2024_CBSA |>
  st_drop_geometry() |>
  transmute(
    CBSA_GEOID = GEOID,            # CBSA code
    CBSA_Name  = NAMELSAD,         # "X Metro/Micro Area"
    MEMI                              # "1" = Metro, "2" = Micro
  )

# Defensive: make sure CBSA_GEOID is unique
stopifnot(!anyDuplicated(cbsa_lu$CBSA_GEOID))
cat("CBSA LU unique keys:", length(unique(cbsa_lu$CBSA_GEOID)), "\n")

csa_lu <- TIGRIS_2024_CSA |>
  st_drop_geometry() |>
  transmute(
    CSA_GEOID = GEOID,
    CSA_Name  = NAME
  )
stopifnot(!anyDuplicated(csa_lu$CSA_GEOID))
cat("CSA LU unique keys:", length(unique(csa_lu$CSA_GEOID)), "\n")

region_lu <- TIGRIS_2024_CENSUS_REGION |>
  st_drop_geometry() |>
  transmute(
    REGION                = REGIONCE,     # matches TIGRIS_2024_STATE$REGION
    Census_Region_GEOID   = GEOID,
    Census_Region_Name    = NAME
  )
stopifnot(!anyDuplicated(region_lu$REGION))

division_lu <- TIGRIS_2024_CENSUS_DIVISION |>
  st_drop_geometry() |>
  transmute(
    DIVISION                = DIVISIONCE, # matches TIGRIS_2024_STATE$DIVISION
    Census_Division_GEOID   = GEOID,
    Census_Division_Name    = NAME
  )
stopifnot(!anyDuplicated(division_lu$DIVISION))

cat("\n--- State attributes (non-sf) ----------------------------------------\n")

# ----------------------------
# State-level attributes (drop geometry to avoid sf<- join error)
# ----------------------------
state_attrs <- TIGRIS_2024_STATE |>
  st_drop_geometry() |>
  select(
    STATEFP,
    State_FIPS         = GEOID,
    State_Name         = NAME,
    State_Abbreviation = STUSPS,
    REGION, DIVISION
  ) |>
  left_join(region_lu,   by = "REGION") |>
  left_join(division_lu, by = "DIVISION") |>
  select(-REGION, -DIVISION)

# Quick sanity checks
stopifnot(!anyDuplicated(state_attrs$STATEFP))
stopifnot(!anyDuplicated(state_attrs$State_FIPS))
cat("State attrs rows:", nrow(state_attrs), "\n")
glimpse(state_attrs, width = 80)

cat("\n--- County base (sf) + CBSA/CSA joins --------------------------------\n")

# ----------------------------
# County base (keep geometry)
# ----------------------------
county_base <- TIGRIS_2024_COUNTY |>
  transmute(
    STATEFP,
    County_Name     = NAMELSAD,           # requested field
    County_GEOID    = GEOID,
    County_Geometry = geometry,           # keep geometry; active geometry column will be 'County_Geometry'
    CBSA_GEOID      = CBSAFP,
    CSA_GEOID       = CSAFP,
    In_CBSA         = !is.na(CBSAFP),
    In_CSA          = !is.na(CSAFP)
  )

# Ensure the active geometry column is 'County_Geometry' (good for clarity)
if (!identical(attr(st_geometry(county_base), "names")[[1]], "County_Geometry")) {
  # st_geometry() returns the active geometry list-column; set its name explicitly
  st_geometry(county_base) <- "County_Geometry"
}

# Join CBSA metadata (name + metro/micro)
county_base <- county_base |>
  left_join(cbsa_lu, by = "CBSA_GEOID") |>
  mutate(
    CBSA_Type = case_when(
      MEMI == "1" ~ "Metropolitan Area",
      MEMI == "2" ~ "Micropolitan Area",
      TRUE        ~ NA_character_
    )
  ) |>
  select(-MEMI)

# Join CSA metadata (name)
county_base <- county_base |>
  left_join(csa_lu, by = "CSA_GEOID")

# Debugging summaries
cat("County base rows:", nrow(county_base), "\n")
cat("  In_CBSA TRUE:", sum(county_base$In_CBSA, na.rm = TRUE),
    " FALSE:", sum(!county_base$In_CBSA, na.rm = TRUE), "\n")
cat("  In_CSA  TRUE:", sum(county_base$In_CSA,  na.rm = TRUE),
    " FALSE:", sum(!county_base$In_CSA,  na.rm = TRUE), "\n")

# ----------------------------
# Final crosswalk (join states as NON-sf on sf)
# ----------------------------
cat("\n--- Building final crosswalk (sf) ------------------------------------\n")

# IMPORTANT: x can be sf, y MUST NOT be sf for dplyr join (we already dropped geometry in state_attrs).
CROSSWALK_2024 <- county_base |>
  left_join(state_attrs, by = "STATEFP") |>
  select(
    State_Name,
    State_FIPS,
    State_Abbreviation,
    County_Name,
    County_GEOID,
    County_Geometry,
    In_CBSA,
    In_CSA,
    CBSA_GEOID,
    CBSA_Name,
    CBSA_Type,
    CSA_GEOID,
    CSA_Name,
    Census_Division_GEOID,
    Census_Division_Name,
    Census_Region_GEOID,
    Census_Region_Name
  )

# Validate columns present and types
req_cols <- c("State_Name","State_FIPS","State_Abbreviation","County_Name","County_GEOID",
              "County_Geometry","In_CBSA","In_CSA","CBSA_GEOID","CBSA_Name","CBSA_Type",
              "CSA_GEOID","CSA_Name","Census_Division_GEOID","Census_Division_Name",
              "Census_Region_GEOID","Census_Region_Name")
stopifnot(all(req_cols %in% names(CROSSWALK_2024)))

# Ensure the object is still sf and geometry is valid
stopifnot(inherits(CROSSWALK_2024, "sf"))
cat("Is sf:", inherits(CROSSWALK_2024, "sf"), "\n")
cat("Active geometry column:", attr(st_geometry(CROSSWALK_2024), "names")[[1]], "\n")

# Fix any invalid geometries (rare)
inv_n <- sum(!sf::st_is_valid(CROSSWALK_2024))
cat("Invalid geometries:", inv_n, "\n")
if (inv_n > 0) {
  cat("  Running st_make_valid() on invalid geometries...\n")
  CROSSWALK_2024 <- sf::st_make_valid(CROSSWALK_2024)
}

# Basic NA diagnostics
cat("\n--- NA diagnostics -----------------------------------------------------\n")
na_summary <- function(x) sum(is.na(x))
na_counts <- sapply(CROSSWALK_2024 |> st_drop_geometry(), na_summary)
print(sort(na_counts, decreasing = TRUE))

# Expected: NAs in CBSA/CSA fields for non-core-based areas
cat("Counties with no CBSA (expected in rural areas):", sum(is.na(CROSSWALK_2024$CBSA_GEOID)), "\n")
cat("Counties with no CSA  (expected):", sum(is.na(CROSSWALK_2024$CSA_GEOID)), "\n")

# Final shape & preview
cat("\n--- Final CROSSWALK_2024 summary -------------------------------------\n")
cat("Rows:", nrow(CROSSWALK_2024), "  Columns:", ncol(CROSSWALK_2024), "\n")
glimpse(CROSSWALK_2024, width = 100)

# Optional: sort for readability
CROSSWALK_2024 <- CROSSWALK_2024 |>
  arrange(State_FIPS, County_GEOID)

# Optional: quick spot-check
cat("\nGlimpse:\n")
glimpse(CROSSWALK_2024)

BNEF_DATA_CENTER_LOCATIONS <- read_excel("~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/BNEF/2025-08-08 - Global Data Center Live IT Capacity Database.xlsx", sheet = "Data Centers", skip = 7) %>%
  #Filter for rows where "Market" is "US"
  filter(`Market` == "US")
glimpse(BNEF_DATA_CENTER_LOCATIONS)

# ============================================================
# Geocode BNEF data centers to County (via CROSSWALK_2024) and CD (119th)
# Produces: BNEF_WITH_GEO (sf) and BNEF_WITH_GEO_TBL (non-sf)
# ============================================================

cat("\n--- Preparing sf points for BNEF data centers ------------------------\n")

# Keep only rows with coordinates
BNEF_POINTS <- BNEF_DATA_CENTER_LOCATIONS %>%
  filter(!is.na(Latitude), !is.na(Longitude)) %>%
  # keep original lon/lat columns for convenience; add geometry
  st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326, remove = FALSE)

cat("BNEF rows with coords:", nrow(BNEF_POINTS), "of", nrow(BNEF_DATA_CENTER_LOCATIONS), "\n")

# Make sure CROSSWALK geometry is active & in same CRS
if (!inherits(CROSSWALK_2024, "sf")) stop("CROSSWALK_2024 lost sf class unexpectedly.")
if (!"County_Geometry" %in% names(CROSSWALK_2024)) stop("County_Geometry column missing from CROSSWALK_2024.")
st_geometry(CROSSWALK_2024) <- "County_Geometry"

if (is.na(st_crs(CROSSWALK_2024))) {
  # TIGER layers are in EPSG:4269 (NAD83) by default; but your sf objects often have it set.
  # If missing, assume 4269, then transform to 4326 to match points.
  st_crs(CROSSWALK_2024) <- 4269
}
CROSSWALK_2024 <- st_transform(CROSSWALK_2024, 4326)

# Also put congressional districts in same CRS
TIGRIS_2024_CONGRESSIONAL_DISTRICTS <- st_transform(TIGRIS_2024_CONGRESSIONAL_DISTRICTS, 4326)

# Select a slim county view to attach (avoid duplicating geometry columns)
county_join_cols <- c(
  "State_Name","State_FIPS","State_Abbreviation","County_Name","County_GEOID",
  "In_CBSA","In_CSA","CBSA_GEOID","CBSA_Name","CBSA_Type",
  "CSA_GEOID","CSA_Name",
  "Census_Division_GEOID","Census_Division_Name",
  "Census_Region_GEOID","Census_Region_Name"
)

COUNTY_LITE <- CROSSWALK_2024 %>%
  select(all_of(county_join_cols), County_Geometry)

# ----------------------------
# 1) Spatial join to County
# ----------------------------
cat("\n--- Joining to County (point-in-polygon) -----------------------------\n")

# Try strict point-in-polygon first
BNEF_JOINED <- st_join(
  BNEF_POINTS,
  COUNTY_LITE,
  join = st_within,
  left = TRUE
)

# If any points fell just outside (due to rounding), assign nearest county
needs_nearest_cty <- which(is.na(BNEF_JOINED$County_GEOID))
if (length(needs_nearest_cty) > 0) {
  cat("  Fallback to nearest county for", length(needs_nearest_cty), "points.\n")
  nearest_idx <- st_nearest_feature(BNEF_JOINED[needs_nearest_cty, ], COUNTY_LITE)
  BNEF_JOINED[needs_nearest_cty, county_join_cols] <- st_drop_geometry(COUNTY_LITE)[nearest_idx, county_join_cols]
}

# ----------------------------
# 2) Spatial join to CD (119th)
# ----------------------------
cat("\n--- Joining to 119th Congressional Districts ------------------------\n")

CD_KEEP <- TIGRIS_2024_CONGRESSIONAL_DISTRICTS %>%
  transmute(
    CD_State_FIPS = STATEFP,
    CD119FP       = CD119FP,
    CD119_GEOID   = GEOID,      # state FIPS + district code; "00" = at-large
    CD_Name       = NAMELSAD,
    CD_Session    = CDSESSN,
    geometry
  )

# Strict point-in-polygon first
BNEF_JOINED <- st_join(
  BNEF_JOINED,
  CD_KEEP,
  join = st_within,
  left = TRUE
)

# Nearest fallback for CDs if needed
needs_nearest_cd <- which(is.na(BNEF_JOINED$CD119_GEOID))
if (length(needs_nearest_cd) > 0) {
  cat("  Fallback to nearest CD for", length(needs_nearest_cd), "points.\n")
  nearest_idx_cd <- st_nearest_feature(BNEF_JOINED[needs_nearest_cd, ], CD_KEEP)
  BNEF_JOINED[needs_nearest_cd, c("CD_State_FIPS","CD119FP","CD119_GEOID","CD_Name","CD_Session")] <-
    st_drop_geometry(CD_KEEP)[nearest_idx_cd, c("CD_State_FIPS","CD119FP","CD119_GEOID","CD_Name","CD_Session")]
}

# ----------------------------
# Output & quick diagnostics
# ----------------------------
BNEF_WITH_GEO <- BNEF_JOINED

# Non-sf table version (drops geometry)
BNEF_WITH_GEO_TBL <- BNEF_WITH_GEO %>%
  st_drop_geometry()

BNEF_WITH_GEO_TBL <- BNEF_WITH_GEO_TBL %>%
  select(-CD_Name, CD_State_FIPS, CD_Session)

# peek
glimpse(BNEF_WITH_GEO_TBL)
