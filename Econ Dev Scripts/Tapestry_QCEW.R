#This R script tests Tapestry estimates for county-level employment estimates (based on QCEW).
#The Tapestry data is available here for download (with a free account): https://tapestry.nkn.uidaho.edu/ 
#Tapesetry data includes estimates for years 2001-2024.
#More information available here: https://rrs.scholasticahq.com/article/123153-tapestry-collaborative-tool-for-regional-data-and-modeling 
#Additional context on Tapestry data downloaded: "For NAICS and FIPS codes, single, multiple or “ALL” codes may be requested. For ownership codes the default is "0" which is the aggregate of variables across all ownership codes, however codes 1-5 may also be requested individually. For employment wage allocation, rules (3-5) are as followed:"
#Rule 3: Returns employment and wage estimates that contain estimates for both a “999999” NAICS code (industry not otherwise classified) and a “999” county FIPS code (county not specified).
#Rule 4: Returns employment and wage estimates that proportionally reallocates the “999” FIPS codes in each state back to the other counties in the state. This way there is no “999” county in any state.
#Rule 5: Returns employment and wage estimates that both proportionally reallocates the “999” FIPS codes and the “999999” NAICS codes so that there are no “999” counties nor “999999” sectors.
#Downloaded data in this test reflects Rule 3, as well as the following selections: 
#Year: 2024 | NAICS Sector Conversion: NAICS 6-digit Conversion | NAICS Processing: Individual records | FIPS Processing: Individual records
#There are also downloads for NAICS5, NAICS4, and NAICS 3 using the same "individual records" settings for NAICS and FIPS processing
#There is also a download for NAICS6D using "individual records" for FIPS and "aggregate records for NAICS" — this might reflect county-level employment totals
#There are also downloads NAICS6D/NAICS5D/NAICS4D/NAICS3D using "individual records" for NAICS and "aggregate records" for FIPS — this might reflect national-level employment by NAICS industry 

#The QCEW data is downloadable here: https://www.bls.gov/cew/downloadable-data-files.htm
#The specific 2024 annual-average data is downloaded from here: https://data.bls.gov/cew/data/files/2024/csv/2024_annual_singlefile.zip 
#Metadata files are available for download in CSV, Excel, and TXT from the following places:
#https://www.bls.gov/cew/about-data/downloadable-file-layouts/annual/naics-based-annual-layout.htm 
#https://www.bls.gov/cew/classifications/industry/industry-titles.htm
#https://www.bls.gov/cew/classifications/areas/qcew-area-titles.htm
#https://www.bls.gov/cew/classifications/ownerships/ownership-titles.htm
#https://www.bls.gov/cew/classifications/size/size-titles.htm
#https://www.bls.gov/cew/classifications/aggregation/agg-level-titles.htm
#https://www.bls.gov/cew/classifications/areas/county-msa-csa-crosswalk.htm
#https://www.bls.gov/cew/classifications/industry/qcew-naics-hierarchy-crosswalk.htm 

# ---- libraries ----
library(readr); library(dplyr); library(tidyverse); library(lubridate)
library(data.table); library(purrr); library(stringr); library(readxl)

# ---- portable root detection (Mac/PC) ----
home <- path.expand("~")
sys  <- Sys.info()[["sysname"]]

# Try common OneDrive roots (Mac and Windows org/person naming)
candidates <- unique(c(
  # macOS OneDrive (new Files Provider)
  file.path(home, "Library", "CloudStorage", "OneDrive-RMI"),
  file.path(home, "Library", "CloudStorage", "OneDrive - RMI"),
  file.path(home, "Library", "CloudStorage", "OneDrive"),
  # Windows: environment variable (most reliable)
  Sys.getenv("OneDrive"),
  # Windows: org-tenant folders commonly used
  file.path(Sys.getenv("USERPROFILE"), "OneDrive - RMI"),
  file.path(Sys.getenv("USERPROFILE"), "OneDrive - Rocky Mountain Institute"),
  file.path(Sys.getenv("USERPROFILE"), "OneDrive"),
  # fallback: user-provided mount under home
  file.path(home, "OneDrive - RMI"),
  file.path(home, "OneDrive")
))

ONE_DRIVE_ROOT <- NULL
for (p in candidates) if (nzchar(p) && dir.exists(p)) { ONE_DRIVE_ROOT <- p; break }
if (is.null(ONE_DRIVE_ROOT)) {
  stop(paste0(
    "Could not locate your OneDrive folder.\n",
    "Checked:\n - ", paste(candidates, collapse = "\n - "),
    "\nPlease update ONE_DRIVE_ROOT manually to your org's OneDrive path."
  ))
}
cat("\n[PATH] Using OneDrive root: ", ONE_DRIVE_ROOT, "\n", sep = "")

# ---- project subpaths (relative to OneDrive) ----
US_PROG_DOCS <- file.path(
  ONE_DRIVE_ROOT,
  "US Program - Documents", "6_Projects", "Clean Regional Economic Development",
  "ACRE", "Data", "Raw Data"
)

QCEW_ANNUAL_DIR  <- file.path(US_PROG_DOCS, "BLS_QCEW", "annual_data")
QCEW_META_DIR    <- file.path(US_PROG_DOCS, "BLS_QCEW", "metadata")
TAPESTRY_DIR_2024 <- file.path(US_PROG_DOCS, "Tapestry_Employment", "2024")

# ---- QCEW annual (2024) ----
QCEW_2024_ANNUAL_FILE <- file.path(QCEW_ANNUAL_DIR, "2024.annual.singlefile.csv")
QCEW_2024_ANNUAL <- {
  is_blank_or_na <- function(x) is.na(x) | trimws(x) == ""
  cat("\n[READ] QCEW_2024_ANNUAL from: ", QCEW_2024_ANNUAL_FILE, "\n", sep = "")
  DT <- fread(
    QCEW_2024_ANNUAL_FILE,
    select = c(
      "area_fips","own_code","industry_code","agglvl_code","size_code","year",
      "disclosure_code","annual_avg_estabs","annual_avg_emplvl",
      "lq_disclosure_code","lq_annual_avg_estabs","lq_annual_avg_emplvl"
    )
  )
  setDT(DT)
  DT[, EMPLOYMENT_DISCLOSED := is_blank_or_na(disclosure_code)]
  DT[, LQ_DISCLOSED         := is_blank_or_na(lq_disclosure_code)]
  if (is.integer(DT$annual_avg_emplvl)) {
    DT[EMPLOYMENT_DISCLOSED == FALSE, annual_avg_emplvl := NA_integer_]
  } else {
    DT[EMPLOYMENT_DISCLOSED == FALSE, annual_avg_emplvl := NA_real_]
  }
  DT[LQ_DISCLOSED == FALSE, lq_annual_avg_emplvl := NA_real_]
  retain_estabs_globally <- DT[, any(!EMPLOYMENT_DISCLOSED &
                                       !is.na(annual_avg_estabs) &
                                       annual_avg_estabs != 0L)]
  if (!retain_estabs_globally) DT[, annual_avg_estabs := NA_integer_]
  DT[]
}
cat("\n[GLIMPSE] QCEW_2024_ANNUAL\n"); glimpse(QCEW_2024_ANNUAL)

# ---- QCEW metadata paths ----
COUNTY_MSA_CSA_CROSSWALK_FILE       <- file.path(QCEW_META_DIR, "qcew_county-msa-csa-crosswalk-2024.csv")
AREA_TITLES_FILE                    <- file.path(QCEW_META_DIR, "area-titles.csv")
INDUSTRY_TITLES_FILE                <- file.path(QCEW_META_DIR, "industry-titles.csv")
SIZE_TITLES_FILE                    <- file.path(QCEW_META_DIR, "size-titles.csv")
AGG_LEVEL_TITLES_FILE               <- file.path(QCEW_META_DIR, "agg-level-titles.csv")
OWNERSHIP_TITLES_FILE               <- file.path(QCEW_META_DIR, "ownership-titles.csv")
QCEW_NAICS_HIERARCHY_CROSSWALK_FILE <- file.path(QCEW_META_DIR, "qcew-naics-hierarchy-crosswalk.xlsx")

# ---- read + inspect metadata ----
cat("\n[READ] COUNTY_MSA_CSA_CROSSWALK from: ", COUNTY_MSA_CSA_CROSSWALK_FILE, "\n", sep = "")
COUNTY_MSA_CSA_CROSSWALK <- read_csv(COUNTY_MSA_CSA_CROSSWALK_FILE)
cat("\n[GLIMPSE] COUNTY_MSA_CSA_CROSSWALK\n"); glimpse(COUNTY_MSA_CSA_CROSSWALK)

cat("\n[READ] AREA_TITLES from: ", AREA_TITLES_FILE, "\n", sep = "")
AREA_TITLES <- read_csv(AREA_TITLES_FILE)
cat("\n[GLIMPSE] AREA_TITLES\n"); glimpse(AREA_TITLES)

cat("\n[READ] INDUSTRY_TITLES from: ", INDUSTRY_TITLES_FILE, "\n", sep = "")
INDUSTRY_TITLES <- read_csv(INDUSTRY_TITLES_FILE)
cat("\n[GLIMPSE] INDUSTRY_TITLES\n"); glimpse(INDUSTRY_TITLES)

cat("\n[READ] SIZE_TITLES from: ", SIZE_TITLES_FILE, "\n", sep = "")
SIZE_TITLES <- read_csv(SIZE_TITLES_FILE)
cat("\n[GLIMPSE] SIZE_TITLES\n"); glimpse(SIZE_TITLES)
cat("\n[PRINT] SIZE_TITLES (n = Inf)\n"); print(SIZE_TITLES, n = Inf, width = Inf)

cat("\n[READ] AGG_LEVEL_TITLES from: ", AGG_LEVEL_TITLES_FILE, "\n", sep = "")
AGG_LEVEL_TITLES <- read_csv(AGG_LEVEL_TITLES_FILE)
cat("\n[GLIMPSE] AGG_LEVEL_TITLES\n"); glimpse(AGG_LEVEL_TITLES)
cat("\n[PRINT] AGG_LEVEL_TITLES (n = Inf)\n"); print(AGG_LEVEL_TITLES, n = Inf, width = Inf)

cat("\n[READ] OWNERSHIP_TITLES from: ", OWNERSHIP_TITLES_FILE, "\n", sep = "")
OWNERSHIP_TITLES <- read_csv(OWNERSHIP_TITLES_FILE)
cat("\n[GLIMPSE] OWNERSHIP_TITLES\n"); glimpse(OWNERSHIP_TITLES)
cat("\n[PRINT] OWNERSHIP_TITLES (n = Inf)\n"); print(OWNERSHIP_TITLES, n = Inf, width = Inf)

cat("\n[READ] QCEW_NAICS_HIERARCHY_CROSSWALK from: ", QCEW_NAICS_HIERARCHY_CROSSWALK_FILE, "\n", sep = "")
QCEW_NAICS_HIERARCHY_CROSSWALK <- read_excel(QCEW_NAICS_HIERARCHY_CROSSWALK_FILE, sheet = "v2022")
cat("\n[GLIMPSE] QCEW_NAICS_HIERARCHY_CROSSWALK\n"); glimpse(QCEW_NAICS_HIERARCHY_CROSSWALK)
# (per request, NO print(n = Inf) for this one)

# ---- QCEW enrichment function ----
QCEW_add_metadata <- function(df){
  df %>%
    left_join(AREA_TITLES, by = "area_fips") %>% relocate(area_title, .after = area_fips) %>%
    left_join(OWNERSHIP_TITLES, by = "own_code") %>% relocate(own_title, .after = own_code) %>%
    left_join(INDUSTRY_TITLES, by = "industry_code") %>% relocate(industry_title, .after = industry_code) %>%
    left_join(AGG_LEVEL_TITLES, by = "agglvl_code") %>% relocate(agglvl_title, .after = agglvl_code) %>%
    left_join(SIZE_TITLES, by = "size_code") %>% relocate(size_title, .after = size_code)
}

# ---- run enrichment on QCEW ----
cat("\n[ENRICH] Building QCEW_2024_ENRICHED\n")
QCEW_2024_ENRICHED <- QCEW_add_metadata(QCEW_2024_ANNUAL)
cat("\n[GLIMPSE] QCEW_2024_ENRICHED\n"); glimpse(QCEW_2024_ENRICHED)

# ---- Tapestry 2024 (loads only; pre-enrichment glimpses commented) ----
cat("\n[READ] TAPESTRY_NAICS6D_COUNTY from: ", file.path(TAPESTRY_DIR_2024, "2024_NAICS6D.csv"), "\n", sep = "")
TAPESTRY_NAICS6D_COUNTY <- read_csv(file.path(TAPESTRY_DIR_2024, "2024_NAICS6D.csv"))
# cat("\n[GLIMPSE] TAPESTRY_NAICS6D_COUNTY\n"); glimpse(TAPESTRY_NAICS6D_COUNTY)

cat("\n[READ] TAPESTRY_NAICS5D_COUNTY from: ", file.path(TAPESTRY_DIR_2024, "2024_NAICS5D.csv"), "\n", sep = "")
TAPESTRY_NAICS5D_COUNTY <- read_csv(file.path(TAPESTRY_DIR_2024, "2024_NAICS5D.csv"))
# cat("\n[GLIMPSE] TAPESTRY_NAICS5D_COUNTY\n"); glimpse(TAPESTRY_NAICS5D_COUNTY)

cat("\n[READ] TAPESTRY_NAICS4D_COUNTY from: ", file.path(TAPESTRY_DIR_2024, "2024_NAICS4D.csv"), "\n", sep = "")
TAPESTRY_NAICS4D_COUNTY <- read_csv(file.path(TAPESTRY_DIR_2024, "2024_NAICS4D.csv"))
# cat("\n[GLIMPSE] TAPESTRY_NAICS4D_COUNTY\n"); glimpse(TAPESTRY_NAICS4D_COUNTY)

cat("\n[READ] TAPESTRY_NAICS3D_COUNTY from: ", file.path(TAPESTRY_DIR_2024, "2024_NAICS3D.csv"), "\n", sep = "")
TAPESTRY_NAICS3D_COUNTY <- read_csv(file.path(TAPESTRY_DIR_2024, "2024_NAICS3D.csv"))
# cat("\n[GLIMPSE] TAPESTRY_NAICS3D_COUNTY\n"); glimpse(TAPESTRY_NAICS3D_COUNTY)

cat("\n[READ] TAPESTRY_COUNTY_EMPLOYMENT_TOTALS from: ", file.path(TAPESTRY_DIR_2024, "2024_COUNTY_AGGREGATE_EMPLOYMENT.csv"), "\n", sep = "")
TAPESTRY_COUNTY_EMPLOYMENT_TOTALS <- read_csv(file.path(TAPESTRY_DIR_2024, "2024_COUNTY_AGGREGATE_EMPLOYMENT.csv"))
# cat("\n[GLIMPSE] TAPESTRY_COUNTY_EMPLOYMENT_TOTALS\n"); glimpse(TAPESTRY_COUNTY_EMPLOYMENT_TOTALS)

cat("\n[READ] TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS from: ", file.path(TAPESTRY_DIR_2024, "2024_NAICS6D_NATIONAL_AGGREGATE.csv"), "\n", sep = "")
TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS <- read_csv(file.path(TAPESTRY_DIR_2024, "2024_NAICS6D_NATIONAL_AGGREGATE.csv"))
# cat("\n[GLIMPSE] TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS\n"); glimpse(TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS)

cat("\n[READ] TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS from: ", file.path(TAPESTRY_DIR_2024, "2024_NAICS5D_NATIONAL_AGGREGATE.csv"), "\n", sep = "")
TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS <- read_csv(file.path(TAPESTRY_DIR_2024, "2024_NAICS5D_NATIONAL_AGGREGATE.csv"))
# cat("\n[GLIMPSE] TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS\n"); glimpse(TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS)

cat("\n[READ] TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS from: ", file.path(TAPESTRY_DIR_2024, "2024_NAICS4D_NATIONAL_AGGREGATE.csv"), "\n", sep = "")
TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS <- read_csv(file.path(TAPESTRY_DIR_2024, "2024_NAICS4D_NATIONAL_AGGREGATE.csv"))
# cat("\n[GLIMPSE] TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS\n"); glimpse(TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS)

cat("\n[READ] TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS from: ", file.path(TAPESTRY_DIR_2024, "2024_NAICS3D_NATIONAL_AGGREGATE.csv"), "\n", sep = "")
TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS <- read_csv(file.path(TAPESTRY_DIR_2024, "2024_NAICS3D_NATIONAL_AGGREGATE.csv"))
# cat("\n[GLIMPSE] TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS\n"); glimpse(TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS)

# ---- Tapestry enrichment helpers ----
add_area_title <- function(df){
  df %>%
    mutate(area_fips = if (is.numeric(.data$area_fips)) sprintf("%05.0f", .data$area_fips) else as.character(.data$area_fips)) %>%
    left_join(AREA_TITLES %>% distinct(area_fips, area_title), by = "area_fips", relationship = "many-to-one") %>%
    relocate(area_title, .after = area_fips)
}
add_industry_title <- function(df){
  lu <- INDUSTRY_TITLES %>% distinct(industry_code, industry_title)
  if ("naics_code" %in% names(df)) {
    df %>% mutate(industry_code = as.character(.data$naics_code)) %>%
      left_join(lu, by = "industry_code", relationship = "many-to-one") %>%
      relocate(industry_title, .after = naics_code) %>% select(-industry_code)
  } else if ("naics_5_digit" %in% names(df)) {
    df %>% mutate(industry_code = as.character(.data$naics_5_digit)) %>%
      left_join(lu, by = "industry_code", relationship = "many-to-one") %>%
      relocate(industry_title, .after = naics_5_digit) %>% select(-industry_code)
  } else if ("naics_4_digit" %in% names(df)) {
    df %>% mutate(industry_code = as.character(.data$naics_4_digit)) %>%
      left_join(lu, by = "industry_code", relationship = "many-to-one") %>%
      relocate(industry_title, .after = naics_4_digit) %>% select(-industry_code)
  } else if ("naics_3_digit" %in% names(df)) {
    df %>% mutate(industry_code = as.character(.data$naics_3_digit)) %>%
      left_join(lu, by = "industry_code", relationship = "many-to-one") %>%
      relocate(industry_title, .after = naics_3_digit) %>% select(-industry_code)
  } else df
}
add_own_title <- function(df){
  df %>%
    left_join(OWNERSHIP_TITLES %>% mutate(own_code = as.numeric(own_code)) %>% distinct(own_code, own_title),
              by = "own_code", relationship = "many-to-one") %>%
    relocate(own_title, .after = own_code)
}
enrich_tapestry_county_naics <- function(df){ df %>% add_area_title() %>% add_own_title() %>% add_industry_title() }

# ---- apply enrichment to ALL Tapestry frames (with banners) ----
cat("\n[ENRICH] Building TAPESTRY_NAICS6D_COUNTY_ENRICHED\n")
TAPESTRY_NAICS6D_COUNTY_ENRICHED  <- enrich_tapestry_county_naics(TAPESTRY_NAICS6D_COUNTY)

cat("\n[ENRICH] Building TAPESTRY_NAICS5D_COUNTY_ENRICHED\n")
TAPESTRY_NAICS5D_COUNTY_ENRICHED  <- enrich_tapestry_county_naics(TAPESTRY_NAICS5D_COUNTY)

cat("\n[ENRICH] Building TAPESTRY_NAICS4D_COUNTY_ENRICHED\n")
TAPESTRY_NAICS4D_COUNTY_ENRICHED  <- enrich_tapestry_county_naics(TAPESTRY_NAICS4D_COUNTY)

cat("\n[ENRICH] Building TAPESTRY_NAICS3D_COUNTY_ENRICHED\n")
TAPESTRY_NAICS3D_COUNTY_ENRICHED  <- enrich_tapestry_county_naics(TAPESTRY_NAICS3D_COUNTY)

cat("\n[ENRICH] Building TAPESTRY_COUNTY_EMPLOYMENT_TOTALS_ENRICHED\n")
TAPESTRY_COUNTY_EMPLOYMENT_TOTALS_ENRICHED <- TAPESTRY_COUNTY_EMPLOYMENT_TOTALS %>% add_area_title() %>% add_own_title()

cat("\n[ENRICH] Building TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS_ENRICHED\n")
TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS_ENRICHED <- TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS %>% add_own_title() %>% add_industry_title()

cat("\n[ENRICH] Building TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS_ENRICHED\n")
TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS_ENRICHED <- TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS %>% add_own_title() %>% add_industry_title()

cat("\n[ENRICH] Building TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS_ENRICHED\n")
TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS_ENRICHED <- TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS %>% add_own_title() %>% add_industry_title()

cat("\n[ENRICH] Building TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS_ENRICHED\n")
TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS_ENRICHED <- TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS %>% add_own_title() %>% add_industry_title()

# ---- GLIMPSE only the ENRICHED Tapestry frames ----
cat("\n[GLIMPSE] TAPESTRY_NAICS6D_COUNTY_ENRICHED\n");  glimpse(TAPESTRY_NAICS6D_COUNTY_ENRICHED)
cat("\n[GLIMPSE] TAPESTRY_NAICS5D_COUNTY_ENRICHED\n");  glimpse(TAPESTRY_NAICS5D_COUNTY_ENRICHED)
cat("\n[GLIMPSE] TAPESTRY_NAICS4D_COUNTY_ENRICHED\n");  glimpse(TAPESTRY_NAICS4D_COUNTY_ENRICHED)
cat("\n[GLIMPSE] TAPESTRY_NAICS3D_COUNTY_ENRICHED\n");  glimpse(TAPESTRY_NAICS3D_COUNTY_ENRICHED)
cat("\n[GLIMPSE] TAPESTRY_COUNTY_EMPLOYMENT_TOTALS_ENRICHED\n"); glimpse(TAPESTRY_COUNTY_EMPLOYMENT_TOTALS_ENRICHED)

cat("\n[GLIMPSE] TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS_ENRICHED\n"); glimpse(TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS_ENRICHED)
cat("\n[GLIMPSE] TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS_ENRICHED\n"); glimpse(TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS_ENRICHED)
cat("\n[GLIMPSE] TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS_ENRICHED\n"); glimpse(TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS_ENRICHED)
cat("\n[GLIMPSE] TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS_ENRICHED\n"); glimpse(TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS_ENRICHED)

# ---- sanity checks ----
stopifnot(nrow(TAPESTRY_NAICS6D_COUNTY_ENRICHED)  == nrow(TAPESTRY_NAICS6D_COUNTY),
          nrow(TAPESTRY_NAICS5D_COUNTY_ENRICHED)  == nrow(TAPESTRY_NAICS5D_COUNTY),
          nrow(TAPESTRY_NAICS4D_COUNTY_ENRICHED)  == nrow(TAPESTRY_NAICS4D_COUNTY),
          nrow(TAPESTRY_NAICS3D_COUNTY_ENRICHED)  == nrow(TAPESTRY_NAICS3D_COUNTY),
          nrow(TAPESTRY_COUNTY_EMPLOYMENT_TOTALS_ENRICHED) == nrow(TAPESTRY_COUNTY_EMPLOYMENT_TOTALS),
          nrow(TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS_ENRICHED) == nrow(TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS),
          nrow(TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS_ENRICHED) == nrow(TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS),
          nrow(TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS_ENRICHED) == nrow(TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS),
          nrow(TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS_ENRICHED) == nrow(TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS))


#Summarize total employment in each Tapestry data frame to—look to the "tap_emplvl_est_3" column for this — compare across Tapestry files
tapestry_emplvl_summaries <- list(
  NAICS6D = TAPESTRY_NAICS6D_COUNTY_ENRICHED %>% summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE)),
  NAICS5D = TAPESTRY_NAICS5D_COUNTY_ENRICHED %>% summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE)),
  NAICS4D = TAPESTRY_NAICS4D_COUNTY_ENRICHED %>% summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE)),
  NAICS3D = TAPESTRY_NAICS3D_COUNTY_ENRICHED %>% summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE)),
  COUNTY_TOTALS = TAPESTRY_COUNTY_EMPLOYMENT_TOTALS_ENRICHED %>% summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE)),
  NATIONAL_NAICS6D = TAPESTRY_NATIONAL_NAICS6D_EMPLOYMENT_TOTALS_ENRICHED %>% summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE)),
  NATIONAL_NAICS5D = TAPESTRY_NATIONAL_NAICS5D_EMPLOYMENT_TOTALS_ENRICHED %>% summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE)),
  NATIONAL_NAICS4D = TAPESTRY_NATIONAL_NAICS4D_EMPLOYMENT_TOTALS_ENRICHED %>% summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE)),
  NATIONAL_NAICS3D = TAPESTRY_NATIONAL_NAICS3D_EMPLOYMENT_TOTALS_ENRICHED %>% summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE))
)
cat("\n[TAPESTRY EMPLOYMENT SUMMARIES]\n")
tapestry_emplvl_summaries %>%
  map_df(~ .x %>% mutate(tapestry_file = names(.x)), .id = "tapestry_file") %>%
  select(tapestry_file, tap_emplvl_est_3) %>%
  arrange(desc(tap_emplvl_est_3)) %>%
  print(n = Inf, width = Inf)

#Now check national employment total for 2024 in QCEW; print out annual_avg_emplvl for the row where "agglvl_code" is "10"
#Filter QCEW_2024_ENRICHED for rows where "agglvl_code" is "10" 
QCEW_2024_NATIONAL_EMPLOYMENT_TOTAL <- QCEW_2024_ENRICHED %>%
  filter(agglvl_code == "10") %>%
  select(area_fips, area_title, own_code, own_title, industry_code, industry_title, size_code, size_title, annual_avg_emplvl) 
cat("\n[GLIMPSE] QCEW_2024_NATIONAL_EMPLOYMENT_TOTAL\n"); glimpse(QCEW_2024_NATIONAL_EMPLOYMENT_TOTAL)
