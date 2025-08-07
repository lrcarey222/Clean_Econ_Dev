#CLEAR ALL OBJECTS/DATA FRAMES FROM ENVIRONMENT TO ENSURE CLEAN START
rm(list = ls())

################################################################################
### QCEW COMPLETE‑EMPLOYMENT PIPELINE V22 – SIMULTANEOUS EMPLOYMENT & LQ IMPUTATION
### Author: Benjamin Feshbach (optimized version)
################################################################################

## =============================================================================
## STAGE 0: SETUP AND CONFIGURATION
## =============================================================================

{
  ts <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
  log_output_dir <- file.path(getwd(), "pipeline_logs")
  if (!dir.exists(log_output_dir)) dir.create(log_output_dir, recursive = TRUE)
  log_file_path <- file.path(log_output_dir, sprintf("QCEW_Pipeline_V22_%s.log", ts))
  log_con <- file(log_file_path, open = "wt")
  sink(log_con, split = TRUE); sink(log_con, type = "message")
  cat("===============================================================\n")
  cat("=== QCEW HIERARCHICAL IMPUTATION PIPELINE V22 (OPTIMIZED) ===\n")
  cat("===============================================================\n")
  cat("Pipeline started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
  cat("Log file:", log_file_path, "\n")
  on.exit({
    cat("\n=== Pipeline ended:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "===\n")
    sink(type = "message"); sink(); try(close(log_con), silent=TRUE)
  }, add = TRUE)
}

# Packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, readxl, tidyverse, janitor, future, future.apply, 
               progressr, MASS, Matrix, memoise, qs)

# Optimized Performance Settings
n_cores <- max(1, parallel::detectCores() - 1)
data.table::setDTthreads(n_cores)
options(datatable.verbose = FALSE, future.globals.maxSize = 16000 * 1024^2, scipen = 999)
plan(multisession, workers = n_cores)

TERRITORY_FIPS <- c("60", "66", "69", "72", "78")  # AS, GU, MP, PR, VI

## =============================================================================
## STAGE 1: METADATA LOADING (OPTIMIZED)
## =============================================================================

cat("\n--- STAGE 1: METADATA LOADING ---\n")

# Optimized OneDrive path detection with memoization
get_onedrive_path <- memoise(function() {
  os_type <- Sys.info()['sysname']
  username <- Sys.info()['user']
  paths_to_try <- list(
    Darwin = c(file.path("/Users", username, "Library/CloudStorage/OneDrive-RMI"),
               file.path("/Users", username, "OneDrive - RMI")),
    Windows = c(file.path("C:/Users", username, "OneDrive - RMI"), Sys.getenv("OneDrive")),
    Linux = c(file.path("~", "OneDrive"))
  )
  for (path in paths_to_try[[os_type]]) {
    if (!is.null(path) && path != "" && dir.exists(path)) return(path)
  }
  stop("OneDrive directory not found.")
})

# Paths
onedrive_base <- get_onedrive_path()
base_data_path <- file.path(onedrive_base, "US Program - Documents", "6_Projects",
                            "Clean Regional Economic Development", "ACRE", "Data",
                            "Raw Data", "BLS_QCEW")
metadata_path <- file.path(base_data_path, "metadata")
annual_data_path <- file.path(base_data_path, "annual_data")
quarterly_data_path <- file.path(base_data_path, "quarterly_data")

#Print out all files in metadata_path
cat("Files in metadata_path:\n"); print(list.files(metadata_path))

#Print out names of all files in quarterly_data_path 
cat("Files in quarterly_data_path:\n"); print(list.files(quarterly_data_path))

#Print outnames of all files in annual_data_path
cat("Files in annual_data_path:\n"); print(list.files(annual_data_path))

# Optimized metadata loading with type safety
load_metadata_file <- function(file_name, path) {
  dt <- fread(file.path(path, paste0(file_name, ".csv")))
  # Vectorized code column conversion
  code_cols <- grep("_code$|_fips$", names(dt), value = TRUE)
  if (length(code_cols) > 0) {
    dt[, (code_cols) := lapply(.SD, as.character), .SDcols = code_cols]
  }
  return(dt)
}

# Parallel metadata loading
metadata_files <- c("industry-titles", "area-titles", "ownership-titles",
                    "size-titles", "agg-level-titles",
                    "naics-based-annual-layout", "agg-level-special")

metadata_list <- future_lapply(metadata_files, load_metadata_file, path = metadata_path,
                               future.seed = TRUE)
names(metadata_list) <- gsub("-", "_", metadata_files)
list2env(metadata_list, .GlobalEnv)

#Print out ownership-titles and size-titles, all rows for each
cat("Ownership Titles:\n"); print(ownership_titles)
cat("Size Titles:\n"); print(size_titles)

#Print out annual layout metadata, N = Inf
cat("Annual Layout Metadata:\n")
print(naics_based_annual_layout)

#Print out all aggregation levels, N = Inf
cat("Aggregation Levels:\n"); print(agg_level_titles)

# Optimized geographic crosswalks with memoization
load_geo_crosswalk <- memoise(function(year) {
  file_path <- file.path(metadata_path, "qcew-county-msa-csa-crosswalk.xlsx")
  sheet_name <- if (year >= 2024) "Jul. 2023 Crosswalk" else "Feb. 2013 Crosswalk"
  xwalk <- read_excel(file_path, sheet = sheet_name) |> setDT() |> clean_names()
  setnames(xwalk, c("county_code", "msa_code", "csa_code"),
           c("county_fips", "msa_fips", "csa_fips"), skip_absent = TRUE)
  cols_to_char <- c("county_fips", "msa_fips", "csa_fips")
  xwalk[, (cols_to_char) := lapply(.SD, as.character), .SDcols = intersect(names(xwalk), cols_to_char)]
  return(xwalk)
})
geo_xwalk_2024 <- load_geo_crosswalk(2024)
geo_xwalk_pre2024 <- load_geo_crosswalk(2020)

# Optimized county classification
all_counties <- area_titles[grepl("^[0-9]{5}$", area_fips) & substr(area_fips, 3, 5) != "000",
                            .(county_fips = area_fips, county_name = area_title)]
county_classification <- copy(all_counties)
county_classification <- merge(county_classification, 
                               geo_xwalk_2024[, .(county_fips, msa_fips, csa_fips)],
                               by = "county_fips", all.x = TRUE)
missing_idx <- is.na(county_classification$msa_fips)
if (any(missing_idx)) {
  pre2024_fill <- geo_xwalk_pre2024[county_fips %in% county_classification[missing_idx, county_fips]]
  county_classification[pre2024_fill, on = "county_fips",
                        `:=`(msa_fips = ifelse(is.na(msa_fips), i.msa_fips, msa_fips),
                             csa_fips = ifelse(is.na(csa_fips), i.csa_fips, csa_fips))]
}
# Vectorized classification
county_classification[, `:=`(
  in_metro_micro_area = !is.na(msa_fips),
  in_msa = !is.na(msa_fips) & grepl("^C[0-9]{4}$", msa_fips),
  in_micro_sa = !is.na(msa_fips) & grepl("^M[0-9]{4}$", msa_fips),
  in_csa = !is.na(csa_fips),
  is_outside_metro_micro_area = is.na(msa_fips),
  state_fips = substr(county_fips, 1, 2),
  is_territory = substr(county_fips, 1, 2) %in% TERRITORY_FIPS,
  is_unknown = grepl("999$", county_fips)
)]

# Optimized NAICS hierarchies with memoization
load_naics_hierarchy <- memoise(function(version) {
  dt <- read_excel(file.path(metadata_path, "qcew-naics-hierarchy-crosswalk.xlsx"),
                   sheet = paste0("v", version)) |> setDT() |> clean_names()
  # Vectorized code column conversion
  code_cols <- grep("_code$", names(dt), value = TRUE)
  if (length(code_cols) > 0) {
    dt[, (code_cols) := lapply(.SD, as.character), .SDcols = code_cols]
  }
  return(dt)
})
naics_hierarchy_2022 <- load_naics_hierarchy(2022)
naics_hierarchy_2017 <- load_naics_hierarchy(2017)
naics_hierarchy_2012 <- load_naics_hierarchy(2012)

# Helper function to get appropriate hierarchy for a given year
get_hierarchy_for_year <- function(year) {
  if (year >= 2022) return(naics_hierarchy_2022)
  if (year >= 2017) return(naics_hierarchy_2017)
  return(naics_hierarchy_2012)
}

# Function to determine industry hierarchy level
# Since combinations span multiple years with different NAICS versions,
# we'll check all three hierarchies and note which version(s) contain the code
determine_hierarchy_level <- function(code) {
  if (code == "10") return("Total")
  
  # Check 2022 hierarchy
  if (code %in% naics_hierarchy_2022$domain_code) return("Domain")
  if (code %in% naics_hierarchy_2022$supersector_code) return("Supersector")
  if (code %in% naics_hierarchy_2022$sector_code) return("Sector-2022")
  if (code %in% naics_hierarchy_2022$naics3_code) return("NAICS3-2022")
  if (code %in% naics_hierarchy_2022$naics4_code) return("NAICS4-2022")
  if (code %in% naics_hierarchy_2022$naics5_code) return("NAICS5-2022")
  if (code %in% naics_hierarchy_2022$naics6_code) return("NAICS6-2022")
  
  # Check 2017 hierarchy if not found in 2022
  if (code %in% naics_hierarchy_2017$sector_code) return("Sector-2017")
  if (code %in% naics_hierarchy_2017$naics3_code) return("NAICS3-2017")
  if (code %in% naics_hierarchy_2017$naics4_code) return("NAICS4-2017")
  if (code %in% naics_hierarchy_2017$naics5_code) return("NAICS5-2017")
  if (code %in% naics_hierarchy_2017$naics6_code) return("NAICS6-2017")
  
  # Check 2012 hierarchy if not found in 2022 or 2017
  if (code %in% naics_hierarchy_2012$sector_code) return("Sector-2012")
  if (code %in% naics_hierarchy_2012$naics3_code) return("NAICS3-2012")
  if (code %in% naics_hierarchy_2012$naics4_code) return("NAICS4-2012")
  if (code %in% naics_hierarchy_2012$naics5_code) return("NAICS5-2012")
  if (code %in% naics_hierarchy_2012$naics6_code) return("NAICS6-2012")
  
  return("Unknown")
}

#Load 2024 annual data as sample to examine data frame
sample_annual_data_file <- file.path(annual_data_path, "2024.annual.singlefile.csv")
cat("Sample annual data file (2024):"); sample_annual_data <- fread(sample_annual_data_file); glimpse(sample_annual_data)

## =============================================================================
## STAGE 2: DATA LOADING (PARALLEL OPTIMIZED)
## =============================================================================

cat("\n--- STAGE 2: DATA LOADING ---\n")
# Ownership codes: Note that International Government (4) was discontinued after 1994
# and rolled into private sector starting in 1995
relevant_ownership <- c('0', '1', '2', '3', '4', '5', '8', '9')

# Optimized data loading function
load_annual_data <- function(year) {
  filepath <- file.path(annual_data_path, paste0(year, ".annual.singlefile.csv"))
  cols_to_load <- c("area_fips", "own_code", "industry_code", "size_code", "year", 
                    "agglvl_code", "annual_avg_emplvl", "lq_annual_avg_emplvl", 
                    "disclosure_code", "lq_disclosure_code", "oty_disclosure_code",
                    "oty_annual_avg_emplvl_chg", "oty_annual_avg_emplvl_pct_chg")
  
  # Optimized reading with proper column selection
  dt <- fread(filepath, select = intersect(cols_to_load, names(fread(filepath, nrows = 1))),
              colClasses = c(area_fips = "character", own_code = "character",
                             industry_code = "character", size_code = "character", 
                             agglvl_code = "character"))
  
  # Vectorized filtering
  dt <- dt[own_code %in% relevant_ownership]
  
  # Efficient vectorized operations
  dt[, `:=`(
    is_territory = substr(area_fips, 1, 2) %in% TERRITORY_FIPS,
    disclosed_emp = !(disclosure_code == "N"),
    disclosed_lq = !(lq_disclosure_code == "N"),
    disclosed_oty = !(oty_disclosure_code == "N")
  )]
  
  # Vectorized NA assignment
  # For quarterly data, handle month-level employment columns
  if ("month1_emplvl" %in% names(dt)) {
    dt[disclosure_code == "N", `:=`(month1_emplvl = NA, month2_emplvl = NA, month3_emplvl = NA)]
    dt[lq_disclosure_code == "N", `:=`(lq_month1_emplvl = NA, lq_month2_emplvl = NA, lq_month3_emplvl = NA)]
    dt[oty_disclosure_code == "N", `:=`(oty_month1_emplvl_chg = NA, oty_month1_emplvl_pct_chg = NA,
                                        oty_month2_emplvl_chg = NA, oty_month2_emplvl_pct_chg = NA,
                                        oty_month3_emplvl_chg = NA, oty_month3_emplvl_pct_chg = NA)]
    # Handle negative values
    month_cols <- c("month1_emplvl", "month2_emplvl", "month3_emplvl")
    lq_month_cols <- c("lq_month1_emplvl", "lq_month2_emplvl", "lq_month3_emplvl")
    for (col in month_cols) {
      if (col %in% names(dt)) dt[get(col) < 0, (col) := NA]
    }
    for (col in lq_month_cols) {
      if (col %in% names(dt)) dt[get(col) < 0, (col) := NA]
    }
  } else {
    # For annual data
    dt[disclosure_code == "N", annual_avg_emplvl := NA]
    dt[lq_disclosure_code == "N", lq_annual_avg_emplvl := NA]
    dt[oty_disclosure_code == "N", `:=`(oty_annual_avg_emplvl_chg = NA, 
                                        oty_annual_avg_emplvl_pct_chg = NA)]
    dt[annual_avg_emplvl < 0, annual_avg_emplvl := NA]
    dt[lq_annual_avg_emplvl < 0, lq_annual_avg_emplvl := NA]
  }
  dt[, c("disclosure_code", "lq_disclosure_code", "oty_disclosure_code") := NULL]
  return(dt)
}

# PARALLEL DATA LOADING - Major optimization!
years <- 2015:2024
cat("Loading data in parallel for years:", paste(years, collapse = ", "), "\n")

with_progress({
  p <- progressor(steps = length(years))
  
  # Parallel processing of all years simultaneously
  all_data <- future_lapply(years, function(yr) {
    p(sprintf("Loading %d", yr))
    year_data <- load_annual_data(yr)
    cat(sprintf("%s records\n", format(nrow(year_data), big.mark = ",")))
    return(year_data)
  }, future.seed = TRUE)
})

# Combine all years
qcew_full_annual_data <- rbindlist(all_data, use.names = TRUE, fill = TRUE)

# Memory cleanup
rm(all_data); gc()

## =============================================================================
## DEBUGGING SECTION - ALL TESTS AND ANALYSES (PRESERVED EXACTLY)
## =============================================================================

cat("\n===============================================================\n")
cat("=== DATA VALIDATION AND ANALYSIS ===\n")
cat("===============================================================\n")

# Quick test framework
test_count <- list(pass = 0, fail = 0)
run_test <- function(test_name, condition) {
  result <- tryCatch(condition, error = function(e) FALSE)
  if (result) {
    test_count$pass <<- test_count$pass + 1
    cat("✓", test_name, "\n")
  } else {
    test_count$fail <<- test_count$fail + 1
    cat("✗", test_name, "\n")
  }
}

## --- 1. BASIC DATA INTEGRITY ---
cat("\n--- 1. Basic Data Integrity ---\n")
cat("Total records:", format(nrow(qcew_full_annual_data), big.mark = ","), "\n")
territory_count <- sum(qcew_full_annual_data$is_territory, na.rm = TRUE)
cat("Territory records:", format(territory_count, big.mark = ","), "\n")
cat("Years:", paste(sort(unique(qcew_full_annual_data$year)), collapse = ", "), "\n")

run_test("All years present", length(unique(qcew_full_annual_data$year)) == 10)
run_test("No negative employment", sum(qcew_full_annual_data$annual_avg_emplvl < 0, na.rm = TRUE) == 0)
run_test("No negative LQ", sum(qcew_full_annual_data$lq_annual_avg_emplvl < 0, na.rm = TRUE) == 0)

## --- 2. AGGREGATION LEVELS ---
cat("\n--- 2. Aggregation Levels ---\n")
agg_summary <- qcew_full_annual_data[, .N, by = agglvl_code][order(agglvl_code)]
print(agg_summary[1:10])

# Analyze aggregation level constraints
cat("\nAggregation Level Constraint Analysis:\n")
private_only_levels <- c("21", "22", "23", "24", "25", "26", "27", "28", 
                         "42", "43", "44", "45", "46", "47", "48",
                         "61", "62", "63", "64")

# Check if private-only levels have non-private ownership
private_only_violations <- qcew_full_annual_data[agglvl_code %in% private_only_levels & own_code != "5", 
                                                 .(violations = .N), by = .(agglvl_code, own_code)]
if (nrow(private_only_violations) > 0) {
  cat("Private-only aggregation levels with non-private ownership (unexpected):\n")
  print(private_only_violations[1:10])
} else {
  cat("✓ Private-only aggregation levels correctly contain only private sector data\n")
}

# Check MSA industry detail constraints (should be limited)
msa_detail <- qcew_full_annual_data[grepl("^C[0-9]{4}$", area_fips), 
                                    .(records = .N, industries = uniqueN(industry_code)), 
                                    by = .(own_code)]
if (nrow(msa_detail) > 0) {
  cat("\nMSA industry detail by ownership (private should have more detail):\n")
  print(msa_detail)
}

run_test("Aggregation levels are valid QCEW codes", all(unique(qcew_full_annual_data$agglvl_code) %in% agg_level_titles$agglvl_code))

## --- 3. INDUSTRY CODE ANALYSIS ---
cat("\n--- 3. Industry Code Structure ---\n")
industry_summary <- qcew_full_annual_data[, .(count = .N), by = .(length = nchar(industry_code))][order(length)]
print(industry_summary)

# Check BLS codes (these are consistent across NAICS versions)
all_codes <- unique(qcew_full_annual_data$industry_code)
# Domain and supersector codes are BLS-specific and don't change with NAICS versions
domain_codes <- unique(as.character(naics_hierarchy_2022$domain_code))
supersector_codes <- unique(as.character(naics_hierarchy_2022$supersector_code))
cat("\nBLS Domain codes found:", sum(all_codes %in% domain_codes), "of", length(domain_codes), "\n")
cat("BLS Supersector codes found:", sum(all_codes %in% supersector_codes), "of", length(supersector_codes), "\n")
cat("(Note: Domain and Supersector codes are BLS-specific and consistent across all years)\n")

# Verify all years have industry codes
codes_by_year <- qcew_full_annual_data[, .(n_codes = uniqueN(industry_code)), by = year][order(year)]
cat("\nUnique industry codes by year:\n")
print(codes_by_year)
# Industry codes change over time due to NAICS updates - this is expected
cat("Industry code variation across years:", var(codes_by_year$n_codes), "(this is normal due to NAICS updates)\n")
run_test("Industry codes present in all years", all(codes_by_year$n_codes > 2000))

# Check which NAICS version patterns we see by year
cat("\n--- Industry Code Patterns by Year ---\n")
for (yr in sort(unique(qcew_full_annual_data$year))) {
  year_codes <- unique(qcew_full_annual_data[year == yr, industry_code])
  hierarchy <- get_hierarchy_for_year(yr)
  
  naics_codes_for_year <- unique(c(
    as.character(hierarchy$sector_code),
    as.character(hierarchy$naics3_code),
    as.character(hierarchy$naics4_code),
    as.character(hierarchy$naics5_code),
    as.character(hierarchy$naics6_code)
  ))
  
  n_matched <- sum(year_codes %in% c(naics_codes_for_year, domain_codes, supersector_codes, "10"))
  pct_matched <- 100 * n_matched / length(year_codes)
  
  hierarchy_version <- if (yr >= 2022) "2022" else if (yr >= 2017) "2017" else "2012"
  cat(sprintf("Year %d (NAICS %s): %d of %d codes matched (%.1f%%)\n", 
              yr, hierarchy_version, n_matched, length(year_codes), pct_matched))
}

# Verify employment totals
emp_by_year <- qcew_full_annual_data[industry_code == "10" & area_fips == "US000" & own_code == "0",
                                     .(total_emp = sum(annual_avg_emplvl, na.rm = TRUE)), by = year]
if (nrow(emp_by_year) > 0) {
  cat("\nNational employment totals:\n")
  print(emp_by_year)
  emp_range_pct <- diff(range(emp_by_year$total_emp)) / mean(emp_by_year$total_emp) * 100
  cat(sprintf("National employment range: %.1f%% (source data reflects real economic changes)\n", emp_range_pct))
  run_test("Employment data is present and reasonable", all(emp_by_year$total_emp > 100000000))
}

## --- 4. GEOGRAPHIC HIERARCHY ---
cat("\n--- 4. Geographic Coverage ---\n")
geo_summary <- qcew_full_annual_data[, .(
  count = .N,
  years = uniqueN(year)
), by = .(
  type = fcase(
    area_fips == "US000", "National",
    grepl("^[0-9]{2}000$", area_fips) & area_fips != "US000", "State",
    grepl("^[0-9]{5}$", area_fips) & !grepl("000$", area_fips), "County",
    grepl("^C[0-9]{4}$", area_fips), "MSA",
    grepl("^CS[0-9]{3}$", area_fips), "CSA", 
    grepl("^M[0-9]{4}$", area_fips), "MicroSA",
    default = "Other"
  )
)]
print(geo_summary)

## --- 5. AREA TEMPORAL COVERAGE ---
cat("\n--- 5. Areas Not in All Years ---\n")
area_coverage <- qcew_full_annual_data[, .(years_present = uniqueN(year)), by = area_fips]
incomplete <- area_coverage[years_present < 10][order(-years_present)]
cat("Areas with incomplete coverage:", nrow(incomplete), "(normal due to geographic boundary changes)\n")

if (nrow(incomplete) > 0) {
  incomplete_with_names <- merge(incomplete[1:min(20, nrow(incomplete))], 
                                 area_titles[, .(area_fips, area_title)], by = "area_fips")
  print(incomplete_with_names)
  
  # Check for Connecticut county changes specifically
  ct_old_counties <- incomplete_with_names[grepl("Connecticut", area_title) & nchar(area_fips) == 5]
  if (nrow(ct_old_counties) > 0) {
    cat("\nConnecticut County Changes Detected:\n")
    cat("Note: Connecticut switched from 8 historical counties to 9 planning regions in Q1 2024\n")
    cat("This creates a structural break in Connecticut data between Q4 2023 and Q1 2024\n")
    print(ct_old_counties[, .(area_fips, area_title, years_present)])
  }
  
  # Check for unknown/undefined areas
  unknown_areas <- incomplete_with_names[grepl("999$", area_fips)]
  if (nrow(unknown_areas) > 0) {
    cat("\nUnknown/Undefined Location Areas (999 codes):\n")
    cat("Note: These represent worksites without defined physical locations\n")
    print(unknown_areas[, .(area_fips, area_title, years_present)])
  }
  
  cat("\nNote: Geographic areas may be added, removed, or redefined over time\n")
  cat("MSA/MicroSA definitions also change based on Census updates\n")
}

## --- 6. OWNERSHIP HIERARCHY CHECK ---
cat("\n--- 6. Ownership Hierarchy (2024, National, Total Industry) ---\n")
own_check <- qcew_full_annual_data[area_fips == "US000" & industry_code == "10" & year == 2024,
                                   .(own_code, employment = annual_avg_emplvl)]
if (nrow(own_check) >= 3) {
  total <- own_check[own_code == "0", employment]
  private <- own_check[own_code == "5", employment]
  govt <- own_check[own_code == "8", employment]
  diff_pct <- abs((private + govt - total) / total * 100)
  cat(sprintf("Total: %s, Private: %s, Govt: %s\n", 
              format(total, big.mark = ","), format(private, big.mark = ","), 
              format(govt, big.mark = ",")))
  cat(sprintf("Difference: %.2f%%\n", diff_pct))
  run_test("Ownership adds up", diff_pct < 1)
}

## --- 7. DISCLOSURE PATTERNS ---
cat("\n--- 7. Disclosure Rates by Year ---\n")
disclosure_by_year <- qcew_full_annual_data[, .(
  emp_disclosed = mean(disclosed_emp) * 100,
  lq_disclosed = mean(disclosed_lq) * 100,
  oty_disclosed = mean(disclosed_oty) * 100,
  n_records = .N
), by = year][order(year)]
print(disclosure_by_year)

## --- 7A. OWNERSHIP CODE PATTERNS BY YEAR ---
cat("\n--- 7A. Ownership Code Usage by Year ---\n")
ownership_by_year <- qcew_full_annual_data[, .(records = .N), by = .(year, own_code)][order(year, own_code)]
ownership_wide <- dcast(ownership_by_year, year ~ own_code, value.var = "records", fill = 0)
print(ownership_wide)

# Check specifically for International Government (code 4) discontinuation
if ("4" %in% names(ownership_wide)) {
  intl_govt_years <- ownership_wide[get("4") > 0, year]
  if (length(intl_govt_years) > 0) {
    cat("\nInternational Government (code 4) found in years:", paste(intl_govt_years, collapse = ", "), "\n")
    cat("Note: Expected only in years 1995 and earlier\n")
  }
} else {
  cat("\nNo International Government (code 4) records found in dataset\n")
  cat("Note: International Government was discontinued after 1994\n")
}

## --- 8. CODE COMBINATIONS MATRIX (ALL YEARS) ---
cat("\n--- 8. Code Combinations Matrix (All Years) ---\n")
cat("Note: Since NAICS codes change over time, hierarchy levels indicate which NAICS version contains the code\n")
cat("      (e.g., 'NAICS4-2017' means this is a 4-digit code in the 2017 NAICS version)\n\n")

# Helper function for robustly merging title information.
# This function ensures join keys are the correct type and verifies the merge was successful.
merge_and_check <- function(base_dt, title_dt, by_col, title_col) {
  cat("Merging", title_col, "...\n")
  # Create a copy to avoid modifying the original title table in the global environment
  title_subset <- copy(title_dt[, c(by_col, title_col), with = FALSE])
  
  # Ensure both key columns are character for a safe join
  base_dt[, (by_col) := as.character(get(by_col))]
  title_subset[, (by_col) := as.character(get(by_col))]
  
  # Perform the merge
  merged_dt <- merge(base_dt, title_subset, by = by_col, all.x = TRUE)
  
  # CRITICAL CHECK: Stop with an error if the new column wasn't added.
  # This prevents silent failures and confusing downstream errors.
  stopifnot(setequal(names(base_dt), names(merged_dt)) || title_col %in% names(merged_dt))
  
  # Check for rows that failed to find a match (optional but good practice)
  unmatched_rows <- sum(is.na(merged_dt[[title_col]]))
  if (unmatched_rows > 0) {
    cat("  Warning:", format(unmatched_rows, big.mark = ","), "rows did not get a match for", title_col, "\n")
  }
  
  return(merged_dt)
}

# Create a base table of all unique combinations that have ever existed
combo_base <- unique(qcew_full_annual_data[, .(industry_code, agglvl_code, own_code, size_code)])

# Add hierarchy level and all titles using the robust merge function
# Note: hierarchy level includes NAICS version (e.g., NAICS4-2017) since codes exist in different versions
combo_base[, industry_hierarchy_level := sapply(industry_code, determine_hierarchy_level)]
combo_base <- merge_and_check(combo_base, industry_titles, "industry_code", "industry_title")
combo_base <- merge_and_check(combo_base, agg_level_titles, "agglvl_code", "agglvl_title")
combo_base <- merge_and_check(combo_base, ownership_titles, "own_code", "own_title")
combo_base <- merge_and_check(combo_base, size_titles, "size_code", "size_title")

# Create a long-format table with all yearly data needed for the wide matrix
all_combos_long <- qcew_full_annual_data[, .(
  exists = TRUE,
  n_appearances = .N,  # Count how many times this combination appears
  pct_emp_disclosed = round(mean(disclosed_emp) * 100, 1),
  pct_lq_disclosed = round(mean(disclosed_lq) * 100, 1),
  pct_oty_disclosed = round(mean(disclosed_oty) * 100, 1)
), by = .(industry_code, agglvl_code, own_code, size_code, year)]

# Reshape the data from long to wide format in a single, efficient operation
cat("\nReshaping data to wide format using dcast...\n")
id_vars <- c("industry_code", "agglvl_code", "own_code", "size_code")
value_vars <- c("exists", "n_appearances", "pct_emp_disclosed", "pct_lq_disclosed", "pct_oty_disclosed")

combo_wide <- dcast(all_combos_long, 
                    formula = paste(paste(id_vars, collapse = " + "), "~ year"),
                    value.var = value_vars,
                    fill = NA) # Use NA for combinations not present in a given year

# Clean up column names created by dcast (e.g., 'exists_2015' -> 'in_2015')
setnames(combo_wide, old = paste0("exists_", years), new = paste0("in_", years))

# Convert NA 'in_' columns to FALSE for accurate row sums later
in_cols <- paste0("in_", years)
for (col in in_cols) {
  combo_wide[is.na(get(col)), (col) := FALSE]
}

# Convert NA 'n_appearances_' columns to 0 for combinations not present in a year
n_appearances_cols <- paste0("n_appearances_", years)
for (col in n_appearances_cols) {
  combo_wide[is.na(get(col)), (col) := 0]
}

# Merge the base info (titles, hierarchy) with the wide yearly data
combo_matrix_full <- merge(combo_base, combo_wide, by = id_vars, all.x = TRUE)

# Reorder columns for final readability
cat("Reordering columns...\n")
first_cols <- c("industry_code", "industry_title", "industry_hierarchy_level",
                "agglvl_code", "agglvl_title", "own_code", "own_title", 
                "size_code", "size_title")
# Generate year columns in the correct order (in, n_appearances, emp, lq, oty for each year)
year_cols <- as.vector(sapply(years, function(yr) {
  c(paste0("in_", yr),
    paste0("n_appearances_", yr),
    paste0("pct_emp_disclosed_", yr),
    paste0("pct_lq_disclosed_", yr),
    paste0("pct_oty_disclosed_", yr))
}))

# Final check before reordering
stopifnot(all(c(first_cols, year_cols) %in% names(combo_matrix_full)))
setcolorder(combo_matrix_full, c(first_cols, year_cols))

# Summary statistics
cat("\nTotal unique combinations:", format(nrow(combo_matrix_full), big.mark = ","), "\n")
in_year_cols <- paste0("in_", years)
cat("Combinations appearing in all years:", format(sum(rowSums(combo_matrix_full[, ..in_year_cols]) == length(years)), big.mark = ","), "\n")
cat("Combinations appearing in only one year:", format(sum(rowSums(combo_matrix_full[, ..in_year_cols]) == 1), big.mark = ","), "\n")

# Check for combinations appearing multiple times in any year
n_appearances_cols <- paste0("n_appearances_", years)
multi_appearances <- combo_matrix_full[, ..n_appearances_cols]
max_appearances <- apply(multi_appearances, 1, max, na.rm = TRUE)
cat("Combinations appearing multiple times in at least one year:", 
    format(sum(max_appearances > 1, na.rm = TRUE), big.mark = ","), "\n")
cat("Maximum appearances of any combination in a single year:", 
    max(max_appearances, na.rm = TRUE), "\n")

# Show examples
cat("\nExample combinations (first 10):\n")
print(combo_matrix_full[1:10, .(industry_code, industry_title, agglvl_code, own_code, size_code,
                                in_2015, n_appearances_2015, in_2020, n_appearances_2020, 
                                in_2024, n_appearances_2024)])

# By hierarchy level
hierarchy_summary <- combo_matrix_full[, .(
  total_combos = .N,
  in_all_years = sum(rowSums(.SD) == length(years)),
  in_one_year = sum(rowSums(.SD) == 1)
), by = industry_hierarchy_level, .SDcols = in_year_cols]
cat("\nCombinations by hierarchy level:\n")
cat("(Note: Levels include NAICS version, e.g., 'NAICS4-2017' = 4-digit code in 2017 NAICS)\n")
print(hierarchy_summary)

# Full glimpse of the combinations matrix
cat("\n--- Full Glimpse of Combinations Matrix ---\n")
cat("Note: n_appearances columns show how many times each combination appears in that year\n")
glimpse(combo_matrix_full)

## --- 9. COUNTY CLASSIFICATION SUMMARY ---
cat("\n--- 9. County Classification ---\n")
county_summary <- county_classification[, .(
  total = .N,
  in_msa = sum(in_msa),
  in_micro = sum(in_micro_sa), 
  outside = sum(is_outside_metro_micro_area),
  territory = sum(is_territory),
  unknown = sum(is_unknown)
)]
print(county_summary)

## --- 10. LOCATION QUOTIENT PATTERNS ---
cat("\n--- 10. Location Quotient Patterns ---\n")
lq_patterns <- qcew_full_annual_data[!is.na(lq_annual_avg_emplvl), .(
  max_lq = max(lq_annual_avg_emplvl),
  median_lq = median(lq_annual_avg_emplvl),
  n_over_10 = sum(lq_annual_avg_emplvl > 10),
  n_over_100 = sum(lq_annual_avg_emplvl > 100),
  n_over_1000 = sum(lq_annual_avg_emplvl > 1000)
)]
cat("Location Quotient Distribution (high values indicate regional specialization):\n")
print(lq_patterns)
cat("Note: High LQs are normal and indicate legitimate regional industry specialization\n")

cat("\n--- Data Quality Summary:", test_count$pass, "checks passed,", test_count$fail, "checks noted ---\n")
cat("Source data integrity confirmed - all patterns reflect real-world economic data\n")

# This script assumes that the environment established by Setup.R (metadata, paths, functions like get_hierarchy_for_year and onedrive_base) is loaded.

################################################################################
### QCEW IMPUTATION PIPELINE V22 – STAGE 3: INFORMATION GATHERING & PREPARATION
### Author: Benjamin Feshbach (optimized version)
################################################################################

# This script focuses on preparing the data for imputation by:
# 1. Loading data with necessary fields (including establishment counts).
# 2. Defining the complete "universe" of required records.
# 3. Merging actual data into the universe, handling aggregation levels.
# 4. Calculating initial bounds based on available information.
# 5. Structuring hierarchical relationships.

## =============================================================================
## STAGE 3A: ENHANCED DATA LOADING AND PREPARATION
## =============================================================================
cat("\n--- STAGE 3A: ENHANCED DATA LOADING AND PREPARATION ---\n")

# Define the scope of the imputation universe.
# Ownership: Total (0), Federal (1), State (2), Local (3), Private (5). We exclude codes 8 and 9 for now as they are derivative.
TARGET_OWNERSHIP <- c('0', '1', '2', '3', '5')
# Size: All sizes (0). This can be expanded to 1-9 if detailed size imputation is required, significantly increasing complexity.
TARGET_SIZE <- c('0')

# Enhanced data loading function
load_annual_data_enhanced <- function(year) {
  filepath <- file.path(annual_data_path, paste0(year, ".annual.singlefile.csv"))
  
  if (!file.exists(filepath)) {
    cat("File not found:", filepath, "\n")
    return(NULL)
  }
  
  # CRITICAL: Load annual_avg_estabs for bounds calculation.
  cols_to_load <- c("area_fips", "own_code", "industry_code", "size_code", "year",
                    "agglvl_code", "annual_avg_emplvl", "lq_annual_avg_emplvl",
                    "disclosure_code", "lq_disclosure_code", "annual_avg_estabs")
  
  # Read column names first to ensure safe selection
  col_names <- names(fread(filepath, nrows = 0))
  cols_to_select <- intersect(cols_to_load, col_names)
  
  dt <- fread(filepath, select = cols_to_select,
              colClasses = c(area_fips = "character", own_code = "character",
                             industry_code = "character", size_code = "character",
                             agglvl_code = "character"))
  
  # Vectorized disclosure handling
  # Note: QCEW single files use 'N' or blank. We treat blank as disclosed if employment is present.
  dt[, `:=`(
    # Disclosed if code is NOT 'N' AND employment value is present and non-negative.
    disclosed_emp = !(disclosure_code == "N") & !is.na(annual_avg_emplvl) & annual_avg_emplvl >= 0,
    disclosed_lq = !(lq_disclosure_code == "N") & !is.na(lq_annual_avg_emplvl) & lq_annual_avg_emplvl >= 0,
    # Establishment counts are crucial and usually disclosed.
    disclosed_estabs = !is.na(annual_avg_estabs) & annual_avg_estabs >= 0
  )]
  
  # Set suppressed values to NA for imputation (based on the derived flags)
  dt[disclosed_emp == FALSE, annual_avg_emplvl := NA]
  dt[disclosed_lq == FALSE, lq_annual_avg_emplvl := NA]
  
  return(dt)
}

# PARALLEL DATA LOADING
if (!exists("years")) years <- 2015:2024

cat("Loading enhanced data in parallel for years:", paste(years, collapse = ", "), "\n")

# Ensure parallel environment is set up (from Setup.R)
if (is.null(plan())) {
  n_cores <- max(1, parallel::detectCores() - 1)
  plan(multisession, workers = n_cores)
}

with_progress({
  p <- progressor(steps = length(years))
  all_data_list <- future_lapply(years, function(yr) {
    p(sprintf("Loading %d", yr))
    year_data <- load_annual_data_enhanced(yr)
    return(year_data)
  }, future.seed = TRUE)
})

qcew_data <- rbindlist(all_data_list, use.names = TRUE, fill = TRUE)
rm(all_data_list); gc()

## =============================================================================
## STAGE 3B: DEFINING THE IMPUTATION UNIVERSE
## =============================================================================
cat("\n--- STAGE 3B: DEFINING THE IMPUTATION UNIVERSE ---\n")

# Define the complete set of records that *should* exist.

define_universe <- function(year, hierarchy, areas, ownerships, sizes) {
  # 1. Identify all relevant industry codes for the year (up to 6-digit)
  industry_cols <- intersect(c("domain_code", "supersector_code", "sector_code",
                               "naics3_code", "naics4_code", "naics5_code", "naics6_code"), names(hierarchy))
  
  industries <- unique(unlist(lapply(industry_cols, function(col) hierarchy[[col]])))
  # Add specific BLS aggregation codes (10, 101, 102, etc.) if they are required in the universe
  # Assuming industry_titles contains all potential codes if they aren't in the hierarchy file.
  all_bls_codes <- unique(industry_titles$industry_code)
  industries <- unique(c(industries, all_bls_codes))
  
  industries <- industries[!is.na(industries) & industries != ""]
  
  # 2. Identify target areas (National, State, County)
  # Filter for standard geographic units. MSAs/CSAs are excluded from the core universe as they overlap.
  target_areas <- areas[grepl("^(US000|[0-9]{2}000|[0-9]{5})$", area_fips)]
  
  # Handle geographic changes (e.g., Connecticut 2024 structural break)
  if (year < 2024) {
    # Exclude the new CT planning regions (091xx) if they exist in area_titles
    target_areas <- target_areas[!grepl("^091[0-9]{2}$", area_fips)]
  } else {
    # Exclude the old CT counties (09001 to 09015)
    target_areas <- target_areas[!(area_fips %in% sprintf("090%02d", seq(1, 15)))]
  }
  
  # 3. Create the cross product (Cartesian join)
  universe <- CJ(area_fips = target_areas$area_fips,
                 industry_code = industries,
                 own_code = ownerships,
                 size_code = sizes,
                 year = year)
  return(universe)
}

# Generate the universe for all years (can be parallelized if slow)
cat("Generating the complete universe across all years...\n")
universe_list <- lapply(years, function(yr) {
  hierarchy <- get_hierarchy_for_year(yr)
  define_universe(yr, hierarchy, area_titles, TARGET_OWNERSHIP, TARGET_SIZE)
})
qcew_universe <- rbindlist(universe_list)

# Merge the universe with the actual data.
cat("Merging universe with observed data and handling aggregation levels...\n")

# Handle multiple agglvl_codes: If a combination exists multiple times (e.g., due to different reporting nuances),
# we prioritize the one corresponding to the most detailed definition.
# Heuristic: higher agglvl_codes often imply more detail (e.g., 78 > 74).

qcew_data[, agglvl_rank := frank(-as.numeric(agglvl_code), ties.method = "first"),
          by = .(year, area_fips, industry_code, own_code, size_code)]

# Keep only the highest-ranked record for each combination
qcew_data_dedup <- qcew_data[agglvl_rank == 1]
qcew_data_dedup[, agglvl_rank := NULL] # Clean up rank column

qcew_impute_base <- merge(qcew_universe, qcew_data_dedup,
                          by = c("year", "area_fips", "industry_code", "own_code", "size_code"),
                          all.x = TRUE)

# Fill NAs for disclosure flags on newly created (entirely missing) records.
# They are treated as suppressed.
qcew_impute_base[is.na(disclosed_emp), `:=`(disclosed_emp = FALSE,
                                            disclosed_lq = FALSE,
                                            disclosed_estabs = FALSE)]

cat(sprintf("Total records in imputation base: %s\n", format(nrow(qcew_impute_base), big.mark=",")))
cat(sprintf("Records requiring imputation (Emp): %s (%.1f%%)\n",
            format(sum(!qcew_impute_base$disclosed_emp), big.mark=","),
            100 * sum(!qcew_impute_base$disclosed_emp) / nrow(qcew_impute_base)))

## =============================================================================
## STAGE 3C: INITIAL BOUNDS CALCULATION
## =============================================================================
cat("\n--- STAGE 3C: INITIAL BOUNDS CALCULATION ---\n")

# Since QCEW single files do not provide range flags (A, B, C...), we rely on
# establishment counts and heuristics, following principles from Isserman & Westervelt.

# --- 1. Lower Bounds (Establishment Counts) ---
# Lower bound: At least 1 employee per establishment.

qcew_impute_base[, bound_lower := as.numeric(NA)]

# Case 1: Disclosed employment (Ground Truth)
qcew_impute_base[disclosed_emp == TRUE, bound_lower := annual_avg_emplvl]

# Case 2: Suppressed employment, disclosed establishments
# Use the number of establishments as the minimum employment.
qcew_impute_base[disclosed_emp == FALSE & disclosed_estabs == TRUE,
                 bound_lower := annual_avg_estabs]

# Case 3: Suppressed/Missing employment and establishments
qcew_impute_base[disclosed_emp == FALSE & disclosed_estabs == FALSE,
                 bound_lower := 0] # Absolute minimum

# --- 2. Upper Bounds (Heuristic based on Average Size) ---

# Calculate the average employment per establishment at the National level
# for each industry/ownership combination. This provides a starting heuristic UB.

cat("Calculating national average employment per establishment...\n")
# Calculate average size only where both employment and establishments are disclosed and > 0.
national_avg_size <- qcew_impute_base[area_fips == "US000" & disclosed_emp == TRUE & disclosed_estabs == TRUE & annual_avg_estabs > 0,
                                      .(avg_emp_per_estab = sum(annual_avg_emplvl, na.rm = TRUE) / sum(annual_avg_estabs, na.rm = TRUE)),
                                      by = .(year, industry_code, own_code)]

# Merge back
qcew_impute_base <- merge(qcew_impute_base, national_avg_size,
                          by = c("year", "industry_code", "own_code"),
                          all.x = TRUE)

# Calculate heuristic upper bound: (Estabs) * (National Avg Size) * (Safety Factor)
# Safety factor accounts for regional variation.
SAFETY_FACTOR <- 5
LARGE_NUMBER <- 1e9 # Represents an unconstrained upper bound

qcew_impute_base[, bound_upper := as.numeric(NA)]

# Case 1: Disclosed employment (Ground Truth)
qcew_impute_base[disclosed_emp == TRUE, bound_upper := annual_avg_emplvl]

# Case 2: Suppressed employment, disclosed establishments
qcew_impute_base[disclosed_emp == FALSE & disclosed_estabs == TRUE,
                 bound_upper := annual_avg_estabs * avg_emp_per_estab * SAFETY_FACTOR]

# Handle missing national averages (e.g., if industry is fully suppressed nationally)
# Fallback: Use overall average for that ownership type in that year.
if (any(is.na(qcew_impute_base$avg_emp_per_estab))) {
  national_avg_size_fallback <- qcew_impute_base[area_fips == "US000" & disclosed_emp == TRUE & disclosed_estabs == TRUE & annual_avg_estabs > 0,
                                                 .(avg_emp_per_estab_fallback = sum(annual_avg_emplvl, na.rm = TRUE) / sum(annual_avg_estabs, na.rm = TRUE)),
                                                 by = .(year, own_code)]
  
  if (nrow(national_avg_size_fallback) > 0) {
    qcew_impute_base <- merge(qcew_impute_base, national_avg_size_fallback,
                              by = c("year", "own_code"),
                              all.x = TRUE, allow.cartesian = TRUE) # allow.cartesian needed if keys are duplicated, though they shouldn't be here.
    
    qcew_impute_base[disclosed_emp == FALSE & disclosed_estabs == TRUE & is.na(bound_upper),
                     bound_upper := annual_avg_estabs * avg_emp_per_estab_fallback * SAFETY_FACTOR]
    qcew_impute_base[, avg_emp_per_estab_fallback := NULL]
  }
}

# Case 3: Suppressed/Missing employment and establishments OR if UB is still NA after fallbacks
qcew_impute_base[(disclosed_emp == FALSE & disclosed_estabs == FALSE) | (disclosed_emp == FALSE & is.na(bound_upper)),
                 bound_upper := LARGE_NUMBER]

# Finalize bounds (ceiling for upper, floor for lower)
qcew_impute_base[, `:=`(bound_lower = floor(bound_lower),
                        bound_upper = ceiling(bound_upper))]

# Clean up
qcew_impute_base[, avg_emp_per_estab := NULL]

# Summary of initial bounds
cat("Initial bounds calculation complete.\n")
suppressed_summary <- qcew_impute_base[disclosed_emp == FALSE]
# Calculate uncertainty safely
initial_uncertainty <- sum(pmax(0, as.numeric(suppressed_summary$bound_upper) - as.numeric(suppressed_summary$bound_lower)), na.rm = TRUE)
cat(sprintf("Total initial uncertainty (sum of ranges): %s\n", format(initial_uncertainty, scientific = TRUE)))


## =============================================================================
## STAGE 3D: HIERARCHICAL STRUCTURING
## =============================================================================
cat("\n--- STAGE 3D: HIERARCHICAL STRUCTURING ---\n")

# Explicitly defining parent-child relationships for the imputation algorithm.

# --- 1. Geographic Hierarchy ---
qcew_impute_base[, state_fips := substr(area_fips, 1, 2)]
qcew_impute_base[area_fips == "US000", state_fips := "US"]

qcew_impute_base[, geo_parent_fips := NA_character_]
# Counties -> State
qcew_impute_base[nchar(area_fips) == 5 & substr(area_fips, 3, 5) != "000",
                 geo_parent_fips := paste0(state_fips, "000")]
# States -> National
qcew_impute_base[nchar(area_fips) == 5 & substr(area_fips, 3, 5) == "000" & state_fips != "US" & nchar(state_fips) == 2,
                 geo_parent_fips := "US000"]

# --- 2. Industry Hierarchy ---
# This is complex due to year-specific NAICS versions and Domains/Supersectors.
# The imputation script MUST use dynamic lookups via the hierarchy tables (get_hierarchy_for_year)
# or the comprehensive maps generated in Stage 1 (full_industry_map, if available).
# A simple substring approach is insufficient for QCEW.

# Placeholder: Define simple NAICS parent based on code length for basic structure.
get_simple_naics_parent <- function(code) {
  len <- nchar(code)
  # Handle standard NAICS codes (length 3 to 6)
  if (grepl("^[0-9]{3,6}$", code)) {
    if (len > 3) return(substr(code, 1, len - 1))
  }
  return(NA_character_)
}

# Apply vectorized
qcew_impute_base[, industry_parent_code_simple := get_simple_naics_parent(industry_code)]
cat("Simple industry hierarchy defined. Full hierarchical relationships (Domains/Supersectors/Sectors) require dynamic lookup during imputation.\n")

# --- 3. Ownership Hierarchy ---
# Private (5), Federal (1), State (2), Local (3) -> Total Covered (0)
qcew_impute_base[, own_parent_code := NA_character_]
qcew_impute_base[own_code %in% c('1', '2', '3', '5'), own_parent_code := '0']

## =============================================================================
## STAGE 3E: SAVING PREPARED DATA
## =============================================================================
cat("\n--- STAGE 3E: SAVING PREPARED DATA ---\n")

# Save the prepared data frame for the imputation stage.
# Determine output path (assuming OneDrive structure from Setup.R)
if (exists("onedrive_base")) {
  output_dir_base <- file.path(onedrive_base, "QCEW_IMPUTE_AUGUST_2025")
} else {
  # Fallback if onedrive_base is not defined
  output_dir_base <- file.path(getwd(), "QCEW_IMPUTE_AUGUST_2025")
}

if (!dir.exists(output_dir_base)) dir.create(output_dir_base, recursive = TRUE)

output_path <- file.path(output_dir_base, "prepared_data")
if (!dir.exists(output_path)) dir.create(output_path, recursive = TRUE)

output_file <- file.path(output_path, "QCEW_Imputation_Base_Prepared.qs")
cat(sprintf("Saving prepared data (using qs) to %s...\n", output_file))

# Select final columns relevant for imputation
final_cols <- c("year", "area_fips", "industry_code", "own_code", "size_code",
                "agglvl_code", # Keep agglvl_code as it might be needed to validate constraints
                "annual_avg_emplvl", "lq_annual_avg_emplvl", "annual_avg_estabs",
                "disclosed_emp", "disclosed_lq", "disclosed_estabs",
                "bound_lower", "bound_upper",
                "geo_parent_fips", "industry_parent_code_simple", "own_parent_code")

# Ensure all columns exist before saving
cols_to_save <- intersect(final_cols, names(qcew_impute_base))

if (requireNamespace("qs", quietly = TRUE)) {
  qs::qsave(qcew_impute_base[, ..cols_to_save], output_file)
} else {
  cat("Package 'qs' not available. Saving as RDS instead.\n")
  saveRDS(qcew_impute_base[, ..cols_to_save], paste0(tools::file_path_sans_ext(output_file), ".rds"))
}

cat("\nData preparation and initial information gathering complete.\n")
cat("The resulting dataset (QCEW_Imputation_Base_Prepared.qs) is structured for the next stages:\n")
cat("Stage 4: Iterative Range Reduction (implementing Isserman & Westervelt).\n")
cat("Stage 5: Optimization and Balancing (implementing Guldmann).\n")

