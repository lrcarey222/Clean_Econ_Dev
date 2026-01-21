
cat(strrep("=", 80), "\n\n")

# Garbage collection
gc()

cat("[SECTION 0] Environment setup and package loading...\n")

# Required packages
pkgs <- c(
  "data.table", "dplyr", "tidyr", "stringr", "stringi", "readr", "readxl",
  
  "sf", "tigris", "spdep", "Matrix", "RSpectra", "purrr", "httr", "curl",
  "magrittr", "future", "furrr", "knitr", "arrow", "lubridate",
  "foreach", "doParallel", "parallel", "tidycensus",
  "duckdb", "duckspatial"
)

# Install missing packages
invisible(lapply(pkgs, function(p) {
  if (!requireNamespace(p, quietly = TRUE)) {
    cat(sprintf("  Installing: %s\n", p))
    install.packages(p, repos = "https://cloud.r-project.org")
  }
}))

# Load packages
suppressPackageStartupMessages(invisible(lapply(pkgs, library, character.only = TRUE)))

# Set options
options(
  tigris_use_cache = FALSE,
  timeout = 600,
  scipen = 999,
  width = 120,
  pillar.width = 120,
  tibble.width = 120,
  error = NULL,
  warn = 1
)

if (identical(.Platform$OS.type, "windows")) {
  options(download.file.method = "libcurl")
}

# Disable S2 spherical geometry for spatial operations
sf::sf_use_s2(FALSE)

cat("  Packages loaded successfully.\n\n")

# ------------------------------------------------------------------------------
# CREATE TIMESTAMPED OUTPUT FOLDER (at start of script, used throughout)
# All intermediate and final outputs will be saved here
# ------------------------------------------------------------------------------

timestamp_str <- format(Sys.time(), "%Y%m%d_%H%M%S")
output_folder_name <- sprintf("tapestry_complexity_output_%s", timestamp_str)
output_folder_path <- file.path(getwd(), output_folder_name)

if (!dir.exists(output_folder_path)) {
  dir.create(output_folder_path, recursive = TRUE)
}

cat(sprintf("[OUTPUT FOLDER] Created: %s\n", output_folder_path))


# Copy this R script to the output folder for reproducibility
script_path <- if (exists("script_source_path")) {
  script_source_path
} else {
  # Try to find the script path from various sources
  tryCatch({
    # If running in RStudio
    if (Sys.getenv("RSTUDIO") == "1") {
      rstudioapi::getSourceEditorContext()$path
    } else {
      # If running via source() or Rscript
      args <- commandArgs(trailingOnly = FALSE)
      file_arg <- grep("--file=", args, value = TRUE)
      if (length(file_arg) > 0) {
        sub("--file=", "", file_arg)
      } else {
        NA_character_
      }
    }
  }, error = function(e) NA_character_)
}

if (!is.na(script_path) && file.exists(script_path)) {
  script_copy_path <- file.path(output_folder_path, basename(script_path))
  file.copy(script_path, script_copy_path, overwrite = TRUE)
  cat(sprintf("[OUTPUT FOLDER] Script copied: %s\n", basename(script_path)))
} else {
  cat("[OUTPUT FOLDER] Note: Could not determine script path for copying\n")
  # Create a placeholder file with timestamp
  writeLines(
    c(
      sprintf("# Tapestry Complexity Analysis"),
      sprintf("# Run timestamp: %s", timestamp_str),
      sprintf("# Output folder: %s", output_folder_path),
      "",
      "# Script source could not be automatically copied.",
      "# Please manually save a copy of the script used for this run."
    ),
    file.path(output_folder_path, "script_info.txt")
  )
}

cat("\n")

# ------------------------------------------------------------------------------
# Helper functions with enhanced debugging
# ------------------------------------------------------------------------------

dbg <- function(fmt, ...) {
  cat(
    format(Sys.time(), "[%H:%M:%S]"),
    tryCatch(sprintf(fmt, ...), error = function(e) paste(fmt, collapse = " ")),
    "\n"
  )
}

dbg_detail <- function(label, value) {
  if (is.numeric(value)) {
    cat(sprintf("    [DEBUG] %s: %s\n", label, format(value, big.mark = ",", scientific = FALSE)))
  } else {
    cat(sprintf("    [DEBUG] %s: %s\n", label, as.character(value)))
  }
}

# Enhanced debug function for matrices
dbg_matrix <- function(label, mat) {
  cat(sprintf("    [MATRIX DEBUG] %s:\n", label))
  cat(sprintf("      Dimensions: %d x %d\n", nrow(mat), ncol(mat)))
  cat(sprintf("      Non-zero entries: %d (%.2f%%)\n", 
              sum(mat != 0), 100 * sum(mat != 0) / length(mat)))
  if (nrow(mat) == ncol(mat)) {
    row_sums <- rowSums(mat)
    cat(sprintf("      Row sums: min=%.6f, max=%.6f, mean=%.6f\n",
                min(row_sums), max(row_sums), mean(row_sums)))
    if (all(abs(row_sums - 1) < 1e-6)) {
      cat("      ✓ Matrix is row-stochastic (rows sum to 1)\n")
    } else {
      cat(sprintf("      ⚠ Matrix is NOT row-stochastic (expected 1, got range [%.4f, %.4f])\n",
                  min(row_sums), max(row_sums)))
    }
  }
}

# Enhanced debug function for eigenvalues
dbg_eigenvalues <- function(label, eigenvalues, n_show = 10) {
  cat(sprintf("    [EIGENVALUE DEBUG] %s:\n", label))
  n <- min(n_show, length(eigenvalues))
  cat(sprintf("      Top %d eigenvalues: %s\n", n, 
              paste(sprintf("%.6f", head(sort(eigenvalues, decreasing = TRUE), n)), collapse = ", ")))
  n_near_one <- sum(abs(eigenvalues - 1) < 1e-4)
  if (n_near_one > 1) {
    cat(sprintf("      ⚠ WARNING: %d eigenvalues near 1 (possible disconnected components)\n", n_near_one))
  }
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
    "Workaround: Open this URL in a browser and save locally:\n    ",
    url, "\n"
  ))
}

cat("  Helper functions defined.\n\n")

# ------------------------------------------------------------------------------
# Parquet Export Helper Functions
# ------------------------------------------------------------------------------

# Simple parquet export function
export_to_parquet <- function(data, parquet_path) {
  if (is.null(data) || nrow(data) == 0) {
    dbg("    Skipping empty data: %s", basename(parquet_path))
    return(invisible(FALSE))
  }
  
  # Convert to data.frame if needed
  if (inherits(data, "data.table")) {
    data <- as.data.frame(data)
  }
  
  # Drop geometry if present but not sf (prevents issues)
  if ("geometry" %in% names(data) && !inherits(data, "sf")) {
    data$geometry <- NULL
  }
  
  arrow::write_parquet(data, parquet_path)
  return(invisible(TRUE))
}

cat("  Parquet helper functions defined.\n\n")

# ==============================================================================
# QC / DEBUG HARNESS (DROP-IN) — writes small artifacts to output_folder_path/qc_artifacts
# Place RIGHT AFTER: cat("  Helper functions defined.\n\n")
# ==============================================================================

QC_ON <- TRUE
QC_WRITE <- FALSE
QC_DIR <- file.path(output_folder_path, "qc_artifacts")
if (QC_WRITE && !dir.exists(QC_DIR)) dir.create(QC_DIR, recursive = TRUE)

qc_write_csv <- function(dt, fname) {
  if (!QC_WRITE) return(invisible(NULL))
  data.table::fwrite(data.table::as.data.table(dt), file.path(QC_DIR, fname))
}
qc_write_rds <- function(obj, fname) {
  if (!QC_WRITE) return(invisible(NULL))
  saveRDS(obj, file.path(QC_DIR, fname))
}
qc_assert <- function(cond, msg) {
  if (!isTRUE(cond)) warning(paste0("[QC] ", msg), call. = FALSE)
}

qc_key_dupes <- function(dt, keys, label = "") {
  dt <- data.table::as.data.table(dt)
  dup <- dt[, .N, by = keys][N > 1]
  if (nrow(dup) > 0) {
    dbg("⚠ [QC] Duplicate keys detected (%s): %d groups duplicated", label, nrow(dup))
    qc_write_csv(dup[order(-N)][1:min(.N, 200)], sprintf("qc_dupe_keys_%s.csv", label))
  } else {
    dbg("✓ [QC] No duplicate keys (%s)", label)
  }
  invisible(dup)
}

qc_totals_snapshot <- function(dt, value_col, geo_col = "county_geoid", state_col = "state_fips") {
  dt <- data.table::as.data.table(dt)
  list(
    grand = dt[, sum(get(value_col), na.rm = TRUE)],
    by_geo   = dt[, .(emp = sum(get(value_col), na.rm = TRUE)), by = geo_col],
    by_state = dt[, .(emp = sum(get(value_col), na.rm = TRUE)), by = state_col]
  )
}

qc_compare_totals <- function(before, after, key_dt = "by_geo", key_col,
                              tol_abs = 0L, tol_rel = 1e-9, label = "") {
  b <- data.table::copy(before[[key_dt]])
  a <- data.table::copy(after[[key_dt]])
  setnames(b, "emp", "emp_before")
  setnames(a, "emp", "emp_after")
  m <- merge(b, a, by = key_col, all = TRUE)
  m[is.na(emp_before), emp_before := 0]
  m[is.na(emp_after),  emp_after  := 0]
  m[, `:=`(
    diff = emp_after - emp_before,
    rel  = data.table::fifelse(emp_before == 0, NA_real_, (emp_after - emp_before) / emp_before)
  )]
  bad <- m[abs(diff) > tol_abs & (is.na(rel) | abs(rel) > tol_rel)]
  dbg("[QC] %s totals compare (%s by %s): %d/%d outside tolerance",
      label, key_dt, key_col, nrow(bad), nrow(m))
  if (nrow(bad) > 0) qc_write_csv(bad[order(-abs(diff))][1:min(.N, 200)],
                                  sprintf("qc_totals_diff_%s_%s.csv", label, key_dt))
  invisible(bad)
}

qc_check_naics_join <- function(dt, naics6_col = "naics6_code",
                                cols_needed = c("naics5_code","naics4_code","naics3_code"),
                                label = "") {
  dt <- data.table::as.data.table(dt)
  miss <- dt[, lapply(.SD, \(x) sum(is.na(x) | x == "")), .SDcols = cols_needed]
  dbg("[QC] %s missing hierarchy fields: %s",
      label, paste(names(miss), unlist(miss), sep="=", collapse=", "))
  bad_codes <- unique(dt[Reduce(`|`, lapply(cols_needed, \(cc) is.na(get(cc)) | get(cc) == "")), get(naics6_col)])
  if (length(bad_codes) > 0) {
    dbg("⚠ [QC] %s NAICS6 codes missing hierarchy mapping: %d", label, length(bad_codes))
    qc_write_csv(data.table::data.table(naics6_code = bad_codes),
                 sprintf("qc_missing_hierarchy_%s.csv", label))
  }
  invisible(bad_codes)
}

qc_lq_sanity <- function(dt, lq_col, emp_loc_col, tot_loc_col, emp_nat_col, tot_nat_col,
                         label = "", n_sample = 200L) {
  dt <- data.table::as.data.table(dt)
  dt[, `:=`(
    is_inf = is.infinite(get(lq_col)),
    is_nan = is.nan(get(lq_col)),
    is_neg = get(lq_col) < 0
  )]
  dbg("[QC] %s %s: NA=%d inf=%d nan=%d neg=%d",
      label, lq_col, sum(is.na(dt[[lq_col]])), sum(dt$is_inf), sum(dt$is_nan), sum(dt$is_neg))
  
  x <- dt[is.finite(get(lq_col)) & !is.na(get(lq_col)), get(lq_col)]
  if (length(x) > 0) {
    qs <- quantile(x, probs = c(0, .5, .9, .99, .999), na.rm = TRUE, names = TRUE)
    dbg("[QC] %s %s quantiles: %s", label, lq_col,
        paste(sprintf("%s=%.3f", names(qs), qs), collapse=", "))
    dbg("[QC] %s %s max: %.3f", label, lq_col, max(x, na.rm = TRUE))
  }
  
  cand <- dt[is.finite(get(lq_col)) & !is.na(get(lq_col)) &
               get(tot_loc_col) > 0 & get(emp_nat_col) > 0 & get(tot_nat_col) > 0]
  if (nrow(cand) == 0) return(invisible(NULL))
  ss <- cand[sample.int(.N, min(n_sample, .N))]
  ss[, lq_recalc := (get(emp_loc_col) / get(tot_loc_col)) / (get(emp_nat_col) / get(tot_nat_col))]
  ss[, rel_err := abs(lq_recalc - get(lq_col)) / pmax(1e-12, abs(get(lq_col)))]
  dbg("[QC] %s %s spot-check: median rel_err=%.3e | p95 rel_err=%.3e | max rel_err=%.3e",
      label, lq_col,
      median(ss$rel_err, na.rm = TRUE),
      quantile(ss$rel_err, 0.95, na.rm = TRUE),
      max(ss$rel_err, na.rm = TRUE))
  
  qc_write_csv(ss[order(-rel_err)][1:min(.N, 50)],
               sprintf("qc_lq_spotcheck_%s_%s.csv", label, lq_col))
  invisible(ss)
}

qc_geo_coverage <- function(present_ids, reference_ids, label = "", n_show = 30L) {
  present_ids   <- unique(as.character(present_ids))
  reference_ids <- unique(as.character(reference_ids))
  missing <- setdiff(reference_ids, present_ids)
  extra   <- setdiff(present_ids, reference_ids)
  dbg("[QC] %s coverage: present=%d ref=%d missing=%d extra=%d",
      label, length(present_ids), length(reference_ids), length(missing), length(extra))
  if (length(missing) > 0) qc_write_csv(data.table::data.table(missing_id = head(missing, n_show)),
                                        sprintf("qc_missing_%s.csv", label))
  if (length(extra) > 0)   qc_write_csv(data.table::data.table(extra_id   = head(extra, n_show)),
                                        sprintf("qc_extra_%s.csv", label))
  invisible(list(missing = missing, extra = extra))
}

qc_phi_invariants <- function(M, phi, label = "", tol_sym = 1e-10) {
  U <- Matrix::crossprod(M)
  ubq <- Matrix::colSums(M)
  diagU <- Matrix::diag(U)
  max_diff <- max(abs(diagU - ubq))
  dbg("[QC] %s diag(M^T M) vs ubiquity max_abs_diff=%.3e", label, max_diff)
  
  sym_err <- max(abs(phi - t(phi)))
  rng <- range(phi, na.rm = TRUE)
  diag_rng <- range(diag(phi), na.rm = TRUE)
  dbg("[QC] %s phi: symmetry_err=%.3e range=[%.3f, %.3f] diag_range=[%.3f, %.3f]",
      label, sym_err, rng[1], rng[2], diag_rng[1], diag_rng[2])
  
  qc_assert(sym_err < tol_sym, sprintf("%s phi not symmetric (max|phi-t(phi)|=%.3e)", label, sym_err))
  qc_assert(rng[1] >= -1e-12 && rng[2] <= 1 + 1e-12,
            sprintf("%s phi outside [0,1] (range=%.4f..%.4f)", label, rng[1], rng[2]))
  
  deg <- rowSums(phi != 0)
  iso <- names(which(deg == 0))
  if (length(iso) > 0) {
    dbg("⚠ [QC] %s phi isolated industries (no nonzero links): %d", label, length(iso))
    qc_write_csv(data.table::data.table(industry_code = head(iso, 200)),
                 sprintf("qc_phi_isolates_%s.csv", label))
  }
  invisible(NULL)
}


# ==============================================================================
# SECTION 1: CONNECTICUT COUNTY -> PLANNING REGION CROSSWALK
# ==============================================================================

cat("[SECTION 1] Building Connecticut county -> planning region crosswalk...\n")

# Constants
CT_STATE_ABBR <- "CT"
LODES_YEARS_CT <- 2010:2023
TARGET_CRS_CT <- 5070  # NAD83 / Conus Albers

# NAICS Sector Mapping for LODES
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

# Download CT census blocks (2020)
dbg("Downloading CT census blocks (2020)...")
ct_blocks_2020 <- tigris::blocks(state = CT_STATE_ABBR, year = 2020, class = "sf")
blk_col <- intersect(c("GEOID20", "GEOID10", "GEOID"), names(ct_blocks_2020))[1]

ct_census_blocks <- ct_blocks_2020 %>%
  dplyr::select(all_of(blk_col), geometry) %>%
  dplyr::rename(block_geoid = all_of(blk_col)) %>%
  dplyr::mutate(block_geoid = as.character(block_geoid)) %>%
  sf::st_transform(crs = TARGET_CRS_CT)

dbg("  CT blocks downloaded: %d", nrow(ct_census_blocks))
dbg_detail("First block GEOID", head(ct_census_blocks$block_geoid, 1))

# Download LODES WAC data for CT
dbg("Downloading LODES WAC data for CT (2010-2023)...")

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
  f <- sprintf("ct_wac_S000_JT00_%d.csv.gz", yr)
  dbg_detail(sprintf("Loading LODES %d", yr), f)
  w <- tryCatch(read_gz(paste0(wac_base_url_ct, f)), error = function(e) NULL)
  if (is.null(w) || !("w_geocode" %in% names(w))) return(NULL)
  
  cols_to_keep <- c("w_geocode", "C000", paste0("CNS", sprintf("%02d", 1:20)))
  cols_present <- intersect(cols_to_keep, names(w))
  
  w %>%
    dplyr::select(all_of(cols_present)) %>%
    dplyr::rename(block_geoid = w_geocode) %>%
    dplyr::mutate(block_geoid = as.character(block_geoid), year = as.integer(yr))
}))

dbg("  LODES records: %d", nrow(wac_all_years_ct))

# Rename CNS columns to NAICS sector codes
cns_rename_map <- setNames(NAICS_SECTOR_MAP$cns_code, NAICS_SECTOR_MAP$naics_sector)
present_cns_codes <- intersect(as.character(cns_rename_map), names(wac_all_years_ct))
cns_rename_map_filtered <- cns_rename_map[cns_rename_map %in% present_cns_codes]
wac_all_years_ct <- wac_all_years_ct %>% dplyr::rename(!!!cns_rename_map_filtered)

# Download county shapefiles
dbg("Downloading CT county shapefiles...")
connecticut_old_counties <- tigris::counties(state = CT_STATE_ABBR, year = 2020, class = "sf") %>%
  dplyr::select(GEOID, NAMELSAD) %>%
  dplyr::rename(old_county_geoid = GEOID, old_county_name = NAMELSAD) %>%
  sf::st_transform(crs = TARGET_CRS_CT)

connecticut_planning_regions <- tigris::counties(state = CT_STATE_ABBR, year = 2024, class = "sf") %>%
  dplyr::select(GEOID, NAMELSAD) %>%
  dplyr::rename(new_region_geoid = GEOID, new_region_name = NAMELSAD) %>%
  sf::st_transform(crs = TARGET_CRS_CT)

dbg("  Old counties: %d | New planning regions: %d", 
    nrow(connecticut_old_counties), nrow(connecticut_planning_regions))

# Pre-compute block geography assignments
dbg("Computing block-to-geography assignments...")
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

# Get sector columns
sector_cols <- NAICS_SECTOR_MAP$naics_sector[NAICS_SECTOR_MAP$naics_sector %in% names(wac_all_years_ct)]

# Create complete block-year panel
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

# Zero-fill
for (col in sector_cols) {
  wac_complete_ct[[col]][is.na(wac_complete_ct[[col]])] <- 0
}

wac_with_geography_ct <- wac_complete_ct %>%
  dplyr::left_join(block_geography_ct, by = "block_geoid")

# Compute allocation factors
dbg("Computing CT allocation factors by sector and year...")
all_crosswalks_ct <- list()

for (yr in LODES_YEARS_CT) {
  for (sector in sector_cols) {
    year_data <- wac_with_geography_ct %>% dplyr::filter(year == yr)
    
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

ct_employment_crosswalk_by_sector <- dplyr::bind_rows(all_crosswalks_ct) %>%
  dplyr::arrange(year, naics_sector, from_geoid, to_geoid)

# Add names
region_names_ct <- connecticut_planning_regions %>%
  sf::st_drop_geometry() %>%
  dplyr::transmute(to_geoid = new_region_geoid, to_name = new_region_name)

county_names_ct <- connecticut_old_counties %>%
  sf::st_drop_geometry() %>%
  dplyr::transmute(from_geoid = old_county_geoid, from_name = old_county_name)

ct_employment_crosswalk_by_sector_named <- ct_employment_crosswalk_by_sector %>%
  dplyr::left_join(county_names_ct, by = "from_geoid") %>%
  dplyr::left_join(region_names_ct, by = "to_geoid") %>%
  dplyr::left_join(NAICS_SECTOR_MAP %>% dplyr::select(naics_sector, sector_name), by = "naics_sector") %>%
  dplyr::mutate(sector_code_for_allocation_mapping = dplyr::if_else(naics_sector == "00", "00", naics_sector)) %>%
  dplyr::select(year, from_geoid, from_name, to_geoid, to_name, naics_sector, sector_name,
                sector_code_for_allocation_mapping, from_geoid_sector_jobs_total, afact)

dbg("CT crosswalk complete: %d rows", nrow(ct_employment_crosswalk_by_sector_named))

if (QC_ON) {
  x <- data.table::as.data.table(ct_employment_crosswalk_by_sector_named)
  # duplicates on the supposed key
  dup <- x[, .N, by = .(year, from_geoid, to_geoid, naics_sector)][N > 1]
  if (nrow(dup) > 0) {
    dbg("⚠ [QC] CT xwalk duplicates on (year,from,to,sector): %d", nrow(dup))
    qc_write_csv(dup[order(-N)], "qc_ct_xwalk_dupes.csv")
  }
  
  # afact sums should be ~1 within (year,from,sector) whenever defined
  sums <- x[!is.na(afact), .(afact_sum = sum(afact), n_to = .N), by = .(year, from_geoid, naics_sector)]
  bad  <- sums[abs(afact_sum - 1) > 1e-6]
  dbg("[QC] CT xwalk afact_sum != 1: %d groups", nrow(bad))
  if (nrow(bad) > 0) qc_write_csv(bad[order(-abs(afact_sum - 1))][1:min(.N, 200)], "qc_ct_xwalk_afact_sum_bad.csv")
}


# Create old CT county to new CT CBSA mapping (for validation)
ct_counties_2021 <- tigris::counties(state = "CT", year = 2021, cb = FALSE, class = "sf") %>%
  dplyr::transmute(county_geoid_2021 = GEOID, county_name_2021 = NAMELSAD)

ct_county_cbsa_2024_check <- tigris::counties(state = "CT", year = 2024, cb = FALSE, class = "sf") %>%
  dplyr::transmute(county_geoid_2024 = GEOID, county_name_2024 = NAMELSAD, cbsa_2024_geoid = CBSAFP) %>%
  dplyr::mutate(county_in_cbsa_2024 = cbsa_2024_geoid != "")

old_ct_county_to_new_ct_cbsa <- ct_counties_2021

cat("  CT crosswalk construction complete.\n\n")


# ==============================================================================
# SECTION 2: ALASKA VALDEZ-CORDOVA CROSSWALK
# ==============================================================================

cat("[SECTION 2] Building Alaska Valdez-Cordova crosswalk...\n")

# Constants
STATE_ABBR_LODES_AK <- "ak"
STATE_ABBR_TIGRIS_AK <- "AK"
LODES_YEARS_AK <- 2010:2016
TARGET_CRS_AK <- 3338  # Alaska Albers
OLD_COUNTY_FIPS_AK <- "02261"
NEW_REGION_FIPS_AK <- c("02063", "02066")

# Download AK blocks
dbg("Downloading AK census blocks (2020)...")
ak_blocks_2020 <- tigris::blocks(state = STATE_ABBR_TIGRIS_AK, year = 2020, class = "sf")
blk_col_ak <- intersect(c("GEOID20", "GEOID10", "GEOID"), names(ak_blocks_2020))[1]

ak_census_blocks <- ak_blocks_2020 %>%
  dplyr::select(all_of(blk_col_ak), geometry) %>%
  dplyr::rename(block_geoid = all_of(blk_col_ak)) %>%
  dplyr::mutate(block_geoid = as.character(block_geoid)) %>%
  sf::st_transform(crs = TARGET_CRS_AK)

dbg("  AK blocks downloaded: %d", nrow(ak_census_blocks))

# Download LODES WAC data for AK
dbg("Downloading LODES WAC data for AK (2010-2016)...")
wac_base_url_ak <- sprintf("https://lehd.ces.census.gov/data/lodes/LODES8/%s/wac/", STATE_ABBR_LODES_AK)

wac_all_years_ak <- dplyr::bind_rows(lapply(LODES_YEARS_AK, function(yr) {
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

dbg("  LODES records: %d", nrow(wac_all_years_ak))

# Rename columns
present_cns_codes_ak <- intersect(as.character(cns_rename_map), names(wac_all_years_ak))
cns_rename_map_filtered_ak <- cns_rename_map[cns_rename_map %in% present_cns_codes_ak]
wac_all_years_ak <- wac_all_years_ak %>% dplyr::rename(!!!cns_rename_map_filtered_ak)

# Download county shapefiles
dbg("Downloading AK county shapefiles...")
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

# Pre-compute block geography assignments
dbg("Computing block-to-geography assignments...")
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

N_BLOCKS_IN_VC <- nrow(block_geography_ak)
dbg("  Blocks in Valdez-Cordova area: %d", N_BLOCKS_IN_VC)

# Get sector columns for AK
sector_cols_ak <- NAICS_SECTOR_MAP$naics_sector[NAICS_SECTOR_MAP$naics_sector %in% names(wac_all_years_ak)]

# Create complete block-year panel for AK
complete_block_year_panel_ak <- tidyr::expand_grid(
  block_geoid = block_geography_ak$block_geoid,
  year = LODES_YEARS_AK
)

wac_complete_ak <- complete_block_year_panel_ak %>%
  dplyr::left_join(
    wac_all_years_ak %>% dplyr::select(block_geoid, year, all_of(sector_cols_ak)),
    by = c("block_geoid", "year")
  )

# Zero-fill
for (col in sector_cols_ak) {
  wac_complete_ak[[col]][is.na(wac_complete_ak[[col]])] <- 0
}

wac_with_geography_ak <- wac_complete_ak %>%
  dplyr::left_join(block_geography_ak, by = "block_geoid")

# Compute allocation factors for AK
dbg("Computing AK allocation factors by sector and year...")
all_crosswalks_ak <- list()

for (yr in LODES_YEARS_AK) {
  for (sector in sector_cols_ak) {
    year_data <- wac_with_geography_ak %>% dplyr::filter(year == yr)
    
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

valdez_cordova_crosswalk_by_sector <- dplyr::bind_rows(all_crosswalks_ak) %>%
  dplyr::arrange(year, naics_sector, from_geoid, to_geoid)

# Add names
region_names_ak <- tibble::tribble(
  ~to_geoid, ~to_name,
  "02063", "Chugach Census Area",
  "02066", "Copper River Census Area"
)

valdez_cordova_crosswalk_by_sector_named <- valdez_cordova_crosswalk_by_sector %>%
  dplyr::left_join(region_names_ak, by = "to_geoid") %>%
  dplyr::left_join(NAICS_SECTOR_MAP %>% dplyr::select(naics_sector, sector_name), by = "naics_sector") %>%
  dplyr::mutate(
    from_name = "Valdez-Cordova Census Area",
    sector_code_for_allocation_mapping = dplyr::if_else(naics_sector == "00", "00", naics_sector)
  ) %>%
  dplyr::select(year, from_geoid, from_name, to_geoid, to_name, naics_sector, sector_name,
                sector_code_for_allocation_mapping, from_geoid_sector_jobs_total, afact)

dbg("AK crosswalk complete: %d rows", nrow(valdez_cordova_crosswalk_by_sector_named))

if (QC_ON) {
  x <- data.table::as.data.table(valdez_cordova_crosswalk_by_sector_named)
  dup <- x[, .N, by = .(year, from_geoid, to_geoid, naics_sector)][N > 1]
  if (nrow(dup) > 0) {
    dbg("⚠ [QC] AK xwalk duplicates on (year,from,to,sector): %d", nrow(dup))
    qc_write_csv(dup[order(-N)], "qc_ak_xwalk_dupes.csv")
  }
  
  sums <- x[!is.na(afact), .(afact_sum = sum(afact), n_to = .N), by = .(year, from_geoid, naics_sector)]
  bad  <- sums[abs(afact_sum - 1) > 1e-6]
  dbg("[QC] AK xwalk afact_sum != 1: %d groups", nrow(bad))
  if (nrow(bad) > 0) qc_write_csv(bad[order(-abs(afact_sum - 1))][1:min(.N, 200)], "qc_ak_xwalk_afact_sum_bad.csv")
}

cat("  AK crosswalk construction complete.\n\n")


# ==============================================================================
# SECTION 3: NAICS HIERARCHY CROSSWALK (UNMODIFIED)
# ==============================================================================

cat("[SECTION 3] Loading NAICS hierarchy crosswalk...\n")

naics_hierarchy_xwalk_link <- "https://www.bls.gov/cew/classifications/industry/qcew-naics-hierarchy-crosswalk.xlsx"
naics_hierarchy_xwalk_path <- file.path(tempdir(), "qcew-naics-hierarchy-crosswalk.xlsx")
download_bls_excel(naics_hierarchy_xwalk_link, naics_hierarchy_xwalk_path)

naics_hierarchy_sheets <- readxl::excel_sheets(naics_hierarchy_xwalk_path)
naics_hierarchy_xwalk <- purrr::map_dfr(
  naics_hierarchy_sheets,
  ~ readxl::read_excel(naics_hierarchy_xwalk_path, sheet = .x) %>%
    dplyr::mutate(sheet_name = .x) %>%
    dplyr::mutate(across(everything(), as.character))
)

dbg("NAICS hierarchy crosswalk loaded: %d rows", nrow(naics_hierarchy_xwalk))

# ------------------------------------------------------------------------------
# NAICS VERSION MAPPING: Tapestry Year -> NAICS Version
# - 2022-2024: NAICS 2022 (v2022)
# - 2017-2021: NAICS 2017 (v2017)
# - 2012-2016: NAICS 2012 (v2012)
# - 2010-2011: NAICS 2007 (v2007)
# ------------------------------------------------------------------------------

NAICS_VERSION_MAP <- tibble::tribble(
  ~tapestry_year_min, ~tapestry_year_max, ~naics_version,
  2022L, 2024L, "v2022",
  2017L, 2021L, "v2017",
  2012L, 2016L, "v2012",
  2010L, 2011L, "v2007"
)

get_naics_version <- function(year) {
  for (i in seq_len(nrow(NAICS_VERSION_MAP))) {
    if (year >= NAICS_VERSION_MAP$tapestry_year_min[i] && 
        year <= NAICS_VERSION_MAP$tapestry_year_max[i]) {
      return(NAICS_VERSION_MAP$naics_version[i])
    }
  }
  return("v2022")  # Default fallback
}

dbg("NAICS version mapping defined:")
for (i in seq_len(nrow(NAICS_VERSION_MAP))) {
  dbg("  %d-%d -> %s", NAICS_VERSION_MAP$tapestry_year_min[i], 
      NAICS_VERSION_MAP$tapestry_year_max[i], NAICS_VERSION_MAP$naics_version[i])
}

# ------------------------------------------------------------------------------
# Create VERSION-SPECIFIC NAICS6 to sector mappings (for CT/AK allocation)
# ------------------------------------------------------------------------------

naics6_to_sector_mappings <- list()

for (nv in c("v2022", "v2017", "v2012", "v2007")) {
  mapping <- naics_hierarchy_xwalk %>%
    dplyr::filter(sheet_name == nv) %>%
    dplyr::select(naics6_code, sector_code, sector_title) %>%
    dplyr::filter(!is.na(naics6_code), !is.na(sector_code)) %>%
    dplyr::distinct(naics6_code, .keep_all = TRUE) %>%
    dplyr::mutate(
      sector_code_for_allocation_mapping = dplyr::if_else(naics6_code == "999999", "00", sector_code)
    )
  naics6_to_sector_mappings[[nv]] <- mapping
  dbg("NAICS6 to sector mapping for %s: %d codes", nv, nrow(mapping))
}

# Helper function to get sector mapping for a given year
get_naics6_to_sector_mapping <- function(year) {
  version <- get_naics_version(year)
  if (!version %in% names(naics6_to_sector_mappings)) {
    warning(sprintf("NAICS version %s not found for sector mapping, falling back to v2022", version))
    version <- "v2022"
  }
  return(naics6_to_sector_mappings[[version]])
}

# Default for backward compatibility
naics6_to_sector_mapping <- naics6_to_sector_mappings[["v2022"]]
dbg("Default (v2022) sector mapping created for backward compatibility")

# ------------------------------------------------------------------------------
# Create VERSION-SPECIFIC title lookups at all NAICS levels (6, 5, 4, 3 digit)
# Each NAICS version (v2022, v2017, v2012, v2007) has its own lookup tables
# ------------------------------------------------------------------------------

naics_versions <- c("v2022", "v2017", "v2012", "v2007")

# Store all lookups in nested lists by version
naics_title_lookups <- list()
naics_hierarchy_lookups <- list()

for (nv in naics_versions) {
  dbg("Creating lookups for NAICS version: %s", nv)
  
  version_data <- naics_hierarchy_xwalk %>%
    dplyr::filter(sheet_name == nv)
  
  if (nrow(version_data) == 0) {
    warning(sprintf("No data found for NAICS version %s in crosswalk", nv))
    next
  }
  
  # NAICS6 title lookup
  naics6_title <- version_data %>%
    dplyr::select(naics6_code, naics6_title) %>%
    dplyr::filter(!is.na(naics6_code), !is.na(naics6_title)) %>%
    dplyr::distinct(naics6_code, .keep_all = TRUE)
  
  # NAICS5 title lookup
  naics5_title <- version_data %>%
    dplyr::select(naics5_code, naics5_title) %>%
    dplyr::filter(!is.na(naics5_code), !is.na(naics5_title)) %>%
    dplyr::distinct(naics5_code, .keep_all = TRUE)
  
  # NAICS4 title lookup
  naics4_title <- version_data %>%
    dplyr::select(naics4_code, naics4_title) %>%
    dplyr::filter(!is.na(naics4_code), !is.na(naics4_title)) %>%
    dplyr::distinct(naics4_code, .keep_all = TRUE)
  
  # NAICS3 title lookup
  naics3_title <- version_data %>%
    dplyr::select(naics3_code, naics3_title) %>%
    dplyr::filter(!is.na(naics3_code), !is.na(naics3_title)) %>%
    dplyr::distinct(naics3_code, .keep_all = TRUE)
  
  # NAICS6 to hierarchy mapping (for aggregating up to 5/4/3 digit)
  naics6_hierarchy <- version_data %>%
    dplyr::select(naics6_code, naics5_code, naics4_code, naics3_code) %>%
    dplyr::filter(!is.na(naics6_code)) %>%
    dplyr::distinct(naics6_code, .keep_all = TRUE)
  
  # Store in nested structure
  naics_title_lookups[[nv]] <- list(
    naics6 = naics6_title,
    naics5 = naics5_title,
    naics4 = naics4_title,
    naics3 = naics3_title
  )
  
  naics_hierarchy_lookups[[nv]] <- naics6_hierarchy
  
  dbg("  %s: NAICS6=%d, NAICS5=%d, NAICS4=%d, NAICS3=%d titles; %d hierarchy mappings",
      nv, nrow(naics6_title), nrow(naics5_title), nrow(naics4_title), nrow(naics3_title),
      nrow(naics6_hierarchy))
}

# Helper function to get the correct lookup for a given year
get_naics_title_lookup <- function(year, naics_level) {
  version <- get_naics_version(year)
  if (!version %in% names(naics_title_lookups)) {
    warning(sprintf("NAICS version %s not found, falling back to v2022", version))
    version <- "v2022"
  }
  return(naics_title_lookups[[version]][[naics_level]])
}

get_naics_hierarchy_lookup <- function(year) {
  version <- get_naics_version(year)
  if (!version %in% names(naics_hierarchy_lookups)) {
    warning(sprintf("NAICS version %s not found, falling back to v2022", version))
    version <- "v2022"
  }
  return(naics_hierarchy_lookups[[version]])
}

# For backward compatibility, create default lookups pointing to v2022
# (These are used in places where year context isn't available)
naics6_title_lookup <- naics_title_lookups[["v2022"]]$naics6
naics5_title_lookup <- naics_title_lookups[["v2022"]]$naics5
naics4_title_lookup <- naics_title_lookups[["v2022"]]$naics4
naics3_title_lookup <- naics_title_lookups[["v2022"]]$naics3
naics6_to_hierarchy <- naics_hierarchy_lookups[["v2022"]]

dbg("Default (v2022) lookups created for backward compatibility")

cat("\n  [GLIMPSE] naics_hierarchy_xwalk:\n")
print(glimpse(naics_hierarchy_xwalk))

# ------------------------------------------------------------------------------
# NAICS HIERARCHY MODIFICATIONS: Custom Aggregations
# ------------------------------------------------------------------------------
# 
# This section defines custom aggregation rules for:
# 1. Three-digit rollups treated as six-digit codes:
#    - 812000/81200/8120: Personal and Laundry Services (aggregates all 812XXX)
#    - 111000/11100/1110: Crop Production (aggregates all 111XXX)
#    - 112000/11200/1120: Animal Production (aggregates all 112XXX)
#
# 2. BLS-only specialty trade contractor codes:
#    - Codes ending in 1 (residential) and 2 (nonresidential) are collapsed
#      to the standard code ending in 0
#    - Example: 238111 + 238112 → 238110
# ------------------------------------------------------------------------------

cat("[SECTION 3b] Applying NAICS hierarchy modifications...\n")

# Define aggregation rules
NAICS_AGGREGATION_RULES <- list(
  
  # Three-digit rollup industries (treated as 6-digit codes)
  three_digit_rollups = list(
    list(
      target_naics6 = "812000",
      target_naics5 = "81200",
      target_naics4 = "8120",
      target_naics3 = "812",
      target_title = "Personal and Laundry Services",
      source_pattern = "^812[0-9]{3}$"  # Matches 812XXX
    ),
    list(
      target_naics6 = "111000",
      target_naics5 = "11100",
      target_naics4 = "1110",
      target_naics3 = "111",
      target_title = "Crop Production",
      source_pattern = "^111[0-9]{3}$"  # Matches 111XXX
    ),
    list(
      target_naics6 = "112000",
      target_naics5 = "11200",
      target_naics4 = "1120",
      target_naics3 = "112",
      target_title = "Animal Production and Aquaculture",
      source_pattern = "^112[0-9]{3}$"  # Matches 112XXX
    ), 
    list(
      target_naics6 = "513190",
      target_naics5 = "51319",
      target_naics4 = "5131",
      target_naics3 = "513",
      target_title = "Other Publishers",
      source_pattern = "^51319[0-9]$"  # Matches 513191, 513199
    )
  ),
  
  
  # BLS-only specialty trade contractor codes (238XX1/238XX2 → 238XX0)
  # These are codes that BLS uses but don't appear in standard NAICS
  bls_contractor_mappings = tibble::tribble(
    ~source_code, ~target_code, ~target_title,
    "238111", "238110", "Poured Concrete Foundation and Structure Contractors",
    "238112", "238110", "Poured Concrete Foundation and Structure Contractors",
    "238121", "238120", "Structural Steel and Precast Concrete Contractors",
    "238122", "238120", "Structural Steel and Precast Concrete Contractors",
    "238131", "238130", "Framing Contractors",
    "238132", "238130", "Framing Contractors",
    "238141", "238140", "Masonry Contractors",
    "238142", "238140", "Masonry Contractors",
    "238151", "238150", "Glass and Glazing Contractors",
    "238152", "238150", "Glass and Glazing Contractors",
    "238161", "238160", "Roofing Contractors",
    "238162", "238160", "Roofing Contractors",
    "238171", "238170", "Siding Contractors",
    "238172", "238170", "Siding Contractors",
    "238191", "238190", "Other Foundation, Structure, and Building Exterior Contractors",
    "238192", "238190", "Other Foundation, Structure, and Building Exterior Contractors",
    "238211", "238210", "Electrical Contractors and Other Wiring Installation Contractors",
    "238212", "238210", "Electrical Contractors and Other Wiring Installation Contractors",
    "238221", "238220", "Plumbing, Heating, and Air-Conditioning Contractors",
    "238222", "238220", "Plumbing, Heating, and Air-Conditioning Contractors",
    "238291", "238290", "Other Building Equipment Contractors",
    "238292", "238290", "Other Building Equipment Contractors",
    "238311", "238310", "Drywall and Insulation Contractors",
    "238312", "238310", "Drywall and Insulation Contractors",
    "238321", "238320", "Painting and Wall Covering Contractors",
    "238322", "238320", "Painting and Wall Covering Contractors",
    "238331", "238330", "Flooring Contractors",
    "238332", "238330", "Flooring Contractors",
    "238341", "238340", "Tile and Terrazzo Contractors",
    "238342", "238340", "Tile and Terrazzo Contractors",
    "238351", "238350", "Finish Carpentry Contractors",
    "238352", "238350", "Finish Carpentry Contractors",
    "238391", "238390", "Other Building Finishing Contractors",
    "238392", "238390", "Other Building Finishing Contractors",
    "238911", "238910", "Site Preparation Contractors",
    "238912", "238910", "Site Preparation Contractors",
    "238991", "238990", "All Other Specialty Trade Contractors",
    "238992", "238990", "All Other Specialty Trade Contractors"
  )
)

# Add synthetic rows to NAICS hierarchy crosswalk for three-digit rollup industries
# These need to exist in the hierarchy so title lookups work
dbg("  Adding synthetic NAICS codes for three-digit rollup industries...")

for (nv in c("v2022", "v2017", "v2012", "v2007")) {
  
  for (rollup in NAICS_AGGREGATION_RULES$three_digit_rollups) {
    
    # Check if this synthetic code already exists
    existing <- naics_hierarchy_xwalk %>%
      dplyr::filter(sheet_name == nv, naics6_code == rollup$target_naics6)
    
    if (nrow(existing) == 0) {
      # Get sector info from an existing code in this subsector
      sample_row <- naics_hierarchy_xwalk %>%
        dplyr::filter(sheet_name == nv, stringr::str_detect(naics6_code, rollup$source_pattern)) %>%
        dplyr::slice(1)
      
      if (nrow(sample_row) > 0) {
        # Create synthetic row
        synthetic_row <- tibble::tibble(
          naics6_code = rollup$target_naics6,
          naics6_title = rollup$target_title,
          naics5_code = rollup$target_naics5,
          naics5_title = rollup$target_title,
          naics4_code = rollup$target_naics4,
          naics4_title = rollup$target_title,
          naics3_code = rollup$target_naics3,
          naics3_title = rollup$target_title,
          sector_code = sample_row$sector_code,
          sector_title = sample_row$sector_title,
          supersector_code = sample_row$supersector_code,
          supersector_title = sample_row$supersector_title,
          domain_code = sample_row$domain_code,
          domain_title = sample_row$domain_title,
          sheet_name = nv
        )
        
        naics_hierarchy_xwalk <- dplyr::bind_rows(naics_hierarchy_xwalk, synthetic_row)
      }
    }
  }
}

dbg("  Synthetic NAICS codes added to hierarchy crosswalk")

# Rebuild the title and hierarchy lookups to include new synthetic codes
for (nv in naics_versions) {
  
  version_data <- naics_hierarchy_xwalk %>%
    dplyr::filter(sheet_name == nv)
  
  if (nrow(version_data) == 0) next
  
  # NAICS6 title lookup
  naics6_title <- version_data %>%
    dplyr::select(naics6_code, naics6_title) %>%
    dplyr::filter(!is.na(naics6_code), !is.na(naics6_title)) %>%
    dplyr::distinct(naics6_code, .keep_all = TRUE)
  
  # NAICS5 title lookup
  naics5_title <- version_data %>%
    dplyr::select(naics5_code, naics5_title) %>%
    dplyr::filter(!is.na(naics5_code), !is.na(naics5_title)) %>%
    dplyr::distinct(naics5_code, .keep_all = TRUE)
  
  # NAICS4 title lookup
  naics4_title <- version_data %>%
    dplyr::select(naics4_code, naics4_title) %>%
    dplyr::filter(!is.na(naics4_code), !is.na(naics4_title)) %>%
    dplyr::distinct(naics4_code, .keep_all = TRUE)
  
  # NAICS3 title lookup
  naics3_title <- version_data %>%
    dplyr::select(naics3_code, naics3_title) %>%
    dplyr::filter(!is.na(naics3_code), !is.na(naics3_title)) %>%
    dplyr::distinct(naics3_code, .keep_all = TRUE)
  
  # NAICS6 to hierarchy mapping
  naics6_hierarchy <- version_data %>%
    dplyr::select(naics6_code, naics5_code, naics4_code, naics3_code) %>%
    dplyr::filter(!is.na(naics6_code)) %>%
    dplyr::distinct(naics6_code, .keep_all = TRUE)
  
  # Update the lookups
  naics_title_lookups[[nv]] <- list(
    naics6 = naics6_title,
    naics5 = naics5_title,
    naics4 = naics4_title,
    naics3 = naics3_title
  )
  
  naics_hierarchy_lookups[[nv]] <- naics6_hierarchy
}

# Update default lookups for backward compatibility
naics6_title_lookup <- naics_title_lookups[["v2022"]]$naics6
naics5_title_lookup <- naics_title_lookups[["v2022"]]$naics5
naics4_title_lookup <- naics_title_lookups[["v2022"]]$naics4
naics3_title_lookup <- naics_title_lookups[["v2022"]]$naics3
naics6_to_hierarchy <- naics_hierarchy_lookups[["v2022"]]

# Also rebuild NAICS6 to sector mappings
for (nv in c("v2022", "v2017", "v2012", "v2007")) {
  mapping <- naics_hierarchy_xwalk %>%
    dplyr::filter(sheet_name == nv) %>%
    dplyr::select(naics6_code, sector_code, sector_title) %>%
    dplyr::filter(!is.na(naics6_code), !is.na(sector_code)) %>%
    dplyr::distinct(naics6_code, .keep_all = TRUE) %>%
    dplyr::mutate(
      sector_code_for_allocation_mapping = dplyr::if_else(naics6_code == "999999", "00", sector_code)
    )
  naics6_to_sector_mappings[[nv]] <- mapping
}
naics6_to_sector_mapping <- naics6_to_sector_mappings[["v2022"]]

dbg("  Title and hierarchy lookups rebuilt with synthetic codes")

cat("  NAICS hierarchy modifications complete.\n")
cat("  NAICS hierarchy loading complete.\n\n")


# ==============================================================================
# SECTION 4: GEOGRAPHIC CROSSWALK (CBSA/CSA/CZ/STATE ASSIGNMENTS)
# [Goal 2] - Integrated county_cbsa_csa_cz_state crosswalk
# ==============================================================================

cat("[SECTION 4] Loading geographic crosswalks and building integrated crosswalk...\n")

# States table
states_data <- tigris::states(year = 2024) %>%
  sf::st_drop_geometry() %>%
  dplyr::filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>%
  dplyr::transmute(state_fips = STATEFP, state_name = NAME)

# County crosswalk with CBSA/CSA
tigris_county_cbsa_csa_state_2024 <- tigris::counties(year = 2024, cb = FALSE) %>%
  dplyr::filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>%
  dplyr::select(STATEFP, COUNTYFP, GEOID, NAMELSAD, CBSAFP, CSAFP, geometry) %>%
  dplyr::rename(
    state_fips = STATEFP, county_fips = COUNTYFP, county_geoid = GEOID,
    county_name = NAMELSAD, cbsa_geoid = CBSAFP, csa_geoid = CSAFP
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
  dplyr::left_join(states_data, by = "state_fips")

dbg("County crosswalk loaded: %d counties", nrow(tigris_county_cbsa_csa_state_2024))

# Create county sf for spatial weights
county_sf <- tigris_county_cbsa_csa_state_2024[, c("county_geoid", "geometry")]

# ------------------------------------------------------------------------------
# COMMUTING ZONE CROSSWALK (2020 Commuting Zones)
# ------------------------------------------------------------------------------

dbg("Loading commuting zone crosswalk...")

# Load county to commuting zone crosswalk from GitHub
county_cz_xwalk_2020 <- readr::read_csv(
  "https://raw.githubusercontent.com/csfowler/CommutingZones2020/refs/heads/main/Output%20Data/county20.csv",
  show_col_types = FALSE
) %>%
  dplyr::rename(
    county_geoid = GEOID,
    commuting_zone_2020 = CZ20
  ) %>%
  dplyr::mutate(county_geoid = as.character(county_geoid))

dbg("  County-CZ crosswalk loaded: %d rows", nrow(county_cz_xwalk_2020))

# Get county population data for creating CZ names
dbg("  Loading county population data for CZ naming...")

county_population_totals_2023 <- tidycensus::get_acs(
  geography = "county",
  variables = "B01003_001",
  year = 2023,
  survey = "acs5",
  geometry = FALSE
) %>%
  dplyr::rename(
    county_geoid = GEOID,
    population_2023 = estimate
  ) %>%
  dplyr::mutate(state_fips = substr(county_geoid, 1, 2)) %>%
  dplyr::filter(!state_fips %in% c("60", "66", "69", "72", "78")) %>%
  dplyr::select(-variable, -moe, -NAME) %>%
  dplyr::left_join(
    tigris::states() %>%
      sf::st_drop_geometry() %>%
      dplyr::select(STATEFP, STUSPS) %>%
      dplyr::rename(state_fips = STATEFP, state_abbreviation = STUSPS),
    by = "state_fips"
  ) %>%
  dplyr::left_join(
    tigris_county_cbsa_csa_state_2024 %>%
      sf::st_drop_geometry() %>%
      dplyr::select(county_geoid, county_name) %>%
      dplyr::mutate(
        county_name_short = stringr::str_remove(county_name, " County$| Parish$| Borough$| Census Area$| Municipality$| City and Borough$| city$")
      ) %>%
      dplyr::select(county_geoid, county_name_short),
    by = "county_geoid"
  )

dbg("  County population data loaded: %d counties", nrow(county_population_totals_2023))

# Join CZ assignment to population data
county_pop_with_cz <- county_population_totals_2023 %>%
  dplyr::left_join(county_cz_xwalk_2020, by = "county_geoid")

# Create commuting zone names based on most populous counties
dbg("  Creating commuting zone names...")

commuting_zone_names <- county_pop_with_cz %>%
  dplyr::filter(!is.na(commuting_zone_2020)) %>%
  dplyr::group_by(commuting_zone_2020) %>%
  dplyr::arrange(dplyr::desc(population_2023), .by_group = TRUE) %>%
  dplyr::summarise(
    commuting_zone_2020_name = paste0(
      paste0(county_name_short[1:min(3, dplyr::n())], collapse = "-"),
      ifelse(dplyr::n() > 1, " ", ""),
      paste0(unique(state_abbreviation[1:min(3, dplyr::n())]), collapse = "-")
    ),
    total_population_2023 = sum(population_2023, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  dplyr::mutate(
    commuting_zone_2020_name = ifelse(
      grepl(" [A-Z]{2}(-[A-Z]{2})*$", commuting_zone_2020_name),
      sub(" ([A-Z]{2}(-[A-Z]{2})*)$", ", \\1", commuting_zone_2020_name),
      commuting_zone_2020_name
    )
  ) %>%
  dplyr::mutate(
    commuting_zone_2020_name = ifelse(
      grepl("([a-zA-Z])([A-Z]{2})$", commuting_zone_2020_name),
      sub("([a-zA-Z])([A-Z]{2})$", "\\1, \\2", commuting_zone_2020_name),
      commuting_zone_2020_name
    )
  )

dbg("  Commuting zone names created: %d CZs", nrow(commuting_zone_names))

# Create final county-CZ crosswalk with names
county_cz_crosswalk <- county_cz_xwalk_2020 %>%
  dplyr::left_join(commuting_zone_names, by = "commuting_zone_2020")

# ------------------------------------------------------------------------------
# [Goal 2] CREATE INTEGRATED CROSSWALK: county_cbsa_csa_cz_state
# ------------------------------------------------------------------------------

dbg("Creating integrated county_cbsa_csa_cz_state crosswalk...")

county_cbsa_csa_cz_state <- tigris_county_cbsa_csa_state_2024 %>%
  dplyr::left_join(county_cz_crosswalk, by = "county_geoid") %>%
  dplyr::select(
    county_geoid, county_name, state_fips, state_name,
    cbsa_geoid, cbsa_name, county_in_cbsa,
    csa_geoid, csa_name, county_in_csa,
    commuting_zone_2020, commuting_zone_2020_name,
    geometry
  )

dbg("county_cbsa_csa_cz_state created: %d rows", nrow(county_cbsa_csa_cz_state))
glimpse(county_cbsa_csa_cz_state)

cat("\n  [SAMPLE CZ NAMES]:\n")
print(head(commuting_zone_names$commuting_zone_2020_name, 20))

cat("  Geographic crosswalks loaded.\n\n")


# ==============================================================================
# SECTION 5: DEFINE COMPLEXITY COMPUTATION FUNCTIONS (ENHANCED DEBUG VERSION)
# [Goal 8] - PA-LQ uses Daboin's proximity matrix (φ) per equation 15
# [Goal 9] - Export co-location industry space (φ) as separate data frames
# ==============================================================================

cat("[SECTION 5] Defining complexity computation functions (ENHANCED DEBUG)...\n")
cat("  (Per Daboin et al. 2019 and Tian et al. 2019 methodologies)\n")

# ------------------------------------------------------------------------------
# VALIDATION THRESHOLDS AND CONSTANTS
# ------------------------------------------------------------------------------
MIN_LOCATIONS_RELIABLE <- 100      # Minimum locations for reliable results
MIN_LOCATIONS_HIGHLY_RELIABLE <- 500  # Minimum for highly reliable results
MIN_ECI_DIVERSITY_COR <- 0.60     # Expected correlation threshold
MIN_ICI_AVGECI_COR <- 0.50        # Expected correlation threshold

# ------------------------------------------------------------------------------
# Function: Create M_ci matrix (binary RCA indicator)
# Per Daboin (2019) equation 5: M_ci = 1[RCA_ci >= 1]
# ------------------------------------------------------------------------------
create_mci_matrix <- function(lq_dt, geo_col = "geoid", ind_col = "naics6_code", lq_col = "industry_location_quotient") {
  dbg_detail("Creating M_ci matrix", sprintf("geo=%s, ind=%s, lq=%s", geo_col, ind_col, lq_col))
  
  lq_dt <- data.table::copy(lq_dt)
  lq_dt <- lq_dt[!is.na(get(geo_col)) & !is.na(get(ind_col))]
  
  if (nrow(lq_dt) == 0) stop("No valid data after filtering NA geoid/naics6_code")
  
  lq_values <- lq_dt[[lq_col]]
  lq_values[is.na(lq_values)] <- 0
  
  lq_dt[, M_ci := as.integer(lq_values >= 1)]
  
  # PATCH (Daboin-consistent): keep only RCA>=1 edges and enforce uniqueness
  lq_dt <- lq_dt[M_ci == 1L, .(M_ci = 1L), by = c(geo_col, ind_col)]
  
  geos <- sort(unique(lq_dt[[geo_col]]))
  inds <- sort(unique(lq_dt[[ind_col]]))
  
  dbg_detail("Unique geographies", length(geos))
  dbg_detail("Unique industries", length(inds))
  
  geo_idx <- setNames(seq_along(geos), geos)
  ind_idx <- setNames(seq_along(inds), inds)
  
  i_idx <- geo_idx[lq_dt[[geo_col]]]
  j_idx <- ind_idx[lq_dt[[ind_col]]]
  
  if (any(is.na(i_idx)) || any(is.na(j_idx))) {
    valid_rows <- !is.na(i_idx) & !is.na(j_idx)
    i_idx <- i_idx[valid_rows]
    j_idx <- j_idx[valid_rows]
    lq_dt <- lq_dt[valid_rows]
  }
  
  M <- Matrix::sparseMatrix(
    i = i_idx, j = j_idx, x = lq_dt$M_ci,
    dims = c(length(geos), length(inds)),
    dimnames = list(geos, inds)
  )
  
  dbg_detail("M_ci matrix dimensions", sprintf("%d x %d", nrow(M), ncol(M)))
  dbg_detail("M_ci sparsity", sprintf("%.2f%% non-zero", 100 * sum(M > 0) / length(M)))
  
  return(M)
}

# ------------------------------------------------------------------------------
# Function: Compute Diversity and Ubiquity
# Per Daboin (2019) equations 6-7
# ------------------------------------------------------------------------------
compute_diversity_ubiquity <- function(M) {
  diversity <- rowSums(M)
  ubiquity <- colSums(M)
  
  dbg_detail("Diversity range", sprintf("%.0f - %.0f", min(diversity), max(diversity)))
  dbg_detail("Ubiquity range", sprintf("%.0f - %.0f", min(ubiquity), max(ubiquity)))
  
  list(diversity = diversity, ubiquity = ubiquity)
}

# ------------------------------------------------------------------------------
# Function: Compute ECI and ICI using eigenvector method (ENHANCED DEBUG)
# Per Daboin (2019) equations 10-14
# ------------------------------------------------------------------------------
compute_eci_ici <- function(M, diversity, ubiquity, n_iter = 20, verbose_debug = TRUE) {
  dbg_detail("Computing ECI/ICI", "Using eigenvector method per Daboin (2019)")
  
  n_geos <- nrow(M)
  n_inds <- ncol(M)
  
  # Sample size warnings
  if (n_geos < MIN_LOCATIONS_RELIABLE) {
    cat("\n")
    cat(strrep("!", 80), "\n")
    cat(sprintf("!!! CRITICAL WARNING: Only %d locations - RESULTS ARE UNRELIABLE !!!\n", n_geos))
    cat(sprintf("!!! Minimum recommended: %d locations (current: %d)\n", MIN_LOCATIONS_RELIABLE, n_geos))
    cat(strrep("!", 80), "\n\n")
  } else if (n_geos < MIN_LOCATIONS_HIGHLY_RELIABLE) {
    cat(sprintf("\n    ⚠ CAUTION: %d locations - results are usable but interpret carefully\n", n_geos))
    cat(sprintf("    For highly reliable results, recommend >= %d locations\n\n", MIN_LOCATIONS_HIGHLY_RELIABLE))
  }
  
  valid_geos <- which(diversity > 0)
  valid_inds <- which(ubiquity > 0)
  
  dbg_detail("Valid geographies (diversity > 0)", length(valid_geos))
  dbg_detail("Valid industries (ubiquity > 0)", length(valid_inds))
  
  if (length(valid_geos) < 2 || length(valid_inds) < 2) {
    warning("Not enough valid locations/industries for ECI/ICI computation")
    return(list(
      ECI = setNames(rep(NA_real_, nrow(M)), rownames(M)),
      ICI = setNames(rep(NA_real_, ncol(M)), colnames(M)),
      diagnostics = list(status = "FAILED", reason = "Insufficient valid locations/industries")
    ))
  }
  
  M_sub <- M[valid_geos, valid_inds, drop = FALSE]
  div_sub <- diversity[valid_geos]
  ubq_sub <- ubiquity[valid_inds]
  
  inv_div <- 1 / div_sub
  inv_ubq <- 1 / ubq_sub
  
  # Build transition matrices
  dbg_detail("Building M̃^C", "D_c^{-1} × M × D_i^{-1} × M^T")
  M_tilde_C <- Matrix::Diagonal(x = inv_div) %*% M_sub %*%
    Matrix::Diagonal(x = inv_ubq) %*% Matrix::t(M_sub)
  
  dbg_detail("Building M̃^I", "D_i^{-1} × M^T × D_c^{-1} × M")
  M_tilde_I <- Matrix::Diagonal(x = inv_ubq) %*% Matrix::t(M_sub) %*%
    Matrix::Diagonal(x = inv_div) %*% M_sub
  
  # Verify row-stochasticity
  if (verbose_debug) {
    M_tilde_C_dense <- as.matrix(M_tilde_C)
    row_sums_C <- rowSums(M_tilde_C_dense)
    
    if (all(abs(row_sums_C - 1) < 1e-10)) {
      dbg_detail("M̃^C row-stochasticity", "✓ VERIFIED (all rows sum to 1)")
    } else {
      cat(sprintf("    [WARNING] M̃^C row sums: min=%.10f, max=%.10f (expected 1)\n",
                  min(row_sums_C), max(row_sums_C)))
    }
    
    M_tilde_I_dense <- as.matrix(M_tilde_I)
    row_sums_I <- rowSums(M_tilde_I_dense)
    
    if (all(abs(row_sums_I - 1) < 1e-10)) {
      dbg_detail("M̃^I row-stochasticity", "✓ VERIFIED (all rows sum to 1)")
    } else {
      cat(sprintf("    [WARNING] M̃^I row sums: min=%.10f, max=%.10f (expected 1)\n",
                  min(row_sums_I), max(row_sums_I)))
    }
    
    rm(M_tilde_C_dense, M_tilde_I_dense)
  }
  
  # Initialize result vectors
  ECI_full <- setNames(rep(NA_real_, nrow(M)), rownames(M))
  ICI_full <- setNames(rep(NA_real_, ncol(M)), colnames(M))
  
  # Eigenvector extraction function
  pick_second_eigenvector <- function(A, label = "matrix", k = 10, tol_1 = 1e-8) {
    n <- nrow(A)
    
    if (n <= 5) {
      eg <- eigen(as.matrix(A), symmetric = FALSE)
      vals <- Re(eg$values)
      vecs <- Re(eg$vectors)
    } else {
      kk <- min(k, n - 1)
      eg <- tryCatch({
        RSpectra::eigs(A, k = kk, which = "LM", opts = list(maxitr = 5000, tol = 1e-12))
      }, error = function(e) {
        eigen(as.matrix(A), symmetric = FALSE)
      })
      vals <- Re(eg$values)
      vecs <- Re(eg$vectors)
    }
    
    ord <- order(vals, decreasing = TRUE)
    vals <- vals[ord]
    vecs <- vecs[, ord, drop = FALSE]
    
    if (verbose_debug) {
      n_show <- min(8, length(vals))
      cat(sprintf("    [EIGENVALUES] %s: %s\n", label,
                  paste(sprintf("%.6f", vals[1:n_show]), collapse = ", ")))
      
      n_near_one <- sum(abs(vals - 1) < 0.001)
      if (n_near_one > 1) {
        cat(sprintf("    [WARNING] %d eigenvalues near 1 - possible disconnected components\n", n_near_one))
      }
    }
    
    idx <- which(vals < (1 - tol_1))
    
    if (length(idx) == 0) {
      idx <- which(vals < 0.9999)
      if (length(idx) == 0) {
        stop(sprintf("No eigenvalue found below 1 for %s; matrix may be degenerate", label))
      }
    }
    
    if (verbose_debug) {
      dbg_detail(sprintf("%s selected eigenvalue", label), sprintf("%.8f (index %d)", vals[idx[1]], idx[1]))
    }
    
    v <- vecs[, idx[1]]
    
    if (sd(v) > 1e-10) {
      v <- (v - mean(v)) / sd(v)
    } else {
      warning(sprintf("Eigenvector for %s has near-zero variance", label))
      v <- rep(0, length(v))
    }
    
    return(list(vector = v, eigenvalue = vals[idx[1]], all_eigenvalues = vals))
  }
  
  # Compute ECI
  dbg_detail("Computing ECI eigenvector", "Finding 2nd eigenvector of M̃^C")
  
  eci_result <- tryCatch({
    pick_second_eigenvector(M_tilde_C, label = "M̃^C (ECI)")
  }, error = function(e) {
    warning(paste("ECI computation failed:", e$message))
    list(vector = rep(NA_real_, length(valid_geos)), eigenvalue = NA_real_, all_eigenvalues = NA)
  })
  
  ECI_sub <- eci_result$vector
  
  # Compute ICI
  dbg_detail("Computing ICI eigenvector", "Finding 2nd eigenvector of M̃^I")
  
  ici_result <- tryCatch({
    pick_second_eigenvector(M_tilde_I, label = "M̃^I (ICI)")
  }, error = function(e) {
    warning(paste("ICI computation failed:", e$message))
    list(vector = rep(NA_real_, length(valid_inds)), eigenvalue = NA_real_, all_eigenvalues = NA)
  })
  
  ICI_sub <- ici_result$vector
  
  # Sign correction for ECI
  eci_div_cor_before <- NA_real_
  eci_div_cor_after <- NA_real_
  eci_sign_flipped <- FALSE
  
  if (!any(is.na(ECI_sub)) && length(ECI_sub) > 2) {
    eci_div_cor_before <- cor(ECI_sub, div_sub)
    dbg_detail("ECI-Diversity correlation (before correction)", sprintf("%.4f", eci_div_cor_before))
    
    if (eci_div_cor_before < 0) {
      ECI_sub <- -ECI_sub
      eci_sign_flipped <- TRUE
      dbg_detail("ECI sign correction", "✓ Applied (was negative)")
    } else {
      dbg_detail("ECI sign correction", "Not needed (already positive)")
    }
    
    eci_div_cor_after <- cor(ECI_sub, div_sub)
  }
  
  # Sign correction for ICI
  ici_avgeci_cor_before <- NA_real_
  ici_avgeci_cor_after <- NA_real_
  ici_ubq_cor <- NA_real_
  ici_sign_flipped <- FALSE
  
  if (!any(is.na(ICI_sub)) && length(ICI_sub) > 2 && !any(is.na(ECI_sub))) {
    M_sub_dense <- as.matrix(M_sub)
    avg_eci_per_industry <- numeric(ncol(M_sub_dense))
    
    for (j in seq_len(ncol(M_sub_dense))) {
      host_cities <- which(M_sub_dense[, j] == 1)
      if (length(host_cities) > 0) {
        avg_eci_per_industry[j] <- mean(ECI_sub[host_cities])
      } else {
        avg_eci_per_industry[j] <- NA_real_
      }
    }
    
    valid_for_cor <- !is.na(ICI_sub) & !is.na(avg_eci_per_industry)
    
    if (sum(valid_for_cor) > 2) {
      ici_avgeci_cor_before <- cor(ICI_sub[valid_for_cor], avg_eci_per_industry[valid_for_cor])
      dbg_detail("ICI-AvgHostECI correlation (before correction)", sprintf("%.4f", ici_avgeci_cor_before))
      
      if (ici_avgeci_cor_before < 0) {
        ICI_sub <- -ICI_sub
        ici_sign_flipped <- TRUE
        dbg_detail("ICI sign correction", "✓ Applied (was negative)")
      } else {
        dbg_detail("ICI sign correction", "Not needed (already positive)")
      }
      
      ici_avgeci_cor_after <- cor(ICI_sub[valid_for_cor], avg_eci_per_industry[valid_for_cor])
    }
    
    ici_ubq_cor <- cor(ICI_sub, ubq_sub)
    dbg_detail("ICI-Ubiquity correlation (diagnostic)", sprintf("%.4f", ici_ubq_cor))
    
    rm(M_sub_dense)
  }
  
  # Validation checks
  validation_passed <- TRUE
  validation_warnings <- character()
  
  if (!is.na(eci_div_cor_after) && abs(eci_div_cor_after) < MIN_ECI_DIVERSITY_COR) {
    validation_warnings <- c(validation_warnings,
                             sprintf("ECI-Diversity correlation weak (%.3f < %.3f)", 
                                     abs(eci_div_cor_after), MIN_ECI_DIVERSITY_COR))
    validation_passed <- FALSE
  }
  
  if (!is.na(ici_avgeci_cor_after) && abs(ici_avgeci_cor_after) < MIN_ICI_AVGECI_COR) {
    validation_warnings <- c(validation_warnings,
                             sprintf("ICI-AvgHostECI correlation weak (%.3f < %.3f)", 
                                     abs(ici_avgeci_cor_after), MIN_ICI_AVGECI_COR))
    validation_passed <- FALSE
  }
  
  if (length(validation_warnings) > 0 && verbose_debug) {
    cat("\n    [VALIDATION WARNINGS]\n")
    for (w in validation_warnings) {
      cat(sprintf("    ⚠ %s\n", w))
    }
    cat("\n")
  }
  
  # Assign to full vectors
  ECI_full[valid_geos] <- ECI_sub
  ICI_full[valid_inds] <- ICI_sub
  
  dbg_detail("ECI range", sprintf("%.4f to %.4f", min(ECI_sub, na.rm = TRUE), max(ECI_sub, na.rm = TRUE)))
  dbg_detail("ICI range", sprintf("%.4f to %.4f", min(ICI_sub, na.rm = TRUE), max(ICI_sub, na.rm = TRUE)))
  
  return(list(
    ECI = ECI_full,
    ICI = ICI_full,
    diagnostics = list(
      n_locations = n_geos,
      n_industries = n_inds,
      eci_eigenvalue = eci_result$eigenvalue,
      ici_eigenvalue = ici_result$eigenvalue,
      eci_diversity_correlation = eci_div_cor_after,
      ici_avgeci_correlation = ici_avgeci_cor_after,
      ici_ubiquity_correlation = ici_ubq_cor,
      eci_sign_flipped = eci_sign_flipped,
      ici_sign_flipped = ici_sign_flipped,
      validation_passed = validation_passed,
      validation_warnings = validation_warnings
    )
  ))
}

# ------------------------------------------------------------------------------
# Function: Compute Proximity Matrix (phi) - Daboin equation 15-16
# [Goal 8] - This is the core industry relatedness matrix
# ------------------------------------------------------------------------------
compute_proximity_matrix <- function(M) {
  dbg_detail("Computing proximity matrix", "φ = U / max(diag) per Daboin eq. 15-16")
  n_ind <- ncol(M)
  U <- Matrix::crossprod(M)  # U = M^T × M, co-occurrence matrix (Daboin eq. 15)
  U_dense <- as.matrix(U)
  diag_U <- base::diag(U_dense)
  max_mat <- outer(diag_U, diag_U, pmax)
  max_mat[max_mat == 0] <- 1
  phi <- U_dense / max_mat  # Daboin eq. 16
  base::diag(phi) <- 0  # Zero diagonal per convention
  
  dbg_detail("Proximity matrix dimensions", sprintf("%d x %d", nrow(phi), ncol(phi)))
  dbg_detail("Mean proximity (excl. diagonal)", sprintf("%.4f", mean(phi[phi > 0])))
  
  rm(U, U_dense, max_mat)
  return(phi)
}

# ------------------------------------------------------------------------------
# [Goal 9] Function: Create Co-Location Industry Space Data Frame
# Exports the proximity matrix φ as a data frame with industry names
# ------------------------------------------------------------------------------
create_industry_space_dataframe <- function(phi, title_lookup, year, geo_level, naics_level = "naics6") {
  dbg_detail("Creating industry space data frame", sprintf("Year=%d, Geo=%s", year, geo_level))
  
  industry_names <- rownames(phi)
  n_ind <- length(industry_names)
  
  # Convert matrix to long format
  phi_long <- data.table::as.data.table(phi, keep.rownames = "industry_code_i")
  phi_long <- data.table::melt(phi_long, id.vars = "industry_code_i", 
                               variable.name = "industry_code_j", 
                               value.name = "proximity_phi")
  phi_long[, industry_code_j := as.character(industry_code_j)]
  
  # Remove self-connections (diagonal)
  phi_long <- phi_long[industry_code_i != industry_code_j]
  
  # Remove zero proximity connections for sparsity
  phi_long <- phi_long[proximity_phi > 0]
  
  # Add year, geo_level, and naics_level
  phi_long[, year := year]
  phi_long[, geo_level := geo_level]
  phi_long[, naics_level := naics_level]
  
  # Determine column names based on NAICS level
  code_col <- paste0(naics_level, "_code")
  title_col <- paste0(naics_level, "_title")
  
  # Add industry names
  title_dt <- data.table::as.data.table(title_lookup)
  
  # Merge for industry i
  if (code_col %in% names(title_dt) && title_col %in% names(title_dt)) {
    phi_long <- merge(phi_long, title_dt[, c(code_col, title_col), with = FALSE], 
                      by.x = "industry_code_i", by.y = code_col, all.x = TRUE)
    setnames(phi_long, title_col, "industry_title_i")
    
    # Merge for industry j
    phi_long <- merge(phi_long, title_dt[, c(code_col, title_col), with = FALSE], 
                      by.x = "industry_code_j", by.y = code_col, all.x = TRUE)
    setnames(phi_long, title_col, "industry_title_j")
  } else {
    phi_long[, industry_title_i := NA_character_]
    phi_long[, industry_title_j := NA_character_]
  }
  
  # Reorder columns
  setcolorder(phi_long, c("year", "geo_level", "naics_level", 
                          "industry_code_i", "industry_title_i", 
                          "industry_code_j", "industry_title_j", "proximity_phi"))
  
  dbg_detail("Industry space edges", nrow(phi_long))
  
  return(phi_long)
}

# ------------------------------------------------------------------------------
# Function: Compute Feasibility / Colocation Density - Daboin eq. 19-20
# density^implicit_{c,i'} = d_{c,i'} = (Σ_i M_{c,i} × φ_{i,i'}) / (Σ_i φ_{i,i'})
#
# For each city c and industry i':
#   - Numerator: Sum of proximity from all present industries to industry i'
#   - Denominator: Total proximity potential of industry i'
#   → Measures how well-positioned city c is to develop industry i'
#     based on its existing industrial capabilities
# ------------------------------------------------------------------------------
compute_feasibility <- function(M, phi) {
  dbg_detail("Computing feasibility/density", "density = M × φ / colSums(φ)")
  
  # phi_col_sums[i'] = Σ_i φ_{i,i'} (total proximity to each industry)
  phi_col_sums <- colSums(phi)
  phi_col_sums[phi_col_sums == 0] <- 1
  
  # (M %*% phi)[c,i'] = Σ_i M_{c,i} × φ_{i,i'}
  density <- as.matrix(M %*% phi)
  
  # Normalize by total proximity to get density[c,i']
  density <- sweep(density, 2, phi_col_sums, "/")
  
  dbg_detail("Feasibility range", sprintf("%.4f - %.4f", min(density), max(density)))
  
  return(density)
}

# ------------------------------------------------------------------------------
# Function: Compute Strategic Index - Daboin eq. 21
# SI_c = Σ_i d_{c,i} × (1 - M_{c,i}) × ICI_i
#
# For each city c, sums over all industries i:
#   - Density: d_{c,i} (how well-positioned is c for industry i?)
#   - Absence: (1 - M_{c,i}) (is industry i NOT present in city c?)
#   - Industry complexity: ICI_i (how complex is industry i?)
#
# Interpretation: Total potential complexity gain from feasible but absent industries
# High SI = city has high potential to add complex industries it can realistically develop
# ------------------------------------------------------------------------------
compute_strategic_index <- function(M, density, ICI) {
  dbg_detail("Computing Strategic Index", "SI = sum(density × absent × ICI)")
  ICI_vec <- ICI[colnames(M)]
  ICI_vec[is.na(ICI_vec)] <- 0
  absent <- 1 - as.matrix(M)
  
  # SI[c] = Σ_i density[c,i] × absent[c,i] × ICI[i]
  SI <- rowSums(density * absent * matrix(ICI_vec, nrow = nrow(M), ncol = ncol(M), byrow = TRUE))
  
  dbg_detail("Strategic Index range", sprintf("%.4f - %.4f", min(SI), max(SI)))
  
  return(SI)
}

# ------------------------------------------------------------------------------
# Function: Compute Strategic Gain - Daboin eq. 22
# SG_{c,i} = [Σ_{i'} (φ_{i,i'} / Σ_{i''} φ_{i'',i'}) × (1 - M_{c,i'}) × ICI_{i'}] - d_{c,i} × ICI_i
#
# Term 1: Sum over all industries i' of:
#   - Normalized proximity: φ_{i,i'} / Σ_{i''} φ_{i'',i'} (how related is i to i')
#   - Absence indicator: (1 - M_{c,i'}) (is i' NOT present in city c?)
#   - Industry complexity: ICI_{i'} (how complex is industry i')
#   → Captures potential complexity gain from related absent industries
#
# Term 2: d_{c,i} × ICI_i
#   - Current density times current industry complexity
#   → Captures current complexity contribution
#
# Interpretation: How much would adding industry i improve city c's position
# in the industry space, beyond its current contribution?
# ------------------------------------------------------------------------------
compute_strategic_gain <- function(M, phi, density, ICI) {
  dbg_detail("Computing Strategic Gain", "SG per industry per location")
  ICI_vec <- ICI[colnames(M)]
  ICI_vec[is.na(ICI_vec)] <- 0
  
  # phi_norm[i,i'] = φ_{i,i'} / Σ_{i''} φ_{i'',i'} (column-normalized proximity)
  phi_col_sums <- colSums(phi)
  phi_col_sums[phi_col_sums == 0] <- 1
  phi_norm <- sweep(phi, 2, phi_col_sums, "/")
  
  # ICI_weighted_absent[c,i'] = (1 - M_{c,i'}) × ICI_{i'}
  absent <- 1 - as.matrix(M)
  ICI_weighted_absent <- sweep(absent, 2, ICI_vec, "*")
  
  # term1[c,i] = Σ_{i'} ICI_weighted_absent[c,i'] × phi_norm[i,i']
  # The transpose gives t(phi_norm)[i',i] = phi_norm[i,i']
  term1 <- ICI_weighted_absent %*% t(phi_norm)
  
  # term2[c,i] = d_{c,i} × ICI_i
  term2 <- sweep(density, 2, ICI_vec, "*")
  
  SG <- term1 - term2
  
  return(SG)
}

# ------------------------------------------------------------------------------
# Function: Compute all complexity indicators (WRAPPER)
# Returns proximity matrix for industry space export
# ------------------------------------------------------------------------------
compute_all_complexity_indicators <- function(lq_dt, lq_col = "industry_location_quotient", 
                                              geo_col = "geoid", ind_col = "industry_code", verbose = TRUE) {
  if (verbose) dbg("    Creating M_ci matrix...")
  M <- create_mci_matrix(lq_dt, geo_col = geo_col, ind_col = ind_col, lq_col = lq_col)
  
  if (verbose) dbg("    Computing diversity and ubiquity...")
  div_ubq <- compute_diversity_ubiquity(M)
  
  if (verbose) dbg("    Computing ECI and ICI (eigenvector method with enhanced diagnostics)...")
  eci_ici <- compute_eci_ici(M, div_ubq$diversity, div_ubq$ubiquity, verbose_debug = verbose)
  
  if (verbose) dbg("    Computing proximity matrix (Daboin φ)...")
  phi <- compute_proximity_matrix(M)
  
  if (verbose) dbg("    Computing feasibility/density...")
  density <- compute_feasibility(M, phi)
  
  if (verbose) dbg("    Computing strategic index...")
  SI <- compute_strategic_index(M, density, eci_ici$ICI)
  
  if (verbose) dbg("    Computing strategic gain...")
  SG <- compute_strategic_gain(M, phi, density, eci_ici$ICI)
  
  return(list(
    M = M, diversity = div_ubq$diversity, ubiquity = div_ubq$ubiquity,
    ECI = eci_ici$ECI, ICI = eci_ici$ICI, proximity = phi,
    feasibility = density, strategic_index = SI, strategic_gain = SG,
    diagnostics = eci_ici$diagnostics
  ))
}

# ------------------------------------------------------------------------------
# Function: Create spatial weight matrix for counties
# Per Tian et al. (2019): Queen-type contiguity matrix, row-standardized
# ------------------------------------------------------------------------------
create_county_spatial_weights <- function(county_sf) {
  dbg_detail("Creating spatial weights", "Queen contiguity, row-standardized")
  county_sf <- sf::st_make_valid(county_sf)
  nb <- spdep::poly2nb(county_sf, queen = TRUE)
  W <- spdep::nb2mat(nb, style = "W", zero.policy = TRUE)
  rownames(W) <- county_sf$county_geoid
  colnames(W) <- county_sf$county_geoid
  
  dbg_detail("Spatial weights dimensions", sprintf("%d x %d", nrow(W), ncol(W)))
  dbg_detail("Mean neighbors per county", sprintf("%.2f", mean(rowSums(W > 0))))
  
  return(W)
}

# ------------------------------------------------------------------------------
# Helper function: Row-standardize a matrix
# ------------------------------------------------------------------------------
row_standardize_matrix <- function(mat) {
  row_sums <- rowSums(mat)
  row_sums[row_sums == 0] <- 1
  return(mat / row_sums)
}

# ------------------------------------------------------------------------------
# [Goal 8] Function: Compute PA-LQ using Daboin's Proximity Matrix
# Per Tian et al. (2019) / StatsAmerica:
#   PA-LQ = [LQ × (ALQ + ε) × (WLQ + ε) × (WALQ + ε)]^(1/4)
# Using Daboin's proximity matrix (φ) for ALQ component
# ------------------------------------------------------------------------------
compute_pa_lq <- function(lq_dt, W, phi, ind_col = "industry_code", epsilon = 0.001) {
  dbg_detail("Computing PA-LQ", "Using Daboin proximity matrix (φ) per Tian/StatsAmerica formula")
  
  industries <- sort(unique(lq_dt[[ind_col]]))
  counties <- sort(unique(lq_dt$geoid))
  
  W_counties <- intersect(counties, rownames(W))
  phi_industries <- intersect(industries, rownames(phi))
  
  dbg_detail("Counties in W", length(W_counties))
  dbg_detail("Industries in φ", length(phi_industries))
  
  lq_dt <- lq_dt[get(ind_col) %in% phi_industries & geoid %in% W_counties]
  
  if (nrow(lq_dt) == 0) {
    warning("No matching counties/industries for PA-LQ computation")
    return(NULL)
  }
  
  W_sub <- W[W_counties, W_counties]
  phi_sub <- phi[phi_industries, phi_industries]
  
  # Row-standardize proximity matrix for ALQ computation
  phi_standardized <- row_standardize_matrix(phi_sub)
  
  # Create LQ matrix (counties × industries)
  lq_wide <- data.table::dcast(lq_dt, as.formula(paste("geoid ~", ind_col)), 
                               value.var = "industry_location_quotient", fill = 0)
  lq_mat <- as.matrix(lq_wide[, -1, with = FALSE])
  rownames(lq_mat) <- lq_wide$geoid
  
  # Align dimensions
  lq_mat <- lq_mat[, phi_industries, drop = FALSE]
  lq_mat <- lq_mat[W_counties, , drop = FALSE]
  
  dbg_detail("LQ matrix dimensions", sprintf("%d counties × %d industries", nrow(lq_mat), ncol(lq_mat)))
  
  # ===========================================================================
  # PA-LQ COMPONENTS (per StatsAmerica / Tian et al. 2019):
  # 1. LQ: Standard location quotient
  # 2. ALQ: φ-weighted average LQ of related industries in county g
  # 3. WLQ: Spatial lag of LQ (concentration in neighboring counties)
  # 4. WALQ: Spatial lag of ALQ (related industries in neighboring counties)
  # ===========================================================================
  
  # ALQ: Related industries concentration using Daboin's φ
  dbg_detail("Computing ALQ", "φ-weighted LQ average of related industries")
  # ALQ_{ig} = Σ_j (φ_{ij} / Σ_k φ_{ik}) × LQ_{jg}
  # In matrix form: ALQ = LQ × φ_standardized^T (each county gets weighted avg across related industries)
  ALQ_mat <- lq_mat %*% t(phi_standardized)
  
  # WLQ: Spatial concentration of same industry in neighboring counties
  dbg_detail("Computing WLQ", "Spatial lag of LQ (W × LQ)")
  WLQ_mat <- W_sub %*% lq_mat
  
  # WALQ: Spatial lag of ALQ (related industries in neighboring counties)
  dbg_detail("Computing WALQ", "Spatial lag of ALQ (W × ALQ)")
  WALQ_mat <- W_sub %*% ALQ_mat
  
  # PA-LQ: Geometric mean of all four components
  dbg_detail("Computing PA-LQ", "Geometric mean: [LQ × (ALQ+ε) × (WLQ+ε) × (WALQ+ε)]^(1/4)")
  PA_LQ_mat <- (lq_mat * (ALQ_mat + epsilon) * (WLQ_mat + epsilon) * (WALQ_mat + epsilon))^0.25
  
  # Set PA-LQ to 0 where LQ is 0 (industry not present)
  PA_LQ_mat[lq_mat == 0] <- 0
  
  dbg_detail("PA-LQ range", sprintf("%.4f - %.4f (excl. zeros)", 
                                    min(PA_LQ_mat[PA_LQ_mat > 0], na.rm = TRUE), 
                                    max(PA_LQ_mat, na.rm = TRUE)))
  
  # Convert to long format
  PA_LQ_dt <- data.table::as.data.table(PA_LQ_mat, keep.rownames = "geoid")
  PA_LQ_long <- data.table::melt(PA_LQ_dt, id.vars = "geoid", variable.name = "industry_code",
                                 value.name = "proximity_adjusted_location_quotient")
  PA_LQ_long[, industry_code := as.character(industry_code)]
  
  # Also return component matrices for debugging/analysis
  ALQ_long <- data.table::melt(data.table::as.data.table(ALQ_mat, keep.rownames = "geoid"),
                               id.vars = "geoid", variable.name = "industry_code", value.name = "ALQ")
  ALQ_long[, industry_code := as.character(industry_code)]
  
  WLQ_long <- data.table::melt(data.table::as.data.table(WLQ_mat, keep.rownames = "geoid"),
                               id.vars = "geoid", variable.name = "industry_code", value.name = "WLQ")
  WLQ_long[, industry_code := as.character(industry_code)]
  
  WALQ_long <- data.table::melt(data.table::as.data.table(WALQ_mat, keep.rownames = "geoid"),
                                id.vars = "geoid", variable.name = "industry_code", value.name = "WALQ")
  WALQ_long[, industry_code := as.character(industry_code)]
  
  # Merge all components
  result <- PA_LQ_long
  result <- merge(result, ALQ_long, by = c("geoid", "industry_code"), all.x = TRUE)
  result <- merge(result, WLQ_long, by = c("geoid", "industry_code"), all.x = TRUE)
  result <- merge(result, WALQ_long, by = c("geoid", "industry_code"), all.x = TRUE)
  
  return(result)
}

cat("  Complexity computation functions defined (ENHANCED DEBUG VERSION).\n")
cat("  - ECI/ICI: Eigenvector method with comprehensive diagnostics\n")
cat("  - PA-LQ: Uses Daboin proximity matrix (φ) per Tian/StatsAmerica formula\n")
cat("  - Industry Space: Exportable φ matrix as data frame with names\n\n")


# ==============================================================================
# SECTION 6: CONFIGURE PATHS AND PROCESSING PARAMETERS
# [Goal 6] - Process years in reverse order (2024 → 2010)
# ==============================================================================

cat("[SECTION 6] Configuring paths and processing parameters...\n")

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

tapestry_base_path <- file.path(cred_raw_data_folder, "Tapestry_Employment", "contains_naics_999999_county_XX999", "NAICS_6D")

# Export directory
export_directory <- getwd()

dbg("Tapestry path: %s [exists: %s]", tapestry_base_path, dir.exists(tapestry_base_path))
dbg("Export directory: %s", export_directory)

if (!dir.exists(tapestry_base_path)) stop("ERROR: Tapestry data path not found")

# [Goal 6] Years to process - REVERSE ORDER (2024 down to 2010)
TAPESTRY_YEARS <- rev(2010:2024)
dbg("Processing years in REVERSE order: %s", paste(head(TAPESTRY_YEARS, 5), collapse = ", "))

# [Goal 5 revised] Process ONE YEAR AT A TIME for aggressive memory management
YEARS_PER_BATCH <- 1
year_batches <- split(TAPESTRY_YEARS, ceiling(seq_along(TAPESTRY_YEARS) / YEARS_PER_BATCH))
dbg("Processing one year at a time: %d total years", length(year_batches))

# Geographic conversion constants
CT_OLD_COUNTIES <- c("09001", "09003", "09005", "09007", "09009", "09011", "09013", "09015")
CT_NEW_PLANNING_REGIONS <- c("09110", "09120", "09130", "09140", "09150", "09160", "09170", "09180", "09190")
AK_VALDEZ_CORDOVA <- "02261"
AK_CHUGACH <- "02063"
AK_COPPER_RIVER <- "02066"

# [Goal 1] Geographic levels for complexity computation
# State = employment + LQ only; County/CBSA/CSA/CZ = full complexity
COMPLEXITY_GEO_LEVELS <- c("county", "cbsa", "csa", "cz")
LQ_ONLY_GEO_LEVELS <- c("state")

cat("  Configuration complete.\n\n")

# ==============================================================================
# SECTION 6b: SELECT NAICS AGGREGATION LEVELS TO PROCESS
# ==============================================================================

cat("[SECTION 6b] Selecting NAICS aggregation levels to process...\n\n")

# Define all available NAICS levels
ALL_NAICS_LEVELS <- c("naics6", "naics5", "naics4", "naics3")

# Prompt for each level
selected_naics_levels <- character(0)

for (lvl in ALL_NAICS_LEVELS) {
  digits <- substr(lvl, 6, 6)
  answer <- readline(prompt = sprintf("Include %s (%s-digit) aggregation? [y/n]: ", toupper(lvl), digits))
  if (tolower(answer) %in% c("y", "yes", "")) {
    selected_naics_levels <- c(selected_naics_levels, lvl)
    cat(sprintf("  ✓ %s selected\n", toupper(lvl)))
  } else {
    cat(sprintf("  ✗ %s skipped\n", toupper(lvl)))
  }
}

# Validate at least one level is selected
if (length(selected_naics_levels) == 0) {
  cat("\n  ⚠ No NAICS levels selected. Defaulting to all levels.\n")
  selected_naics_levels <- ALL_NAICS_LEVELS
}

# Override the NAICS_LEVELS variable used throughout the script
NAICS_LEVELS <- selected_naics_levels

cat("\n")
cat(sprintf("  Selected NAICS levels: %s\n", paste(toupper(NAICS_LEVELS), collapse = ", ")))
cat("  Configuration complete.\n\n")

# ==============================================================================
# SECTION 7: CREATE SPATIAL WEIGHTS AND PREPARE FOR PROCESSING
# ==============================================================================

cat("[SECTION 7] Creating spatial weights and preparing for processing...\n")

dbg("Creating county spatial weights matrix (queen contiguity)...")
W_county <- create_county_spatial_weights(county_sf)
dbg("  Spatial weights matrix: %d x %d", nrow(W_county), ncol(W_county))

# Prepare tigris join data
tigris_join_cols <- if (inherits(tigris_county_cbsa_csa_state_2024, "sf")) {
  data.table::as.data.table(sf::st_drop_geometry(tigris_county_cbsa_csa_state_2024))[
    , .(county_geoid, county_in_cbsa, cbsa_geoid, county_in_csa, csa_geoid)
  ]
} else {
  data.table::as.data.table(tigris_county_cbsa_csa_state_2024)[
    , .(county_geoid, county_in_cbsa, cbsa_geoid, county_in_csa, csa_geoid)
  ]
}

# Output folder was already created at script start (Section 0)
dbg("Using output folder: %s", output_folder_path)

# Export W_county immediately since it's ready
dbg("Exporting county spatial weights matrix immediately...")
saveRDS(W_county, file.path(output_folder_path, "county_spatial_weights_W.rds"))
W_county_df <- as.data.frame(W_county)
W_county_df$county_geoid <- rownames(W_county)
W_county_df <- W_county_df[, c("county_geoid", setdiff(names(W_county_df), "county_geoid"))]
readr::write_csv(W_county_df, file.path(output_folder_path, "county_spatial_weights_W.csv"))
export_to_parquet(W_county_df, file.path(output_folder_path, "county_spatial_weights_W.parquet"))
dbg("  ✓ County spatial weights matrix exported (RDS + CSV + Parquet)")
rm(W_county_df)

cat("  Pre-processing complete.\n\n")


# ==============================================================================
# SECTION 8: MAIN PROCESSING LOOP - PROCESS 2024 FIRST, THEN OPTIONALLY ALL OTHERS
# [Goal 5 revised] - Process one year at a time for memory management
# [Goal 6] - Reverse chronological order (2024 first)
# [Goal 11] - Export files immediately as created
# ==============================================================================

cat(strrep("=", 80), "\n")
cat("SECTION 8: PROCESSING ALL YEARS (2024 FIRST, THEN OTHERS)\n")
cat(strrep("=", 80), "\n\n")

# Store diagnostics across years for summary
all_diagnostics <- list()

# Geography name lookups
county_names_lookup <- tigris_county_cbsa_csa_state_2024 %>%
  sf::st_drop_geometry() %>%
  dplyr::select(county_geoid, county_name, state_fips, state_name) %>%
  dplyr::distinct()

cbsa_names_lookup <- tigris_county_cbsa_csa_state_2024 %>%
  sf::st_drop_geometry() %>%
  dplyr::select(cbsa_geoid, cbsa_name) %>%
  dplyr::filter(!is.na(cbsa_geoid) & cbsa_geoid != "") %>%
  dplyr::distinct()

csa_names_lookup <- tigris_county_cbsa_csa_state_2024 %>%
  sf::st_drop_geometry() %>%
  dplyr::select(csa_geoid, csa_name) %>%
  dplyr::filter(!is.na(csa_geoid) & csa_geoid != "") %>%
  dplyr::distinct()

cz_names_lookup <- commuting_zone_names %>%
  dplyr::mutate(cz_geoid = as.character(commuting_zone_2020)) %>%
  dplyr::select(cz_geoid, commuting_zone_2020_name, total_population_2023)

state_names_lookup <- states_data

# ==============================================================================
# HELPER FUNCTION: Process a single year
# ==============================================================================

process_single_year <- function(yr, delete_intermediate = TRUE, show_top15 = TRUE) {
  
  cat("\n")
  cat(strrep("#", 80), "\n")
  cat(sprintf("### Processing Year %d\n", yr))
  cat(strrep("#", 80), "\n\n")
  
  year_start_time <- Sys.time()
  
  # Track intermediate files for potential cleanup
  year_intermediate_files <- c()
  year_final_files <- c()
  
  # ========================================================================
  # STEP 8.1: LOAD TAPESTRY DATA FOR THIS YEAR
  # ========================================================================
  
  dbg("[8.1] Loading Tapestry data for year %d...", yr)
  
  tapestry_file <- file.path(tapestry_base_path, sprintf("%d.csv", yr))
  
  if (!file.exists(tapestry_file)) {
    dbg("  WARNING: File not found: %s", tapestry_file)
    dbg("  Skipping year %d", yr)
    return(NULL)
  }
  
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
  
  dbg("  Raw data: %d rows", nrow(dt_raw))
  dbg_detail("Employment sum", sum(dt_raw$tap_emplvl_est_3, na.rm = TRUE))
  
  if (yr == 2024) {
    cat("\n  [GLIMPSE] Raw Tapestry data (before any modifications):\n")
    print(glimpse(dt_raw))
  }
  
  # --- INTERMEDIATE EXPORT 1: Raw unmodified Tapestry data ---
  intermediate_01_file <- file.path(output_folder_path, sprintf("%d_intermediate_01_raw_tapestry.parquet", yr))
  export_to_parquet(as.data.frame(dt_raw), intermediate_01_file)
  year_intermediate_files <- c(year_intermediate_files, intermediate_01_file)
  dbg("  ✓ Intermediate 1 exported: Raw Tapestry data (Parquet)")
  
  # ========================================================================
  # STEP 8.2: BASIC CLEANUP AND GEOID RECODING
  # ========================================================================
  
  dbg("[8.2] Basic cleanup and GEOID recoding...")
  
  dt <- dt_raw %>%
    dplyr::rename(county_geoid = area_fips) %>%
    dplyr::mutate(county_geoid = stringr::str_pad(county_geoid, width = 5, side = "left", pad = "0"))
  
  # Wade Hampton -> Kusilvak
  has_wade_hampton <- any(dt$county_geoid == "02270", na.rm = TRUE)
  has_kusilvak <- any(dt$county_geoid == "02158", na.rm = TRUE)
  
  if (yr == 2015) {
    if (has_wade_hampton && has_kusilvak) {
      dt <- dt %>% dplyr::filter(county_geoid != "02270")
      dbg_detail("Wade Hampton handling (2015)", "Removed duplicate, kept Kusilvak")
    }
  } else {
    if (has_wade_hampton && !has_kusilvak) {
      dt <- dt %>% dplyr::mutate(county_geoid = dplyr::if_else(county_geoid == "02270", "02158", county_geoid))
      dbg_detail("Wade Hampton recoding", "02270 -> 02158 (Kusilvak)")
    }
  }
  
  # Shannon County -> Oglala Lakota; Bedford City -> Bedford County
  dt <- dt %>%
    dplyr::mutate(
      county_geoid = dplyr::if_else(county_geoid == "46113", "46102", county_geoid),
      county_geoid = dplyr::if_else(county_geoid == "51515", "51019", county_geoid)
    )
  
  # Collapse after recoding
  dt <- dt %>%
    dplyr::group_by(year, county_geoid, naics_code) %>%
    dplyr::summarise(tap_emplvl_est_3 = sum(tap_emplvl_est_3, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(
      state_fips = stringr::str_sub(county_geoid, 1, 2),
      county_fips = stringr::str_sub(county_geoid, 3, 5),
      unknown_undefined_county = (county_fips == "999")
    ) %>%
    dplyr::rename(naics6_code = naics_code, naics6_employment_county = tap_emplvl_est_3)
  
  dbg("  After cleanup: %d rows", nrow(dt))
  dbg_detail("Unique counties", length(unique(dt$county_geoid)))
  dbg_detail("Unique NAICS6 codes", length(unique(dt$naics6_code)))
  
  if (QC_ON) {
    qc_snap_post_cleanup <- qc_totals_snapshot(dt, "naics6_employment_county")
    dbg("[QC] %d post-cleanup grand employment: %s", yr, format(qc_snap_post_cleanup$grand, big.mark = ","))
    qc_write_rds(qc_snap_post_cleanup, sprintf("qc_%d_snapshot_post_cleanup.rds", yr))
  }
  
  # ========================================================================
  # STEP 8.2b: APPLY NAICS CODE AGGREGATIONS
  # ========================================================================
  
  dbg("[8.2b] Applying NAICS code aggregations...")
  
  n_before_agg <- nrow(dt)
  unique_naics_before <- length(unique(dt$naics6_code))
  
  # BLS-only specialty trade contractor codes
  bls_mappings <- NAICS_AGGREGATION_RULES$bls_contractor_mappings
  n_bls_affected <- sum(dt$naics6_code %in% bls_mappings$source_code)
  
  if (n_bls_affected > 0) {
    dbg("    Remapping BLS-only contractor codes: %d records affected", n_bls_affected)
    bls_recode_vec <- setNames(bls_mappings$target_code, bls_mappings$source_code)
    dt <- dt %>%
      dplyr::mutate(
        naics6_code = dplyr::if_else(
          naics6_code %in% names(bls_recode_vec),
          bls_recode_vec[naics6_code],
          naics6_code
        )
      )
    dbg_detail("BLS contractor codes remapped", n_bls_affected)
  }
  
  # Three-digit rollup industries
  for (rollup in NAICS_AGGREGATION_RULES$three_digit_rollups) {
    pattern_match <- stringr::str_detect(dt$naics6_code, rollup$source_pattern)
    already_target <- dt$naics6_code == rollup$target_naics6
    pattern_match <- pattern_match & !already_target
    n_rollup_affected <- sum(pattern_match, na.rm = TRUE)
    
    if (n_rollup_affected > 0) {
      dbg("    Rolling up %s (%s): %d records", 
          rollup$target_naics6, rollup$target_title, n_rollup_affected)
      dt <- dt %>%
        dplyr::mutate(
          naics6_code = dplyr::if_else(
            stringr::str_detect(naics6_code, rollup$source_pattern) & naics6_code != rollup$target_naics6,
            rollup$target_naics6,
            naics6_code
          )
        )
    }
  }
  
  # Collapse after code transformations
  dt <- dt %>%
    dplyr::group_by(year, county_geoid, naics6_code, state_fips, county_fips, unknown_undefined_county) %>%
    dplyr::summarise(
      naics6_employment_county = sum(naics6_employment_county, na.rm = TRUE),
      .groups = "drop"
    )
  
  n_after_agg <- nrow(dt)
  unique_naics_after <- length(unique(dt$naics6_code))
  
  dbg("    Aggregation complete:")
  dbg_detail("Rows before aggregation", n_before_agg)
  dbg_detail("Rows after aggregation", n_after_agg)
  dbg_detail("Rows collapsed", n_before_agg - n_after_agg)
  
  if (QC_ON) {
    qc_snap_post_naics_agg <- qc_totals_snapshot(dt, "naics6_employment_county")
    dbg("[QC] %d post-NAICS-agg grand employment: %s", yr, format(qc_snap_post_naics_agg$grand, big.mark = ","))
    qc_compare_totals(qc_snap_post_cleanup, qc_snap_post_naics_agg,
                      key_dt = "by_geo", key_col = "county_geoid",
                      tol_abs = 0L, tol_rel = 0,
                      label = sprintf("%d_naics_agg_by_county", yr))
    qc_write_rds(qc_snap_post_naics_agg, sprintf("qc_%d_snapshot_post_naics_agg.rds", yr))
  }
  
  # ========================================================================
  # STEP 8.3: CT COUNTY -> PLANNING REGION CONVERSION
  # ========================================================================
  
  naics6_to_sector_mapping_yr <- get_naics6_to_sector_mapping(yr)
  needs_ct_conversion <- (yr >= 2010 && yr <= 2023)
  
  if (needs_ct_conversion) {
    dbg("[8.3] Applying CT county -> planning region conversion...")
    
    ct_to_convert <- dt %>% dplyr::filter(county_geoid %in% CT_OLD_COUNTIES, !unknown_undefined_county)
    non_ct_rows <- dt %>% dplyr::filter(!(state_fips == "09") | (state_fips == "09" & unknown_undefined_county))
    
    dbg_detail("CT rows to convert", nrow(ct_to_convert))
    
    if (nrow(ct_to_convert) > 0) {
      ct_to_convert <- ct_to_convert %>%
        dplyr::left_join(
          naics6_to_sector_mapping_yr %>% dplyr::select(naics6_code, sector_code_for_allocation_mapping),
          by = "naics6_code"
        ) %>%
        dplyr::mutate(sector_code_for_allocation_mapping = dplyr::if_else(
          is.na(sector_code_for_allocation_mapping), "00", sector_code_for_allocation_mapping))
      
      ct_xwalk_year <- ct_employment_crosswalk_by_sector_named %>%
        dplyr::filter(year == yr) %>%
        dplyr::select(from_geoid, to_geoid, sector_code_for_allocation_mapping, afact)
      
      ct_xwalk_fallback <- ct_xwalk_year %>%
        dplyr::filter(sector_code_for_allocation_mapping == "00") %>%
        dplyr::select(from_geoid, to_geoid, afact_fallback = afact)
      
      ct_with_factors <- ct_to_convert %>%
        dplyr::left_join(ct_xwalk_year, 
                         by = c("county_geoid" = "from_geoid", "sector_code_for_allocation_mapping"),
                         relationship = "many-to-many")
      
      ct_with_factors <- ct_with_factors %>%
        dplyr::left_join(ct_xwalk_fallback, by = c("county_geoid" = "from_geoid", "to_geoid")) %>%
        dplyr::mutate(afact = dplyr::if_else(is.na(afact), afact_fallback, afact)) %>%
        dplyr::select(-afact_fallback)
      
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
      
      if (QC_ON) {
        pre_ct_total  <- sum(ct_to_convert$naics6_employment_county, na.rm = TRUE)
        post_ct_total <- sum(ct_converted$naics6_employment_county, na.rm = TRUE)
        dbg("[QC] %d CT conversion grand total: before=%s after=%s diff=%s (%.4f%%)",
            yr, format(pre_ct_total, big.mark=","), format(post_ct_total, big.mark=","),
            format(post_ct_total - pre_ct_total, big.mark=","),
            100 * (post_ct_total - pre_ct_total) / ifelse(pre_ct_total == 0, 1, pre_ct_total))
      }
      
      dt <- dplyr::bind_rows(non_ct_rows, ct_converted)
      dbg("  CT conversion complete: %d rows", nrow(dt))
    }
  }
  
  # ========================================================================
  # STEP 8.4: AK VALDEZ-CORDOVA CONVERSION
  # ========================================================================
  
  needs_ak_vc_conversion <- (yr >= 2010 && yr <= 2019)
  
  if (needs_ak_vc_conversion) {
    ak_vc_to_convert <- dt %>% dplyr::filter(county_geoid == AK_VALDEZ_CORDOVA)
    non_ak_vc_rows <- dt %>% dplyr::filter(county_geoid != AK_VALDEZ_CORDOVA)
    
    if (nrow(ak_vc_to_convert) > 0) {
      dbg("[8.4] Applying AK Valdez-Cordova conversion...")
      dbg_detail("AK VC rows to convert", nrow(ak_vc_to_convert))
      
      ak_lookup_year <- if (yr > 2016) 2016L else yr
      
      ak_vc_to_convert <- ak_vc_to_convert %>%
        dplyr::left_join(
          naics6_to_sector_mapping_yr %>% dplyr::select(naics6_code, sector_code_for_allocation_mapping),
          by = "naics6_code"
        ) %>%
        dplyr::mutate(sector_code_for_allocation_mapping = dplyr::if_else(
          is.na(sector_code_for_allocation_mapping), "00", sector_code_for_allocation_mapping))
      
      ak_xwalk_year <- valdez_cordova_crosswalk_by_sector_named %>%
        dplyr::filter(year == ak_lookup_year) %>%
        dplyr::select(from_geoid, to_geoid, sector_code_for_allocation_mapping, afact)
      
      ak_xwalk_fallback <- ak_xwalk_year %>%
        dplyr::filter(sector_code_for_allocation_mapping == "00") %>%
        dplyr::select(from_geoid, to_geoid, afact_fallback = afact)
      
      ak_vc_with_factors <- ak_vc_to_convert %>%
        dplyr::left_join(ak_xwalk_year,
                         by = c("county_geoid" = "from_geoid", "sector_code_for_allocation_mapping"),
                         relationship = "many-to-many")
      
      ak_vc_with_factors <- ak_vc_with_factors %>%
        dplyr::left_join(ak_xwalk_fallback, by = c("county_geoid" = "from_geoid", "to_geoid")) %>%
        dplyr::mutate(afact = dplyr::if_else(is.na(afact), afact_fallback, afact)) %>%
        dplyr::select(-afact_fallback)
      
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
      
      dt <- dplyr::bind_rows(non_ak_vc_rows, ak_vc_converted)
      dbg("  AK conversion complete: %d rows", nrow(dt))
    }
  }
  
  # --- INTERMEDIATE EXPORT 2: Cleaned data (after CT/AK conversions) ---
  intermediate_02_file <- file.path(output_folder_path, sprintf("%d_intermediate_02_cleaned.parquet", yr))
  export_to_parquet(as.data.frame(dt), intermediate_02_file)
  year_intermediate_files <- c(year_intermediate_files, intermediate_02_file)
  dbg("  ✓ Intermediate 2 exported: Cleaned Tapestry data (post CT/AK) (Parquet)")
  
  # ========================================================================
  # STEP 8.5: JOIN NAICS HIERARCHY AND COMPUTE AGGREGATES
  # ========================================================================
  
  dbg("[8.5] Joining NAICS hierarchy and computing employment aggregates...")
  
  naics_version_for_year <- get_naics_version(yr)
  naics6_to_hierarchy_yr <- get_naics_hierarchy_lookup(yr)
  dbg_detail("Using NAICS version", naics_version_for_year)
  
  dt <- dt %>%
    dplyr::left_join(naics6_to_hierarchy_yr, by = "naics6_code")
  
  if (QC_ON) {
    qc_check_naics_join(dt, label = sprintf("%d_%s", yr, naics_version_for_year))
  }
  
  dt <- dt %>%
    dplyr::group_by(year, county_geoid, naics6_code, naics5_code, naics4_code, naics3_code) %>%
    dplyr::summarise(naics6_employment_county = sum(naics6_employment_county, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(
      state_fips = stringr::str_sub(county_geoid, 1, 2),
      county_fips = stringr::str_sub(county_geoid, 3, 5),
      unknown_undefined_county = (county_fips == "999")
    )
  
  setDT(dt)
  
  # NAICS6 Aggregates
  dt[, naics6_employment_estimated_state := sum(naics6_employment_county, na.rm = TRUE), by = .(state_fips, naics6_code)]
  dt[, naics6_employment_estimated_nation := sum(naics6_employment_county, na.rm = TRUE), by = .(naics6_code)]
  
  # NAICS5 Aggregates
  dt[, naics5_employment_county := sum(naics6_employment_county, na.rm = TRUE), by = .(county_geoid, naics5_code)]
  dt[, naics5_employment_estimated_state := sum(naics6_employment_county, na.rm = TRUE), by = .(state_fips, naics5_code)]
  dt[, naics5_employment_estimated_nation := sum(naics6_employment_county, na.rm = TRUE), by = .(naics5_code)]
  
  # NAICS4 Aggregates
  dt[, naics4_employment_county := sum(naics6_employment_county, na.rm = TRUE), by = .(county_geoid, naics4_code)]
  dt[, naics4_employment_estimated_state := sum(naics6_employment_county, na.rm = TRUE), by = .(state_fips, naics4_code)]
  dt[, naics4_employment_estimated_nation := sum(naics6_employment_county, na.rm = TRUE), by = .(naics4_code)]
  
  # NAICS3 Aggregates
  dt[, naics3_employment_county := sum(naics6_employment_county, na.rm = TRUE), by = .(county_geoid, naics3_code)]
  dt[, naics3_employment_estimated_state := sum(naics6_employment_county, na.rm = TRUE), by = .(state_fips, naics3_code)]
  dt[, naics3_employment_estimated_nation := sum(naics6_employment_county, na.rm = TRUE), by = .(naics3_code)]
  
  # Total Employment
  dt[, total_employment_estimated_county := sum(naics6_employment_county, na.rm = TRUE), by = .(county_geoid)]
  dt[, total_employment_estimated_state := sum(naics6_employment_county, na.rm = TRUE), by = .(state_fips)]
  dt[, total_employment_estimated_nation := sum(naics6_employment_county, na.rm = TRUE)]
  
  dbg("  Aggregates computed: %d rows", nrow(dt))
  
  # ========================================================================
  # STEP 8.6: COMPUTE LOCATION QUOTIENTS
  # ========================================================================
  
  dbg("[8.6] Computing location quotients at all geographic and NAICS levels...")
  
  tapestry_unknown_undefined <- copy(dt[unknown_undefined_county == TRUE])
  tapestry_unknown_undefined[, `:=`(county_in_cbsa = FALSE, county_in_csa = FALSE,
                                    cbsa_geoid = NA_character_, csa_geoid = NA_character_,
                                    commuting_zone_2020 = NA_integer_)]
  
  tapestry_real_counties <- copy(dt[unknown_undefined_county == FALSE])
  tapestry_real_counties[tigris_join_cols, on = "county_geoid",
                         `:=`(county_in_cbsa = i.county_in_cbsa, cbsa_geoid = i.cbsa_geoid,
                              county_in_csa = i.county_in_csa, csa_geoid = i.csa_geoid)]
  
  cz_join <- data.table::as.data.table(county_cz_crosswalk)[, .(county_geoid, commuting_zone_2020)]
  tapestry_real_counties <- merge(tapestry_real_counties, cz_join, by = "county_geoid", all.x = TRUE)
  
  tapestry_with_cbsa_csa <- rbindlist(list(tapestry_unknown_undefined, tapestry_real_counties),
                                      use.names = TRUE, fill = TRUE)
  setorder(tapestry_with_cbsa_csa, year, county_geoid, naics6_code)
  
  # CBSA/CSA/CZ aggregates for all NAICS levels
  tapestry_with_cbsa_csa[, naics6_employment_estimated_cbsa := sum(naics6_employment_county, na.rm = TRUE), by = .(cbsa_geoid, naics6_code)]
  tapestry_with_cbsa_csa[, naics6_employment_estimated_csa := sum(naics6_employment_county, na.rm = TRUE), by = .(csa_geoid, naics6_code)]
  tapestry_with_cbsa_csa[, naics6_employment_estimated_cz := sum(naics6_employment_county, na.rm = TRUE), by = .(commuting_zone_2020, naics6_code)]
  
  tapestry_with_cbsa_csa[, naics5_employment_estimated_cbsa := sum(naics6_employment_county, na.rm = TRUE), by = .(cbsa_geoid, naics5_code)]
  tapestry_with_cbsa_csa[, naics5_employment_estimated_csa := sum(naics6_employment_county, na.rm = TRUE), by = .(csa_geoid, naics5_code)]
  tapestry_with_cbsa_csa[, naics5_employment_estimated_cz := sum(naics6_employment_county, na.rm = TRUE), by = .(commuting_zone_2020, naics5_code)]
  
  tapestry_with_cbsa_csa[, naics4_employment_estimated_cbsa := sum(naics6_employment_county, na.rm = TRUE), by = .(cbsa_geoid, naics4_code)]
  tapestry_with_cbsa_csa[, naics4_employment_estimated_csa := sum(naics6_employment_county, na.rm = TRUE), by = .(csa_geoid, naics4_code)]
  tapestry_with_cbsa_csa[, naics4_employment_estimated_cz := sum(naics6_employment_county, na.rm = TRUE), by = .(commuting_zone_2020, naics4_code)]
  
  tapestry_with_cbsa_csa[, naics3_employment_estimated_cbsa := sum(naics6_employment_county, na.rm = TRUE), by = .(cbsa_geoid, naics3_code)]
  tapestry_with_cbsa_csa[, naics3_employment_estimated_csa := sum(naics6_employment_county, na.rm = TRUE), by = .(csa_geoid, naics3_code)]
  tapestry_with_cbsa_csa[, naics3_employment_estimated_cz := sum(naics6_employment_county, na.rm = TRUE), by = .(commuting_zone_2020, naics3_code)]
  
  tapestry_with_cbsa_csa[, total_employment_estimated_cbsa := sum(naics6_employment_county, na.rm = TRUE), by = .(cbsa_geoid)]
  tapestry_with_cbsa_csa[, total_employment_estimated_csa := sum(naics6_employment_county, na.rm = TRUE), by = .(csa_geoid)]
  tapestry_with_cbsa_csa[, total_employment_estimated_cz := sum(naics6_employment_county, na.rm = TRUE), by = .(commuting_zone_2020)]
  
  # Compute LQs
  tapestry_with_lq <- copy(tapestry_with_cbsa_csa[unknown_undefined_county == FALSE])
  tapestry_with_lq[, unknown_undefined_county := NULL]
  
  tapestry_with_lq[, naics6_national_share := naics6_employment_estimated_nation / total_employment_estimated_nation, by = naics6_code]
  tapestry_with_lq[, naics5_national_share := naics5_employment_estimated_nation / total_employment_estimated_nation, by = naics5_code]
  tapestry_with_lq[, naics4_national_share := naics4_employment_estimated_nation / total_employment_estimated_nation, by = naics4_code]
  tapestry_with_lq[, naics3_national_share := naics3_employment_estimated_nation / total_employment_estimated_nation, by = naics3_code]
  
  # NAICS6 LQs
  tapestry_with_lq[, `:=`(
    naics6_location_quotient_county = (naics6_employment_county / total_employment_estimated_county) / naics6_national_share,
    naics6_location_quotient_state = (naics6_employment_estimated_state / total_employment_estimated_state) / naics6_national_share,
    naics6_location_quotient_cbsa = (naics6_employment_estimated_cbsa / total_employment_estimated_cbsa) / naics6_national_share,
    naics6_location_quotient_csa = (naics6_employment_estimated_csa / total_employment_estimated_csa) / naics6_national_share,
    naics6_location_quotient_cz = (naics6_employment_estimated_cz / total_employment_estimated_cz) / naics6_national_share
  )]
  
  # NAICS5 LQs
  tapestry_with_lq[, `:=`(
    naics5_location_quotient_county = (naics5_employment_county / total_employment_estimated_county) / naics5_national_share,
    naics5_location_quotient_state = (naics5_employment_estimated_state / total_employment_estimated_state) / naics5_national_share,
    naics5_location_quotient_cbsa = (naics5_employment_estimated_cbsa / total_employment_estimated_cbsa) / naics5_national_share,
    naics5_location_quotient_csa = (naics5_employment_estimated_csa / total_employment_estimated_csa) / naics5_national_share,
    naics5_location_quotient_cz = (naics5_employment_estimated_cz / total_employment_estimated_cz) / naics5_national_share
  )]
  
  # NAICS4 LQs
  tapestry_with_lq[, `:=`(
    naics4_location_quotient_county = (naics4_employment_county / total_employment_estimated_county) / naics4_national_share,
    naics4_location_quotient_state = (naics4_employment_estimated_state / total_employment_estimated_state) / naics4_national_share,
    naics4_location_quotient_cbsa = (naics4_employment_estimated_cbsa / total_employment_estimated_cbsa) / naics4_national_share,
    naics4_location_quotient_csa = (naics4_employment_estimated_csa / total_employment_estimated_csa) / naics4_national_share,
    naics4_location_quotient_cz = (naics4_employment_estimated_cz / total_employment_estimated_cz) / naics4_national_share
  )]
  
  # NAICS3 LQs
  tapestry_with_lq[, `:=`(
    naics3_location_quotient_county = (naics3_employment_county / total_employment_estimated_county) / naics3_national_share,
    naics3_location_quotient_state = (naics3_employment_estimated_state / total_employment_estimated_state) / naics3_national_share,
    naics3_location_quotient_cbsa = (naics3_employment_estimated_cbsa / total_employment_estimated_cbsa) / naics3_national_share,
    naics3_location_quotient_csa = (naics3_employment_estimated_csa / total_employment_estimated_csa) / naics3_national_share,
    naics3_location_quotient_cz = (naics3_employment_estimated_cz / total_employment_estimated_cz) / naics3_national_share
  )]
  
  tapestry_with_lq[, c("naics6_national_share", "naics5_national_share", 
                       "naics4_national_share", "naics3_national_share") := NULL]
  
  dbg("  LQ computation complete for all NAICS levels")
  
  # ========================================================================
  # STEP 8.7: PREPARE DATA FOR COMPLEXITY
  # ========================================================================
  
  dbg("[8.7] Preparing data for complexity computation...")
  
  geo_level_data <- list()
  
  for (naics_lvl in NAICS_LEVELS) {
    geo_level_data[[naics_lvl]] <- list()
    naics_code_col <- paste0(naics_lvl, "_code")
    
    # County level
    county_lq <- unique(tapestry_with_lq[, .(
      year, geoid = county_geoid, industry_code = get(naics_code_col),
      industry_employment_estimated = get(paste0(naics_lvl, "_employment_county")),
      industry_location_quotient = get(paste0(naics_lvl, "_location_quotient_county"))
    )])
    county_lq[, geo_level := "county"]
    county_lq[, naics_level := naics_lvl]
    geo_level_data[[naics_lvl]][["county"]] <- county_lq
    
    # CBSA level
    cbsa_lq <- unique(tapestry_with_lq[!is.na(cbsa_geoid) & cbsa_geoid != "", .(
      year, geoid = cbsa_geoid, industry_code = get(naics_code_col),
      industry_employment_estimated = get(paste0(naics_lvl, "_employment_estimated_cbsa")),
      industry_location_quotient = get(paste0(naics_lvl, "_location_quotient_cbsa"))
    )])
    cbsa_lq[, geo_level := "cbsa"]
    cbsa_lq[, naics_level := naics_lvl]
    geo_level_data[[naics_lvl]][["cbsa"]] <- cbsa_lq
    
    # CSA level
    csa_lq <- unique(tapestry_with_lq[!is.na(csa_geoid) & csa_geoid != "", .(
      year, geoid = csa_geoid, industry_code = get(naics_code_col),
      industry_employment_estimated = get(paste0(naics_lvl, "_employment_estimated_csa")),
      industry_location_quotient = get(paste0(naics_lvl, "_location_quotient_csa"))
    )])
    csa_lq[, geo_level := "csa"]
    csa_lq[, naics_level := naics_lvl]
    geo_level_data[[naics_lvl]][["csa"]] <- csa_lq
    
    # CZ level
    cz_lq <- unique(tapestry_with_lq[!is.na(commuting_zone_2020), .(
      year, geoid = as.character(commuting_zone_2020), industry_code = get(naics_code_col),
      industry_employment_estimated = get(paste0(naics_lvl, "_employment_estimated_cz")),
      industry_location_quotient = get(paste0(naics_lvl, "_location_quotient_cz"))
    )])
    cz_lq[, geo_level := "cz"]
    cz_lq[, naics_level := naics_lvl]
    geo_level_data[[naics_lvl]][["cz"]] <- cz_lq
    
    # State level
    state_lq <- unique(tapestry_with_lq[, .(
      year, geoid = state_fips, industry_code = get(naics_code_col),
      industry_employment_estimated = get(paste0(naics_lvl, "_employment_estimated_state")),
      industry_location_quotient = get(paste0(naics_lvl, "_location_quotient_state"))
    )])
    state_lq[, geo_level := "state"]
    state_lq[, naics_level := naics_lvl]
    geo_level_data[[naics_lvl]][["state"]] <- state_lq
  }
  
  dbg("  Data prepared for all geo levels and NAICS levels")
  
  # --- INTERMEDIATE EXPORT 3 ---
  intermediate_03_file <- file.path(output_folder_path, sprintf("%d_intermediate_03_with_lq_aggregations.parquet", yr))
  export_to_parquet(as.data.frame(tapestry_with_lq), intermediate_03_file)
  year_intermediate_files <- c(year_intermediate_files, intermediate_03_file)
  dbg("  ✓ Intermediate 3 exported: Pre-complexity data (Parquet)")
  
  # ========================================================================
  # STEP 8.8: COMPUTE COMPLEXITY INDICATORS
  # ========================================================================
  
  dbg("[8.8] Computing complexity indicators at all NAICS and geographic levels...")
  
  complexity_results <- list()
  industry_space_results <- list()
  year_diagnostics <- list()
  
  title_lookups <- list(
    naics6 = data.table::as.data.table(get_naics_title_lookup(yr, "naics6")),
    naics5 = data.table::as.data.table(get_naics_title_lookup(yr, "naics5")),
    naics4 = data.table::as.data.table(get_naics_title_lookup(yr, "naics4")),
    naics3 = data.table::as.data.table(get_naics_title_lookup(yr, "naics3"))
  )
  
  for (naics_lvl in NAICS_LEVELS) {
    
    complexity_results[[naics_lvl]] <- list()
    industry_space_results[[naics_lvl]] <- list()
    year_diagnostics[[naics_lvl]] <- list()
    
    title_lookup <- title_lookups[[naics_lvl]]
    industry_code_col <- paste0(naics_lvl, "_code")
    industry_title_col <- paste0(naics_lvl, "_title")
    
    for (geo_lvl in COMPLEXITY_GEO_LEVELS) {
      dbg("  Processing: %s x %s", toupper(naics_lvl), toupper(geo_lvl))
      
      geo_data <- geo_level_data[[naics_lvl]][[geo_lvl]]
      geo_data <- geo_data[!is.na(geoid) & !is.na(industry_code) & !is.na(industry_location_quotient)]
      
      n_geos <- length(unique(geo_data$geoid))
      n_inds <- length(unique(geo_data$industry_code))
      
      if (n_geos < 2 || n_inds < 2) {
        dbg("    Skipping: insufficient data")
        next
      }
      
      complexity <- tryCatch({
        compute_all_complexity_indicators(geo_data, lq_col = "industry_location_quotient", 
                                          geo_col = "geoid", ind_col = "industry_code", verbose = FALSE)
      }, error = function(e) {
        dbg("    ERROR: %s", e$message)
        NULL
      })
      
      if (is.null(complexity)) next
      
      year_diagnostics[[naics_lvl]][[geo_lvl]] <- complexity$diagnostics
      
      industry_space_df <- create_industry_space_dataframe(
        complexity$proximity, title_lookup, yr, geo_lvl, naics_lvl
      )
      industry_space_results[[naics_lvl]][[geo_lvl]] <- industry_space_df
      
      # Build result data.table
      result_dt <- copy(geo_data)
      result_dt[, industry_comparative_advantage := industry_location_quotient >= 1]
      
      div_dt <- data.table(geoid = names(complexity$diversity), industrial_diversity = as.numeric(complexity$diversity))
      result_dt <- merge(result_dt, div_dt, by = "geoid", all.x = TRUE)
      
      ubq_dt <- data.table(industry_code = names(complexity$ubiquity), industry_ubiquity = as.numeric(complexity$ubiquity))
      result_dt <- merge(result_dt, ubq_dt, by = "industry_code", all.x = TRUE)
      
      eci_dt <- data.table(geoid = names(complexity$ECI), economic_complexity_index = as.numeric(complexity$ECI))
      result_dt <- merge(result_dt, eci_dt, by = "geoid", all.x = TRUE)
      
      ici_dt <- data.table(industry_code = names(complexity$ICI), industry_complexity_index = as.numeric(complexity$ICI))
      result_dt <- merge(result_dt, ici_dt, by = "industry_code", all.x = TRUE)
      
      feas_mat <- complexity$feasibility
      feas_dt <- as.data.table(feas_mat, keep.rownames = "geoid")
      feas_long <- melt(feas_dt, id.vars = "geoid", variable.name = "industry_code", value.name = "industry_feasibility")
      feas_long[, industry_code := as.character(industry_code)]
      result_dt <- merge(result_dt, feas_long, by = c("geoid", "industry_code"), all.x = TRUE)
      
      si_dt <- data.table(geoid = names(complexity$strategic_index), strategic_outlook = as.numeric(complexity$strategic_index))
      result_dt <- merge(result_dt, si_dt, by = "geoid", all.x = TRUE)
      
      sg_mat <- complexity$strategic_gain
      sg_dt <- as.data.table(sg_mat, keep.rownames = "geoid")
      sg_long <- melt(sg_dt, id.vars = "geoid", variable.name = "industry_code", value.name = "strategic_gain_from_industry")
      sg_long[, industry_code := as.character(industry_code)]
      result_dt <- merge(result_dt, sg_long, by = c("geoid", "industry_code"), all.x = TRUE)
      
      # Initialize proximity-adjusted columns
      result_dt[, proximity_adjusted_data_available := (geo_lvl == "county" & naics_lvl == "naics6")]
      result_dt[, `:=`(
        proximity_adjusted_location_quotient = NA_real_, ALQ = NA_real_, WLQ = NA_real_, WALQ = NA_real_,
        proximity_adjusted_comparative_advantage = NA,
        proximity_adjusted_industrial_diversity = NA_real_,
        proximity_adjusted_industry_ubiquity = NA_real_,
        proximity_adjusted_economic_complexity_index = NA_real_,
        proximity_adjusted_industry_complexity_index = NA_real_,
        proximity_adjusted_industry_feasibility = NA_real_,
        proximity_adjusted_strategic_outlook = NA_real_,
        proximity_adjusted_strategic_gain_from_industry = NA_real_
      )]
      
      # PA-LQ for NAICS6 county only
      if (geo_lvl == "county" && naics_lvl == "naics6" && !is.null(W_county) && !is.null(complexity$proximity)) {
        pa_lq_data <- tryCatch({
          compute_pa_lq(geo_data, W_county, complexity$proximity, ind_col = "industry_code")
        }, error = function(e) NULL)
        
        if (!is.null(pa_lq_data)) {
          result_dt[, c("proximity_adjusted_location_quotient", "ALQ", "WLQ", "WALQ") := NULL]
          result_dt <- merge(result_dt, pa_lq_data, by = c("geoid", "industry_code"), all.x = TRUE)
          result_dt[, proximity_adjusted_comparative_advantage := proximity_adjusted_location_quotient >= 1]
          
          temp_dt <- copy(geo_data)
          temp_dt <- merge(temp_dt, pa_lq_data[, .(geoid, industry_code, proximity_adjusted_location_quotient)], 
                           by = c("geoid", "industry_code"), all.x = TRUE)
          temp_dt[is.na(proximity_adjusted_location_quotient), proximity_adjusted_location_quotient := 0]
          temp_dt[, industry_location_quotient := proximity_adjusted_location_quotient]
          
          pa_complexity <- tryCatch({
            compute_all_complexity_indicators(temp_dt, lq_col = "industry_location_quotient", 
                                              geo_col = "geoid", ind_col = "industry_code", verbose = FALSE)
          }, error = function(e) NULL)
          
          if (!is.null(pa_complexity)) {
            pa_div_dt <- data.table(geoid = names(pa_complexity$diversity), proximity_adjusted_industrial_diversity = as.numeric(pa_complexity$diversity))
            result_dt[, proximity_adjusted_industrial_diversity := NULL]
            result_dt <- merge(result_dt, pa_div_dt, by = "geoid", all.x = TRUE)
            
            pa_ubq_dt <- data.table(industry_code = names(pa_complexity$ubiquity), proximity_adjusted_industry_ubiquity = as.numeric(pa_complexity$ubiquity))
            result_dt[, proximity_adjusted_industry_ubiquity := NULL]
            result_dt <- merge(result_dt, pa_ubq_dt, by = "industry_code", all.x = TRUE)
            
            pa_eci_dt <- data.table(geoid = names(pa_complexity$ECI), proximity_adjusted_economic_complexity_index = as.numeric(pa_complexity$ECI))
            result_dt[, proximity_adjusted_economic_complexity_index := NULL]
            result_dt <- merge(result_dt, pa_eci_dt, by = "geoid", all.x = TRUE)
            
            pa_ici_dt <- data.table(industry_code = names(pa_complexity$ICI), proximity_adjusted_industry_complexity_index = as.numeric(pa_complexity$ICI))
            result_dt[, proximity_adjusted_industry_complexity_index := NULL]
            result_dt <- merge(result_dt, pa_ici_dt, by = "industry_code", all.x = TRUE)
            
            pa_feas_mat <- pa_complexity$feasibility
            pa_feas_dt <- as.data.table(pa_feas_mat, keep.rownames = "geoid")
            pa_feas_long <- melt(pa_feas_dt, id.vars = "geoid", variable.name = "industry_code", value.name = "proximity_adjusted_industry_feasibility")
            pa_feas_long[, industry_code := as.character(industry_code)]
            result_dt[, proximity_adjusted_industry_feasibility := NULL]
            result_dt <- merge(result_dt, pa_feas_long, by = c("geoid", "industry_code"), all.x = TRUE)
            
            pa_si_dt <- data.table(geoid = names(pa_complexity$strategic_index), proximity_adjusted_strategic_outlook = as.numeric(pa_complexity$strategic_index))
            result_dt[, proximity_adjusted_strategic_outlook := NULL]
            result_dt <- merge(result_dt, pa_si_dt, by = "geoid", all.x = TRUE)
            
            pa_sg_mat <- pa_complexity$strategic_gain
            pa_sg_dt <- as.data.table(pa_sg_mat, keep.rownames = "geoid")
            pa_sg_long <- melt(pa_sg_dt, id.vars = "geoid", variable.name = "industry_code", value.name = "proximity_adjusted_strategic_gain_from_industry")
            pa_sg_long[, industry_code := as.character(industry_code)]
            result_dt[, proximity_adjusted_strategic_gain_from_industry := NULL]
            result_dt <- merge(result_dt, pa_sg_long, by = c("geoid", "industry_code"), all.x = TRUE)
          }
        }
      }
      
      complexity_results[[naics_lvl]][[geo_lvl]] <- result_dt
      dbg("    Complete: %d locations, %d industries", n_geos, n_inds)
    }
  }
  
  # ========================================================================
  # STEP 8.9: ADD NAMES
  # ========================================================================
  
  dbg("[8.9] Adding names and preparing final output data...")
  
  naics_title_dts <- list(
    naics6 = data.table::as.data.table(get_naics_title_lookup(yr, "naics6")),
    naics5 = data.table::as.data.table(get_naics_title_lookup(yr, "naics5")),
    naics4 = data.table::as.data.table(get_naics_title_lookup(yr, "naics4")),
    naics3 = data.table::as.data.table(get_naics_title_lookup(yr, "naics3"))
  )
  
  for (naics_lvl in names(complexity_results)) {
    for (geo_lvl in names(complexity_results[[naics_lvl]])) {
      result_dt <- complexity_results[[naics_lvl]][[geo_lvl]]
      if (is.null(result_dt) || nrow(result_dt) == 0) next
      
      if (geo_lvl == "county") {
        if ("geoid" %in% names(result_dt)) setnames(result_dt, "geoid", "county_geoid")
        result_dt <- merge(result_dt, as.data.table(county_names_lookup), by = "county_geoid", all.x = TRUE)
      } else if (geo_lvl == "cbsa") {
        if ("geoid" %in% names(result_dt)) setnames(result_dt, "geoid", "cbsa_geoid")
        result_dt <- merge(result_dt, as.data.table(cbsa_names_lookup), by = "cbsa_geoid", all.x = TRUE)
      } else if (geo_lvl == "csa") {
        if ("geoid" %in% names(result_dt)) setnames(result_dt, "geoid", "csa_geoid")
        result_dt <- merge(result_dt, as.data.table(csa_names_lookup), by = "csa_geoid", all.x = TRUE)
      } else if (geo_lvl == "cz") {
        if ("geoid" %in% names(result_dt)) setnames(result_dt, "geoid", "cz_geoid")
        result_dt <- merge(result_dt, as.data.table(cz_names_lookup), by = "cz_geoid", all.x = TRUE)
      }
      
      title_dt <- naics_title_dts[[naics_lvl]]
      code_col <- paste0(naics_lvl, "_code")
      title_col <- paste0(naics_lvl, "_title")
      
      if (code_col %in% names(title_dt) && title_col %in% names(title_dt)) {
        if ("industry_code" %in% names(result_dt)) setnames(result_dt, "industry_code", code_col)
        result_dt <- merge(result_dt, title_dt[, c(code_col, title_col), with = FALSE], by = code_col, all.x = TRUE)
        result_dt[, industry_title := get(title_col)]
      }
      
      complexity_results[[naics_lvl]][[geo_lvl]] <- result_dt
    }
  }
  
  # State LQ
  state_lq_final <- copy(geo_level_data[["naics6"]][["state"]])
  if (!is.null(state_lq_final) && nrow(state_lq_final) > 0) {
    if ("geoid" %in% names(state_lq_final)) setnames(state_lq_final, "geoid", "state_fips")
    if ("industry_code" %in% names(state_lq_final)) setnames(state_lq_final, "industry_code", "naics6_code")
    state_lq_final <- merge(state_lq_final, as.data.table(state_names_lookup), by = "state_fips", all.x = TRUE)
    state_lq_final <- merge(state_lq_final, naics_title_dts[["naics6"]], by = "naics6_code", all.x = TRUE)
  }
  
  # ========================================================================
  # STEP 8.10: DISPLAY TOP 15 (if requested)
  # ========================================================================
  
  if (show_top15) {
    cat("\n")
    cat(strrep("=", 80), "\n")
    cat(sprintf("TOP 15 ECI/ICI (YEAR %d) - NAICS6 COUNTY\n", yr))
    cat(strrep("=", 80), "\n")
    
    if ("naics6" %in% names(complexity_results) && "county" %in% names(complexity_results[["naics6"]])) {
      result_dt <- complexity_results[["naics6"]][["county"]]
      
      # Top 15 by ECI
      if ("economic_complexity_index" %in% names(result_dt)) {
        eci_by_area <- unique(result_dt[!is.na(economic_complexity_index), 
                                        .(county_geoid, county_name, economic_complexity_index, industrial_diversity)])
        setorder(eci_by_area, -economic_complexity_index)
        top15_eci <- head(eci_by_area, 15)
        
        cat("\nTOP 15 COUNTIES BY ECI:\n")
        cat(sprintf("%-4s %-50s %10s %10s\n", "Rank", "County", "ECI", "Diversity"))
        cat(strrep("-", 78), "\n")
        for (i in 1:min(15, nrow(top15_eci))) {
          row <- top15_eci[i]
          cat(sprintf("%-4d %-50s %10.4f %10.0f\n", i, substr(row$county_name, 1, 48), 
                      row$economic_complexity_index, row$industrial_diversity))
        }
      }
      
      # Top 15 by ICI
      if ("industry_complexity_index" %in% names(result_dt)) {
        ici_by_ind <- unique(result_dt[!is.na(industry_complexity_index),
                                       .(naics6_code, naics6_title, industry_complexity_index, industry_ubiquity)])
        setorder(ici_by_ind, -industry_complexity_index)
        top15_ici <- head(ici_by_ind, 15)
        
        cat("\nTOP 15 INDUSTRIES BY ICI:\n")
        cat(sprintf("%-4s %-8s %-38s %10s %10s\n", "Rank", "NAICS6", "Title", "ICI", "Ubiquity"))
        cat(strrep("-", 78), "\n")
        for (i in 1:min(15, nrow(top15_ici))) {
          row <- top15_ici[i]
          title_display <- if (!is.na(row$naics6_title)) substr(row$naics6_title, 1, 36) else "Unknown"
          cat(sprintf("%-4d %-8s %-38s %10.4f %10.0f\n", i, row$naics6_code, title_display,
                      row$industry_complexity_index, row$industry_ubiquity))
        }
      }
    }
  }
  
  # ========================================================================
  # STEP 8.11: EXPORT FILES IMMEDIATELY
  # ========================================================================
  
  dbg("[8.11] Exporting complexity results for year %d...", yr)
  
  # Export complexity data
  for (naics_lvl in names(complexity_results)) {
    for (geo_lvl in names(complexity_results[[naics_lvl]])) {
      complexity_dt <- complexity_results[[naics_lvl]][[geo_lvl]]
      if (is.null(complexity_dt) || nrow(complexity_dt) == 0) next
      
      rds_file <- file.path(output_folder_path, sprintf("%d_%s_%s_complexity.rds", yr, naics_lvl, geo_lvl))
      saveRDS(complexity_dt, rds_file)
      year_final_files <- c(year_final_files, rds_file)
      
      parquet_file <- file.path(output_folder_path, sprintf("%d_%s_%s_complexity.parquet", yr, naics_lvl, geo_lvl))
      export_to_parquet(as.data.frame(complexity_dt), parquet_file)
      year_final_files <- c(year_final_files, parquet_file)
      
      dbg("    ✓ %d %s x %s complexity (RDS + Parquet)", yr, toupper(naics_lvl), toupper(geo_lvl))
    }
  }
  
  # Export state LQ
  if (!is.null(state_lq_final) && nrow(state_lq_final) > 0) {
    rds_file <- file.path(output_folder_path, sprintf("%d_state_lq.rds", yr))
    saveRDS(state_lq_final, rds_file)
    parquet_file <- file.path(output_folder_path, sprintf("%d_state_lq.parquet", yr))
    export_to_parquet(as.data.frame(state_lq_final), parquet_file)
    dbg("    ✓ %d State LQ (RDS + Parquet)", yr)
  }
  
  # Export industry space
  for (naics_lvl in names(industry_space_results)) {
    for (geo_lvl in names(industry_space_results[[naics_lvl]])) {
      space_dt <- industry_space_results[[naics_lvl]][[geo_lvl]]
      if (is.null(space_dt) || nrow(space_dt) == 0) next
      
      rds_file <- file.path(output_folder_path, sprintf("%d_%s_%s_industry_space.rds", yr, naics_lvl, geo_lvl))
      saveRDS(space_dt, rds_file)
      parquet_file <- file.path(output_folder_path, sprintf("%d_%s_%s_industry_space.parquet", yr, naics_lvl, geo_lvl))
      export_to_parquet(as.data.frame(space_dt), parquet_file)
      dbg("    ✓ %d %s x %s industry space (RDS + Parquet)", yr, toupper(naics_lvl), toupper(geo_lvl))
    }
  }
  
  # Export diagnostics
  diag_file <- file.path(output_folder_path, sprintf("%d_diagnostics.rds", yr))
  saveRDS(year_diagnostics, diag_file)
  dbg("    ✓ %d Diagnostics (RDS)", yr)
  
  # ========================================================================
  # DELETE INTERMEDIATE FILES (automatic)
  # ========================================================================
  
  if (delete_intermediate && length(year_intermediate_files) > 0) {
    deleted_count <- 0
    for (f in year_intermediate_files) {
      if (file.exists(f)) {
        file.remove(f)
        deleted_count <- deleted_count + 1
      }
    }
    dbg("  Deleted %d intermediate files", deleted_count)
  }
  
  year_elapsed <- round(difftime(Sys.time(), year_start_time, units = "secs"), 1)
  cat(sprintf("\nYear %d complete in %.1f seconds\n", yr, year_elapsed))
  
  # Cleanup and return diagnostics
  rm(dt_raw, dt, tapestry_with_cbsa_csa, tapestry_with_lq, geo_level_data,
     complexity_results, industry_space_results, state_lq_final)
  gc()
  
  return(year_diagnostics)
}

# ==============================================================================
# PROCESS YEAR 2024 FIRST
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("PROCESSING YEAR 2024 FIRST\n")
cat(strrep("=", 80), "\n")

all_diagnostics[["2024"]] <- process_single_year(2024, delete_intermediate = TRUE, show_top15 = TRUE)

# ==============================================================================
# ASK ABOUT REMAINING YEARS
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
remaining_years <- setdiff(TAPESTRY_YEARS, 2024)
cat(sprintf("Remaining years to process: %s\n", paste(remaining_years, collapse = ", ")))
cat(strrep("=", 80), "\n\n")

process_remaining <- readline(prompt = "Process all remaining years (2023-2010)? [y/n]: ")

if (tolower(process_remaining) %in% c("y", "yes", "")) {
  for (yr in remaining_years) {
    all_diagnostics[[as.character(yr)]] <- process_single_year(yr, delete_intermediate = TRUE, show_top15 = FALSE)
  }
  cat("\nAll years processed.\n")
} else {
  cat("\nSkipping remaining years. Only 2024 data exported.\n")
}

cat("\n")
metadata_export_answer <- readline(prompt = "Export metadata/reference files (crosswalks, NAICS hierarchy)? [y/n]: ")

if (tolower(metadata_export_answer) %in% c("y", "yes", "")) {
  
  cat("\n[SECTION 9] Exporting metadata/reference files...\n")
  
  # CT Employment Crosswalk
  saveRDS(ct_employment_crosswalk_by_sector_named, 
          file.path(output_folder_path, "ct_employment_crosswalk_by_sector_named.rds"))
  export_to_parquet(as.data.frame(ct_employment_crosswalk_by_sector_named),
                    file.path(output_folder_path, "ct_employment_crosswalk_by_sector_named.parquet"))
  dbg("  ✓ CT employment crosswalk saved (RDS + Parquet)")
  
  # AK Valdez-Cordova Crosswalk
  saveRDS(valdez_cordova_crosswalk_by_sector_named, 
          file.path(output_folder_path, "valdez_cordova_crosswalk_by_sector_named.rds"))
  export_to_parquet(as.data.frame(valdez_cordova_crosswalk_by_sector_named),
                    file.path(output_folder_path, "valdez_cordova_crosswalk_by_sector_named.parquet"))
  dbg("  ✓ AK Valdez-Cordova crosswalk saved (RDS + Parquet)")
  
  # NAICS Hierarchy with Modifications
  saveRDS(naics_hierarchy_xwalk, 
          file.path(output_folder_path, "naics_hierarchy_xwalk.rds"))
  export_to_parquet(as.data.frame(naics_hierarchy_xwalk),
                    file.path(output_folder_path, "naics_hierarchy_xwalk.parquet"))
  dbg("  ✓ NAICS hierarchy crosswalk saved (RDS + Parquet)")
  
  # Integrated Geographic Crosswalk (tabular - non-spatial)
  county_cbsa_csa_cz_state_no_geom <- sf::st_drop_geometry(county_cbsa_csa_cz_state)
  saveRDS(county_cbsa_csa_cz_state_no_geom,
          file.path(output_folder_path, "county_cbsa_csa_cz_state_crosswalk.rds"))
  export_to_parquet(as.data.frame(county_cbsa_csa_cz_state_no_geom),
                    file.path(output_folder_path, "county_cbsa_csa_cz_state_crosswalk.parquet"))
  dbg("  ✓ County-CBSA-CSA-CZ-State crosswalk saved (tabular: RDS + Parquet)")
  
  # Integrated Geographic Crosswalk (with geometry - spatial)
  sf::st_write(county_cbsa_csa_cz_state, 
               file.path(output_folder_path, "county_cbsa_csa_cz_state_crosswalk.gpkg"),
               quiet = TRUE, delete_dsn = TRUE)
  dbg("  ✓ County-CBSA-CSA-CZ-State crosswalk saved (GeoPackage)")
  
  cat("\n  ✓ All metadata/reference files exported to:", output_folder_path, "\n")
  
} else {
  cat("\n  ✗ Metadata/reference file export skipped.\n")
}
 
# ==============================================================================
# SECTION 10: OUTPUT FOLDER SUMMARY AND OPTIONAL DELETION
# ==============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("OUTPUT FOLDER SUMMARY\n")
cat(strrep("=", 80), "\n\n")

cat(sprintf("Output folder: %s\n\n", output_folder_path))

# List all files in output folder
output_files <- list.files(output_folder_path, full.names = TRUE)
output_file_info <- file.info(output_files)
total_size_mb <- sum(output_file_info$size, na.rm = TRUE) / (1024 * 1024)

cat(sprintf("Total files: %d\n", length(output_files)))
cat(sprintf("Total size: %.1f MB\n\n", total_size_mb))

# Categorize files
final_files <- grep("complexity\\.parquet$|complexity\\.rds$|state_lq\\.|industry_space\\.|diagnostics\\.rds$", 
                    basename(output_files), value = TRUE)
intermediate_files <- grep("intermediate", basename(output_files), value = TRUE)
metadata_files <- grep("crosswalk|naics_hierarchy|spatial_weights|script", 
                       basename(output_files), value = TRUE)

cat("File breakdown:\n")
cat(sprintf("  - Final data files: %d\n", length(final_files)))
cat(sprintf("  - Intermediate files: %d\n", length(intermediate_files)))
cat(sprintf("  - Metadata/reference files: %d\n", length(metadata_files)))

# Prompt to delete the entire output folder
cat("\n")
cat(strrep("-", 60), "\n")
delete_folder_answer <- readline(prompt = sprintf(
  "Delete ENTIRE output folder and all its contents (%d files, %.1f MB)? [y/n]: ",
  length(output_files), total_size_mb
))

if (tolower(delete_folder_answer) %in% c("y", "yes")) {
  # Confirm deletion
  confirm_delete <- readline(prompt = sprintf(
    "CONFIRM: Permanently delete '%s' and ALL contents? Type 'DELETE' to confirm: ",
    output_folder_name
  ))
  
  if (confirm_delete == "DELETE") {
    unlink(output_folder_path, recursive = TRUE)
    if (!dir.exists(output_folder_path)) {
      cat(sprintf("\n  ✓ Output folder deleted: %s\n", output_folder_path))
    } else {
      cat(sprintf("\n  ⚠ Warning: Could not fully delete folder: %s\n", output_folder_path))
    }
  } else {
    cat("\n  Deletion cancelled. Output folder retained.\n")
  }
} else {
  cat("\n  Output folder retained.\n")
  cat(sprintf("  Location: %s\n", output_folder_path))
}

cat("\n")
cat(strrep("=", 80), "\n")
cat("SCRIPT EXECUTION COMPLETE\n")
cat(strrep("=", 80), "\n\n")

#Let's test the RDS file exported for year 2024, naics6, county complexity data
test_file_path <- file.path(output_folder_path, "2024_naics6_county_complexity.rds")
if (file.exists(test_file_path)) {
  test_data <- readRDS(test_file_path)
  cat(sprintf("Test read RDS file successful: %s\n", test_file_path))
  cat(sprintf("Data dimensions: %d rows, %d columns\n", nrow(test_data), ncol(test_data)))
} else {
  cat(sprintf("Test read RDS file failed: File does not exist - %s\n", test_file_path))
}

#Now let's glimpse() the test data
if (exists("test_data")) {
  glimpse(test_data)
}

#Now let's note that, if the test data exists, let's filter it such that if we have a column for naics6_title and a column for industry_title, and all the values within are the same, then we remove the one that says industry_title
if (exists("test_data")) {
  if ("naics6_title" %in% names(test_data) && "industry_title" %in% names(test_data)) {
    if (all(test_data$naics6_title == test_data$industry_title, na.rm = TRUE)) {
      test_data[, industry_title := NULL]
      cat("Removed redundant 'industry_title' column from test data.\n")
    } else {
      cat("'industry_title' column retained as it contains different values from 'naics6_title'.\n")
    }
  }
}

#Now let's make some other modifications to test_data
test_data_for_nesting <- test_data %>%
  select(-geo_level, -naics_level, -ALQ, -WALQ, -WLQ)
glimpse(test_data_for_nesting)

#For the nested data frame, let's first create a data frame with state_fips, county_geoid, and county_name
county_names <- test_data_for_nesting %>%
  select(state_fips, county_geoid, county_name) %>%
  distinct()
glimpse(county_names)

state_names <- test_data_for_nesting %>%
  select(state_fips, state_name) %>%
  distinct()
glimpse(state_names)

industry_names <- test_data_for_nesting %>%
  select(naics6_code, naics6_title) %>%
  distinct()
glimpse(county_names)

industry_specific_metrics <- test_data_for_nesting %>%
  select(naics6_code, industry_employment_estimated, industry_ubiquity, industry_complexity_index, proximity_adjusted_industry_ubiquity, proximity_adjusted_industry_complexity_index) %>%
  #Create an naics6_employment_nation column that sums industry_employment_estimated by naics6_code
  group_by(naics6_code) %>%
  mutate(industry_employment_nation = sum(industry_employment_estimated, na.rm = TRUE)) %>%
  ungroup() %>%
  select(-industry_employment_estimated) %>%
  distinct()
glimpse(industry_specific_metrics)

county_specific_metrics <- test_data_for_nesting %>%
  select(county_geoid, industrial_diversity, economic_complexity_index, strategic_outlook, proximity_adjusted_economic_complexity_index, proximity_adjusted_strategic_outlook) %>%
  distinct()
glimpse(county_specific_metrics)

county_industry_metrics <- test_data_for_nesting %>%
  select(county_geoid, naics6_code, 
         industry_employment_estimated, industry_location_quotient, 
         industry_comparative_advantage, industry_feasibility, 
         strategic_gain_from_industry, proximity_adjusted_location_quotient, 
         proximity_adjusted_comparative_advantage, 
         proximity_adjusted_industry_feasibility, 
         proximity_adjusted_strategic_gain_from_industry) 
glimpse(county_industry_metrics)

#Now create an RDS file containing the following data frames: county_names, state_names, industry_names, industry_specific_metrics, county_specific_metrics, county_industry_metrics
nested_test_data <- list(
  county_names = county_names,
  state_names = state_names,
  industry_names = industry_names,
  industry_specific_metrics = industry_specific_metrics,
  county_specific_metrics = county_specific_metrics,
  county_industry_metrics = county_industry_metrics
)
nested_rds_file <- file.path(output_folder_path, "2024_naics6_county_complexity_nested.rds")
saveRDS(nested_test_data, nested_rds_file)
cat(sprintf("Nested test data RDS file created: %s\n", nested_rds_file))
