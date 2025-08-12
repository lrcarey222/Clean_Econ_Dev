# This R script tests Tapestry estimates for county-level employment
# using BLS QCEW benchmarks and metadata.
#
# Tapestry portal (free account): https://tapestry.nkn.uidaho.edu/
# Paper/context: https://rrs.scholasticahq.com/article/123153-tapestry-collaborative-tool-for-regional-data-and-modeling
#
# QCEW downloads:
# - 2024 annual singlefile ZIP: https://data.bls.gov/cew/data/files/2024/csv/2024_annual_singlefile.zip
# - Layout/titles/crosswalks:
#   https://www.bls.gov/cew/about-data/downloadable-file-layouts/annual/naics-based-annual-layout.htm
#   https://www.bls.gov/cew/classifications/industry/industry-titles.htm
#   https://www.bls.gov/cew/classifications/areas/qcew-area-titles.htm
#   https://www.bls.gov/cew/classifications/ownerships/ownership-titles.htm
#   https://www.bls.gov/cew/classifications/size/size-titles.htm
#   https://www.bls.gov/cew/classifications/aggregation/agg-level-titles.htm
#   https://www.bls.gov/cew/classifications/areas/county-msa-csa-crosswalk.htm
#   https://www.bls.gov/cew/classifications/industry/qcew-naics-hierarchy-crosswalk.htm
#

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(lubridate)
  library(purrr)
  library(stringr)
  library(readxl)
  library(data.table)
})

options(readr.show_col_types = FALSE)

# =========================================================
# Debug helpers
# =========================================================
DEBUG <- TRUE
dbg <- function(...) if (isTRUE(DEBUG)) message("[DEBUG] ", paste0(..., collapse = ""))
tic <- function() { assign(".t0", proc.time(), envir = .GlobalEnv); invisible(TRUE) }
toc <- function(label = "") {
  if (exists(".t0", envir = .GlobalEnv)) {
    dt <- (proc.time() - get(".t0", envir = .GlobalEnv))["elapsed"]
    message(sprintf("[DEBUG] %sElapsed: %.2f sec", if (nzchar(label)) paste0(label, " | ") else "", dt))
  } else {
    message("[DEBUG] Timer not started")
  }
}

# =========================================================
# OneDrive root detection (Windows-first)
# =========================================================
home <- path.expand("~")
sys  <- Sys.info()[["sysname"]]
dbg("OS: ", sys)

candidates <- unique(c(
  file.path(Sys.getenv("USERPROFILE"), "OneDrive - RMI"),
  file.path(Sys.getenv("USERPROFILE"), "OneDrive - Rocky Mountain Institute"),
  file.path(Sys.getenv("USERPROFILE"), "OneDrive"),
  Sys.getenv("OneDrive"),
  file.path(home, "Library", "CloudStorage", "OneDrive-RMI"),
  file.path(home, "Library", "CloudStorage", "OneDrive - RMI"),
  file.path(home, "Library", "CloudStorage", "OneDrive"),
  file.path(home, "OneDrive - RMI"),
  file.path(home, "OneDrive")
))

dbg("Checking OneDrive roots (", length(candidates), " candidates)")
ONE_DRIVE_ROOT <- NULL
for (p in candidates) {
  if (nzchar(p) && dir.exists(p)) {
    ONE_DRIVE_ROOT <- normalizePath(p, winslash = "/", mustWork = TRUE)
    break
  }
}
if (is.null(ONE_DRIVE_ROOT)) {
  stop("Could not locate OneDrive folder. Checked:\n - ",
       paste(candidates, collapse = "\n - "))
}
dbg("Using OneDrive root: ", ONE_DRIVE_ROOT)

# =========================================================
# Project subpaths
# =========================================================
US_PROG_DOCS <- file.path(
  ONE_DRIVE_ROOT,
  "US Program - Documents", "6_Projects", "Clean Regional Economic Development",
  "ACRE", "Data", "Raw Data"
)
QCEW_ANNUAL_DIR   <- file.path(US_PROG_DOCS, "BLS_QCEW", "annual_data")
QCEW_META_DIR     <- file.path(US_PROG_DOCS, "BLS_QCEW", "metadata")
TAPESTRY_EMPLOYED <- file.path(US_PROG_DOCS, "Tapestry_Employment")
TARGET_YEAR <- "2024"

# =========================================================
# QCEW annual singlefile — ingest (single pass) + mask
# =========================================================
dbg("QCEW annual dir: ", QCEW_ANNUAL_DIR)
if (!dir.exists(QCEW_ANNUAL_DIR)) stop("QCEW annual data folder not found: ", QCEW_ANNUAL_DIR)

files_found <- list.files(QCEW_ANNUAL_DIR, pattern = "^2024\\.annual\\.singlefile\\.csv$", full.names = TRUE)
if (length(files_found) == 0) stop("Could not find '2024.annual.singlefile.csv' in: ", QCEW_ANNUAL_DIR)
QCEW_2024_ANNUAL_FILE <- normalizePath(files_found[1], winslash = "/", mustWork = TRUE)
dbg("QCEW file: ", QCEW_2024_ANNUAL_FILE)

.standardize_qcew_cols <- function(df) {
  nm <- names(df)
  if ("area_fips" %in% nm)      df <- df %>% mutate(area_fips = as.character(area_fips))
  if ("industry_code" %in% nm)  df <- df %>% mutate(industry_code = as.character(industry_code))
  for (cc in c("own_code","agglvl_code","size_code","year")) {
    if (cc %in% nm) df[[cc]] <- suppressWarnings(as.numeric(df[[cc]]))
  }
  df
}
.mask_qcew <- function(df) {
  nm <- names(df)
  if ("disclosure_code" %in% nm && "annual_avg_emplvl" %in% nm) {
    disclosed <- is.na(df$disclosure_code) | trimws(df$disclosure_code) == ""
    df$annual_avg_emplvl <- ifelse(disclosed, df$annual_avg_emplvl, NA_real_)
  }
  if ("lq_disclosure_code" %in% nm && "lq_annual_avg_emplvl" %in% nm) {
    lq_disclosed <- is.na(df$lq_disclosure_code) | trimws(df$lq_disclosure_code) == ""
    df$lq_annual_avg_emplvl <- ifelse(lq_disclosed, df$lq_annual_avg_emplvl, NA_real_)
  }
  df
}

tic()
wanted <- c(
  "area_fips","own_code","industry_code","agglvl_code","size_code","year",
  "disclosure_code","annual_avg_estabs","annual_avg_emplvl",
  "lq_disclosure_code","lq_annual_avg_estabs","lq_annual_avg_emplvl"
)
QCEW_2024_ANNUAL <- suppressMessages(
  readr::read_csv(QCEW_2024_ANNUAL_FILE, col_select = any_of(wanted), progress = FALSE)
) %>%
  .standardize_qcew_cols() %>%
  .mask_qcew()
toc("QCEW read")

dbg("QCEW rows: ", nrow(QCEW_2024_ANNUAL), " | cols: ", ncol(QCEW_2024_ANNUAL),
    " | size: ", format(utils::object.size(QCEW_2024_ANNUAL), units = "MB"))

# Optional: if establishments appear universally masked/zero, blank them globally
if (all(c("disclosure_code","annual_avg_estabs") %in% names(QCEW_2024_ANNUAL))) {
  disclosed <- is.na(QCEW_2024_ANNUAL$disclosure_code) | trimws(QCEW_2024_ANNUAL$disclosure_code) == ""
  keep_estabs <- any(!disclosed & !is.na(QCEW_2024_ANNUAL$annual_avg_estabs) & QCEW_2024_ANNUAL$annual_avg_estabs != 0)
  dbg("QCEW keep_estabs heuristic: ", keep_estabs)
  if (!keep_estabs) {
    if (is.integer(QCEW_2024_ANNUAL$annual_avg_estabs)) QCEW_2024_ANNUAL$annual_avg_estabs <- NA_integer_
    else QCEW_2024_ANNUAL$annual_avg_estabs <- NA_real_
  }
}

# =========================================================
# QCEW metadata (area, industry, ownership, agg level, crosswalk)
# =========================================================
AREA_TITLES_FILE                    <- file.path(QCEW_META_DIR, "area-titles.csv")
INDUSTRY_TITLES_FILE                <- file.path(QCEW_META_DIR, "industry-titles.csv")
SIZE_TITLES_FILE                    <- file.path(QCEW_META_DIR, "size-titles.csv")
AGG_LEVEL_TITLES_FILE               <- file.path(QCEW_META_DIR, "agg-level-titles.csv")
OWNERSHIP_TITLES_FILE               <- file.path(QCEW_META_DIR, "ownership-titles.csv")
COUNTY_MSA_CSA_CROSSWALK_FILE       <- file.path(QCEW_META_DIR, "qcew_county-msa-csa-crosswalk-2024.csv")
QCEW_NAICS_HIERARCHY_CROSSWALK_FILE <- file.path(QCEW_META_DIR, "qcew-naics-hierarchy-crosswalk.xlsx")

req_meta <- c(AREA_TITLES_FILE, INDUSTRY_TITLES_FILE, SIZE_TITLES_FILE,
              AGG_LEVEL_TITLES_FILE, OWNERSHIP_TITLES_FILE,
              COUNTY_MSA_CSA_CROSSWALK_FILE, QCEW_NAICS_HIERARCHY_CROSSWALK_FILE)
missing_meta <- req_meta[!file.exists(req_meta)]
if (length(missing_meta)) stop("Missing metadata files:\n - ", paste(missing_meta, collapse = "\n - "))

tic()
AREA_TITLES              <- suppressMessages(readr::read_csv(AREA_TITLES_FILE,     col_types = cols(area_fips = col_character())))
INDUSTRY_TITLES          <- suppressMessages(readr::read_csv(INDUSTRY_TITLES_FILE, col_types = cols(industry_code = col_character())))
SIZE_TITLES              <- suppressMessages(readr::read_csv(SIZE_TITLES_FILE))
AGG_LEVEL_TITLES         <- suppressMessages(readr::read_csv(AGG_LEVEL_TITLES_FILE))
OWNERSHIP_TITLES         <- suppressMessages(readr::read_csv(OWNERSHIP_TITLES_FILE))
COUNTY_MSA_CSA_CROSSWALK <- suppressMessages(readr::read_csv(COUNTY_MSA_CSA_CROSSWALK_FILE))
QCEW_NAICS_HIERARCHY_CROSSWALK <- suppressMessages(readxl::read_excel(QCEW_NAICS_HIERARCHY_CROSSWALK_FILE, sheet = "v2022"))
toc("Metadata read")

dbg("AREA_TITLES rows: ", nrow(AREA_TITLES),
    " | INDUSTRY_TITLES rows: ", nrow(INDUSTRY_TITLES),
    " | AGG_LEVEL_TITLES rows: ", nrow(AGG_LEVEL_TITLES))

# =========================================================
# Build robust NAICS code -> title lookup (from both BLS titles and crosswalk)
# =========================================================
normalize_naics <- function(x) {
  x <- as.character(x)
  x <- str_trim(x)
  # Keep only digits (drop any stray punctuation/hyphens)
  x <- str_replace_all(x, "[^0-9]", "")
  x[x == ""] <- NA_character_
  x
}

# From industry-titles.csv (primary source)
lu_industry_titles <- INDUSTRY_TITLES %>%
  transmute(industry_code = normalize_naics(industry_code),
            industry_title = industry_title) %>%
  filter(!is.na(industry_code)) %>%
  distinct(industry_code, .keep_all = TRUE)

# From NAICS hierarchy crosswalk (secondary/fallback; handle column name variability)
nm_cw <- names(QCEW_NAICS_HIERARCHY_CROSSWALK)
pick_col <- function(patterns) {
  ix <- which(vapply(patterns, function(p) any(str_detect(nm_cw, p)), logical(1)))
  if (!length(ix)) return(NA_character_)
  nm_cw[which(str_detect(nm_cw, patterns[ix[1]]))[1]]
}
cw_pairs <- list()
for (d in c("3","4","5","6")) {
  code_col  <- pick_col(c(paste0("^naics", d, "_?code$"),
                          paste0("^naics_", d, "_?code$"),
                          paste0("^NAICS", d, ".*code$")))
  title_col <- pick_col(c(paste0("^naics", d, "_?title$"),
                          paste0("^naics_", d, "_?title$"),
                          paste0("^NAICS", d, ".*title$")))
  if (!is.na(code_col) && !is.na(title_col)) {
    cw_pairs[[length(cw_pairs) + 1]] <- QCEW_NAICS_HIERARCHY_CROSSWALK %>%
      transmute(industry_code = normalize_naics(.data[[code_col]]),
                industry_title = .data[[title_col]])
  }
}
lu_crosswalk <- bind_rows(cw_pairs) %>%
  filter(!is.na(industry_code), !is.na(industry_title)) %>%
  distinct(industry_code, .keep_all = TRUE)

# Merge both sources (favor BLS titles when duplicates)
IND_LOOKUP <- bind_rows(
  lu_industry_titles %>% mutate(.src = "BLS_titles"),
  lu_crosswalk %>% mutate(.src = "crosswalk")
) %>%
  arrange(industry_code, desc(.src)) %>%
  distinct(industry_code, .keep_all = TRUE) %>%
  select(-.src)

# =========================================================
# Enrichment helpers
# =========================================================
QCEW_add_metadata <- function(df){
  df %>%
    mutate(area_fips = if ("area_fips" %in% names(.)) as.character(area_fips) else NA_character_) %>%
    left_join(AREA_TITLES,      by = "area_fips")      %>% relocate(area_title,   .after = area_fips) %>%
    left_join(OWNERSHIP_TITLES, by = "own_code")       %>% relocate(own_title,    .after = own_code) %>%
    left_join(IND_LOOKUP,       by = c("industry_code"))%>% relocate(industry_title, .after = industry_code) %>%
    left_join(AGG_LEVEL_TITLES, by = "agglvl_code")    %>% relocate(agglvl_title, .after = agglvl_code) %>%
    left_join(SIZE_TITLES,      by = "size_code")      %>% relocate(size_title,   .after = size_code)
}

# Enrich QCEW
tic()
QCEW_2024_ENRICHED <- QCEW_add_metadata(QCEW_2024_ANNUAL)
toc("QCEW enrichment")
dbg("QCEW_2024_ENRICHED rows: ", nrow(QCEW_2024_ENRICHED), " | cols: ", ncol(QCEW_2024_ENRICHED),
    " | size: ", format(utils::object.size(QCEW_2024_ENRICHED), units = "MB"))

# Debug peeks
glimpse(QCEW_2024_ANNUAL)
glimpse(AREA_TITLES)
glimpse(IND_LOOKUP)
glimpse(AGG_LEVEL_TITLES)
glimpse(OWNERSHIP_TITLES)
glimpse(QCEW_2024_ENRICHED)

# =========================================================
# TAPESTRY: file discovery -> read -> enrich (sequential)
# =========================================================
parse_tapestry_filename <- function(path){
  fname <- basename(path)
  m <- str_match(
    fname,
    "^(\\d{4})(?:_(NAICS(\\d)D))?_(COUNTY|NATIONAL)_(?:OWN?CODE(\\d)|AGGREGATE(?:_EMPLOYMENT)?)\\.csv$"
  )
  if (all(is.na(m))) {
    tibble(
      file = path, year = NA_character_, naics_level = NA_character_,
      geo = NA_character_, own_code = NA_character_, is_aggregate = NA
    )
  } else {
    tibble(
      file = path,
      year = m[,2],
      naics_level = m[,4],            # "6","5","4","3" or NA for aggregate
      geo  = m[,5],                   # "COUNTY" or "NATIONAL"
      own_code = m[,6],               # may be NA for aggregate
      is_aggregate = ifelse(is.na(m[,6]), TRUE, FALSE) & !is.na(m[,2])
    )
  }
}

tapestry_year_dir <- file.path(TAPESTRY_EMPLOYED, TARGET_YEAR)
dbg("Tapestry year dir: ", tapestry_year_dir)
if (!dir.exists(tapestry_year_dir)) stop("Tapestry folder not found: ", tapestry_year_dir)

TAPESTRY_FILES <- list.files(tapestry_year_dir, pattern = "\\.csv$", recursive = TRUE, full.names = TRUE)
dbg("Tapestry CSV count: ", length(TAPESTRY_FILES))

TAPESTRY_INDEX <- purrr::map_dfr(TAPESTRY_FILES, parse_tapestry_filename) %>%
  mutate(
    year = coalesce(year, TARGET_YEAR),
    naics_level = ifelse(is.na(naics_level) & grepl("AGGREGATE", basename(file)), NA, naics_level),
    own_code = suppressWarnings(ifelse(!is.na(own_code), as.numeric(own_code), NA_real_)),
    group_label = dplyr::case_when(
      is.na(naics_level) & geo == "COUNTY"    ~ "COUNTY_TOTALS",
      !is.na(naics_level) & geo == "COUNTY"   ~ paste0("NAICS", naics_level, "D"),
      !is.na(naics_level) & geo == "NATIONAL" ~ paste0("NATIONAL_NAICS", naics_level, "D"),
      TRUE ~ "OTHER"
    )
  )

dbg("TAPESTRY index rows: ", nrow(TAPESTRY_INDEX))

add_area_title_if_present <- function(df){
  if ("area_fips" %in% names(df)) {
    df %>%
      mutate(area_fips = if (is.numeric(.data$area_fips)) sprintf("%05.0f", .data$area_fips) else as.character(.data$area_fips)) %>%
      left_join(AREA_TITLES %>% distinct(area_fips, area_title), by = "area_fips", relationship = "many-to-one") %>%
      relocate(area_title, .after = area_fips)
  } else df
}
add_own_title_if_present <- function(df){
  if ("own_code" %in% names(df)) {
    df %>%
      mutate(own_code = suppressWarnings(as.numeric(.data$own_code))) %>%
      left_join(OWNERSHIP_TITLES %>% distinct(own_code, own_title),
                by = "own_code", relationship = "many-to-one") %>%
      relocate(own_title, .after = own_code)
  } else df
}
add_industry_title_if_present <- function(df){
  # Robustly derive a single 'industry_code' character column from any NAICS* column
  has <- names(df)
  if ("industry_code" %in% has) {
    df <- df %>% mutate(industry_code = normalize_naics(.data$industry_code))
  } else if ("naics_code" %in% has) {
    df <- df %>% mutate(industry_code = normalize_naics(.data$naics_code))
  } else if ("naics_6_digit" %in% has) {
    df <- df %>% mutate(industry_code = normalize_naics(.data$naics_6_digit))
  } else if ("naics_5_digit" %in% has) {
    df <- df %>% mutate(industry_code = normalize_naics(.data$naics_5_digit))
  } else if ("naics_4_digit" %in% has) {
    df <- df %>% mutate(industry_code = normalize_naics(.data$naics_4_digit))
  } else if ("naics_3_digit" %in% has) {
    df <- df %>% mutate(industry_code = normalize_naics(.data$naics_3_digit))
  }
  if ("industry_code" %in% names(df)) {
    df %>%
      left_join(IND_LOOKUP, by = "industry_code", relationship = "many-to-one") %>%
      relocate(industry_title, .after = industry_code)
  } else df
}
enrich_tapestry_df <- function(df){
  df %>%
    add_area_title_if_present() %>%
    add_own_title_if_present() %>%
    add_industry_title_if_present()
}

.read_one_tapestry <- function(file_path, meta) {
  tic()
  df <- suppressMessages(readr::read_csv(file_path, progress = FALSE))
  toc(paste0("Read Tapestry: ", basename(file_path)))
  dbg("File: ", basename(file_path), " | rows: ", nrow(df), " | cols: ", ncol(df),
      " | size: ", format(utils::object.size(df), units = "MB"))
  
  df <- df %>%
    mutate(
      .src_file  = file_path,
      .year      = meta$year,
      .geo       = meta$geo,
      .naics_lvl = meta$naics_level,
      .own_code  = meta$own_code,
      .is_agg    = isTRUE(meta$is_aggregate)
    ) %>%
    relocate(.src_file:.is_agg, .before = 1)
  
  tic()
  out <- enrich_tapestry_df(df)
  toc(paste0("Enrich Tapestry: ", basename(file_path)))
  out
}

files_tbl <- TAPESTRY_INDEX %>% distinct(file, .keep_all = TRUE)
fvec <- files_tbl$file
dbg("Sequential load over ", length(fvec), " files")

TAPESTRY_ENRICHED_BY_FILE <- vector("list", length(fvec))
names(TAPESTRY_ENRICHED_BY_FILE) <- basename(fvec)
for (i in seq_along(fvec)) {
  fp <- fvec[[i]]
  meta <- files_tbl %>% dplyr::filter(file == fp) %>% dplyr::slice(1) %>% as.list()
  dbg("(", i, "/", length(fvec), ") -> ", basename(fp))
  TAPESTRY_ENRICHED_BY_FILE[[i]] <- tryCatch(
    .read_one_tapestry(fp, meta),
    error = function(e){
      message("[DEBUG] ERROR reading ", basename(fp), ": ", conditionMessage(e))
      NULL
    }
  )
}

invisible(purrr::iwalk(
  TAPESTRY_ENRICHED_BY_FILE,
  ~ if (!is.null(.x)) glimpse(.x)
))
dbg("Completed. Non-null Tapestry tables: ",
    sum(!vapply(TAPESTRY_ENRICHED_BY_FILE, is.null, logical(1))))

# ===== Memory-Optimized RAM-safe finalizer for Tapestry =====
suppressPackageStartupMessages({ library(data.table) })

# Pull in what you already have (built earlier in the script)
tapestry_compact <- Filter(Negate(is.null), TAPESTRY_ENRICHED_BY_FILE)
stopifnot(length(tapestry_compact) > 0)

# Infer the year directory from loaded tables (names or .src_file inside)
src_candidates <- unique(c(
  names(tapestry_compact),
  unlist(lapply(tapestry_compact, function(x) if (".src_file" %in% names(x)) x$.src_file[1] else NA))
))
src_candidates <- src_candidates[nzchar(src_candidates) & !is.na(src_candidates)]
YEAR_DIR <- if (length(src_candidates)) dirname(src_candidates[1]) else getwd()

# --- Lookups / fixed levels (deduped) ---
AREA_LEVELS       <- unique(c("US000", AREA_TITLES$area_fips))
AREA_TITLE_MAP    <- setNames(AREA_TITLES$area_title, AREA_TITLES$area_fips)
AREA_TITLE_LEVELS <- unique(AREA_TITLES$area_title)

OWN_TITLE_MAP     <- setNames(OWNERSHIP_TITLES$own_title, OWNERSHIP_TITLES$own_code)
OWN_TITLE_LEVELS  <- unique(OWNERSHIP_TITLES$own_title)

# IND_LOOKUP was built earlier from INDUSTRY_TITLES + NAICS hierarchy.
IND_CODES         <- IND_LOOKUP$industry_code
IND_TITLE_LEVELS  <- unique(IND_LOOKUP$industry_title)

GEO_LEVELS <- c("COUNTY","NATIONAL")

# --- MEMORY-OPTIMIZED Normalizer (no wages, no src) ---
.prep_enriched_chunk <- function(df, src_name) {
  setDT(df)
  if (!"area_fips" %in% names(df)) df[, area_fips := NA_character_]
  
  # Create temporary variables for metadata columns that might not exist
  if (!"year" %in% names(df)) df[, year := NA_real_]
  if (!"own_code" %in% names(df)) df[, own_code := NA_integer_]
  
  suppressWarnings({
    if (".year" %in% names(df))      df[, year := as.numeric(.year)]
    if (".naics_lvl" %in% names(df)) df[, naics_level := as.integer(.naics_lvl)] else df[, naics_level := NA_integer_]
    if (".own_code" %in% names(df))  df[, own_code := fifelse(!is.na(own_code), as.integer(own_code), as.integer(.own_code))]
  })
  
  df[, area_fips := as.character(area_fips)]
  if (".geo" %in% names(df)) df[, geo := factor(.geo, levels = GEO_LEVELS)] else df[, geo := NA_character_]
  if (".geo" %in% names(df)) df[, area_fips := fifelse(.geo == "NATIONAL", "US000", area_fips)]
  
  # coalesce NAICS* -> industry_code
  for (c in c("naics_code","naics_6_digit","naics_5_digit","naics_4_digit","naics_3_digit","industry_code")) {
    if (!c %in% names(df)) df[, (c) := NA_character_] else df[, (c) := as.character(get(c))]
  }
  df[, industry_code := fcoalesce(industry_code, naics_code, naics_6_digit, naics_5_digit, naics_4_digit, naics_3_digit)]
  
  # Factors with fixed, deduped levels
  df[, area_fips := factor(area_fips, levels = AREA_LEVELS)]
  idx_area <- match(as.character(df$area_fips), names(AREA_TITLE_MAP))
  df[, area_title := factor(AREA_TITLE_MAP[idx_area], levels = AREA_TITLE_LEVELS)]
  
  df[, industry_code := factor(industry_code, levels = IND_CODES)]
  idx_ind <- match(as.character(df$industry_code), IND_LOOKUP$industry_code)
  df[, industry_title := factor(IND_LOOKUP$industry_title[idx_ind], levels = IND_TITLE_LEVELS)]
  
  idx_own <- match(df$own_code, as.integer(names(OWN_TITLE_MAP)))
  df[, own_title := factor(OWN_TITLE_MAP[idx_own], levels = OWN_TITLE_LEVELS)]
  
  # Only keep employment and establishments (drop wages & provenance)
  for (m in c("tap_estabs_count","tap_emplvl_est_3")) if (!m %in% names(df)) df[, (m) := NA_real_]
  keep <- c("year","geo","area_fips","area_title",
            "naics_level","industry_code","industry_title",
            "own_code","own_title",
            "tap_estabs_count","tap_emplvl_est_3")
  drop <- setdiff(names(df), keep)
  if (length(drop)) df[, (drop) := NULL]
  
  existing_keep <- intersect(keep, names(df))
  if (length(existing_keep) > 0) setcolorder(df, existing_keep)
  df[]
}

# --- (Optional) Stream a large CSV in row-chunks and normalize on the fly ---
.stream_and_normalize <- function(file, chunk_rows = 3e5L) {
  b  <- basename(file)
  rx <- "^(\\d{4})_NAICS(\\d)D_(COUNTY|NATIONAL)_OWNCODE(\\d)\\.csv$"
  year      <- as.integer(sub(rx, "\\1", b))
  naics_lvl <- as.integer(sub(rx, "\\2", b))
  geo       <- sub(rx, "\\3", b)
  own_meta  <- as.integer(sub(rx, "\\4", b))
  
  hdr  <- fread(file, nrows = 0L, showProgress = FALSE)
  coln <- names(hdr)
  
  out  <- vector("list", 0L)
  skip <- 1L  # skip header row
  
  repeat {
    ch <- fread(file, skip = skip, nrows = chunk_rows, header = FALSE,
                col.names = coln, showProgress = FALSE)
    if (!nrow(ch)) break
    
    # harmonize column names seen in dumps
    if ("County Code"  %in% names(ch) && !"area_fips"  %in% names(ch)) setnames(ch, "County Code",  "area_fips")
    if ("County Title" %in% names(ch) && !"area_title" %in% names(ch)) setnames(ch, "County Title", "area_title")
    if ("naics6_code"  %in% names(ch) && !"naics_6_digit" %in% names(ch)) setnames(ch, "naics6_code", "naics_6_digit")
    if ("naics5_code"  %in% names(ch) && !"naics_5_digit" %in% names(ch)) setnames(ch, "naics5_code", "naics_5_digit")
    if ("naics4_code"  %in% names(ch) && !"naics_4_digit" %in% names(ch)) setnames(ch, "naics4_code", "naics_4_digit")
    if ("naics3_code"  %in% names(ch) && !"naics_3_digit" %in% names(ch)) setnames(ch, "naics3_code", "naics_3_digit")
    
    # ensure metrics exist
    for (m in c("tap_estabs_count","tap_emplvl_est_3")) {
      if (!m %in% names(ch)) ch[, (m) := NA_real_]
    }
    
    ch[, `.src_file` := file]
    ch[, `.year`     := as.character(year)]
    ch[, `.geo`      := geo]
    ch[, `.naics_lvl`:= as.character(naics_lvl)]
    ch[, `.own_code` := own_meta]
    
    out[[length(out) + 1L]] <- .prep_enriched_chunk(ch, src_name = b)
    
    skip <- skip + nrow(ch)
    rm(ch); gc()
  }
  rbindlist(out, use.names = TRUE)
}

# --- Build compact chunks from what's already in memory ---
out_small <- vector("list", length(tapestry_compact))
i <- 0L
for (nm in names(tapestry_compact)) {
  i <- i + 1L
  message(sprintf("[DEBUG] Processing %s (%d/%d)", nm, i, length(tapestry_compact)))
  tryCatch({
    out_small[[i]] <- .prep_enriched_chunk(tapestry_compact[[nm]], src_name = nm)
  }, error = function(e) {
    message(sprintf("[DEBUG] Error processing %s: %s", nm, e$message))
    NULL
  })
}
out_small <- Filter(Negate(is.null), out_small[seq_len(i)])

# --- Find any big 6-digit COUNTY files that failed to load; stream only those ---
all_csvs <- list.files(YEAR_DIR, full.names = TRUE, pattern = "\\.csv$")
loaded_basenames <- basename(names(tapestry_compact))
need_stream <- all_csvs[
  grepl("^\\d{4}_NAICS6D_COUNTY_OWNCODE(1|2|3|5)\\.csv$", basename(all_csvs)) &
    !(basename(all_csvs) %in% loaded_basenames)
]

out_big <- if (length(need_stream)) {
  message(sprintf("[CHUNK] Streaming %d file(s): %s", length(need_stream), paste(basename(need_stream), collapse=", ")))
  lapply(need_stream, function(f) {
    tryCatch(.stream_and_normalize(f, chunk_rows = 3e5L),
             error = function(e) { message(sprintf("[DEBUG] Error streaming %s: %s", basename(f), e$message)); NULL })
  })
} else {
  list()
}
out_big <- Filter(Negate(is.null), out_big)

# --- Final bind & order (low-copy) ---
TAPESTRY_TALL <- data.table::rbindlist(c(out_small, out_big), use.names = TRUE, fill = TRUE)
rm(out_small, out_big); gc()

setorder(TAPESTRY_TALL, geo, area_fips, own_code, naics_level, industry_code)

# --- SAFER OUTPUT: Show dimensions and small sample ---
message("[DEBUG] TAPESTRY_TALL dimensions: ", nrow(TAPESTRY_TALL), " rows x ", ncol(TAPESTRY_TALL), " cols")
message("[DEBUG] Size: ", format(object.size(TAPESTRY_TALL), units = "GB"))
message("[DEBUG] Columns: ", paste(names(TAPESTRY_TALL), collapse = ", "))

message("\n[DEBUG] Sample of first 100 rows:")
print(head(TAPESTRY_TALL, 100))

message("\n[DEBUG] Summary by geo:")
print(TAPESTRY_TALL[, .N, by = geo])

message("\n[DEBUG] Summary by naics_level:")
print(TAPESTRY_TALL[, .N, by = naics_level])

message("\n[DEBUG] Summary by own_code:")
print(TAPESTRY_TALL[, .N, by = own_code])

message("[DEBUG] Glimpse of combined Tapestry data:")
glimpse(TAPESTRY_TALL)

# =========================================================
# QCEW slices for join + helpers to align to Tapestry NAICS levels
# (QCEW aggregation level title is QCEW-only)
# =========================================================

# Normalize AGG_LEVEL_TITLES to standard names; build geo/digit helpers
agg_code_col  <- names(AGG_LEVEL_TITLES)[stringr::str_detect(
  names(AGG_LEVEL_TITLES), stringr::regex("aggl?v?l?_?code|aggregation.*code", ignore_case = TRUE)
)][1]
agg_title_col <- names(AGG_LEVEL_TITLES)[stringr::str_detect(
  names(AGG_LEVEL_TITLES), stringr::regex("aggl?v?l?_?title|aggregation.*title", ignore_case = TRUE)
)][1]
if (is.na(agg_code_col) || is.na(agg_title_col)) {
  stop("Could not find agg-level code/title columns in AGG_LEVEL_TITLES.")
}

AGG_LEVEL_TITLES_NORM <- AGG_LEVEL_TITLES %>%
  dplyr::rename(
    agglvl_code  = !!rlang::sym(agg_code_col),
    agglvl_title = !!rlang::sym(agg_title_col)
  ) %>%
  dplyr::mutate(agglvl_code = suppressWarnings(as.numeric(agglvl_code)))

AGG_LEVEL_TITLES_ENR <- AGG_LEVEL_TITLES_NORM %>%
  dplyr::mutate(
    geo_key = dplyr::case_when(
      stringr::str_detect(agglvl_title, stringr::regex("County",   ignore_case = TRUE)) ~ "COUNTY",
      stringr::str_detect(agglvl_title, stringr::regex("National", ignore_case = TRUE)) ~ "NATIONAL",
      stringr::str_detect(agglvl_title, stringr::regex("State",    ignore_case = TRUE)) ~ "STATE",
      TRUE ~ NA_character_
    ),
    naics_digits = dplyr::case_when(
      stringr::str_detect(agglvl_title, stringr::regex("6\\s*[-]?\\s*digit", ignore_case = TRUE)) ~ 6L,
      stringr::str_detect(agglvl_title, stringr::regex("5\\s*[-]?\\s*digit", ignore_case = TRUE)) ~ 5L,
      stringr::str_detect(agglvl_title, stringr::regex("4\\s*[-]?\\s*digit", ignore_case = TRUE)) ~ 4L,
      stringr::str_detect(agglvl_title, stringr::regex("3\\s*[-]?\\s*digit", ignore_case = TRUE)) ~ 3L,
      stringr::str_detect(agglvl_title, stringr::regex("2\\s*[-]?\\s*digit|sector", ignore_case = TRUE)) ~ 2L,
      stringr::str_detect(agglvl_title, stringr::regex("Total",    ignore_case = TRUE)) ~ NA_integer_,
      TRUE ~ NA_integer_
    )
  )

# Filter QCEW to all-establishment-sizes and relevant ownerships; attach meta (no duplicate title)
QCEW_FOR_JOIN_raw <- QCEW_2024_ENRICHED %>%
  dplyr::filter(size_code == 0, own_code %in% c(0,1,2,3,5)) %>%
  dplyr::mutate(
    geography_level = dplyr::if_else(area_fips == "US000", "NATIONAL", "COUNTY"),
    naics_digits    = nchar(industry_code),
    qcew_emp_disclosed = is.na(disclosure_code) | trimws(disclosure_code) == "",
    qcew_lq_disclosed  = is.na(lq_disclosure_code) | trimws(lq_disclosure_code) == ""
  ) %>%
  dplyr::left_join(
    AGG_LEVEL_TITLES_ENR %>%
      dplyr::select(
        agglvl_code,
        geo_key,
        naics_digits_cw = naics_digits,
        agglvl_title_meta = agglvl_title
      ),
    by = "agglvl_code"
  ) %>%
  dplyr::mutate(
    naics_digits       = suppressWarnings(as.integer(naics_digits)),
    naics_digits_final = dplyr::coalesce(naics_digits_cw, naics_digits)
  ) %>%
  dplyr::filter(geo_key %in% c("COUNTY","NATIONAL"))

# Authoritative QCEW aggregation level title (QCEW-only)
if ("agglvl_title" %in% names(QCEW_FOR_JOIN_raw)) {
  QCEW_FOR_JOIN_raw <- QCEW_FOR_JOIN_raw %>%
    dplyr::mutate(qcew_aggregation_level = dplyr::coalesce(agglvl_title, agglvl_title_meta))
} else {
  QCEW_FOR_JOIN_raw <- QCEW_FOR_JOIN_raw %>%
    dplyr::mutate(qcew_aggregation_level = agglvl_title_meta)
}

QCEW_FOR_JOIN <- QCEW_FOR_JOIN_raw %>%
  dplyr::select(
    area_fips, industry_code, own_code, agglvl_code,
    qcew_aggregation_level,                      # <- QCEW-only title
    geography_level_qcew = geography_level,
    qcew_emp_disclosed, qcew_lq_disclosed,
    annual_avg_estabs, annual_avg_emplvl, lq_annual_avg_emplvl,
    naics_digits_final
  ) %>%
  dplyr::mutate(
    area_fips = as.character(area_fips),
    industry_code = as.character(industry_code)
  )

# =========================================================
# Build the combined comparison frame  (FAST, data.table)
# =========================================================
suppressPackageStartupMessages({ library(data.table) })

# Helper: vectorized labeler
label_naics_level_vec <- function(x){
  out <- character(length(x))
  out[is.na(x)] <- "Aggregate (All Industries)"
  out[x == 6L]  <- "NAICS 6-Digit"
  out[x == 5L]  <- "NAICS 5-Digit"
  out[x == 4L]  <- "NAICS 4-Digit"
  out[x == 3L]  <- "NAICS 3-Digit"
  out[!(is.na(out) | x %in% c(3L,4L,5L,6L))] <- paste0("NAICS ", x[!(is.na(out) | x %in% c(3L,4L,5L,6L))], "-Digit")
  out
}

# Small helpers for “prefer real values over zeros/NA”
max_or_na <- function(v){
  m <- suppressWarnings(max(v, na.rm = TRUE))
  if (is.infinite(m)) NA_real_ else m
}

# -----------------------------
# 1) Tapestry slice, keyed + DEDUPED (data.table)
# -----------------------------
TAP <- copy(TAPESTRY_TALL)
setDT(TAP)

# Keep only needed levels and columns for speed
TAP <- TAP[
  !is.na(industry_code) &
    naics_level %in% c(3L,4L,5L,6L) &
    own_code %in% c(0L,1L,2L,3L,5L),
  .(
    geography_level = as.character(geo),
    area_fips       = as.character(area_fips),
    area_title      = as.character(area_title),
    industry_code   = as.character(industry_code),
    industry_title  = as.character(industry_title),
    naics_level     = as.integer(naics_level),
    own_code        = as.integer(own_code),
    own_title       = as.character(own_title),
    tap_estabs_count = as.numeric(tap_estabs_count),
    tap_emplvl_est_3 = as.numeric(tap_emplvl_est_3)
  )
]

# Collapse duplicates quickly; prefer non-zero/non-NA via max()
setkey(TAP,
       geography_level, area_fips, area_title,
       own_code, own_title, naics_level,
       industry_code, industry_title
)

TAP_DEDUP <- TAP[
  , .(
    `Tapestry annual average establishments` = max_or_na(tap_estabs_count),
    `Tapestry annual average employment level` = max_or_na(tap_emplvl_est_3)
  ),
  by = .(geography_level, area_fips, area_title,
         own_code, own_title, naics_level,
         industry_code, industry_title)
]

TAP_DEDUP[
  , `industry aggregation level (e.g. "NAICS 6-Digit")` := label_naics_level_vec(naics_level)
]

# -----------------------------
# 2) QCEW slice, keyed + DEDUPED (data.table)
# -----------------------------
QCEW <- as.data.table(QCEW_FOR_JOIN)[
  , .(
    `QCEW aggregation level` = {x <- na.omit(qcew_aggregation_level); if (length(x)) x[1] else qcew_aggregation_level[1]},
    `QCEW employment disclosed` = any(qcew_emp_disclosed %in% TRUE, na.rm = TRUE),
    `QCEW location quotient disclosed` = any(qcew_lq_disclosed %in% TRUE, na.rm = TRUE),
    `QCEW annual average establishments` = max_or_na(annual_avg_estabs),
    `QCEW annual average employment level (if disclosed)` = max_or_na(annual_avg_emplvl),
    `QCEW employment-based location quotient (if disclosed)` = max_or_na(lq_annual_avg_emplvl),
    naics_digits_final = as.integer(naics_digits_final)
  ),
  by = .(geography_level_qcew, area_fips = as.character(area_fips),
         industry_code = as.character(industry_code),
         own_code = as.integer(own_code))
]

setkey(QCEW, area_fips, industry_code, own_code)

# -----------------------------
# 3) Join + enforce digit/geography alignment
# -----------------------------
setkey(TAP_DEDUP, area_fips, industry_code, own_code)

COMB <- QCEW[TAP_DEDUP, on = .(area_fips, industry_code, own_code)]

# Keep rows where geography & NAICS digits align
COMB <- COMB[
  geography_level == geography_level_qcew &
    naics_level == naics_digits_final
]

# -----------------------------
# 4) Core columns + comparisons (tibble)
# -----------------------------
COMBINED_CORE <- tibble::tibble(
  `geography level (county or national)` = COMB$geography_level,
  `area code` = as.character(COMB$area_fips),
  `area title` = as.character(COMB$area_title),
  `industry code` = as.character(COMB$industry_code),
  `industry title` = as.character(COMB$industry_title),
  `industry aggregation level (e.g. "NAICS 6-Digit")` = as.character(COMB$`industry aggregation level (e.g. "NAICS 6-Digit")`),
  `ownership code` = as.integer(COMB$own_code),
  `ownership title` = as.character(COMB$own_title),
  `QCEW aggregation level` = as.character(COMB$`QCEW aggregation level`),
  `QCEW employment disclosed` = as.logical(COMB$`QCEW employment disclosed`),
  `QCEW location quotient disclosed` = as.logical(COMB$`QCEW location quotient disclosed`),
  `QCEW annual average establishments` = as.numeric(COMB$`QCEW annual average establishments`),
  `QCEW annual average employment level (if disclosed)` = as.numeric(COMB$`QCEW annual average employment level (if disclosed)`),
  `QCEW employment-based location quotient (if disclosed)` = as.numeric(COMB$`QCEW employment-based location quotient (if disclosed)`),
  `Tapestry annual average establishments` = as.numeric(COMB$`Tapestry annual average establishments`),
  `Tapestry annual average employment level` = as.numeric(COMB$`Tapestry annual average employment level`)
)

COMBINED_WITH_COMPARISONS <- COMBINED_CORE %>%
  dplyr::mutate(
    `Employment: Tapestry minus QCEW` =
      `Tapestry annual average employment level` - `QCEW annual average employment level (if disclosed)`,
    `Employment: % diff vs QCEW` =
      dplyr::if_else(!is.na(`QCEW annual average employment level (if disclosed)`) &
                       `QCEW annual average employment level (if disclosed)` != 0,
                     (`Tapestry annual average employment level` - `QCEW annual average employment level (if disclosed)`) /
                       `QCEW annual average employment level (if disclosed)`,
                     NA_real_),
    `Establishments: Tapestry minus QCEW` =
      `Tapestry annual average establishments` - `QCEW annual average establishments`,
    `Establishments: % diff vs QCEW` =
      dplyr::if_else(!is.na(`QCEW annual average establishments`) &
                       `QCEW annual average establishments` != 0,
                     (`Tapestry annual average establishments` - `QCEW annual average establishments`) /
                       `QCEW annual average establishments`,
                     NA_real_)
  )

# =========================================================
# Debug/peek of outputs
# =========================================================
dbg("COMBINED_CORE rows: ", nrow(COMBINED_CORE), " | cols: ", ncol(COMBINED_CORE))
glimpse(COMBINED_CORE)

dbg("COMBINED_WITH_COMPARISONS rows: ", nrow(COMBINED_WITH_COMPARISONS), " | cols: ", ncol(COMBINED_WITH_COMPARISONS))
glimpse(COMBINED_WITH_COMPARISONS)
