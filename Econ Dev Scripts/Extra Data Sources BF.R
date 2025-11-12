###############################################################################
## MASTER R SCRIPT — REORGANIZED BY GEOGRAPHIC LEVEL
## Sections: Setup → Base Geographies → County → State → CBSA → National → 
##           Point/Facility → Cross-Geography → Final
###############################################################################

# =============================================================================
# SECTION 1: SETUP - PACKAGES, PATHS, HELPERS
# =============================================================================

# ---- 1.1 Package Loading (ALL packages upfront) ----------------------------
# Set CRAN mirror first
if (is.null(getOption("repos")) || is.na(getOption("repos")["CRAN"]) || 
    getOption("repos")["CRAN"] %in% c("", "@CRAN@")) {
  options(repos = c(CRAN = "https://cloud.r-project.org"))
}

# Master package list
pkgs <- c(
  # Data manipulation
  "data.table", "dplyr", "tidyr", "tibble", "purrr", "janitor",
  # String/date handling  
  "stringr", "lubridate", "glue",
  # File I/O
  "readr", "readxl", "writexl", "arrow", "fs", "tools",
  # Web scraping/APIs
  "httr", "jsonlite", "rvest", "curl",
  # Spatial
  "sf", "tigris", "lwgeom", "esri2sf",
  # Census/data sources
  "tidycensus", "censusapi", "usmapdata",
  # Visualization
  "ggplot2",
  # Parallel processing
  "furrr", "future",
  # Utilities
  "cli", "crayon", "zoo", "fuzzyjoin", "V8", "magrittr", "withr", "rlang"
)

# Install and load function
ensure_pkg <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    message(sprintf("Installing package: %s", pkg))
    tryCatch(
      utils::install.packages(pkg, quiet = TRUE),
      error = function(e) stop(sprintf("Failed to install '%s': %s", pkg, e$message))
    )
  }
  suppressPackageStartupMessages(
    library(pkg, character.only = TRUE, quietly = TRUE, warn.conflicts = FALSE)
  )
}

# Load all packages
invisible(lapply(pkgs, ensure_pkg))

# ---- 1.2 Global Options -----------------------------------------------------
options(
  timeout = 600,
  tigris_use_cache = TRUE,
  tigris_class = "sf",
  warn = 1
)
sf::sf_use_s2(TRUE)
data.table::setDTthreads(0L)
Sys.setenv(TZ = "America/New_York")

# Census API key
if (!nzchar(Sys.getenv("CENSUS_KEY"))) {
  Sys.setenv(CENSUS_KEY = '0b3d37ac56ab19c5a65cbc188f82d8ce5b36cfe6')
}
readRenviron("~/.Renviron")

# ---- 1.3 Helper Functions ---------------------------------------------------
.now <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.msg <- function(level = "INFO", text) message(sprintf("%s [%s] %s", .now(), level, text))
.warn <- function(text) .msg("WARN", text)
.err <- function(text) .msg("ERROR", text)

DEBUG_MODE <- TRUE
dbg <- function(obj, lbl = deparse(substitute(obj))) {
  if (!DEBUG_MODE) return(invisible(obj))
  .msg("DEBUG", sprintf("Inspecting: %s", lbl))
  if (is.data.frame(obj) || inherits(obj, "sf")) {
    suppressMessages(dplyr::glimpse(obj))
  }
  invisible(obj)
}

g <- function(x, nm) {
  cat("\n--- GLIMPSE:", nm, "---\n")
  suppressMessages(dplyr::glimpse(x))
  invisible(x)
}

# Safe download function
safe_download <- function(url, dest, mode = "wb", tries = 3, sleep_sec = 2, show = TRUE) {
  if (show) .msg("INFO", sprintf("Downloading: %s", basename(dest)))
  for (i in seq_len(tries)) {
    ok <- try({
      resp <- httr::RETRY("GET", url, httr::write_disk(dest, overwrite = TRUE), httr::timeout(120))
      sc <- httr::status_code(resp)
      sc >= 200 && sc < 300
    }, silent = TRUE)
    if (isTRUE(ok) && file.exists(dest) && file.info(dest)$size > 0) return(dest)
    if (i < tries) Sys.sleep(sleep_sec)
  }
  stop(sprintf("Download failed: %s", url))
}

# Safe fread wrapper
safe_fread <- function(path, ...) {
  tryCatch(
    data.table::fread(path, showProgress = FALSE, ...),
    error = function(e) {
      warning("fread error: ", path, " - ", e$message)
      data.table::data.table()
    }
  ) %>% tibble::as_tibble()
}

# Close all sinks safely
close_all_sinks <- function() {
  for (tp in c("message", "output")) {
    repeat {
      n <- tryCatch(sink.number(type = tp), error = function(e) 0L)
      if (is.null(n) || n <= 0L) break
      sink(type = tp)
    }
  }
}

# ---- 1.4 Path Setup ---------------------------------------------------------
resolve_onedrive_root <- function() {
  candidates <- c(
    fs::path_expand("~/Library/CloudStorage/OneDrive-RMI"),
    fs::path_expand("~/Library/CloudStorage/OneDrive - RMI"),
    Sys.getenv("OneDrive"),
    fs::path(Sys.getenv("USERPROFILE"), "OneDrive - RMI"),
    fs::path(Sys.getenv("USERPROFILE"), "OneDrive"),
    fs::path_home()
  )
  candidates <- candidates[nzchar(candidates)]
  for (p in candidates) if (fs::dir_exists(p)) return(fs::path_norm(p))
  stop("Could not locate OneDrive root")
}

base_onedrive_path <- resolve_onedrive_root()
.msg("INFO", sprintf("Base path: %s", base_onedrive_path))
setwd(base_onedrive_path)

# Path shortcuts
fp <- function(...) as.character(fs::path(...))
US_PROG_DIR <- fp(base_onedrive_path, "US Program - Documents")
DATA_FOLDER <- fp(US_PROG_DIR, "6_Projects", "Clean Regional Economic Development", "ACRE", "Data")
RAW_DATA_FOLDER <- fp(DATA_FOLDER, "Raw Data")
PROCESSED_GPKG_FOLDER <- fp(RAW_DATA_FOLDER, "Geopackages", "Processed")
geopkg_folder <- fp(RAW_DATA_FOLDER, "Geopackages")
map_app_dir <- fp(DATA_FOLDER, "ChatGPT", "map_app")

# Create directories
for (d in c(DATA_FOLDER, RAW_DATA_FOLDER, PROCESSED_GPKG_FOLDER, map_app_dir)) {
  fs::dir_create(d, recurse = TRUE)
}

# ---- 1.5 Logging Setup ------------------------------------------------------
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_file_name <- paste0("geographic_data_", timestamp, ".log")
log_file <- file.path(getwd(), log_file_name)
log_con <- file(log_file, open = "wt")
sink(log_con, type = "output", split = TRUE)
sink(log_con, type = "message")
.msg("INFO", sprintf("Logging to: %s", log_file))

# Ensure cleanup on exit
withr::defer({
  try(future::plan(sequential), silent = TRUE)
  close_all_sinks()
  try(if (exists("log_con") && isOpen(log_con)) close(log_con), silent = TRUE)
}, envir = globalenv())

# ---- 1.6 Parallel Processing Setup ------------------------------------------
num_workers <- max(1, min(6, future::availableCores(omit = 1)))
future::plan(multisession, workers = num_workers)
.msg("INFO", sprintf("Parallel workers: %d", future::nbrOfWorkers()))

# Constants
CRS_USE <- 4326
EXCLUDE_TERRITORIES <- c("60", "66", "69", "72", "78", "VI", "GU", "AS", "MP", "PR")

# =============================================================================
# NSF PATENT DATA — function + separate county/state usage blocks
# =============================================================================
#MORE HERE: https://ncses.nsf.gov/pubs/nsb20241/; https://ncses.nsf.gov/pubs/nsb20241/data 
# load_nsf_patent_data()
# - Reads NSF swbinv-3 (county+state) and swbinv-1 (county) workbooks
# - Harmonizes year columns, adds totals/metadata, and returns a list:
#   list(county = <tibble>, state = <tibble>)
# Prereqs used from your script: fp(), RAW_DATA_FOLDER, .msg(), dbg(), g()
load_nsf_patent_data <- function(raw_data_folder = RAW_DATA_FOLDER, parallel = TRUE) {
  .msg("INFO","NSF Patent Data (county/state) ...")
  SWBINV_3_FILE <- fp(raw_data_folder,"Invention, Knowledge Transfer, and Innovation (NSF)","swbinv-3.xlsx")
  SWBINV_1_FILE <- fp(raw_data_folder,"Invention, Knowledge Transfer, and Innovation (NSF)","swbinv-1.xlsx")
  
  INDEX_3 <- if (file.exists(SWBINV_3_FILE)) readxl::read_excel(SWBINV_3_FILE, sheet="Index") else { warning("Missing swbinv-3.xlsx"); tibble::tibble() }
  INDEX_1 <- if (file.exists(SWBINV_1_FILE)) readxl::read_excel(SWBINV_1_FILE, sheet="Index") else { warning("Missing swbinv-1.xlsx"); tibble::tibble() }
  dbg(INDEX_3,"INDEX_3"); dbg(INDEX_1,"INDEX_1")
  
  NSF_PATENT_DATA_COUNTY <- tibble::tibble(); NSF_PATENT_DATA_STATE <- tibble::tibble()
  
  if (nrow(INDEX_3)>0 || nrow(INDEX_1)>0) {
    STATE_FIPS_LOOKUP <- tidycensus::fips_codes %>% dplyr::distinct(state, state_code, state_name) %>%
      dplyr::mutate(state_abbr_upper=toupper(state), state_name_upper=stringr::str_to_upper(state_name))
    dbg(STATE_FIPS_LOOKUP,"STATE_FIPS_LOOKUP")
    
    county_code_to_geoid <- function(code, lkp) {
      geoids <- rep(NA_character_, length(code))
      m <- stringr::str_match(code, "^us-([A-Za-z]{2})-(\\d{3})$")
      idx <- which(!is.na(m[,1]))
      if (!length(idx)) return(geoids)
      st  <- toupper(m[idx,2]); cty <- m[idx,3]
      stf <- lkp$state_code[match(st, lkp$state_abbr_upper)]
      v <- which(!is.na(stf))
      geoids[idx[v]] <- paste0(stf[v], cty[v])
      geoids
    }
    
    extract_bits_swbinv3 <- function(ttl) list(
      innov=stringr::str_to_title(stringr::str_extract(ttl, "(?<=\\bin\\s)(.+?)(?=,\\s*by)")),
      years=stringr::str_extract(ttl, "\\d{4}(–|\\s*to\\s*)\\d{2,4}$")
    )
    extract_bits_swbinv1 <- function(ttl) {
      cat1 <- stringr::str_match(ttl, "inventors in ([^,:]+)")[,2]
      if (is.na(cat1)) {
        alt <- stringr::str_match(ttl, "inventors ([^,:]+)")[,2]
        cat1 <- if(!is.na(alt) && nzchar(trimws(alt))) alt else NA_character_
      }
      list(innov = ifelse(is.na(cat1) | stringr::str_trim(cat1)=="", "All Technologies", stringr::str_to_title(cat1)),
           years = stringr::str_extract(ttl, "\\d{4}(–|\\s*to\\s*)\\d{2,4}$"))
    }
    pad_years_df_list <- function(lst, yrs) purrr::map(lst, function(df) {
      if (is.null(df) || nrow(df)==0) return(df)
      miss <- setdiff(yrs, names(df)); for (mc in miss) df[[mc]] <- NA_real_
      id <- setdiff(names(df), yrs)
      df %>% dplyr::select(dplyr::any_of(id), dplyr::all_of(yrs))
    })
    
    # --- swbinv-3 (county + state) ---
    COUNTY_MASTER_3 <- tibble::tibble(); STATE_MASTER_3 <- tibble::tibble()
    if (nrow(INDEX_3)>0) {
      is_cnty  <- stringr::str_detect(INDEX_3$`Table Title`, "U\\.S\\.\\s+county")
      is_state <- stringr::str_detect(INDEX_3$`Table Title`, "\\bby\\s+state\\b")
      S3_CNTY  <- INDEX_3$`Table Number`[which(is_cnty)]
      S3_STATE <- INDEX_3$`Table Number`[which(is_state)]
      
      process_sheet_swbinv3 <- function(sh, level, idx3, lkp, fp3) {
        ttl <- idx3 %>% dplyr::filter(`Table Number`==sh) %>% dplyr::pull(`Table Title`)
        if (!length(ttl)) return(tibble::tibble())
        bits <- extract_bits_swbinv3(ttl)
        df <- tryCatch(readxl::read_excel(fp3, sheet=sh, skip=3), error=function(e) tibble::tibble())
        if (nrow(df)==0) return(tibble::tibble())
        ycols <- names(df)[stringr::str_detect(names(df), "^\\d{4}$")]
        for (c in ycols) if (is.character(df[[c]])) suppressWarnings(df[[c]] <- as.numeric(df[[c]]))
        ycols <- names(df)[stringr::str_detect(names(df), "^\\d{4}$") & vapply(df, is.numeric, logical(1))]
        if (!length(ycols)) return(tibble::tibble())
        df <- df %>%
          dplyr::mutate(dplyr::across(dplyr::all_of(ycols), as.numeric),
                        `All-Years Total`=rowSums(dplyr::across(dplyr::all_of(ycols)),na.rm=TRUE),
                        Metric="USPTO Utility Patents Granted",
                        `Innovation Category`=bits$innov, `Years Available`=bits$years, .before=1)
        if (level=="county" && "County code" %in% names(df))
          df %>% dplyr::mutate(`County GEOID`=county_code_to_geoid(`County code`, lkp)) %>%
          dplyr::relocate(`County GEOID`, .after=`All-Years Total`) %>%
          dplyr::select(-`County code`)
        else if (level=="state" && !"State" %in% names(df) && length(names(df))>0) {
          names(df)[1] <- "State"; df
        } else df
      }
      
      mapper <- if (parallel) furrr::future_map else purrr::map
      cnty_list_3 <- mapper(S3_CNTY, process_sheet_swbinv3, level="county", idx3=INDEX_3, lkp=STATE_FIPS_LOOKUP, fp3=SWBINV_3_FILE)
      state_list_3 <- mapper(S3_STATE, process_sheet_swbinv3, level="state", idx3=INDEX_3, lkp=STATE_FIPS_LOOKUP, fp3=SWBINV_3_FILE)
      cnty_list_3 <- Filter(function(x) !is.null(x) && nrow(x)>0, cnty_list_3)
      state_list_3 <- Filter(function(x) !is.null(x) && nrow(x)>0, state_list_3)
      yrs3 <- unique(c(unlist(purrr::map(cnty_list_3, names)), unlist(purrr::map(state_list_3, names)))) %>%
        purrr::keep(~ stringr::str_detect(.x,"^\\d{4}$")) %>% sort()
      if (length(yrs3)>0) { cnty_list_3 <- pad_years_df_list(cnty_list_3, yrs3); state_list_3 <- pad_years_df_list(state_list_3, yrs3) }
      COUNTY_MASTER_3 <- if(length(cnty_list_3)>0) dplyr::bind_rows(cnty_list_3) else tibble::tibble()
      STATE_MASTER_3  <- if(length(state_list_3)>0) dplyr::bind_rows(state_list_3) else tibble::tibble()
      if (nrow(COUNTY_MASTER_3)>0 && length(yrs3)>0) COUNTY_MASTER_3 <- COUNTY_MASTER_3 %>%
        dplyr::select(dplyr::any_of(c("Metric","Innovation Category","Years Available","All-Years Total","County GEOID","County","State")), dplyr::all_of(yrs3))
      if (nrow(STATE_MASTER_3)>0  && length(yrs3)>0) STATE_MASTER_3  <- STATE_MASTER_3 %>%
        dplyr::select(dplyr::any_of(c("Metric","Innovation Category","Years Available","All-Years Total","State")), dplyr::all_of(yrs3))
    }
    
    # --- swbinv-1 (county only) ---
    LOOKUP_1 <- if (nrow(INDEX_1)>0 && "Table SWBINV1-1" %in% INDEX_1$`Table Name` && file.exists(SWBINV_1_FILE))
      tryCatch(readxl::read_excel(SWBINV_1_FILE, "Table SWBINV1-1", skip=3) %>% dplyr::select(`County code`, County, State),
               error=function(e) tibble::tibble()) else tibble::tibble()
    
    load_enrich_swbinv1 <- function(sh, idx1, lkp1, lkp_fips, fp1) {
      ttl <- idx1$`Table Title`[match(sh, idx1$`Table Name`)]; if (!length(ttl)) return(tibble::tibble())
      bits <- extract_bits_swbinv1(ttl)
      df <- tryCatch(readxl::read_excel(fp1, sh, skip=3), error=function(e) tibble::tibble()); if (nrow(df)==0) return(tibble::tibble())
      fc <- names(df)[1]
      if (fc!="County code" && !"County code" %in% names(df)) df <- dplyr::rename(df, `County code`=dplyr::all_of(fc)) else if (!"County code" %in% names(df)) return(tibble::tibble())
      df <- df %>% dplyr::left_join(lkp1, by="County code") %>%
        dplyr::relocate(dplyr::any_of(c("County","State")), .after=`County code`) %>%
        dplyr::mutate(`County GEOID`=county_code_to_geoid(`County code`, lkp_fips),
                      Metric="USPTO Utility Patents Granted", `Innovation Category`=bits$innov, `Years Available`=bits$years,.before=1)
      yrs <- names(df) %>% purrr::keep(~ stringr::str_detect(.x,"^\\d{4}$"))
      valid <- character(0)
      for (y in yrs) { if (is.character(df[[y]])) suppressWarnings(df[[y]] <- as.numeric(df[[y]])); if (is.numeric(df[[y]])) valid <- c(valid,y) }
      if (!length(valid)) return(df)
      df %>% dplyr::mutate(dplyr::across(dplyr::all_of(valid), as.numeric),
                           `All-Years Total`=rowSums(dplyr::across(dplyr::all_of(valid)),na.rm=TRUE), .after=`Years Available`)
    }
    
    COUNTY_MASTER_1 <- if (nrow(INDEX_1)>0 && nrow(LOOKUP_1)>0 && file.exists(SWBINV_1_FILE)) {
      sheets <- setdiff(INDEX_1$`Table Name`,"Table SWBINV1-1")
      mapper <- if (parallel) furrr::future_map else purrr::map
      lst1 <- mapper(sheets, load_enrich_swbinv1, idx1=INDEX_1, lkp1=LOOKUP_1, lkp_fips=STATE_FIPS_LOOKUP, fp1=SWBINV_1_FILE)
      lst1 <- Filter(function(x) !is.null(x) && nrow(x)>0, lst1)
      yrs1 <- unique(unlist(purrr::map(lst1,names))) %>% purrr::keep(~ stringr::str_detect(.x,"^\\d{4}$")) %>% sort()
      if (length(yrs1)>0) lst1 <- pad_years_df_list(lst1, yrs1)
      if (length(lst1)>0) dplyr::bind_rows(lst1) else tibble::tibble()
    } else tibble::tibble()
    
    yrs_final <- union(names(COUNTY_MASTER_3), names(COUNTY_MASTER_1)) %>% purrr::keep(~ stringr::str_detect(.x,"^\\d{4}$")) %>% sort()
    if (length(yrs_final)>0) {
      if (nrow(COUNTY_MASTER_3)>0) COUNTY_MASTER_3 <- pad_years_df_list(list(COUNTY_MASTER_3), yrs_final)[[1]]
      if (nrow(COUNTY_MASTER_1)>0) COUNTY_MASTER_1 <- pad_years_df_list(list(COUNTY_MASTER_1), yrs_final)[[1]]
    }
    if (nrow(COUNTY_MASTER_3)>0) COUNTY_MASTER_3 <- COUNTY_MASTER_3 %>%
      dplyr::select(dplyr::any_of(c("Metric","Innovation Category","Years Available","All-Years Total","County GEOID","County","State")), dplyr::all_of(yrs_final))
    if (nrow(COUNTY_MASTER_1)>0) COUNTY_MASTER_1 <- COUNTY_MASTER_1 %>%
      dplyr::select(dplyr::any_of(c("Metric","Innovation Category","Years Available","All-Years Total","County GEOID","County","State")), dplyr::all_of(yrs_final))
    
    NSF_PATENT_DATA_COUNTY <- if (nrow(COUNTY_MASTER_3)>0 && nrow(COUNTY_MASTER_1)>0) dplyr::bind_rows(COUNTY_MASTER_3, COUNTY_MASTER_1)
    else if (nrow(COUNTY_MASTER_3)>0) COUNTY_MASTER_3 else COUNTY_MASTER_1
    NSF_PATENT_DATA_STATE  <- STATE_MASTER_3
  }
  
  .msg("INFO","NSF patent loads complete.")
  list(county = NSF_PATENT_DATA_COUNTY, state = NSF_PATENT_DATA_STATE)
}

# =============================================================================
# SECTION 2: BASE GEOGRAPHIC LAYERS
# =============================================================================

.msg("INFO", "=== LOADING BASE GEOGRAPHIC LAYERS ===")

# ---- 2.1 States -------------------------------------------------------------
STATE_GEODATA <- tigris::states(year = 2023, cb = TRUE) %>%
  sf::st_transform(CRS_USE) %>%
  janitor::clean_names() %>%
  dplyr::rename_with(~ toupper(.x), everything()) %>%
  dplyr::mutate(
    GEO_ID = as.character(GEOID),
    is_territory = STATEFP %in% EXCLUDE_TERRITORIES
  ) %>%
  dplyr::filter(!is_territory) %>%
  dplyr::select(-is_territory, -GEOID)
g(STATE_GEODATA, "STATE_GEODATA")

# ---- 2.2 Counties (multiple years) -----------------------------------------
YEARS <- c(2018, 2020, 2023)
for (y in YEARS) {
  obj_name <- paste0("COUNTY_", y)
  tmp <- tigris::counties(year = y, cb = TRUE) %>%
    sf::st_transform(CRS_USE) %>%
    sf::st_make_valid() %>%
    janitor::clean_names() %>%
    dplyr::rename_with(~ toupper(.x), everything()) %>%
    dplyr::mutate(
      GEO_ID = as.character(GEOID),
      is_territory = STATEFP %in% EXCLUDE_TERRITORIES
    ) %>%
    dplyr::filter(!is_territory) %>%
    dplyr::select(-is_territory)
  assign(obj_name, tmp)
  g(get(obj_name), obj_name)
}
COUNTY_GEODATA <- COUNTY_2023  # Alias to latest

# ---- 2.3 CBSAs and CSAs -----------------------------------------------------
CBSA_GEODATA <- tigris::core_based_statistical_areas(year = 2023, cb = TRUE) %>%
  sf::st_transform(CRS_USE) %>%
  sf::st_make_valid() %>%
  janitor::clean_names() %>%
  dplyr::rename_with(~ toupper(.x), everything()) %>%
  dplyr::mutate(
    GEO_ID = as.character(GEOID),
    is_territory = stringr::str_sub(GEOID, 1, 2) %in% EXCLUDE_TERRITORIES
  ) %>%
  dplyr::filter(!is_territory) %>%
  dplyr::select(-is_territory)
g(CBSA_GEODATA, "CBSA_GEODATA")

CSA_GEODATA <- tryCatch({
  tigris::combined_statistical_areas(year = 2023, cb = TRUE) %>%
    sf::st_transform(CRS_USE) %>%
    sf::st_make_valid() %>%
    janitor::clean_names() %>%
    dplyr::rename_with(~ toupper(.x), everything()) %>%
    dplyr::mutate(
      GEO_ID = as.character(GEOID),
      is_territory = stringr::str_sub(GEOID, 1, 2) %in% EXCLUDE_TERRITORIES
    ) %>%
    dplyr::filter(!is_territory) %>%
    dplyr::select(-is_territory)
}, error = function(e) {
  .warn(paste("CSA load failed:", e$message))
  NULL
})
if (!is.null(CSA_GEODATA)) g(CSA_GEODATA, "CSA_GEODATA")

# ---- 2.4 Geographic Crosswalks ----------------------------------------------
.msg("INFO", "Building geographic crosswalks...")

# County to CBSA
COUNTY_TO_CBSA_2023 <- tryCatch({
  county_pts <- sf::st_point_on_surface(COUNTY_2023)
  join_cbsa <- sf::st_join(county_pts, CBSA_GEODATA[, c("CBSAFP", "NAME", "LSAD")], 
                           left = TRUE, join = sf::st_within)
  tibble::tibble(
    COUNTY_GEOID = county_pts$GEOID,
    COUNTY_NAME = county_pts$NAME,
    STATEFP = county_pts$STATEFP,
    CBSA_CODE = join_cbsa$CBSAFP,
    CBSA_NAME = join_cbsa$NAME,
    CBSA_LSAD = join_cbsa$LSAD,
    NON_MSA = is.na(join_cbsa$CBSAFP)
  )
}, error = function(e) {
  .warn(paste("County-CBSA crosswalk failed:", e$message))
  tibble::tibble()
})
g(COUNTY_TO_CBSA_2023, "COUNTY_TO_CBSA_2023")

# County to CSA
COUNTY_TO_CSA_2023 <- if (!is.null(CSA_GEODATA)) {
  tryCatch({
    county_pts <- sf::st_point_on_surface(COUNTY_2023)
    join_csa <- sf::st_join(county_pts, CSA_GEODATA[, c("CSAFP", "NAME")], 
                            left = TRUE, join = sf::st_within)
    tibble::tibble(
      COUNTY_GEOID = county_pts$GEOID,
      COUNTY_NAME = county_pts$NAME,
      STATEFP = county_pts$STATEFP,
      CSA_CODE = join_csa$CSAFP,
      CSA_NAME = join_csa$NAME
    )
  }, error = function(e) {
    .warn(paste("County-CSA crosswalk failed:", e$message))
    tibble::tibble()
  })
} else tibble::tibble()
if (nrow(COUNTY_TO_CSA_2023) > 0) g(COUNTY_TO_CSA_2023, "COUNTY_TO_CSA_2023")

# =============================================================================
# SECTION 3: COUNTY-LEVEL DATA
# =============================================================================

.msg("INFO", "=== LOADING COUNTY-LEVEL DATA ===")

# ---- 3.1 Headwaters Rural Capacity Index -----------------------------------
#More here: https://headwaterseconomics.org/economic-development/equity/rural-capacity-map/
#Even more here: https://services3.arcgis.com/C0Zv6HSfq6RdocaN/arcgis/rest/services/Rural_Capacity_Index_Live/FeatureServer/layers

hdw <- fp(RAW_DATA_FOLDER, "HE_Rural_Capacity_Index_Feb_2025_Download_Data.xlsx")
HEADWATERS_COUNTY_DATA <- if (file.exists(hdw)) {
  readxl::read_excel(hdw, sheet = "County") %>%
    dplyr::mutate(FIPS = stringr::str_pad(as.character(FIPS), 5, pad = "0")) %>%
    dplyr::rename(COUNTY_GEOID = FIPS) %>%
    dplyr::select(-any_of("GEOIDFQ"))
} else {
  .warn(paste("Missing:", hdw))
  tibble::tibble()
}
g(HEADWATERS_COUNTY_DATA, "HEADWATERS_COUNTY_DATA")

# ---- 3.2 Economic Development Capacity Index -------------------------------
#More here: https://disgeoportal.egs.anl.gov/EDCI/ 
edci_file <- fp(RAW_DATA_FOLDER, "Economic_Development_Capacity_Index_Snapshot_August_2024.xlsx")
EDCI_COUNTY_INDICATORS_2024 <- if (file.exists(edci_file)) {
  rename_indicator_columns <- function(df, dict, suff_pat, suff_lbl) {
    ignore_cols <- c("county_fips", "county", "state_fips", "state", "area_land", 
                     "area_water", "latitude", "longitude", "total_population", 
                     "total_households")
    old <- names(df)
    new <- vapply(old, function(col) {
      if (col %in% ignore_cols || !stringr::str_ends(col, suff_pat)) {
        col
      } else {
        ind <- stringr::str_remove(col, paste0(suff_pat, "$"))
        mr <- dict %>% dplyr::filter(indicator == ind)
        if (nrow(mr) == 1 && !is.na(mr$definition[1])) {
          paste0(mr$definition[1], suff_lbl)
        } else {
          col
        }
      }
    }, character(1), USE.NAMES = FALSE)
    stats::setNames(df, make.unique(new, sep = " "))
  }
  
  dict <- readxl::read_excel(edci_file, sheet = "data_dictionary") %>%
    dplyr::select(indicator, definition) %>%
    dplyr::distinct() %>%
    dplyr::arrange(indicator)
  
  edci_ind <- readxl::read_excel(edci_file, sheet = "indicators") %>%
    rename_indicator_columns(dict, "_unit_value", " Value")
  
  edci_ind %>% 
    dplyr::select(-any_of(c("area_land", "area_water", "latitude", "longitude")))
} else {
  .warn(paste("Missing:", edci_file))
  tibble::tibble()
}
g(EDCI_COUNTY_INDICATORS_2024, "EDCI_COUNTY_INDICATORS_2024")

# ---- 3.3 EIG Distressed Communities Index ----------------------------------
#More here: https://eig.org/dci-hub/
#NOTE: Not all counties covered.
EIG_DISTRESSED_COMMUNITIES_COUNTY <- tryCatch({
  readr::read_csv(fp(RAW_DATA_FOLDER, "EIG Distressed Communities Index 2025", 
                     "EIG_Distressed_Communities_Index_2025_COUNTY.csv"),
                  show_col_types = FALSE)
}, error = function(e) {
  .warn(paste("EIG County:", e$message))
  tibble::tibble()
})
g(EIG_DISTRESSED_COMMUNITIES_COUNTY, "EIG_DISTRESSED_COMMUNITIES_COUNTY")

# ---- 3.4 Employment Carbon Footprint ---------------------------------------
#More here: https://www.pnas.org/doi/10.1073/pnas.2314773121
ECF_DATAFOLDER <- fp(RAW_DATA_FOLDER, "GrahamKnittel_EmploymentCarbonFootprints_Data_2024")
ECF_COUNTY <- tryCatch({
  readr::read_csv(fp(ECF_DATAFOLDER, "ECF_total.csv"), show_col_types = FALSE)
}, error = function(e) {
  .warn(paste("ECF County:", e$message))
  tibble::tibble()
})
g(ECF_COUNTY, "ECF_COUNTY")

# ---- 3.5 Commuting Zone Import Penetration ---------------------------------
#Increased imports from China disproportionately impacted specialized manufacturing communities in the southern and midwestern US
#More here: https://www.brookings.edu/articles/place-based-industrial-strategy-responds-to-past-and-future-industrial-and-labor-market-shocks/
CZ_COMMUTING_ZONE_DATA <- tryCatch({
  cz_tmp <- tempfile(fileext = ".csv")
  on.exit(if (file.exists(cz_tmp)) unlink(cz_tmp), add = TRUE)
  safe_download("https://datawrapper.dwcdn.net/signH/1/dataset.csv", cz_tmp, show = FALSE)
  safe_fread(cz_tmp) %>%
    dplyr::select(countyfips, ctyname2000, imp) %>%
    dplyr::mutate(
      countyfips = ifelse(nchar(as.character(countyfips)) == 4, 
                          paste0("0", countyfips), 
                          as.character(countyfips))
    ) %>%
    dplyr::rename(
      `County GEOID (2020)` = countyfips,
      `County Name (2020)` = ctyname2000,
      `Commuting-Zone Import Penetration Change (2000-2012)` = imp
    )
}, error = function(e) {
  .warn(paste("CZ data:", e$message))
  tibble::tibble()
})
g(CZ_COMMUTING_ZONE_DATA, "CZ_COMMUTING_ZONE_DATA")

# ---- 3.7 County Property Values --------------------------------------------
COUNTY_PROPERTY_VALUES <- safe_fread(fp(RAW_DATA_FOLDER, "county_property_values.csv"))
g(COUNTY_PROPERTY_VALUES, "COUNTY_PROPERTY_VALUES")
 
# ---- 3.8 Median Property Taxes Paid by County, 2023--- 
#More here: https://taxfoundation.org/data/all/state/property-taxes-by-state-county/ 
COUNTY_PROPERTY_TAXES_PAID_2023 <- safe_fread(fp(RAW_DATA_FOLDER, "Tax_Foundation_Property_Taxes_Paid_2023", "county_property_taxes_paid_2023_2022.csv"))
g(COUNTY_PROPERTY_TAXES_PAID_2023, "COUNTY_PROPERTY_TAXES_PAID_2023")

#--3.9: NSF Patent Data, County 

nsf_res <- load_nsf_patent_data(RAW_DATA_FOLDER, parallel = TRUE)
NSF_PATENT_DATA_COUNTY <- nsf_res$county
g(NSF_PATENT_DATA_COUNTY, "NSF_PATENT_DATA_COUNTY")

# ---- 3.10 County Business Patterns Data (FIXED) ---------------------------------------------
#More here: https://api.census.gov/data/2023/cbp.html 
library(censusapi)
library(dplyr)
library(stringr)
library(tibble)
library(janitor)
library(httr)
library(jsonlite)

.msg <- function(type, message) cat(paste0("[", type, "] ", message, "\n"))
.warn <- function(message) warning(message)

.msg("INFO", "Loading Census county-level CBP (2023)…")

cbp_2023 <- tryCatch(
  {
    # NOTE: When you use region = "county:*", the API automatically returns 'state' and 'county'.
    # We therefore don't need the dataset variable "STATE".
    censusapi::getCensus(
      name   = "cbp",
      vintage = 2023,
      vars   = c("NAICS2017", "PAYANN", "EMP"),
      region = "county:*"
    ) |>
      as_tibble() |>
      # robust GEOID and tidy col order
      mutate(
        county_geoid = paste0(
          str_pad(state,  2, pad = "0"),
          str_pad(county, 3, pad = "0")
        )
      ) |>
      relocate(county_geoid, state, county, NAICS2017, EMP, PAYANN)
  },
  error = function(e) {
    .warn(paste("CBP 2023:", e$message)); tibble()
  }
)

# --- Metadata: add NAICS (2017) code descriptions, and keep variable labels for docs
if (nrow(cbp_2023) > 0) {
  # FIXED: Get NAICS code descriptions directly from API
  naics_lu <- tryCatch({
    # Get NAICS codes and descriptions from the API
    naics_response <- httr::GET("https://api.census.gov/data/2023/cbp/variables/NAICS2017.json")
    naics_data <- httr::content(naics_response, "text", encoding = "UTF-8") |>
      jsonlite::fromJSON()
    
    # Extract the values from the nested structure
    naics_values <- naics_data$values$item
    
    # Convert to tibble
    tibble(
      NAICS2017 = names(naics_values),
      naics2017_title = as.character(naics_values)
    )
  }, error = function(e) {
    .warn(paste("Failed to get NAICS lookup:", e$message))
    # Fallback: try censusapi method
    tryCatch({
      censusapi::listValues("cbp", vintage = 2023, var = "NAICS2017") |>
        as_tibble() |>
        transmute(
          NAICS2017 = value,
          naics2017_title = label
        )
    }, error = function(e2) {
      .warn(paste("Fallback NAICS lookup also failed:", e2$message))
      tibble(NAICS2017 = character(0), naics2017_title = character(0))
    })
  })
  
  # Dataset variable labels (e.g., EMP, PAYANN)
  cbp_var_lu <- tryCatch({
    censusapi::listVariables("cbp", vintage = 2023) |>
      as_tibble() |>
      filter(name %in% c("EMP", "PAYANN")) |>
      transmute(variable = name, label)
  }, error = function(e) {
    .warn(paste("Failed to get CBP variable labels:", e$message))
    tibble(
      variable = c("EMP", "PAYANN"),
      label = c("Total employment", "Total annual payroll ($1,000)")
    )
  })
  
  # Join NAICS descriptions if we got them
  if (nrow(naics_lu) > 0) {
    cbp_2023 <- cbp_2023 |>
      left_join(naics_lu, by = "NAICS2017") |>
      mutate(naics_level = nchar(NAICS2017)) |>
      relocate(naics2017_title, .after = NAICS2017)
  } else {
    cbp_2023 <- cbp_2023 |>
      mutate(
        naics2017_title = NA_character_,
        naics_level = nchar(NAICS2017)
      ) |>
      relocate(naics2017_title, .after = NAICS2017)
  }
  
  # Attach human-readable labels to the numeric fields (handy when inspecting)
  if (nrow(cbp_var_lu) > 0) {
    attr(cbp_2023$EMP,    "label") <- cbp_var_lu$label[match("EMP", cbp_var_lu$variable)]
    attr(cbp_2023$PAYANN, "label") <- cbp_var_lu$label[match("PAYANN", cbp_var_lu$variable)]
    
    # (Optional) reference table you can print or write out with results
    cbp_var_lookup <- cbp_var_lu
  }
}

glimpse(cbp_2023)

# ---- ACS Data (FIXED: concepts as column names) --------------------------------------
library(censusapi)
library(dplyr)
library(stringr)
library(janitor)
library(tibble)
library(purrr)
library(httr)
library(jsonlite)

acs_vars <- c(
  "B19013_001E",  # Median household income (infl.-adj.)
  "B17020_001E",  # Poverty status by age: Total
  "B99172_001E",  # Allocation of language status: Total
  "C18120_003E",  # Employment status: Employed
  "C18120_002E",  # Employment status: In the labor force
  "B23025_001E",  # Employment status for the population 16 years and over: Total
  "B25004_001E",  # Vacancy status: Total
  "B01003_001E"   # Total population
)

# 1) Data --------------------------------------------------------------------
acs_5yr_23 <- tryCatch(
  censusapi::getCensus(
    name    = "acs/acs5",
    vintage = 2023,
    vars    = acs_vars,
    region  = "county:*"
  ) |> 
    as_tibble(),
  error = function(e) { warning("ACS 2023: ", e$message); tibble() }
)

# 2) Metadata from API (your working approach) --------------------------------
#    - Use concept as the preferred column title
#    - If two+ variables share the same concept, append the leaf of the label
#      to keep names unique: e.g., "… - Employed", "… - In the labor force"

url <- "https://api.census.gov/data/2023/acs/acs5/variables.json"

acs_dict <- tryCatch({
  # Load JSON and keep only the variables we need
  census_vars <- jsonlite::fromJSON(url)
  vars_tbl <- census_vars$variables %>%
    purrr::keep(names(.) %in% acs_vars) %>%
    bind_rows(.id = "api_var") %>%
    as_tibble()
  
  # Warn if any requested vars not found
  missing_vars <- setdiff(acs_vars, vars_tbl$api_var)
  if (length(missing_vars)) {
    warning("No label metadata found for: ", paste(missing_vars, collapse = ", "))
  }
  
  # Clean label & derive a 'leaf' label for disambiguation
  vars_tbl <- vars_tbl %>%
    mutate(
      table_id       = sub("_.*$", "", api_var),
      full_label_raw = label,
      full_label     = stringr::str_remove(full_label_raw, "^Estimate!!"),
      leaf_label     = purrr::map_chr(
        stringr::str_split(full_label, "!!"),
        ~{ if (length(.x) == 0) "" else .x[length(.x)] }
      ) |> stringr::str_replace(":+$", ""),
      proposed_name  = concept
    )
  
  # Default: concept-only; disambiguate duplicates with the label leaf
  vars_tbl <- vars_tbl %>%
    group_by(proposed_name) %>%
    mutate(
      final_colname = if (n() == 1) {
        proposed_name
      } else {
        paste0(proposed_name, " - ", leaf_label)
      }
    ) %>%
    ungroup()
  
  # If you prefer strictly concept-only with numeric suffixes, comment out the
  # block above and use the two lines below instead:
  # vars_tbl <- vars_tbl %>%
  #   mutate(final_colname = make.unique(proposed_name, sep = " "))
  
  # Return dictionary with mapping
  vars_tbl %>%
    transmute(
      api_var,
      table_id,
      concept,
      full_label_hierarchy = full_label,
      final_colname
    )
}, error = function(e) {
  warning("Failed to fetch/parse ACS metadata: ", e$message)
  # Fallback: keep codes as names if metadata fails
  tibble(
    api_var = acs_vars,
    table_id = sub("_.*$", "", acs_vars),
    concept = paste("Variable", acs_vars),
    full_label_hierarchy = acs_vars,
    final_colname = acs_vars
  )
})

# 3) GEOID + Rename columns to concepts --------------------------------------
if (nrow(acs_5yr_23) > 0) {
  acs_5yr_23_renamed <- acs_5yr_23 %>%
    mutate(
      geoid = paste0(stringr::str_pad(state, 2, pad = "0"),
                     stringr::str_pad(county, 3, pad = "0")),
      .after = county
    )
  
  if (nrow(acs_dict) > 0) {
    # Build mapping: old code -> final concept-based name
    rename_map <- setNames(acs_dict$final_colname, acs_dict$api_var)
    
    cols_to_rename <- intersect(names(acs_5yr_23_renamed), names(rename_map))
    if (length(cols_to_rename) > 0) {
      acs_5yr_23_renamed <- acs_5yr_23_renamed %>%
        rename_with(~ rename_map[.x], .cols = all_of(cols_to_rename))
    }
  }
} else {
  acs_5yr_23_renamed <- tibble()
}

# 4) Lookup table for documentation/auditing ---------------------------------
acs_var_lookup <- acs_dict %>%
  transmute(
    variable_code        = api_var,
    table_id,
    concept,                           # Human-readable table concept
    full_label_hierarchy,              # The full label hierarchy (no "Estimate!!")
    final_colname                      # The actual name used in your data frame
  )

# Peek -----------------------------------------------------------------------
glimpse(acs_5yr_23_renamed)

# ---- 3.11 BEA County GDP ---------------------------------------------------
county_gdp <- safe_fread(fp(RAW_DATA_FOLDER, "county_gdp_2022.csv"), skip = 3)
g(county_gdp, "county_gdp")

get_bea_zip_csv <- function(zip_url, inner_pattern) {
  z <- tempfile(fileext = ".zip")
  on.exit(unlink(z), add = TRUE)
  safe_download(zip_url, z, show = FALSE)
  files <- utils::unzip(z, list = TRUE)$Name
  tgt <- files[grepl(inner_pattern, files, ignore.case = TRUE)]
  if (!length(tgt)) stop("File not found in zip")
  utils::read.csv(unz(z, tgt[1]), stringsAsFactors = FALSE) %>%
    tibble::as_tibble()
}

county_gdp_ind <- tryCatch({
  get_bea_zip_csv("https://apps.bea.gov/regional/zip/CAGDP11.zip", 
                  "CAGDP11__ALL_AREAS_2002_2023.csv")
}, error = function(e) {
  .warn(paste("County GDP by industry:", e$message))
  tibble::tibble()
})
g(county_gdp_ind, "county_gdp_ind")

county_gdp_ind2 <- tryCatch({
  get_bea_zip_csv("https://apps.bea.gov/regional/zip/CAGDP2.zip", 
                  "CAGDP2__ALL_AREAS_2001_2023.csv")
}, error = function(e) {
  .warn(paste("County GDP 2:", e$message))
  tibble::tibble()
})
g(county_gdp_ind2, "county_gdp_ind2")

# ---- 3.12 Renewable LCOE (County) ------------------------------------------
solar_county <- safe_fread(fp(RAW_DATA_FOLDER, "solar_lcoe_county.csv"))
g(solar_county, "solar_county")

wind_county <- safe_fread(fp(RAW_DATA_FOLDER, "wind_lcoe_county.csv"))
g(wind_county, "wind_county")

#Add in county_geoid to wind_county; county_geoid is 5-digit fips code
wind_county <- wind_county %>%

geothermal_county <- safe_fread(fp(RAW_DATA_FOLDER, "geothermal_county.csv"))
g(geothermal_county, "geothermal_county")


glimpse(solar_county)
glimpse(wind_county)
glimpse(geothermal_county)

# ---- 3.13 FEMA National Risk Index (County) --------------------------------
#MORE HERE: https://hazards.fema.gov/nri/map 
fema_base <- fp(RAW_DATA_FOLDER, "FEMA_NationalRiskIndex_April2025Download")
NATIONAL_RISK_INDEX_2023_RAW <- safe_fread(fp(fema_base, "NRI_Table_Counties.csv"))
NATIONAL_RISK_INDEX_DATA_DICTIONARY <- safe_fread(fp(fema_base, "NRIDataDictionary.csv"), quote = "")

NATIONAL_RISK_INDEX_2023 <- if (nrow(NATIONAL_RISK_INDEX_2023_RAW) > 0 && 
                                nrow(NATIONAL_RISK_INDEX_DATA_DICTIONARY) > 0) {
  nmap <- NATIONAL_RISK_INDEX_DATA_DICTIONARY %>%
    dplyr::filter(!is.na(`Field Alias`) & nzchar(trimws(`Field Alias`))) %>%
    dplyr::distinct(`Field Name`, .keep_all = TRUE) %>%
    dplyr::pull(`Field Alias`, name = `Field Name`)
  
  nm <- names(NATIONAL_RISK_INDEX_2023_RAW)
  for (i in seq_along(nm)) {
    if (nm[i] %in% names(nmap)) nm[i] <- nmap[[nm[i]]]
  }
  NATIONAL_RISK_INDEX_2023_RAW %>% `names<-`(make.unique(nm, sep = "_"))
} else {
  NATIONAL_RISK_INDEX_2023_RAW
}
g(NATIONAL_RISK_INDEX_2023, "NATIONAL_RISK_INDEX_2023")

# ---- 3.14 CBS 2024 Election (County) ---------------------------------------
CBS_2024_ELECTION_DATA <- tryCatch({
  safe_fread("https://raw.githubusercontent.com/cbs-news-data/election-2024-maps/refs/heads/master/output/all_counties_clean_2024.csv")
}, error = function(e) {
  .warn(paste("CBS election data:", e$message))
  tibble::tibble()
})
g(CBS_2024_ELECTION_DATA, "CBS_2024_ELECTION_DATA")

# ---- 3.15 Life Expectancy (County) -----------------------------------------
life_19 <- safe_fread(fp(DATA_FOLDER, "US Maps etc", "Econ", "Life Expectancy", 
                         "IHME_county_life_expectancy", 
                         "IHME_USA_LE_COUNTY_RACE_ETHN_2000_2019_LT_2019_BOTH_Y2022M06D16.csv"))
g(life_19, "life_19 (IHME Life Expectancy)")

# ---- 3.16 American Communities Typology (County) ---------------------------
file_url <- 'https://www.americancommunities.org/wp-content/uploads/2023/08/2023-Typology-1.xlsx'
tmp_typ <- tempfile(fileext = ".xlsx")
try(safe_download(file_url, tmp_typ, show = FALSE))
us_communities <- if (file.exists(tmp_typ)) {
  readxl::read_excel(tmp_typ, 1)
} else {
  tibble::tibble()
}
g(us_communities, "us_communities")

# ---- 3.17 FCC PEA (County) -------------------------------------------------
pea_county <- tryCatch({
  readxl::read_excel(fp(RAW_DATA_FOLDER, "FCC_PEA_website.xlsx"), 3)
}, error = function(e) {
  .warn(paste("PEA county:", e$message))
  tibble::tibble()
})
g(pea_county, "pea_county")

# =============================================================================
# SECTION 4: STATE-LEVEL DATA
# =============================================================================

.msg("INFO", "=== LOADING STATE-LEVEL DATA ===")

# ---- 4.1 NREL Technical Potential (State) ----------------------------------
nrel_state_file <- fp(RAW_DATA_FOLDER, "NREL SLOPE", "techpot_baseline", "techpot_baseline_state.csv")
NREL_TECHPOT_STATE_RAW <- safe_fread(nrel_state_file)

NREL_TECHNICAL_POTENTIAL_STATE_WIDE <- if (nrow(NREL_TECHPOT_STATE_RAW) > 0) {
  tmp <- NREL_TECHPOT_STATE_RAW %>%
    dplyr::select(-any_of("Geography ID")) %>%
    tidyr::pivot_wider(
      id_cols = c(`State Name`),
      names_from = Technology,
      values_from = `Technical Generation Potential - MWh MWh`,
      names_prefix = "Technical Generation Potential - MWh: "
    )
  
  missing_states <- setdiff(c(datasets::state.name, "District of Columbia"), tmp$`State Name`)
  if (length(missing_states) > 0) {
    dplyr::bind_rows(tmp, tibble::tibble(`State Name` = missing_states))
  } else {
    tmp
  }
} else {
  tibble::tibble(`State Name` = character())
}
g(NREL_TECHNICAL_POTENTIAL_STATE_WIDE, "NREL_TECHNICAL_POTENTIAL_STATE_WIDE")

# ---- 4.2 EIG Index of State Dynamism ---------------------------------------
#MORE HERE: https://eig.org/state-dynamism/ 
ISD_State_Data <- tryCatch({
  url <- "https://eig.org/state-dynamism-2025/assets/isd_data.csv"
  isd_data <- readr::read_csv(url, show_col_types = FALSE)
  isd_data %>% dplyr::filter(state != "Average")
}, error = function(e) {
  .warn(paste("ISD State data:", e$message))
  tibble::tibble()
})
g(ISD_State_Data, "ISD_State_Data")

ISD_Average <- tryCatch({
  url <- "https://eig.org/state-dynamism-2025/assets/isd_data.csv"
  isd_data <- readr::read_csv(url, show_col_types = FALSE)
  isd_data %>% dplyr::filter(state == "Average")
}, error = function(e) {
  tibble::tibble()
})
g(ISD_Average, "ISD_Average")

# ---- 4.3 ISLR Community Power Scorecard ------------------------------------
#MORE HERE: https://ilsr.org/articles/2025-community-power-scorecard/ 
islr_path <- fp(RAW_DATA_FOLDER, "Institute for Local Self-Reliance 2025 Community Power Scorecard",
                "ILSR_2025_Community_Power_Scorecard_Rankings.csv")
ISLR_DATA <- if (file.exists(islr_path)) {
  df <- readr::read_tsv(islr_path, col_types = cols(.default = col_character()), 
                        show_col_types = FALSE) %>%
    janitor::clean_names()
  
  score_col <- intersect(c("score_percent", "score_pct", "score"), names(df))[1]
  
  df %>%
    dplyr::mutate(
      community_power_score = readr::parse_number(community_power_score),
      score_pct = readr::parse_number(.data[[score_col]]),
      score_pct = if_else(score_pct > 1, score_pct / 100, score_pct)
    )
} else {
  .warn(paste("Missing:", islr_path))
  tibble::tibble()
}
g(ISLR_DATA, "ISLR_DATA")

# ---- 4.4 StatsAmerica Innovation (State) -----------------------------------
#MORE HERE: https://www.statsamerica.org/innovation2/ 
STATSAMERICA_INNOVATION_STATES <- safe_fread(
  fp(RAW_DATA_FOLDER, "StatsAmerica_Innovation_Intelligence",
     "statsamerica_innovation_intelligence_2023_measures_states_pivoted.csv")
)
g(STATSAMERICA_INNOVATION_STATES, "STATSAMERICA_INNOVATION_STATES")

# ---- 4.5 XChange State Climate Policy --------------------------------------
clim_file <- fp(RAW_DATA_FOLDER, "State Climate Policy Dashboard - Full Download (1.28.25) 2.xlsx")
XCHANGE_STATE_CLIMATE_POLICY <- if (file.exists(clim_file)) {
  sheets <- c("Cross-Sector", "Electricity", "Buildings and Efficiency", 
              "Transportation", "Natural and Working Lands", 
              "Industry, Materials,  and Waste ")
  actual <- tryCatch(readxl::excel_sheets(clim_file), error = function(e) NULL)
  
  if (!is.null(actual)) {
    to_read <- intersect(trimws(sheets), trimws(actual))
    if (length(to_read) > 0) {
      plong <- purrr::map_dfr(to_read, ~ {
        readxl::read_excel(clim_file, sheet = .x) %>%
          dplyr::select(any_of(c("State", "Policy", "Policy Status")))
      })
      
      if (nrow(plong) > 0 && all(c("State", "Policy", "Policy Status") %in% names(plong))) {
        plong %>%
          tidyr::pivot_wider(id_cols = State, 
                             names_from = Policy, 
                             values_from = `Policy Status`) %>%
          dplyr::mutate(dplyr::across(-State, ~ factor(.x, 
                                                       levels = c("not-enacted", "enacted", 
                                                                  "in-progress", "partially-enacted"))))
      } else {
        tibble::tibble(State = character())
      }
    } else {
      tibble::tibble(State = character())
    }
  } else {
    tibble::tibble(State = character())
  }
} else {
  .warn(paste("Missing:", clim_file))
  tibble::tibble(State = character())
}
g(XCHANGE_STATE_CLIMATE_POLICY, "XCHANGE_STATE_CLIMATE_POLICY")

# ---- 4.6 CNBC Top States for Business --------------------------------------
CNBC_2024_TOP_STATES <- tryCatch({
  url_cnbc <- "https://www.cnbc.com/2024/07/11/americas-top-states-for-business-full-rankings.html"
  doc <- rvest::read_html(url_cnbc)
  tabs <- rvest::html_elements(doc, "table")
  
  if (length(tabs) == 0) stop("No tables found")
  
  df_list <- lapply(tabs, function(tb) {
    out <- tryCatch(rvest::html_table(tb, fill = TRUE), error = function(e) NULL)
    if (is.null(out) || nrow(out) == 0) return(NULL)
    
    # Ensure first column is State
    if (!any(out[[1]] %in% datasets::state.name)) {
      col_with_state <- which(vapply(out, function(col) {
        any(col %in% datasets::state.name)
      }, logical(1)))
      if (length(col_with_state)) {
        out <- out[, c(col_with_state[1], setdiff(seq_along(out), col_with_state[1]))]
      }
    }
    names(out)[1] <- "State"
    out
  })
  
  df_list <- Filter(function(x) !is.null(x) && "State" %in% names(x), df_list)
  if (!length(df_list)) stop("No valid table found")
  
  df <- df_list[[1]] %>%
    dplyr::rename_with(~ gsub("[^A-Za-z0-9_]+", "_", .x)) %>%
    dplyr::mutate(dplyr::across(everything(), trimws))
  
  rc <- setdiff(names(df), "State")
  n <- nrow(df)
  
  if (n > 0 && length(rc) > 0) {
    df %>%
      dplyr::mutate(dplyr::across(all_of(rc), 
                                  ~ suppressWarnings(as.integer(as.character(.))))) %>%
      dplyr::mutate(dplyr::across(all_of(rc), 
                                  ~ ifelse(is.na(.x), NA_integer_, 
                                           as.integer(round((n - .x + 1) / n * 100))))) %>%
      dplyr::rename_with(~ paste0("CNBC 2024 Top States for Business: ",
                                  stringr::str_to_title(stringr::str_replace_all(
                                    stringr::str_trim(.x), "_", " "), "en"), 
                                  " Score"),
                         all_of(rc))
  } else {
    tibble::tibble(State = character())
  }
}, error = function(e) {
  .warn(paste("CNBC parse:", e$message))
  tibble::tibble(State = character())
})
g(CNBC_2024_TOP_STATES, "CNBC_2024_TOP_STATES")

# ---- 4.7 NSF Patent Data (State) -------------------------------------------
NSF_PATENT_DATA_STATE <- nsf_res$state
g(NSF_PATENT_DATA_STATE, "NSF_PATENT_DATA_STATE")

# ---- 4.8 State GDP ----------------------------------------------------------
state_gdp <- safe_fread(fp(RAW_DATA_FOLDER, "state_gdp_22.csv"), skip = 3)
g(state_gdp, "state_gdp")

gdp_q <- tryCatch({
  sq_zip <- tempfile(fileext = ".zip")
  on.exit(unlink(sq_zip), add = TRUE)
  safe_download("https://apps.bea.gov/regional/zip/SQGDP.zip", sq_zip, show = FALSE)
  files <- utils::unzip(sq_zip, list = TRUE)$Name
  rx <- "^.*SQGDP1__ALL_AREAS_[0-9]{4}[-_][0-9]{4}\\.csv$"
  tgt <- files[grepl(rx, files, ignore.case = TRUE)]
  if (!length(tgt)) stop("SQGDP1 CSV not found")
  tgt <- tgt[order(tolower(tgt), decreasing = TRUE)][1]
  utils::read.csv(unz(sq_zip, tgt[1]), stringsAsFactors = FALSE) %>%
    tibble::as_tibble()
}, error = function(e) {
  .warn(paste("State quarterly GDP:", e$message))
  tibble::tibble()
})
g(gdp_q, "gdp_q (State Quarterly GDP)")

# ---- 4.9 State Emissions ----------------------------------------------------
ems_tmp <- tempfile(fileext = ".xlsx")
try(safe_download('https://www.eia.gov/environment/emissions/state/excel/table1.xlsx', 
                  ems_tmp, show = FALSE))
state_ems <- if (file.exists(ems_tmp)) {
  readxl::read_excel(ems_tmp, 1, skip = 4)
} else {
  tibble::tibble()
}
g(state_ems, "state_ems")

# ---- 4.10 State Gas Prices --------------------------------------------------
gas_tmp <- tempfile(fileext = ".xls")
try(safe_download('https://www.eia.gov/dnav/ng/xls/NG_PRI_SUM_A_EPG0_PIN_DMCF_M.xls', 
                  gas_tmp, show = FALSE))
eia_gas <- if (file.exists(gas_tmp)) {
  readxl::read_excel(gas_tmp, 2, skip = 2)
} else {
  tibble::tibble()
}
g(eia_gas, "eia_gas")

# ---- 4.11 State Electricity Sales -------------------------------------------
sales_tmp <- tempfile(fileext = ".xlsx")
try(safe_download('https://www.eia.gov/electricity/data/eia861m/xls/sales_revenue.xlsx', 
                  sales_tmp, show = FALSE))
eia_sales <- if (file.exists(sales_tmp)) {
  readxl::read_excel(sales_tmp, 1, skip = 2)
} else {
  tibble::tibble()
}
g(eia_sales, "eia_sales")

# ---- 4.12 State-level Census -----------------------------------------------
cbp_2023_nat <- tryCatch({
  censusapi::getCensus(name = "cbp", 
                       vars = c("NAICS2017", "PAYANN", "EMP"), 
                       region = "us:*", 
                       vintage = 2023) %>%
    tibble::as_tibble()
}, error = function(e) {
  .warn(paste("CBP national:", e$message))
  tibble::tibble()
})
g(cbp_2023_nat, "cbp_2023_nat")

# ---- 4.13 Regional/State Maps -----------------------------------------------
states_simple <- safe_fread(fp(DATA_FOLDER, "US Maps etc", "Regions", "rmi_regions.csv"))
g(states_simple, "states_simple")

# =============================================================================
# SECTION 5: CBSA/METRO-LEVEL DATA
# =============================================================================

.msg("INFO", "=== LOADING CBSA/METRO-LEVEL DATA ===")

# ---- 5.1 StatsAmerica Innovation (Metros) ----------------------------------
STATSAMERICA_INNOVATION_METROS_RAW <- safe_fread(
  fp(RAW_DATA_FOLDER, "StatsAmerica_Innovation_Intelligence", 
     "Innovation Intelligence - Measures - Metros.csv")
)

STATSAMERICA_INNOVATION_METROS <- if (nrow(STATSAMERICA_INNOVATION_METROS_RAW) > 0) {
  required_cols <- c("time_id", "geo_id", "description", "Code Description", "Measure Value")
  if (all(required_cols %in% names(STATSAMERICA_INNOVATION_METROS_RAW))) {
    STATSAMERICA_INNOVATION_METROS_RAW %>%
      dplyr::rename(AREA_NAME = description) %>%
      dplyr::filter(time_id == 2023) %>%
      tidyr::pivot_wider(id_cols = c(geo_id, AREA_NAME), 
                         names_from = `Code Description`, 
                         values_from = `Measure Value`)
  } else {
    .warn("StatsAmerica metros missing columns")
    tibble::tibble()
  }
} else {
  tibble::tibble()
}
g(STATSAMERICA_INNOVATION_METROS, "STATSAMERICA_INNOVATION_METROS")

# ---- 5.2 MSA GDP ------------------------------------------------------------
msa_gdp <- safe_fread(fp(RAW_DATA_FOLDER, "msa_gdp_2022.csv"), skip = 3)
g(msa_gdp, "msa_gdp")

# ---- 5.3 County-CBSA Crosswalk ---------------------------------------------
county_cbsa <- safe_fread(fp(DATA_FOLDER, "US Maps etc", "Regions", "csa_cbsa_county.csv"), skip = 2)
g(county_cbsa, "county_cbsa")

# ---- 5.4 BEA Economic Areas -------------------------------------------------
EAs <- tryCatch({
  readxl::read_excel(fp(RAW_DATA_FOLDER, "BEA Economic Areas and Counties.xls"), 2)
}, error = function(e) {
  .warn(paste("BEA EAs:", e$message))
  tibble::tibble()
})
g(EAs, "EAs")

# ---- 5.5 FCC PEAs -----------------------------------------------------------
pea <- tryCatch({
  readxl::read_excel(fp(RAW_DATA_FOLDER, "FCC_PEA_website.xlsx"), 2)
}, error = function(e) {
  .warn(paste("PEA:", e$message))
  tibble::tibble()
})
g(pea, "pea")

# =============================================================================
# SECTION 6: NATIONAL-LEVEL DATA
# =============================================================================

.msg("INFO", "=== LOADING NATIONAL-LEVEL DATA ===")

# ---- 6.1 Clean Investment Monitor (National Summary) -----------------------
investment <- safe_fread(fp(RAW_DATA_FOLDER, "clean_investment_monitor_2_2025",
                            "quarterly_actual_investment.csv"), skip = 4)
g(investment, "investment")

socioecon <- safe_fread(fp(RAW_DATA_FOLDER, "clean_investment_monitor_q2_2025",
                           "socioeconomics.csv"), skip = 4)
g(socioecon, "socioecon")

tax_inv_cat <- safe_fread(fp(RAW_DATA_FOLDER, "clean_investment_monitor_q2_2025",
                             "federal_actual_investment_by_category.csv"), skip = 4)
g(tax_inv_cat, "tax_inv_cat")

tax_inv_state <- safe_fread(fp(RAW_DATA_FOLDER, "clean_investment_monitor_q2_2025",
                               "federal_actual_investment_by_state.csv"), skip = 4)
g(tax_inv_state, "tax_inv_state")

# ---- 6.2 IRA/BIL Federal Grants ---------------------------------------------
ira_bil <- tryCatch({
  readxl::read_excel(fp(RAW_DATA_FOLDER, "Investment Data - SHARED.xlsx"), sheet = 3)
}, error = function(e) {
  .warn(paste("IRA/BIL:", e$message))
  tibble::tibble()
})
g(ira_bil, "ira_bil")

# ---- 6.3 LBNL Interconnection Queue -----------------------------------------
#MORE HERE: https://emp.lbl.gov/maps-projects-region-state-and-county 
.msg("INFO", "Loading LBNL interconnection data...")
lbnl_candidates <- c(
  "https://emp.lbl.gov/sites/default/files/2025-08/LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx",
  "https://emp.lbl.gov/sites/default/files/2025-07/LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx"
)
lbnl_dest <- fp(getwd(), "LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx")

if (!file.exists(lbnl_dest) || file.info(lbnl_dest)$size == 0) {
  ok <- FALSE
  for (u in lbnl_candidates) {
    ok <- tryCatch(!is.null(safe_download(u, lbnl_dest)), 
                   error = function(e) {
                     .warn(e$message)
                     FALSE
                   })
    if (ok) break
  }
}

lbnl_sheets <- if (file.exists(lbnl_dest)) {
  tryCatch(readxl::excel_sheets(lbnl_dest), error = function(e) character())
} else {
  character()
}

if (length(lbnl_sheets) >= 6) {
  sheet5_data <- tryCatch(readxl::read_excel(lbnl_dest, sheet = lbnl_sheets[5]), 
                          error = function(e) tibble::tibble())
  sheet6_data <- tryCatch(readxl::read_excel(lbnl_dest, sheet = lbnl_sheets[6]), 
                          error = function(e) tibble::tibble())
} else {
  sheet5_data <- tibble::tibble()
  sheet6_data <- tibble::tibble()
}
g(sheet5_data, "LBNL Sheet 5")
g(sheet6_data, "LBNL Sheet 6")

# =============================================================================
# SECTION 7: POINT/FACILITY DATA (Geography initially unknown)
# =============================================================================

.msg("INFO", "=== LOADING POINT/FACILITY DATA ===")

# ---- 7.1 Semiconductor Manufacturing Investment ----------------------------
#MORE HERE: https://www.semiconductors.org/chip-supply-chain-investments/ 
SEMICONDUCTOR_MANUFACTURING_INVESTMENT <- tryCatch({
  chart_id <- "u1vJC"
  base_url <- sprintf("https://datawrapper.dwcdn.net/%s/", chart_id)
  html_txt <- httr::RETRY("GET", base_url, httr::timeout(30)) %>%
    httr::content(as = "text", encoding = "UTF-8")
  edition <- stringr::str_match(html_txt, sprintf("/%s/(\\d+)/dataset\\.csv", chart_id))[, 2]
  
  if (!is.na(edition)) {
    df <- safe_fread(sprintf("https://datawrapper.dwcdn.net/%s/%s/dataset.csv", 
                             chart_id, edition))
    if ("Project Size ($)" %in% names(df)) {
      df %>% 
        dplyr::mutate(`Project Size ($)` = as.numeric(gsub("[\\$,]", "", 
                                                           `Project Size ($)`)))
    } else {
      df
    }
  } else {
    tibble::tibble()
  }
}, error = function(e) {
  .warn(paste("Semiconductor data:", e$message))
  tibble::tibble()
})
g(SEMICONDUCTOR_MANUFACTURING_INVESTMENT, "SEMICONDUCTOR_MANUFACTURING_INVESTMENT")

# ---- 7.2 Clean Investment Monitor Facilities -------------------------------
CLEAN_INVESTMENT_MONITOR_FACILITIES <- tryCatch({
  cim <- fp(RAW_DATA_FOLDER, "clean_investment_monitor_q2_2025",
            "manufacturing_energy_and_industry_facility_metadata.csv")
  
  if (file.exists(cim)) {
    df_raw <- safe_fread(cim, skip = 5, check.names = TRUE)
    
    df_raw %>%
      dplyr::mutate(
        Announcement_Date = suppressWarnings(as.Date(Announcement_Date, "%Y-%m-%d")),
        Construction_Start = suppressWarnings(as.Date(Construction_Start, "%Y-%m-%d")),
        Construction_End = suppressWarnings(as.Date(Construction_End, "%Y-%m-%d")),
        Announcement_Year = dplyr::if_else(!is.na(Announcement_Date), 
                                           format(Announcement_Date, "%Y"), NA_character_),
        Announcement_Quarter = dplyr::if_else(!is.na(Announcement_Date), 
                                              paste0(format(Announcement_Date, "%Y"), "-Q", 
                                                     ceiling(as.numeric(format(Announcement_Date, "%m")) / 3)), 
                                              NA_character_),
        Investment_Announced_Post_IRA = dplyr::if_else(!is.na(Announcement_Date), 
                                                       Announcement_Date >= as.Date("2022-08-16"), NA)
      ) %>%
      {
        if ("Estimated_Total_Facility_CAPEX" %in% names(.)) {
          dplyr::mutate(., Estimated_Total_Facility_CAPEX = Estimated_Total_Facility_CAPEX * 1e6)
        } else {
          .
        }
      }
  } else {
    .warn(paste("Missing:", cim))
    tibble::tibble()
  }
}, error = function(e) {
  .warn(paste("CIM facilities:", e$message))
  tibble::tibble()
})
g(CLEAN_INVESTMENT_MONITOR_FACILITIES, "CLEAN_INVESTMENT_MONITOR_FACILITIES")

# ---- 7.3 EIA 860M Generator Data -------------------------------------------
.msg("INFO", "Setting up EIA 860M data...")

# Helper functions for EIA
tech_bucket <- function(x) {
  x <- stringr::str_trim(as.character(x))
  dplyr::case_when(
    is.na(x) ~ NA_character_,
    stringr::str_detect(x, stringr::regex("Steam Coal|Integrated Gasification", TRUE)) ~ "Coal",
    stringr::str_detect(x, stringr::regex("Petroleum|Coke", TRUE)) ~ "Petroleum",
    stringr::str_detect(x, stringr::regex("^(Natural Gas|Other Natural Gas)|Compressed Air", TRUE)) ~ "Fossil Methane",
    stringr::str_detect(x, stringr::regex("Wind Turbine", TRUE)) ~ "Wind",
    stringr::str_detect(x, stringr::regex("^Solar |Solar Thermal", TRUE)) ~ "Solar",
    stringr::str_detect(x, stringr::regex("Geothermal", TRUE)) ~ "Geothermal",
    stringr::str_detect(x, stringr::regex("Biomass|Solid Waste", TRUE)) ~ "Biomass",
    stringr::str_detect(x, stringr::regex("Landfill Gas", TRUE)) ~ "Landfill Methane",
    stringr::str_detect(x, stringr::regex("Flywheel", TRUE)) ~ "Flywheel Storage",
    stringr::str_detect(x, stringr::regex("Batteries?", TRUE)) ~ "Battery Storage",
    stringr::str_detect(x, stringr::regex("Pumped Storage", TRUE)) ~ "Hydroelectric Pumped Storage",
    stringr::str_detect(x, stringr::regex("Nuclear", TRUE)) ~ "Nuclear",
    TRUE ~ "Other"
  )
}

classify_clean_fossil <- function(x) {
  x <- stringr::str_trim(as.character(x))
  dplyr::case_when(
    is.na(x) ~ NA_character_,
    x %in% c("Natural Gas Steam Turbine", "Natural Gas Fired Combined Cycle",
             "Natural Gas Internal Combustion Engine", "Natural Gas Fired Combustion Turbine",
             "Conventional Steam Coal", "Petroleum Liquids") ~ "Fossil",
    x %in% c("Conventional Hydroelectric", "Onshore Wind Turbine", "Offshore Wind Turbine",
             "Batteries", "Solar Photovoltaic", "Solar Thermal with Energy Storage",
             "Hydroelectric Pumped Storage", "Geothermal", "Wood/Wood Waste Biomass",
             "Nuclear") ~ "Clean",
    stringr::str_detect(x, stringr::regex("Coal|Petroleum|Natural Gas", TRUE)) ~ "Fossil",
    stringr::str_detect(x, stringr::regex("Solar|Wind|Hydro|Nuclear|Biomass|Geothermal|Batteries", TRUE)) ~ "Clean",
    TRUE ~ "Other"
  )
}

eia_path <- fp(RAW_DATA_FOLDER, "EIA_860M_June_2025.xlsx")

read_eia <- function(sheet_name, file_path, auto_cancel = FALSE) {
  if (!file.exists(file_path)) {
    .warn(paste("EIA file not found:", file_path))
    return(tibble::tibble())
  }
  
  av <- tryCatch(readxl::excel_sheets(file_path), error = function(e) NULL)
  if (is.null(av)) {
    .warn("Unable to read EIA sheets")
    return(tibble::tibble())
  }
  
  if (auto_cancel) {
    sh <- av[grepl("Cancel", av, ignore.case = TRUE)]
    if (!length(sh)) sh <- "Canceled or Postponed"
    else sh <- sh[1]
  } else {
    sh <- sheet_name
  }
  
  if (!sh %in% av) {
    .warn(paste("Sheet missing:", sh))
    return(tibble::tibble())
  }
  
  readxl::read_excel(file_path, sheet = sh, skip = 2)
}

EIA_860M_PLANNED <- read_eia("Planned", eia_path) %>%
  dplyr::mutate(
    Technology_Category = tech_bucket(Technology),
    Clean_Fossil = classify_clean_fossil(Technology)
  )
g(EIA_860M_PLANNED, "EIA_860M_PLANNED")

EIA_860M_RETIRED <- read_eia("Retired", eia_path) %>%
  dplyr::mutate(
    Technology_Category = tech_bucket(Technology),
    Clean_Fossil = classify_clean_fossil(Technology)
  )
g(EIA_860M_RETIRED, "EIA_860M_RETIRED")

EIA_860M_CANCELED <- tryCatch({
  read_eia("Canceled or Postponed", eia_path, auto_cancel = TRUE) %>%
    dplyr::mutate(
      Technology_Category = tech_bucket(Technology),
      Clean_Fossil = classify_clean_fossil(Technology)
    )
}, error = function(e) {
  .warn(paste("Canceled sheet:", e$message))
  tibble::tibble()
})
g(EIA_860M_CANCELED, "EIA_860M_CANCELED")

# ---- 7.4 Coal Power Plant Reinvestment -------------------------------------
coal_file <- fp(RAW_DATA_FOLDER, "IWG", "NETL Coal Power Plant Reinvestment Visualization Tool",
                "Coal_Retiring_Beta_plants_retirement_2015_2030.csv")
COAL_POWER_PLANT_REINVESTMENT_CSV <- safe_fread(coal_file)
g(COAL_POWER_PLANT_REINVESTMENT_CSV, "COAL_POWER_PLANT_REINVESTMENT_CSV")

# ---- 7.5 Petroleum Terminals ------------------------------------------------
PETROLEUM_TERMINALS_PATH <- fp(PROCESSED_GPKG_FOLDER, "Petroleum_Terminals_Processed.csv")
PETROLEUM_TERMINALS <- safe_fread(PETROLEUM_TERMINALS_PATH)
g(PETROLEUM_TERMINALS, "PETROLEUM_TERMINALS")

# ---- 7.6 Brownfield Sites ---------------------------------------------------
BROWNFIELD_SITES_PATH <- fp(PROCESSED_GPKG_FOLDER, "Brownfield_Sites.csv")
BROWNFIELD_SITES <- safe_fread(BROWNFIELD_SITES_PATH)
g(BROWNFIELD_SITES, "BROWNFIELD_SITES")

# ---- 7.7 Critical Minerals --------------------------------------------------
CRITICAL_MINERALS_PATH <- fp(PROCESSED_GPKG_FOLDER, 
                             "CRITICAL_MINERAL_MINES_DEPOSITS_GLOBAL_NETL.csv")
CRITICAL_MINERAL_MINES_DEPOSITS_GLOBAL_NETL <- if (file.exists(CRITICAL_MINERALS_PATH)) {
  safe_fread(CRITICAL_MINERALS_PATH) %>%
    dplyr::filter(tolower(location) %in% c("united states of america", "usa", 
                                           "united states"))
} else {
  tibble::tibble()
}
g(CRITICAL_MINERAL_MINES_DEPOSITS_GLOBAL_NETL, "CRITICAL_MINERAL_MINES_DEPOSITS_GLOBAL_NETL")

# =============================================================================
# SECTION 8: CROSS-GEOGRAPHIC DATA (Spatial layers, networks, etc.)
# =============================================================================

.msg("INFO", "=== LOADING CROSS-GEOGRAPHIC DATA ===")

# ---- 8.1 Geopackages --------------------------------------------------------
.msg("INFO", "Loading geopackages in parallel...")
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

load_geopackage <- function(gpkg, base_folder) {
  p <- fp(base_folder, gpkg)
  nm <- make.names(tools::file_path_sans_ext(gpkg))
  if (!file.exists(p)) {
    return(list(name = nm, data = NULL, error_msg = paste("Not found:", p)))
  }
  out <- NULL
  err <- NA_character_
  tryCatch({
    out <- sf::st_read(p, quiet = TRUE)
  }, error = function(e) {
    err <<- e$message
    out <<- NULL
  })
  list(name = nm, data = out, error_msg = err)
}

if (dir.exists(geopkg_folder)) {
  res <- furrr::future_map(geopkgs_to_load, load_geopackage, 
                           base_folder = geopkg_folder,
                           .options = furrr::furrr_options(seed = TRUE, packages = "sf"))
  for (r in res) {
    if (!is.null(r$data)) {
      assign(r$name, r$data, envir = .GlobalEnv)
      dbg(get(r$name, envir = .GlobalEnv), r$name)
    } else {
      .warn(paste("Geopkg failed:", r$name, if (!is.na(r$error_msg)) r$error_msg))
    }
  }
}

# ---- 8.2 EIA Spatial Layers (ArcGIS) ---------------------------------------
fetch_layer_as_sf <- function(base_url, layer_id, crs_out = 4326, label = "layer") {
  lyr_url <- paste0(base_url, layer_id)
  meta_url <- paste0(lyr_url, "?f=pjson")
  
  meta <- tryCatch({
    httr::RETRY("GET", meta_url, httr::timeout(30)) %>%
      httr::content(as = "text", encoding = "UTF-8") %>%
      jsonlite::fromJSON()
  }, error = function(e) NULL)
  
  if (is.null(meta) || is.null(meta$fields)) return(NULL)
  
  alias_map <- stats::setNames(meta$fields$alias, meta$fields$name)
  
  sf_obj <- tryCatch(
    esri2sf::esri2sf(lyr_url, where = "1=1", outFields = "*", 
                     crs = crs_out, progress = FALSE),
    error = function(e) NULL
  )
  
  if (is.null(sf_obj) || nrow(sf_obj) == 0) return(NULL)
  
  # Rename fields based on aliases
  geom <- attr(sf_obj, "sf_column")
  attrs <- setdiff(names(sf_obj), geom)
  subset_map <- alias_map[names(alias_map) %in% attrs]
  
  newn <- names(sf_obj)
  idx <- match(names(subset_map), newn)
  if (length(idx) && !any(is.na(idx))) {
    newn[idx] <- make.unique(as.character(subset_map), sep = "_")
    names(sf_obj) <- newn
  }
  sf_obj
}

arc_layers <- list(
  list(base_url = "https://services7.arcgis.com/FGr1D95XCGALKXqM/ArcGIS/rest/services/Renewable_Diesel_and_Other_Biofuels/FeatureServer/",
       layer_id = 245, label = "renewable_biofuel_plants_sf"),
  list(base_url = "https://services7.arcgis.com/FGr1D95XCGALKXqM/ArcGIS/rest/services/Balancing_Authorities/FeatureServer/",
       layer_id = 255, label = "balancing_authorities_sf"),
  list(base_url = "https://services7.arcgis.com/FGr1D95XCGALKXqM/ArcGIS/rest/services/NERC_Regions_EIA/FeatureServer/",
       layer_id = 0, label = "nerc_regions_sf")
)

arc_res <- furrr::future_pmap(
  list(
    base_url = purrr::map_chr(arc_layers, "base_url"),
    layer_id = purrr::map_int(arc_layers, "layer_id"),
    label = purrr::map_chr(arc_layers, "label")
  ),
  fetch_layer_as_sf,
  crs_out = CRS_USE,
  .options = furrr::furrr_options(seed = TRUE, 
                                  packages = c("httr", "jsonlite", "esri2sf", "sf"))
)

default_empty_sf <- sf::st_sf(geometry = sf::st_sfc(crs = CRS_USE))

renewable_biofuel_plants_sf <- if (length(arc_res) >= 1 && !is.null(arc_res[[1]]) && 
                                   inherits(arc_res[[1]], "sf")) {
  arc_res[[1]]
} else {
  default_empty_sf
}
g(sf::st_drop_geometry(renewable_biofuel_plants_sf), "renewable_biofuel_plants_sf")

balancing_authorities_sf <- if (length(arc_res) >= 2 && !is.null(arc_res[[2]]) && 
                                inherits(arc_res[[2]], "sf")) {
  arc_res[[2]]
} else {
  default_empty_sf
}
g(sf::st_drop_geometry(balancing_authorities_sf), "balancing_authorities_sf")

nerc_regions_sf <- if (length(arc_res) >= 3 && !is.null(arc_res[[3]]) && 
                       inherits(arc_res[[3]], "sf")) {
  arc_res[[3]]
} else {
  default_empty_sf
}
g(sf::st_drop_geometry(nerc_regions_sf), "nerc_regions_sf")

# ---- 8.3 Electric Retail Service Territories -------------------------------
retail_shp <- fp(RAW_DATA_FOLDER, "Electric_Retail_Service_Territories.shp")
shapefile <- if (file.exists(retail_shp)) {
  sf::st_read(retail_shp, quiet = TRUE)
} else {
  .warn(paste("Missing:", retail_shp))
  NULL
}
if (!is.null(shapefile)) g(shapefile, "Electric_Retail_Service_Territories")

# ---- 8.4 Electricity Maps Zone Data ----------------------------------------
.msg("INFO", "Loading Electricity Maps zone data...")
zones <- c("US-CAR-YAD", "US-SW-AZPS", "US-MIDW-AECI", "US-NW-AVA", "US-CAL-BANC",
           "US-NW-BPAT", "US-CAL-CISO", "US-NW-TPWR", "US-FLA-TAL", "US-CAR-DUK",
           "US-FLA-FPC", "US-CAR-CPLE", "US-CAR-CPLW", "US-SW-EPE", "US-TEX-ERCO",
           "US-FLA-FMPP", "US-FLA-FPL", "US-FLA-GVL", "US-NW-GRID", "US-NW-IPCO",
           "US-CAL-IID", "US-NE-ISNE", "US-FLA-JEA", "US-CAL-LDWP", "US-MIDW-LGEE",
           "US-MIDW-MISO", "US-NW-NEVP", "US-NY-NYIS", "US-NW-NWMT", "US-MIDA-PJM",
           "US-NW-CHPD", "US-NW-DOPD", "US-NW-GCPD", "US-NW-PACE", "US-NW-PACW",
           "US-NW-PGE", "US-NW-PSCO", "US-SW-PNM", "US-NW-PSEI", "US-SW-SRP",
           "US-NW-SCL", "US-FLA-SEC", "US-CAR-SCEG", "US-CAR-SC", "US-SE-SOCO",
           "US-CENT-SWPP", "US-CENT-SPA", "US-FLA-TEC", "US-TEN-TVA", "US-SW-TEPC",
           "US-CAL-TIDC", "US-SW-WALC", "US-NW-WACM", "US-NW-WAUW")

read_zone_yearly <- function(z) {
  u <- paste0("https://data.electricitymaps.com/2025-01-27/", z, "_2024_yearly.csv")
  tryCatch({
    safe_fread(u) %>% 
      dplyr::mutate(zone = z, .before = 1)
  }, error = function(e) {
    .warn(paste("Electricity Maps fetch failed for", z))
    tibble::tibble()
  })
}

yrly_list <- furrr::future_map(zones, read_zone_yearly,
                               .options = furrr::furrr_options(seed = TRUE, 
                                                               packages = c("data.table", "tibble", "dplyr")))
electricity_maps <- data.table::rbindlist(yrly_list, use.names = TRUE, fill = TRUE) %>%
  tibble::as_tibble()
g(electricity_maps, "electricity_maps")

# ---- 8.5 SSTI Flourish Dashboard Data --------------------------------------
#MORE HERE: https://ssti.org/key-technology-area-investment-data-tool 
.msg("INFO", "Extracting SSTI Flourish dashboard data...")
extract_flourish_data <- function(url) {
  headers <- httr::add_headers(
    "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent" = "Mozilla/5.0"
  )
  
  html <- tryCatch({
    httr::RETRY("GET", url, headers, httr::timeout(30)) %>%
      httr::content("text", "UTF-8") %>%
      rvest::read_html()
  }, error = function(e) NULL)
  
  if (is.null(html)) return(list())
  
  scripts <- rvest::html_nodes(html, "script") %>% rvest::html_text()
  res <- list()
  
  for (i in seq_along(scripts)) {
    scr <- scripts[i]
    if (nchar(scr) < 100) next
    
    matches <- stringr::str_extract_all(scr, "(\\[\\s*\\{.*?\\}\\s*(?:,\\s*\\{.*?\\}\\s*)*\\])")[[1]]
    if (length(matches) > 0) {
      for (j in seq_along(matches)) {
        try({
          parsed <- jsonlite::fromJSON(matches[j], simplifyDataFrame = TRUE)
          if (is.data.frame(parsed) && nrow(parsed) > 0) {
            res[[paste0("data_json_block_", i, "_", j, "_1")]] <- parsed
          }
        }, silent = TRUE)
      }
    }
  }
  res
}

urls <- c("https://flo.uri.sh/visualisation/24071662/embed?auto=1",
          "https://flo.uri.sh/visualisation/24071657/embed?auto=1")
dashboard_data <- list(
  dashboard_1 = extract_flourish_data(urls[1]),
  dashboard_2 = extract_flourish_data(urls[2])
)

SSTI_investment_data <- if (!is.null(dashboard_data$dashboard_1$data_json_block_4_4_1) &&
                            "popup_metadata" %in% names(dashboard_data$dashboard_1$data_json_block_4_4_1)) {
  main_df <- dashboard_data$dashboard_1$data_json_block_4_4_1
  data.frame(
    state = sapply(main_df$popup_metadata, `[[`, 1),
    year = sapply(main_df$popup_metadata, `[[`, 2),
    tech_category = sapply(main_df$popup_metadata, `[[`, 3),
    investment_stage = sapply(main_df$popup_metadata, `[[`, 4),
    deals = as.numeric(sapply(main_df$popup_metadata, `[[`, 5)),
    companies = as.numeric(sapply(main_df$popup_metadata, `[[`, 6)),
    investment = as.numeric(sapply(main_df$popup_metadata, `[[`, 7)) * 1e6,
    stringsAsFactors = FALSE
  )
} else {
  tibble::tibble()
}
g(SSTI_investment_data, "SSTI_investment_data")

# ---- 8.6 Additional Program Files -------------------------------------------
EDCI_FILE <- fp(RAW_DATA_FOLDER, "Economic_Development_Capacity_Index_Snapshot_Download_Aug_2025.xlsx")
if (file.exists(EDCI_FILE)) {
  EDCI_DATA_DICTIONARY <- readxl::read_excel(EDCI_FILE, "data_dictionary")
  g(EDCI_DATA_DICTIONARY, "EDCI_DATA_DICTIONARY")
  EDCI_INDICATORS <- readxl::read_excel(EDCI_FILE, "indicators")
  g(EDCI_INDICATORS, "EDCI_INDICATORS")
}

NERDE_FILE <- fp(RAW_DATA_FOLDER, "National_Economic_Resilience_Data_Explorer_Download_Aug_2025.xlsx")
if (file.exists(NERDE_FILE)) {
  NERDE <- readxl::read_excel(NERDE_FILE)
  g(NERDE, "NERDE")
}

EDA_FILE <- fp(RAW_DATA_FOLDER, "EDA_Distress_Communities_Download_Aug_2025.xlsx")
if (file.exists(EDA_FILE)) {
  EDA_DISTRESSED_COMMUNITIES <- readxl::read_excel(EDA_FILE)
  g(EDA_DISTRESSED_COMMUNITIES, "EDA_DISTRESSED_COMMUNITIES")
}

#Syracuse University State Policy & Politics Database
SYRACUASE_STATE_POLITICS_POLICY_FILE <- fp(RAW_DATA_FOLDER, "Syracuse University State Policy and Politics Database", "SPPD_v1_3.1_2025_Update.xlsx")
if (file.exists(SYRACUASE_STATE_POLITICS_POLICY_FILE)) {
  SYRACUASE_STATE_POLITICS_POLICY <- readxl::read_excel(SYRACUASE_STATE_POLITICS_POLICY_FILE)
  g(SYRACUASE_STATE_POLITICS_POLICY, "SYRACUASE_STATE_POLITICS_POLICY")
}

#Light-Duty Vehicles Trends, DOE
LDV_TRENDS_LINK <- "https://developer.nrel.gov/api/vehicles/v1/light_duty_automobiles.csv?api_key=zWXzDiPTFsCBQQrfRhrlm1Sh1FIC4ZpfvbVY8EIr&download=true"
LDV_TRENDS <- tryCatch({
  safe_fread(LDV_TRENDS_LINK)
}, error = function(e) {
  .warn(paste("LDV Trends:", e$message))
  tibble::tibble()
})
g(LDV_TRENDS, "LDV_TRENDS")

#CATF Fusion Map: https://www.catf.us/global-fusion-map/
suppressPackageStartupMessages({library(httr);library(jsonlite);library(dplyr);library(purrr);library(stringr);library(sf);library(tigris)})
`%||%`<-function(a,b) if(!is.null(a)) a else b
options(tigris_use_cache=TRUE,tigris_class="sf"); UA<-user_agent("Mozilla/5.0 (compatible; catf-fusion-scrape/1.50)")
endpoints<-c(public="https://catf-fusion-2024.vercel.app/Data_for_global_fusion_map_2020-2024update_public.geojson",private="https://catf-fusion-2024.vercel.app/Data_for_global_fusion_map_2020-2024update_private.geojson",devices="https://catf-fusion-2024.vercel.app/Data_for_global_fusion_map_2020-2024update_devices.geojson")
counties_sf<-counties(year=2024,cb=FALSE,progress_bar=FALSE)%>%st_transform(4326)%>%select(STATEFP,GEOID,NAMELSAD,CBSAFP,CSAFP,geometry)
fusion_flat_usa<-imap(endpoints,function(u,nm){
  r<-tryCatch(GET(u,UA,timeout(30)),error=function(e)NULL); if(is.null(r)||status_code(r)!=200)return(tibble())
  j<-tryCatch(fromJSON(content(r,as="text",encoding="UTF-8"),simplifyVector=FALSE),error=function(e)NULL)
  if(!is.list(j)||j$type!="FeatureCollection")return(tibble())
  fe<-j$features; pr<-map(fe,~.x$properties%||%list()); df<-bind_rows(map(pr,as.list))
  gm<-map(fe,~.x$geometry%||%list(type=NA,coordinates=NA)); gt<-map_chr(gm,~.x$type%||%NA_character_)
  lon<-lat<-rep(NA_real_,length(gm)); ip<-which(gt=="Point"); if(length(ip)){cd<-map(gm[ip],~.x$coordinates%||%c(NA,NA)); lon[ip]<-map_dbl(cd,1); lat[ip]<-map_dbl(cd,2)}
  df%>%mutate(dataset=nm,lon=lon,lat=lat,source_url=u)
})%>%bind_rows()%>%
  mutate(
    name=case_when(dataset=="private"~`Company`%||%NA_character_,dataset=="public"~`Company/Institution`%||%NA_character_,dataset=="devices"~((`Device Name`%||%`Full device name`)%||%NA_character_),TRUE~NA_character_),
    entity_type=case_when(dataset=="private"~"company",dataset=="public"~"education_research",dataset=="devices"~"device",TRUE~NA_character_),
    country=str_squish(`country`%||%Country%||%COUNTRY%||%NA_character_), location=str_squish(Location%||%`Location `%||%location%||%NA_character_),
    fusion_category=fusion_category%||%NA_character_, investment_numeric=suppressWarnings(as.numeric(investment_numeric%||%NA_real_)),
    investment_usd=str_squish(investment_usd%||%NA_character_), website=str_squish(website%||%`website*`%||%NA_character_), notes=other_notes%||%notes%||%NA_character_, name=str_squish(ifelse(is.na(name)|name=="",NA_character_,name))
  )%>%
  distinct(entity_type,name,country,lat,lon,.keep_all=TRUE)%>%filter(!is.na(lat)&!is.na(lon)&country=="USA")%>%
  st_as_sf(coords=c("lon","lat"),crs=4326,remove=FALSE)%>%
  st_join(counties_sf,join=st_within,left=TRUE)%>%st_drop_geometry()%>%
  rename(statefp=STATEFP,county_geoid=GEOID,county_namelsad=NAMELSAD,cbsafp=CBSAFP,csafp=CSAFP)%>%
  mutate(investment_usd_million=ifelse(!is.na(investment_numeric),investment_numeric*1e6,NA_real_))%>%
  select(dataset,entity_type,name,country,location,lat,lon,fusion_category,investment_numeric,investment_usd,website,notes,source_url,statefp,county_geoid,county_namelsad,cbsafp,csafp,investment_usd_million)
glimpse(fusion_flat_usa)

#County Health Rankings Data
#Their website: https://www.countyhealthrankings.org/ 
library(readr)
library(dplyr)
library(tidyverse)
library(readxl)
library(writexl)
library(data.table)
library(lubridate)
library(censusapi)

# Other resources of note
# https://www.countyhealthrankings.org/sites/default/files/media/document/analytic_data2025_v2.csv
# https://www.countyhealthrankings.org/sites/default/files/media/document/Analytic%20Dataset%20Codebook%20Supplemental%20Release_November2025.pdf
# https://www.countyhealthrankings.org/sites/default/files/media/document/analytic_supplement_20251104.csv
# https://www.countyhealthrankings.org/sites/default/files/media/document/DataDictionary_2025.pdf
# https://www.countyhealthrankings.org/sites/default/files/media/document/DataDictionary_2025.xlsx
# https://www.countyhealthrankings.org/sites/default/files/media/document/chr_trends_csv_2025.csv
# https://www.countyhealthrankings.org/sites/default/files/media/document/2025%20County%20Health%20Rankings%20Data%20-%20v3.xlsx

#2025 CHR CSV Analytic Data
county_health_rankings_2025 <-read_csv("https://www.countyhealthrankings.org/sites/default/files/media/document/analytic_data2025_v2.csv", skip = 1)
cat("Glimpse of 2025 County Health Rankings Analytic Data:\n"); glimpse(county_health_rankings_2025)
cat("\nColumn names in 2025 County Health Rankings Analytic Data:\n"); print(colnames(county_health_rankings_2025))

#Data dictionary also available here: https://www.countyhealthrankings.org/sites/default/files/media/document/DataDictionary_2025.pdf 
data_dictionary_link <- "https://www.countyhealthrankings.org/sites/default/files/media/document/DataDictionary_2025.xlsx" 
#Confirm that the link works; download temporarily; list sheet names
temp_file <- tempfile(fileext = ".xlsx")
download.file(data_dictionary_link, destfile = temp_file, mode = "wb")
sheet_names <- excel_sheets(temp_file)
print(sheet_names)
#Read in the "Data Dictionary 2025" sheet
data_dictionary_2025 <- read_excel(temp_file, sheet = "Data Dictionary 2025")
glimpse(data_dictionary_2025)

#Revise county_health_rankings_2025 such that we take each column name, we look to see if there is a matching "Variable Name" value in data_dictionary_2025, and if there is, replace the column name with the corresponding "Measure" information from data_dictionary_2025
#Then we would rename this county_health_rankings_2025_readable
county_health_rankings_2025_readable <- county_health_rankings_2025
for (col_name in colnames(county_health_rankings_2025)) {
  measure_name <- data_dictionary_2025 %>%
    filter(`Variable Name` == col_name) %>%
    select(Measure) %>%
    pull()
  if (length(measure_name) > 0) {
    colnames(county_health_rankings_2025_readable)[which(colnames(county_health_rankings_2025_readable) == col_name)] <- measure_name
  }
}
cat("\nGlimpse of 2025 County Health Rankings Analytic Data with Readable Column Names:\n"); glimpse(county_health_rankings_2025_readable)
