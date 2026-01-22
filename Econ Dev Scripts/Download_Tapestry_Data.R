################################################################################
#This R script is for downloading and testing Tapestry data. We compare two different forms in which the data is presented. 
#One format involves re-allocating "NAICS 999999" and "unknown/undefined county" data.
#Another format retains such data, as this data also exists in QCEW.
#Much of this data is already saved to RMI SharePoint, but in case you can't access that, use this.
#Important: Start by creating an account at https://tapestry.nkn.uidaho.edu/. 
#The script should prompt you for your login information. 
##
## ALL OUTPUT IS TO CONSOLE ONLY — NO FILES SAVED
################################################################################

# ==============================================================================
# PACKAGE INSTALLATION AND LOADING
# ==============================================================================

ensure_packages <- function(pkgs) {
  for (pkg in pkgs) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      cat(sprintf("[SETUP] Installing missing package: %s\n", pkg))
      install.packages(pkg, quiet = TRUE)
    }
  }
}

required_packages <- c(
  "httr", "jsonlite", "utils", "readr", "dplyr", "tibble",
  "rvest", "stringr", "future", "furrr", "curl",
  "readxl", "purrr",
  "tigris", "sf"
)

optional_packages <- c("keyring", "askpass", "pryr", "openssl", "digest")

cat("\n[SETUP] Checking required packages...\n")
ensure_packages(required_packages)
ensure_packages(optional_packages)

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(utils)
  library(readr)
  library(dplyr)
  library(tibble)
  library(rvest)
  library(stringr)
  library(future)
  library(furrr)
  library(curl)
  library(readxl)
  library(purrr)
  library(tigris)
  library(sf)
})

has_pryr   <- requireNamespace("pryr", quietly = TRUE)
has_openssl <- requireNamespace("openssl", quietly = TRUE)
has_digest <- requireNamespace("digest", quietly = TRUE)

# ==============================================================================
# CONFIGURATION
# ==============================================================================

BASE_URL <- "https://tapestry.nkn.uidaho.edu"

# Retry configuration
MAX_RETRIES <- 3
RETRY_DELAY_BASE <- 5  # seconds

# Tigris options
options(tigris_use_cache = FALSE)

# Default parameters (v5)
DEFAULT_YEAR <- "2024"
DEFAULT_OWNERSHIP <- "0"
DEFAULT_CONVERSION <- "no_conversion"
DEFAULT_RUN_DIAGNOSTICS <- FALSE

# ==============================================================================
# CONSOLE OUTPUT HELPERS
# ==============================================================================

COLORS <- list(
  reset  = "\033[0m",
  bold   = "\033[1m",
  red    = "\033[31m",
  green  = "\033[32m",
  yellow = "\033[33m",
  blue   = "\033[34m",
  magenta= "\033[35m",
  cyan   = "\033[36m",
  white  = "\033[37m"
)

cat_info    <- function(...) cat(COLORS$cyan,   "[INFO]",   COLORS$reset, " ", ..., "\n", sep = "")
cat_success <- function(...) cat(COLORS$green,  "[OK]",     COLORS$reset, " ", ..., "\n", sep = "")
cat_warn    <- function(...) cat(COLORS$yellow, "[WARN]",   COLORS$reset, " ", ..., "\n", sep = "")
cat_error   <- function(...) cat(COLORS$red,    "[ERROR]",  COLORS$reset, " ", ..., "\n", sep = "")
cat_debug   <- function(...) cat(COLORS$magenta,"[DEBUG]",  COLORS$reset, " ", ..., "\n", sep = "")
cat_timing  <- function(...) cat(COLORS$blue,   "[TIME]",   COLORS$reset, " ", ..., "\n", sep = "")
cat_header  <- function(...) cat("\n", COLORS$bold, COLORS$magenta, ..., COLORS$reset, "\n", sep = "")
cat_subhead <- function(...) cat(COLORS$bold, COLORS$blue, ..., COLORS$reset, "\n", sep = "")
cat_finding <- function(...) cat(COLORS$bold, COLORS$green, "[FINDING]", COLORS$reset, " ", ..., "\n", sep = "")

draw_box <- function(text, width = 72) {
  text_len <- nchar(text)
  padding <- max(0, width - text_len - 4)
  left_pad <- padding %/% 2
  right_pad <- padding - left_pad
  
  cat("\n")
  cat(COLORS$cyan)
  cat("╔", paste(rep("═", width - 2), collapse = ""), "╗\n", sep = "")
  cat("║", paste(rep(" ", left_pad), collapse = ""), text,
      paste(rep(" ", right_pad), collapse = ""), "║\n", sep = "")
  cat("╚", paste(rep("═", width - 2), collapse = ""), "╝\n", sep = "")
  cat(COLORS$reset)
}

get_memory_usage <- function() {
  if (has_pryr) {
    mem <- pryr::mem_used()
    return(format(mem, units = "auto"))
  }
  return("N/A (install pryr for memory tracking)")
}

format_bytes <- function(bytes) {
  if (is.na(bytes) || bytes < 0) return("unknown")
  if (bytes < 1024) return(sprintf("%d B", bytes))
  if (bytes < 1024^2) return(sprintf("%.1f KB", bytes / 1024))
  if (bytes < 1024^3) return(sprintf("%.1f MB", bytes / 1024^2))
  return(sprintf("%.1f GB", bytes / 1024^3))
}

format_number <- function(x) {
  format(x, big.mark = ",", scientific = FALSE)
}

# ==============================================================================
# TIMING UTILITIES
# ==============================================================================

.timing_data <- new.env()
.timing_data$operations <- list()

start_timer <- function(operation_name) {
  .timing_data$operations[[operation_name]] <- list(start = Sys.time(), end = NULL)
}

stop_timer <- function(operation_name) {
  
  if (!is.null(.timing_data$operations[[operation_name]])) {
    .timing_data$operations[[operation_name]]$end <- Sys.time()
    elapsed <- difftime(
      .timing_data$operations[[operation_name]]$end,
      .timing_data$operations[[operation_name]]$start,
      units = "secs"
    )
    cat_timing(sprintf("%s completed in %.2f seconds", operation_name, as.numeric(elapsed)))
    return(as.numeric(elapsed))
  }
  return(NA_real_)
}

print_timing_summary <- function() {
  cat_header("=== TIMING SUMMARY ===")
  total <- 0
  for (name in names(.timing_data$operations)) {
    op <- .timing_data$operations[[name]]
    if (!is.null(op$end)) {
      elapsed <- as.numeric(difftime(op$end, op$start, units = "secs"))
      total <- total + elapsed
      cat(sprintf("  %-40s : %10.2f seconds\n", name, elapsed))
    }
  }
  cat(sprintf("  %-40s : %10.2f seconds\n", "TOTAL (sum of timers)", total))
  cat("\nNOTE: TOTAL (sum of timers) can exceed wall time because timers overlap.\n")
}

# ==============================================================================
# GENERAL HELPERS
# ==============================================================================

safe_content_text <- function(resp) {
  tryCatch(
    content(resp, as = "text", encoding = "UTF-8"),
    error = function(e) content(resp, as = "text")
  )
}

`%||%` <- function(x, y) if (is.null(x)) y else x

debug_response <- function(resp, label = "Response") {
  cat_debug(sprintf("%s Details:", label))
  cat_debug(sprintf("  Status: %d", status_code(resp)))
  cat_debug(sprintf("  Content-Type: %s", headers(resp)[["content-type"]] %||% "N/A"))
  cat_debug(sprintf("  Content-Length: %s", headers(resp)[["content-length"]] %||% "N/A"))
  
  for (h in c("x-request-id", "server", "date", "cache-control", "x-powered-by", "via")) {
    if (!is.null(headers(resp)[[h]])) {
      cat_debug(sprintf("  %s: %s", h, headers(resp)[[h]]))
    }
  }
}

prompt_menu <- function(prompt_text, options, allow_multiple = FALSE) {
  cat("\n")
  cat_subhead(prompt_text)
  cat("  [0] EXIT SCRIPT\n")
  for (i in seq_along(options)) {
    cat(sprintf("  [%d] %s\n", i, options[i]))
  }
  if (allow_multiple) {
    cat("\nEnter numbers separated by commas, 'all', or 0 to exit: ")
  } else {
    cat("\nEnter selection (or 0 to exit): ")
  }
  input <- trimws(readline())
  if (input == "0" || tolower(input) == "exit") {
    cat_warn("User requested exit.")
    cleanup_parallel()
    stop("Script terminated by user.", call. = FALSE)
  }
  if (tolower(input) == "all" && allow_multiple) return(options)
  indices <- suppressWarnings(as.integer(strsplit(input, ",")[[1]]))
  indices <- indices[!is.na(indices) & indices >= 1 & indices <= length(options)]
  if (length(indices) == 0) {
    cat_warn("Invalid selection. Using first option.")
    return(options[1])
  }
  return(options[indices])
}

prompt_yes_no <- function(prompt_text) {
  cat(sprintf("\n%s (y/n/exit): ", prompt_text))
  input <- tolower(trimws(readline()))
  if (input == "exit" || input == "0") {
    cat_warn("User requested exit.")
    cleanup_parallel()
    stop("Script terminated by user.", call. = FALSE)
  }
  return(input %in% c("y", "yes"))
}

# ==============================================================================
# PARALLEL PROCESSING
# ==============================================================================

setup_parallel <- function(workers = NULL) {
  if (is.null(workers)) {
    workers <- max(1, parallel::detectCores() - 1)
  }
  workers <- min(workers, 4)
  
  cat_info(sprintf("Setting up parallel processing with %d workers...", workers))
  cat_debug(sprintf("  Platform: %s", .Platform$OS.type))
  cat_debug(sprintf("  R version: %s", R.version.string))
  
  plan(multisession, workers = workers)
  cat_success(sprintf("Parallel backend ready (%d workers)", workers))
  return(workers)
}

cleanup_parallel <- function() {
  plan(sequential)
  cat_info("Parallel workers cleaned up")
}

# ==============================================================================
# SYSTEM DIAGNOSTICS (COMPREHENSIVE - OPTIONAL)
# ==============================================================================

print_system_diagnostics <- function() {
  cat_header("=== SYSTEM DIAGNOSTICS (COMPREHENSIVE DEBUG) ===")
  
  # SECTION 1: OPERATING SYSTEM
  cat_subhead("1. OPERATING SYSTEM")
  cat(sprintf("  OS Type:          %s\n", .Platform$OS.type))
  cat(sprintf("  System:           %s\n", Sys.info()["sysname"]))
  cat(sprintf("  Release:          %s\n", Sys.info()["release"]))
  cat(sprintf("  Machine:          %s\n", Sys.info()["machine"]))
  cat(sprintf("  Platform:         %s\n", R.version$platform))
  
  # SECTION 2: R ENVIRONMENT
  cat_subhead("\n2. R ENVIRONMENT")
  cat(sprintf("  R Version:        %s\n", R.version.string))
  cat(sprintf("  R Home:           %s\n", R.home()))
  cat(sprintf("  Timezone:         %s\n", Sys.timezone()))
  cat(sprintf("  Working Dir:      %s\n", getwd()))
  
  cat("\n  Key package versions:\n")
  key_pkgs <- c("httr", "curl", "jsonlite", "readr", "dplyr", "tigris")
  for (pkg in key_pkgs) {
    ver <- tryCatch(as.character(packageVersion(pkg)), error = function(e) "NOT INSTALLED")
    cat(sprintf("    %-15s: %s\n", pkg, ver))
  }
  
  # SECTION 3: CONNECTIVITY TESTS
  cat_subhead("\n3. CONNECTIVITY TESTS")
  
  test_urls <- list(
    list(name = "Google", url = "https://www.google.com"),
    list(name = "UIdaho", url = "https://www.uidaho.edu"),
    list(name = "Tapestry", url = "https://tapestry.nkn.uidaho.edu")
  )
  
  for (test in test_urls) {
    result <- tryCatch({
      start_time <- Sys.time()
      resp <- httr::HEAD(test$url, httr::timeout(15))
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
      list(success = TRUE, status = httr::status_code(resp), time = elapsed)
    }, error = function(e) {
      list(success = FALSE, error = conditionMessage(e))
    })
    
    if (result$success) {
      cat(sprintf("  %s: Status %d (%.2fs)\n", test$name, result$status, result$time))
    } else {
      cat(sprintf("  %s: FAILED - %s\n", test$name, result$error))
    }
  }
  
  # SECTION 4: DIAGNOSTIC SUMMARY
  cat_subhead("\n4. DIAGNOSTIC SUMMARY")
  
  tapestry_ok <- tryCatch({
    resp <- httr::HEAD("https://tapestry.nkn.uidaho.edu", httr::timeout(10))
    httr::status_code(resp) == 200
  }, error = function(e) FALSE)
  
  cat("\n")
  cat(sprintf("  Tapestry Server: %s\n", if(tapestry_ok) "✓ OK" else "✗ FAILED"))
  
  return(tapestry_ok)
}

# ==============================================================================
# NAICS HIERARCHY CROSSWALK (BLS)
# ==============================================================================

naics_year_for_data_year <- function(year_int) {
  if (year_int >= 2022) return("2022")
  if (year_int >= 2017) return("2017")
  if (year_int >= 2012) return("2012")
  if (year_int >= 2007) return("2007")
  return("2002")
}

load_bls_qcew_naics_hierarchy <- function(verbose = TRUE) {
  cat_header("=== NAICS HIERARCHY CROSSWALK LOAD ===")
  start_timer("Load NAICS Hierarchy Crosswalk")
  
  url <- "https://www.bls.gov/cew/classifications/industry/qcew-naics-hierarchy-crosswalk.xlsx"
  tmp <- tempfile(fileext = ".xlsx")
  
  if (verbose) cat_info("Downloading BLS NAICS hierarchy crosswalk (.xlsx)...")
  
  # Try multiple download methods due to BLS bot protection
  download_success <- FALSE
  
  
  # Method 1: httr with full browser headers
  if (!download_success) {
    if (verbose) cat_info("Attempting download with httr (browser headers)...")
    resp <- tryCatch({
      httr::GET(
        url,
        httr::add_headers(
          `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          `Accept` = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*",
          `Accept-Language` = "en-US,en;q=0.9",
          `Accept-Encoding` = "gzip, deflate, br",
          `Connection` = "keep-alive",
          `Sec-Fetch-Dest` = "document",
          `Sec-Fetch-Mode` = "navigate",
          `Sec-Fetch-Site` = "none",
          `Sec-Fetch-User` = "?1",
          `Upgrade-Insecure-Requests` = "1"
        ),
        httr::write_disk(tmp, overwrite = TRUE),
        httr::timeout(60)
      )
    }, error = function(e) {
      if (verbose) cat_warn("httr method failed:", conditionMessage(e))
      NULL
    })
    
    if (!is.null(resp) && httr::status_code(resp) == 200 && file.exists(tmp) && file.size(tmp) > 1000) {
      download_success <- TRUE
      if (verbose) cat_success("Download successful via httr")
    }
  }
  
  # Method 2: curl with comprehensive headers
  if (!download_success) {
    if (verbose) cat_info("Attempting download with curl (comprehensive headers)...")
    h <- curl::new_handle()
    curl::handle_setheaders(h,
                            "User-Agent" = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                            "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                            "Accept-Language" = "en-US,en;q=0.9",
                            "Accept-Encoding" = "gzip, deflate, br",
                            "Connection" = "keep-alive",
                            "Sec-Fetch-Dest" = "document",
                            "Sec-Fetch-Mode" = "navigate",
                            "Sec-Fetch-Site" = "none"
    )
    curl::handle_setopt(h, followlocation = TRUE, ssl_verifypeer = TRUE)
    
    result <- tryCatch({
      curl::curl_download(url, tmp, mode = "wb", handle = h, quiet = !verbose)
      TRUE
    }, error = function(e) {
      if (verbose) cat_warn("curl method failed:", conditionMessage(e))
      FALSE
    })
    
    if (result && file.exists(tmp) && file.size(tmp) > 1000) {
      download_success <- TRUE
      if (verbose) cat_success("Download successful via curl")
    }
  }
  
  # Method 3: Base R download (sometimes works when others don't)
  if (!download_success) {
    if (verbose) cat_info("Attempting download with base R download.file()...")
    old_options <- options(timeout = 120)
    on.exit(options(old_options), add = TRUE)
    
    result <- tryCatch({
      download.file(url, tmp, mode = "wb", quiet = !verbose, 
                    headers = c("User-Agent" = "Mozilla/5.0 (compatible; R)"))
      TRUE
    }, error = function(e) {
      if (verbose) cat_warn("Base R method failed:", conditionMessage(e))
      FALSE
    })
    
    if (result && file.exists(tmp) && file.size(tmp) > 1000) {
      download_success <- TRUE
      if (verbose) cat_success("Download successful via base R")
    }
  }
  
  # If all methods failed, try to build from QCEW flat files as fallback
  if (!download_success) {
    cat_warn("All download methods failed for BLS crosswalk")
    cat_info("Building minimal NAICS crosswalk from hardcoded sector data...")
    
    # Fallback: Create minimal crosswalk with sector-level data
    # This covers the essential mappings needed for the analysis
    xwalk <- build_fallback_naics_crosswalk()
    
    if (verbose) {
      cat_success(sprintf("Built fallback crosswalk: %s rows × %d cols",
                          format(nrow(xwalk), big.mark = ","), ncol(xwalk)))
      cat("\n  Fallback crosswalk glimpse():\n")
      glimpse(xwalk)
    }
    
    stop_timer("Load NAICS Hierarchy Crosswalk")
    return(xwalk)
  }
  
  if (verbose) cat_info(sprintf("Downloaded file size: %s", format_bytes(file.size(tmp))))
  
  sheets <- c("v2002", "v2007", "v2012", "v2017", "v2022")
  if (verbose) cat_info("Reading sheets:", paste(sheets, collapse = ", "))
  
  xwalk <- purrr::map_dfr(
    sheets,
    \(s) readxl::read_excel(tmp, sheet = s) |>
      mutate(naics_year = sub("^v", "", s)) |>
      mutate(across(everything(), as.character))
  )
  
  if (verbose) {
    cat_success(sprintf("Loaded crosswalk: %s rows × %d cols",
                        format(nrow(xwalk), big.mark = ","), ncol(xwalk)))
    cat_info("Columns:", paste(names(xwalk), collapse = ", "))
    cat_info("naics_years present:", paste(unique(xwalk$naics_year), collapse = ", "))
    cat_info("Sample:")
    print(head(xwalk, 3))
    cat("\n  glimpse():\n")
    glimpse(xwalk)
  }
  
  stop_timer("Load NAICS Hierarchy Crosswalk")
  return(xwalk)
}

# Fallback function to build minimal NAICS crosswalk if BLS download fails
build_fallback_naics_crosswalk <- function() {
  # NAICS sector definitions (2-digit level)
  sectors <- tribble(
    ~sector_code, ~sector_title,
    "11", "Agriculture, Forestry, Fishing and Hunting",
    "21", "Mining, Quarrying, and Oil and Gas Extraction",
    "22", "Utilities",
    "23", "Construction",
    "31-33", "Manufacturing",
    "42", "Wholesale Trade",
    "44-45", "Retail Trade",
    "48-49", "Transportation and Warehousing",
    "51", "Information",
    "52", "Finance and Insurance",
    "53", "Real Estate and Rental and Leasing",
    "54", "Professional, Scientific, and Technical Services",
    "55", "Management of Companies and Enterprises",
    "56", "Administrative and Support and Waste Management and Remediation Services",
    "61", "Educational Services",
    "62", "Health Care and Social Assistance",
    "71", "Arts, Entertainment, and Recreation",
    "72", "Accommodation and Food Services",
    "81", "Other Services (except Public Administration)",
    "92", "Public Administration",
    "99", "Unclassified"
  )
  
  # Generate entries for all NAICS years
  naics_years <- c("2002", "2007", "2012", "2017", "2022")
  
  # Note: This is a minimal fallback - it won't have 6-digit detail
  # The script will still work but naics6_title will be NA for detailed codes
  xwalk <- expand_grid(
    naics_year = naics_years,
    sectors
  ) %>%
    mutate(
      naics6_code = NA_character_,
      naics6_title = NA_character_,
      naics5_code = NA_character_,
      naics5_title = NA_character_,
      naics4_code = NA_character_,
      naics4_title = NA_character_,
      naics3_code = NA_character_,
      naics3_title = NA_character_,
      supersector_code = NA_character_,
      supersector_title = NA_character_,
      domain_code = NA_character_,
      domain_title = NA_character_
    ) %>%
    select(
      naics6_code, naics6_title,
      naics5_code, naics5_title,
      naics4_code, naics4_title,
      naics3_code, naics3_title,
      sector_code, sector_title,
      supersector_code, supersector_title,
      domain_code, domain_title,
      naics_year
    )
  
  cat_warn("NOTE: Using fallback crosswalk - NAICS 6-digit titles will not be available")
  cat_info("The analysis will still work, but industry names will show as NA")
  cat_info("To get full NAICS titles, manually download the crosswalk from:")
  cat_info("  https://www.bls.gov/cew/classifications/industry/qcew-naics-hierarchy-crosswalk.xlsx")
  
  return(xwalk)
}

# ==============================================================================
# COUNTY / STATE / CBSA / CSA / CZ CROSSWALK (2024)
# ==============================================================================

clean_cz_name <- function(x) {
  x <- stringr::str_squish(as.character(x))
  m <- stringr::str_match(x, "^(.*),\\s*(.+)$")
  place <- ifelse(is.na(m[,2]), x, m[,2])
  st    <- ifelse(is.na(m[,3]), NA_character_, m[,3]) |>
    stringr::str_replace_all("--", "-") |>
    stringr::str_replace_all("\\s*-\\s*", "-") |>
    stringr::str_squish()
  
  place <- place |>
    stringr::str_replace(stringr::regex("\\s+(city and borough|consolidated government \\(balance\\)|city|town|village|CDP)\\s*$", TRUE), "") |>
    stringr::str_squish()
  
  stringr::str_squish(stringr::str_replace(ifelse(is.na(st), place, paste0(place, ", ", st)), ",\\s+", ", "))
}

build_county_state_cbsa_csa_cz_2024 <- function(verbose = TRUE) {
  cat_header("=== COUNTY/STATE/CBSA/CSA/CZ CROSSWALK BUILD (2024) ===")
  start_timer("Build County Crosswalk 2024")
  
  if (verbose) cat_info("Downloading ERS commuting zones (2020) CSV...")
  cz_url <- "https://www.ers.usda.gov/media/6968/2020-commuting-zones.csv?v=56155"
  cz <- readr::read_csv(cz_url, show_col_types = FALSE) %>%
    transmute(
      county_geoid = stringr::str_pad(as.character(FIPStxt), 5, pad = "0"),
      commuting_zone_geoid = as.character(CZ2020),
      commuting_zone_name = as.character(CZName)
    ) %>%
    mutate(
      commuting_zone_name_original = commuting_zone_name,
      commuting_zone_name = clean_cz_name(commuting_zone_name)
    )
  
  if (verbose) cat_success(sprintf("CZ rows: %s", format(nrow(cz), big.mark = ",")))
  if (verbose) {
    cat_info("CZ sample:")
    print(head(cz, 3))
    cat("\n  CZ glimpse():\n")
    glimpse(cz)
  }
  
  if (verbose) cat_info("Downloading TIGRIS counties/states/CBSA/CSA (2024)...")
  counties_2024 <- tigris::counties(year = 2024, cb = FALSE) %>%
    filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>%
    transmute(
      state_fips   = as.character(STATEFP),
      county_fips  = as.character(COUNTYFP),
      county_geoid = as.character(GEOID),
      county_name  = as.character(NAMELSAD),
      cbsa_geoid   = as.character(CBSAFP),
      csa_geoid    = as.character(CSAFP),
      geometry,
      county_in_cbsa = !is.na(CBSAFP) & CBSAFP != "",
      county_in_csa  = !is.na(CSAFP) & CSAFP != ""
    )
  
  if (verbose) {
    cat_success(sprintf("TIGRIS counties: %s rows", format(nrow(counties_2024), big.mark = ",")))
    cat("\n  Counties glimpse():\n")
    glimpse(st_set_geometry(counties_2024, NULL))
  }
  
  states_2024 <- tigris::states(year = 2024, cb = FALSE) %>%
    st_set_geometry(NULL) %>%
    transmute(
      state_fips = as.character(STATEFP),
      state_abbreviation = as.character(STUSPS),
      state_name = as.character(NAME)
    )
  
  if (verbose) {
    cat_success(sprintf("TIGRIS states: %s rows", format(nrow(states_2024), big.mark = ",")))
    cat("\n  States glimpse():\n")
    glimpse(states_2024)
  }
  
  cbsa_2024 <- tigris::core_based_statistical_areas(year = 2024, cb = FALSE) %>%
    st_set_geometry(NULL) %>%
    transmute(
      cbsa_geoid = as.character(CBSAFP),
      cbsa_name  = as.character(NAMELSAD)
    )
  
  if (verbose) {
    cat_success(sprintf("TIGRIS CBSAs: %s rows", format(nrow(cbsa_2024), big.mark = ",")))
    cat("\n  CBSA glimpse():\n")
    glimpse(cbsa_2024)
  }
  
  csa_2024 <- tigris::combined_statistical_areas(year = 2024, cb = FALSE) %>%
    st_set_geometry(NULL) %>%
    transmute(
      csa_geoid = as.character(CSAFP),
      csa_name  = as.character(NAMELSAD)
    )
  
  if (verbose) {
    cat_success(sprintf("TIGRIS CSAs: %s rows", format(nrow(csa_2024), big.mark = ",")))
    cat("\n  CSA glimpse():\n")
    glimpse(csa_2024)
  }
  
  xwalk <- counties_2024 %>%
    left_join(states_2024, by = "state_fips") %>%
    left_join(cbsa_2024, by = "cbsa_geoid") %>%
    left_join(csa_2024,  by = "csa_geoid") %>%
    left_join(cz %>% select(county_geoid, commuting_zone_geoid, commuting_zone_name), by = "county_geoid") %>%
    select(
      state_fips, state_name, state_abbreviation,
      county_fips, county_geoid, county_name,
      cbsa_geoid, cbsa_name, county_in_cbsa,
      csa_geoid, csa_name, county_in_csa,
      commuting_zone_geoid, commuting_zone_name,
      geometry
    )
  
  if (verbose) {
    cat_success(sprintf("County crosswalk rows: %s × %d",
                        format(nrow(xwalk), big.mark = ","), ncol(xwalk)))
    cat_info("Sample:")
    print(head(as.data.frame(st_set_geometry(xwalk, NULL)), 3))
    cat("\n  glimpse():\n")
    glimpse(st_set_geometry(xwalk, NULL))
    cat_warn("NOTE: Placeholder counties like '01999' will NOT be present (expected).")
  }
  
  stop_timer("Build County Crosswalk 2024")
  return(xwalk)
}

# ==============================================================================
# CREDENTIALS
# ==============================================================================

get_tapestry_credentials <- function() {
  service <- "tapestry-uidaho"
  have_keyring <- requireNamespace("keyring", quietly = TRUE)
  
  if (have_keyring) {
    kr <- tryCatch(keyring::key_list(service = service), error = function(e) data.frame())
    if (nrow(kr) > 0) {
      email <- kr$username[1]
      cat_info("Found stored credentials in keyring for:", email)
      return(list(email = email, password = keyring::key_get(service, email)))
    }
  }
  
  cat("\n")
  email <- readline("Tapestry email (or 'exit' to quit): ")
  if (tolower(trimws(email)) == "exit") stop("Script terminated by user.", call. = FALSE)
  
  if (requireNamespace("askpass", quietly = TRUE)) {
    password <- askpass::askpass("Tapestry password: ")
  } else {
    password <- readline("Tapestry password: ")
  }
  
  if (have_keyring) {
    try(keyring::key_set_with_value(service, email, password), silent = TRUE)
    cat_info("Credentials stored in keyring for future use.")
  }
  return(list(email = email, password = password))
}

# ==============================================================================
# AUTHENTICATION
# ==============================================================================

tapestry <- handle(BASE_URL)

login_tapestry <- function(email, password) {
  cat_header("=== AUTHENTICATION ===")
  start_timer("Authentication")
  
  cat_info("Fetching login page for session cookies...")
  login_page <- tryCatch({
    GET(handle = tapestry, path = "/login")
  }, error = function(e) {
    cat_error("Failed to reach login page:", conditionMessage(e))
    return(NULL)
  })
  
  if (is.null(login_page)) {
    stop_timer("Authentication")
    return(FALSE)
  }
  
  debug_response(login_page, "Login page")
  
  cat_info("Submitting credentials...")
  resp <- tryCatch({
    POST(
      handle = tapestry,
      path   = "/login",
      body   = list(email = email, password = password),
      encode = "form"
    )
  }, error = function(e) {
    cat_error("Failed to submit login:", conditionMessage(e))
    return(NULL)
  })
  
  if (is.null(resp)) {
    stop_timer("Authentication")
    return(FALSE)
  }
  
  debug_response(resp, "Login POST")
  
  cat_info("Verifying authentication...")
  check <- tryCatch({
    GET(handle = tapestry, path = "/data")
  }, error = function(e) {
    cat_error("Failed to verify login:", conditionMessage(e))
    return(NULL)
  })
  
  if (is.null(check)) {
    stop_timer("Authentication")
    return(FALSE)
  }
  
  if (status_code(check) == 200 && !grepl("Login", safe_content_text(check))) {
    cat_success("Login Successful!")
    stop_timer("Authentication")
    return(TRUE)
  } else {
    cat_error("Login verification failed - check credentials")
    stop_timer("Authentication")
    return(FALSE)
  }
}

# ==============================================================================
# PHASE 1: ENDPOINT DISCOVERY (SIMPLIFIED FOR v5)
# ==============================================================================

discover_endpoints <- function() {
  cat_header("=== ENDPOINT DISCOVERY ===")
  start_timer("Endpoint Discovery")
  
  backend_info <- list()
  
  cat_info("Fetching /data page...")
  resp <- GET(handle = tapestry, path = "/data")
  html_content <- safe_content_text(resp)
  cat_info("Retrieved /data page (", nchar(html_content), " bytes)")
  
  all_headers <- headers(resp)
  if (!is.null(all_headers[["server"]])) {
    backend_info$server <- all_headers[["server"]]
    cat_info("Server:", backend_info$server)
  }
  
  cat_subhead("Backend Architecture Summary:")
  cat("\n")
  cat("  ┌─────────────────────────────────────────────────────────────────┐\n")
  cat("  │                    BACKEND ARCHITECTURE                         │\n")
  cat("  ├─────────────────────────────────────────────────────────────────┤\n")
  cat(sprintf("  │  Web Server: %-50s │\n", backend_info$server %||% "Unknown"))
  cat("  │  API Style:  REST-like JSON endpoints                           │\n")
  cat("  │  Data Flow:  POST to /download-data → ZIP containing CSV        │\n")
  cat("  └─────────────────────────────────────────────────────────────────┘\n\n")
  
  stop_timer("Endpoint Discovery")
  return(list(backend_info = backend_info))
}

# ==============================================================================
# PHASE 2: DYNAMIC OPTION DISCOVERY
# ==============================================================================

get_available_years <- function() {
  cat_info("Discovering available years...")
  resp <- GET(handle = tapestry, path = "/data")
  txt <- safe_content_text(resp)
  
  matches <- regmatches(txt, gregexpr('value="([0-9]{4})"', txt))[[1]]
  years <- sort(unique(as.integer(sub('value="([0-9]{4})"', "\\1", matches))), decreasing = TRUE)
  years <- years[years >= 2000 & years <= as.integer(format(Sys.Date(), "%Y"))]
  
  if (length(years) == 0) {
    cat_warn("Could not parse years. Using defaults 2024:2001...")
    return(2024:2001)
  }
  
  cat_success("Found", length(years), "years:", paste(head(years, 3), collapse = ", "), "...", tail(years, 1))
  return(years)
}

get_ownership_codes <- function(year = NULL, area_fips = "All") {
  cat_info("Discovering ownership codes...")
  
  query_params <- list(area_fips = area_fips)
  if (!is.null(year)) query_params$year <- as.character(year)
  
  resp <- GET(handle = tapestry, path = "/ownership-options", query = query_params)
  
  if (status_code(resp) == 200) {
    json <- tryCatch(fromJSON(safe_content_text(resp)), error = function(e) NULL)
    if (!is.null(json)) {
      if (is.data.frame(json) && "own_code" %in% names(json)) {
        codes <- as.character(json$own_code)
      } else if (is.list(json)) {
        codes <- as.character(unlist(json))
      } else {
        codes <- as.character(json)
      }
      codes <- unique(c("0", codes))
      cat_success("Found ownership codes:", paste(codes, collapse = ", "))
      return(codes)
    }
  }
  
  cat_warn("Using default ownership codes: 0, 1, 2, 3, 5")
  return(c("0", "1", "2", "3", "5"))
}

get_sector_conversions <- function() {
  list(
    values = c("no_conversion", "bea_conversion", "naics_3_conversion", 
               "naics_4_conversion", "naics_5_conversion"),
    labels = c(
      "no_conversion"       = "NAICS 6-digit (Standard)",
      "bea_conversion"      = "BEA 409 Sector Conversion",
      "naics_3_conversion"  = "NAICS 3-digit Aggregation",
      "naics_4_conversion"  = "NAICS 4-digit Aggregation",
      "naics_5_conversion"  = "NAICS 5-digit Aggregation"
    )
  )
}

# ==============================================================================
# PHASE 3: DATA DOWNLOAD WITH RETRY LOGIC
# ==============================================================================

qcew_col_types <- cols(
  own_code         = col_character(),
  year             = col_integer(),
  area_fips        = col_character(),
  naics_code       = col_character(),
  tap_estabs_count = col_double(),
  .default         = col_double()
)

download_single_dataset <- function(year, wage_rule, ownership, conversion,
                                    area_fips = "All", industry = "All",
                                    verbose = TRUE, show_progress = TRUE,
                                    max_retries = MAX_RETRIES) {
  
  desc_str <- sprintf("Y:%s | W:%s | O:%s | Conv:%s", year, wage_rule, ownership, conversion)
  if (verbose) cat_info("Downloading:", desc_str)
  
  payload <- list(
    area_fips         = list(area_fips),
    own_code          = as.character(ownership),
    industry_code     = list(industry),
    emp_wage_number   = as.character(wage_rule),
    fips_calc         = "fipsInd",
    naics_calc        = "naicsInd",
    year              = as.character(year),
    sector_conversion = conversion
  )
  
  if (verbose) {
    cat_debug("Payload:", toJSON(payload, auto_unbox = TRUE))
  }
  
  for (attempt in 1:max_retries) {
    if (attempt > 1) {
      delay <- RETRY_DELAY_BASE * (2 ^ (attempt - 2))
      cat_warn(sprintf("Retry %d/%d in %d seconds...", attempt, max_retries, delay))
      Sys.sleep(delay)
    }
    
    tmp_zip <- tempfile(fileext = ".zip")
    start_time <- Sys.time()
    
    if (verbose && show_progress) {
      cat_info("Sending request to server (attempt", attempt, ")...")
    }
    
    resp <- tryCatch({
      if (show_progress) {
        POST(
          url = paste0(BASE_URL, "/download-data"),
          handle = tapestry,
          body = payload,
          encode = "json",
          add_headers(
            Accept = "application/json",
            `X-Requested-With` = "XMLHttpRequest"
          ),
          write_disk(tmp_zip, overwrite = TRUE),
          progress(),
          timeout(600)
        )
      } else {
        POST(
          url = paste0(BASE_URL, "/download-data"),
          handle = tapestry,
          body = payload,
          encode = "json",
          add_headers(
            Accept = "application/json",
            `X-Requested-With` = "XMLHttpRequest"
          ),
          write_disk(tmp_zip, overwrite = TRUE),
          timeout(600)
        )
      }
    }, error = function(e) {
      if (verbose) cat_error("Request failed:", conditionMessage(e))
      return(NULL)
    })
    
    download_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    
    if (is.null(resp)) {
      if (file.exists(tmp_zip)) unlink(tmp_zip)
      next
    }
    
    if (verbose) {
      cat_debug(sprintf("Download completed in %.2f seconds", download_time))
      debug_response(resp, "Response")
    }
    
    if (status_code(resp) >= 500) {
      if (verbose) cat_error(sprintf("Server error HTTP %d - will retry", status_code(resp)))
      if (file.exists(tmp_zip)) unlink(tmp_zip)
      next
    }
    
    if (status_code(resp) != 200) {
      if (verbose) cat_error("HTTP", status_code(resp), "- not retrying")
      if (file.exists(tmp_zip)) unlink(tmp_zip)
      return(NULL)
    }
    
    ct <- headers(resp)[["content-type"]]
    if (is.null(ct) || !grepl("zip", ct, ignore.case = TRUE)) {
      if (verbose) cat_error("Not a ZIP response - not retrying")
      if (file.exists(tmp_zip)) unlink(tmp_zip)
      return(NULL)
    }
    
    zip_size <- file.size(tmp_zip)
    if (verbose) cat_debug("ZIP size:", format_bytes(zip_size))
    
    exdir <- tempfile()
    dir.create(exdir)
    files <- unzip(tmp_zip, exdir = exdir)
    
    csvs <- files[grepl("\\.csv$", files, ignore.case = TRUE)]
    
    if (length(csvs) == 0) {
      if (verbose) cat_warn("No CSVs in ZIP - will retry")
      unlink(tmp_zip)
      unlink(exdir, recursive = TRUE)
      next
    }
    
    target_csv <- csvs[1]
    if (verbose) cat_info("Reading CSV:", basename(target_csv))
    
    read_start <- Sys.time()
    df <- tryCatch(
      readr::read_csv(
        target_csv,
        show_col_types = FALSE,
        progress = FALSE,
        col_types = qcew_col_types
      ),
      error = function(e) {
        if (verbose) cat_error("CSV read error:", conditionMessage(e))
        return(NULL)
      }
    )
    read_time <- as.numeric(difftime(Sys.time(), read_start, units = "secs"))
    
    unlink(tmp_zip)
    unlink(exdir, recursive = TRUE)
    
    if (!is.null(df) && verbose) {
      cat_success(sprintf("Got %s rows × %d cols (download: %.1fs, parse: %.1fs)",
                          format_number(nrow(df)), ncol(df), download_time, read_time))
    }
    
    return(df)
  }
  
  cat_error(sprintf("Failed after %d attempts", max_retries))
  return(NULL)
}

# ==============================================================================
# PHASE 4: DATA STANDARDIZATION
# ==============================================================================

standardize_tapestry_data <- function(df, data_year, verbose = TRUE) {
  if (is.null(df) || nrow(df) == 0) return(df)
  
  if (verbose) cat_info("Standardizing Tapestry data...")
  
  orig_cols <- ncol(df)
  
  df_std <- df %>%
    mutate(
      county_geoid = stringr::str_pad(as.character(area_fips), 5, side = "left", pad = "0"),
      state_fips = substr(county_geoid, 1, 2),
      county_fips = substr(county_geoid, 3, 5),
      unknown_undefined_county = grepl("999$", county_geoid),
      naics_code = as.character(naics_code),
      naics_year = naics_year_for_data_year(as.integer(data_year))
    ) %>%
    select(-area_fips) %>%
    select(
      own_code, year,
      county_geoid, state_fips, county_fips, unknown_undefined_county,
      naics_code, naics_year,
      everything()
    )
  
  if (verbose) {
    cat_success(sprintf("Standardized: %d → %d columns", orig_cols, ncol(df_std)))
    n_unknown <- sum(df_std$unknown_undefined_county)
    cat_info(sprintf("Unknown/undefined county rows (XX999): %s (%.2f%%)",
                     format_number(n_unknown),
                     100 * n_unknown / nrow(df_std)))
  }
  
  return(df_std)
}

# ==============================================================================
# PHASE 5: DATA LABELING
# ==============================================================================

label_tapestry_data <- function(df, naics_xwalk, county_xwalk_2024, verbose = TRUE) {
  if (is.null(df) || nrow(df) == 0) return(df)
  
  if (verbose) cat_info("Applying labels (NAICS titles + county/state/CBSA/CSA/CZ)...")
  
  orig_cols <- ncol(df)
  
  x_naics <- naics_xwalk %>%
    select(
      naics_year, 
      naics6_code, naics6_title,
      naics5_code, naics5_title,
      naics4_code, naics4_title,
      naics3_code, naics3_title,
      sector_code, sector_title,
      supersector_code, supersector_title,
      domain_code, domain_title
    ) %>%
    distinct()
  
  x_county <- county_xwalk_2024 %>%
    st_set_geometry(NULL) %>%
    select(
      county_geoid,
      state_name, state_abbreviation,
      county_name,
      cbsa_geoid, cbsa_name, county_in_cbsa,
      csa_geoid, csa_name, county_in_csa,
      commuting_zone_geoid, commuting_zone_name
    ) %>%
    distinct()
  
  df_labeled <- df %>%
    left_join(
      x_naics,
      by = c("naics_year" = "naics_year", "naics_code" = "naics6_code")
    )
  
  df_labeled <- df_labeled %>%
    left_join(x_county, by = "county_geoid")
  
  if (verbose) {
    cat_success(sprintf("Labeled: %d → %d columns", orig_cols, ncol(df_labeled)))
    naics_coverage <- sum(!is.na(df_labeled$naics6_title))
    county_coverage <- sum(!is.na(df_labeled$county_name))
    cat_info(sprintf("NAICS title coverage: %s / %s (%.1f%%)",
                     format_number(naics_coverage),
                     format_number(nrow(df_labeled)),
                     100 * naics_coverage / nrow(df_labeled)))
    cat_info(sprintf("County name coverage: %s / %s (%.1f%%)",
                     format_number(county_coverage),
                     format_number(nrow(df_labeled)),
                     100 * county_coverage / nrow(df_labeled)))
  }
  
  return(df_labeled)
}

process_tapestry_data <- function(df, data_year, naics_xwalk, county_xwalk_2024, verbose = TRUE) {
  if (is.null(df) || nrow(df) == 0) return(df)
  df_std <- standardize_tapestry_data(df, data_year, verbose = verbose)
  df_labeled <- label_tapestry_data(df_std, naics_xwalk, county_xwalk_2024, verbose = verbose)
  return(df_labeled)
}

# ==============================================================================
# PHASE 6: INSPECTION & ANALYSIS UTILITIES
# ==============================================================================

inspect_dataset <- function(df, rule_name) {
  cat("\n")
  cat_subhead(sprintf("INSPECTION: %s", rule_name))
  cat(sprintf("  Dimensions: %s rows × %d cols\n",
              format_number(nrow(df)), ncol(df)))
  cat("  Columns:", paste(colnames(df), collapse = ", "), "\n")
  cat("  Memory:", format(object.size(df), units = "auto"), "\n")
  
  if ("unknown_undefined_county" %in% names(df)) {
    n_unknown <- sum(df$unknown_undefined_county, na.rm = TRUE)
    cat(sprintf("  Unknown/undefined counties (XX999): %s (%.2f%%)\n",
                format_number(n_unknown), 100 * n_unknown / nrow(df)))
  }
  
  if ("naics_code" %in% names(df)) {
    naics_999999 <- sum(df$naics_code == "999999", na.rm = TRUE)
    cat(sprintf("  NAICS '999999' rows: %s (%.2f%%)\n",
                format_number(naics_999999), 100 * naics_999999 / nrow(df)))
  }
  
  # v5: Add glimpse()
  cat("\n  glimpse():\n")
  glimpse(df)
}

check_key_identity <- function(df_a, df_b, name_a, name_b,
                               key_cols = c("own_code", "year", "county_geoid", "naics_code")) {
  cat_subhead(sprintf("KEY IDENTITY CHECK: %s vs %s", name_a, name_b))
  
  missing_a <- setdiff(key_cols, names(df_a))
  missing_b <- setdiff(key_cols, names(df_b))
  if (length(missing_a) > 0 || length(missing_b) > 0) {
    cat_warn("Missing key columns; cannot compare keys.")
    return(invisible(NULL))
  }
  
  make_key_vec <- function(df) {
    paste(df[[key_cols[1]]], df[[key_cols[2]]], df[[key_cols[3]]], df[[key_cols[4]]], sep = "|")
  }
  
  ka <- make_key_vec(df_a)
  kb <- make_key_vec(df_b)
  
  cat_info(sprintf("  %s unique keys: %s", name_a, format_number(length(unique(ka)))))
  cat_info(sprintf("  %s unique keys: %s", name_b, format_number(length(unique(kb)))))
  
  if (has_digest) {
    ha <- digest::digest(sort(unique(ka)), algo = "xxhash64")
    hb <- digest::digest(sort(unique(kb)), algo = "xxhash64")
    cat_info(sprintf("  Keyset hash %s: %s", name_a, ha))
    cat_info(sprintf("  Keyset hash %s: %s", name_b, hb))
    if (identical(ha, hb)) {
      cat_success("  Key sets match (hash equal).")
    } else {
      cat_warn("  Key sets differ (hash mismatch).")
    }
  }
}

compute_rule_deltas <- function(df3, df4, df5) {
  cat_subhead("COMPUTING VALUE DELTAS (Rule4-Rule3, Rule5-Rule3)")
  
  wage3 <- grep("^tap_wages_est_3$", names(df3), value = TRUE)
  emp3  <- grep("^tap_emplvl_est_3$", names(df3), value = TRUE)
  wage4 <- grep("^tap_wages_est_4$", names(df4), value = TRUE)
  emp4  <- grep("^tap_emplvl_est_4$", names(df4), value = TRUE)
  wage5 <- grep("^tap_wages_est_5$", names(df5), value = TRUE)
  emp5  <- grep("^tap_emplvl_est_5$", names(df5), value = TRUE)
  
  if (length(wage3) == 0 || length(wage4) == 0 || length(wage5) == 0) {
    cat_error("Could not find wage columns for all rules.")
    return(NULL)
  }
  if (length(emp3) == 0 || length(emp4) == 0 || length(emp5) == 0) {
    cat_error("Could not find employment columns for all rules.")
    return(NULL)
  }
  
  key_cols <- c("own_code", "year", "county_geoid", "naics_code")
  
  base <- df3 %>%
    select(
      all_of(key_cols),
      state_fips, county_fips, unknown_undefined_county, naics_year,
      tap_estabs_count,
      naics6_title, sector_code, sector_title, naics3_code, naics3_title,
      state_abbreviation, state_name, county_name,
      cbsa_geoid, cbsa_name, csa_geoid, csa_name,
      commuting_zone_geoid, commuting_zone_name,
      wages_3 = all_of(wage3), empl_3 = all_of(emp3)
    )
  
  x4 <- df4 %>%
    select(all_of(key_cols), wages_4 = all_of(wage4), empl_4 = all_of(emp4))
  
  x5 <- df5 %>%
    select(all_of(key_cols), wages_5 = all_of(wage5), empl_5 = all_of(emp5))
  
  out <- base %>%
    left_join(x4, by = key_cols) %>%
    left_join(x5, by = key_cols) %>%
    mutate(
      d_wages_4_3 = wages_4 - wages_3,
      d_empl_4_3  = empl_4  - empl_3,
      d_wages_5_3 = wages_5 - wages_3,
      d_empl_5_3  = empl_5  - empl_3,
      is_naics_999999 = (naics_code == "999999"),
      is_any_placeholder = unknown_undefined_county | is_naics_999999
    )
  
  cat_success("Delta frame created:", format_number(nrow(out)), "rows")
  return(out)
}

summarize_allocation_balance <- function(delta_df) {
  cat_subhead("ALLOCATION BALANCE CHECKS (should net to ~0)")
  
  overall4 <- sum(delta_df$d_wages_4_3, na.rm = TRUE)
  overall5 <- sum(delta_df$d_wages_5_3, na.rm = TRUE)
  
  cat_info(sprintf("  Sum(d_wages_4_3) overall: %s", format_number(overall4)))
  cat_info(sprintf("  Sum(d_wages_5_3) overall: %s", format_number(overall5)))
  
  p4 <- delta_df %>% summarise(
    placeholder = sum(d_wages_4_3[is_any_placeholder], na.rm = TRUE),
    non_placeholder = sum(d_wages_4_3[!is_any_placeholder], na.rm = TRUE)
  )
  p5 <- delta_df %>% summarise(
    placeholder = sum(d_wages_5_3[is_any_placeholder], na.rm = TRUE),
    non_placeholder = sum(d_wages_5_3[!is_any_placeholder], na.rm = TRUE)
  )
  
  cat("\n  Rule 4 partition (wage deltas):\n")
  print(p4)
  
  cat("\n  Rule 5 partition (wage deltas):\n")
  print(p5)
}

analyze_placeholder_values <- function(results) {
  cat_header("=== PLACEHOLDER VALUE ANALYSIS ===")
  
  available_rules <- names(results)
  n_rules <- length(available_rules)
  
  if (n_rules < 2) {
    cat_warn("Need at least 2 wage rules for comparative analysis")
    return(NULL)
  }
  
  get_wage_col <- function(df) grep("tap_wages_est", names(df), value = TRUE)[1]
  get_empl_col <- function(df) grep("tap_emplvl_est", names(df), value = TRUE)[1]
  
  df_first <- results[[available_rules[1]]]
  
  # 1. Unknown county values
  cat_subhead("\n1. VALUES IN UNKNOWN/UNDEFINED COUNTY ROWS (XX999)")
  
  unknown_county_mask <- df_first$unknown_undefined_county
  n_unknown_county <- sum(unknown_county_mask, na.rm = TRUE)
  cat(sprintf("   Total unknown/undefined county rows: %s\n", format_number(n_unknown_county)))
  
  fips_summary <- data.frame(
    Rule = character(), Total_Wages = numeric(), Total_Employment = numeric(),
    NonZero_Wages = integer(), NonZero_Empl = integer(), stringsAsFactors = FALSE
  )
  
  for (rule_name in available_rules) {
    df <- results[[rule_name]]
    wage_col <- get_wage_col(df)
    empl_col <- get_empl_col(df)
    
    fips_summary <- rbind(fips_summary, data.frame(
      Rule = rule_name,
      Total_Wages = sum(df[[wage_col]][unknown_county_mask], na.rm = TRUE),
      Total_Employment = sum(df[[empl_col]][unknown_county_mask], na.rm = TRUE),
      NonZero_Wages = sum(df[[wage_col]][unknown_county_mask] != 0, na.rm = TRUE),
      NonZero_Empl = sum(df[[empl_col]][unknown_county_mask] != 0, na.rm = TRUE),
      stringsAsFactors = FALSE
    ))
  }
  
  cat("\n")
  print(fips_summary)
  
  # 2. NAICS 999999 values
  cat_subhead("\n2. VALUES IN NAICS '999999' ROWS (Unclassified Industry)")
  
  naics_999999_mask <- df_first$naics_code == "999999"
  n_naics_999999 <- sum(naics_999999_mask, na.rm = TRUE)
  cat(sprintf("   Total NAICS '999999' rows: %s\n", format_number(n_naics_999999)))
  
  naics_summary <- data.frame(
    Rule = character(), Total_Wages = numeric(), Total_Employment = numeric(),
    NonZero_Wages = integer(), NonZero_Empl = integer(), stringsAsFactors = FALSE
  )
  
  for (rule_name in available_rules) {
    df <- results[[rule_name]]
    wage_col <- get_wage_col(df)
    empl_col <- get_empl_col(df)
    
    naics_summary <- rbind(naics_summary, data.frame(
      Rule = rule_name,
      Total_Wages = sum(df[[wage_col]][naics_999999_mask], na.rm = TRUE),
      Total_Employment = sum(df[[empl_col]][naics_999999_mask], na.rm = TRUE),
      NonZero_Wages = sum(df[[wage_col]][naics_999999_mask] != 0, na.rm = TRUE),
      NonZero_Empl = sum(df[[empl_col]][naics_999999_mask] != 0, na.rm = TRUE),
      stringsAsFactors = FALSE
    ))
  }
  
  cat("\n")
  print(naics_summary)
  
  # 3. Total preservation
  cat_subhead("\n3. TOTAL VALUE PRESERVATION CHECK")
  
  total_wages <- data.frame(
    Rule = character(), Total_Wages = numeric(), Total_Employment = numeric(),
    Pct_Diff = numeric(), stringsAsFactors = FALSE
  )
  
  baseline_wages <- NULL
  for (rule_name in available_rules) {
    df <- results[[rule_name]]
    wage_col <- get_wage_col(df)
    empl_col <- get_empl_col(df)
    
    tw <- sum(df[[wage_col]], na.rm = TRUE)
    te <- sum(df[[empl_col]], na.rm = TRUE)
    
    if (is.null(baseline_wages)) baseline_wages <- tw
    
    total_wages <- rbind(total_wages, data.frame(
      Rule = rule_name, Total_Wages = tw, Total_Employment = te,
      Pct_Diff = 100 * (tw / baseline_wages - 1), stringsAsFactors = FALSE
    ))
  }
  
  cat("\n")
  print(total_wages)
  
  if (all(abs(total_wages$Pct_Diff) < 0.001)) {
    cat_success("\n   Total wages preserved across rules (< 0.001% difference)")
  }
  
  # 4. Residual detection
  cat_subhead("\n4. RESIDUAL PLACEHOLDER VALUE DETECTION")
  
  both_placeholder_mask <- unknown_county_mask | naics_999999_mask
  cat(sprintf("\n   Rows that are EITHER unknown county OR NAICS '999999': %s\n",
              format_number(sum(both_placeholder_mask, na.rm = TRUE))))
  
  for (rule_name in available_rules) {
    df <- results[[rule_name]]
    wage_col <- get_wage_col(df)
    empl_col <- get_empl_col(df)
    
    nonzero_wages <- sum(df[[wage_col]][both_placeholder_mask] != 0, na.rm = TRUE)
    nonzero_empl <- sum(df[[empl_col]][both_placeholder_mask] != 0, na.rm = TRUE)
    
    cat(sprintf("     %s: %s rows with non-zero wages, %s with non-zero employment\n",
                rule_name, format_number(nonzero_wages), format_number(nonzero_empl)))
  }
  
  # Deep residual investigation
  df_final <- results[[available_rules[length(available_rules)]]]
  wage_col_final <- get_wage_col(df_final)
  
  fips_999_residual_mask <- df_final$unknown_undefined_county & (df_final[[wage_col_final]] != 0)
  fips_999_residuals <- df_final[fips_999_residual_mask, ]
  
  state_summary <- NULL
  industry_summary <- NULL
  
  if (nrow(fips_999_residuals) > 0) {
    state_summary <- fips_999_residuals %>%
      group_by(state_fips) %>%
      summarise(
        n_residual_rows = n(),
        total_residual_wages = sum(.data[[wage_col_final]], na.rm = TRUE),
        unique_industries = n_distinct(naics_code),
        .groups = "drop"
      ) %>%
      arrange(desc(total_residual_wages))
    
    industry_summary <- fips_999_residuals %>%
      group_by(naics_code, naics6_title) %>%
      summarise(
        n_states = n_distinct(state_fips),
        total_wages = sum(.data[[wage_col_final]], na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(desc(total_wages))
  }
  
  naics_999999_residual_mask <- (df_final$naics_code == "999999") & (df_final[[wage_col_final]] != 0)
  naics_999999_residuals <- df_final[naics_999999_residual_mask, ]
  
  return(list(
    fips_999_summary = fips_summary,
    naics_999999_summary = naics_summary,
    total_wages = total_wages,
    fips_999_residuals = if (nrow(fips_999_residuals) > 0) fips_999_residuals else NULL,
    naics_999999_residuals = if (nrow(naics_999999_residuals) > 0) naics_999999_residuals else NULL,
    state_summary = state_summary,
    industry_summary = industry_summary
  ))
}

# ==============================================================================
# PHASE 7: LOCATION QUOTIENT ANALYSIS (v5 NEW)
# ==============================================================================

compute_location_quotients <- function(delta_df, verbose = TRUE) {
  cat_header("=== LOCATION QUOTIENT ANALYSIS ===")
  start_timer("Location Quotient Analysis")
  
  if (verbose) cat_info("Computing location quotients for each wage rule...")
  
  # Filter to real counties and real industries only
  real_data <- delta_df %>%
    filter(!unknown_undefined_county) %>%
    filter(naics_code != "999999")
  
  if (verbose) {
    cat_info(sprintf("Filtered to real counties and industries: %s rows",
                     format_number(nrow(real_data))))
  }
  
  # Compute national totals for each rule
  national_totals <- real_data %>%
    summarise(
      total_empl_3 = sum(empl_3, na.rm = TRUE),
      total_empl_4 = sum(empl_4, na.rm = TRUE),
      total_empl_5 = sum(empl_5, na.rm = TRUE)
    )
  
  if (verbose) {
    cat_info(sprintf("National employment totals: Rule3=%s, Rule4=%s, Rule5=%s",
                     format_number(national_totals$total_empl_3),
                     format_number(national_totals$total_empl_4),
                     format_number(national_totals$total_empl_5)))
  }
  
  # Compute national industry totals for each rule
  national_industry <- real_data %>%
    group_by(naics_code) %>%
    summarise(
      industry_empl_national_3 = sum(empl_3, na.rm = TRUE),
      industry_empl_national_4 = sum(empl_4, na.rm = TRUE),
      industry_empl_national_5 = sum(empl_5, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Compute county totals for each rule
  county_totals <- real_data %>%
    group_by(county_geoid) %>%
    summarise(
      county_total_empl_3 = sum(empl_3, na.rm = TRUE),
      county_total_empl_4 = sum(empl_4, na.rm = TRUE),
      county_total_empl_5 = sum(empl_5, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Join everything together
  lq_data <- real_data %>%
    select(
      county_geoid, state_fips, county_fips,
      naics_code, naics_year,
      state_name, state_abbreviation, county_name,
      naics6_title, sector_code, sector_title,
      empl_3, empl_4, empl_5
    ) %>%
    left_join(county_totals, by = "county_geoid") %>%
    left_join(national_industry, by = "naics_code") %>%
    mutate(
      # Add national totals
      national_total_3 = national_totals$total_empl_3,
      national_total_4 = national_totals$total_empl_4,
      national_total_5 = national_totals$total_empl_5
    )
  
  # Compute location quotients
  # LQ = (industry emp in county / total emp in county) / (industry emp in nation / total emp in nation)
  lq_data <- lq_data %>%
    mutate(
      # County share of industry
      county_industry_share_3 = ifelse(county_total_empl_3 > 0, empl_3 / county_total_empl_3, 0),
      county_industry_share_4 = ifelse(county_total_empl_4 > 0, empl_4 / county_total_empl_4, 0),
      county_industry_share_5 = ifelse(county_total_empl_5 > 0, empl_5 / county_total_empl_5, 0),
      
      # National share of industry
      national_industry_share_3 = ifelse(national_total_3 > 0, industry_empl_national_3 / national_total_3, 0),
      national_industry_share_4 = ifelse(national_total_4 > 0, industry_empl_national_4 / national_total_4, 0),
      national_industry_share_5 = ifelse(national_total_5 > 0, industry_empl_national_5 / national_total_5, 0),
      
      # Location quotients
      lq_3 = ifelse(national_industry_share_3 > 0, county_industry_share_3 / national_industry_share_3, 0),
      lq_4 = ifelse(national_industry_share_4 > 0, county_industry_share_4 / national_industry_share_4, 0),
      lq_5 = ifelse(national_industry_share_5 > 0, county_industry_share_5 / national_industry_share_5, 0),
      
      # Industry presence (LQ > 0)
      industry_presence_rule_3 = lq_3 > 0,
      industry_presence_rule_4 = lq_4 > 0,
      industry_presence_rule_5 = lq_5 > 0,
      
      # Comparative advantage (LQ >= 1)
      comparative_advantage_rule_3 = lq_3 >= 1,
      comparative_advantage_rule_4 = lq_4 >= 1,
      comparative_advantage_rule_5 = lq_5 >= 1
    )
  
  if (verbose) {
    cat_success(sprintf("Location quotients computed for %s county×industry combinations",
                        format_number(nrow(lq_data))))
  }
  
  # Find discrepancies in comparative advantage
  cat_subhead("\nFinding comparative advantage discrepancies between rules...")
  
  discrepancies <- lq_data %>%
    filter(
      (comparative_advantage_rule_3 != comparative_advantage_rule_4) |
        (comparative_advantage_rule_3 != comparative_advantage_rule_5) |
        (comparative_advantage_rule_4 != comparative_advantage_rule_5)
    ) %>%
    select(
      county_geoid, county_name, state_name, state_abbreviation,
      naics_code, naics6_title, sector_title,
      empl_3, empl_4, empl_5,
      lq_3, lq_4, lq_5,
      comparative_advantage_rule_3,
      comparative_advantage_rule_4,
      comparative_advantage_rule_5
    ) %>%
    arrange(state_name, county_name, naics_code)
  
  if (verbose) {
    cat_info(sprintf("Found %s county×industry pairs with comparative advantage discrepancies",
                     format_number(nrow(discrepancies))))
  }
  
  # Summary statistics
  cat_subhead("\nComparative Advantage Summary by Rule:")
  
  ca_summary <- lq_data %>%
    summarise(
      total_pairs = n(),
      ca_rule_3 = sum(comparative_advantage_rule_3, na.rm = TRUE),
      ca_rule_4 = sum(comparative_advantage_rule_4, na.rm = TRUE),
      ca_rule_5 = sum(comparative_advantage_rule_5, na.rm = TRUE),
      presence_rule_3 = sum(industry_presence_rule_3, na.rm = TRUE),
      presence_rule_4 = sum(industry_presence_rule_4, na.rm = TRUE),
      presence_rule_5 = sum(industry_presence_rule_5, na.rm = TRUE)
    )
  
  cat("\n")
  cat(sprintf("  Total county×industry pairs analyzed: %s\n", format_number(ca_summary$total_pairs)))
  cat(sprintf("\n  Industry Presence (LQ > 0):\n"))
  cat(sprintf("    Rule 3: %s (%.1f%%)\n", format_number(ca_summary$presence_rule_3), 
              100 * ca_summary$presence_rule_3 / ca_summary$total_pairs))
  cat(sprintf("    Rule 4: %s (%.1f%%)\n", format_number(ca_summary$presence_rule_4),
              100 * ca_summary$presence_rule_4 / ca_summary$total_pairs))
  cat(sprintf("    Rule 5: %s (%.1f%%)\n", format_number(ca_summary$presence_rule_5),
              100 * ca_summary$presence_rule_5 / ca_summary$total_pairs))
  cat(sprintf("\n  Comparative Advantage (LQ >= 1):\n"))
  cat(sprintf("    Rule 3: %s (%.1f%%)\n", format_number(ca_summary$ca_rule_3),
              100 * ca_summary$ca_rule_3 / ca_summary$total_pairs))
  cat(sprintf("    Rule 4: %s (%.1f%%)\n", format_number(ca_summary$ca_rule_4),
              100 * ca_summary$ca_rule_4 / ca_summary$total_pairs))
  cat(sprintf("    Rule 5: %s (%.1f%%)\n", format_number(ca_summary$ca_rule_5),
              100 * ca_summary$ca_rule_5 / ca_summary$total_pairs))
  
  # Transition analysis
  cat_subhead("\nComparative Advantage Transitions:")
  
  # Rule 3 → Rule 4 transitions
  transitions_3_4 <- lq_data %>%
    mutate(
      transition_3_4 = case_when(
        !comparative_advantage_rule_3 & comparative_advantage_rule_4 ~ "Gained CA (3→4)",
        comparative_advantage_rule_3 & !comparative_advantage_rule_4 ~ "Lost CA (3→4)",
        TRUE ~ "No change"
      )
    ) %>%
    count(transition_3_4)
  
  cat("\n  Rule 3 → Rule 4:\n")
  print(transitions_3_4)
  
  # Rule 3 → Rule 5 transitions
  transitions_3_5 <- lq_data %>%
    mutate(
      transition_3_5 = case_when(
        !comparative_advantage_rule_3 & comparative_advantage_rule_5 ~ "Gained CA (3→5)",
        comparative_advantage_rule_3 & !comparative_advantage_rule_5 ~ "Lost CA (3→5)",
        TRUE ~ "No change"
      )
    ) %>%
    count(transition_3_5)
  
  cat("\n  Rule 3 → Rule 5:\n")
  print(transitions_3_5)
  
  # Rule 4 → Rule 5 transitions
  transitions_4_5 <- lq_data %>%
    mutate(
      transition_4_5 = case_when(
        !comparative_advantage_rule_4 & comparative_advantage_rule_5 ~ "Gained CA (4→5)",
        comparative_advantage_rule_4 & !comparative_advantage_rule_5 ~ "Lost CA (4→5)",
        TRUE ~ "No change"
      )
    ) %>%
    count(transition_4_5)
  
  cat("\n  Rule 4 → Rule 5:\n")
  print(transitions_4_5)
  
  stop_timer("Location Quotient Analysis")
  
  return(list(
    lq_data = lq_data,
    discrepancies = discrepancies,
    summary = ca_summary,
    transitions_3_4 = transitions_3_4,
    transitions_3_5 = transitions_3_5,
    transitions_4_5 = transitions_4_5
  ))
}

print_comparative_advantage_discrepancies <- function(lq_results) {
  cat_header("=== COMPARATIVE ADVANTAGE DISCREPANCIES (FULL LIST) ===")
  
  if (is.null(lq_results$discrepancies) || nrow(lq_results$discrepancies) == 0) {
    cat_info("No comparative advantage discrepancies found between wage rules.")
    return(invisible(NULL))
  }
  
  cat_info(sprintf("Printing all %s county×industry pairs with discrepancies (n = Inf)...\n",
                   format_number(nrow(lq_results$discrepancies))))
  
  # Format for printing
  print_df <- lq_results$discrepancies %>%
    mutate(
      lq_3 = round(lq_3, 4),
      lq_4 = round(lq_4, 4),
      lq_5 = round(lq_5, 4)
    ) %>%
    rename(
      `County` = county_name,
      `State` = state_name,
      `NAICS` = naics_code,
      `Industry` = naics6_title,
      `Empl_R3` = empl_3,
      `Empl_R4` = empl_4,
      `Empl_R5` = empl_5,
      `LQ_R3` = lq_3,
      `LQ_R4` = lq_4,
      `LQ_R5` = lq_5,
      `CA_R3` = comparative_advantage_rule_3,
      `CA_R4` = comparative_advantage_rule_4,
      `CA_R5` = comparative_advantage_rule_5
    )
  
  print(print_df, n = Inf)
  
  cat("\n")
  cat_info("CA = Comparative Advantage (TRUE if LQ >= 1)")
  cat_info("LQ = Location Quotient = (county industry share) / (national industry share)")
}

# ==============================================================================
# PHASE 8: MAIN INVESTIGATION
# ==============================================================================

investigate_wage_rules <- function(year, ownership, conversion,
                                   naics_xwalk, county_xwalk_2024) {
  cat_header("=== WAGE RULE INVESTIGATION ===")
  start_timer("Wage Rule Investigation")
  
  cat_info(sprintf("Parameters: Year=%s, Ownership=%s, Conversion=%s", year, ownership, conversion))
  cat_info("Memory before downloads:", get_memory_usage())
  
  results <- list()
  failed_rules <- character()
  
  for (rule in c("3", "4", "5")) {
    cat_subhead(sprintf("\n--- Downloading Wage Rule %s ---", rule))
    start_timer(sprintf("Download Rule %s", rule))
    
    df <- download_single_dataset(
      year = year, wage_rule = rule, ownership = ownership, conversion = conversion,
      verbose = TRUE, show_progress = TRUE
    )
    
    stop_timer(sprintf("Download Rule %s", rule))
    
    if (!is.null(df)) {
      df <- process_tapestry_data(df, year, naics_xwalk, county_xwalk_2024, verbose = TRUE)
      
      rule_name <- paste0("rule_", rule)
      results[[rule_name]] <- df
      inspect_dataset(df, sprintf("Wage Rule %s (processed)", rule))
    } else {
      cat_error(sprintf("Failed to download Wage Rule %s", rule))
      failed_rules <- c(failed_rules, rule)
    }
    
    Sys.sleep(0.5)
  }
  
  cat_info("Memory after downloads:", get_memory_usage())
  
  cat_header("=== DOWNLOAD SUMMARY ===")
  cat(sprintf("  Successfully downloaded: %d of 3 rules\n", length(results)))
  if (length(failed_rules) > 0) {
    cat(sprintf("  Failed rules: %s\n", paste(failed_rules, collapse = ", ")))
  }
  
  cat_header("=== COMPARATIVE ANALYSIS ===")
  
  if (length(results) < 2) {
    cat_warn("Need at least 2 rules for comparative analysis")
    stop_timer("Wage Rule Investigation")
    return(list(
      datasets = results, 
      placeholder_analysis = NULL, 
      delta_df = NULL, 
      lq_analysis = NULL,
      crosswalks = list(
        naics_xwalk = naics_xwalk,
        county_xwalk_2024 = county_xwalk_2024
      )
    ))
  }
  
  # Row counts
  cat_subhead("Row Count Comparison:")
  for (name in names(results)) {
    cat(sprintf("  %s: %s rows\n", name, format_number(nrow(results[[name]]))))
  }
  
  if (length(unique(sapply(results, nrow))) == 1) {
    draw_box("ALL DOWNLOADED RULES HAVE IDENTICAL ROW COUNTS")
    cat_info("This confirms: wage rules modify VALUES, not row structure")
  }
  
  # Key identity checks
  if (length(results) >= 2) {
    check_key_identity(results[[1]], results[[2]], names(results)[1], names(results)[2])
  }
  if (length(results) >= 3) {
    check_key_identity(results[[1]], results[[3]], names(results)[1], names(results)[3])
  }
  
  # Placeholder analysis
  placeholder_analysis <- analyze_placeholder_values(results)
  
  # Delta computations
  delta_df <- NULL
  lq_analysis <- NULL
  
  if (length(results) == 3) {
    delta_df <- compute_rule_deltas(results$rule_3, results$rule_4, results$rule_5)
    if (!is.null(delta_df)) {
      summarize_allocation_balance(delta_df)
      
      # Location quotient analysis (v5)
      lq_analysis <- compute_location_quotients(delta_df, verbose = TRUE)
      
      # Print all discrepancies
      print_comparative_advantage_discrepancies(lq_analysis)
    }
  }
  
  # Conclusions
  cat_header("=== INVESTIGATION CONCLUSIONS ===")
  cat("\n")
  cat("  ┌─────────────────────────────────────────────────────────────────┐\n")
  cat("  │                         KEY FINDINGS                            │\n")
  cat("  ├─────────────────────────────────────────────────────────────────┤\n")
  cat("  │  1. ROW STRUCTURE: Identical across all wage rules              │\n")
  cat("  │  2. UNKNOWN COUNTY REALLOCATION (Rule 4 & 5): Values            │\n")
  cat("  │     redistributed from statewide (XX999) to counties            │\n")
  cat("  │  3. NAICS '999999' REALLOCATION (Rule 5 only): Values           │\n")
  cat("  │     redistributed to specific industries                        │\n")
  cat("  │  4. LOCATION QUOTIENT ANALYSIS: Comparative advantage           │\n")
  cat("  │     classifications can change between wage rules               │\n")
  cat("  │  5. RECOMMENDATION: Use Rule 5 for most complete allocation     │\n")
  cat("  └─────────────────────────────────────────────────────────────────┘\n\n")
  
  stop_timer("Wage Rule Investigation")
  
  return(list(
    datasets = results,
    placeholder_analysis = placeholder_analysis,
    delta_df = delta_df,
    lq_analysis = lq_analysis,
    crosswalks = list(
      naics_xwalk = naics_xwalk,
      county_xwalk_2024 = county_xwalk_2024
    )
  ))
}

# ==============================================================================
# PHASE 9: FULL OUTPUT PRINTING (v5 NEW)
# ==============================================================================

print_all_investigation_results <- function(investigation_results) {
  cat_header("=== FULL INVESTIGATION RESULTS OUTPUT ===")
  
  # Crosswalks first (metadata)
  cat_subhead("\n0. CROSSWALKS (metadata)")
  
  if (!is.null(investigation_results$crosswalks)) {
    if (!is.null(investigation_results$crosswalks$naics_xwalk)) {
      cat_header("--- NAICS HIERARCHY CROSSWALK ---")
      naics_xwalk <- investigation_results$crosswalks$naics_xwalk
      cat(sprintf("\nDimensions: %s rows × %d cols\n", 
                  format_number(nrow(naics_xwalk)), ncol(naics_xwalk)))
      cat("\nprint() output (first 20 rows):\n")
      print(head(naics_xwalk, 20))
      cat("\nglimpse() output:\n")
      glimpse(naics_xwalk)
      cat("\nUnique NAICS years:\n")
      print(table(naics_xwalk$naics_year))
    }
    
    if (!is.null(investigation_results$crosswalks$county_xwalk_2024)) {
      cat_header("--- COUNTY/STATE/CBSA/CSA/CZ CROSSWALK (2024) ---")
      county_xwalk <- investigation_results$crosswalks$county_xwalk_2024
      county_xwalk_no_geom <- sf::st_set_geometry(county_xwalk, NULL)
      cat(sprintf("\nDimensions: %s rows × %d cols\n", 
                  format_number(nrow(county_xwalk)), ncol(county_xwalk)))
      cat("\nprint() output (first 20 rows, no geometry):\n")
      print(head(county_xwalk_no_geom, 20))
      cat("\nglimpse() output (no geometry):\n")
      glimpse(county_xwalk_no_geom)
      cat("\nStates represented:\n")
      print(sort(unique(county_xwalk_no_geom$state_abbreviation)))
      cat(sprintf("\nCounties in CBSAs: %d / %d\n", 
                  sum(county_xwalk_no_geom$county_in_cbsa, na.rm = TRUE),
                  nrow(county_xwalk_no_geom)))
      cat(sprintf("Counties in CSAs: %d / %d\n", 
                  sum(county_xwalk_no_geom$county_in_csa, na.rm = TRUE),
                  nrow(county_xwalk_no_geom)))
    }
  }
  
  # Datasets
  cat_subhead("\n1. DATASETS (print + glimpse)")
  if (!is.null(investigation_results$datasets)) {
    for (name in names(investigation_results$datasets)) {
      cat_header(sprintf("--- %s ---", toupper(name)))
      df <- investigation_results$datasets[[name]]
      cat("\nprint() output:\n")
      print(df)
      cat("\nglimpse() output:\n")
      glimpse(df)
    }
  }
  
  # Delta df
  if (!is.null(investigation_results$delta_df)) {
    cat_header("--- DELTA_DF ---")
    cat("\nprint() output:\n")
    print(investigation_results$delta_df)
    cat("\nglimpse() output:\n")
    glimpse(investigation_results$delta_df)
  }
  
  # Placeholder analysis
  if (!is.null(investigation_results$placeholder_analysis)) {
    pa <- investigation_results$placeholder_analysis
    
    cat_header("--- PLACEHOLDER ANALYSIS: fips_999_summary ---")
    print(pa$fips_999_summary)
    
    cat_header("--- PLACEHOLDER ANALYSIS: naics_999999_summary ---")
    print(pa$naics_999999_summary)
    
    cat_header("--- PLACEHOLDER ANALYSIS: total_wages ---")
    print(pa$total_wages)
    
    if (!is.null(pa$fips_999_residuals)) {
      cat_header("--- PLACEHOLDER ANALYSIS: fips_999_residuals (print n=Inf) ---")
      print(pa$fips_999_residuals, n = Inf)
      cat("\nglimpse() output:\n")
      glimpse(pa$fips_999_residuals)
    }
    
    if (!is.null(pa$naics_999999_residuals)) {
      cat_header("--- PLACEHOLDER ANALYSIS: naics_999999_residuals (print n=Inf) ---")
      print(pa$naics_999999_residuals, n = Inf)
      cat("\nglimpse() output:\n")
      glimpse(pa$naics_999999_residuals)
    }
    
    if (!is.null(pa$state_summary)) {
      cat_header("--- PLACEHOLDER ANALYSIS: state_summary (print n=Inf) ---")
      print(pa$state_summary, n = Inf)
    }
    
    if (!is.null(pa$industry_summary)) {
      cat_header("--- PLACEHOLDER ANALYSIS: industry_summary (print n=Inf) ---")
      print(pa$industry_summary, n = Inf)
    }
  }
  
  # LQ Analysis
  if (!is.null(investigation_results$lq_analysis)) {
    lq <- investigation_results$lq_analysis
    
    cat_header("--- LQ ANALYSIS: summary ---")
    print(lq$summary)
    
    cat_header("--- LQ ANALYSIS: transitions_3_4 ---")
    print(lq$transitions_3_4)
    
    cat_header("--- LQ ANALYSIS: transitions_3_5 ---")
    print(lq$transitions_3_5)
    
    cat_header("--- LQ ANALYSIS: transitions_4_5 ---")
    print(lq$transitions_4_5)
    
    if (!is.null(lq$discrepancies) && nrow(lq$discrepancies) > 0) {
      cat_header("--- LQ ANALYSIS: discrepancies (print n=Inf) ---")
      print(lq$discrepancies, n = Inf)
      cat("\nglimpse() output:\n")
      glimpse(lq$discrepancies)
    }
    
    cat_header("--- LQ ANALYSIS: lq_data (glimpse only - large) ---")
    glimpse(lq$lq_data)
  }
  
  # Quick access examples
  cat_header("=== QUICK ACCESS EXAMPLES (EXECUTED) ===")
  
  if (!is.null(investigation_results$datasets$rule_5)) {
    cat_subhead("\n# Get the most-processed rule data:")
    cat("df_rule5 <- investigation_results$datasets$rule_5\n")
    df_rule5 <- investigation_results$datasets$rule_5
    cat(sprintf("Result: %s rows × %d cols\n", format_number(nrow(df_rule5)), ncol(df_rule5)))
    
    cat_subhead("\n# Filter out ALL placeholder rows:")
    cat("df_clean <- df_rule5 %>%\n")
    cat("  filter(!unknown_undefined_county) %>%\n")
    cat("  filter(naics_code != '999999')\n")
    df_clean <- df_rule5 %>%
      filter(!unknown_undefined_county) %>%
      filter(naics_code != "999999")
    cat(sprintf("Result: %s rows × %d cols\n", format_number(nrow(df_clean)), ncol(df_clean)))
    
    cat_subhead("\n# Compare LA County across rules:")
    cat("county <- '06037'  # LA County\n")
    county <- "06037"
    la_comparison <- lapply(investigation_results$datasets, function(df) {
      wage_col <- grep("tap_wages", names(df), value = TRUE)[1]
      sum(df[[wage_col]][df$county_geoid == county], na.rm = TRUE)
    })
    cat("Result:\n")
    print(la_comparison)
  }
  
  if (!is.null(investigation_results$placeholder_analysis$fips_999_residuals)) {
    cat_subhead("\n# Get residual records that couldn't be reallocated:")
    cat("residuals <- investigation_results$placeholder_analysis$fips_999_residuals\n")
    residuals <- investigation_results$placeholder_analysis$fips_999_residuals
    cat(sprintf("Result: %s rows\n", format_number(nrow(residuals))))
  }
  
  if (!is.null(investigation_results$placeholder_analysis$state_summary)) {
    cat_subhead("\n# See which states have the most residuals:")
    cat("investigation_results$placeholder_analysis$state_summary\n")
    print(investigation_results$placeholder_analysis$state_summary, n = Inf)
  }
  
  if (!is.null(investigation_results$placeholder_analysis$industry_summary)) {
    cat_subhead("\n# See which industries have unreallocatable values:")
    cat("investigation_results$placeholder_analysis$industry_summary\n")
    print(investigation_results$placeholder_analysis$industry_summary, n = Inf)
  }
  
  if (!is.null(investigation_results$delta_df)) {
    cat_subhead("\n# Biggest county changes in Rule4 vs Rule3:")
    cat("delta %>%\n")
    cat("  group_by(county_geoid, county_name, state_abbreviation) %>%\n")
    cat("  summarise(d_wages_4_3 = sum(d_wages_4_3, na.rm = TRUE), .groups = 'drop') %>%\n")
    cat("  arrange(desc(abs(d_wages_4_3))) %>%\n")
    cat("  head(25)\n")
    delta_summary <- investigation_results$delta_df %>%
      group_by(county_geoid, county_name, state_abbreviation) %>%
      summarise(d_wages_4_3 = sum(d_wages_4_3, na.rm = TRUE), .groups = "drop") %>%
      arrange(desc(abs(d_wages_4_3))) %>%
      head(25)
    print(delta_summary)
  }
}

print_environment_summary <- function(investigation_results) {
  cat_header("=== R ENVIRONMENT SUMMARY ===")
  
  cat_subhead("Data frames created in this session:")
  cat("\n")
  
  # Crosswalks
  if (!is.null(investigation_results$crosswalks)) {
    if (!is.null(investigation_results$crosswalks$naics_xwalk)) {
      xw <- investigation_results$crosswalks$naics_xwalk
      cat("  • investigation_results$crosswalks$naics_xwalk\n")
      cat(sprintf("      Dimensions: %s rows × %d columns\n", format_number(nrow(xw)), ncol(xw)))
      cat(sprintf("      Size: %s\n", format(object.size(xw), units = "auto")))
      cat(sprintf("      NAICS years: %s\n", paste(unique(xw$naics_year), collapse = ", ")))
      cat("\n")
    }
    
    if (!is.null(investigation_results$crosswalks$county_xwalk_2024)) {
      xw <- investigation_results$crosswalks$county_xwalk_2024
      cat("  • investigation_results$crosswalks$county_xwalk_2024\n")
      cat(sprintf("      Dimensions: %s rows × %d columns\n", format_number(nrow(xw)), ncol(xw)))
      cat(sprintf("      Size: %s\n", format(object.size(xw), units = "auto")))
      cat("      (Includes geometry for mapping)\n")
      cat("\n")
    }
  }
  
  if (!is.null(investigation_results$datasets) && length(investigation_results$datasets) > 0) {
    for (name in names(investigation_results$datasets)) {
      df <- investigation_results$datasets[[name]]
      cat(sprintf("  • investigation_results$datasets$%s\n", name))
      cat(sprintf("      Dimensions: %s rows × %d columns\n", format_number(nrow(df)), ncol(df)))
      cat(sprintf("      Size: %s\n", format(object.size(df), units = "auto")))
      cat("\n")
    }
  }
  
  if (!is.null(investigation_results$delta_df)) {
    cat("  • investigation_results$delta_df\n")
    cat(sprintf("      Dimensions: %s rows × %d columns\n",
                format_number(nrow(investigation_results$delta_df)),
                ncol(investigation_results$delta_df)))
    cat(sprintf("      Size: %s\n", format(object.size(investigation_results$delta_df), units = "auto")))
    cat("\n")
  }
  
  if (!is.null(investigation_results$lq_analysis)) {
    cat("  • investigation_results$lq_analysis$lq_data\n")
    cat(sprintf("      Dimensions: %s rows × %d columns\n",
                format_number(nrow(investigation_results$lq_analysis$lq_data)),
                ncol(investigation_results$lq_analysis$lq_data)))
    cat(sprintf("      Size: %s\n", format(object.size(investigation_results$lq_analysis$lq_data), units = "auto")))
    cat("\n")
    
    if (!is.null(investigation_results$lq_analysis$discrepancies)) {
      cat("  • investigation_results$lq_analysis$discrepancies\n")
      cat(sprintf("      Dimensions: %s rows\n",
                  format_number(nrow(investigation_results$lq_analysis$discrepancies))))
      cat("\n")
    }
  }
  
  if (!is.null(investigation_results$placeholder_analysis)) {
    pa <- investigation_results$placeholder_analysis
    if (!is.null(pa$fips_999_residuals)) {
      cat("  • investigation_results$placeholder_analysis$fips_999_residuals\n")
      cat(sprintf("      Dimensions: %s rows\n", format_number(nrow(pa$fips_999_residuals))))
      cat("\n")
    }
    if (!is.null(pa$state_summary)) {
      cat("  • investigation_results$placeholder_analysis$state_summary\n")
      cat(sprintf("      Dimensions: %d rows\n", nrow(pa$state_summary)))
      cat("\n")
    }
    if (!is.null(pa$industry_summary)) {
      cat("  • investigation_results$placeholder_analysis$industry_summary\n")
      cat(sprintf("      Dimensions: %d rows\n", nrow(pa$industry_summary)))
      cat("\n")
    }
  }
}

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

main <- function() {
  draw_box("TAPESTRY QCEW - MASTER SCRIPT v5")
  
  cat("\n")
  cat_info("Features in v5:")
  cat("  • glimpse() of each data frame after wage rule download\n")
  cat("  • Smart defaults: Year 2024, NAICS 6-digit, Ownership 0\n")
  cat("  • Location Quotient analysis with comparative advantage tracking\n")
  cat("  • Full print/glimpse of all investigation results\n")
  cat("  • Comparative advantage discrepancies printed with n = Inf\n")
  cat("\n")
  cat("Type '0' or 'exit' at any prompt to stop.\n")
  cat_info("Initial memory:", get_memory_usage())
  
  start_timer("Total Script Execution")
  
  # ----- STEP 1: Ask about defaults -----
  cat_header("=== PARAMETER CONFIGURATION ===")
  cat("\n")
  cat("  Default settings:\n")
  cat(sprintf("    • Run diagnostics: %s\n", if(DEFAULT_RUN_DIAGNOSTICS) "Yes" else "No"))
  cat(sprintf("    • Year: %s\n", DEFAULT_YEAR))
  cat(sprintf("    • Ownership: %s (All Ownership)\n", DEFAULT_OWNERSHIP))
  cat(sprintf("    • Conversion: %s (NAICS 6-digit Standard)\n", DEFAULT_CONVERSION))
  cat(sprintf("    • Implied naics_year: %s\n", naics_year_for_data_year(as.integer(DEFAULT_YEAR))))
  cat("\n")
  
  cat("Options:\n")
  cat("  [1] Proceed with defaults\n")
  cat("  [2] Revise parameters\n")
  cat("  [0] Exit\n")
  cat("\nEnter selection: ")
  
  choice <- trimws(readline())
  
  if (choice == "0" || tolower(choice) == "exit") {
    cat_warn("User requested exit.")
    stop("Script terminated by user.", call. = FALSE)
  }
  
  # Initialize parameters with defaults
  run_diagnostics <- DEFAULT_RUN_DIAGNOSTICS
  selected_year <- DEFAULT_YEAR
  selected_ownership <- DEFAULT_OWNERSHIP
  selected_conversion <- DEFAULT_CONVERSION
  
  if (choice == "2") {
    # Revise parameters
    run_diagnostics <- prompt_yes_no("Run comprehensive system diagnostics?")
    
    # Will need to discover options after authentication
    # For now, just set flags
    revise_params <- TRUE
  } else {
    revise_params <- FALSE
  }
  
  # ----- STEP 2: Optional System Diagnostics -----
  connectivity_ok <- TRUE
  if (run_diagnostics) {
    connectivity_ok <- print_system_diagnostics()
    
    if (!connectivity_ok) {
      cat_warn("\nConnectivity issues detected!")
      if (!prompt_yes_no("Continue anyway?")) {
        cat_error("Exiting due to connectivity issues.")
        return(invisible(NULL))
      }
    }
  } else {
    cat_info("Skipping diagnostics - quick connection test...")
    connectivity_ok <- tryCatch({
      resp <- httr::HEAD("https://tapestry.nkn.uidaho.edu", httr::timeout(15))
      httr::status_code(resp) == 200
    }, error = function(e) FALSE)
    
    if (!connectivity_ok) {
      cat_error("Cannot connect to Tapestry server!")
      if (!prompt_yes_no("Continue anyway?")) {
        return(invisible(NULL))
      }
    } else {
      cat_success("Tapestry server is reachable")
    }
  }
  
  # ----- STEP 3: Parallel Processing Setup -----
  num_workers <- setup_parallel()
  
  on.exit({
    cleanup_parallel()
    print_timing_summary()
  }, add = TRUE)
  
  # ----- STEP 4: Load Reference Datasets -----
  naics_xwalk <- load_bls_qcew_naics_hierarchy(verbose = TRUE)
  county_xwalk_2024 <- build_county_state_cbsa_csa_cz_2024(verbose = TRUE)
  
  # ----- STEP 5: Get Credentials -----
  creds <- get_tapestry_credentials()
  
  # ----- STEP 6: Authentication -----
  if (!login_tapestry(creds$email, creds$password)) {
    cat_error("Authentication failed. Please check your credentials.")
    return(invisible(NULL))
  }
  
  # ----- STEP 7: Endpoint Discovery -----
  endpoint_info <- discover_endpoints()
  
  # ----- STEP 8: Parameter Selection (if revising) -----
  if (revise_params) {
    cat_header("=== OPTIONS DISCOVERY ===")
    start_timer("Options Discovery")
    
    years <- get_available_years()
    ownership_codes <- get_ownership_codes()
    conversions <- get_sector_conversions()
    
    stop_timer("Options Discovery")
    
    cat_header("=== PARAMETER SELECTION ===")
    
    selected_year <- prompt_menu("Select YEAR:", as.character(years))
    
    ownership_labels <- c(
      "0" = "0 - All Ownership", "1" = "1 - Federal", "2" = "2 - State",
      "3" = "3 - Local", "5" = "5 - Private"
    )
    ownership_display <- sapply(ownership_codes, function(x) ownership_labels[x] %||% x)
    selected_ownership <- gsub(" .*", "", prompt_menu("Select OWNERSHIP:", ownership_display))
    
    conversion_display <- paste0(conversions$values, " - ", conversions$labels[conversions$values])
    selected_conversion <- gsub(" - .*", "", prompt_menu("Select CONVERSION:", conversion_display))
  }
  
  # ----- STEP 9: Confirm and Run -----
  cat_header("=== FINAL SELECTION SUMMARY ===")
  cat("  Year:", selected_year, "\n")
  cat("  Ownership:", selected_ownership, "\n")
  cat("  Conversion:", selected_conversion, "\n")
  cat("  Implied naics_year mapping:", naics_year_for_data_year(as.integer(selected_year)), "\n")
  
  if (!prompt_yes_no("Proceed with investigation?")) {
    cat_warn("Cancelled.")
    return(invisible(NULL))
  }
  
  # ----- STEP 10: Investigation -----
  investigation <- investigate_wage_rules(
    year = selected_year,
    ownership = selected_ownership,
    conversion = selected_conversion,
    naics_xwalk = naics_xwalk,
    county_xwalk_2024 = county_xwalk_2024
  )
  
  stop_timer("Total Script Execution")
  
  # ----- STEP 11: Full Results Output (v5) -----
  print_all_investigation_results(investigation)
  
  # ----- STEP 12: Environment Summary -----
  print_environment_summary(investigation)
  
  cat_header("=== COMPLETE ===")
  cat_info("All results printed to console. No files saved.")
  cat_info("Final memory:", get_memory_usage())
  
  return(invisible(investigation))
}

# ==============================================================================
# RUN
# ==============================================================================

investigation_results <- main()
