# =========================================================
# Tapestry Economic Complexity — county × NAICS6 (2015–2024)
# Robust implementation with symmetric eigen step + safe sparsity
# Math per Brookings technical paper eqs. 4–7, 8–14, 15–22.
# =========================================================

# ---- Libraries ----
suppressPackageStartupMessages({
  library(readr); library(readxl); library(dplyr); library(tidyr)
  library(stringr); library(purrr); library(tibble); library(Matrix)
})
if (!requireNamespace("RSpectra", quietly = TRUE)) {
  stop("Please install RSpectra: install.packages('RSpectra')")
}


# ---- Dynamic, cross-platform paths (mac/pc/user-agnostic) ----

suppressPackageStartupMessages({ library(stringr); library(purrr) })

# Helpers
norm_dir <- function(p) normalizePath(p, winslash = "/", mustWork = FALSE)
existing_dir <- function(cands) {
  cands <- unique(cands[!is.na(cands) & nzchar(cands)])
  ex <- cands[dir.exists(cands)]
  if (length(ex)) return(norm_dir(ex[1]))
  NULL
}
assert_dir <- function(p, hint = NULL) {
  if (is.null(p) || !dir.exists(p)) {
    msg <- paste0(
      "Required directory not found.\nTried: ", paste(hint$tried, collapse = "\n  - "),
      "\n\nSet an override env var and re-run, e.g.:\n",
      "Sys.setenv(", hint$env, '="', hint$suggest, '")'
    )
    stop(msg, call. = FALSE)
  }
}

home <- path.expand("~")

# 1) Candidate OneDrive roots from env + common install paths (macOS & Windows)
od_envs <- c(Sys.getenv("ONEDRIVE_ROOT"),
             Sys.getenv("OneDrive"),           # Win consumer
             Sys.getenv("OneDriveCommercial"), # Win business
             Sys.getenv("OneDriveConsumer"))   # Win consumer (alt)

# macOS new OneDrive location(s)
mac_cloud_lib <- file.path(home, "Library", "CloudStorage")
mac_od_glob <- if (dir.exists(mac_cloud_lib)) {
  list.dirs(mac_cloud_lib, full.names = TRUE, recursive = FALSE)
} else character(0)
mac_od_glob <- mac_od_glob[basename(mac_od_glob) %>% str_detect("^OneDrive")]
# Old macOS default (rare now)
mac_old <- file.path(home, "OneDrive")

# Windows typical
win_od_org <- list.dirs(home, full.names = TRUE, recursive = FALSE)
win_od_org <- win_od_org[basename(win_od_org) %>% str_detect("^OneDrive( - |$)")]

onedrive_candidates <- c(od_envs, mac_od_glob, mac_old, win_od_org) %>% unique()
onedrive_root <- existing_dir(onedrive_candidates)

# Optional: allow user to hard-override via ONEDRIVE_ROOT; if missing, we’ll try to proceed.
if (is.null(onedrive_root)) {
  message("OneDrive root not auto-detected. You can set it with: ",
          'Sys.setenv(ONEDRIVE_ROOT="/path/to/OneDrive-YourOrg")')
}

# 2) Build your project-specific relative paths
RAW_DATA_REL <- file.path(
  "US Program - Documents", "6_Projects", "Clean Regional Economic Development",
  "ACRE", "Data", "Raw Data"
)

QCEW_META_REL <- file.path(RAW_DATA_REL, "BLS_QCEW", "metadata")
TAPESTRY_REL  <- file.path(RAW_DATA_REL, "Tapestry_Employment",
                           "contains_naics_999999_county_XX999", "NAICS_6D")

# 3) Allow per-path env var overrides; otherwise place under detected OneDrive
QCEW_META_DIR <- Sys.getenv("QCEW_META_DIR")
TAPESTRY_DIR_NAICS6D <- Sys.getenv("TAPESTRY_DIR_NAICS6D")

if (!nzchar(QCEW_META_DIR)) {
  QCEW_META_DIR <- if (!is.null(onedrive_root)) file.path(onedrive_root, QCEW_META_REL) else NA_character_
}
if (!nzchar(TAPESTRY_DIR_NAICS6D)) {
  TAPESTRY_DIR_NAICS6D <- if (!is.null(onedrive_root)) file.path(onedrive_root, TAPESTRY_REL) else NA_character_
}

QCEW_META_DIR <- norm_dir(QCEW_META_DIR)
TAPESTRY_DIR_NAICS6D <- norm_dir(TAPESTRY_DIR_NAICS6D)

# 4) Validate & give actionable hints if missing
assert_dir(
  QCEW_META_DIR,
  hint = list(
    env = "QCEW_META_DIR",
    tried = c(
      paste0("Env QCEW_META_DIR: ", Sys.getenv("QCEW_META_DIR")),
      paste0("ONEDRIVE_ROOT + REL: ", file.path(ifelse(nzchar(Sys.getenv("ONEDRIVE_ROOT")), Sys.getenv("ONEDRIVE_ROOT"), "<not set>"), QCEW_META_REL))
    ),
    suggest = file.path(ifelse(!is.null(onedrive_root), onedrive_root, "/ABS/PATH/TO/OneDrive-YourOrg"), QCEW_META_REL)
  )
)

assert_dir(
  TAPESTRY_DIR_NAICS6D,
  hint = list(
    env = "TAPESTRY_DIR_NAICS6D",
    tried = c(
      paste0("Env TAPESTRY_DIR_NAICS6D: ", Sys.getenv("TAPESTRY_DIR_NAICS6D")),
      paste0("ONEDRIVE_ROOT + REL: ", file.path(ifelse(nzchar(Sys.getenv("ONEDRIVE_ROOT")), Sys.getenv("ONEDRIVE_ROOT"), "<not set>"), TAPESTRY_REL))
    ),
    suggest = file.path(ifelse(!is.null(onedrive_root), onedrive_root, "/ABS/PATH/TO/OneDrive-YourOrg"), TAPESTRY_REL)
  )
)

AREA_TITLES_FILE                  <- file.path(QCEW_META_DIR, "area-titles.csv")
INDUSTRY_TITLES_FILE              <- file.path(QCEW_META_DIR, "industry-titles.csv")
OWNERSHIP_TITLES_FILE             <- file.path(QCEW_META_DIR, "ownership-titles.csv")

# ---- Load metadata (fail fast) ----
req_meta <- c(AREA_TITLES_FILE, INDUSTRY_TITLES_FILE, OWNERSHIP_TITLES_FILE)
missing_meta <- req_meta[!file.exists(req_meta)]
if (length(missing_meta)) stop("Missing metadata files:\n - ", paste(missing_meta, collapse = "\n - "))

AREA_TITLES <- suppressMessages(readr::read_csv(
  AREA_TITLES_FILE,
  col_types = cols(area_fips = col_character(), area_title = col_character())
))
INDUSTRY_TITLES <- suppressMessages(readr::read_csv(
  INDUSTRY_TITLES_FILE,
  col_types = cols(industry_code = col_character(), industry_title = col_character())
))
OWNERSHIP_TITLES <- suppressMessages(readr::read_csv(
  OWNERSHIP_TITLES_FILE,
  col_types = cols(own_code = col_character(), own_title = col_character())
))

# ---- Helpers ----
is_valid_county_fips <- function(x) {
  # Keep 5-digit numeric counties; drop XX000/XX999 rollups
  str_detect(x, "^[0-9]{5}$") & !str_detect(x, "(000|999)$")
}

is_valid_naics6 <- function(x) {
  # Keep strictly 6 numeric digits; drop 999999/000000 and non-digits
  str_detect(x, "^[0-9]{6}$") & !(x %in% c("999999","000000"))
}

zscore <- function(x) {
  if (all(is.na(x))) return(x)
  mu <- mean(x, na.rm = TRUE); sdv <- sd(x, na.rm = TRUE)
  if (is.na(sdv) || sdv == 0) return(rep(NA_real_, length(x)))
  (x - mu) / sdv
}

# ---- Year reader & enrichment (safe types, LQ) ----
read_and_enrich_year <- function(yr) {
  path <- file.path(TAPESTRY_DIR_NAICS6D, paste0(yr, ".csv"))
  if (!file.exists(path)) stop("Missing file: ", path)
  
  df <- readr::read_csv(
    path,
    col_types = cols(
      own_code         = col_character(),
      year             = col_integer(),
      area_fips        = col_character(),
      naics_code       = col_character(),
      tap_estabs_count = col_double(),
      tap_wages_est_3  = col_double(),
      tap_emplvl_est_3 = col_double()
    )
  ) %>%
    mutate(
      year      = coalesce(year, as.integer(yr)),
      area_fips = str_pad(area_fips, 5, "left", "0"),
      naics_code= str_pad(naics_code, 6, "left", "0")
    ) %>%
    left_join(OWNERSHIP_TITLES, by = "own_code") %>%
    left_join(AREA_TITLES,      by = "area_fips") %>%
    left_join(INDUSTRY_TITLES,  by = c("naics_code" = "industry_code"))
  
  # Filter to county-level + NAICS6 good codes for the complexity core;
  # keep all rows for the enriched output but mark what's eligible.
  df <- df %>%
    mutate(
      complexity_core = is_valid_county_fips(area_fips) & is_valid_naics6(naics_code)
    )
  
  # Totals for LQ
  county_totals <- df %>%
    group_by(area_fips, year) %>%
    summarise(total_employment_in_county = sum(tap_emplvl_est_3, na.rm = TRUE), .groups = "drop")
  
  industry_totals <- df %>%
    group_by(naics_code, year) %>%
    summarise(total_industry_employment_in_nation = sum(tap_emplvl_est_3, na.rm = TRUE), .groups = "drop")
  
  national_totals <- df %>%
    group_by(year) %>%
    summarise(total_employment_national = sum(tap_emplvl_est_3, na.rm = TRUE), .groups = "drop")
  
  df <- df %>%
    left_join(county_totals,   by = c("area_fips", "year")) %>%
    left_join(industry_totals, by = c("naics_code", "year")) %>%
    left_join(national_totals, by = "year") %>%
    mutate(
      area_share     = if_else(total_employment_in_county > 0,
                               tap_emplvl_est_3 / total_employment_in_county, NA_real_),
      national_share = if_else(total_employment_national > 0,
                               total_industry_employment_in_nation / total_employment_national, NA_real_),
      location_quotient = if_else(!is.na(area_share) & !is.na(national_share) & national_share > 0,
                                  area_share / national_share, NA_real_)
    )
  
  df
}

# ---- Complexity engine for one year ----
compute_complexity_for_year <- function(df_year) {
  yr <- unique(df_year$year)
  message("== Year ", yr, " ==")
  
  # --- Build M (RCA≥1) on the core (county+good NAICS6) ---
  core <- df_year %>%
    filter(complexity_core) %>%
    select(area_fips, naics_code, location_quotient) %>%
    distinct() %>%
    mutate(M_ci = as.integer(!is.na(location_quotient) & location_quotient >= 1))
  
  counties  <- sort(unique(core$area_fips))
  industries<- sort(unique(core$naics_code))
  core <- core %>%
    mutate(i = match(area_fips, counties),
           j = match(naics_code, industries))
  
  M <- sparseMatrix(i = core$i, j = core$j, x = core$M_ci,
                    dims = c(length(counties), length(industries)),
                    dimnames = list(counties, industries))
  
  Kc0 <- rowSums(M) # Diversity (eq. 6)
  Ki0 <- colSums(M) # Ubiquity (eq. 7)
  
  # Drop zero rows/cols for spectral steps
  keep_c <- which(Kc0 > 0)
  keep_i <- which(Ki0 > 0)
  
  if (length(keep_c) < 2 || length(keep_i) < 2) {
    stop("Not enough positive-diversity counties or positive-ubiquity industries in year ", yr)
  }
  
  Mtrim <- M[keep_c, keep_i, drop = FALSE]
  kc    <- pmax(rowSums(Mtrim), 1)        # safe denominators
  ki    <- pmax(colSums(Mtrim), 1)
  
  # --- Symmetric ICI (second eigenvector of A_I) (eqs. 10–14) ---
  # A_I = Di^{-1/2} * M' * Dc^{-1} * M * Di^{-1/2}
  Dc_inv  <- Diagonal(x = 1 / kc)
  Di_mh   <- Diagonal(x = 1 / sqrt(ki))
  
  A_I <- Di_mh %*% (t(Mtrim) %*% Dc_inv %*% Mtrim) %*% Di_mh
  # Largest eigenvector corresponds to trivial structure; take the second one
  ev_I <- RSpectra::eigs_sym(A_I, k = 2, which = "LM")
  v2_I <- as.numeric(ev_I$vectors[,2])
  
  # Map back to raw ICI on trimmed industries
  ICI_raw_trim <- v2_I  # already real; relative scale is fine
  names(ICI_raw_trim) <- colnames(Mtrim)
  
  # County ECI as average ICI of present industries (eq. 11/14 derivation)
  ECI_raw_trim <- as.numeric( (Mtrim %*% ICI_raw_trim) / kc )
  names(ECI_raw_trim) <- rownames(Mtrim)
  
  # --- Co-location proximity φ (eqs. 15–16) ---
  # U = M' M ; φ_{i,i'} = U_{i,i'} / max(U_{i,i}, U_{i',i'})
  U <- t(Mtrim) %*% Mtrim
  u_diag <- diag(U)
  # Build dense φ safely at trimmed scale (industries are ~1–2k; manageable)
  U_dense <- as.matrix(U)
  max_den <- pmax(outer(u_diag, u_diag, FUN = pmax), 1)  # avoid 0/0
  phi <- U_dense / max_den
  diag(phi) <- 0
  
  # --- Density d_{c,i} (eq. 19) ---
  col_den   <- pmax(colSums(phi), 1)
  density   <- as.matrix(Mtrim %*% phi) / rep(col_den, each = nrow(Mtrim))
  rownames(density) <- rownames(Mtrim); colnames(density) <- colnames(Mtrim)
  
  # --- Strategic Index (eq. 21) & Strategic Gain (eq. 22) ---
  ICI_vec <- ICI_raw_trim
  ICI_vec[is.na(ICI_vec)] <- 0
  M_abs   <- 1 - as.matrix(Mtrim)
  
  # SI_c = sum_i d_{c,i} * (1 - M_{c,i}) * ICI_i
  SI_trim <- as.numeric(density %*% ( (1 - diag(ncol(density))) * 0 + ICI_vec )) # prep
  # The above line just coerces vector length; do properly:
  SI_trim <- rowSums( density * (M_abs * rep(ICI_vec, each = nrow(Mtrim))) )
  
  # SG_{c,i} = [ sum_{i'} ( φ_{i,i'} / sum_{i''} φ_{i'',i'} ) * (1 - M_{c,i'}) * ICI_{i'} ] - d_{c,i} * ICI_i
  phi_norm_by_den <- sweep(phi, 2, col_den, "/")
  term1 <- (M_abs %*% (t(phi_norm_by_den) * ICI_vec))  # (C×I)
  term2 <- density * rep(ICI_vec, each = nrow(density))
  SG_trim <- term1 - term2
  rownames(SG_trim) <- rownames(Mtrim); colnames(SG_trim) <- colnames(Mtrim)
  
  # --- Standardize within year (z-score) ---
  ECI_z <- zscore(ECI_raw_trim)
  ICI_z <- zscore(ICI_raw_trim)
  
  # --- Frame results & join back to full df_year ---
  ECI_df <- tibble(area_fips = names(ECI_z),
                   economic_complexity_index_county = as.numeric(ECI_z))
  ICI_df <- tibble(naics_code = names(ICI_z),
                   industry_complexity_index_score = as.numeric(ICI_z))
  
  density_df <- as_tibble(density, rownames = "area_fips") |>
    pivot_longer(-area_fips, names_to = "naics_code",
                 values_to = "industry_density_for_county")
  
  SI_df <- tibble(area_fips = names(SI_trim),
                  strategic_index = as.numeric(SI_trim))
  
  SG_df <- as_tibble(SG_trim, rownames = "area_fips") |>
    pivot_longer(-area_fips, names_to = "naics_code",
                 values_to = "strategic_gain")
  
  out <- df_year %>%
    mutate(industry_comparative_advantage_county =
             as.integer(!is.na(location_quotient) & location_quotient >= 1)) %>%
    left_join(ECI_df, by = "area_fips") %>%
    left_join(ICI_df, by = "naics_code") %>%
    left_join(density_df, by = c("area_fips","naics_code")) %>%
    left_join(SI_df, by = "area_fips") %>%
    left_join(SG_df, by = c("area_fips","naics_code"))
  
  # Set ECI/ICI/density/SI/SG to NA for rows that were not in the core
  out <- out %>%
    mutate(
      across(c(economic_complexity_index_county,
               industry_complexity_index_score,
               industry_density_for_county,
               strategic_index, strategic_gain),
             ~ if_else(complexity_core, .x, NA_real_))
    )
  
  message("..done ", yr)
  out
}

# ---- Run for all years ----
years <- 2015:2024
tapestry_list <- map(years, ~ compute_complexity_for_year(read_and_enrich_year(.x)))

# Examples: access per-year data frame
TAPESTRY_2015 <- tapestry_list[[1]]
TAPESTRY_2024 <- tapestry_list[[length(tapestry_list)]]

# Optional: save RDS per year
# pwalk(list(tapestry_list, years),
#       ~ saveRDS(..1, file = file.path("output", paste0("tapestry_", ..2, ".rds"))))

# Quick check
dplyr::glimpse(TAPESTRY_2024)

# =========================================================
# Combine all years + unique geographies + ECI deltas & insights
# =========================================================

years <- 2015:2024

# 0) Stack all years once
TAPESTRY_ALL_YEARS <- dplyr::bind_rows(tapestry_list)
dplyr::glimpse(TAPESTRY_ALL_YEARS)
