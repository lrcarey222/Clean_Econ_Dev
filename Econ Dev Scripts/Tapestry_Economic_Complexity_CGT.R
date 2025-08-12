# Tapestry Economic Complexity — per-year enrichment + LQ
# =========================================================
# Load required libraries
# =========================================================
library(readr)
library(readxl)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(tibble)
library(Matrix) # For sparse matrix operations

# =========================================================
# Define file paths for QCEW metadata
# =========================================================
# Note: User must update this path to their local directory
QCEW_META_DIR <- "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/BLS_QCEW/metadata"

AREA_TITLES_FILE                  <- file.path(QCEW_META_DIR, "area-titles.csv")
INDUSTRY_TITLES_FILE              <- file.path(QCEW_META_DIR, "industry-titles.csv")
SIZE_TITLES_FILE                  <- file.path(QCEW_META_DIR, "size-titles.csv")
AGG_LEVEL_TITLES_FILE             <- file.path(QCEW_META_DIR, "agg-level-titles.csv")
OWNERSHIP_TITLES_FILE             <- file.path(QCEW_META_DIR, "ownership-titles.csv")
COUNTY_MSA_CSA_CROSSWALK_FILE     <- file.path(QCEW_META_DIR, "qcew_county-msa-csa-crosswalk-2024.csv")
QCEW_NAICS_HIERARCHY_CROSSWALK_FILE <- file.path(QCEW_META_DIR, "qcew-naics-hierarchy-crosswalk.xlsx")

# =========================================================
# Check for and load QCEW metadata
# =========================================================
req_meta <- c(
  AREA_TITLES_FILE, INDUSTRY_TITLES_FILE, SIZE_TITLES_FILE,
  AGG_LEVEL_TITLES_FILE, OWNERSHIP_TITLES_FILE,
  COUNTY_MSA_CSA_CROSSWALK_FILE, QCEW_NAICS_HIERARCHY_CROSSWALK_FILE
)
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
SIZE_TITLES             <- suppressMessages(readr::read_csv(SIZE_TITLES_FILE))
AGG_LEVEL_TITLES        <- suppressMessages(readr::read_csv(AGG_LEVEL_TITLES_FILE))
OWNERSHIP_TITLES        <- suppressMessages(readr::read_csv(
  OWNERSHIP_TITLES_FILE,
  col_types = cols(own_code = col_character(), own_title = col_character())
))
COUNTY_MSA_CSA_CROSSWALK <- suppressMessages(readr::read_csv(COUNTY_MSA_CSA_CROSSWALK_FILE))
QCEW_NAICS_HIERARCHY_CROSSWALK <- suppressMessages(readxl::read_excel(
  QCEW_NAICS_HIERARCHY_CROSSWALK_FILE, sheet = "v2022"
))

# =========================================================
# Data loader and enrichment function (per year)
# =========================================================
# Note: User must update this path to their local directory
TAPESTRY_DIR_NAICS6D <- "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/Tapestry_Employment/contains_naics_999999_county_XX999/NAICS_6D"

read_and_enrich_year <- function(yr) {
  path <- file.path(TAPESTRY_DIR_NAICS6D, paste0(yr, ".csv"))
  if (!file.exists(path)) stop("Missing file: ", path)
  
  # Force correct types and pad keys to keep leading zeros
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
      area_fips  = str_pad(area_fips,  width = 5, side = "left", pad = "0"),
      naics_code = str_pad(naics_code, width = 6, side = "left", pad = "0")
    ) %>%
    # ---- enrichment (hard-coded joins) ----
  left_join(OWNERSHIP_TITLES, by = "own_code") %>%
    left_join(AREA_TITLES,      by = "area_fips") %>%
    left_join(INDUSTRY_TITLES, by = c("naics_code" = "industry_code"))
  
  # ---- totals for this year (from tap_emplvl_est_3) ----
  # county total (area_fips, year)
  county_totals <- df %>%
    group_by(area_fips, year) %>%
    summarise(total_employment_in_county = sum(tap_emplvl_est_3, na.rm = TRUE), .groups = "drop")
  
  # national industry total (naics_code, year)
  industry_totals <- df %>%
    group_by(naics_code, year) %>%
    summarise(total_industry_employment_in_nation = sum(tap_emplvl_est_3, na.rm = TRUE), .groups = "drop")
  
  # national total (year)
  national_totals <- df %>%
    group_by(year) %>%
    summarise(total_employment_national = sum(tap_emplvl_est_3, na.rm = TRUE), .groups = "drop")
  
  # bring totals back
  df <- df %>%
    left_join(county_totals,   by = c("area_fips", "year")) %>%
    left_join(industry_totals, by = c("naics_code", "year")) %>%
    left_join(national_totals, by = "year")
  
  # ---- Location Quotient (safe division) ----
  df <- df %>%
    mutate(
      area_share      = if_else(total_employment_in_county > 0,
                                tap_emplvl_est_3 / total_employment_in_county, NA_real_),
      national_share  = if_else(total_employment_national > 0,
                                total_industry_employment_in_nation / total_employment_national, NA_real_),
      location_quotient = if_else(!is.na(area_share) & !is.na(national_share) & national_share > 0,
                                  area_share / national_share, NA_real_)
    ) %>%
    select(
      area_fips, area_title,
      naics_code, industry_title,
      own_code, own_title,
      year, tap_emplvl_est_3,
      total_employment_in_county,
      total_industry_employment_in_nation,
      total_employment_national,
      area_share, national_share, location_quotient,
      everything()
    )
  
  df
}

# =========================================================
# NEW: Economic Complexity Calculation Function
# =========================================================
add_economic_complexity_metrics <- function(df) {
  
  message("Calculating complexity metrics for ", df$year[1], "...")
  
  # --- 1. Prepare M_ci (Revealed Comparative Advantage) Matrix ---
  # Based on equation 5[cite: 2], M_ci is 1 if RCA (location_quotient) >= 1.
  message("  Step 1/7: Creating RCA matrix (M_ci)...")
  df_clean <- df %>%
    select(area_fips, naics_code, location_quotient) %>%
    distinct(area_fips, naics_code, .keep_all = TRUE) %>%
    mutate(M_ci = if_else(!is.na(location_quotient) & location_quotient >= 1, 1, 0))
  
  # Create a sparse county-by-industry matrix for efficiency
  counties <- sort(unique(df_clean$area_fips))
  industries <- sort(unique(df_clean$naics_code))
  
  df_clean$county_idx <- match(df_clean$area_fips, counties)
  df_clean$industry_idx <- match(df_clean$naics_code, industries)
  
  M <- sparseMatrix(i = df_clean$county_idx,
                    j = df_clean$industry_idx,
                    x = df_clean$M_ci,
                    dims = c(length(counties), length(industries)),
                    dimnames = list(counties, industries))
  
  # --- 2. Calculate Diversity and Ubiquity ---
  # Diversity (eq. 6) [cite: 2] & Ubiquity (eq. 7) [cite: 2]
  message("  Step 2/7: Calculating Diversity and Ubiquity...")
  Diversity <- rowSums(M)
  Ubiquity <- colSums(M)
  
  # Create safe denominators for division to avoid Inf/-Inf
  Diversity_div <- ifelse(Diversity == 0, 1, Diversity)
  Ubiquity_div <- ifelse(Ubiquity == 0, 1, Ubiquity)
  
  # --- 3. Calculate ICI and ECI ---
  # Using the eigenvector method described in section 2.1[cite: 4, 6].
  # ECI is the 2nd largest eigenvector of M_tilde_C, and ICI for M_tilde_I.
  # We calculate ICI first, then derive ECI, as ECI is the average ICI
  # of industries present in a county.
  message("  Step 3/7: Calculating Industry Complexity (ICI) and Economic Complexity (ECI)...")
  
  # Construct the matrix for the ICI eigenvector calculation
  M_tilde_I_num <- t(M) %*% sweep(M, 1, Diversity_div, "/")
  M_tilde_I <- sweep(M_tilde_I_num, 1, Ubiquity_div, "/")
  
  # Eigen decomposition. The paper notes the second largest eigenvector is used.
  # Eigenvalues are sorted in decreasing order by default.
  eigen_I <- eigen(as.matrix(M_tilde_I))
  ICI_vector <- Re(eigen_I$vectors[, 2]) # Take real part to handle potential floating point inaccuracies
  
  # Derive ECI from ICI (as per Hausmann & Hidalgo)
  ECI_raw <- (M %*% ICI_vector) / Diversity_div
  ECI_vector <- as.numeric(ECI_raw)
  
  # Standardize vectors for interpretability (z-scores)
  ECI_vector[is.infinite(ECI_vector) | is.nan(ECI_vector)] <- NA
  ICI_vector[is.infinite(ICI_vector) | is.nan(ICI_vector)] <- NA
  
  ECI_std <- scale(ECI_vector)
  ICI_std <- scale(ICI_vector)
  
  ECI_df <- tibble(area_fips = counties, economic_complexity_index_county = as.numeric(ECI_std))
  ICI_df <- tibble(naics_code = industries, industry_complexity_index_score = as.numeric(ICI_std))
  
  # --- 4. Calculate Proximity (phi) ---
  # Based on co-location, as per equation 16.
  message("  Step 4/7: Calculating Proximity (phi)...")
  U <- t(M) %*% M # Co-occurrence matrix
  diag_U <- diag(U) # This is also the Ubiquity vector
  
  max_U_matrix <- outer(diag_U, diag_U, pmax)
  
  phi <- U
  phi[max_U_matrix > 0] <- U[max_U_matrix > 0] / max_U_matrix[max_U_matrix > 0]
  phi[max_U_matrix == 0] <- 0
  diag(phi) <- 0 # Proximity to self is set to 0
  
  # --- 5. Calculate Density ---
  # Measures relatedness of a county's industries to a target industry (eq. 19).
  message("  Step 5/7: Calculating Industry Density...")
  density_den <- colSums(phi)
  density_den_div <- ifelse(density_den == 0, 1, density_den)
  
  density_num <- M %*% phi
  density_matrix <- sweep(density_num, 2, density_den_div, FUN = "/")
  
  density_df <- as.data.frame(as.matrix(density_matrix)) %>%
    rownames_to_column(var = "area_fips") %>%
    pivot_longer(cols = -area_fips, names_to = "naics_code", values_to = "industry_density_for_county")
  
  # --- 6. Calculate Strategic Index (SI) ---
  # Measures value of absent industries weighted by density and ICI (eq. 21).
  message("  Step 6/7: Calculating Strategic Index (SI)...")
  M_absent <- 1 - M
  term_to_sum <- density_matrix * M_absent
  ICI_vector_named <- setNames(as.numeric(ICI_std), industries)
  
  # Handle NAs in ICI before sweep
  ICI_vector_named[is.na(ICI_vector_named)] <- 0
  
  term_to_sum_weighted <- sweep(term_to_sum, 2, ICI_vector_named, FUN = "*")
  SI_vector <- rowSums(term_to_sum_weighted)
  
  SI_df <- tibble(area_fips = counties, strategic_index = SI_vector)
  
  # --- 7. Calculate Strategic Gain (SG) ---
  # Estimates improvement by adding a specific nascent industry (eq. 22).
  message("  Step 7/7: Calculating Strategic Gain (SG)...")
  phi_norm_by_den <- sweep(phi, 2, density_den_div, FUN = "/")
  M_absent_ici <- sweep(M_absent, 2, ICI_vector_named, FUN = "*")
  
  SG_part1_matrix <- t(phi_norm_by_den %*% t(M_absent_ici))
  SG_part2_matrix <- sweep(density_matrix, 2, ICI_vector_named, FUN = "*")
  
  SG_matrix <- SG_part1_matrix - SG_part2_matrix
  
  SG_df <- as.data.frame(as.matrix(SG_matrix)) %>%
    rownames_to_column(var = "area_fips") %>%
    pivot_longer(cols = -area_fips, names_to = "naics_code", values_to = "strategic_gain")
  
  # --- Final Assembly: Join all new metrics back to the original dataframe ---
  message("  Joining all metrics back to the main data frame...")
  final_df <- df %>%
    mutate(
      industry_comparative_advantage_county = if_else(!is.na(location_quotient) & location_quotient >= 1, 1, 0)
    ) %>%
    left_join(ECI_df, by = "area_fips") %>%
    left_join(ICI_df, by = "naics_code") %>%
    left_join(density_df, by = c("area_fips", "naics_code")) %>%
    left_join(SI_df, by = "area_fips") %>%
    left_join(SG_df, by = c("area_fips", "naics_code"))
  
  message("...done for ", df$year[1], ".\n")
  return(final_df)
}

# =========================================================
# --------- Generate final data frames for each year --------
# --------- with enrichment, LQ, and complexity metrics ---
# =========================================================
TAPESTRY_2015 <- add_economic_complexity_metrics(read_and_enrich_year(2015))
TAPESTRY_2016 <- add_economic_complexity_metrics(read_and_enrich_year(2016))
TAPESTRY_2017 <- add_economic_complexity_metrics(read_and_enrich_year(2017))
TAPESTRY_2018 <- add_economic_complexity_metrics(read_and_enrich_year(2018))
TAPESTRY_2019 <- add_economic_complexity_metrics(read_and_enrich_year(2019))
TAPESTRY_2020 <- add_economic_complexity_metrics(read_and_enrich_year(2020))
TAPESTRY_2021 <- add_economic_complexity_metrics(read_and_enrich_year(2021))
TAPESTRY_2022 <- add_economic_complexity_metrics(read_and_enrich_year(2022))
TAPESTRY_2023 <- add_economic_complexity_metrics(read_and_enrich_year(2023))
TAPESTRY_2024 <- add_economic_complexity_metrics(read_and_enrich_year(2024))

# =========================================================
# GLIMPSE FINAL DATA FRAME TO VERIFY NEW COLUMNS
# =========================================================
glimpse(TAPESTRY_2024)
