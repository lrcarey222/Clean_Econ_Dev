# ============================================================
# MASTER PIPELINE (HARDENED/INSTRUMENTED) — v5 (No CIM)
# County x NAICS-6 → RCA/M + ECI/ICI + φ + Feasibility + ALQ/WLQ/WALQ + PA‑LQ
# + PA‑M + PA‑ECI/PA‑ICI + Ranks/Percentiles + Stress Tests & Diagnostics
#
# WHAT'S NEW vs v4
# - FIXED: sample totals check no longer uses `unique()` on a numeric vector
#          (which collapsed different counties that happened to have equal totals).
#          Now uses the authoritative `county_totals` table.
# - Added deeper diagnostics if any totals mismatch is detected.
# - Projected centroids for kNN isolate-bridging; extra row-sum checks after merge.
# - Safer z-score (handles constant vectors), extra eigen sanity checks.
# - More explicit invariants for LQ weighted means under ALL vs SAMPLE national scopes.
# - More memory/timing breadcrumbs; additional component correlation prints.
#
# CITATIONS (embedded for traceability):
# - Economic Complexity (RCA→M; method-of-reflections; normalized SVD; proximity φ; density):
#   Daboín (2018), eqs. (1)–(16).  :contentReference[oaicite:0]{index=0}
# - PA‑LQ components (LQ, ALQ, WLQ, WALQ) & geometric mean with ε; interpretation & attenuation:
#   PA‑LQ User Guide.  :contentReference[oaicite:1]{index=1}
# - ALQ/WLQ/WALQ motivation & construction (IO- and spatial‑augmented LQ family):
#   Measuring Industry Co‑location (Tian, Gottlieb & Goetz).  :contentReference[oaicite:2]{index=2}
# ============================================================

suppressPackageStartupMessages({
  # Core wrangling / viz
  library(tidyverse)   # dplyr, tidyr, ggplot2, readr, stringr, purrr, tibble
  library(janitor)     # clean_names
  library(glue)
  library(scales)
  
  # Spatial & weights
  library(tigris)
  library(sf)
  library(spdep)
  
  # Linear algebra / eigen
  library(Matrix)
  library(RSpectra)
  
  # Dates / I/O
  library(lubridate)
  library(fs)
  
  # Parallel / progress
  library(future)
  library(future.apply)
  library(progressr)
})

# ------------------------------ Global options & environment -----------------
options(
  tigris_use_cache       = TRUE,
  tigris_class           = "sf",
  readr.show_col_types   = FALSE,
  dplyr.summarise.inform = FALSE,
  scipen                 = 999,
  warn                   = 1L,     # promote warnings
  stringsAsFactors       = FALSE
)
sf::sf_use_s2(FALSE)              # planar ops for robust polygon adjacency
# Quiet a harmless macOS malloc message if it appears
try(Sys.setenv(MALLOC_STACK_LOGGING = "0"), silent = TRUE)
set.seed(42)

# Keep BLAS from over‑subscribing when futures are used
Sys.setenv(
  OMP_NUM_THREADS        = 1L,
  MKL_NUM_THREADS        = 1L,
  OPENBLAS_NUM_THREADS   = 1L,
  VECLIB_MAXIMUM_THREADS = 1L
)

# ------------------------------ Feature toggles ------------------------------
EXPORT_FINAL              <- TRUE  # write CSVs
WRITE_DIAGNOSTICS_TABLES  <- TRUE  # write isolates table etc.
PLOT_DIAGNOSTICS          <- TRUE  # save ggplots
CONNECT_ISOLATES_KNN      <- TRUE  # connect isolates (e.g., HI/AK) via kNN
ENABLE_PARALLEL           <- TRUE   # multisession futures
MAX_WORKERS               <- NA     # NA → min(available-1, 4)
FUTURE_GLOBALS_MAXSIZE_GB <- as.numeric(Sys.getenv("FUTURE_GLOBALS_MAXSIZE_GB", "4"))
DEBUG_VERBOSE             <- TRUE   # verbose debug printing
FAIL_FAST                 <- TRUE   # stop on critical assertion failure

# Stress tests
STRESS_TESTS_LIGHT        <- TRUE   # bounds, correlations, invariants
STRESS_TESTS_HEAVY        <- TRUE  # permutation nulls; epsilon sensitivity grid
N_PERMUTATIONS_ECI        <- 10     # # permutations if HEAVY
EPS_GRID                  <- c(1e-9, 1e-6, 1e-4)  # epsilon sweep for PA‑LQ if HEAVY

# Domain parameters
KEEP_STATES   <- setdiff(unique(tigris::fips_codes$state), c("AS","GU","MP","PR","VI")) # 50+DC
NAICS_LEVEL   <- 6
DROP_NAICS    <- c("999999")
RCA_THRESHOLD <- 1.0
PA_THRESHOLD  <- 1.0
EPS_PALQ      <- 1e-6
SNAP_TOL      <- 1e-6
KNN_K         <- 1
CRS_GEO       <- 4326
CRS_PROJ      <- 5070
PLOT_SAMPLE_N <- 200000

# *** IMPORTANT: Consistency of national shares vs analysis sample ***
# If TRUE, *national* totals for RCA/LQ are computed over the exact counties in analysis (50+DC),
# which makes the employment-weighted mean LQ across those counties equal to 1 (per industry).
# If FALSE, national totals use ALL rows in the raw input (may include territories/misc.),
# in which case the weighted mean across the 50+DC sample will generally be < 1.
USE_SAMPLE_FOR_NATIONAL_SHARES <- TRUE

# ============================================================
# Utilities (debugging, assertions, timers, helpers)
# ============================================================
.now <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
.msg <- function(level="INFO", text="") cat(sprintf("[%s] %s — %s\n", level, .now(), text))
.dbg <- function(text) if (isTRUE(DEBUG_VERBOSE)) .msg("DEBUG", text)
`%||%` <- function(a,b) if (is.null(a) || length(a)==0 || (length(a)==1 && (is.na(a) || a==""))) b else a
ensure_dir <- function(p) { if (!dir.exists(p)) dir.create(p, recursive = TRUE, showWarnings = FALSE) }
slugify <- function(x) x %>% as.character() %>% stringr::str_to_lower() %>% stringr::str_replace_all("&"," and ") %>% stringr::str_replace_all("[^a-z0-9]+","_") %>% stringr::str_replace_all("_+","_") %>% stringr::str_replace_all("^_|_$","")
mem_note <- function(label="") { g <- gc(); used_gb <- sum(g[, "used"]) * 8 / 1024^3; cat(sprintf("[MEM]  %-48s  ~%.2f GB used (gc)\n", label, used_gb)) }
tick <- local({ .t0 <- Sys.time(); function(step) { dt <- difftime(Sys.time(), .t0, units="secs"); cat(sprintf("[TIME] %-48s  +%6.1fs\n", step, as.numeric(dt))); .t0 <<- Sys.time() }})
safe_select <- function(df, cols) dplyr::select(df, any_of(cols))

# Assertions (defensive)
assert_true <- function(cond, msg) { if (!isTRUE(cond)) { .msg("ERROR", msg); if (FAIL_FAST) stop(msg, call. = FALSE) else warning(msg, call. = FALSE) } }
assert_cols <- function(df, cols, label="") {
  missing <- setdiff(cols, names(df))
  if (length(missing)) assert_true(FALSE, sprintf("Missing column(s) in %s: %s", label, paste(missing, collapse=", ")))
  invisible(TRUE)
}

# Small helpers
.safe_read_csv <- function(path, ...) {
  .dbg(glue("Reading CSV: {path}"))
  assert_true(fs::file_exists(path), glue("Input CSV not found: {path}"))
  out <- try(readr::read_csv(path, show_col_types = FALSE, progress = FALSE, ...), silent = TRUE)
  if (inherits(out, "try-error")) stop(glue("Failed to read CSV: {path}"))
  out
}
safe_detect_emp_col <- function(df) {
  cand <- names(df) %>% tolower() %>% {.[stringr::str_detect(., "empl|employ|jobs|emp_")]}
  if (length(cand)==0) stop("No employment column found. Rename to 'employment' or set manually.")
  cand[[1]]
}
eig2_sym <- function(S, label="") {
  stopifnot(nrow(S)==ncol(S))
  eig <- tryCatch(RSpectra::eigs_sym(S, k=2, which="LM"), error=function(e) NULL)
  if (is.null(eig)) {
    message(glue("RSpectra failed on {label}; falling back to base::eigen (dense)."))
    e <- eigen(as.matrix(S), symmetric=TRUE); ord <- order(Re(e$values), decreasing=TRUE)
    eig <- list(values = e$values[ord][1:2], vectors = e$vectors[,ord][1:2,drop=FALSE])
  }
  list(v1 = as.numeric(eig$vectors[,1]),
       v2 = as.numeric(eig$vectors[,2]),
       lambda1 = as.numeric(eig$values[1]),
       lambda2 = as.numeric(eig$values[2]),
       gap = as.numeric(eig$values[1] - eig$values[2]))
}
zscore <- function(x) {
  # robust to constant vectors (avoid NA from sd=0)
  mu <- mean(x, na.rm=TRUE); s <- sd(x, na.rm=TRUE)
  if (!is.finite(s) || s == 0) return(rep(0, length(x)))
  (x - mu) / s
}
wide_add <- function(mat, value_name, counties, industries) {
  stopifnot(nrow(mat)==nrow(counties), ncol(mat)==nrow(industries))
  out <- as_tibble(as.matrix(mat)); colnames(out) <- industries$`Industry Code`
  out %>% mutate(`County GEOID` = counties$`County GEOID`) %>%
    relocate(`County GEOID`) %>% pivot_longer(-`County GEOID`, names_to="Industry Code", values_to=value_name)
}

# Parallel setup/teardown
setup_parallel <- function(enable=TRUE, max_workers=NA) {
  out <- list(enabled = FALSE, workers = 1L, plan = "sequential")
  if (!enable) { .msg("WARN","Parallel disabled; using sequential."); if (requireNamespace("progressr", quietly = TRUE)) progressr::handlers(global = TRUE); return(out) }
  options(future.globals.maxSize = FUTURE_GLOBALS_MAXSIZE_GB * 1024^3, future.rng.onMisuse = "ignore")
  cores <- tryCatch(future::availableCores(), error=function(e) { .msg("WARN","availableCores() failed; using detectCores()"); max(1L, parallel::detectCores(logical=TRUE)) })
  workers <- if (is.na(max_workers) || !is.finite(max_workers) || max_workers <= 0) min(max(1L, cores-1L), 4L) else max(1L, min(as.integer(max_workers), cores))
  future::plan(future::multisession, workers = workers)
  if (requireNamespace("progressr", quietly = TRUE)) progressr::handlers(global = TRUE)
  .msg("INFO", sprintf("Parallel plan: multisession with %d worker(s); globals max=%.1f GiB", workers, FUTURE_GLOBALS_MAXSIZE_GB))
  out$enabled <- TRUE; out$workers <- workers; out$plan <- "multisession"; out
}
teardown_parallel <- function() { future::plan(future::sequential); .msg("INFO","Parallel plan reset → sequential.") }
PAR <- setup_parallel(ENABLE_PARALLEL, MAX_WORKERS)
on.exit(teardown_parallel(), add = TRUE)

# ============================================================
# ---- A) Load County×NAICS employment, titles; build RCA/M
# ============================================================

# ---- User paths (edit if needed)
path_tapestry_csv    <- "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/Tapestry_Employment/contains_naics_999999_county_XX999/NAICS_6D/2024.csv"
path_industry_titles <- "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/BLS_QCEW/metadata/industry-titles.csv"

.msg("INFO","Loading Tapestry and BLS industry titles…")
tap_raw   <- .safe_read_csv(path_tapestry_csv)

# Robust BLS loader: tolerate multiple header variants
bls_raw   <- .safe_read_csv(path_industry_titles) %>% janitor::clean_names()
code_col  <- intersect(names(bls_raw), c("industry_code","code","naics","naics_code")) %>% .[1] %||% stop("No code column in BLS titles.")
name_col  <- intersect(names(bls_raw), c("industry_name","industry_title","name","title")) %>% .[1] %||% stop("No name column in BLS titles.")
bls_titles <- bls_raw %>%
  transmute(`Industry Code` = as.character(.data[[code_col]]),
            `Industry Name` = as.character(.data[[name_col]]))

if (DEBUG_VERBOSE) {
  cat("\n[DEBUG] tap_raw (head)\n"); print(utils::head(tap_raw, 5))
  cat("\n[DEBUG] bls_titles (head)\n"); print(utils::head(bls_titles, 5))
}

assert_cols(tap_raw, c("area_fips","naics_code"), "tapestry_csv")
emp_col <- safe_detect_emp_col(tap_raw)

tap_clean_all <- tap_raw %>%
  rename_with(~"employment", all_of(emp_col)) %>%
  mutate(`County GEOID` = stringr::str_pad(as.character(area_fips), 5, pad="0"),
         `Industry Code`= as.character(naics_code),
         employment     = suppressWarnings(as.numeric(employment))) %>%
  filter(nchar(`Industry Code`)==NAICS_LEVEL, !`Industry Code` %in% DROP_NAICS)

assert_true(nrow(tap_clean_all) > 0, "No rows after filtering to 6‑digit NAICS and excluding drops.")
if (DEBUG_VERBOSE) {
  .dbg(glue("tap_clean_all rows: {nrow(tap_clean_all)}; unique counties: {dplyr::n_distinct(tap_clean_all$`County GEOID`)}; unique industries: {dplyr::n_distinct(tap_clean_all$`Industry Code`)}"))
}

# ---- County geometries (TIGER/Line)
.msg("INFO","Loading county geometries (TIGER/Line 2024)…")
counties_sf <- tryCatch({
  tigris::counties(year=2024, cb=FALSE, class="sf") %>%
    mutate(STATEFP = substr(GEOID,1,2)) %>%
    left_join(tigris::fips_codes %>% distinct(state_code, state) %>% rename(STATEFP=state_code, STUSPS=state), by="STATEFP") %>%
    filter(STUSPS %in% KEEP_STATES) %>% sf::st_make_valid()
}, error = function(e) stop("Failed to load county geometries via tigris."))
assert_cols(counties_sf, c("GEOID","NAMELSAD","STUSPS"), "counties_sf")

counties_lu <- counties_sf %>% sf::st_drop_geometry() %>%
  transmute(`County GEOID`=GEOID, `County Name` = NAMELSAD, `State Abbreviation` = STUSPS)

tap_with_names <- tap_clean_all %>% left_join(counties_lu, by="County GEOID")

missing_in_tigris <- tap_clean_all %>% distinct(`County GEOID`) %>% anti_join(counties_lu, by="County GEOID")
if (nrow(missing_in_tigris) > 0) .msg("WARN", glue("County GEOIDs in Tapestry not in TIGRIS (50+DC): {nrow(missing_in_tigris)} — excluded from analysis sample."))

# ---- Totals & join titles (ALL vs SAMPLE)
# All rows (may include territories/misc.)
industry_totals_all <- tap_clean_all %>%
  group_by(`Industry Code`) %>%
  summarise(`Industry Employment in Nation (ALL)` = sum(employment, na.rm=TRUE), .groups="drop")
national_total_all <- sum(industry_totals_all$`Industry Employment in Nation (ALL)`, na.rm=TRUE)

# Analysis sample (50+DC only: keep rows that matched TIGRIS)
tap_5051 <- tap_with_names %>% filter(!is.na(`County Name`))
assert_true(nrow(tap_5051) > 0, "No 50+DC county rows available after left join.")

industry_totals_5051 <- tap_5051 %>%
  group_by(`Industry Code`) %>%
  summarise(`Industry Employment in Nation (SAMPLE)` = sum(employment, na.rm=TRUE), .groups="drop")
national_total_5051 <- sum(industry_totals_5051$`Industry Employment in Nation (SAMPLE)`, na.rm=TRUE)

county_totals <- tap_5051 %>% group_by(`County GEOID`,`County Name`,`State Abbreviation`) %>%
  summarise(`Total Employment in County` = sum(employment, na.rm=TRUE), .groups="drop")

.msg("INFO", glue("National total employment (ALL):    {format(national_total_all, big.mark=',')}"))
.msg("INFO", glue("National total employment (50+DC): {format(national_total_5051, big.mark=',')}"))
if (abs(national_total_all - national_total_5051) > 0) {
  .msg("EXPLAIN","Using 50+DC SAMPLE totals for RCA/LQ yields invariant LQ mean=1 across the analysis units; using ALL totals would not.")
}

# Prepare dat0 with titles and BOTH sets of totals for transparency
dat0 <- tap_5051 %>%
  left_join(bls_titles, by="Industry Code") %>%
  left_join(industry_totals_all,  by="Industry Code") %>%
  left_join(industry_totals_5051, by="Industry Code") %>%
  left_join(county_totals, by=c("County GEOID","County Name","State Abbreviation")) %>%
  mutate(
    `Total Employment in Nation (ALL)`    = national_total_all,
    `Total Employment in Nation (SAMPLE)` = national_total_5051
  )

# ---- Sanity checks: invariants on raw aggregation
sum_emp_all <- sum(tap_clean_all$employment, na.rm=TRUE)
sum_ind_all <- sum(industry_totals_all$`Industry Employment in Nation (ALL)`, na.rm=TRUE)
sum_ind_smp <- sum(industry_totals_5051$`Industry Employment in Nation (SAMPLE)`, na.rm=TRUE)

# CRITICAL FIX: compute county total correctly by summing the authoritative county_totals table.
tot_50dc    <- county_totals %>% summarise(tt=sum(`Total Employment in County`, na.rm=TRUE)) %>% pull(tt)

# For debugging, show what the (incorrect) unique()-based sum would have produced
tot_50dc_bad_unique <- sum(unique(dat0$`Total Employment in County`), na.rm=TRUE)
if (abs(tot_50dc_bad_unique - tot_50dc) > 0) {
  .msg("WARN", glue("Demonstration: unique()-based county sum would be {format(tot_50dc_bad_unique,big.mark=',')} vs correct {format(tot_50dc,big.mark=',')} (duplicates across equal-valued counties got dropped)."))
}

if (DEBUG_VERBOSE) {
  cat("\n[DEBUG] Totals sanity\n"); print(tibble(sum_emp_all, sum_ind_all, sum_ind_smp, tot_50dc))
}
assert_true(all.equal(sum_emp_all, sum_ind_all, tolerance=0) == TRUE, "Sum mismatch: per‑row employment vs ALL industry totals.")
assert_true(all.equal(sum_ind_smp, tot_50dc, tolerance=0) == TRUE, "Sum mismatch: SAMPLE industry totals vs sum of county totals.")

# Deep diagnostics if something still mismatches (not expected after fix)
if (!isTRUE(all.equal(sum_ind_smp, tot_50dc))) {
  .msg("DEBUG","Running mismatch diagnostics (per industry & per county)…")
  per_county_check <- tap_5051 %>% group_by(`County GEOID`) %>%
    summarise(emp_by_sum = sum(employment, na.rm=TRUE), .groups="drop") %>%
    left_join(county_totals, by="County GEOID") %>%
    mutate(diff = emp_by_sum - `Total Employment in County`) %>% filter(abs(diff) > 0)
  per_ind_check <- tap_5051 %>% group_by(`Industry Code`) %>%
    summarise(emp_by_sum = sum(employment, na.rm=TRUE), .groups="drop") %>%
    left_join(industry_totals_5051, by="Industry Code") %>%
    mutate(diff = emp_by_sum - `Industry Employment in Nation (SAMPLE)`) %>% filter(abs(diff) > 0)
  .dbg(glue("Per-county diffs n={nrow(per_county_check)}; per-industry diffs n={nrow(per_ind_check)}"))
  if (nrow(per_county_check)) print(utils::head(per_county_check, 10))
  if (nrow(per_ind_check))  print(utils::head(per_ind_check, 10))
}

# ---- RCA/LQ choice of national scope (critical for weighted-mean invariant)
if (USE_SAMPLE_FOR_NATIONAL_SHARES) {
  .msg("INFO","RCA/LQ will use SAMPLE (50+DC) national shares → invariant LQ mean=1 across analysis counties.")
  dat_rca <- dat0 %>%
    mutate(
      share_ci = if_else(`Total Employment in County`>0, employment/`Total Employment in County`, NA_real_),
      share_i  = if_else(`Total Employment in Nation (SAMPLE)`>0, `Industry Employment in Nation (SAMPLE)`/`Total Employment in Nation (SAMPLE)`, NA_real_),
      `Revealed Comparative Advantage (RCA)` = share_ci/share_i,
      `Comparative-Advantage Status (M)`     = as.integer(`Revealed Comparative Advantage (RCA)` >= RCA_THRESHOLD)
    )
} else {
  .msg("WARN","RCA/LQ will use ALL national shares (may include non‑50+DC rows) → weighted mean LQ across 50+DC will not be 1 by design.")
  dat_rca <- dat0 %>%
    mutate(
      share_ci = if_else(`Total Employment in County`>0, employment/`Total Employment in County`, NA_real_),
      share_i  = if_else(`Total Employment in Nation (ALL)`>0, `Industry Employment in Nation (ALL)`/`Total Employment in Nation (ALL)`, NA_real_),
      `Revealed Comparative Advantage (RCA)` = share_ci/share_i,
      `Comparative-Advantage Status (M)`     = as.integer(`Revealed Comparative Advantage (RCA)` >= RCA_THRESHOLD)
    )
}

# Diversity / Ubiquity (counts of M)
diversity <- dat_rca %>% group_by(`County GEOID`,`County Name`,`State Abbreviation`) %>%
  summarise(`Diversity` = sum(`Comparative-Advantage Status (M)`, na.rm=TRUE), .groups="drop")
ubiquity  <- dat_rca %>% group_by(`Industry Code`,`Industry Name`) %>%
  summarise(`Industry Ubiquity` = sum(`Comparative-Advantage Status (M)`, na.rm=TRUE), .groups="drop")

dat_rca <- dat_rca %>% left_join(diversity, by=c("County GEOID","County Name","State Abbreviation")) %>%
  left_join(ubiquity,  by=c("Industry Code","Industry Name"))

if (nrow(diversity %>% filter(Diversity==0))>0) .msg("WARN","Some counties have Diversity==0 (allowed).")
if (nrow(ubiquity  %>% filter(`Industry Ubiquity`==0))>0) .msg("WARN","Some industries have Ubiquity==0 (allowed).")

mem_note("After RCA/M+div/ubi")
tick("A) complete")

# ============================================================
# ---- B) Build M; ECI/ICI via normalized SVD (Daboín 2018)  [CITED]
# ============================================================
counties <- dat_rca %>% distinct(`County GEOID`,`County Name`,`State Abbreviation`) %>% arrange(`County GEOID`) %>% mutate(c_index=row_number())
industries <- dat_rca %>% distinct(`Industry Code`,`Industry Name`) %>% arrange(`Industry Code`) %>% mutate(i_index=row_number())

dat_idx <- dat_rca %>%
  inner_join(counties,  by=c("County GEOID","County Name","State Abbreviation")) %>%
  inner_join(industries, by=c("Industry Code","Industry Name"))

M_coo <- dat_idx %>% select(c_index, i_index, `Comparative-Advantage Status (M)`) %>% filter(`Comparative-Advantage Status (M)`==1L)
M <- sparseMatrix(i=M_coo$c_index, j=M_coo$i_index, x=1L,
                  dims=c(nrow(counties), nrow(industries)),
                  dimnames=list(counties$`County GEOID`, industries$`Industry Code`))
assert_true(all(dim(M) == c(nrow(counties), nrow(industries))), "M dimension mismatch.")
.dbg(glue("M has {sum(M)} ones, {nrow(M)}x{ncol(M)}"))

kc0 <- Matrix::rowSums(M); ki0 <- Matrix::colSums(M)
kc0_safe <- ifelse(kc0>0, kc0, 1); ki0_safe <- ifelse(ki0>0, ki0, 1)

D_c_inv_sqrt <- Diagonal(x=as.numeric(1/sqrt(kc0_safe)))
D_i_inv_sqrt <- Diagonal(x=as.numeric(1/sqrt(ki0_safe)))
B  <- D_c_inv_sqrt %*% M %*% D_i_inv_sqrt
Sc <- B %*% t(B)
Si <- t(B) %*% B

.msg("INFO","Computing ECI eigenvectors (normalized SVD)…")
ec <- eig2_sym(Sc, "B B'")
ECI_raw <- ec$v2; ECI_z <- zscore(ECI_raw)

emp_by_county <- county_totals %>% arrange(`County GEOID`) %>% pull(`Total Employment in County`)
if (stats::cor(ECI_z, emp_by_county, use="pairwise.complete.obs") < 0) ECI_z <- -ECI_z
.msg("INFO", glue("ECI eigen λ1={round(ec$lambda1,6)}, λ2={round(ec$lambda2,6)}, gap={round(ec$gap,6)}"))
.msg("EXPLAIN","A sizable spectral gap implies a stable second component; sign aligned to correlate positively with county employment (scale).")

.msg("INFO","Computing ICI eigenvectors (normalized SVD)…")
ei <- eig2_sym(Si, "B' B")
ICI_raw <- ei$v2; ICI_z <- zscore(ICI_raw)
.msg("INFO", glue("ICI eigen λ1={round(ei$lambda1,6)}, λ2={round(ei$lambda2,6)}, gap={round(ei$gap,6)}"))

# Identity checks (SVD equivalence)
u_from_v <- as.numeric(B %*% ICI_raw)
v_from_u <- as.numeric(t(B) %*% ECI_raw)
corr_uv  <- suppressWarnings(stats::cor(zscore(u_from_v), zscore(ECI_raw), use="pairwise.complete.obs"))
corr_vu  <- suppressWarnings(stats::cor(zscore(v_from_u), zscore(ICI_raw), use="pairwise.complete.obs"))
.msg("INFO", glue("SVD checks: cor(B v2, u2)={round(corr_uv,6)}; cor(B' u2, v2)={round(corr_vu,6)}"))
assert_true(abs(corr_uv)>0.999 && abs(corr_vu)>0.999, "SVD identity checks failed (u/v mismatch).")

# Normalized construction implies top singular value near 1
assert_true(abs(ec$lambda1 - 1) < 1e-6 && abs(ei$lambda1 - 1) < 1e-6, "Top singular value not ~1; check normalization.")

ECI_tbl <- counties  %>% transmute(`County GEOID`,`County Name`,`State Abbreviation`,
                                   `County Economic Complexity Index (ECI)` = as.numeric(ECI_z))
ICI_tbl <- industries %>% transmute(`Industry Code`,`Industry Name`,
                                    `Industry Complexity Index (ICI)` = as.numeric(ICI_z))

dat_rca <- dat_rca %>% left_join(ECI_tbl, by=c("County GEOID","County Name","State Abbreviation")) %>%
  left_join(ICI_tbl, by=c("Industry Code","Industry Name"))

mem_note("After ECI/ICI")
tick("B) complete")

# ============================================================
# ---- C) φ proximity & Feasibility density ∈ [0,1] (Daboín eq. 15–16)  [CITED]
# ============================================================
U <- t(M) %*% M
diagU <- diag(U)
den_max <- pmax(outer(as.numeric(diagU), as.numeric(diagU), pmax), 1)
phi_dense <- as.matrix(U) / den_max; diag(phi_dense) <- 0
phi <- Matrix(phi_dense, sparse=TRUE)

# Stress: symmetry and bounds
if (STRESS_TESTS_LIGHT) {
  assert_true(max(abs(phi - t(phi))) < 1e-10, "φ is not symmetric.")
  assert_true(min(phi_dense) >= -1e-12 && max(phi_dense) <= 1+1e-12, "φ out of [0,1].")
}

phi_colsum <- Matrix::colSums(phi); phi_colsum_safe <- ifelse(phi_colsum>0, phi_colsum, 1)
density_mat <- (M %*% phi)
rownames(density_mat) <- counties$`County GEOID`
colnames(density_mat) <- industries$`Industry Code`
density_mat <- sweep(as.matrix(density_mat), 2, phi_colsum_safe, "/")
assert_true(all(density_mat >= -1e-9 & density_mat <= 1+1e-9, na.rm=TRUE), "Feasibility density out of [0,1] bounds.")

density_df <- as_tibble(density_mat) %>% mutate(`County GEOID`=counties$`County GEOID`) %>%
  relocate(`County GEOID`) %>% pivot_longer(-`County GEOID`, names_to="Industry Code", values_to="Co-location Density (Feasibility)")

dat_rca <- dat_rca %>% left_join(density_df, by=c("County GEOID","Industry Code"))
mem_note("After φ & feasibility density")
tick("C) complete")

# ============================================================
# ---- D) Spatial weights W (queen) + WLQ / WALQ (Tian et al.)  [CITED]
# ============================================================
.msg("INFO","Building queen‑contiguity spatial weights (W)…")
relevant_counties_sf <- counties_sf %>% semi_join(counties %>% transmute(GEOID=`County GEOID`), by="GEOID") %>% arrange(GEOID)
assert_true(all(relevant_counties_sf$GEOID == counties$`County GEOID`), "County ordering mismatch between sf and index.")

nb_queen <- spdep::poly2nb(relevant_counties_sf, queen= TRUE, snap=SNAP_TOL)
W_listw  <- spdep::nb2listw(nb_queen, style="W", zero.policy=TRUE)
W_mat    <- spdep::listw2mat(W_listw)
W        <- Matrix(W_mat, sparse=TRUE)

rowsums_W <- Matrix::rowSums(W)
iso_idx <- which(spdep::card(nb_queen)==0)
if (STRESS_TESTS_LIGHT) {
  ok_rows <- setdiff(seq_len(nrow(W)), iso_idx)
  assert_true(max(abs(rowsums_W[ok_rows] - 1)) < 1e-10, "Row‑standardized W has rows not summing to 1 (non‑isolates).")
}
if (length(iso_idx)>0) {
  iso_tbl <- relevant_counties_sf %>% sf::st_drop_geometry() %>% slice(iso_idx) %>%
    transmute(`County GEOID`=GEOID, `County Name`=NAMELSAD, `State Abbreviation`=STUSPS)
  .msg("WARN", glue("{nrow(iso_tbl)} isolate county(ies); WLQ/WALQ=0 there (zero.policy=TRUE)."))
  if (DEBUG_VERBOSE) print(iso_tbl)
}
comp <- spdep::n.comp.nb(nb_queen)
.msg("INFO", glue("Adjacency graph has {comp$nc} component(s)."))
.msg("EXPLAIN","WLQ/WALQ reflect spatial spillovers; isolates (e.g., some HI/AK counties) have WLQ=WALQ=0 with zero.policy=TRUE.")

if (CONNECT_ISOLATES_KNN && length(iso_idx)>0) {
  .msg("INFO", glue("CONNECT_ISOLATES_KNN=TRUE: connecting isolates via k={KNN_K} nearest neighbours."))
  # Use projected centroids for kNN stability
  cent_proj <- sf::st_centroid(sf::st_transform(relevant_counties_sf, CRS_PROJ))
  coords    <- sf::st_coordinates(cent_proj)
  knn <- spdep::knearneigh(coords, k=KNN_K)
  nb_knn <- spdep::knn2nb(knn)
  nb_merged <- nb_queen
  for (i in iso_idx) nb_merged[[i]] <- nb_knn[[i]]
  nb_queen <- nb_merged; W_listw <- spdep::nb2listw(nb_queen, style="W", zero.policy=TRUE)
  W_mat <- spdep::listw2mat(W_listw); W <- Matrix(W_mat, sparse=TRUE)
  rowsums_W <- Matrix::rowSums(W)
  if (STRESS_TESTS_LIGHT) {
    ok_rows <- setdiff(seq_len(nrow(W)), which(spdep::card(nb_queen)==0))
    assert_true(max(abs(rowsums_W[ok_rows] - 1)) < 1e-10, "Row sums after kNN merge not ~1 for non‑isolates.")
  }
}

mem_note("After W construction")
tick("D) complete")

# ============================================================
# ---- E) LQ, ALQ, WLQ, WALQ; PA‑LQ (ε‑geometric mean; LQ gate)
# ============================================================
LQ_wide <- dat_rca %>% select(`County GEOID`,`Industry Code`,`Revealed Comparative Advantage (RCA)`) %>%
  rename(`Location Quotient (LQ)` = `Revealed Comparative Advantage (RCA)`) %>%
  pivot_wider(names_from = `Industry Code`, values_from = `Location Quotient (LQ)`) %>% arrange(`County GEOID`)
assert_true(all(LQ_wide$`County GEOID` == counties$`County GEOID`), "LQ_wide county order mismatch.")
LQ_mat <- as.matrix(LQ_wide[,-1]); LQ_mat[is.na(LQ_mat)] <- 0

# Invariant: employment‑weighted mean LQ across ANALYSIS counties equals 1 *IFF*
# national shares used in RCA/LQ were computed over the same ANALYSIS set.
if (STRESS_TESTS_LIGHT) {
  emp_by_county_vec <- county_totals %>% arrange(`County GEOID`) %>% pull(`Total Employment in County`)
  check_means <- function(x_col) sum(x_col * emp_by_county_vec) / sum(emp_by_county_vec)
  LQ_weighted_means <- apply(LQ_mat, 2, check_means)
  
  if (USE_SAMPLE_FOR_NATIONAL_SHARES) {
    assert_true(max(abs(LQ_weighted_means - 1)) < 1e-8,
                "Employment‑weighted mean LQ not ~1 for some industries (SAMPLE scope).")
  } else {
    # With ALL totals, the expected mean across 50+DC equals E_i(50+DC)/E_i(ALL). Report it.
    expected_ratio <- industry_totals_5051 %>%
      left_join(industry_totals_all, by="Industry Code") %>%
      mutate(ratio = `Industry Employment in Nation (SAMPLE)` / `Industry Employment in Nation (ALL)`) %>%
      arrange(`Industry Code`) %>% pull(ratio)
    max_dev <- max(abs(LQ_weighted_means - expected_ratio), na.rm=TRUE)
    .msg("EXPLAIN", glue("Using ALL totals: weighted mean LQ across 50+DC equals E_i(50+DC)/E_i(ALL). Max deviation vs expectation = {signif(max_dev,6)}."))
  }
}

# ALQ via φ (implicit co‑location proximity).  [CITED: Daboín proximity; Tian et al. for ALQ intuition]
phi_nodiag <- phi; diag(phi_nodiag) <- 0
phi_rowsum <- Matrix::rowSums(phi_nodiag); phi_rowsum_safe <- ifelse(phi_rowsum>0, phi_rowsum, 1)
phi_std    <- phi_nodiag / phi_rowsum_safe

ALQ_phi_mat  <- LQ_mat %*% t(as.matrix(phi_std))
WLQ_mat      <- as.matrix(W %*% LQ_mat)
WALQ_phi_mat <- as.matrix(W %*% ALQ_phi_mat)

# PA‑LQ = geometric mean with ε; force 0 when LQ==0 (gate).  [CITED: PA‑LQ User Guide]
LQ_nonzero <- (LQ_mat > 0)
PA_LQ_mat  <- (LQ_mat * (ALQ_phi_mat + EPS_PALQ) * (WLQ_mat + EPS_PALQ) * (WALQ_phi_mat + EPS_PALQ))^(1/4)
PA_LQ_mat[!LQ_nonzero] <- 0

# Distribution sanity (attenuation of extremes)
if (STRESS_TESTS_LIGHT) {
  lq_max   <- max(LQ_mat, na.rm=TRUE);    palq_max <- max(PA_LQ_mat, na.rm=TRUE)
  lq_q95   <- stats::quantile(LQ_mat[LQ_mat>0], 0.95, na.rm=TRUE)
  palq_q95 <- stats::quantile(PA_LQ_mat[PA_LQ_mat>0], 0.95, na.rm=TRUE)
  .msg("EXPLAIN", glue("LQ attenuation: max(LQ)={round(lq_max,2)} vs max(PA‑LQ)={round(palq_max,2)}; 95th pct LQ={round(lq_q95,2)} vs PA‑LQ={round(palq_q95,2)}."))
}

ALQ_df   <- wide_add(ALQ_phi_mat,  "Relatedness-Weighted Location Quotient (ALQ-Φ)",                 counties, industries)
WLQ_df   <- wide_add(WLQ_mat,      "Spatially-Lagged Location Quotient (WLQ)",                        counties, industries)
WALQ_df  <- wide_add(WALQ_phi_mat, "Spatially-Lagged Relatedness-Weighted LQ (WALQ-Φ)",              counties, industries)
PALQ_df  <- wide_add(PA_LQ_mat,    "Proximity-Adjusted Location Quotient (PA-LQ)",                   counties, industries)

mem_note("After LQ/ALQ/WLQ/WALQ/PA-LQ")
tick("E) complete")

# ============================================================
# ---- F) First pass table
# ============================================================
first_pass <- dat_rca %>%
  left_join(ALQ_df,  by=c("County GEOID","Industry Code")) %>%
  left_join(WLQ_df,  by=c("County GEOID","Industry Code")) %>%
  left_join(WALQ_df, by=c("County GEOID","Industry Code")) %>%
  left_join(PALQ_df, by=c("County GEOID","Industry Code")) %>%
  arrange(`County GEOID`,`Industry Code`) %>%
  mutate(`Industry Employment in County` = employment) %>%
  select(
    `County Name`,`County GEOID`,`State Abbreviation`,
    `Industry Code`,`Industry Name`,
    `Industry Employment in County`,
    `Total Employment in County`,
    `Industry Employment in Nation (SAMPLE)`,
    `Total Employment in Nation (SAMPLE)`,
    `Industry Employment in Nation (ALL)`,
    `Total Employment in Nation (ALL)`,
    `Revealed Comparative Advantage (RCA)`,
    `Comparative-Advantage Status (M)`,
    `Diversity`,
    `Industry Ubiquity`,
    `County Economic Complexity Index (ECI)`,
    `Industry Complexity Index (ICI)`,
    `Co-location Density (Feasibility)`,
    `Relatedness-Weighted Location Quotient (ALQ-Φ)`,
    `Spatially-Lagged Location Quotient (WLQ)`,
    `Spatially-Lagged Relatedness-Weighted LQ (WALQ-Φ)`,
    `Proximity-Adjusted Location Quotient (PA-LQ)`
  )

mem_note("After first_pass assembly")
tick("F) complete")

# ============================================================
# ---- G) Second pass: PA-M, PA‑Feasibility, PA‑ECI/PA‑ICI
# ============================================================
.msg("INFO","Computing proximity‑adjusted gates & complexity (PA‑M; PA‑ECI/PA‑ICI)…")
PA_M_df <- PALQ_df %>%
  mutate(`Proximity-Adjusted Comparative-Advantage Status (PA-M)` = as.integer(`Proximity-Adjusted Location Quotient (PA-LQ)` >= PA_THRESHOLD)) %>%
  select(-`Proximity-Adjusted Location Quotient (PA-LQ)`)

first_pass <- first_pass %>% left_join(PA_M_df, by=c("County GEOID","Industry Code"))

M_PA_coo <- PA_M_df %>%
  inner_join(counties,  by="County GEOID") %>%
  inner_join(industries, by="Industry Code") %>%
  filter(`Proximity-Adjusted Comparative-Advantage Status (PA-M)`==1L)

M_PA <- sparseMatrix(i=M_PA_coo$c_index, j=M_PA_coo$i_index, x=1L,
                     dims=dim(M), dimnames=dimnames(M))
kc0_PA <- Matrix::rowSums(M_PA); ki0_PA <- Matrix::colSums(M_PA)
kc0_PA_safe <- ifelse(kc0_PA>0, kc0_PA, 1); ki0_PA_safe <- ifelse(ki0_PA>0, ki0_PA, 1)

div_PA <- tibble(`County GEOID` = rownames(M_PA), `Proximity-Adjusted Diversity` = as.numeric(kc0_PA))
ubi_PA <- tibble(`Industry Code` = colnames(M_PA), `Proximity-Adjusted Industry Ubiquity` = as.numeric(ki0_PA))

U_PA <- t(M_PA) %*% M_PA
diagU_PA <- diag(U_PA)
den_max_PA <- pmax(outer(as.numeric(diagU_PA), as.numeric(diagU_PA), pmax), 1)
phi_PA_dense <- as.matrix(U_PA) / den_max_PA; diag(phi_PA_dense) <- 0
phi_PA <- Matrix(phi_PA_dense, sparse=TRUE)

phiPA_colsum <- Matrix::colSums(phi_PA); phiPA_colsum_safe <- ifelse(phiPA_colsum>0, phiPA_colsum, 1)
density_PA_mat <- (M_PA %*% phi_PA); density_PA_mat <- sweep(as.matrix(density_PA_mat), 2, phiPA_colsum_safe, "/")
assert_true(all(density_PA_mat >= -1e-9 & density_PA_mat <= 1+1e-9, na.rm=TRUE), "PA‑Feasibility density out of [0,1] bounds.")
density_PA_df <- as_tibble(density_PA_mat) %>% mutate(`County GEOID`=counties$`County GEOID`) %>%
  relocate(`County GEOID`) %>% pivot_longer(-`County GEOID`, names_to="Industry Code",
                                            values_to="Proximity-Adjusted Co-location Density (PA-Feasibility)")

# PA‑ECI/PA‑ICI via normalized reflections with PA‑M (as the gate)
B_PA  <- (Diagonal(x=as.numeric(1/sqrt(kc0_PA_safe))) %*% M_PA %*% Diagonal(x=as.numeric(1/sqrt(ki0_PA_safe))))
Sc_PA <- B_PA %*% t(B_PA)
Si_PA <- t(B_PA) %*% B_PA
epa <- eig2_sym(Sc_PA, "B^PA (B^PA)'")
ECI_PA_raw <- epa$v2; ECI_PA_z <- zscore(ECI_PA_raw)
if (stats::cor(ECI_PA_z, emp_by_county, use="pairwise.complete.obs") < 0) ECI_PA_z <- -ECI_PA_z
eia <- eig2_sym(Si_PA, "(B^PA)' B^PA")
ICI_PA_raw <- eia$v2; ICI_PA_z <- zscore(ICI_PA_raw)

ECI_PA_tbl <- counties  %>% transmute(`County GEOID`,`Proximity-Adjusted ECI (PA-ECI)`=as.numeric(ECI_PA_z))
ICI_PA_tbl <- industries %>% transmute(`Industry Code`,`Proximity-Adjusted ICI (PA-ICI)`=as.numeric(ICI_PA_z))

second_pass <- first_pass %>%
  left_join(div_PA,         by="County GEOID") %>%
  left_join(ubi_PA,         by="Industry Code") %>%
  left_join(ECI_PA_tbl,     by="County GEOID") %>%
  left_join(ICI_PA_tbl,     by="Industry Code") %>%
  left_join(density_PA_df,  by=c("County GEOID","Industry Code"))

mem_note("After PA‑M/PA‑ECI/PA‑ICI/PA‑Feasibility")
tick("G) complete")

# ============================================================
# ---- H) QA checks with EXPLAIN notes
# ============================================================
.msg("INFO","Sanity checks…")
assert_true(
  second_pass %>% group_by(`County GEOID`) %>% summarise(n_eci=n_distinct(`County Economic Complexity Index (ECI)`), .groups="drop") %>% summarise(all_one=all(n_eci==1L)) %>% pull(all_one),
  "ECI is not constant within county."
)
assert_true(
  second_pass %>% group_by(`Industry Code`) %>% summarise(n_ici=n_distinct(`Industry Complexity Index (ICI)`), .groups="drop") %>% summarise(all_one=all(n_ici==1L)) %>% pull(all_one),
  "ICI is not constant within industry."
)
assert_true(
  second_pass %>% group_by(`County GEOID`) %>% summarise(n_eci_pa=n_distinct(`Proximity-Adjusted ECI (PA-ECI)`), .groups="drop") %>% summarise(all_one=all(n_eci_pa==1L)) %>% pull(all_one),
  "PA‑ECI is not constant within county."
)
assert_true(
  second_pass %>% group_by(`Industry Code`) %>% summarise(n_ici_pa=n_distinct(`Proximity-Adjusted ICI (PA-ICI)`), .groups="drop") %>% summarise(all_one=all(n_ici_pa==1L)) %>% pull(all_one),
  "PA‑ICI is not constant within industry."
)
# PA‑LQ must be 0 whenever LQ==0 (construction gate)
zcheck <- second_pass %>% summarise(ok = all((`Revealed Comparative Advantage (RCA)`==0 & `Proximity-Adjusted Location Quotient (PA-LQ)`==0) | (`Revealed Comparative Advantage (RCA)`>0)))
assert_true(zcheck$ok, "PA‑LQ nonzero where RCA==0 (violates construction).")

.msg("EXPLAIN","Checks confirm: (i) ECI/PA‑ECI are county‑level scalars; ICI/PA‑ICI industry‑level scalars; (ii) PA‑LQ only positive when LQ>0 (gate).")
mem_note("After QA checks")
tick("H) complete")

# ============================================================
# ---- I) Final object; memory & timing
# ============================================================
county_industry_palq <- second_pass
.msg("INFO", glue("county_industry_palq: {nrow(county_industry_palq)} rows × {ncol(county_industry_palq)} cols"))
if (DEBUG_VERBOSE) dplyr::glimpse(county_industry_palq)
mem_note("After building county_industry_palq")
tick("I) complete")

# ============================================================
# ---- J) Ranks / Percentiles / Bins (per‑industry; ECI across counties)
# ============================================================
.msg("INFO","Adding ranks/percentiles/bins (per‑industry; and ECI across counties)…")

# 1) Per‑industry ranks (across counties)
county_industry_ranked <- county_industry_palq %>%
  group_by(`Industry Code`) %>%
  mutate(
    `Feasibility Rank`        = dplyr::min_rank(dplyr::desc(`Co-location Density (Feasibility)`)),
    `Feasibility Percentile`  = dplyr::percent_rank(`Co-location Density (Feasibility)`) * 100,
    `Feasibility Quintile`    = dplyr::ntile(`Co-location Density (Feasibility)`, 5),
    `Feasibility Decile`      = dplyr::ntile(`Co-location Density (Feasibility)`, 10),
    
    `RCA Rank`                = dplyr::min_rank(dplyr::desc(`Revealed Comparative Advantage (RCA)`)),
    `RCA Percentile`          = dplyr::percent_rank(`Revealed Comparative Advantage (RCA)`) * 100,
    `RCA Quintile`            = dplyr::ntile(`Revealed Comparative Advantage (RCA)`, 5),
    `RCA Decile`              = dplyr::ntile(`Revealed Comparative Advantage (RCA)`, 10),
    
    `PA-LQ Rank`              = dplyr::min_rank(dplyr::desc(`Proximity-Adjusted Location Quotient (PA-LQ)`)),
    `PA-LQ Percentile`        = dplyr::percent_rank(`Proximity-Adjusted Location Quotient (PA-LQ)`) * 100,
    `PA-LQ Quintile`          = dplyr::ntile(`Proximity-Adjusted Location Quotient (PA-LQ)`, 5),
    `PA-LQ Decile`            = dplyr::ntile(`Proximity-Adjusted Location Quotient (PA-LQ)`, 10),
    
    `PA-Feasibility Rank`     = dplyr::min_rank(dplyr::desc(`Proximity-Adjusted Co-location Density (PA-Feasibility)`)),
    `PA-Feasibility Percentile` = dplyr::percent_rank(`Proximity-Adjusted Co-location Density (PA-Feasibility)`) * 100,
    `PA-Feasibility Quintile` = dplyr::ntile(`Proximity-Adjusted Co-location Density (PA-Feasibility)`, 5),
    `PA-Feasibility Decile`   = dplyr::ntile(`Proximity-Adjusted Co-location Density (PA-Feasibility)`, 10)
  ) %>% ungroup()

# 2) County‑level ECI/PA‑ECI ranks (unique per county)
eci_county <- county_industry_palq %>%
  distinct(`County GEOID`,
           `County Economic Complexity Index (ECI)`,
           `Proximity-Adjusted ECI (PA-ECI)`) %>%
  mutate(
    `ECI Rank`         = min_rank(desc(`County Economic Complexity Index (ECI)`)),
    `ECI Percentile`   = percent_rank(`County Economic Complexity Index (ECI)`) * 100,
    `ECI Quintile`     = ntile(`County Economic Complexity Index (ECI)`, 5),
    `ECI Decile`       = ntile(`County Economic Complexity Index (ECI)`, 10),
    
    `PA-ECI Rank`      = min_rank(desc(`Proximity-Adjusted ECI (PA-ECI)`)),
    `PA-ECI Percentile`= percent_rank(`Proximity-Adjusted ECI (PA-ECI)`) * 100,
    `PA-ECI Quintile`  = ntile(`Proximity-Adjusted ECI (PA-ECI)`, 5),
    `PA-ECI Decile`    = ntile(`Proximity-Adjusted ECI (PA-ECI)`, 10)
  )

# 3) Merge back; canonicalize
county_industry_palq_with_percentile <- county_industry_ranked %>%
  left_join(eci_county, by = "County GEOID", suffix = c("", ".eci")) %>%
  mutate(
    `County Economic Complexity Index (ECI)` = coalesce(`County Economic Complexity Index (ECI)`, `County Economic Complexity Index (ECI).eci`),
    `Proximity-Adjusted ECI (PA-ECI)`        = coalesce(`Proximity-Adjusted ECI (PA-ECI)`, `Proximity-Adjusted ECI (PA-ECI).eci`)
  ) %>% select(-ends_with(".eci"))

# QA: ECI ranks should vary across county rows
eci_rank_var <- county_industry_palq_with_percentile %>% summarise(var_rank = stats::var(`ECI Rank`, na.rm = TRUE)) %>% pull(var_rank)
assert_true(is.finite(eci_rank_var) && eci_rank_var > 0, "ECI Rank variance is zero — check ECI ranking join.")
mem_note("After adding ranks/percentiles/bins")
tick("J) complete")
if (DEBUG_VERBOSE) dplyr::glimpse(county_industry_palq_with_percentile)

# ============================================================
# ---- K) Light & Heavy Stress Tests (optional)
# ============================================================
if (STRESS_TESTS_LIGHT) {
  .msg("INFO","Light stress tests…")
  # 1) Correlation sanity: ECI vs. Diversity; ECI vs. employment scale
  ecidiv <- county_industry_palq %>% distinct(`County GEOID`,`County Economic Complexity Index (ECI)`) %>%
    left_join(diversity, by="County GEOID") %>%
    left_join(county_totals %>% select(`County GEOID`,`Total Employment in County`), by="County GEOID")
  cor_div <- suppressWarnings(stats::cor(ecidiv$`County Economic Complexity Index (ECI)`, ecidiv$Diversity, use="pairwise.complete.obs"))
  cor_emp <- suppressWarnings(stats::cor(ecidiv$`County Economic Complexity Index (ECI)`, log1p(ecidiv$`Total Employment in County`), use="pairwise.complete.obs"))
  .msg("EXPLAIN", glue("ECI correlations: with Diversity={round(cor_div,3)}; with log employment={round(cor_emp,3)} (signs/data‑dependent)."))
  
  # 2) Component correlations: LQ vs. ALQ/WLQ/WALQ vs. PA‑LQ (attenuation & reinforcement)
  comp_df <- county_industry_palq %>% select(`Revealed Comparative Advantage (RCA)`,
                                             `Relatedness-Weighted Location Quotient (ALQ-Φ)`,
                                             `Spatially-Lagged Location Quotient (WLQ)`,
                                             `Spatially-Lagged Relatedness-Weighted LQ (WALQ-Φ)`,
                                             `Proximity-Adjusted Location Quotient (PA-LQ)`) %>% na.omit()
  cor_mat <- suppressWarnings(stats::cor(comp_df))
  .msg("DEBUG", "Component correlation matrix (sample):"); print(round(cor_mat,3))
}

if (STRESS_TESTS_HEAVY) {
  .msg("INFO","HEAVY stress tests enabled … may take time.")
  set.seed(424242)
  # A) Permutation nulls for ECI (permute columns of M)
  perm_gap <- numeric(N_PERMUTATIONS_ECI)
  for (p in seq_len(N_PERMUTATIONS_ECI)) {
    perm_cols <- sample(ncol(M))
    M_perm <- M[, perm_cols]
    kc0p <- Matrix::rowSums(M_perm); ki0p <- Matrix::colSums(M_perm)
    Bp   <- (Diagonal(x=as.numeric(1/sqrt(ifelse(kc0p>0, kc0p, 1)))) %*% M_perm %*% Diagonal(x=as.numeric(1/sqrt(ifelse(ki0p>0, ki0p, 1)))))
    Scp  <- Bp %*% t(Bp)
    eg   <- eig2_sym(Scp, "perm Sc")
    perm_gap[p] <- eg$gap
  }
  .msg("EXPLAIN", glue("Permutation spectral gaps (median) = {round(stats::median(perm_gap),4)} vs observed {round(ec$gap,4)} — larger observed gap suggests non‑random structure."))
  
  # B) EPS sensitivity sweep for PA‑LQ
  eps_results <- lapply(EPS_GRID, function(eps) {
    PALQ_eps <- (LQ_mat * (ALQ_phi_mat + eps) * (WLQ_mat + eps) * (WALQ_phi_mat + eps))^(1/4)
    PALQ_eps[!LQ_nonzero] <- 0
    data.frame(eps=eps,
               cor_with_base = suppressWarnings(stats::cor(c(PA_LQ_mat), c(PALQ_eps), use="pairwise.complete.obs")))
  }) %>% bind_rows()
  .msg("EXPLAIN", glue("PA‑LQ ε‑sensitivity (corr with ε={EPS_PALQ} baseline): {paste(sprintf('ε=%g→corr=%.4f', eps_results$eps, eps_results$cor_with_base), collapse='; ')}."))
}

# ============================================================
# ---- L) Optional exports / plots / diagnostics
# ============================================================
if (EXPORT_FINAL) {
  out_dir <- "~/Library/CloudStorage/OneDrive-RMI"
  ensure_dir(out_dir)
  readr::write_csv(county_industry_palq,                 fs::path(out_dir, "county_industry_palq.csv"))
  readr::write_csv(county_industry_palq_with_percentile, fs::path(out_dir, "county_industry_palq_with_percentile.csv"))
  .msg("INFO", glue("Exported outputs to {out_dir}"))
}
if (WRITE_DIAGNOSTICS_TABLES) {
  out_dir <- "~/Library/CloudStorage/OneDrive-RMI"
  ensure_dir(out_dir)
  iso_tbl <- tryCatch({
    relevant_counties_sf %>% sf::st_drop_geometry() %>% slice(which(spdep::card(nb_queen)==0)) %>%
      transmute(`County GEOID`=GEOID, `County Name`=NAMELSAD, `State Abbreviation`=STUSPS)
  }, error=function(e) tibble())
  readr::write_csv(iso_tbl, fs::path(out_dir, "isolates.csv"))
  .msg("INFO", glue("Wrote diagnostics to {out_dir}"))
}
if (PLOT_DIAGNOSTICS) {
  plot_dir <- "~/Library/CloudStorage/OneDrive-RMI"
  ensure_dir(plot_dir)
  try({
    ggplot(county_industry_palq %>% dplyr::sample_n(min(n(), PLOT_SAMPLE_N))) +
      geom_histogram(aes(x = `Proximity-Adjusted Location Quotient (PA-LQ)`), bins = 60) +
      labs(title="Distribution of PA‑LQ (sample)", x="PA‑LQ", y="Count") +
      theme_minimal()
    ggsave(fs::path(plot_dir, "palq_distribution.png"), width=7, height=4.5, dpi=150)
    .msg("INFO", glue("Saved plot → {fs::path(plot_dir, 'palq_distribution.png')}"))
  }, silent = TRUE)
}

.msg("INFO","Pipeline finished.")
if (DEBUG_VERBOSE) sessionInfo()
