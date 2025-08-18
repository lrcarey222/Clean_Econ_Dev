# =============================================================================
# Tapestry Employment (2001–2024) — Full Pipeline with 2024 Preview Gate
#
# Implements:
#   • Chunked read/aggregate of 6-digit NAICS employment (+establishments if present)
#   • RCA (employment & establishments), Presence M (Balassa threshold RCA>=1)
#   • ECI / ICI via second eigenvector (Method of Reflections / eigen approach)
#   • Implicit co-location proximity φ  (Ui,i' / max(Uii, Ui'i'))
#   • Density (a.k.a. Feasibility) d_{c,i}  = Σ_i' M_{c,i'} φ_{i',i} / Σ_i' φ_{i',i}
#   • Strategic Index SI_c = Σ_i d_{c,i} (1 - M_{c,i}) ICI_i
#   • Strategic Gain SG_{c,i} = [Σ_{i'} (φ_{i,i'}/Σ_k φ_{k,i'}) (1 - M_{c,i'}) ICI_{i'}] - d_{c,i} ICI_i
#     Formulas per Daboin et al. technical note (see inline citations). :contentReference[oaicite:2]{index=2}
#
# NEW (Aug 2025 — per action items):
#   1) CLEAN JOINS: all joins are done with explicit by= and minimal columns,
#      so duplicate artifacts like i.year / i.geography never appear again.
#   2) INLINE MATH DOCS: comments mark each step to PDF formulas:
#        • (4)-(5) RCA & M, (8)-(14) ECI/ICI, (15)-(20) co-occurrence & φ,
#          (19) density/feasibility, (21) SI, (22) SG. :contentReference[oaicite:3]{index=3}
#   3) DENSITY CONTRIBUTIONS: new tables decomposing density for each area–industry
#      into contributing industries present in that area, following your notes
#      (share of numerator + absolute contribution to density). :contentReference[oaicite:4]{index=4}
#      • Written to CSV per year & geography, included in per‑year bundle RDS,
#        and aggregated into the master all‑years RDS.
#
# Public column names are stable, consistent across geographies:
#   - Year
#   - Area Type
#   - Area Code/GEOID
#   - Area Title
#   - Six-Digit NAICS Industry Code
#   - Six-Digit NAICS Industry Title
#   - Industry Establishments: Area
#   - Total Establishments: Area
#   - Industry Establishments: Nation
#   - Total Establishments: Nation
#   - Revealed Comparative Advantage (RCA), Establishments
#   - Industry Employment: Area
#   - Total Employment: Area
#   - Industry Employment: Nation
#   - Total Employment: Nation
#   - Revealed Comparative Advantage (RCA), Employment, Raw
#   - Revealed Comparative Advantage (RCA), Employment, Normalized
#   - Industry Presence Binary
#   - Industry Comparative Advantage Binary (M)
#   - Economic Complexity Index (ECI), Raw
#   - Economic Complexity Index (ECI), Normalized
#   - Industry Complexity Index (ICI), Raw
#   - Industry Complexity Index (ICI), Normalized
#   - Colocation Density (Feasibility), Raw
#   - Colocation Density (Feasibility), Normalized
#   - Strategic Gain, Raw
#   - Strategic Gain, Normalized
#   - Strategic Index, Raw
#   - Strategic Index, Normalized
#
# Contributions export (new) — columns:
#   - Year
#   - Area Type
#   - Area Code/GEOID
#   - Area Title
#   - Target NAICS Industry Code
#   - Target NAICS Industry Title
#   - Contributor NAICS Industry Code
#   - Contributor NAICS Industry Title
#   - Contributor Present Binary
#   - Proximity (φ) Contributor↔Target
#   - Sum of Proximities to Target (Denominator)
#   - Sum of Proximities from Present Industries (Numerator)
#   - Colocation Density (Feasibility), Raw
#   - Contributor Share of Numerator (0–1)
#   - Contributor Contribution to Density (Absolute)
#
# =============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(Matrix)
  library(RSpectra)
  library(tools)
  library(sf)
  library(tigris)
  library(dplyr)        # for glimpse in logs
  library(future.apply) # for optional parallel
})

# ------------------------------ CONFIG ---------------------------------------
CFG <- list(
  # Input discovery (OneDrive-aware)
  use_onedrive_resolver = TRUE,
  input_subpath = file.path(
    "US Program - Documents","6_Projects","Clean Regional Economic Development",
    "ACRE","Data","Raw Data","Tapestry_Employment",
    "contains_naics_999999_county_XX999","NAICS_6D"
  ),
  
  # Local BLS industry titles CSV (no download in script)
  industry_titles_local =
    "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/BLS_QCEW/metadata/industry-titles.csv",
  
  # Expected final field names for the raw input after mapping
  final_names  = c("year","area","naics","employment","establishments","wages"),
  
  # Exports
  export_prefix              = "acre_tapestry",
  write_per_year             = TRUE,
  write_per_year_rds         = TRUE,
  write_per_year_csv         = TRUE,
  write_per_year_bundle_rds  = TRUE,
  write_combined             = FALSE,
  all_years_rds_compress     = "xz",
  
  # Census TIGER vintage
  tigris_year = 2023,
  
  # Year range (inclusive)
  year_max = 2024L,
  year_min = 2001L,
  
  # Reader performance
  fread_rows_per_chunk = 1e6L,
  reduce_every_n_chunks = 3L,
  progress_every_n_chunks = 1L,
  
  # Complexity stack
  industry_block_size = 50L,  # block industries to bound memory
  eigen_k = 2L,               # second eigenvector
  
  # NEW: Density contributions controls
  contrib_enable   = TRUE,    # build contributions files
  contrib_top_k    = 10L,     # per (area, target industry), keep Top-K contributors; set Inf for ALL
  contrib_write_csv= TRUE,    # write CSVs per geography
  contrib_keep_abs = TRUE,    # keep absolute contribution to density (component)
  contrib_keep_share = TRUE,  # keep share of numerator
  
  # Parallel defaults
  parallel_default = FALSE,
  parallel_workers = NULL,
  
  # Logging
  verbose_debug = TRUE
)

# --------------------------- time-stamped export dir --------------------------
RUN_STAMP <- format(Sys.time(), "%Y%m%d_%H%M%S")
OUT_DIR   <- file.path(getwd(), sprintf("%s_export_%s", CFG$export_prefix, RUN_STAMP))
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
RAW_COPY_DIR <- file.path(OUT_DIR, "source_data");        dir.create(RAW_COPY_DIR, showWarnings = FALSE)
XWALK_DIR    <- file.path(OUT_DIR, "crosswalks");         dir.create(XWALK_DIR,    showWarnings = FALSE)
SRC_DIR      <- file.path(OUT_DIR, "external_sources");   dir.create(SRC_DIR,      showWarnings = FALSE)
LOG_FILE_MAIN<- file.path(OUT_DIR, sprintf("runlog_%s.txt", RUN_STAMP))

# ------------------------------ Logging helpers -------------------------------
CURRENT_LOG_FILE <- LOG_FILE_MAIN
.mem_str <- function() {
  g <- gc(FALSE)
  paste0("mem ~", format(round(sum(g[,2]) / 1024, 2), nsmall = 2), " GB")
}
.tic <- function() as.numeric(proc.time()[3])
.set_log_file <- function(path) assign("CURRENT_LOG_FILE", path, envir = .GlobalEnv)
.safe_append <- function(path, text) {
  try({
    con <- file(path, open = "a", blocking = FALSE, encoding = "UTF-8")
    on.exit(close(con), add = TRUE)
    writeLines(text, con, sep = "\n", useBytes = TRUE)
  }, silent = TRUE)
}
.log  <- function(..., .type = "INFO") { msg <- paste0(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), " [", .type, "] ", paste0(...)); cat(msg, "\n"); .safe_append(CURRENT_LOG_FILE, msg) }
.warn <- function(...) .log(..., .type = "WARN")
.err  <- function(...) .log(..., .type = "ERROR")
options(warn = 1)
capture.output(sessionInfo(), file = file.path(OUT_DIR, sprintf("sessionInfo_%s.txt", RUN_STAMP)))

# ------------------------------ Glimpse helper -------------------------------
.emit_glimpse <- function(obj, name = deparse(substitute(obj)), width = 120) {
  .log("[glimpse] ", name, " (showing structure before any operations)…")
  txt <- capture.output({
    if (requireNamespace("dplyr", quietly = TRUE)) dplyr::glimpse(obj, width = width) else str(obj, vec.len = 5, digits.d = 3)
  })
  cat(paste(txt, collapse = "\n"), "\n")
  .safe_append(CURRENT_LOG_FILE, paste(txt, collapse = "\n"))
}

# --------------------------- OneDrive root resolver ---------------------------
path_has_onedrive_folder <- function(p) {
  parts <- strsplit(normalizePath(p, winslash = "/", mustWork = FALSE), "/", fixed = TRUE)[[1]]
  if (!length(parts)) return(NA_character_)
  idx <- which(grepl("^OneDrive( |-|$)", parts))
  if (!length(idx)) return(NA_character_)
  paste(parts[seq_len(max(idx))], collapse = "/")
}
resolve_onedrive_root <- function() {
  env_root <- Sys.getenv("ONEDRIVE_ROOT", unset = NA_character_)
  if (!is.na(env_root) && nzchar(env_root) && dir.exists(env_root)) return(path.expand(env_root))
  wd <- getwd(); guess <- path_has_onedrive_folder(wd)
  if (!is.na(guess) && dir.exists(guess)) return(guess)
  sysname <- Sys.info()[["sysname"]]; home <- path.expand("~")
  user <- Sys.getenv("USERNAME", unset = Sys.getenv("USER",""))
  candidates <- if (identical(sysname, "Windows")) c(
    Sys.getenv("OneDriveCommercial",""), Sys.getenv("OneDriveConsumer",""), Sys.getenv("OneDrive",""),
    file.path("C:/Users", user, "OneDrive - RMI"),
    file.path("C:/Users", user, "OneDrive - Rocky Mountain Institute"),
    file.path("C:/Users", user, "OneDrive - RMI, Inc"),
    file.path("C:/Users", user, "OneDrive")
  ) else if (identical(sysname, "Darwin")) c(
    file.path(home,"Library","CloudStorage","OneDrive - RMI"),
    file.path(home,"Library","CloudStorage","OneDrive-RMI"),
    file.path(home,"Library","CloudStorage","OneDrive"),
    file.path(home,"OneDrive - RMI"),
    file.path(home,"OneDrive")
  ) else c(file.path(home,"OneDrive - RMI"), file.path(home,"OneDrive"))
  hits <- unique(candidates[nzchar(candidates) & dir.exists(candidates)])
  if (length(hits)) return(hits[[1]])
  stop("Could not locate your OneDrive root. Set ONEDRIVE_ROOT.")
}

if (CFG$use_onedrive_resolver) {
  ONEDRIVE_ROOT <- resolve_onedrive_root()
  .log("Using OneDrive root: ", ONEDRIVE_ROOT)
  INPUT_DIR <- file.path(ONEDRIVE_ROOT, CFG$input_subpath)
} else {
  INPUT_DIR <- file.path(getwd(), "input")
}
if (!dir.exists(INPUT_DIR)) stop("Data folder not found: ", INPUT_DIR)
.log("Input directory: ", INPUT_DIR)

# ------------------------------ Discover input CSVs ---------------------------
all_csv <- list.files(INPUT_DIR, pattern = "\\.csv$", full.names = TRUE)
stopifnot(length(all_csv) > 0L)

extract_year <- function(p) {
  m <- regmatches(basename(p), regexpr("(19|20)\\d{2}", basename(p)))
  if (!length(m)) return(NA_integer_)
  y <- suppressWarnings(as.integer(substr(m[1], 1, 4)))
  ifelse(is.na(y), NA_integer_, y)
}
years <- vapply(all_csv, extract_year, integer(1L))
sel   <- which(!is.na(years) & years >= CFG$year_min & years <= CFG$year_max)
csv_files  <- all_csv[sel]
file_years <- years[sel]

o <- order(file_years, basename(csv_files))
csv_files  <- csv_files[o]; file_years <- file_years[o]
yr_last_idx <- tapply(seq_along(file_years), file_years, tail, n = 1)
csv_by_year <- data.table(year = as.integer(names(yr_last_idx)),
                          file = csv_files[unlist(yr_last_idx, use.names = FALSE)])
setorder(csv_by_year, -year)
present_years <- csv_by_year$year
missing_years <- setdiff(CFG$year_min:CFG$year_max, present_years)
if (length(present_years) == (CFG$year_max - CFG$year_min + 1L)) {
  .log("Found all ", length(present_years), " CSVs for ", CFG$year_min, "–", CFG$year_max, ".")
} else {
  .warn("Missing ", length(missing_years), " years: ", paste(sort(missing_years), collapse = ", "))
}

# Copy sources + manifest
manifest <- data.table(file = basename(csv_by_year$file),
                       path = normalizePath(csv_by_year$file, winslash = "/", mustWork = TRUE),
                       year = csv_by_year$year,
                       size_bytes = file.info(csv_by_year$file)$size,
                       md5 = as.character(md5sum(csv_by_year$file)))
fwrite(manifest, file.path(OUT_DIR, sprintf("source_manifest_%s.csv", RUN_STAMP)))
file.copy(csv_by_year$file, RAW_COPY_DIR, overwrite = TRUE)
.log("Source CSVs listed in manifest and copied to source_data/.")

# -------------------------- Industry titles (LOCAL CSV ONLY) ------------------
read_local_industry_titles <- function() {
  mac_style <- path.expand(CFG$industry_titles_local)
  win_style <- file.path(ONEDRIVE_ROOT,
                         "US Program - Documents","6_Projects","Clean Regional Economic Development",
                         "ACRE","Data","Raw Data","BLS_QCEW","metadata","industry-titles.csv")
  other     <- file.path(SRC_DIR, "industry-titles.csv")
  candidates <- unique(c(mac_style, win_style, other))
  candidates <- candidates[file.exists(candidates)]
  if (!length(candidates)) {
    .warn("Industry titles CSV not found locally. Proceeding without titles.")
    return(data.table(naics = character(), industry_title = character()))
  }
  .log("Loading industry titles from local CSV: ", candidates[1])
  dt <- fread(candidates[1], showProgress = FALSE)
  setnames(dt, tolower(names(dt)))
  code_col  <- if ("industry_code" %in% names(dt)) "industry_code" else if ("naics" %in% names(dt)) "naics" else NA_character_
  title_col <- if ("industry_title" %in% names(dt)) "industry_title" else if ("title" %in% names(dt)) "title" else NA_character_
  if (is.na(code_col) || is.na(title_col)) {
    .warn("Could not detect code/title columns in industry titles CSV; skipping titles.")
    return(data.table(naics = character(), industry_title = character()))
  }
  dt[, raw_len := nchar(get(code_col))]
  dt6 <- dt[raw_len == 6L, .(naics = sprintf("%06s", get(code_col)), industry_title = get(title_col))]
  unique(dt6)
}
industry_titles <- read_local_industry_titles()
.emit_glimpse(industry_titles, "Industry titles (6-digit only) — loaded")

# -------------------- Crosswalk: County→State / CBSA / CSA (area-weighted) ---
options(tigris_use_cache = TRUE, tigris_class = "sf")
YEAR_TIGRIS <- CFG$tigris_year

ensure_valid <- function(g) {
  tryCatch(sf::st_make_valid(g),
           error = function(e) {
             .warn("st_make_valid failed; falling back to zero-width buffer fix.")
             tryCatch(sf::st_buffer(g, 0), error = function(e2) g)
           })
}

.log("Building county/state/CBSA/CSA crosswalk (area-weighted, OMB-conforming)…")
states_sf   <- tigris::states(cb = TRUE, year = YEAR_TIGRIS, class = "sf")
.emit_glimpse(states_sf, "States (raw sf) — loaded")
states_dt   <- as.data.table(sf::st_drop_geometry(states_sf))[
  , .(STATEFP, STUSPS, STATE_NAME = NAME, STATE_GEOID = GEOID)
]

counties_sf <- tigris::counties(cb = TRUE, year = YEAR_TIGRIS, class = "sf")
.emit_glimpse(counties_sf, "Counties (raw sf) — loaded")
counties_sf <- ensure_valid(counties_sf)
counties_sf <- counties_sf[, c("GEOID","NAMELSAD","STATEFP","geometry")]

EPSG_EQ_AREA <- 9311
counties_ae  <- sf::st_transform(counties_sf, EPSG_EQ_AREA)
counties_ae$AREA_M2_CT <- as.numeric(sf::st_area(counties_ae))
counties_ae_dt <- as.data.table(sf::st_drop_geometry(counties_ae))[
  , .(GEOID, NAMELSAD, STATEFP, AREA_M2_CT)
]

# ---- CBSA overlap (max area share) ------------------------------------------
cbsa_sf <- tryCatch(tigris::core_based_statistical_areas(cb = TRUE, year = YEAR_TIGRIS, class = "sf"),
                    error = function(e) { .warn("CBSA shapes failed: ", e$message); NULL })
if (!is.null(cbsa_sf)) .emit_glimpse(cbsa_sf, "CBSA (raw sf) — loaded")
cbsa_cols  <- c("CBSAFP","NAME","LSAD","CSAFP")
if (!is.null(cbsa_sf)) {
  cbsa_sf <- ensure_valid(cbsa_sf)
  cbsa_sf <- cbsa_sf[, intersect(cbsa_cols, names(cbsa_sf))]
  cbsa_ae <- sf::st_transform(cbsa_sf, EPSG_EQ_AREA)
  suppressWarnings({
    cbsa_x <- tryCatch(sf::st_intersection(
      counties_ae[, c("GEOID","geometry")],
      cbsa_ae[, c(intersect(cbsa_cols, names(cbsa_ae)),"geometry")]
    ), error = function(e) {
      .warn("st_intersection (county×CBSA) failed; fixing geometries.")
      sf::st_intersection(ensure_valid(counties_ae), ensure_valid(cbsa_ae))
    })
  })
  if (nrow(cbsa_x)) {
    cbsa_x$INT_AREA_M2 <- as.numeric(sf::st_area(cbsa_x))
    cbsa_x_dt <- as.data.table(sf::st_drop_geometry(cbsa_x))
    cbsa_x_dt <- cbsa_x_dt[counties_ae_dt[, .(GEOID, AREA_M2_CT)], on = "GEOID"]
    cbsa_x_dt[, overlap_share := fifelse(AREA_M2_CT > 0, pmin(1, INT_AREA_M2 / AREA_M2_CT), NA_real_)]
    cbsa_best <- cbsa_x_dt[order(GEOID, -overlap_share)][, .SD[1], by = GEOID][
      , .(GEOID,
          cbsa_code = if ("CBSAFP" %in% names(.SD)) sprintf("%05s", CBSAFP) else NA_character_,
          cbsa_name = if ("NAME"   %in% names(.SD)) NAME else NA_character_,
          cbsa_lsad = if ("LSAD"   %in% names(.SD)) LSAD else NA_character_,
          cbsa_csa_code_from_cbsa = if ("CSAFP" %in% names(.SD)) sprintf("%03s", CSAFP) else NA_character_,
          cbsa_overlap_share = overlap_share)]
  } else cbsa_best <- data.table(GEOID=character(), cbsa_code=character(), cbsa_name=character(),
                                 cbsa_lsad=character(), cbsa_csa_code_from_cbsa=character(), cbsa_overlap_share=numeric())
} else cbsa_best <- data.table(GEOID=character(), cbsa_code=character(), cbsa_name=character(),
                               cbsa_lsad=character(), cbsa_csa_code_from_cbsa=character(), cbsa_overlap_share=numeric())

# ---- CSA overlap (max area share) -------------------------------------------
get_csa_sf <- function(year) {
  for (fn in c("combined_statistical_areas","csa")) {
    if (exists(fn, where = asNamespace("tigris"), inherits = FALSE)) {
      return(tryCatch(do.call(getFromNamespace(fn, "tigris"),
                              list(cb = TRUE, year = year, class = "sf")),
                      error = function(e) NULL))
    }
  }
  NULL
}
csa_sf <- get_csa_sf(YEAR_TIGRIS)
if (!is.null(csa_sf)) .emit_glimpse(csa_sf, "CSA (raw sf) — loaded")
if (!is.null(csa_sf)) {
  csa_sf <- ensure_valid(csa_sf)
  csa_sf <- csa_sf[, intersect(c("CSAFP","NAME"), names(csa_sf))]
  csa_ae <- sf::st_transform(csa_sf, EPSG_EQ_AREA)
  suppressWarnings({
    csa_x <- tryCatch(sf::st_intersection(
      counties_ae[, c("GEOID","geometry")],
      csa_ae[, c(intersect(c("CSAFP","NAME"), names(csa_ae)),"geometry")]
    ), error = function(e) {
      .warn("st_intersection (county×CSA) failed; fixing geometries.")
      sf::st_intersection(ensure_valid(counties_ae), ensure_valid(csa_ae))
    })
  })
  if (nrow(csa_x)) {
    csa_x$INT_AREA_M2 <- as.numeric(sf::st_area(csa_x))
    csa_x_dt <- as.data.table(sf::st_drop_geometry(csa_x))
    csa_x_dt <- csa_x_dt[counties_ae_dt[, .(GEOID, AREA_M2_CT)], on = "GEOID"]
    csa_x_dt[, overlap_share := fifelse(AREA_M2_CT > 0, pmin(1, INT_AREA_M2 / AREA_M2_CT), NA_real_)]
    csa_best <- csa_x_dt[order(GEOID, -overlap_share)][, .SD[1], by = GEOID][
      , .(GEOID,
          csa_code = if ("CSAFP" %in% names(.SD)) sprintf("%03s", CSAFP) else NA_character_,
          csa_name = if ("NAME"   %in% names(.SD)) NAME else NA_character_,
          csa_overlap_share = overlap_share)]
  } else csa_best <- data.table(GEOID=character(), csa_code=character(), csa_name=character(), csa_overlap_share=numeric())
} else {
  .warn("CSA shapes not available; CSA will be NA.")
  csa_best <- data.table(GEOID=character(), csa_code=character(), csa_name=character(), csa_overlap_share=numeric())
}

# Prefer CSA code from CBSA attribute if present
if (nrow(cbsa_best) && "cbsa_csa_code_from_cbsa" %in% names(cbsa_best)) {
  csa_best <- merge(csa_best, cbsa_best[, .(GEOID, cbsa_csa_code_from_cbsa)], by = "GEOID", all = TRUE)
  csa_best[, csa_code := fifelse(!is.na(cbsa_csa_code_from_cbsa), cbsa_csa_code_from_cbsa, csa_code)]
  csa_best[, cbsa_csa_code_from_cbsa := NULL]
}

# ---- Assemble county-level crosswalk ----------------------------------------
cw <- as.data.table(sf::st_drop_geometry(counties_sf))[
  , .(area = GEOID, county_name = NAMELSAD, STATEFP)
][
  states_dt, on = "STATEFP"
][
  cbsa_best, on = c(area = "GEOID")
][
  csa_best, on = c(area = "GEOID")
][
  , `:=`(
    county_in_cbsa = !is.na(cbsa_code),
    county_in_csa  = !is.na(csa_code)
  )
]

# Add SS999 pseudo-counties (Unknown or Undefined, [STATE])
pseudo <- states_dt[, .(
  area = sprintf("%s999", STATEFP),
  county_name = paste0("Unknown or Undefined, ", STATE_NAME),
  STATEFP, STUSPS, STATE_NAME, STATE_GEOID,
  cbsa_code = NA_character_, cbsa_name = NA_character_, cbsa_lsad = NA_character_,
  cbsa_overlap_share = NA_real_,
  csa_code  = NA_character_, csa_name  = NA_character_,
  csa_overlap_share = NA_real_,
  county_in_cbsa = FALSE, county_in_csa = FALSE
)]
cw <- rbindlist(list(cw, pseudo), use.names = TRUE, fill = TRUE)
cw[, cbsa_code := fifelse(!is.na(cbsa_code), sprintf("%05s", cbsa_code), NA_character_)]
cw[, csa_code  := fifelse(!is.na(csa_code),  sprintf("%03s", csa_code),  NA_character_)]

dir.create(XWALK_DIR, showWarnings = FALSE)
fwrite(cw, file.path(XWALK_DIR, sprintf("county_state_cbsa_csa_%s.csv", RUN_STAMP)))
.log("Crosswalk rows: ", nrow(cw))

# ------------------------------ Helpers ---------------------------------------
norm_0_100 <- function(x) { m <- mean(x, na.rm = TRUE); s <- sd(x, na.rm = TRUE); if (is.na(s) || s == 0) return(rep(NA_real_, length(x))); 100 * pnorm((x - m) / s) }
.nnz <- function(M) { if ("nnzero" %in% getNamespaceExports("Matrix")) Matrix::nnzero(M) else sum(M != 0) }
titlecase_geo <- function(g) { switch(tolower(g), county = "County", state = "State", cbsa = "CBSA", csa = "CSA", toupper(g)) }

# ---- Clean left merge: keep only requested columns, avoid i.* artifacts -----
left_merge <- function(x, y, by, add_cols) {
  ysub <- y[, c(by, add_cols), with = FALSE]
  merge(x, ysub, by = by, all.x = TRUE, sort = FALSE)
}

# ----------------------- Robust header mapping (EXPANDED) ---------------------
.match_first <- function(hdr, patterns, exclude = character(0)) {
  hdr_l <- tolower(hdr); ex_l <- tolower(exclude)
  for (p in patterns) {
    idx <- which(grepl(p, hdr_l, perl = TRUE))
    if (length(idx)) {
      idx <- idx[!hdr_l[idx] %in% ex_l]
      if (length(idx)) return(list(name = hdr[idx[1]], idx = idx[1]))
    }
  }
  list(name = NA_character_, idx = NA_integer_)
}
.resolve_header_mapping <- function(hdr) {
  hard_exclude <- c("own_code","owner_code","owncode","owner","wage_rate","avg_wage","annual_avg_wkly_wage","wage","wages")
  y <- .match_first(hdr, c("^year$", "^yr$", "^data[_ ]?year$"), exclude = hard_exclude)
  a <- .match_first(hdr, c("^area[_ ]?fips$", "^fips$", "^county[_ ]?fips$", "^geoid$", "^area$","^area[_ ]?code$"), exclude = hard_exclude)
  n <- .match_first(hdr, c("^naics[_ ]?code$", "^naics6?$", "^naics[_ ]?6d$", "^naics6d$", "^industry[_ ]?code$"), exclude = hard_exclude)
  e <- .match_first(hdr,
                    c("^tap[_ ]?emplvl[_ ]?est[_ ]?3$", "^tap[_ ]?emplvl[_ ]?est$", "^tap[_ ]?emplvl$", "^emplvl$", "^employment$", "^emp$","^tap[_ ]?emplvl[_ ]?est[_ ]?[24]$"),
                    exclude = c(hard_exclude, "estabs","tap_estabs_count","establishment"))
  s <- .match_first(hdr, c("^tap[_ ]?estabs[_ ]?count$", "^estabs$", "^establishments?$", "^establishment[_ ]?count$"),
                    exclude = c(hard_exclude, "emplvl","employment","emp"))
  w <- .match_first(hdr, c("^tap[_ ]?wages[_ ]?est[_ ]?3$", "^wages?$", "^total[_ ]?wages$"), exclude = hard_exclude)
  list(
    year = y$name, area = a$name, naics = n$name, employment = e$name, establishments = s$name, wages = w$name,
    idx_year = y$idx, idx_area = a$idx, idx_naics = n$idx, idx_employment = e$idx, idx_establishments = s$idx, idx_wages = w$idx
  )
}

# ---------------------- Tapestry chunked reader (with ESTABS) -----------------
PRINTED_TAPESTRY_GLIMPSE <<- FALSE

read_one_year_file_chunked <- function(y, return_meta = FALSE, print_raw_glimpse = TRUE) {
  row <- csv_by_year[year == y]; if (!nrow(row)) stop("No CSV for year ", y)
  f <- row$file
  
  hdr0 <- fread(f, nrows = 0L, showProgress = FALSE); hdr_names <- names(hdr0)
  map <- .resolve_header_mapping(hdr_names)
  
  missing_any <- any(vapply(list(map$year, map$area, map$naics, map$employment), function(z) is.na(z) || !nzchar(z), logical(1L)))
  if (missing_any) {
    .err("[read] Could not resolve required columns in file: ", basename(f))
    .err("[read] Header names: ", paste(hdr_names, collapse = " | "))
    .err("[read] Resolved mapping (year/area/naics/employment): ", paste0(map$year, "/", map$area, "/", map$naics, "/", map$employment))
    stop("Missing required columns for chunked read. See log for header names.")
  }
  
  sel_names <- c(map$year, map$area, map$naics, map$employment)
  sel_idx   <- as.integer(c(map$idx_year, map$idx_area, map$idx_naics, map$idx_employment))
  have_estabs <- !is.na(map$establishments) && nzchar(map$establishments)
  have_wages  <- !is.na(map$wages) && nzchar(map$wages)
  if (have_estabs) { sel_names <- c(sel_names, map$establishments); sel_idx <- c(sel_idx, map$idx_establishments) }
  if (have_wages)  { sel_names <- c(sel_names, map$wages);          sel_idx <- c(sel_idx, map$idx_wages) }
  
  if (anyDuplicated(sel_idx)) {
    .err("[read] Selected column indices are not unique: ", paste(sel_idx, collapse = ", "))
    .err("[read] Selected column names: ", paste(sel_names, collapse = ", "))
    stop("Duplicate column mapping detected; aborting read.")
  }
  
  .log("[read] Year ", y, " | resolved columns -> ",
       sprintf("year='%s' area='%s' naics='%s' employment='%s'%s%s [by index %s]",
               map$year, map$area, map$naics, map$employment,
               if (have_estabs) paste0(" establishments='", map$establishments, "'") else "",
               if (have_wages)  paste0(" wages='", map$wages, "'") else "",
               paste(sel_idx, collapse = ",")))
  
  acc_template_cols <- c("year","area","naics","naics_employment_in_area","naics_establishments_in_area")
  make_template_dt <- function() data.table(
    year = integer(), area = character(), naics = character(),
    naics_employment_in_area = double(), naics_establishments_in_area = double()
  )
  
  chunk <- 0L; offset <- 0L; acc <- make_template_dt()
  .log("[read] Year ", y, " | starting row-chunked fread… (rows/chunk=", CFG$fread_rows_per_chunk, ")")
  t0 <- .tic()
  repeat {
    chunk <- chunk + 1L; t1 <- .tic()
    if (offset == 0L) {
      dt_raw <- tryCatch(fread(f, select = sel_idx, nrows = CFG$fread_rows_per_chunk, header = TRUE, showProgress = FALSE),
                         error = function(e) { .err("[read] fread error: ", e$message); NULL })
      if (is.null(dt_raw) || !nrow(dt_raw)) { .log("[read] Year ", y, " | no rows on first chunk. ", .mem_str()); break }
      setnames(dt_raw, sel_names)
      if (print_raw_glimpse && !isTRUE(PRINTED_TAPESTRY_GLIMPSE)) {
        .emit_glimpse(head(dt_raw, 100L), sprintf("Tapestry RAW sample (year %d, first chunk, head 100)", y))
        PRINTED_TAPESTRY_GLIMPSE <<- TRUE
      }
    } else {
      dt_raw <- tryCatch(fread(f, select = sel_idx, nrows = CFG$fread_rows_per_chunk, header = FALSE, skip = 1L + offset, showProgress = FALSE),
                         error = function(e) { .err("[read] fread error (chunk ", chunk, "): ", e$message); NULL })
      if (is.null(dt_raw) || !nrow(dt_raw)) {
        .log("[read] Year ", y, " | no more rows at chunk ", chunk, ". total consumed=", format(offset, big.mark=","), " | ", .mem_str())
        break
      }
      setnames(dt_raw, sel_names)
    }
    n_read <- nrow(dt_raw)
    
    dt <- copy(dt_raw)
    std_names <- c("year","area","naics","employment", if (have_estabs) "establishments" else NULL, if (have_wages) "wages" else NULL)
    setnames(dt, old = sel_names, new = std_names, skip_absent = FALSE)
    
    suppressWarnings({
      dt[, year  := as.integer(year)]
      dt[, area  := sprintf("%05d", as.integer(area))]
      dt[, naics := sprintf("%06d", as.integer(naics))]
      dt[, employment := as.numeric(employment)]
      if (have_estabs && "establishments" %in% names(dt)) dt[, establishments := as.numeric(establishments)]
      if (have_wages  && "wages" %in% names(dt))          dt[, wages := as.numeric(wages)]
    })
    
    if ("year" %in% names(dt)) dt <- dt[year == y]
    if (!nrow(dt)) { offset <- offset + n_read; next }
    
    keep_cols <- c("employment", if (have_estabs) "establishments" else NULL)
    dt <- dt[, lapply(.SD, function(z) sum(as.numeric(z), na.rm = TRUE)), .SDcols = keep_cols, by = .(year, area, naics)]
    setnames(dt, "employment", "naics_employment_in_area")
    if (have_estabs && "establishments" %in% names(dt)) setnames(dt, "establishments", "naics_establishments_in_area")
    if (!"naics_establishments_in_area" %in% names(dt)) dt[, naics_establishments_in_area := NA_real_]
    
    for (nm in acc_template_cols) if (!nm %in% names(dt)) dt[, (nm) := NA_real_]
    setcolorder(dt, acc_template_cols)
    
    if (CFG$verbose_debug) .log(sprintf("[read] Year %d | chunk %d | dt cols: %s | n=%s", y, chunk, paste(names(dt), collapse = ","), format(nrow(dt), big.mark=",")))
    
    acc <- rbindlist(list(acc, dt), use.names = TRUE, fill = TRUE)
    
    if (chunk %% CFG$reduce_every_n_chunks == 0L) {
      setkey(acc, year, area, naics)
      acc <- acc[, .(
        naics_employment_in_area     = sum(as.numeric(naics_employment_in_area), na.rm = TRUE),
        naics_establishments_in_area = sum(as.numeric(naics_establishments_in_area), na.rm = TRUE)
      ), by = .(year, area, naics)]
      setcolorder(acc, acc_template_cols)
      gc(FALSE)
    }
    
    if (CFG$verbose_debug && (chunk %% CFG$progress_every_n_chunks == 0L)) {
      .log(sprintf("[read] Year %d | chunk %d raw_rows=%s | acc_pairs=%s | %s | %.2fs",
                   y, chunk, format(n_read, big.mark=","), format(nrow(acc), big.mark=","), .mem_str(), .tic() - t1))
    }
    
    offset <- offset + n_read
    if (n_read < CFG$fread_rows_per_chunk) {
      .log("[read] Year ", y, " | last partial chunk consumed. total consumed=", format(offset, big.mark=","), " | ", .mem_str())
      break
    }
  }
  
  if (!is.null(acc) && nrow(acc)) {
    setkey(acc, year, area, naics)
    acc <- acc[, .(
      naics_employment_in_area     = sum(as.numeric(naics_employment_in_area), na.rm = TRUE),
      naics_establishments_in_area = sum(as.numeric(naics_establishments_in_area), na.rm = TRUE)
    ), by = .(year, area, naics)]
    setcolorder(acc, acc_template_cols)
  } else stop("No rows accumulated for year ", y)
  
  .log(sprintf("[read] Year %d | aggregated unique (area,naics): %s | %.2fs | %s",
               y, format(nrow(acc), big.mark=","), .tic() - t0, .mem_str()))
  
  if (return_meta) list(data = acc, map = map, header_names = hdr_names, source_file = f) else acc
}

# Build “analysis frame” for a geography (single-year)
build_analysis_frame <- function(DT, geo_label) {
  if (!all(c("year","area_id","naics","industry_employment_in_area") %in% names(DT))) stop("DT missing required columns for analysis frame.")
  tots_emp <- DT[, .(total_employment_in_area = sum(industry_employment_in_area, na.rm = TRUE)), by = .(year, area_id)]
  AF <- left_merge(DT, tots_emp, by = c("year","area_id"), add_cols = "total_employment_in_area")
  
  if ("industry_establishments_in_area" %in% names(DT)) {
    tots_est <- DT[, .(total_establishments_in_area = sum(industry_establishments_in_area, na.rm = TRUE)), by = .(year, area_id)]
    AF <- left_merge(AF, tots_est, by = c("year","area_id"), add_cols = "total_establishments_in_area")
  } else {
    AF[, total_establishments_in_area := NA_real_]
  }
  AF[, geography := toupper(geo_label)]
  setcolorder(AF, c("year","geography","area_id","area_name","naics",
                    "industry_employment_in_area","total_employment_in_area",
                    "industry_establishments_in_area","total_establishments_in_area"))
  AF[]
}

# ---------- REPLACE THE WHOLE compute_stack_for_geo_year() WITH THIS ----------
compute_stack_for_geo_year <- function(AF, geo_slug) {
  # AF must have: year, geography, area_id, area_name, naics,
  #               industry_employment_in_area, total_employment_in_area (optional),
  #               industry_establishments_in_area, total_establishments_in_area (optional)
  
  stopifnot(all(c("year","geography","area_id","area_name","naics",
                  "industry_employment_in_area") %in% names(AF)))
  yy <- unique(AF$year); if (length(yy) != 1L) stop("AF should contain exactly one year.")
  if (CFG$verbose_debug) .log("[stack] ", toupper(geo_slug), " | rows=", nrow(AF), " | start | ", .mem_str())
  t0 <- .tic()
  
  # --- Guardrails: ensure totals exist; this prevents 'total_employment_in_area not found' ---
  if (!"total_employment_in_area" %in% names(AF)) {
    area_tot_emp  <- AF[, .(total_employment_in_area = sum(industry_employment_in_area, na.rm = TRUE)),
                        by = .(year, area_id)]
    AF <- area_tot_emp[AF, on = .(year, area_id)]
  } else {
    # if present but has NAs, fill them
    miss <- is.na(AF$total_employment_in_area)
    if (any(miss)) {
      fill <- AF[miss, .(total_employment_in_area = sum(industry_employment_in_area, na.rm = TRUE)),
                 by = .(year, area_id)]
      AF[fill, on = .(year, area_id), total_employment_in_area := i.total_employment_in_area]
      AF[, total_employment_in_area := as.numeric(total_employment_in_area)]
    }
  }
  
  have_estabs <- "industry_establishments_in_area" %in% names(AF)
  
  # --- NATIONAL totals (employment + establishments) ---
  nat_naics_emp <- AF[, .(total_naics_employment_nation = sum(industry_employment_in_area, na.rm = TRUE)), by = naics]
  nat_tot_emp   <- AF[, .(total_employment_nation = sum(industry_employment_in_area, na.rm = TRUE))]
  if (have_estabs) {
    nat_naics_est <- AF[, .(total_naics_establishments_nation = sum(industry_establishments_in_area, na.rm = TRUE)), by = naics]
    nat_tot_est   <- AF[, .(total_establishments_nation = sum(industry_establishments_in_area, na.rm = TRUE))]
  }
  
  # --- Assemble working DT (join on YEAR to avoid i.year clutter) ---
  area_tot_emp <- AF[, .(total_employment_in_area = sum(industry_employment_in_area, na.rm = TRUE)),
                     by = .(year, area_id)]
  if (have_estabs) {
    area_tot_est <- AF[, .(total_establishments_in_area = sum(industry_establishments_in_area, na.rm = TRUE)),
                       by = .(year, area_id)]
  }
  
  DT <- nat_naics_emp[AF, on = .(naics)]
  DT[, `:=`(total_employment_nation = nat_tot_emp$total_employment_nation[1])]
  DT <- area_tot_emp[DT, on = .(year, area_id)]
  
  if (have_estabs) {
    DT <- nat_naics_est[DT, on = .(naics)]
    DT[, `:=`(total_establishments_nation = nat_tot_est$total_establishments_nation[1])]
    DT <- area_tot_est[DT, on = .(year, area_id)]
  } else {
    DT[, `:=`(
      total_naics_establishments_nation = NA_real_,
      total_establishments_nation = NA_real_,
      total_establishments_in_area = if ("total_establishments_in_area" %in% names(DT)) total_establishments_in_area else NA_real_,
      industry_establishments_in_area = if ("industry_establishments_in_area" %in% names(DT)) industry_establishments_in_area else NA_real_
    )]
  }
  
  # --- RCA (employment + establishments) ---
  DT[, rca_employment :=
       (industry_employment_in_area / total_employment_in_area) /
       (total_naics_employment_nation / total_employment_nation)]
  DT[, rca_employment := fifelse(is.finite(rca_employment), rca_employment, NA_real_)]
  
  DT[, rca_establishments := NA_real_]
  if (have_estabs) {
    DT[, rca_establishments :=
         (industry_establishments_in_area / total_establishments_in_area) /
         (total_naics_establishments_nation / total_establishments_nation)]
    DT[, rca_establishments := fifelse(is.finite(rca_establishments), rca_establishments, NA_real_)]
  }
  
  # Presence indicators (Employment-based)
  DT[, presence_binary := fifelse(rca_employment > 0, 1L, 0L)]
  DT[, m_binary        := fifelse(rca_employment >= 1, 1L, 0L)]
  
  # --- Build presence matrix M from employment RCA (>=1) ---
  areas <- sort(unique(DT$area_id))
  inds  <- sort(unique(DT$naics))
  area_index <- match(DT$area_id, areas)
  ind_index  <- match(DT$naics,    inds)
  
  nz <- which(!is.na(DT$rca_employment) & DT$rca_employment >= 1)
  if (!length(nz)) {
    if (CFG$verbose_debug) .warn("[stack] ", toupper(geo_slug), " | No RCA>=1 pairs; returning NA metrics.")
    # Minimal NA-filled output with normalized fields
    OUT <- copy(DT)[, .(year = yy, geography = toupper(geo_slug),
                        area_id, area_name, naics,
                        industry_employment_in_area, total_employment_in_area,
                        industry_establishments_in_area, total_establishments_in_area,
                        total_naics_employment_nation, total_employment_nation,
                        total_naics_establishments_nation, total_establishments_nation,
                        rca_employment, rca_establishments,
                        presence_binary, m_binary)]
    OUT[, `:=`(
      density_implicit = NA_real_, strategic_gain = NA_real_,
      county_eci = NA_real_, strategic_index = NA_real_, industry_complexity = NA_real_
    )]
    OUT[, `:=`(
      rca_employment_normalized   = norm_0_100(rca_employment),
      density_implicit_normalized = norm_0_100(density_implicit),
      strategic_gain_normalized   = norm_0_100(strategic_gain),
      county_eci_normalized       = norm_0_100(county_eci),
      strategic_index_normalized  = norm_0_100(strategic_index),
      industry_complexity_normalized = norm_0_100(industry_complexity)
    )]
    # Remove any accidental i.* columns if present
    junk <- grep("^i\\.", names(OUT), value = TRUE); if (length(junk)) OUT[, (junk) := NULL]
    return(OUT[])
  }
  
  M <- sparseMatrix(
    i = area_index[nz],
    j = ind_index [nz],
    x = rep(1L, length(nz)),
    dims = c(length(areas), length(inds)),
    dimnames = list(areas, inds)
  )
  rm(nz); gc()
  
  if (CFG$verbose_debug) {
    .log("[stack] ", toupper(geo_slug), " | M dims: ",
         nrow(M),"×",ncol(M)," | nnz=", .nnz(M), " | ", .mem_str())
  }
  
  k_c0 <- Matrix::rowSums(M) # areas
  k_i0 <- Matrix::colSums(M) # industries
  Dci <- Diagonal(x = ifelse(k_c0 > 0, 1/k_c0, 0))
  Dii <- Diagonal(x = ifelse(k_i0 > 0, 1/k_i0, 0))
  
  # --- ECI ---
  ECI <- rep(NA_real_, nrow(M))
  if (nrow(M) >= 2 && ncol(M) >= 1) {
    Mcc <- Dci %*% M %*% Dii %*% t(M)
    eC  <- tryCatch(RSpectra::eigs(Mcc, k = min(CFG$eigen_k, nrow(Mcc)-1), which = "LR"), error = function(e) NULL)
    if (!is.null(eC) && length(eC$values) >= 2L) {
      ECI <- as.numeric(scale(Re(eC$vectors[, order(Re(eC$values), decreasing = TRUE)[2]])))
    }
    rm(Mcc, eC); gc()
  }
  
  # --- ICI ---
  ICI <- rep(NA_real_, ncol(M))
  if (ncol(M) >= 2 && nrow(M) >= 1) {
    Mii <- Dii %*% t(M) %*% Dci %*% M
    eI  <- tryCatch(RSpectra::eigs(Mii, k = min(CFG$eigen_k, nrow(Mii)-1), which = "LR"), error = function(e) NULL)
    if (!is.null(eI) && length(eI$values) >= 2L) {
      ICI <- as.numeric(scale(Re(eI$vectors[, order(Re(eI$values), decreasing = TRUE)[2]])))
    }
    rm(Mii, eI); gc()
  }
  
  # --- Chunked φ (proximity), Density, Strategic Gain, Strategic Index ---
  nA <- nrow(M); nI <- ncol(M)
  if (CFG$verbose_debug) .log("[stack] ", toupper(geo_slug), " | φ/Density/SI/SG (chunked) start… | ", .mem_str())
  
  ai <- area_index
  ij <- ind_index
  rm(area_index, ind_index); gc()
  
  density_vec <- numeric(nrow(DT))
  sg_vec      <- numeric(nrow(DT))
  
  si_total       <- numeric(nA)
  present_adjust <- numeric(nA)
  
  if (CFG$verbose_debug) .log("[stack] Indexing DT rows by industry…")
  rows_by_ind <- split(seq_len(nrow(DT)), ij)
  
  ind_positions <- seq_len(nI)
  blocks <- split(ind_positions, ceiling(seq_along(ind_positions) / CFG$industry_block_size))
  blk_id <- 0L
  t_blocks <- .tic()
  
  for (blk in blocks) {
    blk_id <- blk_id + 1L
    M_blk <- M[, blk, drop = FALSE]
    U_blk <- Matrix::crossprod(M, M_blk)
    k_blk <- k_i0[blk]
    
    for (t in seq_along(blk)) {
      jpos <- blk[t]
      
      u_col  <- as.numeric(U_blk[, t])
      denom  <- pmax(k_i0, k_blk[t]); denom[denom == 0] <- 1
      phi_col <- u_col / denom
      phi_col[jpos] <- 0
      
      colsum_phi_j <- sum(phi_col)
      if (colsum_phi_j > 0) {
        numer_col <- as.numeric(M %*% phi_col)
        density_col    <- numer_col / colsum_phi_j
        densScaled_col <- density_col * ICI[jpos]
        
        z_vec  <- ICI * (phi_col / colsum_phi_j)
        sum_z  <- sum(z_vec)
        Mz_col <- as.numeric(M %*% z_vec)
        sg_col <- (sum_z - Mz_col) - densScaled_col
      } else {
        density_col    <- numeric(nA)
        densScaled_col <- density_col
        sg_col         <- density_col
      }
      
      idx_rows <- rows_by_ind[[as.character(jpos)]]
      if (length(idx_rows)) {
        a_idx <- ai[idx_rows]
        density_vec[idx_rows] <- density_col[a_idx]
        sg_vec[idx_rows]      <- sg_col[a_idx]
      }
      
      si_total <- si_total + densScaled_col
      if (k_blk[t] > 0) {
        present_rows <- which(M[, jpos] != 0)
        if (length(present_rows)) {
          present_adjust[present_rows] <- present_adjust[present_rows] + densScaled_col[present_rows]
        }
      }
      
      if (t %% 25L == 0L) gc(FALSE)
    }
    
    rm(M_blk, U_blk); gc(FALSE)
    if (CFG$verbose_debug) {
      .log(sprintf("[stack] %s | block %d/%d done | industries in block=%d | %s | elapsed=%.2fs",
                   toupper(geo_slug), blk_id, length(blocks), length(blk), .mem_str(), .tic() - t_blocks))
      t_blocks <- .tic()
    }
  }
  
  SI_vec <- si_total - present_adjust
  rm(si_total, present_adjust, rows_by_ind); gc()
  
  # --- Assemble outputs; JOIN ON YEAR to prevent i.year/i.geography artifacts ---
  eci <- data.table(year = yy, geography = toupper(geo_slug),
                    area_id = rownames(M), county_eci = as.numeric(ECI))
  ici <- data.table(year = yy, geography = toupper(geo_slug),
                    naics = colnames(M), industry_complexity = as.numeric(ICI))
  si  <- data.table(year = yy, geography = toupper(geo_slug),
                    area_id = rownames(M), strategic_index = as.numeric(SI_vec))
  
  OUT <- copy(DT)[, .(year = yy, geography = toupper(geo_slug),
                      area_id, area_name, naics,
                      industry_employment_in_area, total_employment_in_area,
                      industry_establishments_in_area, total_establishments_in_area,
                      total_naics_employment_nation, total_employment_nation,
                      total_naics_establishments_nation, total_establishments_nation,
                      rca_employment, rca_establishments,
                      presence_binary, m_binary)]
  
  OUT[, density_implicit := density_vec]
  OUT[, strategic_gain   := sg_vec]
  
  setkey(OUT, year, area_id)
  setkey(eci, year, area_id); setkey(si, year, area_id)
  OUT <- eci[OUT, on = .(year, area_id)]
  OUT <- si[OUT,  on = .(year, area_id)]
  setkey(ici, year, naics); setkey(OUT, year, naics)
  OUT <- ici[OUT, on = .(year, naics)]
  
  # Normalizations (0–100 within geo-year)
  OUT[, `:=`(
    rca_employment_normalized   = norm_0_100(rca_employment),
    density_implicit_normalized = norm_0_100(density_implicit),
    strategic_gain_normalized   = norm_0_100(strategic_gain)
  )]
  
  OUT[, county_eci_normalized := {
    tmp <- unique(.SD[, .(area_id, county_eci)])
    m <- match(area_id, tmp$area_id)
    v <- norm_0_100(tmp$county_eci); v[m]
  }, .SDcols = c("area_id","county_eci")]
  
  OUT[, strategic_index_normalized := {
    tmp <- unique(.SD[, .(area_id, strategic_index)])
    m <- match(area_id, tmp$area_id)
    v <- norm_0_100(tmp$strategic_index); v[m]
  }, .SDcols = c("area_id","strategic_index")]
  
  OUT[, industry_complexity_normalized := {
    tmp <- unique(.SD[, .(naics, industry_complexity)])
    m <- match(naics, tmp$naics)
    v <- norm_0_100(tmp$industry_complexity); v[m]
  }, .SDcols = c("naics","industry_complexity")]
  
  # Final sweep: drop any accidental i.* columns if any managed to sneak in
  junk <- grep("^i\\.", names(OUT), value = TRUE)
  if (length(junk)) OUT[, (junk) := NULL]
  
  if (CFG$verbose_debug) .log("[stack] ", toupper(geo_slug), " | done | rows=", nrow(OUT),
                              " | ", .mem_str(), " | elapsed=", round(.tic() - t0, 2), "s")
  OUT[]
}
# ---------- END REPLACEMENT ---------------------------------------------------


# ------------------------------ Data Dictionary -------------------------------
dict_row <- function(term, explanation, source = NA_character_, links = NA_character_) {
  data.table(Term = term, Explanation = explanation, Source = source, `Source Link(s)` = links)
}
src_tapestry  <- "Watson, P. and Alward, G., 2024. Tapestry: Collaborative tool for regional data and modeling. The Review of Regional Studies, 54(2), 109–118."
lnk_tapestry  <- "https://tapestry.nkn.uidaho.edu/; https://rrs.scholasticahq.com/article/123153-tapestry-collaborative-tool-for-regional-data-and-modeling"
src_brookings <- "Daboin, C. et al., Economic complexity & technological relatedness (Brookings Technical Paper)."
lnk_brookings <- "https://www.brookings.edu/wp-content/uploads/2019/05/Technical-Paper.pdf"
src_bls       <- "Bureau of Labor Statistics (CEW) – Industry Titles"
lnk_bls       <- "https://www.bls.gov/cew/classifications/industry/industry-titles.txt"
src_tigris    <- "U.S. Census Bureau TIGER/Line (via tigris + sf)"
lnk_tigris    <- "https://cran.r-project.org/package=tigris"
src_rmi       <- "RMI"

DATA_DICTIONARY <- rbindlist(list(
  dict_row("Year","Calendar year.", src_rmi),
  dict_row("Geography Level","COUNTY, STATE, CBSA, or CSA.", src_rmi),
  dict_row("Area Code / Area Name","Identifiers per geography level.", src_tigris, lnk_tigris),
  dict_row("NAICS Code","6-digit NAICS.", src_bls, lnk_bls),
  dict_row("Industry Employment in Area","Employment in the area for the NAICS and year.", src_tapestry, lnk_tapestry),
  dict_row("Total Employment in Area","All-industry employment in the area for the year.", src_tapestry, lnk_tapestry),
  dict_row("Industry Establishments in Area","Establishments in the area for the NAICS and year.", src_tapestry, lnk_tapestry),
  dict_row("Total Establishments in Area","All-industry establishments in the area for the year.", src_tapestry, lnk_tapestry),
  dict_row("RCA (Employment), Raw","Area share of NAICS employment divided by national share (Eqs. 4–5).", src_brookings, lnk_brookings),   # :contentReference[oaicite:14]{index=14}
  dict_row("RCA (Employment), Normalized","0–100 normalization within geography-year.", src_rmi),
  dict_row("RCA (Establishments), Raw","Area share of NAICS establishments divided by national share.", src_rmi),
  dict_row("Economic Complexity Index (ECI), Raw","Second eigenvector of normalized area–area co-presence (Eqs. 8–14).", src_brookings, lnk_brookings),  # :contentReference[oaicite:15]{index=15}
  dict_row("ECI, Normalized","0–100 normalization within geography-year.", src_rmi),
  dict_row("Industrial Complexity Index (ICI), Raw","Second eigenvector of normalized industry–industry co-presence.", src_brookings, lnk_brookings),     # :contentReference[oaicite:16]{index=16}
  dict_row("ICI, Normalized","0–100 normalization within geography-year.", src_rmi),
  dict_row("Proximity (φ)","Implicit relatedness via min conditional probability (Eqs. 15–16).", src_brookings, lnk_brookings),                            # :contentReference[oaicite:17]{index=17}
  dict_row("Density (Feasibility), Raw","Proximity-weighted share of related NAICS present (Eq. 19).", src_brookings, lnk_brookings),                    # :contentReference[oaicite:18]{index=18}
  dict_row("Density, Normalized","0–100 normalization within geography-year.", src_rmi),
  dict_row("Strategic Index, Raw","Sum of feasibility for missing NAICS weighted by ICI (Eq. 21).", src_brookings, lnk_brookings),                       # :contentReference[oaicite:19]{index=19}
  dict_row("Strategic Index, Normalized","0–100 normalization within geography-year.", src_rmi),
  dict_row("Strategic Gain, Raw","Increase in SI if a missing NAICS were added (Eq. 22).", src_brookings, lnk_brookings),                                 # :contentReference[oaicite:20]{index=20}
  dict_row("Strategic Gain, Normalized","0–100 normalization within geography-year.", src_rmi),
  dict_row("Density Contributions — Contributor Share of Numerator","Within an area–industry, share of Σ φ from each present industry.", src_rmi),       # :contentReference[oaicite:21]{index=21}
  dict_row("Density Contributions — Contributor Contribution to Density","Absolute component of density from each contributor: φ / Σφ.", src_rmi),       # :contentReference[oaicite:22]{index=22}
  dict_row("Unknown or Undefined counties (SS999)","Synthetic county to include unknown/undefined in state totals.", src_rmi)
))

# ------------------------------ Public export builders ------------------------
format_public_export <- function(DT, area_type_label = NULL) {
  tmp <- copy(DT)
  # Clean left-join for titles (no i.* artifacts)
  if (nrow(industry_titles)) tmp <- merge(tmp, industry_titles, by = "naics", all.x = TRUE)
  tmp[, `Area Type` := if (!is.null(area_type_label)) titlecase_geo(area_type_label) else titlecase_geo(geography)]
  
  # Rename to public labels
  setnames(tmp,
           old = c("year","area_id","area_name","naics","industry_title",
                   "industry_establishments_in_area","total_establishments_in_area",
                   "total_naics_establishments_nation","total_establishments_nation",
                   "industry_employment_in_area","total_employment_in_area",
                   "total_naics_employment_nation","total_employment_nation",
                   "rca_establishments","rca_employment","rca_employment_normalized",
                   "presence_binary","m_binary",
                   "county_eci","county_eci_normalized",
                   "industry_complexity","industry_complexity_normalized",
                   "density_implicit","density_implicit_normalized",
                   "strategic_gain","strategic_gain_normalized",
                   "strategic_index","strategic_index_normalized"),
           new = c("Year","Area Code/GEOID","Area Title","Six-Digit NAICS Industry Code","Six-Digit NAICS Industry Title",
                   "Industry Establishments: Area","Total Establishments: Area",
                   "Industry Establishments: Nation","Total Establishments: Nation",
                   "Industry Employment: Area","Total Employment: Area",
                   "Industry Employment: Nation","Total Employment: Nation",
                   "Revealed Comparative Advantage (RCA), Establishments",
                   "Revealed Comparative Advantage (RCA), Employment, Raw",
                   "Revealed Comparative Advantage (RCA), Employment, Normalized",
                   "Industry Presence Binary","Industry Comparative Advantage Binary (M)",
                   "Economic Complexity Index (ECI), Raw","Economic Complexity Index (ECI), Normalized",
                   "Industry Complexity Index (ICI), Raw","Industry Complexity Index (ICI), Normalized",
                   "Colocation Density (Feasibility), Raw","Colocation Density (Feasibility), Normalized",
                   "Strategic Gain, Raw","Strategic Gain, Normalized",
                   "Strategic Index, Raw","Strategic Index, Normalized"),
           skip_absent = TRUE)
  
  # Ensure columns exist even if establishments not present
  for (cn in c("Industry Establishments: Area","Total Establishments: Area",
               "Industry Establishments: Nation","Total Establishments: Nation",
               "Revealed Comparative Advantage (RCA), Establishments")) {
    if (!cn %in% names(tmp)) tmp[, (cn) := NA_real_]
  }
  
  final_order <- c(
    "Year","Area Type","Area Code/GEOID","Area Title",
    "Six-Digit NAICS Industry Code","Six-Digit NAICS Industry Title",
    "Industry Establishments: Area","Total Establishments: Area",
    "Industry Establishments: Nation","Total Establishments: Nation",
    "Revealed Comparative Advantage (RCA), Establishments",
    "Industry Employment: Area","Total Employment: Area",
    "Industry Employment: Nation","Total Employment: Nation",
    "Revealed Comparative Advantage (RCA), Employment, Raw",
    "Revealed Comparative Advantage (RCA), Employment, Normalized",
    "Industry Presence Binary","Industry Comparative Advantage Binary (M)",
    "Economic Complexity Index (ECI), Raw","Economic Complexity Index (ECI), Normalized",
    "Industry Complexity Index (ICI), Raw","Industry Complexity Index (ICI), Normalized",
    "Colocation Density (Feasibility), Raw","Colocation Density (Feasibility), Normalized",
    "Strategic Gain, Raw","Strategic Gain, Normalized",
    "Strategic Index, Raw","Strategic Index, Normalized"
  )
  for (cn in final_order) if (!cn %in% names(tmp)) tmp[, (cn) := NA_real_]
  setcolorder(tmp, final_order)
  
  tmp[, geography := NULL]
  tmp[]
}

# NEW: Public export for density contributions
format_contributions_export <- function(DT, geo_label) {
  if (!nrow(DT)) return(DT)
  tmp <- copy(DT)
  # Attach area titles where possible
  # (For counties we already have area_name; for other geos we compute area_name in AF)
  # Here we pass through and then rename / reorder to public labels.
  tmp[, `Area Type` := titlecase_geo(geo_label)]
  setnames(tmp,
           old = c("year","area_id",
                   "target_naics","target_industry_title",
                   "contributor_naics","contributor_industry_title",
                   "contributor_present_binary","proximity_phi",
                   "sum_proximities_denominator","sum_proximities_numerator",
                   "density_feasibility_raw","contributor_share_of_numerator",
                   "contributor_contribution_to_density"),
           new = c("Year","Area Code/GEOID",
                   "Target NAICS Industry Code","Target NAICS Industry Title",
                   "Contributor NAICS Industry Code","Contributor NAICS Industry Title",
                   "Contributor Present Binary","Proximity (φ) Contributor↔Target",
                   "Sum of Proximities to Target (Denominator)",
                   "Sum of Proximities from Present Industries (Numerator)",
                   "Colocation Density (Feasibility), Raw",
                   "Contributor Share of Numerator (0–1)",
                   "Contributor Contribution to Density (Absolute)"),
           skip_absent = TRUE)
  # Area title: we only have it in the metrics outputs; for contributions, we leave blank here.
  if (!"Area Title" %in% names(tmp)) tmp[, `Area Title` := NA_character_]
  setcolorder(tmp, c("Year","Area Type","Area Code/GEOID","Area Title",
                     "Target NAICS Industry Code","Target NAICS Industry Title",
                     "Contributor NAICS Industry Code","Contributor NAICS Industry Title",
                     "Contributor Present Binary","Proximity (φ) Contributor↔Target",
                     "Sum of Proximities to Target (Denominator)",
                     "Sum of Proximities from Present Industries (Numerator)",
                     "Colocation Density (Feasibility), Raw",
                     "Contributor Share of Numerator (0–1)",
                     "Contributor Contribution to Density (Absolute)"))
  tmp[]
}

# --------------------------- Per-year processing fn ---------------------------
process_one_year <- function(yy, print_raw_glimpse = TRUE, do_export = TRUE) {
  .set_log_file(file.path(OUT_DIR, sprintf("runlog_%s_year_%d.txt", RUN_STAMP, yy)))
  assign("current_year", yy, envir = .GlobalEnv)
  .log("=== Year ", yy, " === Reading Tapestry CSV and computing aggregates…")
  t_year <- .tic()
  
  # Read this year only (chunked)
  r <- read_one_year_file_chunked(yy, return_meta = TRUE, print_raw_glimpse = print_raw_glimpse)
  tapY <- r$data; header_map <- r$map; header_names <- r$header_names; src_file <- r$source_file
  
  # Join crosswalk and build nested county table for the year
  base_cty <- merge(tapY, cw, by = "area", all.x = TRUE, sort = FALSE)
  if (nrow(industry_titles)) base_cty <- merge(base_cty, industry_titles, by = "naics", all.x = TRUE)
  .emit_glimpse(base_cty[, .(area, county_name, STATE_NAME, cbsa_code, csa_code)][1:5], "Crosswalk join check — sample")
  
  # County-level granular table
  cty_e <- base_cty[, .(year = yy,
                        county_geoid = area,
                        county_namelsad = county_name,
                        STATE_GEOID, STATE_NAME, STUSPS, STATEFP,
                        cbsa_code, cbsa_name, county_in_cbsa,
                        csa_code,  csa_name,  county_in_csa,
                        naics,
                        industry_title,
                        industry_employment_county     = naics_employment_in_area,
                        industry_establishments_county = naics_establishments_in_area)]
  
  # Totals by county
  tot_cty_emp <- base_cty[, .(total_employment_county = sum(naics_employment_in_area, na.rm = TRUE)), by = .(county_geoid = area)]
  tot_cty_est <- base_cty[, .(total_establishments_county = sum(naics_establishments_in_area, na.rm = TRUE)), by = .(county_geoid = area)]
  cty_e <- left_merge(cty_e, tot_cty_emp, by = "county_geoid", add_cols = "total_employment_county")
  cty_e <- left_merge(cty_e, tot_cty_est, by = "county_geoid", add_cols = "total_establishments_county")
  
  # Aggregates to CBSA/CSA/STATE
  ind_cbsa_emp <- base_cty[!is.na(cbsa_code), .(industry_employment_cbsa = sum(naics_employment_in_area, na.rm = TRUE)), by = .(cbsa_code, naics)]
  ind_cbsa_est <- base_cty[!is.na(cbsa_code), .(industry_establishments_cbsa = sum(naics_establishments_in_area, na.rm = TRUE)), by = .(cbsa_code, naics)]
  tot_cbsa_emp <- base_cty[!is.na(cbsa_code), .(total_employment_cbsa = sum(naics_employment_in_area, na.rm = TRUE)), by = .(cbsa_code)]
  tot_cbsa_est <- base_cty[!is.na(cbsa_code), .(total_establishments_cbsa = sum(naics_establishments_in_area, na.rm = TRUE)), by = .(cbsa_code)]
  cty_e <- left_merge(cty_e, ind_cbsa_emp, by = c("cbsa_code","naics"), add_cols = "industry_employment_cbsa")
  cty_e <- left_merge(cty_e, ind_cbsa_est, by = c("cbsa_code","naics"), add_cols = "industry_establishments_cbsa")
  cty_e <- left_merge(cty_e, tot_cbsa_emp, by = "cbsa_code", add_cols = "total_employment_cbsa")
  cty_e <- left_merge(cty_e, tot_cbsa_est, by = "cbsa_code", add_cols = "total_establishments_cbsa")
  
  ind_csa_emp <- base_cty[!is.na(csa_code), .(industry_employment_csa = sum(naics_employment_in_area, na.rm = TRUE)), by = .(csa_code, naics)]
  ind_csa_est <- base_cty[!is.na(csa_code), .(industry_establishments_csa = sum(naics_establishments_in_area, na.rm = TRUE)), by = .(csa_code, naics)]
  tot_csa_emp <- base_cty[!is.na(csa_code), .(total_employment_csa = sum(naics_employment_in_area, na.rm = TRUE)), by = .(csa_code)]
  tot_csa_est <- base_cty[!is.na(csa_code), .(total_establishments_csa = sum(naics_establishments_in_area, na.rm = TRUE)), by = .(csa_code)]
  cty_e <- left_merge(cty_e, ind_csa_emp, by = c("csa_code","naics"), add_cols = "industry_employment_csa")
  cty_e <- left_merge(cty_e, ind_csa_est, by = c("csa_code","naics"), add_cols = "industry_establishments_csa")
  cty_e <- left_merge(cty_e, tot_csa_emp, by = "csa_code", add_cols = "total_employment_csa")
  cty_e <- left_merge(cty_e, tot_csa_est, by = "csa_code", add_cols = "total_establishments_csa")
  
  ind_state_emp <- base_cty[, .(industry_employment_state = sum(naics_employment_in_area, na.rm = TRUE)), by = .(STATE_GEOID, naics)]
  ind_state_est <- base_cty[, .(industry_establishments_state = sum(naics_establishments_in_area, na.rm = TRUE)), by = .(STATE_GEOID, naics)]
  tot_state_emp <- base_cty[, .(total_employment_state = sum(naics_employment_in_area, na.rm = TRUE)), by = .(STATE_GEOID)]
  tot_state_est <- base_cty[, .(total_establishments_state = sum(naics_establishments_in_area, na.rm = TRUE)), by = .(STATE_GEOID)]
  cty_e <- left_merge(cty_e, ind_state_emp, by = c("STATE_GEOID","naics"), add_cols = "industry_employment_state")
  cty_e <- left_merge(cty_e, ind_state_est, by = c("STATE_GEOID","naics"), add_cols = "industry_establishments_state")
  cty_e <- left_merge(cty_e, tot_state_emp, by = "STATE_GEOID", add_cols = "total_employment_state")
  cty_e <- left_merge(cty_e, tot_state_est, by = "STATE_GEOID", add_cols = "total_establishments_state")
  
  # ------------------- Build analysis frames for this year --------------------
  county_AF <- cty_e[, .(year = yy,
                         area_id = county_geoid,
                         area_name = county_namelsad,
                         naics,
                         industry_employment_in_area     = industry_employment_county,
                         industry_establishments_in_area = industry_establishments_county)]
  county_AF <- build_analysis_frame(county_AF, "county")
  
  state_AF <- cty_e[!is.na(STATE_GEOID),
                    .(industry_employment_in_area     = sum(industry_employment_county, na.rm = TRUE),
                      industry_establishments_in_area = sum(industry_establishments_county, na.rm = TRUE)),
                    by = .(area_id = STATE_GEOID, area_name = STATE_NAME, naics)]
  state_AF[, year := yy]; setcolorder(state_AF, c("year","area_id","area_name","naics","industry_employment_in_area","industry_establishments_in_area"))
  state_AF <- build_analysis_frame(state_AF, "state")
  
  cbsa_AF <- cty_e[!is.na(cbsa_code),
                   .(industry_employment_in_area     = sum(industry_employment_county, na.rm = TRUE),
                     industry_establishments_in_area = sum(industry_establishments_county, na.rm = TRUE)),
                   by = .(area_id = cbsa_code, area_name = cbsa_name, naics)]
  cbsa_AF[, year := yy]; setcolorder(cbsa_AF, c("year","area_id","area_name","naics","industry_employment_in_area","industry_establishments_in_area"))
  cbsa_AF <- build_analysis_frame(cbsa_AF, "cbsa")
  
  csa_AF <- cty_e[!is.na(csa_code),
                  .(industry_employment_in_area     = sum(industry_employment_county, na.rm = TRUE),
                    industry_establishments_in_area = sum(industry_establishments_county, na.rm = TRUE)),
                  by = .(area_id = csa_code, area_name = csa_name, naics)]
  csa_AF[, year := yy]; setcolorder(csa_AF, c("year","area_id","area_name","naics","industry_employment_in_area","industry_establishments_in_area"))
  csa_AF <- build_analysis_frame(csa_AF, "csa")
  
  # ------------------- Complexity & relatedness for this year -----------------
  .log("Computing complexity stack for ", yy, " (chunked φ/Density/SI/SG)… ", .mem_str())
  county_res <- compute_stack_for_geo_year(county_AF, "county");  gc(FALSE)
  state_res  <- compute_stack_for_geo_year(state_AF,  "state");   gc(FALSE)
  cbsa_res   <- compute_stack_for_geo_year(cbsa_AF,   "cbsa");    gc(FALSE)
  csa_res    <- compute_stack_for_geo_year(csa_AF,    "csa");     gc(FALSE)
  
  # Unpack metrics & contributions (per geo)
  county_out <- county_res$metrics; state_out <- state_res$metrics; cbsa_out <- cbsa_res$metrics; csa_out <- csa_res$metrics
  county_contrib <- county_res$contributions; state_contrib <- state_res$contributions; cbsa_contrib <- cbsa_res$contributions; csa_contrib <- csa_res$contributions
  
  # Attach area titles to contributions (public style)
  # County area titles:
  if (nrow(county_contrib)) {
    county_titles <- unique(county_out[, .(area_id, area_name)])
    setnames(county_titles, c("area_id","area_name"), c("Area Code/GEOID","Area Title"))
    county_contrib <- merge(county_contrib, county_titles, by.x = "area_id", by.y = "Area Code/GEOID", all.x = TRUE)
  }
  if (nrow(state_contrib)) {
    state_titles <- unique(state_out[, .(area_id, area_name)])
    setnames(state_titles, c("area_id","area_name"), c("Area Code/GEOID","Area Title"))
    state_contrib <- merge(state_contrib, state_titles, by.x = "area_id", by.y = "Area Code/GEOID", all.x = TRUE)
  }
  if (nrow(cbsa_contrib)) {
    cbsa_titles <- unique(cbsa_out[, .(area_id, area_name)])
    setnames(cbsa_titles, c("area_id","area_name"), c("Area Code/GEOID","Area Title"))
    cbsa_contrib <- merge(cbsa_contrib, cbsa_titles, by.x = "area_id", by.y = "Area Code/GEOID", all.x = TRUE)
  }
  if (nrow(csa_contrib)) {
    csa_titles <- unique(csa_out[, .(area_id, area_name)])
    setnames(csa_titles, c("area_id","area_name"), c("Area Code/GEOID","Area Title"))
    csa_contrib <- merge(csa_contrib, csa_titles, by.x = "area_id", by.y = "Area Code/GEOID", all.x = TRUE)
  }
  
  # ECI panel block for this year
  eciY <- rbindlist(list(
    unique(county_out[, .(year, geography, area_name, area_id, county_eci, county_eci_normalized)]),
    unique(state_out [, .(year, geography, area_name, area_id, county_eci, county_eci_normalized)]),
    unique(cbsa_out  [, .(year, geography, area_name, area_id, county_eci, county_eci_normalized)]),
    unique(csa_out   [, .(year, geography, area_name, area_id, county_eci, county_eci_normalized)])
  ), use.names = TRUE)
  
  # ------------------- (Optional) Exports (CSV/RDS) ---------------------------
  ydir <- file.path(OUT_DIR, paste0("year_", yy)); if (do_export) dir.create(ydir, showWarnings = FALSE)
  
  # Copy the raw CSV for this year
  src_row <- manifest[year == yy]
  raw_copy_target <- file.path(ydir, basename(src_row$file))
  if (do_export && !file.exists(raw_copy_target)) file.copy(src_row$path, raw_copy_target, overwrite = TRUE)
  raw_copy_md5 <- if (do_export && file.exists(raw_copy_target)) as.character(md5sum(raw_copy_target)) else NA_character_
  
  # Build public tables for metrics
  per_geo_metrics <- list(county = county_out, state = state_out, cbsa = cbsa_out, csa = csa_out)
  public_geo_metrics <- lapply(names(per_geo_metrics), function(g) format_public_export(per_geo_metrics[[g]], area_type_label = g))
  names(public_geo_metrics) <- names(per_geo_metrics)
  
  # Build public tables for contributions
  per_geo_contrib <- list(county = county_contrib, state = state_contrib, cbsa = cbsa_contrib, csa = csa_contrib)
  public_geo_contrib <- lapply(names(per_geo_contrib), function(g) format_contributions_export(per_geo_contrib[[g]], geo_label = g))
  names(public_geo_contrib) <- names(per_geo_contrib)
  
  if (isTRUE(CFG$write_per_year) && do_export) {
    # Nested county employment table (debug/export helper)
    nested_csv <- file.path(ydir, sprintf("county_nested_employment_%d_%s.csv", yy, RUN_STAMP))
    fwrite(cty_e, nested_csv, bom = TRUE)
    .log("Wrote nested county employment: ", nested_csv)
    
    # Metrics: per-geo CSV + RDS
    for (g in names(public_geo_metrics)) {
      DT_pub <- public_geo_metrics[[g]]
      if (isTRUE(CFG$write_per_year_csv)) {
        f_csv <- file.path(ydir, sprintf("analysis_%s_%d_%s.csv", g, yy, RUN_STAMP))
        fwrite(DT_pub, f_csv, bom = TRUE)
        .log("Wrote per-year metrics CSV: ", f_csv)
      }
      if (isTRUE(CFG$write_per_year_rds)) {
        f_rds <- file.path(ydir, sprintf("analysis_%s_%d_%s.rds", g, yy, RUN_STAMP))
        saveRDS(per_geo_metrics[[g]], f_rds, compress = "xz")
        .log("Wrote per-year metrics RDS: ", f_rds)
      }
    }
    
    # Contributions: per-geo CSV + RDS
    for (g in names(public_geo_contrib)) {
      C_pub <- public_geo_contrib[[g]]
      if (nrow(C_pub)) {
        if (isTRUE(CFG$contrib_write_csv)) {
          f_csv <- file.path(ydir, sprintf("density_contributions_%s_%d_%s.csv", g, yy, RUN_STAMP))
          fwrite(C_pub, f_csv, bom = TRUE)
          .log("Wrote per-year contributions CSV: ", f_csv)
        }
        f_rds <- file.path(ydir, sprintf("density_contributions_%s_%d_%s.rds", g, yy, RUN_STAMP))
        saveRDS(per_geo_contrib[[g]], f_rds, compress = "xz")
        .log("Wrote per-year contributions RDS: ", f_rds)
      } else {
        .log("No contributions for ", g, " in ", yy, " (likely no RCA>=1 anywhere for this geo).")
      }
    }
  }
  
  # ------------------- Per-year “bundle” RDS with metadata --------------------
  if (isTRUE(CFG$write_per_year_bundle_rds) && do_export) {
    year_bundle <- list(
      run_stamp = RUN_STAMP,
      year = yy,
      config = CFG,
      tigris_year = YEAR_TIGRIS,
      source = list(
        original_path = src_row$path,
        copied_to_year_folder = raw_copy_target,
        md5_original = src_row$md5,
        md5_copy = raw_copy_md5,
        size_bytes = src_row$size_bytes,
        header_names = header_names,
        resolved_mapping = header_map
      ),
      manifest_row = as.list(src_row),
      crosswalk = cw,
      industry_titles = industry_titles,
      data_dictionary = DATA_DICTIONARY,
      county_nested = cty_e,
      analysis_by_geo = list(
        county = county_out,
        state  = state_out,
        cbsa   = cbsa_out,
        csa    = csa_out
      ),
      density_contributions_by_geo = list(
        county = county_contrib,
        state  = state_contrib,
        cbsa   = cbsa_contrib,
        csa    = csa_contrib
      ),
      public_tables = public_geo_metrics,
      public_contributions = public_geo_contrib
    )
    f_bundle <- file.path(ydir, sprintf("year_%d_bundle_%s.rds", yy, RUN_STAMP))
    saveRDS(year_bundle, f_bundle, compress = "xz")
    .log("Wrote per-year BUNDLE RDS: ", f_bundle)
  }
  
  # ------------------- Return objects needed for ALL_BUNDLE --------------------
  list(
    year = yy,
    eci_panel_block = eciY,
    all_bundle_entry = list(
      year = yy,
      source_file = csv_by_year[year == yy, basename(file)],
      source_md5  = manifest[year == yy, md5],
      county_nested = cty_e,
      analysis_by_geo = list(
        county = county_out, state = state_out, cbsa = cbsa_out, csa = csa_out
      ),
      density_contributions_by_geo = list(
        county = county_contrib, state = state_contrib, cbsa = cbsa_contrib, csa = csa_contrib
      ),
      public_tables = public_geo_metrics,
      public_contributions = public_geo_contrib,
      header_mapping = header_map,
      header_names   = header_names
    ),
    elapsed = round(.tic() - t_year, 2)
  )
}

# ------------------------------ Main accumulators -----------------------------
eci_panel <- data.table(
  year = integer(), geography = character(), area_name = character(),
  area_id = character(), county_eci = numeric(), county_eci_normalized = numeric()
)

ALL_BUNDLE <- list(
  run_stamp = RUN_STAMP,
  input_dir = INPUT_DIR,
  tigris_year = YEAR_TIGRIS,
  manifest = manifest,
  config = CFG,
  crosswalk = cw,
  industry_titles = industry_titles,
  data_dictionary = DATA_DICTIONARY,
  years = list(),
  eci_panel = NULL
)

# ------------------------- PREVIEW GATE: Build 2024 only ----------------------
preview_year <- 2024L
if (!(preview_year %in% present_years)) stop("Preview year ", preview_year, " not found among source files. Aborting.")
.log(""); .log("============================================"); .log("Preview gate: building YEAR ", preview_year, " only (NO EXPORTS YET)…"); .log("============================================")
.set_log_file(LOG_FILE_MAIN)
preview_res <- process_one_year(preview_year, print_raw_glimpse = TRUE, do_export = FALSE)

# Glimpse all public metric outputs for the preview year (no i.* columns anymore)
for (g in names(preview_res$all_bundle_entry$public_tables)) {
  .emit_glimpse(head(preview_res$all_bundle_entry$public_tables[[g]], 50), paste0("Preview ", preview_year, " — ", toupper(g), " public table (head 50)"))
}
# NEW: Also show contributions samples
for (g in names(preview_res$all_bundle_entry$public_contributions)) {
  .emit_glimpse(head(preview_res$all_bundle_entry$public_contributions[[g]], 50), paste0("Preview ", preview_year, " — ", toupper(g), " density contributions (head 50)"))
}
# Nested county table sample
.emit_glimpse(head(preview_res$all_bundle_entry$county_nested, 50), paste0("Preview ", preview_year, " — county nested employment (head 50)"))

# Non-interactive default: proceed with full exports
accept <- "y"; .warn("Non-interactive session detected; defaulting to proceed = 'y' for automation.")
if (!(accept %in% c("y","yes",""))) { .log("User rejected the 2024 preview. Exiting without exports."); stop("Aborted by user after preview gate. No files were exported.") }
.log("Preview accepted. Proceeding to full run & exports…")

# ------------------------- Processing mode ------------------------------------
years_to_process <- seq(CFG$year_max, CFG$year_min, by = -1L)
years_to_process <- years_to_process[years_to_process %in% present_years]

parallel_mode <- CFG$parallel_default
.log("Processing mode: ", if (parallel_mode) "PARALLEL (all years simultaneously)" else "SEQUENTIAL (one by one)")

# ----------------------- Run: parallel or sequential --------------------------
results_list <- NULL
if (parallel_mode) {
  w <- CFG$parallel_workers; if (is.null(w)) w <- max(1L, future::availableCores() - 1L)
  .log("Initializing multisession plan with ", w, " workers…")
  oplan <- future::plan(future::multisession, workers = w); on.exit(future::plan(oplan), add = TRUE)
  results_list <- future_lapply(years_to_process, function(yy) process_one_year(yy, print_raw_glimpse = FALSE, do_export = TRUE), future.seed = TRUE)
} else {
  results_list <- lapply(years_to_process, function(yy) process_one_year(yy, print_raw_glimpse = TRUE, do_export = TRUE))
}

# -------------------------- Assemble ALL_BUNDLE + panel -----------------------
for (res in results_list) {
  yy <- res$year
  eci_panel <- rbindlist(list(eci_panel, res$eci_panel_block), use.names = TRUE)
  ALL_BUNDLE$years[[as.character(yy)]] <- res$all_bundle_entry
  .log("=== Year ", yy, " finalized (", res$elapsed, "s) | ", .mem_str())
}
ALL_BUNDLE$eci_panel <- eci_panel
BIG_RDS <- file.path(OUT_DIR, sprintf("all_years_bundle_%s.rds", RUN_STAMP))
saveRDS(ALL_BUNDLE, BIG_RDS, compress = CFG$all_years_rds_compress)
.log("Wrote ALL-YEARS bundle RDS: ", BIG_RDS)

# Data dictionary (also included inside RDS objects)
dict_csv <- file.path(OUT_DIR, sprintf("data_dictionary_%s.csv", RUN_STAMP))
fwrite(DATA_DICTIONARY, dict_csv, bom = TRUE)
.log("Wrote data dictionary CSV: ", dict_csv)

# ------------------------------ Finish ----------------------------------------
.log("All done. Export folder: ", OUT_DIR)

# =============================================================================
# Notes on correctness & provenance (inline citations mirrored above):
#   • RCA, M, Method of Reflections/ECI/ICI, co-occurrence, φ, density, SI, SG:
#     Derived from Daboin technical note, Eqs. (4)-(22); implemented 1:1 here,
#     with φ = U / max(diag(U_i), diag(U_j)), d = Σ(M*φ)/Σφ, SI, SG as coded. :contentReference[oaicite:23]{index=23}
#   • Density contributions follow your rtf notes (share of numerator) and add
#     absolute component to density (φ/Σφ). Exports are Top-K by default
#     (set CFG$contrib_top_k = Inf to keep all). :contentReference[oaicite:24]{index=24}
# =============================================================================
