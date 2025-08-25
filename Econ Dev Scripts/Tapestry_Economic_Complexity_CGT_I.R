# =============================================================================
# Tapestry Employment — ALL YEARS (County NAICS 6-digit)
# Cross-platform (Mac/Windows), OneDrive-only outputs, Parallel I/O, Memory-safe
# FULL DEBUG VERSION with timestamped folders and verification
#
# Per your rules (2025-08-24):
#   • Output dir = OneDrive path:
#     .../OneDrive - RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/TAPESTRY_EMPLOYMENT_CGT
#   • NAICS 999999 is JUST ANOTHER INDUSTRY:
#       - Keep it, but DO NOT use for County/National totals.
#   • Exports (same base filename, same folder):
#       1) CSV (combined, streamed by year)
#       2) DuckDB (.duckdb)  — built from CSV
#       3) Parquet (.parquet) — streamed out FROM DuckDB (avoids Arrow OOM)
#       4) RDS (.rds) — **list** of separate data frames:
#           INDUSTRY_TITLES
#           AREA_TITLES
#           YEAR_AREA_INDUSTRY_ESTABLISHMENTS
#           YEAR_AREA_INDUSTRY_EMPLOYMENT
#           YEAR_AREA_INDUSTRY_LQ
#
# IMPORTANT: All exports go DIRECTLY to OneDrive (no local scratch staging).
# =============================================================================

cat("\n=============== SCRIPT START ===============\n")
cat("Time:", as.character(Sys.time()), "\n")
cat("============================================\n\n")

suppressPackageStartupMessages({
  library(data.table)
  library(readr)
  library(stringr)
  library(future)
  library(future.apply)
  suppressWarnings(requireNamespace("progressr", quietly = TRUE))
  library(dplyr)
  # Optional (for verification only)
  have_arrow <- requireNamespace("arrow", quietly = TRUE)
  # Required for DB & Parquet export
  if (!requireNamespace("duckdb", quietly = TRUE) || !requireNamespace("DBI", quietly = TRUE)) {
    stop("Packages 'duckdb' and 'DBI' are required. Install with install.packages(c('duckdb','DBI')).")
  }
})

# ------------------------------ CONFIG ---------------------------------------
CFG <- list(
  # Resolve OneDrive root automatically
  use_onedrive_resolver = TRUE,
  
  # Relative subpath (below OneDrive root) to NAICS_6D folder (INPUT):
  input_subpath = file.path(
    "US Program - Documents","6_Projects","Clean Regional Economic Development",
    "ACRE","Data","Raw Data","Tapestry_Employment",
    "contains_naics_999999_county_XX999","NAICS_6D"
  ),
  
  # Optional override to bypass resolver:
  input_absolute_override = NULL,
  
  # Metadata sources (prefer explicit Windows paths; fallback to OneDrive root):
  area_titles_path_win     = "C:/Users/benjamin.feshbach/OneDrive - RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/BLS_QCEW/metadata/area-titles.csv",
  industry_titles_path_win = "C:/Users/benjamin.feshbach/OneDrive - RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/BLS_QCEW/metadata/industry-titles.csv",
  bls_metadata_rel = file.path(
    "US Program - Documents","6_Projects","Clean Regional Economic Development",
    "ACRE","Data","Raw Data","BLS_QCEW","metadata"
  ),
  
  # *** OUTPUT DIRECTORY (OneDrive-aware) ***
  output_subpath = file.path(
    "US Program - Documents","6_Projects","Clean Regional Economic Development",
    "ACRE","Data","TAPESTRY_EMPLOYMENT_CGT"
  ),
  
  # Export toggles (all ON)
  write_combined_csv = TRUE,
  write_duckdb       = TRUE,
  write_parquet      = TRUE,  # via DuckDB COPY (streamed)
  write_rds_split    = TRUE,
  
  # Titles during read
  attach_titles_during_read = TRUE,
  
  # Parallel options:
  parallel_workers = NULL,  # NULL → availableCores()-1
  show_progress = TRUE
)

# data.table threads
options(datatable.print.topn = 3, datatable.print.nrows = 80)
setDTthreads(max(1L, parallel::detectCores(logical = TRUE) - 1L))

# ------------------------------ DEBUG helpers ---------------------------------
.mem_str <- function() {
  g <- gc(FALSE); paste0("mem~", format(round(sum(g[,2]) / 1024, 2), nsmall = 2), " GB")
}
fmt_bytes <- function(b) {
  if (is.na(b)) return("NA")
  u <- c("B","KB","MB","GB","TB"); i <- findInterval(b, c(0, 1024^(1:4)))
  paste0(round(b/1024^i, 2), " ", u[i+1])
}
tictoc <- local({
  .t0 <- NULL
  start <- function() { .t0 <<- proc.time()[3]; .t0 }
  stop  <- function(msg = "") {
    if (is.null(.t0)) return(invisible(NA_real_))
    dt <- proc.time()[3] - .t0; .t0 <<- NULL
    if (nzchar(msg)) cat(sprintf("%s Elapsed: %.2f s\n", msg, dt))
    dt
  }
  list(start = start, stop = stop)
})
ts <- function() format(Sys.time(), "%Y-%m-%d %H:%M:%S")
safe_glimpse <- function(dt, n = 5L, title = "SAMPLE") {
  cat(sprintf("\n[%s] --- %s --- rows=%s cols=%s %s\n",
              ts(), title, format(nrow(dt), big.mark=","), ncol(dt), .mem_str()))
  if (nrow(dt)) print(head(dt, n))
  cat("Column classes (first few):\n")
  cls <- vapply(dt, function(v) paste(class(v), collapse = "/"), character(1))
  print(utils::head(cls, n = 12))
  cat("----\n")
}
sql_quote <- function(path) gsub("'", "''", path, fixed = TRUE)

cat("\n========== DEBUG ENV ==========\n")
print(list(
  R.version = R.version.string,
  Platform  = R.version$platform,
  Sys.time  = as.character(Sys.time()),
  getwd     = getwd(),
  R.home    = R.home()
))
cat("================================\n\n")

# --------------------------- OneDrive root resolver ---------------------------
cat("[DEBUG] Starting OneDrive resolution...\n")
path_has_onedrive_folder <- function(p) {
  parts <- strsplit(normalizePath(p, winslash = "/", mustWork = FALSE), "/", fixed = TRUE)[[1]]
  if (!length(parts)) return(NA_character_)
  idx <- which(grepl("^OneDrive( |-|$)", parts))
  if (!length(idx)) return(NA_character_)
  paste(parts[seq_len(max(idx))], collapse = "/")
}
resolve_onedrive_root <- function() {
  env_root <- Sys.getenv("ONEDRIVE_ROOT", unset = NA_character_)
  if (!is.na(env_root) && nzchar(env_root) && dir.exists(env_root)) {
    cat("[DEBUG] Found ONEDRIVE_ROOT env var:", env_root, "\n"); return(path.expand(env_root))
  }
  wd <- getwd(); guess <- path_has_onedrive_folder(wd)
  if (!is.na(guess) && dir.exists(guess)) {
    cat("[DEBUG] Found OneDrive in working directory path:", guess, "\n"); return(guess)
  }
  sysname <- Sys.info()[["sysname"]]; home <- path.expand("~")
  user <- Sys.getenv("USERNAME", unset = Sys.getenv("USER",""))
  candidates <- if (identical(sysname, "Windows")) c(
    Sys.getenv("OneDriveCommercial",""),
    Sys.getenv("OneDriveConsumer",""),
    Sys.getenv("OneDrive",""),
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
  ) else c(
    file.path(home,"OneDrive - RMI"),
    file.path(home,"OneDrive")
  )
  cat("[DEBUG] Checking OneDrive candidates...\n")
  hits <- unique(candidates[nzchar(candidates) & dir.exists(candidates)])
  if (length(hits)) { cat("[DEBUG] Found OneDrive at:", hits[[1]], "\n"); return(hits[[1]]) }
  stop("Could not locate your OneDrive root. Set ONEDRIVE_ROOT or provide CFG$input_absolute_override.")
}

# Resolve input dir and output base dir (OneDrive-only)
if (!is.null(CFG$input_absolute_override)) {
  INPUT_DIR <- path.expand(CFG$input_absolute_override)
  ONEDRIVE_ROOT <- NA_character_
  cat("[DEBUG] Using absolute override for input\n")
} else if (isTRUE(CFG$use_onedrive_resolver)) {
  ONEDRIVE_ROOT <- resolve_onedrive_root()
  message("Using OneDrive root: ", ONEDRIVE_ROOT)
  INPUT_DIR <- file.path(ONEDRIVE_ROOT, CFG$input_subpath)
} else {
  stop("OneDrive resolver disabled; this script writes only to OneDrive per requirement.")
}
if (!dir.exists(INPUT_DIR)) stop("Data folder not found: ", INPUT_DIR)
message("Input directory: ", gsub("\\\\","/", INPUT_DIR))

# Resolve metadata file paths (area titles / industry titles)
cat("[DEBUG] Resolving metadata paths...\n")
resolve_file_first_hit <- function(candidates) {
  hits <- candidates[file.exists(candidates)]
  if (length(hits)) return(hits[[1]])
  NA_character_
}
area_titles_path <- resolve_file_first_hit(c(
  CFG$area_titles_path_win,
  if (!is.na(ONEDRIVE_ROOT)) file.path(ONEDRIVE_ROOT, CFG$bls_metadata_rel, "area-titles.csv") else NA_character_
))
industry_titles_path <- resolve_file_first_hit(c(
  CFG$industry_titles_path_win,
  if (!is.na(ONEDRIVE_ROOT)) file.path(ONEDRIVE_ROOT, CFG$bls_metadata_rel, "industry-titles.csv") else NA_character_
))
if (is.na(area_titles_path)) {
  warning("area-titles.csv not found; 'County Name' will be NA.")
} else {
  cat("[DEBUG] Found area-titles.csv at:", area_titles_path, "\n")
}
if (is.na(industry_titles_path)) {
  warning("industry-titles.csv not found; 'Six-Digit NAICS Industry Title' will be NA.")
} else {
  cat("[DEBUG] Found industry-titles.csv at:", industry_titles_path, "\n")
}

# ------------------------------ Output paths ----------------------------------
if (is.na(ONEDRIVE_ROOT)) stop("ONEDRIVE_ROOT not resolved; cannot construct output path.")
BASE_OUT_DIR <- file.path(ONEDRIVE_ROOT, CFG$output_subpath)
if (!dir.exists(BASE_OUT_DIR)) {
  cat("[DEBUG] Creating base output directory:", BASE_OUT_DIR, "\n")
  dir.create(BASE_OUT_DIR, recursive = TRUE, showWarnings = FALSE)
}
RUN_STAMP <- format(Sys.time(), "%Y%m%d_%H%M%S")
OUT_DIR <- file.path(BASE_OUT_DIR, paste0("run_", RUN_STAMP))
cat("[DEBUG] Creating timestamped output directory:", OUT_DIR, "\n")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
if (!dir.exists(OUT_DIR)) stop("Failed to create output directory: ", OUT_DIR)
message("Output directory: ", gsub("\\\\","/", OUT_DIR))

# Base filename (same across formats)
BASE_NAME <- "tapestry_employment_all_years_processed"

# ------------------------------ Helpers ---------------------------------------
read_tapestry_csv <- function(path) {
  dt <- readr::read_csv(
    file = path,
    col_types = cols_only(
      year             = col_integer(),
      area_fips        = col_character(),
      naics_code       = col_character(),
      tap_estabs_count = col_double(),
      tap_emplvl_est_3 = col_double()
    ),
    show_col_types = FALSE, progress = FALSE
  )
  dt <- dt |>
    mutate(
      area_fips  = stringr::str_pad(area_fips,  width = 5, side = "left", pad = "0"),
      naics_code = stringr::str_pad(naics_code, width = 6, side = "left", pad = "0")
    )
  as.data.table(dt)
}
extract_year <- function(p) {
  b <- basename(p)
  m <- stringr::str_match(b, "^((19|20)\\d{2})\\.csv$")[,2]
  as.integer(m)
}
to_title_case <- function(x) {
  if (length(x) == 0) return(x)
  small <- c("and","or","of","the","for","in","on","at","by","to","with","a","an","vs","via","per","from")
  vapply(x, function(s) {
    s <- trimws(s); if (!nzchar(s)) return(s)
    s <- gsub("\\s+", " ", s)
    words <- unlist(strsplit(tolower(s), " ", fixed = TRUE))
    cap <- function(w) paste0(toupper(substr(w,1,1)), substr(w,2,nchar(w)))
    paste(vapply(seq_along(words), function(i) {
      w <- words[i]
      if (i == 1 || i == length(words) || !(w %in% small)) cap(w) else w
    }, character(1)), collapse = " ")
  }, character(1))
}
clean_industry_title <- function(raw_title, code = NULL) {
  s <- raw_title
  s <- ifelse(is.na(s), "", s)
  s <- gsub("^\\s*NAICS\\s*\\d{2,6}\\s+", "", s, ignore.case = TRUE)
  s <- gsub("^\\s*\\d{2,6}\\s*[-:,]?\\s*", "", s)
  if (!is.null(code)) {
    if (length(code) != length(s)) code <- rep_len(code, length.out = length(s))
    s <- mapply(function(si, ci) {
      if (!is.na(ci) && nzchar(ci)) sub(paste0("^\\s*", ci, "\\s*[-:,]?\\s*"), "", si, ignore.case = TRUE) else si
    }, si = s, ci = code, USE.NAMES = FALSE)
  }
  s <- gsub("\\s*,\\s*", ", ", s)
  to_title_case(trimws(s))
}
build_area_map <- function(path) {
  if (is.na(path)) return(NULL)
  at <- tryCatch(
    readr::read_csv(path,
                    col_types = cols(
                      area_fips  = col_character(),
                      area_title = col_character()
                    ),
                    show_col_types = FALSE, progress = FALSE),
    error = function(e) { warning("Failed to read area-titles.csv: ", e$message); NULL }
  )
  if (is.null(at) || !nrow(at)) return(NULL)
  at <- as.data.table(at)
  at[, area_fips := stringr::str_pad(area_fips, width = 5, side = "left", pad = "0")]
  at <- at[nchar(area_fips) == 5 & !grepl("US", area_fips, fixed = TRUE)]
  at <- at[substr(area_fips, 3, 5) != "000"]
  setnames(at, c("area_fips","area_title"), c("County GEOID","County Name"))
  unique(at, by = "County GEOID")
}
build_naics6_map <- function(path) {
  if (is.na(path)) return(NULL)
  it <- tryCatch(
    readr::read_csv(path,
                    col_types = cols(
                      industry_code  = col_character(),
                      industry_title = col_character()
                    ),
                    show_col_types = FALSE, progress = FALSE),
    error = function(e) { warning("Failed to read industry-titles.csv: ", e$message); NULL }
  )
  if (is.null(it) || !nrow(it)) {
    return(data.table(`Six-Digit NAICS Code` = "999999",
                      `Six-Digit NAICS Industry Title` = "All Industries"))
  }
  it <- as.data.table(it)
  it[, code6 := gsub("\\D", "", industry_code)]
  it[nchar(code6) != 6, code6 := NA_character_]
  dt1 <- it[!is.na(code6), .(code6, title = clean_industry_title(industry_title, code6))]
  dt2 <- it[is.na(code6) & grepl("\\bNAICS\\s*\\d{6}\\b", industry_title, ignore.case = TRUE)]
  if (nrow(dt2)) {
    dt2[, code6 := sub(".*NAICS\\s*(\\d{6}).*", "\\1", industry_title, ignore.case = TRUE)]
    dt2 <- dt2[, .(code6, title = clean_industry_title(industry_title))]
  }
  out <- rbindlist(list(dt1, dt2), use.names = TRUE, fill = TRUE)
  out <- out[!is.na(code6)]
  if (!("999999" %in% out$code6)) {
    out <- rbind(out, data.table(code6 = "999999", title = "All Industries"))
  }
  setnames(out, c("code6","title"), c("Six-Digit NAICS Code","Six-Digit NAICS Industry Title"))
  unique(out, by = "Six-Digit NAICS Code")
}

# ------------------------------ Discover files --------------------------------
cat("[DEBUG] Discovering CSV files...\n")
all_csv <- list.files(INPUT_DIR, pattern = "\\.csv$", full.names = TRUE)
if (!length(all_csv)) stop("No CSV files found in INPUT_DIR: ", INPUT_DIR)
years <- vapply(all_csv, extract_year, integer(1L), USE.NAMES = FALSE)
keep <- !is.na(years)
all_csv  <- all_csv[keep]
years    <- years[keep]
o <- order(years, basename(all_csv))
all_csv  <- all_csv[o]; years <- years[o]
yr_last_idx <- tapply(seq_along(years), years, tail, n = 1)
csv_by_year <- data.table(year = as.integer(names(yr_last_idx)),
                          file = all_csv[unlist(yr_last_idx, use.names = FALSE)])
setorder(csv_by_year, year)
cat("[DEBUG] Found", nrow(csv_by_year), "year files\n")
message("Years detected: ", paste(csv_by_year$year, collapse = ", "))

# ------------------------------ Load metadata maps ----------------------------
cat("[DEBUG] Loading metadata maps...\n")
AREA_MAP  <- build_area_map(area_titles_path)
NAICS_MAP <- build_naics6_map(industry_titles_path)
cat(sprintf("[META] AREA_MAP rows=%s; NAICS_MAP rows=%s\n",
            format(if(is.null(AREA_MAP)) 0 else nrow(AREA_MAP), big.mark=","), 
            format(if(is.null(NAICS_MAP)) 0 else nrow(NAICS_MAP), big.mark=",")))
if (!is.null(AREA_MAP))  safe_glimpse(AREA_MAP,  n = 3, title = "AREA_MAP sample")
if (!is.null(NAICS_MAP)) safe_glimpse(NAICS_MAP, n = 3, title = "NAICS_MAP sample")

# ------------------------------ Parallel setup --------------------------------
cat("[DEBUG] Setting up parallel processing...\n")
if (is.null(CFG$parallel_workers)) {
  w <- max(1L, future::availableCores() - 1L)
} else {
  w <- max(1L, as.integer(CFG$parallel_workers))
}
plan(multisession, workers = w)
on.exit({ try(plan(sequential), silent = TRUE) }, add = TRUE)
message("Parallel workers: ", w)

# ------------------------------ Read ALL years (parallel) ---------------------
message("[READ] Starting parallel ingest across years… (", .mem_str(), ")")
read_all_years <- function() {
  attach_titles <- isTRUE(CFG$attach_titles_during_read)
  reader <- function(i) {
    y <- csv_by_year$year[i]; f <- csv_by_year$file[i]
    dt <- read_tapestry_csv(f)
    if ("year" %in% names(dt)) dt <- dt[year == y]
    if (!nrow(dt)) return(NULL)
    setnames(dt,
             old = c("year","area_fips","naics_code","tap_estabs_count","tap_emplvl_est_3"),
             new = c("Year","County GEOID","Six-Digit NAICS Code",
                     "NAICS Industry Establishments, County",
                     "NAICS Industry Employment, County"),
             skip_absent = FALSE)
    if (attach_titles) {
      if (!is.null(AREA_MAP)) {
        idxA <- match(dt$`County GEOID`, AREA_MAP$`County GEOID`)
        dt[, `County Name` := AREA_MAP$`County Name`[idxA]]
      } else dt[, `County Name` := NA_character_]
      if (!is.null(NAICS_MAP)) {
        idxN <- match(dt$`Six-Digit NAICS Code`, NAICS_MAP$`Six-Digit NAICS Code`)
        dt[, `Six-Digit NAICS Industry Title` := NAICS_MAP$`Six-Digit NAICS Industry Title`[idxN]]
      } else dt[, `Six-Digit NAICS Industry Title` := NA_character_]
    }
    n_na_county <- sum(is.na(dt$`County Name`))
    n_na_naics  <- sum(is.na(dt$`Six-Digit NAICS Industry Title`))
    n_geoid     <- data.table::uniqueN(dt$`County GEOID`)
    n_naics     <- data.table::uniqueN(dt$`Six-Digit NAICS Code`)
    n_999999    <- sum(dt$`Six-Digit NAICS Code` == "999999")
    message(sprintf("[READ %d] rows=%s | counties=%s | naics=%s | 999999 rows=%s | NA County Name=%s | NA NAICS Title=%s",
                    y, format(nrow(dt), big.mark=","), n_geoid, n_naics,
                    format(n_999999, big.mark=","), format(n_na_county, big.mark=","), format(n_na_naics, big.mark=",")))
    dt[]
  }
  if (isTRUE(CFG$show_progress) && requireNamespace("progressr", quietly = TRUE)) {
    progressr::with_progress({
      p <- progressr::progressor(steps = nrow(csv_by_year))
      res <- future_lapply(seq_len(nrow(csv_by_year)), function(i) {
        p(sprintf("Reading %d", csv_by_year$year[i])); reader(i)
      }, future.seed = TRUE)
      res
    })
  } else {
    future_lapply(seq_len(nrow(csv_by_year)), reader, future.seed = TRUE)
  }
}
year_chunks <- read_all_years()
cat("[DEBUG] Combining year chunks...\n")
DT <- rbindlist(year_chunks, use.names = TRUE, fill = TRUE)
rm(year_chunks); gc(FALSE)

if (!nrow(DT)) stop("No rows ingested from any year. Check input files.")
message(sprintf("[READ] Done. Rows=%s | Years=%s | %s",
                format(nrow(DT), big.mark=","), length(unique(DT$Year)), .mem_str()))
safe_glimpse(DT[sample(.N, min(.N, 5L))], title = "DT random sample")

# ============================= AGGREGATIONS ===================================
cat("\n[DEBUG] Starting aggregations phase...\n")
# 999999 is JUST ANOTHER INDUSTRY; exclude only from totals
not999 <- DT$`Six-Digit NAICS Code` != "999999"

# 1) County totals (exclude 999999)
cat("[DEBUG] Computing county totals (excluding 999999)...\n")
county_totals <- DT[not999,
                    .(`Total Establishments, County` = sum(`NAICS Industry Establishments, County`, na.rm = TRUE),
                      `Total Employment, County`     = sum(`NAICS Industry Employment, County`, na.rm = TRUE)),
                    by = .(Year, `County GEOID`)
]
cat("[DEBUG] Joining county totals back to main data...\n")
key_cty <- paste(county_totals$Year, county_totals$`County GEOID`)
key_all <- paste(DT$Year, DT$`County GEOID`)
ix_cty  <- match(key_all, key_cty)
DT[, `Total Establishments, County` := county_totals$`Total Establishments, County`[ix_cty]]
DT[, `Total Employment, County`     := county_totals$`Total Employment, County`[ix_cty]]
rm(county_totals, key_cty, key_all, ix_cty); gc(FALSE)

# 2) National totals by NAICS (sum of counties) — includes 999999 as its own industry
cat("[DEBUG] Computing national totals by NAICS...\n")
nation_by_naics <- DT[, .(
  `Total NAICS Industry Establishments, Nation` = sum(`NAICS Industry Establishments, County`, na.rm = TRUE),
  `Total NAICS Industry Employment, Nation`     = sum(`NAICS Industry Employment, County`, na.rm = TRUE)
), by = .(Year, `Six-Digit NAICS Code`)]
cat("[DEBUG] Joining national NAICS totals back to main data...\n")
key_nb   <- paste(nation_by_naics$Year, nation_by_naics$`Six-Digit NAICS Code`)
key_all2 <- paste(DT$Year, DT$`Six-Digit NAICS Code`)
ix_nb    <- match(key_all2, key_nb)
DT[, `Total NAICS Industry Establishments, Nation` := nation_by_naics$`Total NAICS Industry Establishments, Nation`[ix_nb]]
DT[, `Total NAICS Industry Employment, Nation`     := nation_by_naics$`Total NAICS Industry Employment, Nation`[ix_nb]]

# 3) National overall totals (exclude 999999)
cat("[DEBUG] Computing overall national totals (excluding 999999)...\n")
nation_all <- DT[DT$`Six-Digit NAICS Code` != "999999",
                 .( `Total Establishments, Nation` = sum(`NAICS Industry Establishments, County`, na.rm = TRUE),
                    `Total Employment, Nation`     = sum(`NAICS Industry Employment, County`, na.rm = TRUE)
                 ),
                 by = .(Year)
]
ix_nall <- match(DT$Year, nation_all$Year)
DT[, `Total Establishments, Nation` := nation_all$`Total Establishments, Nation`[ix_nall]]
DT[, `Total Employment, Nation`     := nation_all$`Total Employment, Nation`[ix_nall]]
rm(nation_by_naics, nation_all, ix_nb, ix_nall, key_nb, key_all2); gc(FALSE)

# 4) Location Quotients (guard against 0)
cat("[DEBUG] Computing location quotients...\n")
DT[, `NAICS Employment Location Quotient` :=
     fifelse(`Total Employment, County` > 0 &
               `Total Employment, Nation` > 0 &
               `Total NAICS Industry Employment, Nation` > 0,
             (`NAICS Industry Employment, County` / `Total Employment, County`) /
               (`Total NAICS Industry Employment, Nation` / `Total Employment, Nation`),
             NA_real_)]

DT[, `NAICS Establishments Location Quotient` :=
     fifelse(`Total Establishments, County` > 0 &
               `Total Establishments, Nation` > 0 &
               `Total NAICS Industry Establishments, Nation` > 0,
             (`NAICS Industry Establishments, County` / `Total Establishments, County`) /
               (`Total NAICS Industry Establishments, Nation` / `Total Establishments, Nation`),
             NA_real_)]

gc(FALSE)
message("[CHECK] Years in output: ", paste(sort(unique(DT$Year)), collapse = ", "))

# ============================= DEBUGGING/SANITY ===============================
cat("\n[DEBUG] Running sanity checks...\n")
na_cty  <- sum(is.na(DT$`County Name`))
na_ind  <- sum(is.na(DT$`Six-Digit NAICS Industry Title`))
message(sprintf("[CHECK] Title NA counts → County Name: %s | NAICS Title: %s",
                format(na_cty, big.mark=","), format(na_ind, big.mark=",")))

tmp_999 <- DT[`Six-Digit NAICS Code` == "999999", .N, by = Year][order(Year)]
if (nrow(tmp_999)) {
  message("[CHECK] 999999 rows by Year: ",
          paste(sprintf("%d:%s", tmp_999$Year, format(tmp_999$N, big.mark=",")), collapse=" | "))
}
# Sample: industry vs totals
if (nrow(DT)) {
  samp <- DT[`Six-Digit NAICS Code` != "999999" & 
               (`NAICS Industry Employment, County` > 0 | `NAICS Industry Establishments, County` > 0)][
                 sample(.N, min(.N, 5L))]
  safe_glimpse(as.data.table(samp[, .(Year, `County GEOID`, `Six-Digit NAICS Code`,
                                      `NAICS Industry Employment, County`,
                                      `Total Employment, County`,
                                      `NAICS Industry Establishments, County`,
                                      `Total Establishments, County`)]),
               title = "SANITY sample: industry vs totals")
}
# No negatives; zeros by year
neg_count <- DT[`Total Employment, County` < 0 | `Total Establishments, County` < 0, .N]
if (neg_count > 0) warning("Negative totals detected: ", neg_count)
zero_totals <- DT[, .(
  zero_emp_cty = sum(`Total Employment, County` == 0, na.rm = TRUE),
  zero_est_cty = sum(`Total Establishments, County` == 0, na.rm = TRUE)
), by = Year][order(Year)]
message("[CHECK] Zero county totals by Year (emp/est): ",
        paste(sprintf("%d:%s/%s", zero_totals$Year,
                      format(zero_totals$zero_emp_cty, big.mark=","),
                      format(zero_totals$zero_est_cty, big.mark=",")),
              collapse=" | "))

# ============================= COLUMN ORDER ===================================
cat("[DEBUG] Setting column order...\n")
front_cols <- c("Year", "County GEOID", "County Name",
                "Six-Digit NAICS Code", "Six-Digit NAICS Industry Title",
                "NAICS Industry Establishments, County", "NAICS Industry Employment, County",
                "Total Establishments, County", "Total Employment, County",
                "Total NAICS Industry Establishments, Nation", "Total NAICS Industry Employment, Nation",
                "Total Establishments, Nation", "Total Employment, Nation",
                "NAICS Employment Location Quotient", "NAICS Establishments Location Quotient")
front_cols <- intersect(front_cols, names(DT))
setcolorder(DT, c(front_cols, setdiff(names(DT), front_cols)))

cat("[DEBUG] Data dimensions after processing: rows=", format(nrow(DT), big.mark=","), 
    " cols=", ncol(DT), " ", .mem_str(), "\n", sep="")

# ============================= BUILD RDS SPLIT TABLES =========================
cat("\n[DEBUG] Building separate tables for RDS export...\n")
INDUSTRY_TITLES <- if (!is.null(NAICS_MAP)) NAICS_MAP[, .(`Six-Digit NAICS Code`, `Six-Digit NAICS Industry Title`)] else
  unique(DT[, .(`Six-Digit NAICS Code`, `Six-Digit NAICS Industry Title`)])
data.table::setkeyv(INDUSTRY_TITLES, c("Six-Digit NAICS Code"))

AREA_TITLES <- if (!is.null(AREA_MAP)) AREA_MAP[, .(`County GEOID`, `County Name`)] else
  unique(DT[, .(`County GEOID`, `County Name`)])
data.table::setkeyv(AREA_TITLES, c("County GEOID"))

YEAR_AREA_INDUSTRY_ESTABLISHMENTS <- DT[, .(
  Year, `County GEOID`, `Six-Digit NAICS Code`,
  `NAICS Industry Establishments, County`,
  `Total Establishments, County`,
  `Total NAICS Industry Establishments, Nation`,
  `Total Establishments, Nation`
)]
YEAR_AREA_INDUSTRY_EMPLOYMENT <- DT[, .(
  Year, `County GEOID`, `Six-Digit NAICS Code`,
  `NAICS Industry Employment, County`,
  `Total Employment, County`,
  `Total NAICS Industry Employment, Nation`,
  `Total Employment, Nation`
)]
YEAR_AREA_INDUSTRY_LQ <- DT[, .(
  Year, `County GEOID`, `Six-Digit NAICS Code`,
  `NAICS Establishments Location Quotient`,
  `NAICS Employment Location Quotient`
)]

# Validate column exclusions
stopifnot(!"County Name" %in% names(YEAR_AREA_INDUSTRY_ESTABLISHMENTS),
          !"Six-Digit NAICS Industry Title" %in% names(YEAR_AREA_INDUSTRY_ESTABLISHMENTS),
          !"County Name" %in% names(YEAR_AREA_INDUSTRY_EMPLOYMENT),
          !"Six-Digit NAICS Industry Title" %in% names(YEAR_AREA_INDUSTRY_EMPLOYMENT),
          !"County Name" %in% names(YEAR_AREA_INDUSTRY_LQ),
          !"Six-Digit NAICS Industry Title" %in% names(YEAR_AREA_INDUSTRY_LQ))

safe_glimpse(INDUSTRY_TITLES,  n = 3, title = "INDUSTRY_TITLES (sample)")
safe_glimpse(AREA_TITLES,       n = 3, title = "AREA_TITLES (sample)")
safe_glimpse(YEAR_AREA_INDUSTRY_ESTABLISHMENTS[sample(.N, min(.N, 5L))], n = 5, title = "ESTABLISHMENTS (sample)")
safe_glimpse(YEAR_AREA_INDUSTRY_EMPLOYMENT[sample(.N, min(.N, 5L))],     n = 5, title = "EMPLOYMENT (sample)")
safe_glimpse(YEAR_AREA_INDUSTRY_LQ[sample(.N, min(.N, 5L))],             n = 5, title = "LQ (sample)")

# ============================= EXPORT PATHS ===================================
CSV_PATH     <- file.path(OUT_DIR, paste0(BASE_NAME, ".csv"))
DUCKDB_PATH  <- file.path(OUT_DIR, paste0(BASE_NAME, ".duckdb"))
PARQUET_PATH <- file.path(OUT_DIR, paste0(BASE_NAME, ".parquet"))
RDS_PATH     <- file.path(OUT_DIR, paste0(BASE_NAME, ".rds"))

cat("\n[DEBUG] Export targets:\n")
cat("  CSV:     ", CSV_PATH, "\n")
cat("  DuckDB:  ", DUCKDB_PATH, "\n")
cat("  Parquet: ", PARQUET_PATH, "\n")
cat("  RDS:     ", RDS_PATH, "\n\n")

# ============================= CSV (DIRECT TO ONEDRIVE) =======================
if (isTRUE(CFG$write_combined_csv)) {
  cat("[EXPORT] Writing combined CSV directly to OneDrive (streamed by year)...\n")
  if (file.exists(CSV_PATH)) { cat("[DEBUG] Removing existing CSV to re-write cleanly...\n"); file.remove(CSV_PATH) }
  yrs <- sort(unique(DT$Year))
  total_rows <- nrow(DT)
  tictoc$start()
  rows_written <- 0L
  for (i in seq_along(yrs)) {
    yy <- yrs[i]
    cat(sprintf("[CSV] Writing Year %d (%d of %d)...\n", yy, i, length(yrs)))
    part <- DT[Year == yy]
    data.table::fwrite(part, CSV_PATH,
                       append    = (i != 1L),
                       col.names = (i == 1L),
                       quote     = TRUE,
                       sep       = ",",
                       eol       = "\n",
                       na        = "",
                       bom       = FALSE,
                       showProgress = TRUE)
    rows_written <- rows_written + nrow(part)
    cat(sprintf("[CSV] Year %d done. Cumulative rows: %s / %s\n",
                yy, format(rows_written, big.mark=","), format(total_rows, big.mark=",")))
    rm(part); if ((i %% 3) == 0) gc(FALSE)
  }
  elapsed_csv <- tictoc$stop("[TIMER] CSV write complete.")
  if (file.exists(CSV_PATH)) {
    fsize <- file.info(CSV_PATH)$size
    cat("[DEBUG] CSV file present. Size:", fmt_bytes(fsize), " | Rows:", format(rows_written, big.mark=","), "\n")
  } else {
    warning("[WARN] CSV not found after write.")
  }
} else {
  cat("[DEBUG] write_combined_csv=FALSE → skipping CSV export.\n")
}


# ============================= RDS (LIST OF TABLES) ===========================
# Save RDS now (after CSV). If the single-file write fails (e.g., OneDrive connection),
# fall back to sharded per-year RDS files and write a manifest at RDS_PATH.
if (isTRUE(CFG$write_rds_split)) {
  cat("\n[EXPORT] Saving RDS (list of separate tables) directly to OneDrive...\n")
  if (file.exists(RDS_PATH)) { cat("[DEBUG] Removing existing RDS to re-write cleanly...\n"); file.remove(RDS_PATH) }
  
  # Free the big combined DT before serializing to make room
  cat("[DEBUG] Freeing combined DT before RDS save to minimize memory pressure...\n")
  rmDT_bytes <- tryCatch(object.size(DT), error = function(e) NA_integer_)
  if (!is.na(rmDT_bytes)) cat("[DEBUG] DT approx size in memory:", fmt_bytes(as.numeric(rmDT_bytes)), "\n")
  rm(DT); gc(TRUE)
  
  RDS_LIST <- list(
    INDUSTRY_TITLES = INDUSTRY_TITLES,
    AREA_TITLES = AREA_TITLES,
    YEAR_AREA_INDUSTRY_ESTABLISHMENTS = YEAR_AREA_INDUSTRY_ESTABLISHMENTS,
    YEAR_AREA_INDUSTRY_EMPLOYMENT = YEAR_AREA_INDUSTRY_EMPLOYMENT,
    YEAR_AREA_INDUSTRY_LQ = YEAR_AREA_INDUSTRY_LQ
  )
  stopifnot(
    all(c("Six-Digit NAICS Code","Six-Digit NAICS Industry Title") %in% names(RDS_LIST$INDUSTRY_TITLES)),
    all(c("County GEOID","County Name") %in% names(RDS_LIST$AREA_TITLES)),
    !("County Name" %in% names(RDS_LIST$YEAR_AREA_INDUSTRY_ESTABLISHMENTS)),
    !("Six-Digit NAICS Industry Title" %in% names(RDS_LIST$YEAR_AREA_INDUSTRY_ESTABLISHMENTS)),
    !("County Name" %in% names(RDS_LIST$YEAR_AREA_INDUSTRY_EMPLOYMENT)),
    !("Six-Digit NAICS Industry Title" %in% names(RDS_LIST$YEAR_AREA_INDUSTRY_EMPLOYMENT)),
    !("County Name" %in% names(RDS_LIST$YEAR_AREA_INDUSTRY_LQ)),
    !("Six-Digit NAICS Industry Title" %in% names(RDS_LIST$YEAR_AREA_INDUSTRY_LQ))
  )
  
  # ---- Attempt 1: write a single compressed RDS (xz, smallest) ----
  save_ok <- FALSE
  last_err <- NULL
  cat("[RDS] Attempting xz-compressed single-file RDS...\n")
  try({
    con_xz <- xzfile(RDS_PATH, open = "wb", compression = 9)
    on.exit(try(close(con_xz), silent = TRUE), add = TRUE)
    tictoc$start()
    saveRDS(RDS_LIST, con_xz)
    tictoc$stop("[TIMER] RDS write complete (xz).")
    save_ok <- TRUE
  }, silent = TRUE)
  
  # ---- Attempt 2: gzip (if xz failed) ----
  if (!save_ok) {
    try(unlink(RDS_PATH), silent = TRUE)
    cat("[RDS] xz failed; attempting gzip-compressed single-file RDS...\n")
    tryCatch({
      con_gz <- gzfile(RDS_PATH, open = "wb", compression = 6)
      on.exit(try(close(con_gz), silent = TRUE), add = TRUE)
      tictoc$start()
      saveRDS(RDS_LIST, con_gz)
      tictoc$stop("[TIMER] RDS write complete (gzip).")
      save_ok <- TRUE
    }, error = function(e) last_err <<- conditionMessage(e))
  }
  
  # ---- Fallback: shard into many smaller RDS files (per year) ----
  if (!save_ok) {
    warning("[WARN] Single-file RDS write failed (", last_err, "). Falling back to sharded per-year RDS files.")
    SHARD_DIR <- file.path(OUT_DIR, "rds_shards")
    dir.create(SHARD_DIR, showWarnings = FALSE, recursive = TRUE)
    
    # Always save the two small lookup tables as separate RDS files
    saveRDS(INDUSTRY_TITLES, file.path(SHARD_DIR, "industry_titles.rds"), compress = TRUE)
    saveRDS(AREA_TITLES,     file.path(SHARD_DIR, "area_titles.rds"),     compress = TRUE)
    
    shard_by_year <- function(dt, subdir, stem) {
      d <- file.path(SHARD_DIR, subdir)
      dir.create(d, showWarnings = FALSE, recursive = TRUE)
      yrs <- sort(unique(dt$Year))
      n <- length(yrs)
      for (i in seq_along(yrs)) {
        yy <- yrs[i]
        fn <- file.path(d, sprintf("%s_%d.rds", stem, yy))
        cat(sprintf("[RDS][Shard] Writing %s (%d/%d): %s\n", subdir, i, n, basename(fn)))
        part <- dt[Year == yy]
        saveRDS(part, fn, compress = TRUE)
        rm(part); if ((i %% 3)==0) gc(FALSE)
      }
      list(dir = basename(d), files = list.files(d, pattern = "\\.rds$", full.names = FALSE))
    }
    
    manifest <- list(
      type = "tapestry_rds_sharded",
      version = 1L,
      base_dir = basename(SHARD_DIR),
      INDUSTRY_TITLES = "industry_titles.rds",
      AREA_TITLES     = "area_titles.rds",
      YEAR_AREA_INDUSTRY_ESTABLISHMENTS = shard_by_year(YEAR_AREA_INDUSTRY_ESTABLISHMENTS, "establishments", "estab"),
      YEAR_AREA_INDUSTRY_EMPLOYMENT     = shard_by_year(YEAR_AREA_INDUSTRY_EMPLOYMENT,     "employment",     "empl"),
      YEAR_AREA_INDUSTRY_LQ             = shard_by_year(YEAR_AREA_INDUSTRY_LQ,             "lq",             "lq")
    )
    saveRDS(manifest, RDS_PATH, compress = TRUE)
    cat("[INFO] Wrote shard manifest at:", RDS_PATH, "\n")
    cat("[INFO] Shards directory:", SHARD_DIR, "\n")
  } else {
    if (file.exists(RDS_PATH)) {
      fsize <- file.info(RDS_PATH)$size
      cat("[DEBUG] RDS file present. Size:", fmt_bytes(fsize), "\n")
    } else {
      warning("[WARN] RDS not found after write (unexpected).")
    }
  }
  
  # Free split tables now (keeps memory low for subsequent steps)
  cat("[DEBUG] Clearing split tables from memory...\n")
  rm(INDUSTRY_TITLES, AREA_TITLES,
     YEAR_AREA_INDUSTRY_ESTABLISHMENTS,
     YEAR_AREA_INDUSTRY_EMPLOYMENT,
     YEAR_AREA_INDUSTRY_LQ)
  gc(TRUE)
} else {
  cat("[DEBUG] write_rds_split=FALSE → skipping RDS export.\n")
}



# ============================= DUCKDB (FROM CSV) ==============================
if (isTRUE(CFG$write_duckdb) || isTRUE(CFG$write_parquet)) {
  cat("\n[EXPORT] Creating DuckDB database directly in OneDrive (source = CSV)...\n")
  if (file.exists(DUCKDB_PATH)) { cat("[DEBUG] Removing existing DuckDB file to re-create...\n"); file.remove(DUCKDB_PATH) }
  con <- DBI::dbConnect(duckdb::duckdb(), DUCKDB_PATH, read_only = FALSE)
  on.exit({ try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE) }, add = TRUE)
  
  # Use near-full cores
  n_threads <- max(1L, parallel::detectCores() - 1L)
  DBI::dbExecute(con, sprintf("PRAGMA threads=%d;", n_threads))
  cat("[DEBUG][DuckDB] PRAGMA threads set to:", n_threads, "\n")
  
  # Build table from CSV (streamed by DuckDB)
  DBI::dbExecute(con, "DROP TABLE IF EXISTS tapestry_employment;")
  csv_sql_path <- gsub("\\\\","/", CSV_PATH)
  cat("[DEBUG][DuckDB] Loading CSV into table 'tapestry_employment' ...\n")
  tictoc$start()
  # Auto-detect types; keep header; large sample for safer typing
  DBI::dbExecute(con, sprintf(
    "CREATE TABLE tapestry_employment AS SELECT * FROM read_csv_auto('%s', HEADER=TRUE, SAMPLE_SIZE=200000);",
    sql_quote(csv_sql_path)
  ))
  elapsed_load <- tictoc$stop("[TIMER][DuckDB] CSV -> table load complete.")
  rc <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM tapestry_employment")$n[1]
  cat("[VERIFY][DuckDB] Row count in 'tapestry_employment':", format(rc, big.mark=","), "\n")
  
  if (isTRUE(CFG$write_duckdb)) {
    # Touch the DB file to ensure it's durable on disk (already created by connect + table)
    db_bytes <- if (file.exists(DUCKDB_PATH)) file.info(DUCKDB_PATH)$size else NA_real_
    cat("[DEBUG] DuckDB file:", DUCKDB_PATH, " Size:", fmt_bytes(db_bytes), "\n")
  }
  
  # ========================== PARQUET (FROM DUCKDB) ===========================
  if (isTRUE(CFG$write_parquet)) {
    cat("\n[EXPORT] Streaming Parquet write FROM DuckDB (single file, uncompressed)...\n")
    if (file.exists(PARQUET_PATH)) { cat("[DEBUG] Removing existing Parquet to re-write cleanly...\n"); file.remove(PARQUET_PATH) }
    pq_sql_path <- gsub("\\\\","/", PARQUET_PATH)
    tictoc$start()
    # Single-file Parquet export (no partitioning)
    DBI::dbExecute(con, sprintf(
      "COPY (SELECT * FROM tapestry_employment) TO '%s' (FORMAT PARQUET, COMPRESSION 'UNCOMPRESSED');",
      sql_quote(pq_sql_path)
    ))
    elapsed_pq <- tictoc$stop("[TIMER][DuckDB] Parquet COPY complete.")
    if (file.exists(PARQUET_PATH)) {
      fsize <- file.info(PARQUET_PATH)$size
      cat("[DEBUG] Parquet file present. Size:", fmt_bytes(fsize), "\n")
      # Verify by counting via DuckDB's reader (avoids Arrow memory)
      vcount <- DBI::dbGetQuery(con, sprintf(
        "SELECT COUNT(*) AS n FROM read_parquet('%s');", sql_quote(pq_sql_path)
      ))$n[1]
      cat("[VERIFY][Parquet] Row count via DuckDB read_parquet():", format(vcount, big.mark=","), "\n")
    } else {
      warning("[WARN] Parquet not found after COPY.")
    }
  } else {
    cat("[DEBUG] write_parquet=FALSE → skipping Parquet export.\n")
  }
} else {
  cat("[DEBUG] write_duckdb=FALSE & write_parquet=FALSE → skipping DuckDB/Parquet steps.\n")
}

# ============================= VERIFICATION SUITE =============================
cat("\n[DEBUG] Starting verification suite...\n")

# A) CSV head check
if (file.exists(CSV_PATH)) {
  cat("[VERIFY][CSV] Reading first few rows...\n")
  try({
    csv_head <- readr::read_csv(CSV_PATH, n_max = 5, show_col_types = FALSE, progress = FALSE)
    print(csv_head)
  }, silent = TRUE)
} else {
  cat("[VERIFY][CSV] Skipped (file not found).\n")
}

# B) DuckDB counts and quick stats (if DB exists)
if (file.exists(DUCKDB_PATH)) {
  cat("[VERIFY][DuckDB] Sampling few rows and years...\n")
  try({
    con2 <- DBI::dbConnect(duckdb::duckdb(), DUCKDB_PATH, read_only = TRUE)
    on.exit({ try(DBI::dbDisconnect(con2, shutdown = TRUE), silent = TRUE) }, add = TRUE)
    print(DBI::dbGetQuery(con2, "SELECT MIN(Year) AS min_year, MAX(Year) AS max_year, COUNT(*) AS n FROM tapestry_employment"))
    print(DBI::dbGetQuery(con2, "SELECT Year, COUNT(*) AS n FROM tapestry_employment GROUP BY Year ORDER BY Year LIMIT 5"))
  }, silent = TRUE)
} else {
  cat("[VERIFY][DuckDB] Skipped (file not found).\n")
}

# C) Parquet quick verify with Arrow (optional)
if (have_arrow && file.exists(PARQUET_PATH)) {
  cat("[VERIFY][Arrow] Loading small sample from Parquet via 'arrow'...\n")
  try({
    tbl <- arrow::read_parquet(PARQUET_PATH, as_data_frame = TRUE, col_select = c("Year","County GEOID","Six-Digit NAICS Code"))[1:5, ]
    print(tbl)
  }, silent = TRUE)
} else {
  cat("[VERIFY][Arrow] Skipped (arrow not available or file not found).\n")
}

# ============================= FINAL CLEANUP ==================================
cat("\n[DEBUG] Final memory state:", .mem_str(), "\n")
cat("[DEBUG] Output files saved to:", OUT_DIR, "\n")
message("\n===============================================")
message("All done successfully! ", .mem_str())
message("Output directory: ", gsub("\\\\","/", OUT_DIR))
message("Base filename: ", BASE_NAME)
message("Wrote: ",
        paste(c(
          if (CFG$write_combined_csv) "CSV",
          if (CFG$write_duckdb)       "DuckDB",
          if (CFG$write_parquet)      "Parquet",
          if (CFG$write_rds_split)    "RDS (split tables)"
        ), collapse = " + "))
message("===============================================\n")
