# ACRE Pipeline — Q2 2025 (prep through geo_long + ALL raw loads + GEO coverage checks with names)
# Updated: 2025-09-28 | Full, start-to-finish script (nothing omitted) | Extra debugging throughout
# Coverage checks report MISSING geographies *with names* wherever possible

suppressPackageStartupMessages({
  library(tidyverse); library(sf); library(dplyr); library(tidyr); library(stringr); library(purrr); library(tibble)
  library(rlang); library(readr); library(readxl); library(jsonlite); library(httr); library(tigris); library(censusapi); library(tidycensus)
}); options(stringsAsFactors=FALSE, scipen=999, timeout=max(300,getOption("timeout",60))); options(tigris_use_cache=TRUE); sf::sf_use_s2(TRUE)

# Utilities / Safety patches
`%||%` <- function(a,b) if(!is.null(a)) a else b
fix_bad_names <- function(df){ if(is.null(df)) return(df); nm <- names(df); if(is.null(nm)) return(df); bad <- is.na(nm)|nm==""; if(any(bad)){ nm[bad] <- paste0("X", seq_along(nm))[bad]; names(df) <- nm }; df }
trim_names <- function(df){ if(is.null(df)||is.null(names(df))) return(df); names(df) <- gsub("\\s+$","", gsub("^\\s+","", names(df))); df }
if (exists("fix_df", envir=globalenv()) && bindingIsLocked("fix_df", globalenv())) unlockBinding("fix_df", globalenv())
fix_df <- function(df){
  if (is.null(df)) return(df); if (inherits(df, c("sf","sfc"))) return(df)
  if (inherits(df,"tbl_df")) df <- as.data.frame(df, stringsAsFactors=FALSE)
  nm <- names(df); if(!is.null(nm)){ nm <- gsub("\\s+$","", gsub("^\\s+","", nm)); bad <- is.na(nm)|nm==""; if(any(bad)) nm[bad] <- paste0("X", which(bad)); names(df) <- nm }
  tibble::as_tibble(df, .name_repair="unique")
}; try(lockBinding("fix_df", globalenv()), silent=TRUE)

# Debug helpers
dbg <- function(x,name, head_n=6){
  cat("\n==== ",name," ====\n",sep=""); if(is.null(x)){cat("<<not loaded>>\n"); return(invisible(NULL))}
  cat("Rows:", tryCatch(nrow(x), error=function(e) NA_integer_), "  Cols:", tryCatch(ncol(x), error=function(e) NA_integer_), "\n")
  suppressWarnings(try(dplyr::glimpse(x), silent=TRUE))
  pr <- tryCatch(readr::problems(x), error=function(e) NULL); if(!is.null(pr) && nrow(pr)>0){cat("\n---- parsing problems (top 10) ----\n"); print(utils::head(pr,10))}
  nas <- tryCatch(sapply(x, function(v) sum(is.na(v))), error=function(e) NULL); if(!is.null(nas)){ nas <- nas[nas>0]; if(length(nas)){cat("\n---- NA counts (top 20) ----\n"); print(utils::head(sort(nas,decreasing=TRUE),20))} }
  invisible(x)
}
nz_chr <- function(x) ifelse(is.na(x)|!nzchar(x), NA_character_, x)
# New (NA-safe)
nf <- function(x) { x <- suppressWarnings(as.character(x)); ifelse(is.na(x), NA_character_, stringr::str_pad(x, 5, pad="0")) }
ns <- function(x) { x <- suppressWarnings(as.character(x)); ifelse(is.na(x), NA_character_, stringr::str_pad(x, 2, pad="0")) }
inspect_csv_issues <- function(path, expected_cols, skip=0, n_show=10){
  cat("\n== Inspect:", basename(path), "==\n"); suppressWarnings(print(tryCatch(readr::guess_encoding(path, n_max=5000), error=function(e) tibble(note="failed to guess encoding"))))
  cf <- tryCatch(utils::count.fields(path, sep=",", quote="\""), error=function(e) integer(0)); if(length(cf)==0){ cat("Could not count fields; file unreadable?\n"); return(invisible(NULL)) }
  bad <- which(cf != expected_cols); cat("\nTotal lines:", length(cf), "\nExpected cols:", expected_cols, "\nBad lines:", length(bad), "\n")
  if(length(bad)){ show_idx <- utils::tail(bad, n_show); rl <- tryCatch(readr::read_lines(path), error=function(e) character(0)); cat("\n-- Last", length(show_idx), "problem lines --\n"); for(i in show_idx){ line <- if(i<=length(rl)) rl[i] else "<out of range>"; cat(sprintf("[%d:%d] %s\n", i, cf[i], line)) } }
}
read_csv_trim_block <- function(path, expected_cols=3, col_types=cols(.default=col_character())){
  cf <- utils::count.fields(path, sep=",", quote="\""); keep <- which(cf==expected_cols); if(!length(keep)) stop("No rows with expected column count in: ", path)
  rng <- range(keep); readr::read_csv(I(readr::read_lines(path)[rng[1]:rng[2]]), col_types=col_types, show_col_types=FALSE) %>% fix_df()
}
assert_unique_by <- function(df, keys, name=deparse(substitute(df))) {
  d <- df %>% count(across(all_of(keys)), name="n") %>% filter(n>1)
  if (nrow(d)) stop(sprintf("[%s] has %d duplicate keys by (%s). Example:\n%s", name, nrow(d), paste(keys, collapse=", "), paste(utils::capture.output(print(head(d, 10))), collapse="\n")))
  invisible(df)
}
make_unique_by <- function(df, keys, summarise_fn=dplyr::first){ df %>% group_by(across(all_of(keys))) %>% summarise(across(everything(), summarise_fn), .groups="drop") }

# Paths
find_acre_data_dir <- function(){
  env_dir <- nz_chr(Sys.getenv("RMI_DATA_DIR")); if(!is.na(env_dir)&&dir.exists(env_dir)) return(normalizePath(env_dir, winslash="/", mustWork=FALSE))
  cand <- unique(na.omit(c(nz_chr(Sys.getenv("OneDriveCommercial")), nz_chr(Sys.getenv("OneDrive")), nz_chr(Sys.getenv("ONEDRIVE")),
                           path.expand("~/Library/CloudStorage/OneDrive-RMI"), file.path(path.expand("~"), "OneDrive - RMI"), file.path(path.expand("~"), "OneDrive"), path.expand("~"))))
  anchors <- c(file.path("US Program - Documents","6_Projects","Clean Regional Economic Development","ACRE","Data"), "Data")
  for(r in cand) for(a in anchors){ p <- file.path(r,a); if(dir.exists(p) && (dir.exists(file.path(p,"Raw Data"))||dir.exists(file.path(p,"US Maps etc")))) return(normalizePath(p, winslash="/", mustWork=FALSE)) }
  NA_character_
}
DATA_DIR <- find_acre_data_dir(); if(is.na(DATA_DIR)){ warning("Set RMI_DATA_DIR to .../ACRE/Data; using getwd().", immediate.=TRUE); DATA_DIR <- normalizePath(getwd(), winslash="/", mustWork=FALSE) }
paths <- list(data_root=DATA_DIR, raw_data=file.path(DATA_DIR,"Raw Data"), us_maps=file.path(DATA_DIR,"US Maps etc"), states_data=file.path(DATA_DIR,"States Data"),
              downloads_dir=file.path(path.expand("~"),"Downloads"), chatgpt_map_app=file.path(DATA_DIR,"ChatGPT","map_app"),
              cgp_raw=file.path(DATA_DIR,"Clean-growth-project","raw"), ct_fips_data=file.path(DATA_DIR,"Raw Data","CT_FIPS_CHANGES"))
invisible(lapply(paths, function(p) if(!dir.exists(p)) dir.create(p, recursive=TRUE, showWarnings=FALSE)))

ct_fips_changes_raw <-readr::read_csv(file.path(paths$ct_fips_data,"geocorr_county_to_county_CT.csv"), skip = 1); glimpse(ct_fips_changes_raw)

# API keys
# --- Census API key bootstrapper (interactive-safe) ---------------------------
ensure_census_key <- function() {
  readRenviron("~/.Renviron")
  key <- Sys.getenv("CENSUS_KEY"); if (!nzchar(key)) key <- Sys.getenv("CENSUS_API_KEY")
  if (!nzchar(key)) {
    if (!interactive()) stop("Census API key not set and session is non-interactive. Please run interactively once to save your key.", call.=FALSE)
    message("\nA Census API key is required. You can request one at https://api.census.gov/data/key_signup.html")
    tmp <- readline("Enter your Census API key: "); key <- nzchar(tmp) |> ifelse(tmp, "")
    if (!nzchar(key)) stop("No key entered.", call.=FALSE)
  }
  is_valid <- function(k) {
    url <- "https://api.census.gov/data/2022/acs/acs1"
    resp <- httr::GET(url, query = list(get = "NAME,B01001_001E", `for`="us:1", key = k), timeout(15))
    if (httr::http_error(resp)) return(FALSE)
    txt <- httr::content(resp, as="text", encoding="UTF-8")
    !grepl("(?i)error|invalid|not authorized|forbidden", txt)
  }
  if (!is_valid(key)) stop("That key didn’t validate with the Census API. Double-check and try again.", call.=FALSE)
  save_key <- function(k) {
    renv <- path.expand("~/.Renviron")
    old <- if (file.exists(renv)) readLines(renv, warn = FALSE) else character()
    keep <- !grepl("^\\s*(CENSUS_KEY|CENSUS_API_KEY)\\s*=", old)
    new <- c(old[keep], sprintf("CENSUS_KEY=%s", k), sprintf("CENSUS_API_KEY=%s", k))
    if (file.exists(renv) && !file.exists(paste0(renv, ".bak"))) file.copy(renv, paste0(renv, ".bak"), overwrite = FALSE)
    writeLines(new, renv, useBytes = TRUE); invisible(TRUE)
  }
  save_key(key); Sys.setenv(CENSUS_KEY = key, CENSUS_API_KEY = key)
  suppressWarnings({
    if ("tidycensus" %in% .packages(all.available = TRUE)) try(tidycensus::census_api_key(key, install = FALSE), silent = TRUE)
    if ("censusapi"  %in% .packages(all.available = TRUE)) try(censusapi::census_api_key(key,  install = FALSE), silent = TRUE)
  })
  message("Census API key is set and saved to ~/.Renviron. Future runs will use it automatically.")
  invisible(key)
}

# ---- Core file paths you set previously ----
PATH_COUNTY_GDP <- file.path(paths$raw_data,"county_gdp_2022.csv"); PATH_MSA_GDP <- file.path(paths$raw_data,"msa_gdp_2022.csv")
PATH_STATES_SIMPLE <- file.path(paths$us_maps,"Regions","rmi_regions.csv"); PATH_CBSA_COUNTY <- file.path(paths$us_maps,"Regions","csa_cbsa_county.csv")
PATH_CD119_COUNTY <- file.path(paths$raw_data,"cd_119_counties.csv"); PATH_FCC_PEA_XLSX <- file.path(paths$raw_data,"FCC_PEA_website.xlsx")

# ---- Read local GDP CSVs (with header/footer trim) ----
county_gdp_csv <- read_csv_trim_block(PATH_COUNTY_GDP,3,cols(GeoFips=col_character(),GeoName=col_character(),"2022"=col_character())) %>%
  filter(nchar(GeoFips)==5) %>% fix_df(); dbg(county_gdp_csv,"county_gdp_csv (raw)")
msa_gdp <- read_csv_trim_block(PATH_MSA_GDP,3,cols(GeoFips=col_character(),GeoName=col_character(),"2022"=col_double())) %>%
  filter(nchar(GeoFips)==5|GeoFips=="00998") %>% fix_df(); dbg(msa_gdp,"msa_gdp (raw)")

states_simple <- readr::read_csv(PATH_STATES_SIMPLE, show_col_types=FALSE) %>% fix_df()
if ("...1" %in% names(states_simple)) states_simple <- select(states_simple, -"...1")
states_simple <- states_simple %>%
  mutate(fips_int=suppressWarnings(as.integer(fips)), statefp_chr=ns(fips_int), abbr=as.character(abbr), full=as.character(full)) %>%
  fix_df(); dbg(states_simple,"states_simple (clean)")

# Rebuild county_cbsa cleanly
county_cbsa <- readr::read_csv(PATH_CBSA_COUNTY, skip=2, show_col_types=FALSE) %>% fix_df() %>%
  filter(!is.na(`FIPS State Code`), !is.na(`FIPS County Code`)) %>%
  mutate(`FIPS State Code`=stringr::str_pad(as.character(`FIPS State Code`),2,pad="0"),
         `FIPS County Code`=stringr::str_pad(as.character(`FIPS County Code`),3,pad="0"),
         fips=paste0(`FIPS State Code`,`FIPS County Code`)) %>%
  select(fips, `CBSA Title`, `CBSA Code`) %>% distinct() %>% fix_df(); dbg(county_cbsa,"county_cbsa (raw)")
dup_cbsa <- county_cbsa %>% count(fips) %>% filter(n>1)
if (nrow(dup_cbsa)) warning("county_cbsa has duplicate fips rows; check source before joining.")

cd_119_county <- readr::read_csv(PATH_CD119_COUNTY, show_col_types=FALSE) %>% fix_df(); dbg(cd_119_county,"cd_119_county (raw)")

# 2023 American Communities Typology (for 2020 counties)
tmp_x <- tempfile(fileext=".xlsx")
invisible(httr::RETRY("GET","https://www.americancommunities.org/wp-content/uploads/2023/08/2023-Typology-1.xlsx", write_disk(tmp_x, overwrite=TRUE), times=3))
us_communities <- readxl::read_excel(tmp_x, 1) %>% fix_df(); dbg(us_communities,"us_communities (raw)")

# FCC PEA workbook (county list + PEA names)
pea_county_raw <- readxl::read_excel(PATH_FCC_PEA_XLSX, 3) %>% fix_df()
pea_raw        <- readxl::read_excel(PATH_FCC_PEA_XLSX, 2) %>% fix_df()
dbg(pea_county_raw,"pea_county (raw)"); dbg(pea_raw,"pea (raw)")

# ---- Prep to GEO master (2020 county base) ----
county_2020 <- us_communities %>% as.data.frame() %>%
  rename(GeoName="County name", fips="Fips", Community="2023 Typology") %>%
  mutate(GEOID=nf(substr(GEO_ID,10,14)), fips=nf(fips)) %>%
  select(GeoName, GEOID, fips, Community) %>% fix_df(); dbg(county_2020,"county_2020")

cd_119 <- cd_119_county %>% mutate(GEOID=nf(GEOID), STATEFP_chr=ns(STATEFP)) %>%
  left_join(select(states_simple, statefp_chr, abbr, full), by=c("STATEFP_chr"="statefp_chr")) %>%
  mutate(cd_119=paste0(abbr,"-",sprintf("%02d", as.numeric(CD119FP)))) %>%
  select(cd_119, GEOID_2, GEOID, percent_district) %>% fix_df(); dbg(cd_119,"cd_119")

# ===== BEA County GDP (2022) + special AK/HI/VA crosswalks =====
tmp_zip <- tempfile(fileext=".zip"); tmp_dir <- tempdir()
download.file("https://apps.bea.gov/regional/zip/CAGDP2.zip", tmp_zip, mode="wb", quiet=TRUE); unzip(tmp_zip, exdir=tmp_dir)
cagdp2_path <- list.files(tmp_dir, pattern="^CAGDP2__ALL_AREAS_\\d{4}_\\d{4}\\.csv$", full.names=TRUE)[1]
CAGDP2_wide <- suppressWarnings(readr::read_csv(cagdp2_path, col_types=cols(.default=col_character()), show_col_types=FALSE)) %>%
  filter(nchar(GeoFIPS)==5) %>% fix_df()
yrs <- grep("^\\d{4}$", names(CAGDP2_wide), value=TRUE); na_tokens <- c("(D)","(L)","(S)","(NA)","(N/A)","(X)")
CAGDP2_long <- CAGDP2_wide %>% pivot_longer(all_of(yrs), names_to="Year", values_to="Value_raw") %>%
  mutate(Year=as.integer(Year), Value=readr::parse_number(Value_raw, na=na_tokens)) %>% select(-Value_raw) %>% fix_df()
CAGDP2_LONG_2022_ALLINDUSTRY_GDP <- CAGDP2_long %>%
  filter(Description=="All industry total", Year==2022) %>% filter(substr(GeoFIPS,3,5)!="000")

# BEA county/city crosswalk for CAGDP2 special geos (population weighted with ACS 2022)
bea_va_xwalk <- tibble::tribble(
  ~GeoFIPS, ~target_fips, ~label,
  "51901","51003","Albemarle + Charlottesville","51901","51540","Albemarle + Charlottesville",
  "51903","51005","Alleghany + Covington","51903","51580","Alleghany + Covington",
  "51907","51015","Augusta + Staunton + Waynesboro","51907","51790","Augusta + Staunton + Waynesboro","51907","51820","Augusta + Staunton + Waynesboro",
  "51911","51031","Campbell + Lynchburg","51911","51680","Campbell + Lynchburg",
  "51913","51035","Carroll + Galax","51913","51640","Carroll + Galax",
  "51918","51053","Dinwiddie + Colonial Heights + Petersburg","51918","51570","Dinwiddie + Colonial Heights + Petersburg","51918","51730","Dinwiddie + Colonial Heights + Petersburg",
  "51919","51059","Fairfax + Fairfax City + Falls Church","51919","51600","Fairfax + Fairfax City + Falls Church","51919","51610","Fairfax + Fairfax City + Falls Church",
  "51921","51069","Frederick + Winchester","51921","51840","Frederick + Winchester",
  "51923","51081","Greensville + Emporia","51923","51595","Greensville + Emporia",
  "51929","51089","Henry + Martinsville","51929","51690","Henry + Martinsville",
  "51931","51095","James City + Williamsburg","51931","51830","James City + Williamsburg",
  "51933","51121","Montgomery + Radford","51933","51750","Montgomery + Radford",
  "51939","51143","Pittsylvania + Danville","51939","51590","Pittsylvania + Danville",
  "51941","51149","Prince George + Hopewell","51941","51670","Prince George + Hopewell",
  "51942","51153","Prince William + Manassas + Manassas Park","51942","51683","Prince William + Manassas + Manassas Park","51942","51685","Prince William + Manassas + Manassas Park",
  "51944","51161","Roanoke + Salem","51944","51775","Roanoke + Salem",
  "51945","51163","Rockbridge + Buena Vista + Lexington","51945","51530","Rockbridge + Buena Vista + Lexington","51945","51678","Rockbridge + Buena Vista + Lexington",
  "51947","51165","Rockingham + Harrisonburg","51947","51660","Rockingham + Harrisonburg",
  "51949","51175","Southampton + Franklin","51949","51620","Southampton + Franklin",
  "51951","51177","Spotsylvania + Fredericksburg","51951","51630","Spotsylvania + Fredericksburg",
  "51953","51191","Washington + Bristol","51953","51520","Washington + Bristol",
  "51955","51195","Wise + Norton","51955","51720","Wise + Norton",
  "51958","51199","York + Poquoson","51958","51735","York + Poquoson"
) %>% select(GeoFIPS, target_fips)
bea_hi_xwalk <- tibble::tribble(~GeoFIPS, ~target_fips, "15901","15009","15901","15005")
bea_ak_xwalk <- tibble::tribble(~GeoFIPS, ~target_fips, "02261","02063","02261","02066","02232","02105","02232","02230","02280","02275","02280","02195","02201","02198","02201","02130")
bea_cagdp2_xwalk <- bind_rows(bea_va_xwalk, bea_hi_xwalk, bea_ak_xwalk) %>% mutate(GeoFIPS=nf(GeoFIPS), target_fips=nf(target_fips)) %>% fix_df()

# Pull ACS 2022 5-year population at county level for weights
acs_pop_2022 <- censusapi::getCensus(name="acs/acs5", vintage=2022, vars=c("B01003_001E"), region="county:*", regionin="state:*", key=Sys.getenv("CENSUS_KEY")) %>%
  transmute(fips=nf(paste0(state, county)), pop_2022=suppressWarnings(as.numeric(B01003_001E))) %>% fix_df()

# Build population weights for BEA combined geos
xw_pop <- bea_cagdp2_xwalk %>% left_join(acs_pop_2022, by=c("target_fips"="fips")) %>% group_by(GeoFIPS) %>%
  mutate(group_pop=sum(pop_2022, na.rm=TRUE), weight_pop=dplyr::if_else(is.finite(group_pop)&group_pop>0, pop_2022/group_pop, NA_real_)) %>% ungroup()
xw_weights <- xw_pop %>% group_by(GeoFIPS) %>% mutate(n_members=dplyr::n(), eq_wt=1/n_members, weight=dplyr::coalesce(weight_pop, eq_wt)) %>%
  ungroup() %>% select(GeoFIPS, target_fips, weight)

# Expand & allocate CAGDP2 GDP (All industry, 2022) to actual counties
county_gdp_expanded <- CAGDP2_LONG_2022_ALLINDUSTRY_GDP %>% select(GeoFIPS, Value) %>% left_join(xw_weights, by="GeoFIPS") %>%
  mutate(target_fips=dplyr::coalesce(target_fips, nf(GeoFIPS)), weight=dplyr::coalesce(weight, 1.0)) %>%
  group_by(target_fips) %>% summarise(gdp_2022=sum(Value*weight, na.rm=TRUE), .groups="drop") %>% rename(fips=target_fips) %>% fix_df()

# Prefer BEA-expanded GDP; use CSV as final backstop
county_gdp_clean <- county_gdp_expanded %>%
  full_join(county_gdp_csv %>% transmute(fips=nf(GeoFips), gdp_2022_csv=readr::parse_number(`2022`)), by="fips") %>%
  mutate(gdp_2022=coalesce(gdp_2022, gdp_2022_csv)) %>% select(fips, gdp_2022) %>% fix_df() %>%
  right_join(county_2020 %>% select(fips), by="fips") %>% arrange(fips) %>% fix_df()
assert_unique_by(county_gdp_clean, "fips")

# MSA GDP clean
msa_gdp_clean <- msa_gdp %>% transmute(cbsa_code=as.character(GeoFips), msa_gdp_2022=`2022`) %>% fix_df(); dbg(msa_gdp_clean,"msa_gdp_clean")

# State total GDP (not used below but preserved)
CAGDP2_LONG_STATE <- CAGDP2_long %>% filter(Description=="All industry total") %>% filter(substr(GeoFIPS,3,5)=="000" & GeoFIPS!="00000")

# ===== TIGRIS baselayers =====
tigris_counties_2024_raw <- tigris::counties(cb=FALSE, year=2024, class="sf"); dbg(tigris_counties_2024_raw,"tigris_counties_2024_raw")
tigris_congressional_districts_2024_raw <- tigris::congressional_districts(cb=FALSE, year=2024, class="sf"); dbg(tigris_congressional_districts_2024_raw,"tigris_congressional_districts_2024_raw")
tigris_cbsa_2024_raw <- tigris::core_based_statistical_areas(cb=FALSE, year=2024, class="sf"); dbg(tigris_cbsa_2024_raw,"tigris_cbsa_2024_raw")
tigris_counties_2020_raw <- tigris::counties(cb=FALSE, year=2020, class="sf"); dbg(tigris_counties_2020_raw,"tigris_counties_2020_raw")
tigris_cbsa_2020_raw <- tigris::core_based_statistical_areas(cb=FALSE, year=2020, class="sf"); dbg(tigris_cbsa_2020_raw,"tigris_cbsa_2020_raw")
tigris_counties_2018_raw <- tigris::counties(cb=FALSE, year=2018, class="sf"); dbg(tigris_counties_2018_raw,"tigris_counties_2018_raw")
tigris_cbsa_2018_raw <- tigris::core_based_statistical_areas(cb=FALSE, year=2018, class="sf"); dbg(tigris_cbsa_2018_raw,"tigris_cbsa_2018_raw")
tigris_states_2024_raw <- tigris::states(year=2024, cb=FALSE, class="sf"); dbg(tigris_states_2024_raw,"tigris_states_2024_raw")
tigris_regions_2020_raw <- tigris::regions(year=2020, class="sf"); dbg(tigris_regions_2020_raw,"tigris_regions_2020_raw")
tigris_divisions_2020_raw <- tigris::divisions(year=2020, class="sf"); dbg(tigris_divisions_2020_raw,"tigris_divisions_2020_raw")

# ===== County crosswalk builder (unchanged) =====
build_county_crosswalk <- function(counties_sf, states_year=2024, cbsa_year=2020, acs_year=2022){
  states_x <- states(year=states_year, cb=FALSE, class="sf") |> st_drop_geometry() |> filter(!(STATEFP %in% c("60","66","69","72","78"))) |>
    transmute(STATE_FIPS=STATEFP, STATE_NAME=NAME, STATE_ABBREVIATION=STUSPS, CENSUS_REGION_GEOID=REGION, CENSUS_DIVISION_GEOID=DIVISION) |>
    left_join(regions(year=2020, class="sf") |> st_drop_geometry() |> transmute(CENSUS_REGION_GEOID=GEOID, CENSUS_REGION_NAME=NAMELSAD), by="CENSUS_REGION_GEOID") |>
    left_join(divisions(year=2020, class="sf") |> st_drop_geometry() |> transmute(CENSUS_DIVISION_GEOID=GEOID, CENSUS_DIVISION_NAME=NAMELSAD), by="CENSUS_DIVISION_GEOID")
  cbsa_x <- core_based_statistical_areas(cb=FALSE, year=cbsa_year, class="sf") |> st_drop_geometry() |> transmute(CBSA_GEOID=GEOID, CBSA_NAME=NAMELSAD)
  pop_x <- get_acs("county","B01003_001", year=acs_year, survey="acs5", geometry=FALSE) |> transmute(COUNTY_GEOID=GEOID, POP_2022=estimate) |>
    full_join(get_decennial("county","P1_001N", year=2020, geometry=FALSE) |> transmute(COUNTY_GEOID=GEOID, POP_2020=value), by="COUNTY_GEOID") |>
    mutate(POP_FINAL=coalesce(POP_2022, POP_2020)) |> select(COUNTY_GEOID, `2022_COUNTY_POPULATION`=POP_FINAL)
  counties_sf |> st_drop_geometry() |> filter(!(STATEFP %in% c("60","66","69","72","78"))) |>
    transmute(STATE_FIPS=STATEFP, COUNTY_GEOID=GEOID, COUNTY_NAME=NAMELSAD, CBSA_GEOID=CBSAFP, IN_CBSA=!is.na(CBSAFP)) |>
    left_join(cbsa_x, by="CBSA_GEOID") |> left_join(states_x, by="STATE_FIPS") |> left_join(pop_x, by="COUNTY_GEOID") |>
    select(CENSUS_REGION_GEOID, CENSUS_REGION_NAME, CENSUS_DIVISION_GEOID, CENSUS_DIVISION_NAME,
           STATE_FIPS, STATE_NAME, STATE_ABBREVIATION, COUNTY_GEOID, COUNTY_NAME, IN_CBSA, CBSA_GEOID, CBSA_NAME,
           `2022_COUNTY_POPULATION`) |> distinct()
}

# ======= FIPS change shim for legacy county codes (PEA)  =======
# Handles: 46113->46102 (SD), 02270->02158 (AK), 02261 split->{02063,02066} (AK), 51515->51019 (VA)
fips_change_table <- tibble::tribble(
  ~old_fips, ~new_fips, ~type,   ~note,
  "46113",  "46102",   "rename","Shannon County SD -> Oglala Lakota County SD (2015)",
  "02270",  "02158",   "rename","Wade Hampton AK -> Kusilvak AK (2015 FIPS update)",
  "51515",  "51019",   "merge", "Bedford city VA dissolved 2013 -> Bedford County VA",
  "02261",  "02063",   "split", "Valdez-Cordova AK -> Chugach AK (2019)",
  "02261",  "02066",   "split", "Valdez-Cordova AK -> Copper River AK (2019)"
)

normalize_pea_fips <- function(pea_df, tigris_2020){
  req <- c("FCC_PEA_Number","FIPS","CountyName","State"); miss <- setdiff(req, names(pea_df))
  if(length(miss)) stop("pea_df missing columns: ", paste(miss, collapse=", "))
  # normalize FIPS to 5-char
  pea0 <- pea_df %>% mutate(FIPS = nf(FIPS))
  # Apply non-split renames/merges first
  ren_tbl <- fips_change_table %>% filter(type != "split") %>% distinct(old_fips, new_fips, type, note)
  pea1 <- pea0 %>%
    left_join(ren_tbl %>% select(old_fips, new_fips), by=c("FIPS"="old_fips")) %>%
    mutate(FIPS_fixed = coalesce(new_fips, FIPS),
           fix_applied = case_when(is.na(new_fips) ~ "none", TRUE ~ "renamed_or_merged")) %>%
    select(-new_fips)
  # Split 02261 -> two rows
  split_tbl <- fips_change_table %>% filter(type == "split") %>% distinct(old_fips, new_fips)
  to_split <- pea1 %>% filter(FIPS_fixed %in% split_tbl$old_fips)
  split_rows <- if(nrow(to_split)){
    to_split %>%
      inner_join(split_tbl, by=c("FIPS_fixed"="old_fips")) %>%
      mutate(FIPS_fixed = new_fips, fix_applied = "split") %>%
      select(-new_fips)
  } else tibble()
  pea2 <- pea1 %>%
    filter(!(FIPS_fixed %in% split_tbl$old_fips)) %>% # drop originals that were split
    bind_rows(split_rows) %>%
    # De-duplicate in case the "new" FIPS already appears in source (keep 1)
    arrange(FIPS_fixed, FCC_PEA_Number) %>%
    distinct(FIPS_fixed, .keep_all = TRUE)
  # Debug coverage before/after
  bad_before <- pea0 %>% filter(!FIPS %in% tigris_2020$GEOID) %>% distinct(FIPS, CountyName, State, FCC_PEA_Number)
  bad_after  <- pea2 %>% filter(!FIPS_fixed %in% tigris_2020$GEOID) %>% distinct(FIPS_fixed, CountyName, State, FCC_PEA_Number)
  cat("\n---- PEA FIPS normalization report ----\n")
  cat("Rows: before =", nrow(pea0), " after =", nrow(pea2), " delta =", nrow(pea2)-nrow(pea0), "\n")
  cat("Unmatched to TIGRIS 2020 before fix:", nrow(bad_before), "\n"); if(nrow(bad_before)) print(bad_before)
  cat("Unmatched to TIGRIS 2020 after  fix:", nrow(bad_after),  "\n"); if(nrow(bad_after))  print(bad_after)
  # Return normalized frame with FIPS_fixed
  pea2 %>% rename(FIPS_norm = FIPS_fixed)
}

# ======= Build crosswalk and add GDP =======
COUNTY_CROSSWALK_SUPPLEMENT <- tigris_counties_2020_raw %>% build_county_crosswalk(); dbg(COUNTY_CROSSWALK_SUPPLEMENT,"COUNTY_CROSSWALK_SUPPLEMENT")

COUNTY_CROSSWALK_SUPPLEMENT_GDP <- COUNTY_CROSSWALK_SUPPLEMENT %>%
  left_join(county_gdp_clean, by=c("COUNTY_GEOID"="fips")) %>%
  rename(COUNTY_GDP_2022=gdp_2022) %>% fix_df()
dbg(COUNTY_CROSSWALK_SUPPLEMENT_GDP,"COUNTY_CROSSWALK_SUPPLEMENT_GDP")

# ======= Apply FIPS normalization to PEA, then join =======
pea_county_norm <- normalize_pea_fips(pea_county_raw, tigris_counties_2020_raw)
# quick sanity: the four legacy codes should be gone now
stopifnot(!any(c("02261","02270","46113","51515") %in% pea_county_norm$FIPS_norm))

pea <- pea_county_norm %>%
  mutate(fips = nf(FIPS_norm)) %>%
  left_join(pea_raw, by="FCC_PEA_Number") %>%
  select(fips, FCC_PEA_Name, FCC_PEA_Number) %>%
  arrange(fips, FCC_PEA_Number) %>%
  distinct(fips, .keep_all = TRUE) %>%  # each county should map to exactly one PEA
  fix_df()
# diagnostics: ensure uniqueness and flag duplicates if any
dup_pea <- pea %>% count(fips) %>% filter(n>1)
if(nrow(dup_pea)) {
  cat("\n[WARN] Duplicate PEA assignments for fips:\n"); print(dup_pea %>% left_join(pea, by="fips"))
}
assert_unique_by(pea, "fips", "pea")

# Join PEA to the county crosswalk
COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA <- COUNTY_CROSSWALK_SUPPLEMENT_GDP %>%
  left_join(pea %>% rename(COUNTY_GEOID=fips, PEA_NAME=FCC_PEA_Name, PEA_NUMBER=FCC_PEA_Number), by="COUNTY_GEOID") %>%
  fix_df()
dbg(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA,"COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA")

# Coverage check: any counties still lacking PEA?
county_2020_no_matching_PEA <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
  filter(is.na(PEA_NAME) | is.na(PEA_NUMBER))
cat("\nCounties with NO PEA after FIPS normalization: ", nrow(county_2020_no_matching_PEA), "\n")
if(nrow(county_2020_no_matching_PEA)) print(county_2020_no_matching_PEA %>% select(COUNTY_GEOID, COUNTY_NAME, STATE_NAME) %>% arrange(COUNTY_GEOID))

# ======= GeoCorr 2020 county -> 119th CD (with CT special file) =======
geocorr_county_2020_cd_119 <- readr::read_csv(file.path(paths$raw_data,"geocorr_county_2020_cd_119.csv"), skip=1, show_col_types=FALSE)
geocorr_county_2020_cd_119 <- geocorr_county_2020_cd_119 %>%
  mutate(State_code=stringr::str_pad(as.character(`State code`),2,pad="0"),
         CD119_code=stringr::str_pad(as.character(`Congressional district code (119th Congress)`),2,pad="0"),
         CD119_GEOID=paste0(State_code, CD119_code),
         COUNTY_GEOID=stringr::str_pad(as.character(`County code`),5,pad="0"))

missing_counties <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
  filter(!(COUNTY_GEOID %in% geocorr_county_2020_cd_119$COUNTY_GEOID)) %>% select(COUNTY_GEOID, COUNTY_NAME)
if(nrow(missing_counties)>0){ cat("The following COUNTY_GEOID values are missing in geocorr_county_2020_cd_119:\n"); print(missing_counties) } else { cat("All COUNTY_GEOID values present in geocorr_county_2020_cd_119.\n") }

geocorr_ct_county_cd_119 <- readr::read_csv(file.path(paths$raw_data,"geocorr_ct_county_cd_119.csv"), skip=1, show_col_types=FALSE)

compute_cd119_population_weighted_gdp <- function(county_gdp_df, xwalk_df, ct_xwalk_df, gdp_col="COUNTY_GDP_2022", pop_col="2022_COUNTY_POPULATION"){
  suppressPackageStartupMessages({require(dplyr, quietly=TRUE); require(stringr, quietly=TRUE); require(tidyr, quietly=TRUE); require(rlang, quietly=TRUE)})
  gdp_q <- rlang::ensym(gdp_col); pop_q <- rlang::ensym(pop_col)
  req_cols_county <- c("COUNTY_GEOID","STATE_FIPS","STATE_ABBREVIATION","STATE_NAME", as_string(gdp_q), as_string(pop_q))
  missing_county <- setdiff(req_cols_county, names(county_gdp_df)); if(length(missing_county)) stop("county_gdp_df is missing required columns: ", paste(missing_county, collapse=", "))
  xwalk_df_std <- xwalk_df %>% mutate(State_code=str_pad(as.character(`State code`),2,pad="0"),
                                      CD119_code=str_pad(as.character(`Congressional district code (119th Congress)`),2,pad="0"),
                                      CD119_GEOID=paste0(State_code, CD119_code),
                                      COUNTY_GEOID=str_pad(as.character(`County code`),5,pad="0")) %>%
    rename(alloc_factor=`cd119-to-county allocation factor`) %>% select(COUNTY_GEOID, CD119_GEOID, alloc_factor)
  ct_fips <- "09"; ct_counties_expected <- county_gdp_df %>% filter(STATE_FIPS==ct_fips) %>% distinct(COUNTY_GEOID) %>% pull()
  present_in_nat <- xwalk_df_std %>% distinct(COUNTY_GEOID) %>% pull()
  ct_missing <- setdiff(ct_counties_expected, present_in_nat); if(length(ct_missing)>0 && length(ct_counties_expected)>0) message("Handling ", length(ct_missing), " CT county GEOIDs via ct_xwalk_df: ", paste(ct_missing, collapse=", "))
  county_nonct <- county_gdp_df %>% filter(STATE_FIPS!=ct_fips)
  alloc_nonct <- county_nonct %>% inner_join(xwalk_df_std, by="COUNTY_GEOID") %>%
    mutate(allocated_gdp=(!!gdp_q)*alloc_factor, allocated_pop=(!!pop_q)*alloc_factor) %>%
    select(COUNTY_GEOID, STATE_FIPS, STATE_ABBREVIATION, STATE_NAME, CD119_GEOID, allocated_gdp, allocated_pop)
  ct_xwalk_std <- ct_xwalk_df %>% mutate(COUNTY_GEOID=str_pad(as.character(`County code`),5,pad="0"),
                                         CD119_code=str_pad(as.character(`Congressional district code (119th Congress)`),2,pad="0"),
                                         State_code=ct_fips, CD119_GEOID=paste0(State_code, CD119_code)) %>%
    rename(alloc_factor=`cd119-to-CTcounty allocation factor`) %>% select(COUNTY_GEOID, CD119_GEOID, alloc_factor)
  county_ct <- county_gdp_df %>% filter(STATE_FIPS==ct_fips)
  alloc_ct <- county_ct %>% inner_join(ct_xwalk_std, by="COUNTY_GEOID") %>%
    mutate(allocated_gdp=(!!gdp_q)*alloc_factor, allocated_pop=(!!pop_q)*alloc_factor) %>%
    select(COUNTY_GEOID, STATE_FIPS, STATE_ABBREVIATION, STATE_NAME, CD119_GEOID, allocated_gdp, allocated_pop)
  alloc_all <- bind_rows(alloc_nonct, alloc_ct)
  cd_meta <- alloc_all %>% mutate(STATE_CODE=substr(CD119_GEOID,1,2), DISTRICT_CODE=substr(CD119_GEOID,3,4)) %>%
    mutate(DISTRICT_LABEL=if_else(DISTRICT_CODE=="00","AL",DISTRICT_CODE)) %>%
    group_by(CD119_GEOID, STATE_CODE, DISTRICT_CODE, DISTRICT_LABEL) %>%
    summarise(total_gdp_2022=sum(allocated_gdp, na.rm=TRUE), total_pop_alloc=sum(allocated_pop, na.rm=TRUE),
              STATE_ABBREVIATION=dplyr::first(STATE_ABBREVIATION), STATE_NAME=dplyr::first(STATE_NAME), .groups="drop") %>%
    mutate(CD119_LABEL=paste0(STATE_ABBREVIATION,"-",DISTRICT_LABEL), gdp_per_capita=if_else(total_pop_alloc>0, total_gdp_2022/total_pop_alloc, NA_real_)) %>%
    select(CD119_GEOID, STATE_CODE, STATE_ABBREVIATION, STATE_NAME, DISTRICT_CODE, CD119_LABEL, total_gdp_2022, total_pop_alloc, gdp_per_capita) %>%
    arrange(STATE_ABBREVIATION, DISTRICT_CODE)
  attr(cd_meta,"notes") <- paste0("Population-weighted GDP allocated from counties to 119th districts using Geocorr factors. ",
                                  "CT handled via special crosswalk. GDP column: ", as_string(gdp_q), "; Pop column: ", as_string(pop_q), ".")
  cd_meta
}
gdp_cd119 <- compute_cd119_population_weighted_gdp(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA, geocorr_county_2020_cd_119, geocorr_ct_county_cd_119)

# ---------- Rollups by geography (with County + PEA, no Region/Division) ----------
cc <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA

# County GDP rows
county_gdp_rows <- cc %>%
  transmute(
    geo_type                = "county",
    geo_code                = COUNTY_GEOID,
    geo_name                = COUNTY_NAME,
    geo_gdp_2022_estimated  = COUNTY_GDP_2022
  )

# PEA GDP rows
pea_gdp_rows <- cc %>%
  filter(!is.na(PEA_NUMBER)) %>%
  group_by(PEA_NUMBER, PEA_NAME) %>%
  summarise(val = sum(COUNTY_GDP_2022, na.rm = TRUE), .groups = "drop") %>%
  transmute(
    geo_type                = "pea",
    geo_code                = as.character(PEA_NUMBER),
    geo_name                = PEA_NAME,
    geo_gdp_2022_estimated  = val
  )

# State GDP rows
state_gdp <- cc %>%
  group_by(STATE_FIPS, STATE_NAME) %>%
  summarise(val = sum(COUNTY_GDP_2022, na.rm = TRUE), .groups = "drop") %>%
  transmute(
    geo_type                = "state",
    geo_code                = STATE_FIPS,
    geo_name                = STATE_NAME,
    geo_gdp_2022_estimated  = val
  )

# CBSA GDP rows
cbsa_gdp <- cc %>%
  filter(!is.na(CBSA_GEOID), IN_CBSA) %>%
  group_by(CBSA_GEOID, CBSA_NAME) %>%
  summarise(val = sum(COUNTY_GDP_2022, na.rm = TRUE), .groups = "drop") %>%
  transmute(
    geo_type                = "cbsa",
    geo_code                = CBSA_GEOID,
    geo_name                = CBSA_NAME,
    geo_gdp_2022_estimated  = val
  )

# Congressional District GDP (population-weighted)
cd119_gdp <- gdp_cd119 %>%
  transmute(
    geo_type                = "cd119",
    geo_code                = CD119_GEOID,
    geo_name                = CD119_LABEL,
    geo_gdp_2022_estimated  = total_gdp_2022
  )

# Combine all (no region/division)
GEO_LONG_GDP <- bind_rows(
  state_gdp,
  county_gdp_rows,
  cbsa_gdp,
  pea_gdp_rows,
  cd119_gdp
) %>%
  mutate(
    geo_type_standardized = dplyr::recode(
      geo_type,
      "state"   = "State",
      "county"  = "County",
      "cbsa"    = "Metro Area",
      "pea"     = "Economic Area",
      "cd119"   = "Congressional District",
      .default  = NA_character_
    )
  ) %>%
  arrange(geo_type, geo_code) %>%
  fix_df()
# Sanity: unique per (geo_type, geo_code)
assert_unique_by(GEO_LONG_GDP, c("geo_type","geo_code"))
cat("\nGEO_LONG_GDP rows:", nrow(GEO_LONG_GDP), "\n")

# ======= GEO master + long =======
census_divisions <- tibble(State.Name=state.name, state_abbr=state.abb, Region=as.character(state.region), Division=as.character(state.division)) %>%
  add_row(State.Name="District of Columbia", state_abbr="DC", Region="South", Division="South Atlantic") %>% fix_df()

geo <- county_2020 %>% mutate(state.fips=as.integer(substr(fips,1,2))) %>%
  select(state.fips, GeoName, fips, Community) %>%
  left_join(select(states_simple, fips_int, abbr, full), by=c("state.fips"="fips_int")) %>% rename(State.Name=full, state_abbr=abbr) %>%
  left_join(census_divisions, by=c("State.Name","state_abbr")) %>%
  select(Region, Division, State.Name, state_abbr, state.fips, GeoName, fips, Community) %>%
  left_join(county_cbsa, by="fips") %>%
  left_join(pea %>% rename(PEA=FCC_PEA_Name), by="fips") %>%
  left_join(cd_119, by=c("fips"="GEOID")) %>%
  left_join(county_gdp_clean, by="fips") %>%
  rename(gdp=gdp_2022) %>%
  left_join(msa_gdp_clean, by=c("CBSA Code"="cbsa_code")) %>%
  fix_df(); dbg(geo,"geo (master)")

# All GDP present?
missing_gdp_geo <- geo %>% filter(is.na(gdp)) %>% select(GeoName, fips) %>% arrange(GeoName) %>% distinct(GeoName, fips)
cat("\nCounties with missing GDP: ", nrow(missing_gdp_geo), "\n")
if(nrow(missing_gdp_geo)) print(missing_gdp_geo, n=Inf)

# Alaska CD fill rule (unchanged)
geo <- geo %>% mutate(cd_119=ifelse(is.na(cd_119) & substr(fips,1,2)=="02", "AK-00", cd_119)) %>% fix_df()

# Build flavors
geo_states   <- geo %>% select(geo_name=State.Name, geo_code=state.fips) %>% distinct() %>% mutate(geo="State", geo_code=as.character(geo_code)) %>% fix_df(); dbg(geo_states,"geo_states")
geo_counties <- geo %>% select(geo_name=GeoName,  geo_code=fips) %>% distinct() %>% mutate(geo="County") %>% fix_df(); dbg(geo_counties,"geo_counties")
geo_cd       <- geo %>% select(geo_name=cd_119,   geo_code=GEOID_2) %>% distinct() %>% mutate(geo="Congressional District", geo_code=as.character(geo_code)) %>% fix_df(); dbg(geo_cd,"geo_cd")
geo_pea      <- geo %>% select(geo_name=PEA,      geo_code=FCC_PEA_Number) %>% distinct() %>% mutate(geo="Economic Area", geo_code=as.character(geo_code)) %>% fix_df(); dbg(geo_pea,"geo_pea")
geo_metro    <- geo %>% filter(!is.na(`CBSA Code`), !is.na(`CBSA Title`)) %>%
  distinct(geo_name=`CBSA Title`, geo_code=`CBSA Code`) %>% mutate(geo="Metro Area") %>% fix_df(); dbg(geo_metro,"geo_metro")

geo_long <- bind_rows(
  geo %>% transmute(geo_type="State",  geo_name=State.Name, geo_code=as.character(state.fips)),
  geo %>% transmute(geo_type="County", geo_name=GeoName,    geo_code=fips),
  geo %>% transmute(geo_type="Congressional District", geo_name=cd_119, geo_code=as.character(GEOID_2)),
  geo %>% transmute(geo_type="Economic Area", geo_name=PEA, geo_code=as.character(FCC_PEA_Number)),
  geo %>% filter(!is.na(`CBSA Code`), !is.na(`CBSA Title`)) %>% transmute(geo_type="Metro Area", geo_name=`CBSA Title`, geo_code=`CBSA Code`)
) %>%
  filter(!is.na(geo_name), !is.na(geo_code)) %>%
  distinct(geo_type, geo_name, geo_code) %>%
  fix_df(); dbg(geo_long,"geo_long (master)")

# Final PEA coverage re-check (explicitly naming the formerly-problematic geos)
formerly_problematic <- c("02158","46102","02066","02063")
cat("\nFormerly-problematic counties PEA assignment:\n")
print(
  COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
    filter(COUNTY_GEOID %in% formerly_problematic) %>%
    select(COUNTY_GEOID, COUNTY_NAME, STATE_NAME, PEA_NUMBER, PEA_NAME) %>% arrange(COUNTY_GEOID),
  n=Inf
)



# Full text fixer: quotes/control → re-encode (twice) → simple fixes → squish
fix_text <- function(x) {
  if (is.null(x)) return(x)
  x <- as.character(x)
  x <- strip_outer_quotes(x)
  x <- strip_control_bytes(x)
  x <- fix_reencode_once(x)
  x <- fix_reencode_once(x)
  x <- fix_mojibake_simple(x)
  x <- norm_name(x)
  x
}


soft_assert <- function(ok, msg) { if (!ok) message("⚠️ ", msg); invisible(ok) }
to_num      <- function(x) suppressWarnings(as.numeric(x))
norm_name   <- function(x) stringr::str_squish(x)
territory_state_fips <- c("60","66","69","72","78")  # AS, GU, MP, PR, VI

# Strip plain ASCII quotes added by some CSV/logging paths
strip_outer_quotes <- function(x) stringr::str_replace_all(x, '^"+|"+$', '')

# Remove C0/C1 control chars (keeps \t, \n if desired; here we drop all)
strip_control_bytes <- function(x) stringr::str_replace_all(x, "[\\x00-\\x1F\\x7F-\\x9F]", "")

# Classic single-pass mojibake fixes seen in Puerto Rico + misc.
fix_mojibake_simple <- function(x) {
  repl <- c(
    "Do√±a"      = "Doña",
    "Pe√±uelas"  = "Peñuelas",
    "A√±asco"    = "Añasco",
    "Mayag√ºez"  = "Mayagüez",
    "Rinc√ón"    = "Rincón",
    "Cata√±o"    = "Cataño",
    "Lo√≠za"     = "Loíza",
    "Manat√í"    = "Manatí",
    "R√≠o"       = "Río",
    "Germ√án"    = "Germán",
    # Common non‑PR offenders we’re seeing
    "Ca√±on"     = "Cañon",
    "Espa√±ola"  = "Española"
  )
  stringr::str_replace_all(x, repl)
}

# Attempt to repair "UTF‑8 seen as Latin‑1/Windows‑1252" by round‑tripping
fix_reencode_once <- function(x) {
  y1 <- suppressWarnings(iconv(iconv(x, from = "UTF-8", to = "latin1"),        from = "latin1",        to = "UTF-8"))
  y2 <- suppressWarnings(iconv(iconv(x, from = "UTF-8", to = "windows-1252"),  from = "windows-1252",  to = "UTF-8"))
  bad_ct <- function(s) stringr::str_count(s, "[ÃÂâ√]")
  choose <- ifelse(bad_ct(y2) < bad_ct(y1), y2, y1)
  choose[is.na(choose)] <- x[is.na(choose)]
  choose
}


# Targeted text diagnostics
debug_text_issues <- function(df, label, col = "geo_name", n = 10) {
  v <- df[[col]]
  pat_bad <- "[ÃÂâ√]|\\\\u00"
  idx_bad <- which(stringr::str_detect(v, pat_bad))
  if (length(idx_bad) > 0) {
    cat("\n--- Text diagnostics:", label, "---\n")
    cat("Rows with suspected mojibake:", length(idx_bad), "\n")
    print(df[idx_bad, , drop=FALSE] %>% dplyr::select(any_of(col)) %>% head(n), n=Inf)
  }
}

# --- Robust column-detection helpers ------------------------------------------
norm_cols <- function(nms) gsub("[^a-z0-9]+", "", tolower(nms))

find_col <- function(df, candidates = character(), keywords = character(), label = "column", required = TRUE) {
  nms      <- names(df)
  nms_norm <- norm_cols(nms)
  
  # 1) exact match on normalized candidates
  for (cand in candidates) {
    c_norm <- norm_cols(cand)
    hit <- which(nms_norm == c_norm)
    if (length(hit) == 1) {
      cat("•", label, "→", nms[hit], "(exact)\n")
      return(nms[hit])
    }
  }
  # 2) partial match on candidates
  for (cand in candidates) {
    c_norm <- norm_cols(cand)
    hit <- which(stringr::str_detect(nms_norm, fixed(c_norm)))
    if (length(hit) == 1) {
      cat("•", label, "→", nms[hit], "(partial)\n")
      return(nms[hit])
    }
  }
  # 3) all-keyword containment (e.g., c("planning","region"))
  if (length(keywords)) {
    kw <- norm_cols(keywords)
    ok <- sapply(nms_norm, function(x) all(stringr::str_detect(x, kw)))
    if (sum(ok) == 1) {
      hit <- which(ok)
      cat("•", label, "→", nms[hit], "(keywords)\n")
      return(nms[hit])
    }
    # choose best score if multiple
    if (sum(ok) > 1) {
      scores <- sapply(nms_norm[ok], function(x) sum(stringr::str_detect(x, kw)))
      hit <- which(ok)[which.max(scores)]
      cat("•", label, "→", nms[hit], "(keywords-best)\n")
      return(nms[hit])
    }
  }
  # 4) failure path
  if (required) {
    cat("\n⚠️ Could not find", label, ". Available columns:\n")
    print(nms)
    stop(paste("Required", label, "not found. See column list above."))
  } else {
    cat("•", label, "not found (optional).\n")
    return(NA_character_)
  }
}


# --- 0) Lookups (STATE, COUNTY, CBSA, PEA, CD119) -----------------------------
# States (2024)
lkp_state <- tigris_states_2024_raw %>%
  transmute(state_fips = STATEFP,
            state_abbr = STUSPS,
            State.Name = fix_text(NAME))

# Counties (2020 geog, drop geometry)
lkp_county <- tigris_counties_2020_raw %>%
  sf::st_drop_geometry() %>%
  transmute(
    county_geoid = GEOID,
    county_name  = fix_text(NAMELSAD),
    state_fips   = substr(GEOID, 1, 2)
  )

# CBSA + PEA (county base 2020; titles will be swapped to tigris NAME)
lkp_cbsa_pea <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
  transmute(county_geoid = COUNTY_GEOID,
            CBSA.Code   = CBSA_GEOID,
            CBSA.Title  = fix_text(CBSA_NAME),
            PEA         = fix_text(PEA_NAME),
            PEA_NO      = PEA_NUMBER)

# Authoritative CBSA names from tigris
lkp_cbsa_names <- tigris_cbsa_2020_raw %>%
  sf::st_drop_geometry() %>%
  transmute(CBSA.Code = GEOID, CBSA.Name = fix_text(NAME))

# Early CBSA name QA (should catch Cañon/Española here)
debug_text_issues(lkp_cbsa_names, "lkp_cbsa_names (raw fixed)", "CBSA.Name", n=10)

# County→CD119 factors (national GeoCorr)
lkp_cd119_nat <- geocorr_county_2020_cd_119 %>%
  transmute(
    county_geoid = COUNTY_GEOID,
    CD119_code   = CD119_code,
    state_fips   = State_code,
    co_to_cd119  = `county-to-cd119 allocation factor`
  ) %>%
  mutate(cd_119 = paste0(lkp_state$state_abbr[match(state_fips, lkp_state$state_fips)],
                         "-", str_pad(CD119_code, 2, pad = "0")))

# Connecticut-specific county (legacy) → CD119 bridge
cat("\n--- Detecting column names in geocorr_ct_county_cd_119 ---\n")
gc_ct_names <- names(geocorr_ct_county_cd_119); print(gc_ct_names)

gc_ct_col_county   <- find_col(geocorr_ct_county_cd_119,
                               candidates = c("County code","County Code","County_code","County"),
                               keywords   = c("county","code"),
                               label = "CT county code")
gc_ct_col_cd       <- find_col(geocorr_ct_county_cd_119,
                               candidates = c("Congressional district code (119th Congress)",
                                              "CD119_code","cd119","CongressionalDistrictCode119"),
                               keywords   = c("congressional","119"),
                               label = "CD119 code")
gc_ct_col_alloc    <- find_col(geocorr_ct_county_cd_119,
                               candidates = c("CTcounty-to-cd119 allocation factor",
                                              "ctcounty to cd119 allocation factor"),
                               keywords   = c("allocation","cd119"),
                               label = "CTcounty→CD119 allocation factor")

lkp_cd119_ct <- geocorr_ct_county_cd_119 %>%
  transmute(
    county_geoid = .data[[gc_ct_col_county]],
    CD119_code   = .data[[gc_ct_col_cd]],
    co_to_cd119  = to_num(.data[[gc_ct_col_alloc]]),
    cd_119       = paste0("CT-", str_pad(CD119_code, 2, pad = "0"))
  )

# Prefer CT-specific rows over national
lkp_cd119 <- bind_rows(
  lkp_cd119_nat %>% filter(!startsWith(county_geoid, "09")),
  lkp_cd119_ct
) %>%
  filter(!is.na(co_to_cd119), co_to_cd119 > 0)


# --- 3) CT PR (2023) → legacy counties (2020) reallocation --------------------
cat("\n--- Detecting column names in ct_fips_changes_raw ---\n")
ct_names <- names(ct_fips_changes_raw); print(ct_names)

ct_col_county   <- find_col(ct_fips_changes_raw,
                            candidates = c("County code","County Code","County_code","County"),
                            keywords   = c("county","code"),
                            label = "Legacy County code (2020)")
ct_col_pr       <- find_col(ct_fips_changes_raw,
                            candidates = c("Connecticut planning region","Planning region code","Planning region"),
                            keywords   = c("planning","region"),
                            label = "CT Planning Region code (2023)")
ct_col_pr2ct    <- find_col(ct_fips_changes_raw,
                            candidates = c("county-to-CTcounty allocation factor","county to ctcounty allocation factor"),
                            keywords   = c("county","ctcounty","allocation"),
                            label = "County→CTcounty allocation factor")
ct_col_ct2pr    <- find_col(ct_fips_changes_raw,
                            candidates = c("CTcounty-to-county allocation factor","ctcounty to county allocation factor"),
                            keywords   = c("ctcounty","county","allocation"),
                            label = "CTcounty→County allocation factor",
                            required = FALSE)

# Build & normalize CT weights (defensive)
ct_alloc_raw <- ct_fips_changes_raw %>%
  transmute(
    ctcounty_geoid = stringr::str_pad(as.character(.data[[ct_col_county]]), 5, pad = "0"),
    pr_geoid       = stringr::str_pad(as.character(.data[[ct_col_pr]]),     5, pad = "0"),
    w_pr_to_ct     = to_num(.data[[ct_col_pr2ct]]),             # PR→County
    w_ct_to_pr     = if (!is.na(ct_col_ct2pr)) to_num(.data[[ct_col_ct2pr]]) else NA_real_  # County→PR (optional)
  )

# Show sample of CT alloc table for sanity
cat("\n--- ct_alloc_raw sample ---\n"); head(ct_alloc_raw, 10)

ct_map <- ct_alloc_raw %>%
  mutate(
    w_ct_to_pr = tidyr::replace_na(w_ct_to_pr, 0),
    w_pr_to_ct = tidyr::replace_na(w_pr_to_ct, 0)
  ) %>%
  group_by(pr_geoid) %>%
  mutate(w_pr_to_ct = if (sum(w_pr_to_ct) > 0) w_pr_to_ct / sum(w_pr_to_ct) else w_pr_to_ct) %>%
  ungroup() %>%
  group_by(ctcounty_geoid) %>%
  mutate(w_ct_to_pr = if (sum(w_ct_to_pr) > 0) w_ct_to_pr / sum(w_ct_to_pr) else w_ct_to_pr) %>%
  ungroup()

qa_pr <- ct_map %>% group_by(pr_geoid)       %>% summarise(s = sum(w_pr_to_ct), .groups="drop")
qa_ct <- ct_map %>% group_by(ctcounty_geoid) %>% summarise(s = sum(w_ct_to_pr), .groups="drop")
soft_assert(all(abs(qa_pr$s - 1) < 1e-6), "CT PR→County weights do not sum to 1 (after normalization).")
soft_assert(all(abs(qa_ct$s - 1) < 1e-6), "CT County→PR weights do not sum to 1 (after normalization).")

# Define geographies
geographies <- c("State.Name", "cd_119", "PEA", "CBSA.Title", "GeoName")

# Normalize all geography names (dots -> spaces)
geographies_clean <- gsub("\\.", " ", geographies)

cim_dir <- file.path(paths$raw_data,"clean_investment_monitor_q2_2025")

apply_fix_df <- function(df) {
  if (exists("fix_df", mode = "function")) tryCatch(fix_df(df), error = function(e) df) else df
}


dbg_head <- function(df, nm = deparse(substitute(df)), n = 5) {
  if (DEBUG_LEVEL < 2L) return(invisible(df))
  message("\n==== ", nm, " ====")
  message("class: ", paste(class(df), collapse = ", "))
  message("rows x cols: ", nrow(df), " x ", ncol(df))
  print(utils::head(df, n))
  invisible(df)
}

norm_fips <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x <- ifelse(is.na(x) | x == "", NA_character_, x)
  x <- stringr::str_replace_all(x, "[^0-9]", "")   # keep digits only
  x <- stringr::str_pad(x, 5, pad = "0")
  # If after cleaning the string isn't 5 digits, set NA (caught later)
  x[!grepl("^[0-9]{5}$", x)] <- NA_character_
  x
}

validate_fips <- function(v, nm, sample_n = MAX_SHOW) {
  bad_idx <- which(is.na(v))
  if (length(bad_idx) > 0) {
    .warnf("%s: %d invalid/NA FIPS after normalization. Showing up to %d examples.",
           nm, length(bad_idx), sample_n)
    .debug(paste("Bad FIPS sample:", paste(utils::head(bad_idx, sample_n), collapse = ", ")))
  }
}

check_required_cols <- function(df, nm, required) {
  missing <- setdiff(required, names(df))
  if (length(missing) > 0) .failf("Missing required columns in %s: %s", nm, paste(missing, collapse = ", "))
}

check_dups <- function(df, key, nm, show = MAX_SHOW) {
  dups <- df %>% count(.data[[key]], name = "n") %>% filter(n > 1)
  if (nrow(dups) > 0) {
    .warnf("Duplicates in %s on key %s: %d (showing %d)", nm, key, nrow(dups), min(nrow(dups), show))
    print(utils::head(dups, show))
  } else {
    .debug("No duplicates in %s on key %s", nm, key)
  }
  invisible(dups)
}

na_report <- function(df, cols, nm, show = MAX_SHOW) {
  out <- purrr::map_dfr(cols, ~tibble(
    column = .x,
    na_cnt = sum(is.na(df[[.x]])),
    non_na = sum(!is.na(df[[.x]]))
  ))
  .note("NA report for %s:\n%s", nm, paste(capture.output(print(out)), collapse = "\n"))
  invisible(out)
}

`%||%` <- function(x, y) if (!is.null(x)) x else y
wavg <- function(x, w) {
  w <- as.numeric(w); x <- as.numeric(x)
  if (all(is.na(x)) || all(is.na(w)) || sum(w[!is.na(w)], na.rm = TRUE) <= 0) return(NA_real_)
  stats::weighted.mean(x, w = w, na.rm = TRUE)
}
sum_safe <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  s <- sum(x, na.rm = TRUE)
  if (is.na(s)) NA_real_ else s
}

apply_fix_df <- function(df) {
  if (exists("fix_df", mode = "function")) tryCatch(fix_df(df), error = function(e) df) else df
}

dedup_county <- function(df, nm, value_col, weight_col) {
  d0 <- nrow(df)
  dups <- check_dups(df, "county_geoid", nm)
  if (nrow(dups) > 0) {
    .warnf("%s: aggregating duplicates to county level (weighted by %s)", nm, weight_col)
    df <- df %>%
      group_by(county_geoid) %>%
      summarise(
        across(all_of(value_col), ~ wavg(.x, .data[[weight_col]])),
        across(all_of(weight_col), sum_safe),
        .groups = "drop"
      )
  }
  d1 <- nrow(df)
  if (d1 < d0) .note("%s: rows %d -> %d after deduplication.", nm, d0, d1)
  df
}

pct_sanitize <- function(p, nm = "percent") {
  p <- as.numeric(p)
  bad <- which(!is.na(p) & (p < 0 | p > 100))
  if (length(bad) > 0) .warnf("%s: %d out-of-range values detected; clamping to [0,100].", nm, length(bad))
  p <- pmin(pmax(p, 0), 100)
  p
}

================================================================================
# END setup.R
================================================================================


================================================================================
# BEGIN Facilities.R
================================================================================


# Clean Investment Monitor (raw CSV drops)

facilities <- suppressWarnings(read.csv(
  file.path(cim_dir,"manufacturing_energy_and_industry_facility_metadata.csv"),
  skip=5, check.names=FALSE)) %>% fix_df(); dbg(facilities,"facilities_raw")
print(unique(facilities$Current_Facility_Status)); print(unique(facilities$Investment_Status))

cat("\n================ FACILITIES PIPELINE (START) ================\n")

# Helpers
safe_date <- function(x){
  suppressWarnings(as.Date(x, tryFormats=c("%m/%d/%y","%m/%d/%Y","%Y-%m-%d")))
}
norm_status <- function(x){
  x0 <- trimws(as.character(x)); xU <- toupper(x0)
  dplyr::case_when(
    xU %in% c("C","CANCELED","CANCELLED") | grepl("CANCEL", xU, fixed=TRUE) ~ "C",
    xU %in% c("R","RETIRED")                                                ~ "R",
    xU %in% c("O","OPERATING")                                              ~ "Operating",
    xU %in% c("A","ANNOUNCED")                                              ~ "Announced",
    xU %in% c("U","U/C","UNDER CONSTRUCTION")                               ~ "Under Construction",
    TRUE                                                                    ~ x0
  )
}
nz_num <- function(x) dplyr::coalesce(suppressWarnings(as.numeric(x)), 0)
dbg_count_na <- function(df, cols, label){
  miss <- sapply(cols, function(cn) if(cn %in% names(df)) sum(is.na(df[[cn]])) else NA_integer_)
  cat("\n-- NA counts:", label, "--\n"); print(stats::setNames(as.integer(miss), cols)); invisible(NULL)
}

# --- Stage 1: Spatial geocoding ---
cat("\n--- Stage 1: Spatial geocoding ---\n")
cat("\nUnique Current_Facility_Status (raw):\n"); print(unique(facilities$Current_Facility_Status))
cat("\nUnique Investment_Status (raw):\n");       print(unique(facilities$Investment_Status))

facilities_sf <- facilities %>%
  dplyr::filter(is.finite(Latitude), is.finite(Longitude)) %>%
  sf::st_as_sf(coords=c("Longitude","Latitude"), crs=4326, remove=FALSE)
cat("facilities_sf rows:", nrow(facilities_sf), "\n")

counties_4326 <- tigris_counties_2020_raw %>%
  sf::st_transform(4326) %>% dplyr::select(GEOID, STATEFP, NAMELSAD, CBSAFP)
cd_4326 <- tigris_congressional_districts_2024_raw %>%
  sf::st_transform(4326) %>% dplyr::select(CD_GEOID = GEOID, CD_NAME = NAMELSAD)

fac_geo <- facilities_sf %>%
  sf::st_join(counties_4326, join=sf::st_within) %>%
  sf::st_join(cd_4326,      join=sf::st_within) %>%
  sf::st_drop_geometry() %>%
  dplyr::mutate(
    fips      = GEOID,
    fips      = dplyr::coalesce(fips, if("county_2020_geoid" %in% names(.))
      stringr::str_pad(as.character(county_2020_geoid),5,pad="0") else NA_character_),
    ann_date  = safe_date(Announcement_Date),
    year_str  = format(ann_date, "%Y"),
    status_now       = norm_status(Current_Facility_Status),
    Investment_Status= norm_status(Investment_Status)
  ) %>%
  dplyr::left_join(
    geo %>% dplyr::select(
      fips, Region, Division, State.Name, state_abbr, GeoName,
      `CBSA Title`, `CBSA Code`, PEA, FCC_PEA_Number, gdp) %>%
      dplyr::distinct(fips, .keep_all=TRUE) %>%
      dplyr::rename(CBSA.Title=`CBSA Title`, CBSA.Code=`CBSA Code`),
    by="fips"
  ) %>%
  dplyr::mutate(CBSA.Code=dplyr::coalesce(CBSA.Code, CBSAFP)) %>%
  dplyr::left_join(
    tigris_cbsa_2024_raw %>% sf::st_drop_geometry() %>%
      dplyr::select(CBSA.Code=GEOID, CBSA.Name.2024=NAMELSAD),
    by="CBSA.Code"
  ) %>%
  dplyr::mutate(CBSA.Title=dplyr::coalesce(CBSA.Title, CBSA.Name.2024)) %>%
  dplyr::mutate(
    cd_119 = {
      cd_raw <- ifelse(is.na(CD_GEOID),"",CD_GEOID)
      statefp_from_cd <- substr(cd_raw,1,2); cd_num <- substr(cd_raw,3,4)
      ab <- states_simple$abbr[match(statefp_from_cd, states_simple$statefp_chr)]
      cd_built <- ifelse(!is.na(ab) & nzchar(cd_num),
                         paste0(ab,"-", ifelse(cd_num=="00","AL",cd_num)), NA_character_)
      if("CD119_2024_Name" %in% names(.)) dplyr::coalesce(cd_built, .$CD119_2024_Name) else cd_built
    }
  ) %>%
  dplyr::distinct()

geo_success <- fac_geo %>% dplyr::summarise(
  total=dplyr::n(), has_county=sum(!is.na(fips)), has_cd=sum(!is.na(cd_119)),
  has_cbsa=sum(!is.na(CBSA.Code)), has_cbsa_title=sum(!is.na(CBSA.Title)))
cat("\nSpatial geocoding results:\n"); print(geo_success)
dbg_count_na(fac_geo, c("fips","cd_119","CBSA.Code","CBSA.Title","State.Name","GeoName","PEA"), "fac_geo key columns")

# --- Stage 2: Build fac_long (tidy, one geo per row) ---
cat("\n--- Stage 2: Build fac_long (tidy, one geo per row) ---\n")
fac_long <- fac_geo %>%
  dplyr::transmute(
    Region, Division, State.Name, GeoName, PEA, CBSA.Title, cd_119, Company,
    Decarb_Sector, Segment, Technology, Subcategory,
    Estimated_Total_Facility_CAPEX=nz_num(Estimated_Total_Facility_CAPEX),
    status_now, Investment_Status, ann_date, year_str, Latitude, Longitude
  ) %>%
  dplyr::mutate(industry=dplyr::if_else(Segment=="Manufacturing",
                                        paste0(Technology," Manufacturing"), Technology)) %>%
  tidyr::pivot_longer(
    cols=c(State.Name, cd_119, PEA, CBSA.Title, GeoName),
    names_to="geo", values_to="geo_name"
  ) %>%
  dplyr::filter(!is.na(geo_name), nzchar(geo_name)) %>%
  dplyr::mutate(geo=dplyr::recode(geo,
                                  "State.Name"="State","GeoName"="County","CBSA.Title"="Metro Area",
                                  "PEA"="Economic Area","cd_119"="Congressional District"))
cat("fac_long rows:", nrow(fac_long), "\n")
cat("fac_long unique geos:", paste(unique(fac_long$geo), collapse=", "), "\n")
dbg_count_na(fac_long, c("geo","geo_name","ann_date","Investment_Status","status_now","Estimated_Total_Facility_CAPEX"), "fac_long essentials")

# --- Stage 3: Metric builders ---
build_total <- function(df){
  out <- df %>% dplyr::filter(!is.na(ann_date), ann_date>as.Date("2022-08-15"), Investment_Status!="") %>%
    dplyr::group_by(geo, geo_name, status_now) %>%
    dplyr::summarise(val=sum(Estimated_Total_Facility_CAPEX, na.rm=TRUE), .groups="drop") %>%
    dplyr::mutate(status_now=dplyr::recode(status_now,
                                           "Operating"="Operating Investment since IRA",
                                           "Announced"="Announced Investment since IRA",
                                           "Under Construction"="Investment under Construction since IRA",
                                           .default=NA_character_)) %>%
    dplyr::filter(!is.na(status_now)) %>%
    tidyr::pivot_wider(names_from=status_now, values_from=val, values_fill=0)
  for(nm in c("Operating Investment since IRA","Announced Investment since IRA","Investment under Construction since IRA"))
    if(!nm %in% names(out)) out[[nm]] <- 0
  out %>% dplyr::mutate(total_investment=rowSums(dplyr::across(c(
    `Operating Investment since IRA`,`Announced Investment since IRA`,
    `Investment under Construction since IRA`)), na.rm=TRUE))
}
build_man_total <- function(df){
  out <- df %>% dplyr::filter(!is.na(ann_date), ann_date>as.Date("2022-08-15"),
                              Decarb_Sector=="Clean Tech Manufacturing", !Investment_Status %in% c("C","R")) %>%
    dplyr::group_by(geo, geo_name, status_now) %>%
    dplyr::summarise(val=sum(Estimated_Total_Facility_CAPEX, na.rm=TRUE), .groups="drop") %>%
    dplyr::mutate(status_now=dplyr::recode(status_now,
                                           "Operating"="Operating Cleantech Manufacturing Investment since IRA",
                                           "Announced"="Announced Cleantech Manufacturing Investment since IRA",
                                           "Under Construction"="Cleantech Manufacturing Investment under Construction since IRA",
                                           .default=NA_character_)) %>%
    dplyr::filter(!is.na(status_now)) %>%
    tidyr::pivot_wider(names_from=status_now, values_from=val, values_fill=0)
  for(nm in c("Operating Cleantech Manufacturing Investment since IRA",
              "Announced Cleantech Manufacturing Investment since IRA",
              "Cleantech Manufacturing Investment under Construction since IRA"))
    if(!nm %in% names(out)) out[[nm]] <- 0
  out %>% dplyr::mutate(Total_Manufacturing_Investment=rowSums(dplyr::across(c(
    `Operating Cleantech Manufacturing Investment since IRA`,
    `Announced Cleantech Manufacturing Investment since IRA`,
    `Cleantech Manufacturing Investment under Construction since IRA`)), na.rm=TRUE))
}
build_tech_buckets <- function(df){
  keep <- c("Solar","Storage","Batteries Manufacturing","Hydrogen","Wind Manufacturing",
            "Zero Emission Vehicles Manufacturing","Solar Manufacturing","Critical Minerals Manufacturing")
  df %>% dplyr::filter(!status_now %in% c("C"), year_str %in% c("2024","2025")) %>%
    dplyr::group_by(geo, geo_name, industry) %>%
    dplyr::summarise(Estimated_Total_Facility_CAPEX=sum(Estimated_Total_Facility_CAPEX, na.rm=TRUE), .groups="drop") %>%
    dplyr::mutate(industry=factor(industry)) %>%
    dplyr::filter(as.character(industry) %in% keep) %>%
    tidyr::pivot_wider(names_from=industry, values_from=Estimated_Total_Facility_CAPEX, values_fill=0, names_prefix="inv_")
}
build_top5 <- function(df){
  df %>% dplyr::filter(!is.na(ann_date), year_str %in% c("2024","2025"), !status_now %in% c("C")) %>%
    dplyr::group_by(geo, geo_name, Segment, Technology, Subcategory) %>%
    dplyr::summarise(val=sum(Estimated_Total_Facility_CAPEX, na.rm=TRUE), .groups="drop") %>%
    dplyr::mutate(label=dplyr::if_else(Segment=="Manufacturing", paste0(Subcategory," Manufacturing"), Subcategory)) %>%
    dplyr::group_by(geo, geo_name) %>% dplyr::slice_max(order_by=val, n=5, with_ties=FALSE) %>%
    dplyr::arrange(geo, geo_name, dplyr::desc(val)) %>%
    dplyr::mutate(rn=dplyr::row_number()) %>%
    dplyr::summarise(inv_description=paste0(rn, ". ", label, " ($", round(val,1), "m)", collapse=", "), .groups="drop")
}
build_top_facility <- function(df){
  df %>% dplyr::filter(!is.na(ann_date), ann_date>as.Date("2022-08-15"), !Investment_Status %in% c("C","R")) %>%
    dplyr::group_by(geo, geo_name) %>%
    dplyr::slice_max(order_by=Estimated_Total_Facility_CAPEX, n=1, with_ties=FALSE) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(topinv_desc=paste0(
      "In ", format(ann_date,"%Y"), ", ", Company,
      ifelse(status_now=="Operating"," made an investment of "," announced an investment of "),
      round(Estimated_Total_Facility_CAPEX,1), " million dollars in a ",
      ifelse(Segment=="Manufacturing", paste0(Technology," Manufacturing"), Technology), " project."
    )) %>% dplyr::select(geo, geo_name, topinv_desc)
}
build_top_companies <- function(df){
  df %>% dplyr::filter(!is.na(ann_date), ann_date>as.Date("2022-08-15"), !Investment_Status %in% c("C","R")) %>%
    dplyr::mutate(Company=gsub("\\([0-9]+\\)","",Company), Company=gsub(", LLC","",Company)) %>%
    dplyr::group_by(geo, geo_name, Company) %>%
    dplyr::summarise(val=sum(Estimated_Total_Facility_CAPEX, na.rm=TRUE), .groups="drop_last") %>%
    dplyr::slice_max(order_by=val, n=3, with_ties=FALSE) %>%
    dplyr::arrange(dplyr::desc(val), .by_group=TRUE) %>%
    dplyr::summarise(top_industries=paste0(Company, " ($", round(val,1), "m)", collapse=", "), .groups="drop")
}

# --- Stage 4: Compute metrics ---
fac_total   <- build_total(fac_long);     cat("fac_total rows:",   nrow(fac_total),   "cols:", ncol(fac_total),   "\n")
fac_man     <- build_man_total(fac_long); cat("fac_man rows:",     nrow(fac_man),     "cols:", ncol(fac_man),     "\n")
fac_tech    <- build_tech_buckets(fac_long); cat("fac_tech rows:",    nrow(fac_tech),    "cols:", ncol(fac_tech),    "\n")
fac_top5    <- build_top5(fac_long);      cat("fac_top5 rows:",    nrow(fac_top5),    "cols:", ncol(fac_top5),    "\n")
fac_top     <- build_top_facility(fac_long); cat("fac_top rows:",     nrow(fac_top),     "cols:", ncol(fac_top),     "\n")
fac_company <- build_top_companies(fac_long); cat("fac_company rows:", nrow(fac_company), "cols:", ncol(fac_company), "\n")

# --- Stage 5: GDP keyed by geo codes ---
lookup_geo <- geo_long %>% dplyr::rename(geo=geo_type) %>%
  dplyr::mutate(geo_code=dplyr::case_when(
    geo=="State" ~ stringr::str_pad(as.character(geo_code), width=2, pad="0"),
    TRUE ~ as.character(geo_code)
  ))

gdp_by_code <- GEO_LONG_GDP %>%
  dplyr::transmute(
    geo=dplyr::recode(geo_type,
                      "state"="State","county"="County","cbsa"="Metro Area",
                      "pea"="Economic Area","cd119"="Congressional District", .default=NA_character_),
    geo_code=as.character(geo_code),
    gdp=as.numeric(geo_gdp_2022_estimated)
  ) %>% dplyr::filter(!is.na(geo))
assert_unique_by(gdp_by_code, c("geo","geo_code"))

# --- Stage 6: Build scaffold + attach metrics ---
scaffold <- lookup_geo %>% dplyr::select(geo, geo_name, geo_code) %>% dplyr::distinct()
facilities_all <- scaffold %>%
  dplyr::left_join(fac_total,   by=c("geo","geo_name")) %>%
  dplyr::left_join(fac_man,     by=c("geo","geo_name")) %>%
  dplyr::left_join(fac_tech,    by=c("geo","geo_name")) %>%
  dplyr::left_join(fac_top5,    by=c("geo","geo_name")) %>%
  dplyr::left_join(fac_top,     by=c("geo","geo_name")) %>%
  dplyr::left_join(fac_company, by=c("geo","geo_name")) %>%
  dplyr::left_join(gdp_by_code, by=c("geo","geo_code"))
cat("After joins: facilities_all rows:", nrow(facilities_all), "cols:", ncol(facilities_all), "\n")

# Coalesce numeric metric columns to zero
facilities_all <- {
  out <- facilities_all
  metric_cols <- c(
    "Operating Investment since IRA","Announced Investment since IRA",
    "Investment under Construction since IRA","total_investment",
    "Operating Cleantech Manufacturing Investment since IRA",
    "Announced Cleantech Manufacturing Investment since IRA",
    "Cleantech Manufacturing Investment under Construction since IRA",
    "Total_Manufacturing_Investment"
  )
  metric_cols <- intersect(metric_cols, names(out))
  inv_num_cols <- names(out)[startsWith(names(out),"inv_") & vapply(out, is.numeric, logical(1))]
  cat("Numeric inv_* columns to coalesce:", length(inv_num_cols), "\n")
  out %>%
    dplyr::mutate(dplyr::across(dplyr::all_of(metric_cols), ~ dplyr::coalesce(.x,0))) %>%
    dplyr::mutate(dplyr::across(dplyr::all_of(inv_num_cols), ~ dplyr::coalesce(.x,0)))
}

# Ratio (unit-corrected) & ranks
facilities_all <- facilities_all %>%
  dplyr::mutate(
    gdp_2022_millions=dplyr::if_else(is.finite(gdp), gdp/1000, NA_real_),
    inv_gdp=dplyr::if_else(is.finite(gdp_2022_millions) & !is.na(gdp_2022_millions) & gdp_2022_millions>0,
                           (dplyr::coalesce(total_investment,0)/gdp_2022_millions)*100, NA_real_)
  ) %>%
  dplyr::group_by(geo) %>%
  dplyr::mutate(
    Operating_Investment_Rank           = rank(-`Operating Investment since IRA`, ties.method="min", na.last="keep"),
    Total_Investment_Rank               = rank(-total_investment,                 ties.method="min", na.last="keep"),
    Total_Manufacturing_Investment_Rank = rank(-Total_Manufacturing_Investment,   ties.method="min", na.last="keep"),
    CAPEX_GDP_Rank                      = rank(-inv_gdp,                          ties.method="min", na.last="keep")
  ) %>% dplyr::ungroup()

cat("\nType check (inv_description/top_industries should be <chr>):\n")
if("inv_description" %in% names(facilities_all)) cat("  inv_description:", class(facilities_all$inv_description), "\n")
if("top_industries" %in% names(facilities_all))  cat("  top_industries :", class(facilities_all$top_industries),  "\n")

cat("\nfacilities_all built: rows=", nrow(facilities_all), "  cols=", ncol(facilities_all), "\n")
dbg_count_na(facilities_all, c("gdp","total_investment","inv_gdp"), "facilities_all metrics")

geo_summary <- facilities_all %>%
  dplyr::group_by(geo) %>%
  dplyr::summarise(count=dplyr::n(),
                   with_investment=sum(dplyr::coalesce(total_investment,0)>0, na.rm=TRUE),
                   pct_with_investment=round(100*with_investment/count,1), .groups="drop")
cat("\nCoverage by geo (count / % with any investment):\n"); print(geo_summary)

final_check <- scaffold %>%
  dplyr::left_join(facilities_all %>% dplyr::select(geo, geo_name) %>% dplyr::distinct(), by=c("geo","geo_name")) %>%
  dplyr::group_by(geo) %>%
  dplyr::summarise(expected=dplyr::n(), found=sum(!is.na(geo_name)), missing=expected - found, .groups="drop")
cat("\nFinal verification vs geo_long:\n"); print(final_check)

# --- Stage 7: facilities_clean_geo (point map rows) ---
cat("\n--- Stage 7: facilities_clean_geo (point map rows) ---\n")
facilities_clean_geo <- fac_geo %>%
  dplyr::filter(!is.na(Announcement_Date)) %>%
  dplyr::mutate(announce_date=safe_date(Announcement_Date)) %>%
  dplyr::filter(announce_date > as.Date("2022-08-01")) %>%
  dplyr::select(-dplyr::any_of(c("state_abbr","CBSA.Name.2024"))) %>%
  dplyr::transmute(
    Region, Division,
    State=State.Name, `Economic Area`=PEA, `Metro Area`=CBSA.Title,
    `Congressional District`=cd_119, County=GeoName,
    Company, Decarb_Sector, Technology,
    Estimated_Total_Facility_CAPEX=nz_num(Estimated_Total_Facility_CAPEX),
    Latitude, Longitude
  ) %>%
  dplyr::distinct() %>% dplyr::filter(!is.na(Latitude), !is.na(Longitude))
cat("facilities_clean_geo built: rows=", nrow(facilities_clean_geo), "  cols=", ncol(facilities_clean_geo), "\n")
dbg_count_na(facilities_clean_geo, c("State","Economic Area","Metro Area","Congressional District","County","Estimated_Total_Facility_CAPEX"), "facilities_clean_geo NA key fields")

cat("\n================ FACILITIES PIPELINE (END) ================\n")
dbg_count_na(facilities_all, c("gdp","inv_gdp"), "facilities_all final")

missing_gdp_facilities_all <- facilities_all %>%
  dplyr::filter(is.na(gdp) | gdp=="") %>%
  dplyr::select(geo, geo_name, geo_code, gdp)
cat("\nFacilities_all rows with missing/blank gdp:\n"); print(missing_gdp_facilities_all)

================================================================================
# END Facilities.R
================================================================================


================================================================================
# BEGIN Rengen.R
================================================================================


# EIA 860M (latest)
get_latest_eia860m <- function(lookback_months=3, timeout_sec=30){
  base <- as.Date(format(Sys.Date(), "%Y-%m-01")); mon <- seq(base, length.out=lookback_months, by="-1 month")
  urls <- sprintf("https://www.eia.gov/electricity/data/eia860m/xls/%s_generator%s.xlsx", tolower(format(mon,"%B")), format(mon,"%Y"))
  for(u in urls){
    r <- try(RETRY("HEAD", u, times=2, quiet=TRUE, terminate_on=c(200,403,404), timeout(timeout_sec)), silent=TRUE)
    if(inherits(r,"response") && status_code(r)==200){
      tf <- tempfile(fileext=".xlsx"); RETRY("GET", u, times=2, quiet=TRUE, write_disk(tf, overwrite=TRUE), timeout(timeout_sec)); return(tf)
    }
  }
  NA_character_
}
eia860m_path <- get_latest_eia860m(3); eia860m_raw <- if(!is.na(eia860m_path)) tryCatch(readxl::read_excel(eia860m_path, sheet=1, skip=2), error=function(e) tibble()) else tibble(); eia860m_raw <- fix_df(eia860m_raw)

# geocode
eia_860m_geocoded <- eia860m_raw %>% filter(!is.na(Latitude), !is.na(Longitude)) %>%
  st_as_sf(coords=c("Longitude","Latitude"), crs=4326, remove=FALSE) %>%
  st_join(tigris_counties_2020_raw %>% st_transform(4326) %>% transmute(COUNTY_GEOID_2020=GEOID, COUNTY_NAME_2020=NAME, COUNTY_NAMELSAD_2020=NAMELSAD), join=st_intersects) %>%
  st_join(tigris_congressional_districts_2024_raw %>% st_transform(4326) %>% transmute(CD119_GEOID=GEOID), join=st_intersects) %>%
  st_join(tigris_cbsa_2020_raw %>% st_transform(4326) %>% transmute(CBSA_2020_CODE=GEOID, CBSA_2020_TITLE=NAME), join=st_intersects) %>%
  st_join(tigris_states_2024_raw %>% st_transform(4326) %>% transmute(STATE_ABBR=STUSPS, STATE_NAME=NAME, STATE_FIPS=STATEFP), join=st_intersects) %>%
  st_join(tigris_counties_2024_raw %>% st_transform(4326) %>% transmute(COUNTY_GEOID_2024=GEOID, COUNTY_NAME_2024=NAME, COUNTY_NAMELSAD_2024=NAMELSAD), join=st_intersects)
glimpse(eia_860m_geocoded)

# ---- setup ----
suppressPackageStartupMessages({library(dplyr); library(tidyr); library(purrr); library(sf); library(stringr); library(readr)})
options(dplyr.summarise.inform=FALSE); cat("=== START PIPELINE ===\n")

audit_n <- function(df, name) message(sprintf("[AUDIT] %-35s rows=%s, cols=%s", name, nrow(df), ncol(df)))
is_blank <- function(x) trimws(x) == "" | is.na(x); pct <- function(n, d) ifelse(d>0, round(100*n/d,2), NA_real_)
pad2 <- function(x) stringr::str_pad(as.character(x),2,pad="0"); pad4 <- function(x) stringr::str_pad(as.character(x),4,pad="0"); pad5 <- function(x) stringr::str_pad(as.character(x),5,pad="0"); as_chr <- function(x) as.character(x)

# ---- input snapshots ----
cat("\n=== INPUT SNAPSHOTS ===\n"); suppressWarnings({
  if (exists("eia_860m_geocoded")) audit_n(eia_860m_geocoded, "eia_860m_geocoded (sf)")
  if (exists("geo_long"))           audit_n(geo_long,           "geo_long")
  if (exists("geo"))                audit_n(geo,                "geo")
})

# ---- 1) County + State + CBSA reference ----
cat("\n=== BUILD county/state/CBSA reference ===\n")
territory_statefps <- c("60","66","69","72","78")
tigris_county_state_cbsa_2020 <- tigris_counties_2020_raw %>%
  filter(!STATEFP %in% territory_statefps) %>% st_drop_geometry() %>%
  transmute(COUNTY_GEOID_2020=as_chr(GEOID), COUNTY_NAME_2020=NAMELSAD, STATEFP=as_chr(STATEFP), CBSAFP=as_chr(CBSAFP)) %>%
  left_join(tigris_states_2024_raw %>% st_drop_geometry() %>% transmute(STATEFP=as_chr(STATEFP), STATE_ABBR=STUSPS, STATE_NAME=NAME), by="STATEFP") %>%
  left_join(tigris_cbsa_2020_raw %>% st_drop_geometry() %>% transmute(CBSAFP=as_chr(GEOID), METRO_AREA_NAME_2020=NAME), by="CBSAFP") %>%
  left_join(pea %>% transmute(COUNTY_GEOID_2020=as_chr(fips), FCC_PEA_Name, FCC_PEA_Number=as.numeric(FCC_PEA_Number)), by="COUNTY_GEOID_2020")
audit_n(tigris_county_state_cbsa_2020, "tigris_county_state_cbsa_2020")
n_cty <- n_distinct(tigris_county_state_cbsa_2020$COUNTY_GEOID_2020); n_st <- n_distinct(tigris_county_state_cbsa_2020$STATEFP)
n_cbsa_with_name <- sum(!is.na(tigris_county_state_cbsa_2020$METRO_AREA_NAME_2020))
message(sprintf("[AUDIT] County keys: %s unique; States observed: %s; Counties with CBSA: %s (%.2f%%)", n_cty, n_st, n_cbsa_with_name, pct(n_cbsa_with_name, n_cty)))
n_pea <- sum(!is.na(tigris_county_state_cbsa_2020$FCC_PEA_Number))
message(sprintf("[AUDIT] Counties with FCC PEA mapping: %s of %s (%.2f%%)", n_pea, n_cty, pct(n_pea, n_cty)))

# ---- 2) Congressional Districts (119th) reference ----
cat("\n=== BUILD congressional district (CD119) reference ===\n")
tigris_CD_119 <- tigris_congressional_districts_2024_raw %>%
  st_drop_geometry() %>% transmute(STATEFP=as_chr(STATEFP), CD119_GEOID=as_chr(GEOID)) %>%
  left_join(tigris_states_2024_raw %>% st_drop_geometry() %>% transmute(STATEFP=as_chr(STATEFP), STATE_ABBR=STUSPS, STATE_NAME=NAME), by="STATEFP") %>%
  filter(!STATEFP %in% territory_statefps)
audit_n(tigris_CD_119, "tigris_CD_119")
message(sprintf("[AUDIT] Distinct CD119s: %s; distinct states in CD table: %s", n_distinct(tigris_CD_119$CD119_GEOID), n_distinct(tigris_CD_119$STATEFP)))

# ---- 3) Clean EIA units & expand active-by-year ----
cat("\n=== CLEAN EIA units & expand active-by-year ===\n")
eia_units <- eia_860m_geocoded %>% st_drop_geometry() %>%
  filter(!is.na(`Operating Year`), !is.na(`Nameplate Capacity (MW)`)) %>%
  mutate(Technology=tidyr::replace_na(Technology,"Unknown"),
         start_year=as.integer(`Operating Year`),
         nameplate_mw=suppressWarnings(as.numeric(`Nameplate Capacity (MW)`))) %>%
  transmute(COUNTY_GEOID_2020=as_chr(COUNTY_GEOID_2020), CD119_GEOID=as_chr(CD119_GEOID), Technology, start_year, nameplate_mw)
audit_n(eia_units, "eia_units (post-clean)")
miss_cty <- sum(is.na(eia_units$COUNTY_GEOID_2020)); miss_cd <- sum(is.na(eia_units$CD119_GEOID))
both_miss <- sum(is.na(eia_units$COUNTY_GEOID_2020) & is.na(eia_units$CD119_GEOID))
message(sprintf("[AUDIT] EIA rows missing COUNTY_GEOID_2020: %s (%.2f%%)", miss_cty, pct(miss_cty, nrow(eia_units))))
message(sprintf("[AUDIT] EIA rows missing CD119_GEOID: %s (%.2f%%)", miss_cd, pct(miss_cd, nrow(eia_units))))
message(sprintf("[AUDIT] EIA rows missing BOTH cty & CD: %s (%.2f%%)", both_miss, pct(both_miss, nrow(eia_units))))

min_year <- min(eia_units$start_year, na.rm=TRUE); max_year <- as.integer(format(Sys.Date(), "%Y"))
all_years <- seq(min_year, max_year)
tech_levels <- eia_units %>% distinct(Technology) %>% arrange(Technology) %>% pull()
eia_active_by_year <- eia_units %>% mutate(Year=purrr::map(start_year, ~seq(.x, max_year))) %>% select(-start_year) %>% unnest(Year) %>% as_tibble()
audit_n(eia_active_by_year, "eia_active_by_year")
message(sprintf("[AUDIT] Technologies: %s; Year span: %s..%s (n=%s)", length(tech_levels), min_year, max_year, length(all_years)))

# ---- 4) COUNTY × Technology × Year (zero-filled) ----
cat("\n=== COUNTY × Technology × Year panel ===\n")
county_dim <- tigris_county_state_cbsa_2020 %>% select(COUNTY_GEOID_2020, COUNTY_NAME_2020, STATE_ABBR, STATEFP, STATE_NAME, CBSAFP, METRO_AREA_NAME_2020, FCC_PEA_Name, FCC_PEA_Number)
audit_n(county_dim, "county_dim")
county_agg <- eia_active_by_year %>% filter(!is.na(COUNTY_GEOID_2020)) %>%
  group_by(COUNTY_GEOID_2020, Technology, Year) %>% summarise(operational_nameplate_mw=sum(nameplate_mw, na.rm=TRUE), .groups="drop")
audit_n(county_agg, "county_agg (non-zero universe)")
county_skeleton <- county_dim %>% tidyr::crossing(tibble(Technology=tech_levels), tibble(Year=all_years))
audit_n(county_skeleton, "county_skeleton (zero-filled universe)")
expected_cty_rows <- nrow(county_dim) * length(tech_levels) * length(all_years)
message(sprintf("[AUDIT] Expected county_skeleton rows = %s; observed = %s", format(expected_cty_rows, big.mark=","), format(nrow(county_skeleton), big.mark=",")))
county_capacity_by_tech_year <- county_skeleton %>%
  left_join(county_agg, by=c("COUNTY_GEOID_2020","Technology","Year")) %>%
  mutate(operational_nameplate_mw=tidyr::replace_na(operational_nameplate_mw,0)) %>%
  arrange(COUNTY_GEOID_2020, Technology, Year)
audit_n(county_capacity_by_tech_year, "county_capacity_by_tech_year")
n_zero_cty <- sum(county_capacity_by_tech_year$operational_nameplate_mw==0)
message(sprintf("[AUDIT] County×Tech×Year zeros: %s of %s (%.2f%%)", format(n_zero_cty, big.mark=","), format(nrow(county_capacity_by_tech_year), big.mark=","), pct(n_zero_cty, nrow(county_capacity_by_tech_year))))

# ---- 5) CD119 × Technology × Year (zero-filled) ----
cat("\n=== CD119 × Technology × Year panel ===\n")
cd_dim <- tigris_CD_119 %>% select(STATEFP, STATE_ABBR, STATE_NAME, CD119_GEOID)
audit_n(cd_dim, "cd_dim")
cd119_agg <- eia_active_by_year %>% filter(!is.na(CD119_GEOID)) %>%
  group_by(CD119_GEOID, Technology, Year) %>% summarise(operational_nameplate_mw=sum(nameplate_mw, na.rm=TRUE), .groups="drop")
audit_n(cd119_agg, "cd119_agg")
cd_skeleton <- cd_dim %>% tidyr::crossing(tibble(Technology=tech_levels), tibble(Year=all_years))
audit_n(cd_skeleton, "cd_skeleton")
expected_cd_rows <- nrow(cd_dim) * length(tech_levels) * length(all_years)
message(sprintf("[AUDIT] Expected cd_skeleton rows = %s; observed = %s", format(expected_cd_rows, big.mark=","), format(nrow(cd_skeleton), big.mark=",")))
cd119_capacity_by_tech_year <- cd_skeleton %>%
  left_join(cd119_agg, by=c("CD119_GEOID","Technology","Year")) %>%
  mutate(operational_nameplate_mw=tidyr::replace_na(operational_nameplate_mw,0)) %>%
  arrange(CD119_GEOID, Technology, Year)
audit_n(cd119_capacity_by_tech_year, "cd119_capacity_by_tech_year")

# ---- 6) Metros, States, PEAs from county panel ----
cat("\n=== Aggregate county panel to Metro, State, PEA ===\n")
metro_area_capacity_by_year <- county_capacity_by_tech_year %>%
  filter(!is.na(CBSAFP)) %>% group_by(METRO_AREA_NAME_2020, CBSAFP, Technology, Year) %>%
  summarise(operational_nameplate_mw=sum(operational_nameplate_mw, na.rm=TRUE), .groups="drop")
state_capacity_by_tech_year <- county_capacity_by_tech_year %>%
  filter(!is.na(STATEFP)) %>% group_by(STATE_ABBR, STATEFP, STATE_NAME, Technology, Year) %>%
  summarise(operational_nameplate_mw=sum(operational_nameplate_mw, na.rm=TRUE), .groups="drop")
economic_area_capacity_by_tech_year <- county_capacity_by_tech_year %>%
  filter(!is.na(FCC_PEA_Name)) %>% group_by(FCC_PEA_Name, FCC_PEA_Number, Technology, Year) %>%
  summarise(operational_nameplate_mw=sum(operational_nameplate_mw, na.rm=TRUE), .groups="drop")
audit_n(metro_area_capacity_by_year,"metro_area_capacity_by_year")
audit_n(state_capacity_by_tech_year,"state_capacity_by_tech_year")
audit_n(economic_area_capacity_by_tech_year,"economic_area_capacity_by_tech_year")

# ---- 7) Map Technology → Clean/Fossil/Other ----
tech_to_clean <- function(tech) dplyr::case_when(
  tech %in% c("Conventional Steam Coal","Petroleum Liquids","Petroleum Coke","Natural Gas Steam Turbine","Natural Gas Fired Combined Cycle","Natural Gas Fired Combustion Turbine","Natural Gas Internal Combustion Engine","Other Natural Gas","Other Gases","Coal Integrated Gasification Combined Cycle","Natural Gas with Compressed Air Storage") ~ "Fossil",
  tech %in% c("Conventional Hydroelectric","Hydroelectric Pumped Storage","Onshore Wind Turbine","Offshore Wind Turbine","Solar Photovoltaic","Solar Thermal with Energy Storage","Solar Thermal without Energy Storage","Geothermal","Batteries","Flywheels","Wood/Wood Waste Biomass","Other Waste Biomass","Landfill Gas","Municipal Solid Waste","Nuclear") ~ "Clean",
  TRUE ~ "Other"
)

# ---- 8) Build tidy fact tables & keep codes ----
cat("\n=== Build unified fact table (retain geo_code) ===\n")
county_fact <- county_capacity_by_tech_year %>% transmute(geo="County", geo_name=COUNTY_NAME_2020, geo_code=COUNTY_GEOID_2020, Technology, Year=as.integer(Year), operational_nameplate_mw)
cd_fact <- cd119_capacity_by_tech_year %>% mutate(district_2=substr(CD119_GEOID,3,4), cd_label=paste0(STATE_ABBR,"-", ifelse(district_2=="00","AL",district_2))) %>%
  transmute(geo="Congressional District", geo_name=cd_label, geo_code=CD119_GEOID, Technology, Year=as.integer(Year), operational_nameplate_mw)
state_fact <- state_capacity_by_tech_year %>% transmute(geo="State", geo_name=STATE_NAME, geo_code=STATEFP, Technology, Year=as.integer(Year), operational_nameplate_mw)
metro_fact <- metro_area_capacity_by_year %>% transmute(geo="Metro Area", geo_name=METRO_AREA_NAME_2020, geo_code=CBSAFP, Technology, Year=as.integer(Year), operational_nameplate_mw)
pea_fact <- economic_area_capacity_by_tech_year %>% transmute(geo="Economic Area", geo_name=FCC_PEA_Name, geo_code=as_chr(FCC_PEA_Number), Technology, Year=as.integer(Year), operational_nameplate_mw)
fact_all <- bind_rows(county_fact, cd_fact, state_fact, metro_fact, pea_fact)
audit_n(fact_all, "fact_all (all geos x tech x year)")

# ---- 9) Cumulative capacity ----
cat("\n=== Compute cumulative capacity ===\n")
fact_all_cum <- fact_all %>% arrange(geo, geo_name, Technology, Year) %>%
  group_by(geo, geo_name, geo_code, Technology) %>% mutate(cum_cap=cumsum(operational_nameplate_mw)) %>% ungroup()
audit_n(fact_all_cum, "fact_all_cum")

# ---- 10) Back-compat final_results ----
cat("\n=== Build final_results (back-compat shape) ===\n")
final_results <- fact_all_cum %>% mutate(clean=tech_to_clean(Technology)) %>% filter(clean %in% c("Clean","Fossil")) %>%
  transmute(State.Name=ifelse(geo=="State", geo_name, NA_character_),
            cd_119=ifelse(geo=="Congressional District", geo_name, NA_character_),
            PEA=ifelse(geo=="Economic Area", geo_name, NA_character_),
            CBSA.Title=ifelse(geo=="Metro Area", geo_name, NA_character_),
            GeoName=ifelse(geo=="County", geo_name, NA_character_),
            geo, geo_name, geo_code, `Operating Year`=as.character(Year), Technology, clean, cum_cap)
audit_n(final_results, "final_results")

# ---- 11) rengen snapshots ----
cat("\n=== Build rengen snapshots ===\n")
snap_years <- c(2019L,2021L,2024L); year_univ <- unique(as.integer(fact_all_cum$Year))
snap_years <- snap_years[snap_years %in% year_univ]
safe_div <- function(num, den) ifelse(is.finite(num/den), num/den, NA_real_)
rengen <- final_results %>% group_by(geo,geo_code,geo_name,clean,`Operating Year`) %>%
  summarise(cum_cap=sum(cum_cap, na.rm=TRUE), .groups="drop") %>% filter(!is_blank(geo_name)) %>%
  filter(`Operating Year` %in% as.character(snap_years)) %>%
  tidyr::pivot_wider(names_from=`Operating Year`, values_from=cum_cap) %>%
  mutate(change_19_24=if("2019" %in% names(.)) round(`2024`-`2019`,1) else NA_real_,
         growth_19_24=if("2019" %in% names(.)) round(100*safe_div(`2024`,`2019`)-100,1) else NA_real_,
         change_21_24=if("2021" %in% names(.)) round(`2024`-`2021`,1) else NA_real_,
         growth_21_24=if("2021" %in% names(.)) round(100*safe_div(`2024`,`2021`)-100,1) else NA_real_) %>%
  select(-any_of(c("2019","2021"))) %>%
  tidyr::pivot_wider(names_from=clean, values_from=c(`2024`,growth_19_24,change_19_24,growth_21_24,change_21_24), names_sep="_") %>%
  mutate(`2024_Fossil`=dplyr::coalesce(`2024_Fossil`,0),
         clean_share=round(100*safe_div(`2024_Clean`,(`2024_Clean`+`2024_Fossil`)),1)) %>%
  arrange(geo, geo_name)
audit_n(rengen, "rengen (one row per geography)")

# ---- 12) Tech-cap snapshots ----
cat("\n=== Build tech_cap snapshots ===\n")
tech_cap <- final_results %>% group_by(geo,geo_code,geo_name,Technology,`Operating Year`) %>%
  summarise(cum_cap=sum(cum_cap, na.rm=TRUE), .groups="drop") %>% filter(!is_blank(geo_name)) %>%
  filter(`Operating Year` %in% c("2014","2021","2024")) %>%
  tidyr::pivot_wider(names_from=`Operating Year`, values_from=cum_cap) %>%
  mutate(growth_14_24=round(`2024`-`2014`,1), growth_21_24=round(`2024`-`2021`,1)) %>%
  arrange(geo, geo_name, Technology)
audit_n(tech_cap, "tech_cap")

# ---- 13) Audits vs geo_long ----
cat("\n=== AUDITS vs geo_long (counts & mismatches) ===\n")
normalize_geo_code <- function(geo_type, code) dplyr::case_when(
  geo_type=="State" ~ pad2(code), geo_type=="County" ~ pad5(code),
  geo_type=="Congressional District" ~ pad4(code), geo_type=="Metro Area" ~ pad5(code),
  geo_type=="Economic Area" ~ as_chr(as.integer(code)), TRUE ~ as_chr(code)
)
rengen_geos <- rengen %>% transmute(geo_type=geo, geo_name, geo_code) %>% distinct()
audit_n(rengen_geos, "rengen_geos (registry)")
if(!all(c("geo_type","geo_name","geo_code") %in% names(geo_long))) stop("geo_long must have columns: geo_type, geo_name, geo_code")
geo_long_norm <- geo_long %>% mutate(geo_code_norm=normalize_geo_code(geo_type, geo_code)) %>% distinct(geo_type, geo_name, geo_code=geo_code_norm)
audit_n(geo_long_norm, "geo_long_norm")
rengen_geos_norm <- rengen_geos %>% mutate(geo_code_norm=normalize_geo_code(geo_type, geo_code)) %>% transmute(geo_type, geo_name, geo_code=geo_code_norm) %>% distinct()
audit_n(rengen_geos_norm, "rengen_geos_norm")
counts_geo_long <- geo_long_norm %>% count(geo_type, name="n_geo_long") %>% arrange(geo_type)
counts_rengen <- rengen_geos_norm %>% count(geo_type, name="n_rengen") %>% arrange(geo_type)
counts_join <- counts_geo_long %>% full_join(counts_rengen, by="geo_type") %>% replace_na(list(n_geo_long=0L, n_rengen=0L)) %>% mutate(delta=n_rengen-n_geo_long)
print(counts_join)

geo_long_count <- geo_long_norm %>% filter(!is_blank(geo_name)) %>% distinct(geo_type, geo_name) %>% nrow()
rengen_count <- rengen_geos_norm %>% distinct(geo_type, geo_name) %>% nrow()
cat("\n--- Overall distinct geographies ---\n")
cat("geo_long distinct (geo_type, geo_name): ", geo_long_count, "\n", sep="")
cat("rengen   distinct (geo_type, geo_name): ", rengen_count, "\n", sep="")

by_type <- sort(unique(c(geo_long_norm$geo_type, rengen_geos_norm$geo_type)))
for (gt in by_type) {
  cat("\n[CHECK] Geo type: ", gt, "\n", sep="")
  lhs <- geo_long_norm  %>% filter(geo_type==gt)
  rhs <- rengen_geos_norm %>% filter(geo_type==gt)
  only_in_geo_long <- anti_join(lhs, rhs, by=c("geo_type","geo_code"))
  only_in_rengen   <- anti_join(rhs, lhs, by=c("geo_type","geo_code"))
  cat(sprintf("  - In geo_long not in rengen: %s\n", nrow(only_in_geo_long)))
  cat(sprintf("  - In rengen   not in geo_long: %s\n", nrow(only_in_rengen)))
  if (nrow(only_in_geo_long) > 0) { cat("    * Sample missing from rengen:\n"); print(head(only_in_geo_long, 10)) }
  if (nrow(only_in_rengen)   > 0) { cat("    * Sample missing from geo_long:\n"); print(head(only_in_rengen,   10)) }
}

glimpse(rengen)

================================================================================
# END Rengen.R
================================================================================


================================================================================
# BEGIN elec_grid.R
================================================================================

# ====================== PREP: Quick glances at input data already in memory ======================
cat("Glimpse COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA:\n"); glimpse(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA)
cat("Glimpse geo:\n"); glimpse(geo)
cat("Glimpse geo_long:\n"); glimpse(geo_long)
cat("Glimpse tigris_counties_2020_raw:\n"); glimpse(tigris_counties_2020_raw)
cat("Glimpse geocorr_county_2020_cd_119: \n"); glimpse(geocorr_county_2020_cd_119)
cat("Glimpse geocorr_ct_county_cd_119: \n"); glimpse(geocorr_ct_county_cd_119)
cat("Glimpse (tigris_cbsa_2020_raw"); glimpse(tigris_cbsa_2020_raw)
cat("Glimpse tigris_states_2024_raw:\n"); glimpse(tigris_states_2024_raw)
cat("Glimpse tigris_congressional_districts_2024_raw"); glimpse(tigris_congressional_districts_2024_raw)
cat("Glimpse geographies"); glimpse(geographies)
cat("Glimpse geographies_clean"); glimpse(geographies_clean)

# ==============================================================
# Electricity grid → geographies with 100% coverage of geo_long
# (Compressed: identical logic, same outputs)
# ==============================================================

# -----------------------
# Helpers / configuration
# -----------------------
DEBUG <- TRUE
dcat <- function(...) if (DEBUG) cat(...)
dbg  <- function(x, title = deparse(substitute(x))) {
  if (!DEBUG) return(invisible(NULL))
  cat("\n====", title, "====\n", sep = " ")
  if (inherits(x, "sf")) {
    cat("Rows:", nrow(x), " Cols:", ncol(x), "  CRS:", sf::st_crs(x)$epsg, "\n")
    print(head(as.data.frame(sf::st_drop_geometry(x)), 3), row.names = FALSE)
  } else {
    cat("Rows:", nrow(x), " Cols:", ncol(x), "\n")
    print(dplyr::glimpse(x, width = 80))
  }
  invisible(NULL)
}
norm_key <- function(x) x %>%
  toupper() %>%
  str_replace_all("&", "AND") %>%
  str_replace_all("PUD NO\\.", "PUBLIC UTILITY DISTRICT NO.") %>%
  str_replace_all("[^A-Z0-9 ]+", " ") %>%
  str_squish()
pick_col <- function(df, pat){
  nm <- names(df)
  h  <- nm[str_detect(nm, regex(pat, TRUE))]
  ifelse(length(h), h[1], NA_character_)
}
wmean_safe <- function(x, w){
  if (length(x) == 0) return(NA_real_)
  w <- replace_na(w, 0)
  if (all(is.na(x))) return(NA_real_)
  sw <- sum(w, na.rm = TRUE)
  if (is.na(sw) || sw == 0) mean(x, na.rm = TRUE) else stats::weighted.mean(x, w, na.rm = TRUE)
}

# ---------------
# Preconditions
# ---------------
territory_path <- nz_chr(Sys.getenv("ELECTRIC_TERRITORIES_SHP"))
if (is.na(territory_path)) {
  cand <- file.path(paths$raw_data, "Electric_Retail_Service_Territories.shp")
  territory_path <- if (file.exists(cand)) cand else NA_character_
}
retail_territories_raw <- st_read(territory_path); cat("Glimpse of retail_territories_raw..."); glimpse(retail_territories_raw)

need <- c("retail_territories_raw","tigris_counties_2020_raw","geo","geographies","county_gdp_clean","geo_long")
miss <- need[!sapply(need, exists)]; if (length(miss)) stop("Missing required objects in environment: ", paste(miss, collapse = ", "))

if ("CBSA Title" %in% names(geo) && !("CBSA.Title" %in% names(geo))) geo <- dplyr::rename(geo, CBSA.Title = `CBSA Title`)
if ("CBSA Code" %in% names(geo) && !("CBSA.Code" %in% names(geo))) geo <- dplyr::rename(geo, CBSA.Code = `CBSA Code`)

# -------------------------------
# 1) Electricity Maps 2024 (raw)
# -------------------------------
zones <- c(
  "US-CAR-YAD","US-SW-AZPS","US-MIDW-AECI","US-NW-AVA","US-CAL-BANC","US-NW-BPAT",
  "US-CAL-CISO","US-NW-TPWR","US-FLA-TAL","US-CAR-DUK","US-FLA-FPC","US-CAR-CPLE",
  "US-CAR-CPLW","US-SW-EPE","US-TEX-ERCO","US-FLA-FMPP","US-FLA-FPL","US-FLA-GVL",
  "US-NW-GRID","US-NW-IPCO","US-CAL-IID","US-NE-ISNE","US-FLA-JEA","US-CAL-LDWP",
  "US-MIDW-LGEE","US-MIDW-MISO","US-NW-NEVP","US-NY-NYIS","US-NW-NWMT","US-MIDA-PJM",
  "US-NW-CHPD","US-NW-DOPD","US-NW-GCPD","US-NW-PACE","US-NW-PACW","US-NW-PGE",
  "US-NW-PSCO","US-SW-PNM","US-NW-PSEI","US-SW-SRP","US-NW-SCL","US-FLA-SEC",
  "US-CAR-SCEG","US-CAR-SC","US-SE-SOCO","US-CENT-SWPP","US-CENT-SPA","US-FLA-TEC",
  "US-TEN-TVA","US-SW-TEPC","US-CAL-TIDC","US-SW-WALC","US-NW-WACM","US-NW-WAUW"
)
em_list <- lapply(zones, function(z){
  u <- paste0("https://data.electricitymaps.com/2025-01-27/", z, "_2024_yearly.csv")
  tryCatch(read.csv(u, check.names = FALSE), error = function(e) tibble(zone = z, err = as.character(e)))
})
electricity_maps_2024_raw <- bind_rows(em_list) %>% fix_df()
dbg(electricity_maps_2024_raw, "electricity_maps_2024_raw")

# -------------------------------
# 2) Retail territories → counties
# -------------------------------
counties_sf <- st_as_sf(tigris_counties_2020_raw, sf_column_name = "geometry",
                        crs = st_crs(tigris_counties_2020_raw$geometry)) %>%
  st_transform(4326)
dcat("[DEBUG] counties CRS:", st_crs(counties_sf)$epsg, "\n")

territories_to_county <- retail_territories_raw %>%
  st_transform(st_crs(counties_sf)) %>%
  st_make_valid()
sf::sf_use_s2(FALSE)
territories_to_county <- st_join(territories_to_county, counties_sf, join = st_intersects)
sf::sf_use_s2(TRUE)
territories_to_county <- territories_to_county %>%
  st_drop_geometry() %>%
  transmute(GEOID = sprintf("%05d", as.integer(GEOID)),
            CNTRL_AREA = norm_key(CNTRL_AREA)) %>%
  distinct()

# ----------------------------------------
# 3) Clean Electricity Maps zone metadata
# ----------------------------------------
ci_col  <- pick_col(electricity_maps_2024_raw, "^Carbon\\s*Intensity.*\\(direct\\)$")
ren_col <- pick_col(electricity_maps_2024_raw, "^Renewable[\\._ ]*Percentage$")
if (any(is.na(c(ci_col, ren_col)))) stop("Could not find CI/Renewable columns in electricity_maps_2024_raw.")
emap_fix <- c(
  "Northwestern Energy"="NORTHWESTERN ENERGY (NWMT)",
  "Duke Energy Progress West"="DUKE ENERGY PROGRESS WEST",
  "Gridforce Energy Management, LLC"="GRIDFORCE ENERGY MANAGEMENT LLC",
  "Jacksonville Electric Authority"="JEA",
  "Midcontinent Independent Transmission System Operator, Inc."="MIDCONTINENT INDEPENDENT TRANSMISSION SYSTEM OPERATOR, INC.",
  "PUD No. 1 of Chelan County"="PUBLIC UTILITY DISTRICT NO. 1 OF CHELAN COUNTY",
  "PUD No. 2 of Grant County, Washington"="PUBLIC UTILITY DISTRICT NO. 2 OF GRANT COUNTY, WASHINGTON",
  "Pacificorp East"="PACIFICORP - EAST",
  "Pacificorp West"="PACIFICORP - WEST"
)
em24 <- electricity_maps_2024_raw %>%
  rename(`Zone Name` = any_of(c("Zone Name","Zone.Name")),
         `Zone Id`   = any_of(c("Zone Id","Zone.Id"))) %>%
  mutate(
    `Zone Name`    = coalesce(emap_fix[`Zone Name`], `Zone Name`),
    cntrl_area_key = norm_key(`Zone Name`),
    ci_direct      = suppressWarnings(as.numeric(.data[[ci_col]])),
    ren_share      = suppressWarnings(as.numeric(.data[[ren_col]]))
  ) %>%
  select(`Zone Id`,`Zone Name`,cntrl_area_key,ci_direct,ren_share)

# ------------------------------------------------------
# 4) GDP weights (county-level)
# ------------------------------------------------------
gdp_cols <- grep("^gdp_\\d{4}$|^gdp$", names(county_gdp_clean), value = TRUE)
if (!length(gdp_cols)) stop("county_gdp_clean must have a 'gdp' or 'gdp_YYYY' column.")
gdp_col <- gdp_cols[order(gdp_cols, decreasing = TRUE)][1]
gdp_county_final <- county_gdp_clean %>%
  mutate(fips = sprintf("%05d", as.integer(.data[["fips"]]))) %>%
  transmute(GEOID = fips, gdp = as.numeric(.data[[gdp_col]])) %>%
  group_by(GEOID) %>%
  summarise(gdp = sum(gdp, na.rm = TRUE), .groups = "drop")
dbg(gdp_county_final, "gdp_county_final")

# ------------------------------------------------------
# 5) Build per-county join table with attributes + GDP
# ------------------------------------------------------
em_counties <- territories_to_county %>%
  inner_join(em24, by = c("CNTRL_AREA" = "cntrl_area_key")) %>%
  left_join(select(geo, fips, State.Name, cd_119, percent_district, PEA, CBSA.Title, GeoName, gdp),
            by = c("GEOID" = "fips")) %>%
  left_join(gdp_county_final %>% rename(gdp_county = gdp), by = "GEOID") %>%
  mutate(gdp_any = coalesce(gdp, gdp_county, 0))

# ------------------------------------------------------
# 6) Primary aggregation (fast path)
# ------------------------------------------------------
agg_one <- function(key){
  if (!key %in% names(em_counties)) return(NULL)
  df <- filter(em_counties, !is.na(.data[[key]]))
  if (key == "cd_119") {
    df %>% group_by(.data[[key]]) %>% summarise(
      `Electricity Consumption Carbon Intensity (CO2eq/kWh)` =
        wmean_safe(ci_direct, replace_na(gdp_county, 0) * replace_na(percent_district, 0) / 100),
      `Electricity Consumption Renewable Percentage` =
        wmean_safe(ren_share,  replace_na(gdp_county, 0) * replace_na(percent_district, 0) / 100),
      .groups = "drop"
    )
  } else {
    df %>% distinct(.data[[key]], ci_direct, ren_share, gdp_any) %>%
      group_by(.data[[key]]) %>% summarise(
        `Electricity Consumption Carbon Intensity (CO2eq/kWh)` =
          wmean_safe(ci_direct, replace_na(gdp_any, 0)),
        `Electricity Consumption Renewable Percentage` =
          wmean_safe(ren_share,  replace_na(gdp_any, 0)),
        .groups = "drop"
      )
  }
}

elec_grid <- bind_rows(lapply(geographies, agg_one)) %>%
  ungroup() %>%
  mutate(
    geo      = case_when(!is.na(State.Name) ~ "State",
                         !is.na(cd_119)     ~ "Congressional District",
                         !is.na(PEA)        ~ "Economic Area",
                         !is.na(GeoName)    ~ "County",
                         !is.na(CBSA.Title) ~ "Metro Area",
                         TRUE               ~ NA_character_),
    geo_name = coalesce(State.Name, cd_119, PEA, GeoName, CBSA.Title)
  ) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(-State.Name, -cd_119, -PEA, -CBSA.Title, -GeoName)

# ------------------------------------------------------
# 7) Coverage backstop (guarantee 100% of geo_long)
# ------------------------------------------------------
county_metrics <- em_counties %>%
  group_by(GEOID) %>%
  summarise(ci_direct_cm = mean(ci_direct, na.rm = TRUE),
            ren_share_cm = mean(ren_share, na.rm = TRUE), .groups = "drop") %>%
  left_join(gdp_county_final, by = "GEOID")

state_by_county <- geo %>%
  select(fips, State.Name) %>%
  mutate(fips = sprintf("%05d", as.integer(fips))) %>%
  distinct()

state_metrics <- county_metrics %>%
  left_join(state_by_county, by = c("GEOID" = "fips")) %>%
  filter(!is.na(State.Name)) %>%
  group_by(State.Name) %>%
  summarise(ci_state = wmean_safe(ci_direct_cm, gdp),
            ren_state = wmean_safe(ren_share_cm, gdp), .groups = "drop")

national_metrics <- county_metrics %>%
  summarise(ci_nat  = wmean_safe(ci_direct_cm, gdp),
            ren_nat = wmean_safe(ren_share_cm, gdp))

geo_col_map <- c(
  "State"                  = "State.Name",
  "Congressional District" = "cd_119",
  "Economic Area"          = "PEA",
  "County"                 = "GeoName",
  "Metro Area"             = "CBSA.Title"
)

counties_for_geo <- function(geo_type, geo_name) {
  key <- geo_col_map[[geo_type]]
  if (is.na(key) || !key %in% names(geo)) return(character(0))
  geo %>%
    filter(.data[[key]] == geo_name) %>%
    transmute(GEOID = sprintf("%05d", as.integer(fips))) %>%
    distinct(GEOID) %>%
    pull(GEOID)
}

agg_from_counties <- function(geo_type, geo_name) {
  geos <- counties_for_geo(geo_type, geo_name)
  if (!length(geos)) {
    if (geo_type == "State") {
      st_row <- state_metrics %>% filter(State.Name == geo_name)
      if (nrow(st_row)) return(tibble(
        `Electricity Consumption Carbon Intensity (CO2eq/kWh)` = st_row$ci_state[1],
        `Electricity Consumption Renewable Percentage`         = st_row$ren_state[1]
      ))
    }
    return(tibble(
      `Electricity Consumption Carbon Intensity (CO2eq/kWh)` = national_metrics$ci_nat[1],
      `Electricity Consumption Renewable Percentage`         = national_metrics$ren_nat[1]
    ))
  }
  df <- county_metrics %>% filter(GEOID %in% geos)
  if (!nrow(df)) {
    st_mode <- geo %>%
      filter(fips %in% as.integer(geos)) %>%
      count(State.Name, sort = TRUE) %>%
      slice_head(n = 1) %>%
      pull(State.Name)
    if (length(st_mode) && st_mode %in% state_metrics$State.Name) {
      st_row <- state_metrics %>% filter(State.Name == st_mode)
      return(tibble(
        `Electricity Consumption Carbon Intensity (CO2eq/kWh)` = st_row$ci_state[1],
        `Electricity Consumption Renewable Percentage`         = st_row$ren_state[1]
      ))
    }
    return(tibble(
      `Electricity Consumption Carbon Intensity (CO2eq/kWh)` = national_metrics$ci_nat[1],
      `Electricity Consumption Renewable Percentage`         = national_metrics$ren_nat[1]
    ))
  }
  tibble(
    `Electricity Consumption Carbon Intensity (CO2eq/kWh)` = wmean_safe(df$ci_direct_cm, df$gdp),
    `Electricity Consumption Renewable Percentage`         = wmean_safe(df$ren_share_cm, df$gdp)
  )
}

elec_key   <- elec_grid %>% transmute(geo, geo_name)
target_key <- geo_long  %>% transmute(geo = geo_type, geo_name)
missing_keys <- anti_join(target_key, elec_key, by = c("geo","geo_name")) %>% distinct()

if (nrow(missing_keys)) {
  backstop_vals <- missing_keys %>%
    mutate(agg = map2(geo, geo_name, agg_from_counties)) %>%
    unnest(agg)
  elec_grid <- elec_grid %>% bind_rows(backstop_vals)
}

# Final tidy + ranks
elec_grid <- elec_grid %>%
  group_by(geo) %>%
  mutate(
    `Grid Carbon Intensity Rank`      = rank(-`Electricity Consumption Carbon Intensity (CO2eq/kWh)`, ties.method = "first"),
    `Grid Renewables Percentage Rank` = rank(-`Electricity Consumption Renewable Percentage`,          ties.method = "first")
  ) %>% ungroup()

# ---------------------------------
# 8) Hard guarantees / validations
# ---------------------------------
elec_grid <- elec_grid %>% semi_join(target_key, by = c("geo","geo_name"))

key_na <- elec_grid %>%
  filter(is.na(`Electricity Consumption Carbon Intensity (CO2eq/kWh)`) |
           is.na(`Electricity Consumption Renewable Percentage`) |
           is.nan(`Electricity Consumption Carbon Intensity (CO2eq/kWh)`) |
           is.nan(`Electricity Consumption Renewable Percentage`))
if (nrow(key_na)) stop("Post-backstop still has NA/NaN metrics. Investigate unexpected gaps.")

gap <- anti_join(target_key, elec_grid %>% select(geo, geo_name), by = c("geo","geo_name"))
if (nrow(gap)) stop("Coverage gap remains after backstop (unexpected). Missing examples: ",
                    paste(utils::head(paste(gap$geo, gap$geo_name, sep = " = "), 10), collapse = "; "), " …")

cat("\n[OK] 100% coverage of geo_long with zero NA/NaN in key metrics.\n")
dbg(elec_grid, "elec_grid (final, 100% coverage)")

================================================================================
# END elec_grid.R
================================================================================


================================================================================
# BEGIN gdp_by_industry.R
================================================================================

cat("Glimpse COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA:\n"); glimpse(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA)
cat("Glimpse geo:\n"); glimpse(geo)
cat("Glimpse geo_long:\n"); glimpse(geo_long)
cat("Glimpse tigris_counties_2020_raw:\n"); glimpse(tigris_counties_2020_raw)
cat("Glimpse geocorr_county_2020_cd_119: \n"); glimpse(geocorr_county_2020_cd_119)
cat("Glimpse geocorr_ct_county_cd_119: \n"); glimpse(geocorr_ct_county_cd_119)

# ===================================================================
# County GDP by Industry (BEA CAGDP11/CAGDP2) — HARDENED
# Goal: produce `county_gdp_ind_final` aligned EXACTLY to `geo_long`
# Fixes for your current run:
#   • Robust handling of `geo_long` with columns (geo_type, geo_name, geo_code)
#   • Normalizes `geo` column names (e.g., "CBSA Title" → "CBSA.Title")
#   • Builds `geographies` ONLY from columns that actually exist in `geo`
#   • Eliminates the CBSA.Title select() error and wrong quoted column in fallback
#   • Fills missing GeoName after HI/VA/CT expansion by also joining to `geo`
#   • Adds deeper diagnostics (NA counts, joins, coverage) at each step
# ABSOLUTELY NOTHING OMITTED. Extra debug throughout.
# ===================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(readr)
  library(purrr)
})

# ------------------------ Debug helpers -----------------------------------
if (!exists("dbg")) {
  dbg <- function(x, label = deparse(substitute(x)), n = 6) {
    cat("\n====", label, "====\n")
    cat("Rows:", nrow(x), "Cols:", ncol(x), "\n")
    print(dplyr::glimpse(dplyr::as_tibble(x), width = 100))
    if (nrow(x) > 0) { cat("Head:\n"); print(utils::head(x, n)) }
    invisible(x)
  }
}
if (!exists("dbg_count_na")) {
  dbg_count_na <- function(df, cols, label="NA counts") {
    cat("\n-- NA counts:", label, "--\n")
    out <- sapply(cols, function(cn) if (cn %in% names(df)) sum(is.na(df[[cn]])) else NA_integer_)
    print(out)
    invisible(out)
  }
}
msg <- function(...) cat("\n#", sprintf(...), "\n")

# dplyr-1.1+ compatible "many-to-many ok" joins (fallback for older dplyr)
mm_left_join <- function(x, y, by, ...) {
  if (utils::packageVersion("dplyr") >= "1.1.0") {
    dplyr::left_join(x, y, by = by, relationship = "many-to-many", ...)
  } else dplyr::left_join(x, y, by = by, ...)
}
mm_inner_join <- function(x, y, by, ...) {
  if (utils::packageVersion("dplyr") >= "1.1.0") {
    dplyr::inner_join(x, y, by = by, relationship = "many-to-many", ...)
  } else dplyr::inner_join(x, y, by = by, ...)
}

# ------------------------ General helpers ---------------------------------
na_tokens <- c("(D)","(L)","(S)","(NA)","(N/A)","(X)","(T)","(C)","(Z)")
numify    <- function(x) readr::parse_number(x, na = na_tokens, locale = locale(grouping_mark = ","))
nf5       <- function(x) stringr::str_pad(as.character(x), 5, pad = "0")

# Strong county-equivalent filter: keep 50 states + DC; drop territories & US/state totals & 00xxx artifacts
is_real_county <- function(fips5) {
  fips5 <- nf5(fips5)
  st    <- substr(fips5, 1, 2)
  last3 <- substr(fips5, 3, 5)
  valid_states <- sprintf("%02d", c(1:56))
  territories  <- c("60","66","69","72","78") # AS, GU, MP, PR, VI
  is_state_ok  <- st %in% setdiff(valid_states, territories)
  not_state_total <- last3 != "000"
  not_us_total    <- fips5 != "00000"
  not_weird_000xx <- substr(fips5, 1, 2) != "00" # drops 00002, 00011, etc.
  is_state_ok & not_state_total & not_us_total & not_weird_000xx
}

# Clean & longify BEA table (CAGDP11 or CAGDP2)
longify_bea <- function(df, name) {
  yr_cols <- names(df)[grepl("^\\d{4}$", names(df))]
  df %>%
    mutate(
      GeoFIPS = nf5(str_extract(GeoFIPS, "\\d+")),
      GeoName = GeoName %>%
        str_replace_all("\\*", "") %>%
        str_replace_all("\\s*\\(.*?\\)", "") %>%
        str_squish(),
      Description = str_squish(Description),
      Unit        = str_squish(Unit)
    ) %>%
    filter(is_real_county(GeoFIPS)) %>%
    pivot_longer(all_of(yr_cols), names_to = "Year", values_to = "Value_raw") %>%
    mutate(
      Year  = as.integer(Year),
      Value = numify(Value_raw),
      Table = name
    ) %>%
    select(Table, GeoFIPS, GeoName, Year, Value, Region, LineCode,
           IndustryClassification, Description, Unit)
}

# ------------------------ Load BEA raw ------------------------------------
safe_unzip_pick <- function(zip_url, pattern, label) {
  zf <- tempfile(fileext = ".zip")
  download.file(zip_url, zf, mode = "wb", quiet = TRUE)
  lst <- try(unzip(zf, list = TRUE), silent = TRUE)
  stopifnot(!inherits(lst, "try-error"))
  pick <- lst$Name[grepl(pattern, lst$Name)][1]
  stopifnot(!is.na(pick))
  exd <- tempdir()
  unzip(zf, files = pick, exdir = exd)
  out <- read.csv(file.path(exd, pick), check.names = FALSE)
  out <- if (exists("fix_df")) fix_df(out) else tibble::as_tibble(out)
  dbg(out, paste0(label, " (raw)"))
  # Quick NA audit on a few key columns
  dbg_count_na(out, c("GeoFIPS","GeoName","Region","LineCode"), paste0(label, " (raw NA key cols)"))
  out
}

cagdp11_raw <- safe_unzip_pick(
  "https://apps.bea.gov/regional/zip/CAGDP11.zip",
  "^CAGDP11__ALL_AREAS_\\d{4}_\\d{4}\\.csv$",
  "CAGDP11"
)
cagdp2_raw <- safe_unzip_pick(
  "https://apps.bea.gov/regional/zip/CAGDP2.zip",
  "^CAGDP2__ALL_AREAS_\\d{4}_\\d{4}\\.csv$",
  "CAGDP2"
)

CAGDP11_LONG <- longify_bea(cagdp11_raw, "CAGDP11")
CAGDP2_LONG  <- longify_bea(cagdp2_raw,  "CAGDP2")
dbg(CAGDP11_LONG, "CAGDP11_LONG")
dbg_count_na(CAGDP11_LONG, c("Value","GeoName"), "CAGDP11_LONG key fields")
dbg(CAGDP2_LONG,  "CAGDP2_LONG")
dbg_count_na(CAGDP2_LONG, c("Value","GeoName"), "CAGDP2_LONG key fields")

# ------------------------ Crosswalks (HI / VA / CT) -----------------------
# Optional: detect a population column for VA/HI weighted splits.
detect_pop <- function() {
  cand <- list(
    if (exists("county_2020")) county_2020 else NULL,
    if (exists("us_communities")) us_communities else NULL,
    if (exists("geo")) geo else NULL
  ) %>% compact()
  for (df in cand) {
    nm <- names(df)
    fips_col <- nm[grepl("^fips$|^Fips$|^GEOID$|^GEOID_?fips$", nm, ignore.case = TRUE)][1]
    pop_col  <- nm[grepl("pop|population|P001001|TOT_POP", nm, ignore.case = TRUE)][1]
    if (!is.na(fips_col) && !is.na(pop_col)) {
      out <- df %>%
        transmute(fips = nf5(!!sym(fips_col)), pop = suppressWarnings(as.numeric(!!sym(pop_col)))) %>%
        group_by(fips) %>% summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop")
      if (nrow(out) > 0) return(out)
    }
  }
  NULL
}
pop_lookup <- detect_pop()
if (is.null(pop_lookup)) {
  warning("No population column found for VA/HI allocation. Falling back to equal splits.")
}

# HI: 15901 (Maui+Kalawao) → 15009 + 15005
hi_map <- tibble(
  from_fips = "15901",
  to_fips   = c("15009","15005"),
  alloc     = {
    if (!is.null(pop_lookup)) {
      pops <- pop_lookup %>% filter(fips %in% c("15009","15005")) %>% pull(pop)
      if (length(pops) == 2 && sum(pops, na.rm = TRUE) > 0) pops / sum(pops, na.rm = TRUE) else c(0.999, 0.001)
    } else c(0.999, 0.001)
  }
)

# VA: 519xx aggregates → components (pop-weighted when available)
va_combo_map <- tribble(
  ~from_fips, ~to_fips,
  "51901","51003", "51901","51540",
  "51903","51005", "51903","51580",
  "51907","51015", "51907","51790", "51907","51820",
  "51911","51031", "51911","51680",
  "51913","51035", "51913","51640",
  "51918","51053", "51918","51570", "51918","51730",
  "51919","51059", "51919","51600", "51919","51610",
  "51921","51069", "51921","51840",
  "51923","51081", "51923","51595",
  "51929","51089", "51929","51690",
  "51931","51095", "51931","51830",
  "51933","51121", "51933","51750",
  "51939","51143", "51939","51590",
  "51941","51149", "51941","51670",
  "51942","51153", "51942","51683", "51942","51685",
  "51944","51161", "51944","51775",
  "51945","51163", "51945","51530", "51945","51678",
  "51947","51165", "51947","51660",
  "51949","51175", "51949","51620",
  "51951","51177", "51951","51630",
  "51953","51191", "51953","51520",
  "51955","51195", "51955","51720",
  "51958","51199", "51958","51735"
) %>% group_by(from_fips) %>%
  mutate(alloc = {
    comps <- to_fips
    if (!is.null(pop_lookup)) {
      pops <- pop_lookup %>% filter(fips %in% comps) %>% pull(pop)
      if (length(pops) == length(comps) && sum(pops, na.rm = TRUE) > 0) pops / sum(pops, na.rm = TRUE)
      else rep(1/length(comps), length(comps))
    } else rep(1/length(comps), length(comps))
  }) %>% ungroup()

# CT: 091xx planning regions → 090xx counties (allocation factors provided)
if (!exists("ct_fips_changes_raw")) {
  stop("Required object `ct_fips_changes_raw` is missing. Provide the geocorr PR→County crosswalk.")
}
ct_pr_to_county <- ct_fips_changes_raw %>%
  transmute(
    from_fips = nf5(`Connecticut planning region`),
    to_fips   = nf5(`County code`),
    alloc     = as.numeric(`CTcounty-to-county allocation factor`)
  ) %>%
  filter(!is.na(alloc), alloc > 0)

dbg(hi_map,        "xwalk HI 15901 -> 15009/15005")
dbg(va_combo_map,  "xwalk VA 519xx -> components")
dbg(ct_pr_to_county,"xwalk CT PR -> counties")

# General-purpose expander; also try to fill names from BEA df_long and from `geo`
expand_by_xwalk <- function(df_long, xw, label) {
  before <- nrow(df_long)
  out <- df_long %>%
    mm_left_join(xw, by = c("GeoFIPS" = "from_fips")) %>%
    mutate(
      to_fips = if_else(is.na(to_fips), GeoFIPS, to_fips),
      alloc   = if_else(is.na(alloc),   1,       alloc),
      Value   = Value * alloc
    ) %>%
    group_by(Table, to_fips, Year, LineCode, IndustryClassification, Description, Unit) %>%
    summarise(Value = sum(Value, na.rm = TRUE), .groups = "drop")
  # Attach GeoName first from df_long (if available), then from `geo` as fallback
  out <- out %>%
    left_join(
      df_long %>% select(GeoFIPS, GeoName) %>% distinct(),
      by = c("to_fips" = "GeoFIPS")
    ) %>%
    rename(GeoFIPS = to_fips, GeoName = GeoName)
  # Fallback name fill from `geo` if available
  if (exists("geo")) {
    geo_norm_for_names <- {
      gtmp <- geo
      if ("CBSA Title" %in% names(gtmp) && !("CBSA.Title" %in% names(gtmp))) {
        gtmp <- gtmp %>% rename(`CBSA.Title` = `CBSA Title`)
      }
      gtmp
    }
    out <- out %>%
      mm_left_join(
        geo_norm_for_names %>% select(fips, GeoName) %>% mutate(fips = nf5(fips)) %>% distinct(),
        by = c("GeoFIPS" = "fips")
      ) %>%
      mutate(GeoName = coalesce(GeoName.x, GeoName.y)) %>%
      select(-GeoName.x, -GeoName.y)
  }
  after <- nrow(out)
  msg("%s: rows %s -> %s | missing GeoName now: %s",
      label, format(before, big.mark=","), format(after, big.mark=","), sum(is.na(out$GeoName)))
  out
}

expand_specials <- function(df_long) {
  out <- df_long
  out <- expand_by_xwalk(out, hi_map,         "HI expand")
  out <- expand_by_xwalk(out, va_combo_map,   "VA expand")
  out <- expand_by_xwalk(out, ct_pr_to_county,"CT expand")
  out
}

C11_EXP <- expand_specials(CAGDP11_LONG)
C02_EXP <- expand_specials(CAGDP2_LONG)
dbg(C11_EXP, "CAGDP11 (expanded)")
dbg_count_na(C11_EXP, c("GeoName","Value"), "CAGDP11 (expanded) key fields")
dbg(C02_EXP, "CAGDP2 (expanded)")
dbg_count_na(C02_EXP, c("GeoName","Value"), "CAGDP2 (expanded) key fields")

# Coverage sanity (shared years only)
overlap_years <- intersect(unique(C11_EXP$Year), unique(C02_EXP$Year))
M11 <- C11_EXP %>% filter(Year %in% overlap_years) %>% distinct(Year, GeoFIPS)
M02 <- C02_EXP %>% filter(Year %in% overlap_years) %>% distinct(Year, GeoFIPS)
missing_in_11 <- anti_join(M02, M11, by = c("Year","GeoFIPS"))
missing_in_02 <- anti_join(M11, M02, by = c("Year","GeoFIPS"))
if (nrow(missing_in_11) == 0 && nrow(missing_in_02) == 0) {
  msg("[OK] No CAGDP coverage discrepancies after expansions.")
} else {
  if (nrow(missing_in_11) > 0) { msg("[WARN] In CAGDP2 not in CAGDP11:"); print(missing_in_11 %>% count(Year, name="n")) }
  if (nrow(missing_in_02) > 0) { msg("[WARN] In CAGDP11 not in CAGDP2:"); print(missing_in_02 %>% count(Year, name="n")) }
}

# ------------------------ Legacy wide-ish raws (compat only) --------------
county_gdp_ind <- cagdp11_raw %>%
  mutate(
    GeoFIPS     = nf5(str_trim(GeoFIPS)),
    LineCode    = suppressWarnings(as.integer(LineCode)),
    Description = str_squish(Description),
    Unit        = str_squish(Unit)
  )
county_gdp_ind2 <- cagdp2_raw %>%
  mutate(
    GeoFIPS     = nf5(str_trim(GeoFIPS)),
    LineCode    = suppressWarnings(as.integer(LineCode)),
    Description = str_squish(Description),
    Unit        = str_squish(Unit)
  )
yr_cols_raw <- intersect(as.character(2001:2023), names(county_gdp_ind2))
county_gdp_ind2 <- county_gdp_ind2 %>%
  mutate(across(all_of(yr_cols_raw), numify)) %>%
  rename_with(~ paste0("X", .x), all_of(yr_cols_raw))

# ------------------------ Build clean county-level subset ------------------
needed_linecodes <- c(1, 10, 11, 13, 60) # total, utilities, construction, durable mfg, prof/sci/tech
C02_NEED <- C02_EXP %>% filter(LineCode %in% needed_linecodes)
msg("C02_NEED rows: %s", format(nrow(C02_NEED), big.mark=","))

# ------------------------ GEO NORMALIZATION (***FIXES YOUR ERROR***) -------
stopifnot(exists("geo"))
# Normalize geo naming so we can always use CBSA.Title, CBSA.Code, etc.
normalize_geo_columns <- function(g) {
  nm <- names(g)
  out <- g
  if ("CBSA Title" %in% nm && !("CBSA.Title" %in% nm)) out <- out %>% rename(`CBSA.Title` = `CBSA Title`)
  if ("CBSA Code" %in% nm && !("CBSA.Code" %in% nm))  out <- out %>% rename(`CBSA.Code` = `CBSA Code`)
  out
}
geo_norm <- normalize_geo_columns(geo)
msg("geo columns available: %s", paste(names(geo_norm), collapse=", "))

# Build available geographies from the columns that ACTUALLY exist
canonical_geogs <- c("State.Name","cd_119","PEA","CBSA.Title","GeoName")
avail_geographies <- intersect(canonical_geogs, names(geo_norm))
missing_geogs <- setdiff(canonical_geogs, avail_geographies)
msg("Using geographies: %s", paste(avail_geographies, collapse=", "))
if (length(missing_geogs)) msg("[INFO] Skipping missing geographies: %s", paste(missing_geogs, collapse=", "))

# Helper to robustly extract chr columns (works if list/data.frame columns appear)
extract_chr_col <- function(df, col) {
  x <- df[[col]]
  if (is.null(x)) return(NULL)
  if (is.list(x) && !is.atomic(x)) {
    purrr::map_chr(x, function(.x) {
      if (is.null(.x)) return(NA_character_)
      if (is.atomic(.x)) return(as.character(.x[1]))
      if (is.data.frame(.x)) {
        vals <- unlist(.x, use.names = FALSE); vals <- vals[!is.na(vals)]
        if (length(vals)) return(as.character(vals[1])) else return(NA_character_)
      }
      as.character(.x)
    })
  } else as.character(x)
}

# Build master target keys from geo_long if present; accept either (geo, geo_name) or (geo_type, geo_name)
get_geo_long_keys <- function() {
  if (exists("geo_long") && is.data.frame(geo_long)) {
    gl_cols <- names(geo_long)
    msg("geo_long present. Columns: %s", paste(gl_cols, collapse=", "))
    if (all(c("geo","geo_name") %in% gl_cols)) {
      g <- tibble(
        geo      = extract_chr_col(geo_long, "geo"),
        geo_name = extract_chr_col(geo_long, "geo_name")
      )
    } else if (all(c("geo_type","geo_name") %in% gl_cols)) {
      g <- tibble(
        geo      = extract_chr_col(geo_long, "geo_type"),
        geo_name = extract_chr_col(geo_long, "geo_name")
      )
    } else {
      msg("[WARN] geo_long lacks required name columns; falling back to `geo`.")
      g <- NULL
    }
    if (!is.null(g)) {
      g <- g %>%
        mutate(geo = str_squish(geo), geo_name = str_squish(geo_name)) %>%
        filter(!is.na(geo), !is.na(geo_name), geo != "", geo_name != "") %>%
        distinct() %>%
        mutate(.ord = row_number())
      return(g)
    }
  }
  # Fallback: derive a faithful long key set from `geo_norm` using the available geo columns
  g <- list()
  if ("State.Name" %in% avail_geographies) g$state <- geo_norm %>% filter(!is.na(State.Name))   %>% transmute(geo="State",                   geo_name=State.Name)
  if ("cd_119"     %in% avail_geographies) g$cd    <- geo_norm %>% filter(!is.na(cd_119))       %>% transmute(geo="Congressional District",  geo_name=cd_119)
  if ("PEA"        %in% avail_geographies) g$pea   <- geo_norm %>% filter(!is.na(PEA))          %>% transmute(geo="Economic Area",           geo_name=PEA)
  if ("CBSA.Title" %in% avail_geographies) g$cbsa  <- geo_norm %>% filter(!is.na(`CBSA.Title`)) %>% transmute(geo="Metro Area",              geo_name=`CBSA.Title`)
  if ("GeoName"    %in% avail_geographies) g$cty   <- geo_norm %>% filter(!is.na(GeoName))      %>% transmute(geo="County",                  geo_name=GeoName)
  g <- bind_rows(g) %>% distinct() %>% mutate(.ord = row_number())
  msg("[INFO] Derived geo_long_keys from `geo` (rows=%s).", nrow(g))
  g
}
geo_long_keys <- get_geo_long_keys()
dbg(geo_long_keys, "geo_long_keys (target keys)")

# ------------------------ County→geo mappings (unique pairs) ---------------
geo_key <- geo_norm %>%
  mutate(fips = nf5(fips)) %>%
  select(fips, all_of(avail_geographies), percent_district) %>%
  distinct()

make_mapping <- function(geog) {
  base <- geo_key %>% select(fips, !!sym(geog), percent_district) %>% distinct()
  if (geog == "cd_119") {
    out <- base %>% filter(!is.na(!!sym(geog))) %>%
      rename(geo_id = !!sym(geog), weight = percent_district) %>%
      mutate(weight = coalesce(as.numeric(weight), 0) / 100)
  } else {
    out <- base %>% filter(!is.na(!!sym(geog))) %>%
      rename(geo_id = !!sym(geog)) %>%
      mutate(weight = 1)
  }
  out %>% distinct(fips, geo_id, .keep_all = TRUE)
}
geo_maps <- setNames(lapply(avail_geographies, make_mapping), avail_geographies)

# Duplicate audit on maps
for (geog in names(geo_maps)) {
  dup_ct <- geo_maps[[geog]] %>% count(fips, geo_id) %>% filter(n > 1)
  msg("Map rows (%s): %s | duplicates: %s", geog, format(nrow(geo_maps[[geog]]), big.mark=","), nrow(dup_ct))
  if (nrow(dup_ct)) {
    msg("[WARN] Mapping duplicates for %s (showing up to 10)", geog)
    print(utils::head(dup_ct, 10))
  }
}

# ------------------------ Aggregate GDP by geography -----------------------
# Years needed for growth calcs; keep LineCode THROUGH aggregation
C02_NEED_YRS <- C02_NEED %>%
  filter(Year %in% c(2013, 2018, 2022, 2023)) %>%
  select(GeoFIPS, Year, LineCode, Description, Value)

# County×Line wide (for clean weighted sums)
C02_WIDE <- C02_NEED_YRS %>%
  mutate(Y = paste0("Y", Year)) %>%
  select(-Year) %>%
  pivot_wider(names_from = Y, values_from = Value) %>%
  rename(fips = GeoFIPS)

# NA→0 before weighting/summing; track how many we fill
na_before <- colSums(sapply(C02_WIDE %>% select(starts_with("Y")), is.na))
C02_WIDE <- C02_WIDE %>% mutate(across(starts_with("Y"), ~coalesce(., 0)))
msg("Filled NA→0 in C02_WIDE (Y-cols): %s", paste(paste(names(na_before), na_before, sep=":"), collapse=", "))
dbg(C02_WIDE, "C02_WIDE (county x line; Y2013/Y2018/Y2022/Y2023)")

# Aggregate with weights; keep LineCode (robust to text drift)
results_list <- list()
for (geog in avail_geographies) {
  mp <- geo_maps[[geog]]
  joined <- C02_WIDE %>% mm_inner_join(mp, by = "fips")
  msg("Join check (%s): joined rows=%s (C02_WIDE=%s, map=%s)",
      geog, format(nrow(joined), big.mark=","), format(nrow(C02_WIDE), big.mark=","), format(nrow(mp), big.mark=","))
  agg <- joined %>%
    group_by(geo_id, LineCode) %>%
    summarise(
      GDP13 = sum(Y2013 * weight, na.rm = TRUE),
      GDP18 = sum(Y2018 * weight, na.rm = TRUE),
      GDP22 = sum(Y2022 * weight, na.rm = TRUE),
      GDP23 = sum(Y2023 * weight, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(geo = dplyr::case_when(
      geog == "State.Name" ~ "State",
      geog == "cd_119"     ~ "Congressional District",
      geog == "PEA"        ~ "Economic Area",
      geog == "CBSA.Title" ~ "Metro Area",
      geog == "GeoName"    ~ "County",
      TRUE ~ geog
    )) %>%
    rename(geo_name = geo_id)
  msg("Agg (%s): %s rows", geog, format(nrow(agg), big.mark=","))
  results_list[[geog]] <- agg
}
county_gdp_industry <- bind_rows(results_list) %>% distinct()
dbg(county_gdp_industry, "county_gdp_industry (aggregated, by LineCode)")

# ------------------------ Growth + contribution shares --------------------
# Growth: if base==0 → 0 when both are 0, else NA-protected % change
safe_growth <- function(n1, n0) {
  out <- (n1 / dplyr::na_if(n0, 0) - 1) * 100
  out[is.nan(out) & n1 == 0 & n0 == 0] <- 0
  round(out, 1)
}

countygdp_industry2 <- county_gdp_industry %>%
  mutate(
    gdp_10yr = safe_growth(GDP23, GDP13),
    gdp_5yr  = safe_growth(GDP23, GDP18),
    gdp_1yr  = safe_growth(GDP23, GDP22)
  ) %>%
  group_by(geo, LineCode) %>%
  mutate(
    total_gdp_diff_2223 = sum(GDP23 - GDP22, na.rm = TRUE),
    gdp_22_23 = dplyr::if_else(total_gdp_diff_2223 == 0, 0,
                               round(((GDP23 - GDP22) / total_gdp_diff_2223) * 100, 1)),
    total_gdp_diff_1323 = sum(GDP23 - GDP13, na.rm = TRUE),
    gdp_13_23 = dplyr::if_else(total_gdp_diff_1323 == 0, 0,
                               round(((GDP23 - GDP13) / total_gdp_diff_1323) * 100, 1))
  ) %>%
  ungroup() %>%
  select(geo, geo_name, LineCode, gdp_10yr, gdp_5yr, gdp_1yr, GDP23, gdp_22_23, gdp_13_23)

dbg(countygdp_industry2, "countygdp_industry2 (growth + shares)")

# Contribution sanity: should sum ~100 within each (geo, LineCode)
contrib_check <- countygdp_industry2 %>%
  group_by(geo, LineCode) %>%
  summarise(sum_share_22_23 = round(sum(gdp_22_23, na.rm = TRUE), 1),
            sum_share_13_23 = round(sum(gdp_13_23, na.rm = TRUE), 1), .groups = "drop")
msg("Contribution check (first 12 rows):")
print(utils::head(contrib_check, 12))

# ------------------------ Wide tables for legacy outputs ------------------
# Map the specific LineCodes to output variable stems
line_to_var <- c(`1`="total", `13`="durable_man", `60`="prof_science_tech")

county_gdp_5yr <- countygdp_industry2 %>%
  filter(as.character(LineCode) %in% names(line_to_var)) %>%
  mutate(var = paste0(line_to_var[as.character(LineCode)], "_5yrgdp")) %>%
  select(geo, geo_name, var, gdp_5yr) %>%
  distinct() %>%
  pivot_wider(names_from = var, values_from = gdp_5yr)

county_gdp_contr <- countygdp_industry2 %>%
  filter(as.character(LineCode) %in% names(line_to_var)) %>%
  mutate(var = paste0(line_to_var[as.character(LineCode)], "_gdp_2223_contr")) %>%
  select(geo, geo_name, var, gdp_22_23) %>%
  distinct() %>%
  pivot_wider(names_from = var, values_from = gdp_22_23)

county_gdp_10yr <- countygdp_industry2 %>%
  filter(LineCode == 1) %>%
  transmute(geo, geo_name, total_gdp_10yr = gdp_10yr) %>%
  distinct()

county_gdp_1yr <- countygdp_industry2 %>%
  filter(LineCode == 1) %>%
  transmute(geo, geo_name, total_gdp_1yr = gdp_1yr) %>%
  distinct()

# ------------------------ Align to geo_long_keys & finalize ---------------
# Force EXACT key coverage + ordering to geo_long (or fallback) using .ord
county_gdp_ind_final <- geo_long_keys %>%
  select(geo, geo_name, .ord) %>%
  left_join(county_gdp_10yr,  by = c("geo","geo_name")) %>%
  left_join(county_gdp_1yr,   by = c("geo","geo_name")) %>%
  left_join(county_gdp_5yr,   by = c("geo","geo_name")) %>%
  left_join(county_gdp_contr, by = c("geo","geo_name")) %>%
  arrange(.ord) %>%
  select(-.ord)

dbg(county_gdp_ind_final, "county_gdp_ind_final (final)")

# Alignment checks vs target keys
missing_in_final <- anti_join(geo_long_keys %>% select(geo, geo_name),
                              county_gdp_ind_final %>% select(geo, geo_name) %>% distinct(),
                              by = c("geo","geo_name"))
extra_in_final   <- anti_join(county_gdp_ind_final %>% select(geo, geo_name) %>% distinct(),
                              geo_long_keys %>% select(geo, geo_name),
                              by = c("geo","geo_name"))

msg("Keys missing in final (should be 0): %s", nrow(missing_in_final))
if (nrow(missing_in_final)) print(utils::head(missing_in_final, 20))
msg("Extra keys in final (should be 0): %s", nrow(extra_in_final))
if (nrow(extra_in_final)) print(utils::head(extra_in_final, 20))

# NA diagnostics
dbg_count_na(
  county_gdp_ind_final,
  c("geo","geo_name","total_gdp_10yr","total_gdp_1yr",
    "total_5yrgdp","durable_man_5yrgdp","prof_science_tech_5yrgdp",
    "total_gdp_2223_contr","durable_man_gdp_2223_contr","prof_science_tech_gdp_2223_contr"),
  "county_gdp_ind_final -- columns"
)

# Focused diagnostics for potential sparse lines (esp. prof_science_tech)
na_prof <- county_gdp_ind_final %>%
  filter(is.na(prof_science_tech_5yrgdp)) %>%
  count(geo, name = "n_na") %>% arrange(desc(n_na))
msg("Top NA counts for prof_science_tech_5yrgdp by geo (first 10):")
print(utils::head(na_prof, 10))

# Sanity: confirm geo categories overlap between target keys and aggregates
cat_overlap <- setdiff(unique(geo_long_keys$geo), unique(county_gdp_industry$geo))
if (length(cat_overlap)) {
  msg("[WARN] geo categories in geo_long with no aggregates: %s", paste(cat_overlap, collapse=", "))
} else {
  msg("[OK] All geo categories in geo_long are covered by aggregates.")
}


================================================================================
# END gdp_by_industry.R
================================================================================


================================================================================
# BEGIN property_values.R
================================================================================

# ---- Property Values ----
county_property_values_raw <- suppressWarnings(
  read.csv(file.path(paths$raw_data, "county_property_values.csv"), check.names = FALSE)
) %>% fix_df()
dbg(county_property_values_raw, "county_property_values_raw")

# Create county_geoid column as character, padding with leading zeros if necessary, using GEOID as base
county_property_values_raw <- county_property_values_raw %>%
  mutate(GEOID = str_pad(as.character(GEOID), 5, pad = "0"))
dbg(county_property_values_raw, "county_property_values_raw (with padded GEOID)")

# helper to standardize any GEOID-like field to 5-char string
std_geoid <- function(x) stringr::str_pad(as.character(x), 5, pad = "0")

county_prop <- county_property_values_raw %>%
  # keep GEOID as 5-char character
  mutate(GEOID = std_geoid(GEOID)) %>%
  select(-NAME) %>%
  # make sure geo$fips is also 5-char character before joining
  left_join(
    geo %>% mutate(fips = std_geoid(fips)),
    by = c("GEOID" = "fips")
  ) %>%
  # keep TIGRIS GEOID as character; only ALAND needs numeric
  left_join(
    tigris_counties_2020_raw %>%
      sf::st_drop_geometry() %>%
      transmute(
        GEOID = std_geoid(GEOID),
        ALAND = as.numeric(ALAND)
      ),
    by = "GEOID"
  )

dbg(county_prop, "county_prop after joins")

results_list <- list()
for (geog in geographies) {
  prop <- if (geog == "cd_119") {
    county_prop %>%
      group_by(!!rlang::sym(geog)) %>%
      summarize(
        PropertyValueUSD = {
          x <- PropertyValueUSD
          w <- ALAND * percent_district / 100
          ok <- !is.na(x) & !is.na(w) & w > 0
          if (!any(ok)) NA_real_ else weighted.mean(x[ok], w[ok])
        },
        .groups = "drop"
      )
  } else {
    county_prop %>%
      group_by(!!rlang::sym(geog)) %>%
      distinct(!!rlang::sym(geog), PropertyValueUSD, ALAND, .keep_all = TRUE) %>%
      summarize(
        PropertyValueUSD = {
          x <- PropertyValueUSD
          w <- ALAND
          ok <- !is.na(x) & !is.na(w) & w > 0
          if (!any(ok)) NA_real_ else weighted.mean(x[ok], w[ok])
        },
        .groups = "drop"
      )
  }
  dbg(prop, paste0("prop aggregated at ", geog))
  results_list[[geog]] <- prop
}

# keep 'prop' numeric all the way through
prop <- dplyr::bind_rows(results_list) %>%
  mutate(
    geo = case_when(
      !is.na(State.Name) ~ "State",
      !is.na(cd_119)     ~ "Congressional District",
      !is.na(PEA)        ~ "Economic Area",
      !is.na(GeoName)    ~ "County",
      TRUE               ~ "Metro Area"
    ),
    geo_name = case_when(
      !is.na(State.Name) ~ State.Name,
      !is.na(cd_119)     ~ cd_119,
      !is.na(PEA)        ~ PEA,
      !is.na(GeoName)    ~ GeoName,
      TRUE               ~ "CBSA Title"
    )
  ) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(geo, geo_name, PropertyValueUSD) %>%
  # ensure NaN -> NA before ranking
  mutate(PropertyValueUSD = ifelse(is.nan(PropertyValueUSD), NA_real_, PropertyValueUSD)) %>%
  group_by(geo) %>%
  mutate(PropertyValue_Rank = dplyr::min_rank(dplyr::desc(PropertyValueUSD))) %>%
  ungroup()

# (Optional) export-friendly copy where you blank NA as strings
prop_export <- prop %>% mutate(
  across(where(is.numeric), ~ ifelse(is.na(.), "", as.character(.)))
)

dbg(prop, "prop (final)")

================================================================================
# END property_values.R
================================================================================


================================================================================
# BEGIN manshare_manpay_geo.R
================================================================================


# =====================================================================
# CBP 2023 pipeline → manshare + manpay_geo (aligned to geo_long)
# =====================================================================

# --- Utilities ---------------------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr); library(tidyr); library(stringr); library(sf)
})
options(dplyr.summarise.inform = FALSE)

soft_assert <- function(ok, msg) { if (!ok) message("⚠️ ", msg); invisible(ok) }
to_num      <- function(x) suppressWarnings(as.numeric(x))
norm_name   <- function(x) stringr::str_squish(x)
territory_state_fips <- c("60","66","69","72","78")  # AS, GU, MP, PR, VI

# Strip plain ASCII quotes added by some CSV/logging paths
strip_outer_quotes <- function(x) stringr::str_replace_all(x, '^"+|"+$', '')

# Remove C0/C1 control chars (keeps \t, \n if desired; here we drop all)
strip_control_bytes <- function(x) stringr::str_replace_all(x, "[\\x00-\\x1F\\x7F-\\x9F]", "")

# Classic single-pass mojibake fixes seen in Puerto Rico + misc.
fix_mojibake_simple <- function(x) {
  repl <- c(
    "Do√±a"      = "Doña",
    "Pe√±uelas"  = "Peñuelas",
    "A√±asco"    = "Añasco",
    "Mayag√ºez"  = "Mayagüez",
    "Rinc√ón"    = "Rincón",
    "Cata√±o"    = "Cataño",
    "Lo√≠za"     = "Loíza",
    "Manat√í"    = "Manatí",
    "R√≠o"       = "Río",
    "Germ√án"    = "Germán",
    # Common non‑PR offenders we’re seeing
    "Ca√±on"     = "Cañon",
    "Espa√±ola"  = "Española"
  )
  stringr::str_replace_all(x, repl)
}

# Attempt to repair "UTF‑8 seen as Latin‑1/Windows‑1252" by round‑tripping
fix_reencode_once <- function(x) {
  y1 <- suppressWarnings(iconv(iconv(x, from = "UTF-8", to = "latin1"),        from = "latin1",        to = "UTF-8"))
  y2 <- suppressWarnings(iconv(iconv(x, from = "UTF-8", to = "windows-1252"),  from = "windows-1252",  to = "UTF-8"))
  bad_ct <- function(s) stringr::str_count(s, "[ÃÂâ√]")
  choose <- ifelse(bad_ct(y2) < bad_ct(y1), y2, y1)
  choose[is.na(choose)] <- x[is.na(choose)]
  choose
}

# Full text fixer: quotes/control → re-encode (twice) → simple fixes → squish
fix_text <- function(x) {
  if (is.null(x)) return(x)
  x <- as.character(x)
  x <- strip_outer_quotes(x)
  x <- strip_control_bytes(x)
  x <- fix_reencode_once(x)
  x <- fix_reencode_once(x)
  x <- fix_mojibake_simple(x)
  x <- norm_name(x)
  x
}

# ASCII transliteration for compare keys
strip_diacritics <- function(x) { y <- suppressWarnings(iconv(x, to = "ASCII//TRANSLIT")); ifelse(is.na(y), x, y) }

# Compare key: always run through fix_text first
name_cmp <- function(x) norm_name(strip_diacritics(fix_text(x)))

# Format numeric to string with blanks for NA
fmt_num <- function(x, digits = NULL) {
  if (!is.null(digits) && is.numeric(x)) x <- round(x, digits)
  ifelse(is.na(x), "", as.character(x))
}

# Small helper for concise debug dumps
print_mismatches <- function(df, heading, nmax = 200) {
  cat("\n---", heading, "---\n")
  cat("Count:", nrow(df), "\n")
  if (nrow(df) > 0) {
    if (nrow(df) > nmax) {
      cat("(showing first", nmax, ")\n")
      print(df %>% transmute(geo_type, geo_name) %>% head(nmax), n=nmax)
    } else {
      print(df %>% transmute(geo_type, geo_name), n=Inf)
    }
  }
}

# Targeted text diagnostics
debug_text_issues <- function(df, label, col = "geo_name", n = 10) {
  v <- df[[col]]
  pat_bad <- "[ÃÂâ√]|\\\\u00"
  idx_bad <- which(stringr::str_detect(v, pat_bad))
  if (length(idx_bad) > 0) {
    cat("\n--- Text diagnostics:", label, "---\n")
    cat("Rows with suspected mojibake:", length(idx_bad), "\n")
    print(df[idx_bad, , drop=FALSE] %>% dplyr::select(any_of(col)) %>% head(n), n=Inf)
  }
}

# --- Robust column-detection helpers ------------------------------------------
norm_cols <- function(nms) gsub("[^a-z0-9]+", "", tolower(nms))

find_col <- function(df, candidates = character(), keywords = character(), label = "column", required = TRUE) {
  nms      <- names(df)
  nms_norm <- norm_cols(nms)
  
  # 1) exact match on normalized candidates
  for (cand in candidates) {
    c_norm <- norm_cols(cand)
    hit <- which(nms_norm == c_norm)
    if (length(hit) == 1) {
      cat("•", label, "→", nms[hit], "(exact)\n")
      return(nms[hit])
    }
  }
  # 2) partial match on candidates
  for (cand in candidates) {
    c_norm <- norm_cols(cand)
    hit <- which(stringr::str_detect(nms_norm, fixed(c_norm)))
    if (length(hit) == 1) {
      cat("•", label, "→", nms[hit], "(partial)\n")
      return(nms[hit])
    }
  }
  # 3) all-keyword containment (e.g., c("planning","region"))
  if (length(keywords)) {
    kw <- norm_cols(keywords)
    ok <- sapply(nms_norm, function(x) all(stringr::str_detect(x, kw)))
    if (sum(ok) == 1) {
      hit <- which(ok)
      cat("•", label, "→", nms[hit], "(keywords)\n")
      return(nms[hit])
    }
    # choose best score if multiple
    if (sum(ok) > 1) {
      scores <- sapply(nms_norm[ok], function(x) sum(stringr::str_detect(x, kw)))
      hit <- which(ok)[which.max(scores)]
      cat("•", label, "→", nms[hit], "(keywords-best)\n")
      return(nms[hit])
    }
  }
  # 4) failure path
  if (required) {
    cat("\n⚠️ Could not find", label, ". Available columns:\n")
    print(nms)
    stop(paste("Required", label, "not found. See column list above."))
  } else {
    cat("•", label, "not found (optional).\n")
    return(NA_character_)
  }
}

# --- Fetch CBP (fetch before use) ---------------------------------------------
cbp_2023_us_raw <- getCensus(name="cbp", vars=c("NAICS2017","PAYANN","EMP"),
                             region="us:*", vintage=2023)
if (exists("dbg")) dbg(cbp_2023_us_raw, "cbp_2023_us_raw")

cbp_2023_cnty_raw <- getCensus(name="cbp",
                               vars=c("STATE","COUNTY","NAICS2017","PAYANN","EMP"),
                               region="county:*", vintage=2023)
if (exists("dbg")) dbg(cbp_2023_cnty_raw, "cbp_2023_cnty_raw")

# --- 0) Lookups (STATE, COUNTY, CBSA, PEA, CD119) -----------------------------
# States (2024)
lkp_state <- tigris_states_2024_raw %>%
  transmute(state_fips = STATEFP,
            state_abbr = STUSPS,
            State.Name = fix_text(NAME))

# Counties (2020 geog, drop geometry)
lkp_county <- tigris_counties_2020_raw %>%
  sf::st_drop_geometry() %>%
  transmute(
    county_geoid = GEOID,
    county_name  = fix_text(NAMELSAD),
    state_fips   = substr(GEOID, 1, 2)
  )

# CBSA + PEA (county base 2020; titles will be swapped to tigris NAME)
lkp_cbsa_pea <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
  transmute(county_geoid = COUNTY_GEOID,
            CBSA.Code   = CBSA_GEOID,
            CBSA.Title  = fix_text(CBSA_NAME),
            PEA         = fix_text(PEA_NAME),
            PEA_NO      = PEA_NUMBER)

# Authoritative CBSA names from tigris
lkp_cbsa_names <- tigris_cbsa_2020_raw %>%
  sf::st_drop_geometry() %>%
  transmute(CBSA.Code = GEOID, CBSA.Name = fix_text(NAME))

# Early CBSA name QA (should catch Cañon/Española here)
debug_text_issues(lkp_cbsa_names, "lkp_cbsa_names (raw fixed)", "CBSA.Name", n=10)

# County→CD119 factors (national GeoCorr)
lkp_cd119_nat <- geocorr_county_2020_cd_119 %>%
  transmute(
    county_geoid = COUNTY_GEOID,
    CD119_code   = CD119_code,
    state_fips   = State_code,
    co_to_cd119  = `county-to-cd119 allocation factor`
  ) %>%
  mutate(cd_119 = paste0(lkp_state$state_abbr[match(state_fips, lkp_state$state_fips)],
                         "-", str_pad(CD119_code, 2, pad = "0")))

# Connecticut-specific county (legacy) → CD119 bridge
cat("\n--- Detecting column names in geocorr_ct_county_cd_119 ---\n")
gc_ct_names <- names(geocorr_ct_county_cd_119); print(gc_ct_names)

gc_ct_col_county   <- find_col(geocorr_ct_county_cd_119,
                               candidates = c("County code","County Code","County_code","County"),
                               keywords   = c("county","code"),
                               label = "CT county code")
gc_ct_col_cd       <- find_col(geocorr_ct_county_cd_119,
                               candidates = c("Congressional district code (119th Congress)",
                                              "CD119_code","cd119","CongressionalDistrictCode119"),
                               keywords   = c("congressional","119"),
                               label = "CD119 code")
gc_ct_col_alloc    <- find_col(geocorr_ct_county_cd_119,
                               candidates = c("CTcounty-to-cd119 allocation factor",
                                              "ctcounty to cd119 allocation factor"),
                               keywords   = c("allocation","cd119"),
                               label = "CTcounty→CD119 allocation factor")

lkp_cd119_ct <- geocorr_ct_county_cd_119 %>%
  transmute(
    county_geoid = .data[[gc_ct_col_county]],
    CD119_code   = .data[[gc_ct_col_cd]],
    co_to_cd119  = to_num(.data[[gc_ct_col_alloc]]),
    cd_119       = paste0("CT-", str_pad(CD119_code, 2, pad = "0"))
  )

# Prefer CT-specific rows over national
lkp_cd119 <- bind_rows(
  lkp_cd119_nat %>% filter(!startsWith(county_geoid, "09")),
  lkp_cd119_ct
) %>%
  filter(!is.na(co_to_cd119), co_to_cd119 > 0)

# --- 1) US manufacturing share for LQ baseline --------------------------------
manshare_us <- cbp_2023_us_raw %>%
  filter(NAICS2017 %in% c("31-33","00")) %>%
  select(-PAYANN) %>%
  tidyr::pivot_wider(names_from = NAICS2017, values_from = EMP) %>%
  mutate(man_share_us = ifelse(as.numeric(`00`) > 0,
                               as.numeric(`31-33`) / as.numeric(`00`) * 100,
                               NA_real_)) %>%
  transmute(man_share_us = round(man_share_us, 1))
man_share_us_val <- manshare_us$man_share_us
if (!is.finite(man_share_us_val) || is.na(man_share_us_val)) man_share_us_val <- 9

# --- 2) CBP 2023 county GEOIDs; drop territories in CBP -----------------------
cbp_cnty <- cbp_2023_cnty_raw %>%
  mutate(county_geoid = paste0(str_pad(STATE,  2, pad = "0"),
                               str_pad(COUNTY, 3, pad = "0"))) %>%
  filter(!substr(county_geoid, 1, 2) %in% territory_state_fips)

# --- 3) CT PR (2023) → legacy counties (2020) reallocation --------------------
cat("\n--- Detecting column names in ct_fips_changes_raw ---\n")
ct_names <- names(ct_fips_changes_raw); print(ct_names)

ct_col_county   <- find_col(ct_fips_changes_raw,
                            candidates = c("County code","County Code","County_code","County"),
                            keywords   = c("county","code"),
                            label = "Legacy County code (2020)")
ct_col_pr       <- find_col(ct_fips_changes_raw,
                            candidates = c("Connecticut planning region","Planning region code","Planning region"),
                            keywords   = c("planning","region"),
                            label = "CT Planning Region code (2023)")
ct_col_pr2ct    <- find_col(ct_fips_changes_raw,
                            candidates = c("county-to-CTcounty allocation factor","county to ctcounty allocation factor"),
                            keywords   = c("county","ctcounty","allocation"),
                            label = "County→CTcounty allocation factor")
ct_col_ct2pr    <- find_col(ct_fips_changes_raw,
                            candidates = c("CTcounty-to-county allocation factor","ctcounty to county allocation factor"),
                            keywords   = c("ctcounty","county","allocation"),
                            label = "CTcounty→County allocation factor",
                            required = FALSE)

# Build & normalize CT weights (defensive)
ct_alloc_raw <- ct_fips_changes_raw %>%
  transmute(
    ctcounty_geoid = stringr::str_pad(as.character(.data[[ct_col_county]]), 5, pad = "0"),
    pr_geoid       = stringr::str_pad(as.character(.data[[ct_col_pr]]),     5, pad = "0"),
    w_pr_to_ct     = to_num(.data[[ct_col_pr2ct]]),             # PR→County
    w_ct_to_pr     = if (!is.na(ct_col_ct2pr)) to_num(.data[[ct_col_ct2pr]]) else NA_real_  # County→PR (optional)
  )

# Show sample of CT alloc table for sanity
cat("\n--- ct_alloc_raw sample ---\n"); print(head(ct_alloc_raw, 10), n=10)

ct_map <- ct_alloc_raw %>%
  mutate(
    w_ct_to_pr = tidyr::replace_na(w_ct_to_pr, 0),
    w_pr_to_ct = tidyr::replace_na(w_pr_to_ct, 0)
  ) %>%
  group_by(pr_geoid) %>%
  mutate(w_pr_to_ct = if (sum(w_pr_to_ct) > 0) w_pr_to_ct / sum(w_pr_to_ct) else w_pr_to_ct) %>%
  ungroup() %>%
  group_by(ctcounty_geoid) %>%
  mutate(w_ct_to_pr = if (sum(w_ct_to_pr) > 0) w_ct_to_pr / sum(w_ct_to_pr) else w_ct_to_pr) %>%
  ungroup()

qa_pr <- ct_map %>% group_by(pr_geoid)       %>% summarise(s = sum(w_pr_to_ct), .groups="drop")
qa_ct <- ct_map %>% group_by(ctcounty_geoid) %>% summarise(s = sum(w_ct_to_pr), .groups="drop")
soft_assert(all(abs(qa_pr$s - 1) < 1e-6), "CT PR→County weights do not sum to 1 (after normalization).")
soft_assert(all(abs(qa_ct$s - 1) < 1e-6), "CT County→PR weights do not sum to 1 (after normalization).")

reallocate_ct_pr_to_counties <- function(cbp_df, ct_map) {
  stopifnot(all(c("STATE","COUNTY","NAICS2017","PAYANN","EMP","county_geoid") %in% names(cbp_df)))
  pr_keys    <- unique(ct_map$pr_geoid)
  cbp_ct_pr  <- cbp_df %>% filter(county_geoid %in% pr_keys)
  cbp_non_ct <- cbp_df %>% filter(!county_geoid %in% pr_keys)
  if (nrow(cbp_ct_pr) == 0L) return(cbp_df)
  
  before <- cbp_ct_pr %>% group_by(NAICS2017) %>%
    summarise(PAYANN = sum(PAYANN, na.rm=TRUE), EMP = sum(EMP, na.rm=TRUE), .groups="drop")
  
  cbp_ct_alloc <- cbp_ct_pr %>%
    inner_join(ct_map %>% select(pr_geoid, ctcounty_geoid, w_pr_to_ct),
               by = c("county_geoid" = "pr_geoid"), relationship = "many-to-many") %>%
    mutate(PAYANN = PAYANN * w_pr_to_ct,
           EMP    = EMP    * w_pr_to_ct,
           STATE        = substr(ctcounty_geoid, 1, 2),
           COUNTY       = substr(ctcounty_geoid, 3, 5),
           county_geoid = ctcounty_geoid) %>%
    group_by(STATE, COUNTY, county_geoid, NAICS2017) %>%
    summarise(PAYANN = sum(PAYANN, na.rm=TRUE),
              EMP    = sum(EMP,    na.rm=TRUE), .groups="drop")
  
  after <- cbp_ct_alloc %>% group_by(NAICS2017) %>%
    summarise(PAYANN = sum(PAYANN, na.rm=TRUE), EMP = sum(EMP, na.rm=TRUE), .groups="drop")
  
  chk <- before %>% left_join(after, by="NAICS2017", suffix=c("_b","_a")) %>%
    mutate(dP = PAYANN_a - PAYANN_b, dE = EMP_a - EMP_b)
  soft_assert(all(abs(chk$dP) < 1e-4 & abs(chk$dE) < 1e-4),
              "CT totals changed after PR→county reallocation.")
  
  bind_rows(cbp_non_ct, cbp_ct_alloc) %>% arrange(STATE, COUNTY, NAICS2017)
}
cbp_2023 <- reallocate_ct_pr_to_counties(cbp_cnty, ct_map)

# Guard: no CT PR rows remain
pr_geoids     <- unique(ct_map$pr_geoid)
leftover_prs  <- cbp_2023 %>% filter(county_geoid %in% pr_geoids) %>% distinct(county_geoid)
soft_assert(nrow(leftover_prs) == 0, paste("Some CT PR GEOIDs remain:", paste(leftover_prs$county_geoid, collapse=", ")))

# --- 4) County metrics ---------------------------------------------------------
cbp_cnty_00 <- cbp_2023 %>%
  filter(NAICS2017 == "00") %>%
  select(county_geoid, EMP00 = EMP)

cbp_cnty_mfg <- cbp_2023 %>%
  filter(NAICS2017 == "31-33") %>%
  select(county_geoid, PAYANN_mfg = PAYANN, EMP_mfg = EMP) %>%
  mutate(worker_pay = if_else(EMP_mfg > 0, PAYANN_mfg / EMP_mfg * 1000, NA_real_))

county_base <- lkp_county %>%
  left_join(lkp_state, by = "state_fips") %>%
  select(county_geoid, county_name, State.Name)

county_metrics <- county_base %>%
  left_join(cbp_cnty_00,  by = "county_geoid") %>%
  left_join(cbp_cnty_mfg, by = "county_geoid") %>%
  mutate(
    man_share_raw = dplyr::case_when(
      is.na(EMP00) ~ NA_real_,
      !is.na(EMP00) & (is.na(EMP_mfg) | EMP_mfg == 0) ~ 0,
      EMP00 > 0 ~ round(EMP_mfg / EMP00 * 100, 1),
      TRUE ~ NA_real_
    ),
    worker_pay = if_else(EMP_mfg > 0, PAYANN_mfg / EMP_mfg * 1000, NA_real_)
  ) %>%
  mutate(county_name = fix_text(county_name),
         State.Name  = fix_text(State.Name)) %>%
  filter(!is.na(EMP00))   # keep only counties with CBP "00" rows

# --- 5) CD119 rollup (county→CD weights; includes CT) -------------------------
cd119_emp <- lkp_cd119 %>%
  inner_join(cbp_cnty_00,  by = "county_geoid") %>%
  inner_join(cbp_cnty_mfg, by = "county_geoid") %>%
  mutate(EMP00_w   = EMP00   * co_to_cd119,
         EMP_mfg_w = EMP_mfg * co_to_cd119,
         PAY_w     = PAYANN_mfg * co_to_cd119) %>%
  group_by(cd_119) %>%
  summarise(EMP00   = sum(EMP00_w,   na.rm=TRUE),
            EMP_mfg = sum(EMP_mfg_w, na.rm=TRUE),
            PAY_mfg = sum(PAY_w,     na.rm=TRUE), .groups="drop") %>%
  mutate(
    man_share = dplyr::case_when(
      is.na(EMP00) ~ NA_real_,
      !is.na(EMP00) & (is.na(EMP_mfg) | EMP_mfg == 0) ~ 0,
      EMP00 > 0 ~ round(EMP_mfg / EMP00 * 100, 1),
      TRUE ~ NA_real_
    ),
    man_pay = if_else(EMP_mfg > 0, round(PAY_mfg / EMP_mfg * 1000, 2), NA_real_)
  )

# --- 6) State / CBSA / PEA rollups --------------------------------------------
# State (exclude territories for output parity with geo_long)
state_metrics <- county_metrics %>%
  mutate(state_fips = substr(county_geoid, 1, 2)) %>%
  filter(!state_fips %in% territory_state_fips) %>%
  group_by(state_fips) %>%
  summarise(EMP00 = sum(EMP00, na.rm=TRUE),
            EMP_mfg = sum(EMP_mfg, na.rm=TRUE),
            PAY_mfg = sum(PAYANN_mfg, na.rm=TRUE), .groups="drop") %>%
  left_join(lkp_state, by="state_fips") %>%
  mutate(
    man_share = dplyr::case_when(
      is.na(EMP00) ~ NA_real_,
      !is.na(EMP00) & (is.na(EMP_mfg) | EMP_mfg == 0) ~ 0,
      EMP00 > 0 ~ round(EMP_mfg / EMP00 * 100, 1),
      TRUE ~ NA_real_
    ),
    man_pay = if_else(EMP_mfg > 0, round(PAY_mfg / EMP_mfg * 1000, 2), NA_real_)
  ) %>%
  select(State.Name, man_share, man_pay)

# CBSA (join to tigris names for exact title alignment with geo_long)
cbsa_metrics <- county_metrics %>%
  inner_join(lkp_cbsa_pea %>% filter(!is.na(CBSA.Code)), by = "county_geoid") %>%
  group_by(CBSA.Code) %>%
  summarise(EMP00   = sum(EMP00,     na.rm=TRUE),
            EMP_mfg = sum(EMP_mfg,   na.rm=TRUE),
            PAY_mfg = sum(PAYANN_mfg,na.rm=TRUE), .groups="drop") %>%
  left_join(lkp_cbsa_names, by = "CBSA.Code") %>%
  mutate(
    CBSA.Name = fix_text(CBSA.Name),
    man_share = dplyr::case_when(
      is.na(EMP00) ~ NA_real_,
      !is.na(EMP00) & (is.na(EMP_mfg) | EMP_mfg == 0) ~ 0,
      EMP00 > 0 ~ round(EMP_mfg / EMP00 * 100, 1),
      TRUE ~ NA_real_
    ),
    man_pay = if_else(EMP_mfg > 0, round(PAY_mfg / EMP_mfg * 1000, 2), NA_real_)
  )

# PEA
pea_metrics <- county_metrics %>%
  inner_join(lkp_cbsa_pea %>% filter(!is.na(PEA)), by = "county_geoid") %>%
  group_by(PEA) %>%
  summarise(EMP00   = sum(EMP00,     na.rm=TRUE),
            EMP_mfg = sum(EMP_mfg,   na.rm=TRUE),
            PAY_mfg = sum(PAYANN_mfg,na.rm=TRUE), .groups="drop") %>%
  mutate(
    PEA      = fix_text(PEA),
    man_share = dplyr::case_when(
      is.na(EMP00) ~ NA_real_,
      !is.na(EMP00) & (is.na(EMP_mfg) | EMP_mfg == 0) ~ 0,
      EMP00 > 0 ~ round(EMP_mfg / EMP00 * 100, 1),
      TRUE ~ NA_real_
    ),
    man_pay = if_else(EMP_mfg > 0, round(PAY_mfg / EMP_mfg * 1000, 2), NA_real_)
  )

# County output (ensure "<County>, <State>")
county_out <- county_metrics %>%
  transmute(
    GeoName   = fix_text(paste0(county_name, ", ", State.Name)),
    man_share = man_share_raw,
    man_pay   = round(worker_pay, 2)
  )

# --- 7) Assemble numeric outputs ----------------------------------------------
manpay_geo_num <- bind_rows(
  state_metrics %>% transmute(geo="State",                  geo_name=fix_text(State.Name),  man_pay),
  cd119_emp     %>% transmute(geo="Congressional District", geo_name=fix_text(cd_119),      man_pay),
  pea_metrics   %>% transmute(geo="Economic Area",          geo_name=fix_text(PEA),         man_pay),
  county_out    %>% transmute(geo="County",                 geo_name=fix_text(GeoName),     man_pay),
  cbsa_metrics  %>% transmute(geo="Metro Area",             geo_name=fix_text(CBSA.Name),   man_pay)
) %>%
  filter(!is.na(geo_name), geo_name != "") %>%
  group_by(geo) %>% mutate(Manpay_rank = rank(-tidyr::replace_na(man_pay, -Inf), ties.method="min")) %>%
  ungroup()

manshare_num <- bind_rows(
  state_metrics %>% transmute(geo="State",                  geo_name=fix_text(State.Name), man_share),
  cd119_emp     %>% transmute(geo="Congressional District", geo_name=fix_text(cd_119),     man_share),
  pea_metrics   %>% transmute(geo="Economic Area",          geo_name=fix_text(PEA),        man_share),
  county_out    %>% transmute(geo="County",                 geo_name=fix_text(GeoName),    man_share),
  cbsa_metrics  %>% transmute(geo="Metro Area",             geo_name=fix_text(CBSA.Name),  man_share)
) %>%
  filter(!is.na(geo_name), geo_name != "") %>%
  group_by(geo) %>% mutate(Manshare_rank = rank(-tidyr::replace_na(man_share, -Inf), ties.method="min")) %>%
  ungroup() %>%
  mutate(manufacturing_lq = if_else(!is.na(man_share),
                                    round(man_share / man_share_us_val, 1),
                                    NA_real_))

# --- 8) Convert NA → blanks ("") for final emission ----------------------------
manpay_geo <- manpay_geo_num %>%
  mutate(
    geo_name    = fix_text(geo_name),
    man_pay     = fmt_num(man_pay, 2),
    Manpay_rank = ifelse(is.na(Manpay_rank), "", as.character(Manpay_rank))
  )

manshare <- manshare_num %>%
  mutate(
    geo_name         = fix_text(geo_name),
    man_share        = fmt_num(man_share, 1),
    Manshare_rank    = ifelse(is.na(Manshare_rank), "", as.character(Manshare_rank)),
    manufacturing_lq = fmt_num(manufacturing_lq, 1)
  )

# --- 9) Canonicalize tricky county name once (Doña Ana, NM) -------------------
canon_nm <- "Doña Ana County, New Mexico"
normalize_nm <- function(df) {
  df %>% mutate(
    geo_name = dplyr::if_else(geo == "County" & name_cmp(geo_name) == name_cmp(canon_nm),
                              canon_nm, geo_name)
  )
}
manpay_geo <- manpay_geo %>% normalize_nm() %>% distinct(geo, geo_name, .keep_all = TRUE)
manshare   <- manshare   %>% normalize_nm() %>% distinct(geo, geo_name, .keep_all = TRUE)

# Additional quick checks for common offenders in outputs
debug_text_issues(manpay_geo, "manpay_geo (post-format)")
debug_text_issues(manshare,   "manshare   (post-format)")

# --- 10) Alignment report + backfill for display parity with geo_long ----------
geo_long_norm <- geo_long %>%
  mutate(
    geo_type      = norm_name(geo_type),
    geo_name      = fix_text(geo_name),
    geo_name_norm = norm_name(geo_name),
    geo_name_cmp  = name_cmp(geo_name)
  )

out_states <- manpay_geo %>%
  filter(geo=="State") %>% transmute(geo_type="State",  geo_name, geo_name_cmp = name_cmp(geo_name))
out_cds    <- manshare   %>%
  filter(geo=="Congressional District") %>% transmute(geo_type="Congressional District", geo_name, geo_name_cmp = name_cmp(geo_name))
out_pea    <- manpay_geo %>%
  filter(geo=="Economic Area") %>% transmute(geo_type="Economic Area", geo_name, geo_name_cmp = name_cmp(geo_name))
out_cnty   <- manshare   %>%
  filter(geo=="County") %>% transmute(geo_type="County", geo_name, geo_name_cmp = name_cmp(geo_name))
out_cbsa   <- manpay_geo %>%
  filter(geo=="Metro Area") %>% transmute(geo_type="Metro Area", geo_name, geo_name_cmp = name_cmp(geo_name))

not_found_states <- anti_join(geo_long_norm %>% filter(geo_type=="State")                  %>% select(geo_type, geo_name, geo_name_cmp),
                              out_states %>% select(geo_type, geo_name_cmp), by=c("geo_type","geo_name_cmp"))
not_found_cds    <- anti_join(geo_long_norm %>% filter(geo_type=="Congressional District") %>% select(geo_type, geo_name, geo_name_cmp),
                              out_cds    %>% select(geo_type, geo_name_cmp), by=c("geo_type","geo_name_cmp"))
not_found_pea    <- anti_join(geo_long_norm %>% filter(geo_type=="Economic Area")          %>% select(geo_type, geo_name, geo_name_cmp),
                              out_pea    %>% select(geo_type, geo_name_cmp), by=c("geo_type","geo_name_cmp"))
not_found_cnty   <- anti_join(geo_long_norm %>% filter(geo_type=="County")                 %>% select(geo_type, geo_name, geo_name_cmp),
                              out_cnty   %>% select(geo_type, geo_name_cmp), by=c("geo_type","geo_name_cmp"))
not_found_cbsa   <- anti_join(geo_long_norm %>% filter(geo_type=="Metro Area")             %>% select(geo_type, geo_name, geo_name_cmp),
                              out_cbsa   %>% select(geo_type, geo_name_cmp), by=c("geo_type","geo_name_cmp"))

cat("\n===== Alignment report: geo_long items with NO output (post-backfill) =====\n")
print_mismatches(not_found_states, "States")
print_mismatches(not_found_cds,    "Congressional Districts")
print_mismatches(not_found_pea,    "Economic Areas (PEA)")
print_mismatches(not_found_cnty,   "Counties")
print_mismatches(not_found_cbsa,   "Metro Areas (CBSA)")

# Backfill (display only) missing COUNTY/CBSA rows with blanks
if (nrow(not_found_cnty) > 0) {
  manpay_geo <- bind_rows(
    manpay_geo,
    not_found_cnty %>% transmute(geo="County", geo_name, man_pay="", Manpay_rank="")
  )
  manshare <- bind_rows(
    manshare,
    not_found_cnty %>% transmute(geo="County", geo_name, man_share="", Manshare_rank="", manufacturing_lq="")
  )
}
if (nrow(not_found_cbsa) > 0) {
  manpay_geo <- bind_rows(
    manpay_geo,
    not_found_cbsa %>% transmute(geo="Metro Area", geo_name, man_pay="", Manpay_rank="")
  )
  manshare <- bind_rows(
    manshare,
    not_found_cbsa %>% transmute(geo="Metro Area", geo_name, man_share="", Manshare_rank="", manufacturing_lq="")
  )
}

# Dedup any accidental duplicates after backfill
manpay_geo <- manpay_geo %>% distinct(geo, geo_name, .keep_all = TRUE)
manshare   <- manshare   %>% distinct(geo, geo_name, .keep_all = TRUE)

# Recompute alignment POST-BACKFILL for clean report (cmp-based only)
out_states <- manpay_geo %>% filter(geo=="State") %>% transmute(geo_type="State",  geo_name, geo_name_cmp = name_cmp(geo_name))
out_cds    <- manshare   %>% filter(geo=="Congressional District") %>% transmute(geo_type="Congressional District", geo_name, geo_name_cmp = name_cmp(geo_name))
out_pea    <- manpay_geo %>% filter(geo=="Economic Area") %>% transmute(geo_type="Economic Area", geo_name, geo_name_cmp = name_cmp(geo_name))
out_cnty   <- manshare   %>% filter(geo=="County") %>% transmute(geo_type="County", geo_name, geo_name_cmp = name_cmp(geo_name))
out_cbsa   <- manpay_geo %>% filter(geo=="Metro Area") %>% transmute(geo_type="Metro Area", geo_name, geo_name_cmp = name_cmp(geo_name))

not_found_states <- anti_join(geo_long_norm %>% filter(geo_type=="State")                  %>% select(geo_type, geo_name, geo_name_cmp),
                              out_states %>% select(geo_type, geo_name_cmp), by=c("geo_type","geo_name_cmp"))
not_found_cds    <- anti_join(geo_long_norm %>% filter(geo_type=="Congressional District") %>% select(geo_type, geo_name, geo_name_cmp),
                              out_cds    %>% select(geo_type, geo_name_cmp), by=c("geo_type","geo_name_cmp"))
not_found_pea    <- anti_join(geo_long_norm %>% filter(geo_type=="Economic Area")          %>% select(geo_type, geo_name, geo_name_cmp),
                              out_pea    %>% select(geo_type, geo_name_cmp), by=c("geo_type","geo_name_cmp"))
not_found_cnty   <- anti_join(geo_long_norm %>% filter(geo_type=="County")                 %>% select(geo_type, geo_name, geo_name_cmp),
                              out_cnty   %>% select(geo_type, geo_name_cmp), by=c("geo_type","geo_name_cmp"))
not_found_cbsa   <- anti_join(geo_long_norm %>% filter(geo_type=="Metro Area")             %>% select(geo_type, geo_name, geo_name_cmp),
                              out_cbsa   %>% select(geo_type, geo_name_cmp), by=c("geo_type","geo_name_cmp"))

cat("\n===== Output-only items (present in outputs but NOT in geo_long) — cmp-based =====\n")
only_out_states <- anti_join(out_states %>% select(geo_type, geo_name, geo_name_cmp),
                             geo_long_norm %>% filter(geo_type=="State") %>% select(geo_type, geo_name_cmp),
                             by=c("geo_type","geo_name_cmp")) %>% select(geo_type, geo_name)
only_out_cds    <- anti_join(out_cds    %>% select(geo_type, geo_name, geo_name_cmp),
                             geo_long_norm %>% filter(geo_type=="Congressional District") %>% select(geo_type, geo_name_cmp),
                             by=c("geo_type","geo_name_cmp")) %>% select(geo_type, geo_name)
only_out_pea    <- anti_join(out_pea    %>% select(geo_type, geo_name, geo_name_cmp),
                             geo_long_norm %>% filter(geo_type=="Economic Area") %>% select(geo_type, geo_name_cmp),
                             by=c("geo_type","geo_name_cmp")) %>% select(geo_type, geo_name)
only_out_cnty   <- anti_join(out_cnty   %>% select(geo_type, geo_name, geo_name_cmp),
                             geo_long_norm %>% filter(geo_type=="County") %>% select(geo_type, geo_name_cmp),
                             by=c("geo_type","geo_name_cmp")) %>% select(geo_type, geo_name)
only_out_cbsa   <- anti_join(out_cbsa   %>% select(geo_type, geo_name, geo_name_cmp),
                             geo_long_norm %>% filter(geo_type=="Metro Area") %>% select(geo_type, geo_name_cmp),
                             by=c("geo_type","geo_name_cmp")) %>% select(geo_type, geo_name)

print_mismatches(only_out_states, "States")
print_mismatches(only_out_cds,    "Congressional Districts")
print_mismatches(only_out_pea,    "Economic Areas (PEA)")
print_mismatches(only_out_cnty,   "Counties")
print_mismatches(only_out_cbsa,   "Metro Areas (CBSA)")

# Targeted check for problem names in outputs
cat("\n===== Targeted name checks (should show proper UTF-8) =====\n")
cat("Doña Ana (any):\n"); print((manshare$geo_name[manshare$geo=="County"] %>% unique())[stringr::str_detect((manshare$geo_name[manshare$geo=="County"] %>% unique()), "Do")][1:5])
cat("Cañon / Canon City (Metro Area):\n"); print((manpay_geo$geo_name[manpay_geo$geo=="Metro Area"] %>% unique())[stringr::str_detect((manpay_geo$geo_name[manpay_geo$geo=="Metro Area"] %>% unique()), "Canon|Cañon")][1:5])
cat("Española / Espanola (Metro Area):\n"); print((manpay_geo$geo_name[manpay_geo$geo=="Metro Area"] %>% unique())[stringr::str_detect((manpay_geo$geo_name[manpay_geo$geo=="Metro Area"] %>% unique()), "Espan|Españ")][1:5])

# --- 11) Extra QA (succinct) ---------------------------------------------------
cat("\n=== Quick QA ===\n")
cat("US man_share_us_val:", man_share_us_val, "\n")
cat("Output counts by geo:\n")
print(manpay_geo %>% count(geo) %>% arrange(desc(n)))
cat("Distinct CDs in output:",
    manshare %>% filter(geo == "Congressional District") %>% dplyr::pull(geo_name) %>% dplyr::n_distinct(),
    "\n")

# --- Final debug prints (if dbg() exists) -------------------------------------
if (exists("dbg")) {
  dbg(manpay_geo, "manpay_geo (final)")
  dbg(manshare,   "manshare (final)")
}

================================================================================
# END manshare_manpay_geo.R
================================================================================


================================================================================
# BEGIN supplycurve_geo.R
================================================================================

solar_lcoe_county_raw <- suppressWarnings(
  read.csv(file.path(paths$raw_data, "solar_lcoe_county.csv"), check.names = FALSE)
) %>% apply_fix_df()
wind_lcoe_county_raw  <- suppressWarnings(
  read.csv(file.path(paths$raw_data, "wind_lcoe_county.csv"),  check.names = FALSE)
) %>% apply_fix_df()
geothermal_lcoe_county_raw <- suppressWarnings(
  read.csv(file.path(paths$raw_data, "geothermal_county.csv"),  check.names = FALSE)
) %>% apply_fix_df()

derive_county_geoid <- function(df, nm) {
  src <- intersect(c("county_geoid", "cnty_fips", "fips", "FIPS", "CountyFIPS"), names(df))
  if (length(src) == 0) .failf("%s: could not find a FIPS column (looked for county_geoid/cnty_fips/fips/FIPS/CountyFIPS).", nm)
  if (!"county_geoid" %in% names(df)) {
    df <- df %>% rename(county_geoid = !!rlang::sym(src[1]))
  }
  df <- df %>% mutate(county_geoid = norm_fips(county_geoid))
  validate_fips(df$county_geoid, nm)
  df
}

solar_lcoe_county_raw <- derive_county_geoid(solar_lcoe_county_raw, "solar_lcoe_county_raw")
wind_lcoe_county_raw  <- derive_county_geoid(wind_lcoe_county_raw,  "wind_lcoe_county_raw")
geothermal_lcoe_county_raw <- derive_county_geoid(geothermal_lcoe_county_raw, "geothermal_lcoe_county_raw")

glimpse(solar_lcoe_county_raw)
glimpse(wind_lcoe_county_raw)
glimpse(geothermal_lcoe_county_raw)

#Exclude "state" and "county" columns from each
solar_lcoe_county_raw <- solar_lcoe_county_raw %>% select(-state) %>% select(-county)
wind_lcoe_county_raw  <- wind_lcoe_county_raw  %>% select(-state) %>% select(-county)
geothermal_lcoe_county_raw <- geothermal_lcoe_county_raw %>% select(-state) %>% select(-county)

library(dplyr)

# 1) Master list of every county_geoid seen in ANY tech
master_counties <- bind_rows(
  solar_lcoe_county_raw  %>% select(county_geoid),
  wind_lcoe_county_raw   %>% select(county_geoid),
  geothermal_lcoe_county_raw %>% select(county_geoid)
) %>%
  distinct()

# 2) Left-join each tech onto the master to ensure every county appears in each
solar_lcoe_county_complete <- master_counties %>%
  left_join(solar_lcoe_county_raw, by = "county_geoid")

wind_lcoe_county_complete <- master_counties %>%
  left_join(wind_lcoe_county_raw, by = "county_geoid")

geothermal_lcoe_county_complete <- master_counties %>%
  left_join(geothermal_lcoe_county_raw, by = "county_geoid")

# 3) One combined table where all three techs are present
lcoe_county_combined <- master_counties %>%
  left_join(solar_lcoe_county_raw,      by = "county_geoid") %>%
  left_join(wind_lcoe_county_raw,       by = "county_geoid") %>%
  left_join(geothermal_lcoe_county_raw, by = "county_geoid")

glimpse(lcoe_county_combined)

#Do all lcoe_county_combined values exist as COUNTY_GEOID values in county_crosswalk_supplement_gdp_pea?
potential_missing_counties <- setdiff(lcoe_county_combined$county_geoid, COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA$COUNTY_GEOID)
if (length(potential_missing_counties) > 0) {
  warningf("The following county_geoids appear in lcoe_county_combined but not in COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA: %s", paste(potential_missing_counties, collapse = ", "))
} else {
  print("All county_geoids in lcoe_county_combined are present in COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA.")
}

#Now join information from COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA using county_geoid/COUNTY_GEOID as matching key
county_crosswalk_with_lcoe <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
  mutate(COUNTY_GEOID = norm_fips(COUNTY_GEOID)) %>%
  left_join(lcoe_county_combined, by = c("COUNTY_GEOID" = "county_geoid"))
glimpse(county_crosswalk_with_lcoe)

# --- Supply-curve aggregation to build `supplycurve_geo` ---
# This code assumes you've already created:
# - lcoe_county_combined (with county_geoid + Solar/Wind/Geothermal *_total_lcoe and *_capacity_mw)
# - geo (county-level crosswalk with State.Name, PEA, CBSA Title, GeoName, state_abbr, state.fips, etc.)
# - geo_long (master list of unique geographies to target)
# - (optional) cd_119_county (county–CD119 intersections w/ percent_district) OR geocorr_county_2020_cd_119
# It adapts to column-name differences vs. the original pipeline.

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(tidyr)
})

# ---------- Helpers ----------
# Flexible column picker (first match wins)
.pick_col <- function(df, candidates) {
  hit <- intersect(candidates, names(df))
  if (length(hit) == 0) stop(sprintf("None of the candidate columns found: %s", paste(candidates, collapse = ", ")))
  hit[1]
}

# Safe weighted average (returns NA if all weights are 0 or x is all NA)
.wavg <- function(x, w) {
  w <- replace(w, is.na(w), 0)
  if (all(is.na(x)) || sum(w, na.rm = TRUE) == 0) return(NA_real_)
  stats::weighted.mean(x, w, na.rm = TRUE)
}

# Build a robust CD label like "AK-AL" for 00 at-large districts, otherwise "AL-01"
.cd_label <- function(state_abbr, cd_fp) {
  out <- ifelse(cd_fp == "00", paste0(state_abbr, "-AL"), paste0(state_abbr, "-", cd_fp))
  as.character(out)
}

# ---------- Standardize LCOE + Capacity column names from lcoe_county_combined ----------
solar_lcoe_col <- .pick_col(lcoe_county_combined, c("Solar_total_lcoe", "solar_total_lcoe", "solar_mean_lcoe", "Solar_mean_lcoe"))
wind_lcoe_col  <- .pick_col(lcoe_county_combined, c("Wind_total_lcoe", "wind_total_lcoe", "wind_mean_lcoe", "Wind_mean_lcoe"))
geo_lcoe_col   <- .pick_col(lcoe_county_combined, c("Geothermal_total_lcoe", "geothermal_total_lcoe", "geothermal_resource_lcoe"))

solar_cap_col  <- .pick_col(lcoe_county_combined, c("Solar_capacity_mw", "solar_capacity_mw_dc", "solar_capacity_mw"))
wind_cap_col   <- .pick_col(lcoe_county_combined, c("Wind_capacity_mw", "wind_capacity_mw"))
geo_cap_col    <- .pick_col(lcoe_county_combined, c("Geothermal_capacity_mw", "geothermal_capacity_MW", "geothermal_capacity_mw"))

# Ensure county FIPS are 5-char strings
lcoe_county_combined <- lcoe_county_combined %>%
  mutate(county_geoid = stringr::str_pad(county_geoid, 5, pad = "0"))

# ---------- Base county table: join geo attributes ----------
geo_min <- geo %>%
  transmute(
    fips = stringr::str_pad(fips, 5, pad = "0"),
    State.Name, "CBSA Title", PEA, GeoName,
    state_abbr, state.fips
  ) %>%
  distinct()

base_counties <- lcoe_county_combined %>%
  left_join(geo_min, by = c("county_geoid" = "fips"))

# ---------- Build county→CD119 share table (percent_district), preferring cd_119_county then geocorr ----------
# State FIPS ↔ Abbr map (from your `geo` table)
state_map <- geo %>%
  transmute(
    state_fips_chr = str_pad(as.character(state.fips), 2, pad = "0"),
    state_abbr = state_abbr
  ) %>%
  distinct()

cd_shares <- NULL

if (exists("cd_119_county")) {
  # Use your precomputed intersections (area-based percent_district, 0–100)
  cd_shares <- cd_119_county %>%
    transmute(
      county_geoid = str_pad(GEOID, 5, pad = "0"),
      STATEFP = str_pad(STATEFP, 2, pad = "0"),
      CD119FP = str_pad(CD119FP, 2, pad = "0"),
      percent_district = as.numeric(percent_district)
    ) %>%
    left_join(state_map, by = c("STATEFP" = "state_fips_chr")) %>%
    mutate(
      cd_119 = .cd_label(state_abbr, CD119FP)
    ) %>%
    select(county_geoid, cd_119, percent_district) %>%
    distinct()
} else if (exists("geocorr_county_2020_cd_119")) {
  # Fall back to population-based allocation factors from Geocorr
  # Use "county-to-cd119 allocation factor" (~share of county in the CD) * 100
  cd_shares <- geocorr_county_2020_cd_119 %>%
    transmute(
      county_geoid = str_pad(COUNTY_GEOID, 5, pad = "0"),
      cd_119 = paste0(`State abbr.`, "-", str_pad(`Congressional district code (119th Congress)`, 2, pad = "0")),
      percent_district = as.numeric(`county-to-cd119 allocation factor`) * 100
    ) %>%
    distinct()
} else {
  # Last resort: assume 100% of each county is in the single cd_119 value present in `geo` (if available)
  cd_shares <- geo %>%
    transmute(
      county_geoid = str_pad(fips, 5, pad = "0"),
      cd_119 = cd_119,
      percent_district = 100
    ) %>%
    filter(!is.na(cd_119)) %>%
    distinct()
}

# ---------- Aggregations by geography ----------
# 1) Congressional Districts (requires expansion by cd_shares)
supply_cd <- base_counties %>%
  inner_join(cd_shares, by = "county_geoid") %>%
  mutate(weight = percent_district / 100) %>%
  group_by(cd_119) %>%
  summarize(
    solar_totallcoe        = .wavg(.data[[solar_lcoe_col]], .data[[solar_cap_col]] * weight),
    wind_totallcoe         = .wavg(.data[[wind_lcoe_col]],  .data[[wind_cap_col]]  * weight),
    geothermal_totallcoe   = .wavg(.data[[geo_lcoe_col]],   .data[[geo_cap_col]]   * weight),
    solar_capacitymw_dc    = sum(.data[[solar_cap_col]] * weight, na.rm = TRUE),
    wind_capacitymw        = sum(.data[[wind_cap_col]]  * weight, na.rm = TRUE),
    geothermal_capacityMW  = sum(.data[[geo_cap_col]]   * weight, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  transmute(
    geo = "Congressional District",
    geo_name = cd_119,
    solar_totallcoe, wind_totallcoe, geothermal_totallcoe,
    solar_capacitymw_dc, wind_capacitymw, geothermal_capacityMW
  )

# 2) States
supply_state <- base_counties %>%
  filter(!is.na(State.Name), State.Name != "") %>%
  mutate(weight = 1) %>%
  group_by(State.Name) %>%
  summarize(
    solar_totallcoe        = .wavg(.data[[solar_lcoe_col]], .data[[solar_cap_col]] * weight),
    wind_totallcoe         = .wavg(.data[[wind_lcoe_col]],  .data[[wind_cap_col]]  * weight),
    geothermal_totallcoe   = .wavg(.data[[geo_lcoe_col]],   .data[[geo_cap_col]]   * weight),
    solar_capacitymw_dc    = sum(.data[[solar_cap_col]] * weight, na.rm = TRUE),
    wind_capacitymw        = sum(.data[[wind_cap_col]]  * weight, na.rm = TRUE),
    geothermal_capacityMW  = sum(.data[[geo_cap_col]]   * weight, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  transmute(
    geo = "State",
    geo_name = State.Name,
    solar_totallcoe, wind_totallcoe, geothermal_totallcoe,
    solar_capacitymw_dc, wind_capacitymw, geothermal_capacityMW
  )

# 3) Economic Areas (PEA)
supply_pea <- base_counties %>%
  filter(!is.na(PEA), PEA != "") %>%
  mutate(weight = 1) %>%
  group_by(PEA) %>%
  summarize(
    solar_totallcoe        = .wavg(.data[[solar_lcoe_col]], .data[[solar_cap_col]] * weight),
    wind_totallcoe         = .wavg(.data[[wind_lcoe_col]],  .data[[wind_cap_col]]  * weight),
    geothermal_totallcoe   = .wavg(.data[[geo_lcoe_col]],   .data[[geo_cap_col]]   * weight),
    solar_capacitymw_dc    = sum(.data[[solar_cap_col]] * weight, na.rm = TRUE),
    wind_capacitymw        = sum(.data[[wind_cap_col]]  * weight, na.rm = TRUE),
    geothermal_capacityMW  = sum(.data[[geo_cap_col]]   * weight, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  transmute(
    geo = "Economic Area",
    geo_name = PEA,
    solar_totallcoe, wind_totallcoe, geothermal_totallcoe,
    solar_capacitymw_dc, wind_capacitymw, geothermal_capacityMW
  )

# 4) Metro Areas (CBSA Title)
supply_cbsa <- base_counties %>%
  filter(!is.na("CBSA Title"), "CBSA Title" != "") %>%
  mutate(weight = 1) %>%
  group_by("CBSA Title") %>%
  summarize(
    solar_totallcoe        = .wavg(.data[[solar_lcoe_col]], .data[[solar_cap_col]] * weight),
    wind_totallcoe         = .wavg(.data[[wind_lcoe_col]],  .data[[wind_cap_col]]  * weight),
    geothermal_totallcoe   = .wavg(.data[[geo_lcoe_col]],   .data[[geo_cap_col]]   * weight),
    solar_capacitymw_dc    = sum(.data[[solar_cap_col]] * weight, na.rm = TRUE),
    wind_capacitymw        = sum(.data[[wind_cap_col]]  * weight, na.rm = TRUE),
    geothermal_capacityMW  = sum(.data[[geo_cap_col]]   * weight, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  transmute(
    geo = "Metro Area",
    geo_name = "CBSA Title",
    solar_totallcoe, wind_totallcoe, geothermal_totallcoe,
    solar_capacitymw_dc, wind_capacitymw, geothermal_capacityMW
  )

# 5) Counties
supply_county <- base_counties %>%
  filter(!is.na(GeoName), GeoName != "") %>%
  mutate(weight = 1) %>%
  group_by(GeoName) %>%
  summarize(
    solar_totallcoe        = .wavg(.data[[solar_lcoe_col]], .data[[solar_cap_col]] * weight),
    wind_totallcoe         = .wavg(.data[[wind_lcoe_col]],  .data[[wind_cap_col]]  * weight),
    geothermal_totallcoe   = .wavg(.data[[geo_lcoe_col]],   .data[[geo_cap_col]]   * weight),
    solar_capacitymw_dc    = sum(.data[[solar_cap_col]] * weight, na.rm = TRUE),
    wind_capacitymw        = sum(.data[[wind_cap_col]]  * weight, na.rm = TRUE),
    geothermal_capacityMW  = sum(.data[[geo_cap_col]]   * weight, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  transmute(
    geo = "County",
    geo_name = GeoName,
    solar_totallcoe, wind_totallcoe, geothermal_totallcoe,
    solar_capacitymw_dc, wind_capacitymw, geothermal_capacityMW
  )

# ---------- Combine, then conform to `geo_long` (ensure all target geographies present) ----------
supply_all <- bind_rows(supply_state, supply_cd, supply_pea, supply_cbsa, supply_county)

# --- PATCH: keep `geo_code` in `supplycurve_geo` ---

# 1) Preserve `geo_code` when normalizing `geo_long`
geo_long_norm <- geo_long %>%
  mutate(
    geo = case_when(
      grepl("^state$", geo_type, ignore.case = TRUE) ~ "State",
      grepl("congress|cd", geo_type, ignore.case = TRUE) ~ "Congressional District",
      grepl("economic|pea", geo_type, ignore.case = TRUE) ~ "Economic Area",
      grepl("metro|cbsa", geo_type, ignore.case = TRUE) ~ "Metro Area",
      grepl("county", geo_type, ignore.case = TRUE) ~ "County",
      TRUE ~ geo_type
    )
  ) %>%
  distinct(geo, geo_name, geo_code)

# 2) Build `supplycurve_geo` while retaining `geo_code`
supplycurve_geo <- geo_long_norm %>%
  left_join(supply_all, by = c("geo", "geo_name")) %>%
  group_by(geo) %>%
  mutate(
    # Potential (capacity) ranks — treat missing capacity as 0
    Solar_potential_rank      = rank(-as.numeric(replace_na(solar_capacitymw_dc, 0)), ties.method = "min"),
    Wind_potential_rank       = rank(-as.numeric(replace_na(wind_capacitymw, 0)), ties.method = "min"),
    Geothermal_potential_rank = rank(-as.numeric(replace_na(geothermal_capacityMW, 0)), ties.method = "min"),
    # Cost ranks — put missing costs at the end
    Solar_cost_rank           = rank(ifelse(is.na(solar_totallcoe), Inf, as.numeric(solar_totallcoe)), ties.method = "min"),
    Wind_cost_rank            = rank(ifelse(is.na(wind_totallcoe), Inf, as.numeric(wind_totallcoe)), ties.method = "min"),
    Geothermal_cost_rank      = rank(ifelse(is.na(geothermal_totallcoe), Inf, as.numeric(geothermal_totallcoe)), ties.method = "min")
  ) %>%
  ungroup() %>%
  relocate(geo_code, .after = geo_name) %>%  # <— keep geo_code in the final output
  arrange(geo, geo_name)

glimpse(supplycurve_geo)

# `supplycurve_geo` is now ready and aligned to the unique geographies in `geo_long`.
# It includes:
#   geo, geo_name,
#   solar_totallcoe, wind_totallcoe, geothermal_totallcoe,
#   solar_capacitymw_dc, wind_capacitymw, geothermal_capacityMW,
#   Solar_potential_rank, Wind_potential_rank, Geothermal_potential_rank,
#   Solar_cost_rank, Wind_cost_rank, Geothermal_cost_rank

================================================================================
# END supplycurve_geo.R
================================================================================


================================================================================
# BEGIN techpot_geo.R
================================================================================

# =========================== BOOT + INPUT GLIMPSES ===========================
cat("Glimpse tigris_congressional_districts_2024_raw:\n"); glimpse(tigris_congressional_districts_2024_raw)
cat("Glimpse COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA:\n"); glimpse(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA)
cat("Glimpse geo:\n"); glimpse(geo)
cat("Glimpse geo_long:\n"); glimpse(geo_long)
cat("Glimpse tigris_counties_2020_raw:\n"); glimpse(tigris_counties_2020_raw)
cat("Glimpse geocorr_county_2020_cd_119:\n"); glimpse(geocorr_county_2020_cd_119)
cat("Glimpse geocorr_ct_county_cd_119:\n"); glimpse(geocorr_ct_county_cd_119)

# --- read TechPot once (assumes paths + fix_df available) ---
techpot_county_raw <- suppressWarnings(read.csv(
  file.path(paths$raw_data, "NREL SLOPE", "techpot_baseline", "techpot_baseline_county.csv"),
  check.names = FALSE
)) %>% fix_df()

cat("Glimpse of techpot_county_raw:\n"); glimpse(techpot_county_raw)

# Quick QA on TechPot raw
num_unique_county_state <- techpot_county_raw %>%
  select(`County Name`, `State Name`) %>%
  distinct() %>%
  nrow()
cat("Number of unique County Name/State Name combinations in techpot_county_raw:", num_unique_county_state, "\n")

unique_states_techpot_raw <- sort(unique(techpot_county_raw$`State Name`))
cat("Unique states in techpot_county_raw (", length(unique_states_techpot_raw), "):\n", paste(unique_states_techpot_raw, collapse=", "), "\n")

unique_technologies <- sort(unique(techpot_county_raw$Technology))
cat("Unique Technology values in techpot_county_raw (", length(unique_technologies), "):\n", paste(unique_technologies, collapse=", "), "\n")

# =========================== LIBRARIES & OPTIONS ============================
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(sf)
  library(purrr)
  library(rlang)
})

# Optional dependencies
has_fuzzyjoin <- requireNamespace("fuzzyjoin", quietly = TRUE)
has_stringi   <- requireNamespace("stringi",   quietly = TRUE)
has_tigris    <- requireNamespace("tigris",    quietly = TRUE)

options(tigris_use_cache = TRUE, tigris_class = "sf")

# =========================== SMALL DEBUG HELPERS ============================
dbg_dim <- function(x, name = deparse(substitute(x))) {
  if (is.null(x)) return(invisible(x))
  cat(sprintf("[dbg] %s: %s x %s\n", name, nrow(x), ncol(x)))
  invisible(x)
}
dbg_head <- function(x, name = deparse(substitute(x)), n = 3) {
  cat(sprintf("[dbg] head(%s):\n", name)); print(utils::head(x, n)); invisible(x)
}
nz_chr <- function(x) ifelse(is.na(x) | trimws(x)=="", "", x)
safe_div <- function(num, den) ifelse(is.na(den) | den == 0, NA_real_, num/den)

# =========================== NORMALIZATION HELPERS ==========================
normalize_name <- function(x) {
  x <- x %>% stringr::str_trim()
  if (has_stringi) x <- stringi::stri_trans_general(x, "Latin-ASCII")
  x %>%
    stringr::str_to_lower() %>%
    stringr::str_replace_all("\\bst\\.?\\b",  "saint") %>%
    stringr::str_replace_all("\\bste\\.?\\b", "sainte") %>%
    stringr::str_replace_all("[[:punct:]&&[^']]", " ") %>%
    stringr::str_squish()
}

repair_county_norm <- function(x) {
  x %>%
    stringr::str_replace("^o brien$",        "o'brien") %>%
    stringr::str_replace("^prince george s$", "prince george's") %>%
    stringr::str_replace("^queen anne s$",    "queen anne's") %>%
    stringr::str_replace("^saint mary s$",    "saint mary's") %>%
    stringr::str_replace("^dewitt$",   "de witt") %>%
    stringr::str_replace("^desoto$",   "de soto") %>%
    stringr::str_replace("^dekalb$",   "de kalb") %>%
    stringr::str_replace("^laporte$",  "la porte") %>%
    stringr::str_replace("^lasalle$",  "la salle") %>%
    identity()
}

normalize_county_like <- function(x) {
  x %>%
    normalize_name() %>%
    repair_county_norm() %>%
    stringr::str_replace("\\bcounty\\b$", "") %>%
    stringr::str_replace("\\bparish\\b$", "") %>%
    stringr::str_replace("\\bcity and borough\\b$", "") %>%
    stringr::str_replace("\\bborough\\b$", "") %>%
    stringr::str_replace("\\bcensus area\\b$", "") %>%
    stringr::str_replace("\\bmunicipality\\b$", "") %>%
    stringr::str_squish()
}

# =========================== TIGRIS LOOKUPS (SOURCE OF TRUTH) ===============
territory_statefps <- c("60","66","69","72","74","78")  # AS, GU, MP, PR, UM, VI

tigris_states_for_lookup <- (get0("tigris_states_2024_raw", inherits = TRUE) %||% {
  stopifnot(has_tigris); tigris::states(year = 2024, cb = TRUE)
}) %>%
  st_drop_geometry() %>%
  filter(!STATEFP %in% territory_statefps) %>%
  transmute(
    STATEFP, STUSPS,
    TIGRIS_STATE_NAME = NAME,
    state_name_norm   = normalize_name(NAME)
  ) %>% distinct()

tigris_counties_for_lookup <- (get0("tigris_counties_2020_raw", inherits = TRUE) %||% {
  stopifnot(has_tigris); tigris::counties(year = 2020, cb = TRUE)
}) %>%
  filter(!STATEFP %in% territory_statefps) %>%
  transmute(
    GEOID, STATEFP, CBSAFP,
    TIGRIS_COUNTY_NAME = NAME,
    TIGRIS_NAMELSAD    = NAMELSAD,
    geometry
  ) %>%
  left_join(tigris_states_for_lookup, by = "STATEFP") %>%
  mutate(county_name_norm = normalize_county_like(TIGRIS_COUNTY_NAME)) %>%
  distinct(STATEFP, county_name_norm, .keep_all = TRUE) %>%
  st_as_sf()

dbg_dim(tigris_states_for_lookup, "tigris_states_for_lookup")
dbg_dim(tigris_counties_for_lookup, "tigris_counties_for_lookup")

# =========================== TECHPOT -> COUNTY GEOIDS =======================
stopifnot(exists("techpot_county_raw"))

tech_gen_col <- names(techpot_county_raw) %>%
  stringr::str_subset(stringr::regex("technical\\s*generation\\s*potential", ignore_case = TRUE)) %>%
  { .[1] }
if (is.na(tech_gen_col)) stop("Could not locate the Technical Generation Potential column in techpot_county_raw.")

tp_states <- sort(unique(techpot_county_raw$`State Name`))
cat("TechPot states (", length(tp_states), "): ", paste(tp_states, collapse = ", "), "\n", sep = "")

techpot_counties_unique <- techpot_county_raw %>%
  select(`County Name`, `State Name`) %>%
  distinct() %>%
  mutate(
    tp_state_name_norm  = normalize_name(`State Name`),
    tp_county_name_norm = normalize_county_like(`County Name`),
    tp_county_name_norm_repaired = repair_county_norm(tp_county_name_norm)
  ) %>%
  left_join(
    tigris_states_for_lookup %>% select(STATEFP, state_name_norm),
    by = c("tp_state_name_norm" = "state_name_norm")
  ) %>%
  filter(!is.na(STATEFP)) %>%
  distinct(STATEFP, tp_county_name_norm, .keep_all = TRUE)

tp_join1 <- techpot_counties_unique %>%
  left_join(
    tigris_counties_for_lookup %>% st_drop_geometry() %>%
      select(STATEFP, county_name_norm, GEOID),
    by = c("STATEFP", "tp_county_name_norm" = "county_name_norm")
  )

tp_join2 <- tp_join1 %>%
  left_join(
    tigris_counties_for_lookup %>% st_drop_geometry() %>%
      select(STATEFP, county_name_norm, GEOID) %>% rename(GEOID_repaired = GEOID),
    by = c("STATEFP", "tp_county_name_norm_repaired" = "county_name_norm")
  ) %>%
  mutate(county_geoid_from_tigris = coalesce(GEOID, GEOID_repaired)) %>%
  select(-GEOID, -GEOID_repaired)

if (has_fuzzyjoin) {
  still_unmatched <- tp_join2 %>% filter(is.na(county_geoid_from_tigris))
  if (nrow(still_unmatched) > 0) {
    message("[info] Attempting in-state fuzzy rescue for ", nrow(still_unmatched), " county names…")
    candidates <- tigris_counties_for_lookup %>% st_drop_geometry() %>%
      transmute(STATEFP, county_name_norm, GEOID)
    fuzz <- fuzzyjoin::stringdist_left_join(
      still_unmatched,
      candidates,
      by = c("STATEFP" = "STATEFP", "tp_county_name_norm_repaired" = "county_name_norm"),
      method = "lv", max_dist = 1, distance_col = "dist"
    ) %>%
      group_by(`County Name`, `State Name`) %>%
      slice_min(order_by = dist, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      transmute(`County Name`, `State Name`, STATEFP,
                county_geoid_from_tigris_fuzzy = GEOID)
    
    tp_join2 <- tp_join2 %>%
      left_join(fuzz, by = c("County Name", "State Name", "STATEFP")) %>%
      mutate(county_geoid_from_tigris = coalesce(county_geoid_from_tigris, county_geoid_from_tigris_fuzzy)) %>%
      select(-county_geoid_from_tigris_fuzzy)
  }
}

techpot_county_with_geoid <- tp_join2 %>%
  left_join(
    tigris_counties_for_lookup %>%
      select(
        GEOID,
        county_name_full = TIGRIS_NAMELSAD,
        state_fips = STATEFP,
        geometry,
        cbsa_code = CBSAFP
      ),
    by = c("county_geoid_from_tigris" = "GEOID")
  ) %>%
  transmute(
    county_name_techpot = `County Name`,
    state_name_techpot  = `State Name`,
    state_fips,
    county_geoid = county_geoid_from_tigris,
    county_name_full,
    cbsa_code,
    geometry
  ) %>%
  st_as_sf()

dbg_dim(techpot_county_with_geoid, "techpot_county_with_geoid")

# =========================== COVERAGE VS CROSSWALK ==========================
xwalk <- get0("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA", inherits = TRUE)
if (!is.null(xwalk)) {
  xwalk_keys <- xwalk %>%
    transmute(
      county_geoid = COUNTY_GEOID,
      state_fips   = STATE_FIPS,
      cbsa_code    = CBSA_GEOID,
      cbsa_title   = CBSA_NAME,
      pea_code     = as.character(PEA_NUMBER),
      pea_title    = PEA_NAME,
      county_pop_2022 = `2022_COUNTY_POPULATION`,
      county_gdp_2022 = COUNTY_GDP_2022
    ) %>% distinct()
  
  covered <- techpot_county_with_geoid %>% st_drop_geometry() %>% select(county_geoid) %>% distinct()
  missing  <- xwalk_keys %>% anti_join(covered, by = "county_geoid")
  cat(sprintf("[coverage] TechPot counties matched: %d / %d (missing %d)\n",
              nrow(covered), nrow(xwalk_keys), nrow(missing)))
  if (nrow(missing) > 0) {
    cat("[coverage] Examples of missing county_geoids:\n")
    print(utils::head(missing, 10))
  }
} else {
  warning("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA not found; skipping coverage QA and context enrichments.")
}

# =========================== LONG-FORM TECHPOT (COUNTY) =====================
techpot_county_long <- techpot_county_raw %>%
  select(
    `County Name`, `State Name`,
    Technology = any_of("Technology"),
    tech_gen   = all_of(tech_gen_col)
  ) %>%
  mutate(
    tech = case_when(
      Technology == "land_based_wind"                      ~ "Wind Potential (MWh)",
      Technology == "utility_pv"                           ~ "Utility Solar Potential (MWh)",
      Technology %in% c("residential_pv", "commercial_pv") ~ "Rooftop Solar Potential (MWh)",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(tech)) %>%
  left_join(
    techpot_county_with_geoid %>% st_drop_geometry() %>%
      transmute(
        `County Name` = county_name_techpot,
        `State Name`  = state_name_techpot,
        county_geoid,
        state_fips,
        cbsa_code
      ),
    by = c("County Name", "State Name")
  ) %>%
  filter(!is.na(county_geoid))

dbg_dim(techpot_county_long, "techpot_county_long")

# =========================== HUMAN-READABLE LOOKUPS =========================
county_lookup <- tigris_counties_for_lookup %>%
  st_drop_geometry() %>%
  transmute(
    county_geoid = GEOID,
    county_name_full = TIGRIS_NAMELSAD,
    state_fips = STATEFP,
    STUSPS,
    State.Name = TIGRIS_STATE_NAME
  )

state_lookup <- tigris_states_for_lookup %>%
  transmute(
    state_fips = STATEFP,
    STUSPS,
    State.Name = TIGRIS_STATE_NAME
  )

cbsa_lookup <- {
  if (!is.null(xwalk)) {
    xwalk %>%
      transmute(cbsa_code = CBSA_GEOID, CBSA.Title = CBSA_NAME) %>%
      distinct()
  } else if (!is.null(get0("tigris_cbsa_2020_raw", inherits = TRUE))) {
    get("tigris_cbsa_2020_raw", inherits = TRUE) %>%
      st_drop_geometry() %>%
      transmute(cbsa_code = CBSAFP, CBSA.Title = NAME) %>%
      distinct()
  } else if (has_tigris) {
    tigris::core_based_statistical_areas(year = 2020, cb = TRUE) %>%
      st_drop_geometry() %>%
      transmute(cbsa_code = CBSAFP, CBSA.Title = NAME) %>%
      distinct()
  } else {
    tibble(cbsa_code = character(), CBSA.Title = character())
  }
}

# =========================== CONTEXT (POP / GDP) ============================
stopifnot(!is.null(xwalk))
county_context <- xwalk %>%
  transmute(
    county_geoid = COUNTY_GEOID,
    county_pop_2022 = `2022_COUNTY_POPULATION`,
    county_gdp_2022 = COUNTY_GDP_2022
  ) %>% distinct()

# State context from ALL counties
state_context <- xwalk %>%
  group_by(state_fips = STATE_FIPS) %>%
  summarize(
    pop_2022 = sum(`2022_COUNTY_POPULATION`, na.rm = TRUE),
    gdp_2022 = sum(COUNTY_GDP_2022, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(state_lookup, by = "state_fips") %>%
  transmute(
    geo = "State",
    geo_code = state_fips,
    geo_name = coalesce(State.Name, ""),
    pop_2022, gdp_2022
  )

# County context with names
county_context_named <- county_context %>%
  left_join(county_lookup, by = "county_geoid") %>%
  transmute(
    geo = "County",
    geo_code = county_geoid,
    geo_name = paste0(coalesce(county_name_full, ""), if_else(!is.na(STUSPS), paste0(", ", STUSPS), "")),
    pop_2022 = county_pop_2022,
    gdp_2022 = county_gdp_2022
  )

# CBSA context (sum counties in CBSA)
cbsa_context <- xwalk %>%
  filter(!is.na(CBSA_GEOID) & CBSA_GEOID != "") %>%
  group_by(cbsa_code = CBSA_GEOID, CBSA.Title = CBSA_NAME) %>%
  summarize(
    pop_2022 = sum(`2022_COUNTY_POPULATION`, na.rm = TRUE),
    gdp_2022 = sum(COUNTY_GDP_2022, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  transmute(
    geo = "Metro Area",
    geo_code = cbsa_code,
    geo_name = coalesce(CBSA.Title, ""),
    pop_2022, gdp_2022
  )

# =========================== CD119 SHARE MAP (CT-SAFE) ======================
# Build a county->CD119 share table that:
#  - uses generic geocorr for all states EXCEPT Connecticut, and
#  - uses the CT-specific pre-2023 county mapping for Connecticut.
#
# We always use *county-to-cd* allocation factors so that for each county
# the shares across CDs sum to 1.0.

# Generic (non-CT) county->CD shares
county_cd119_share_generic <- {
  xw <- get0("geocorr_county_2020_cd_119", inherits = TRUE)
  stopifnot(!is.null(xw))
  xw %>%
    # keep all states EXCEPT CT ("09")
    filter(`State code` != "09") %>%
    transmute(
      county_geoid = `County code`,
      cd_119       = paste0(`State code`, `Congressional district code (119th Congress)`),
      share        = as.numeric(`county-to-cd119 allocation factor`)
    ) %>%
    filter(!is.na(share), share > 0)
}

# CT-specific county->CD shares (pre-2023 county equivalents)
county_cd119_share_ct <- {
  xw_ct <- get0("geocorr_ct_county_cd_119", inherits = TRUE)
  if (is.null(xw_ct)) {
    warning("CT-specific geocorr not found; CT CDs may have missing context.")
    tibble(county_geoid = character(), cd_119 = character(), share = numeric())
  } else {
    xw_ct %>%
      transmute(
        county_geoid = `County code`, # e.g., 09001 .. 09015
        cd_119       = paste0("09", `Congressional district code (119th Congress)`),  # e.g., 0901..0905
        share        = as.numeric(`CTcounty-to-cd119 allocation factor`)
      ) %>%
      filter(!is.na(share), share > 0)
  }
}

county_cd119_share_full <- bind_rows(county_cd119_share_generic, county_cd119_share_ct)

dbg_dim(county_cd119_share_generic, "county_cd119_share_generic (ex-CT)")
dbg_dim(county_cd119_share_ct, "county_cd119_share_ct (CT only)")
dbg_dim(county_cd119_share_full, "county_cd119_share_full (all states)")

# --- CT diagnostics: shares should sum to ~1 per county; all 5 CDs present
ct_share_checks <- county_cd119_share_full %>%
  filter(substr(county_geoid, 1, 2) == "09") %>%
  group_by(county_geoid) %>%
  summarize(share_sum = sum(share, na.rm = TRUE), n_cds = dplyr::n(), .groups = "drop")
cat("[ct-check] CT counties share sums & number of CDs per county:\n"); print(ct_share_checks)

ct_cds_present <- county_cd119_share_full %>%
  filter(substr(cd_119, 1, 2) == "09") %>%
  distinct(cd_119) %>% arrange(cd_119)
cat("[ct-check] CT CDs present in share map:\n"); print(ct_cds_present)

# =========================== CONTEXT BY CD (using CT-safe shares) ===========
cd_context <- county_cd119_share_full %>%
  inner_join(county_context, by = "county_geoid", relationship = "many-to-many") %>%
  group_by(cd_119) %>%
  summarize(
    pop_2022 = sum(county_pop_2022 * share, na.rm = TRUE),
    gdp_2022 = sum(county_gdp_2022 * share, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  transmute(
    geo = "Congressional District",
    geo_code = cd_119,
    geo_name = cd_119,
    pop_2022, gdp_2022
  )

# --- CT diagnostics: CD context should not be NA anymore
cat("[ct-check] CD context for CT (0901..0905):\n")
print(cd_context %>% filter(substr(geo_code, 1, 2) == "09") %>% arrange(geo_code))

# PEA context (unchanged)
county_pea_xwalk <- {
  xw <- get0("county_to_pea_xwalk", inherits = TRUE)
  if (!is.null(xw)) {
    xw %>% transmute(
      county_geoid = as.character(county_geoid),
      PEA          = as.character(pea_code),
      PEA_Name     = as.character(coalesce(pea_name, ""))
    )
  } else if (!is.null(xwalk)) {
    xwalk %>% transmute(
      county_geoid = COUNTY_GEOID,
      PEA          = as.character(PEA_NUMBER),
      PEA_Name     = coalesce(PEA_NAME, "")
    ) %>% distinct()
  } else {
    tibble(county_geoid = character(), PEA = character(), PEA_Name = character())
  }
}

pea_context <- county_pea_xwalk %>%
  inner_join(county_context, by = "county_geoid") %>%
  group_by(PEA, PEA_Name) %>%
  summarize(
    pop_2022 = sum(county_pop_2022, na.rm = TRUE),
    gdp_2022 = sum(county_gdp_2022, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  transmute(
    geo = "Economic Area",
    geo_code = PEA,
    geo_name = if_else(PEA_Name == "" | is.na(PEA_Name), PEA, PEA_Name),
    pop_2022, gdp_2022
  )

context_all <- bind_rows(
  state_context,
  cd_context,
  pea_context,
  county_context_named,
  cbsa_context
)

dbg_dim(context_all, "context_all (pop/gdp only, unique per geo)")

# =========================== TECH AGGREGATIONS (NO CONTEXT) =================
# County level tech sums
tech_by_county <- techpot_county_long %>%
  group_by(county_geoid, tech) %>%
  summarize(tech_pot = sum(tech_gen, na.rm = TRUE), .groups = "drop") %>%
  left_join(county_lookup, by = c("county_geoid" = "county_geoid")) %>%
  transmute(
    geo = "County",
    geo_code = county_geoid,
    geo_name = paste0(coalesce(county_name_full, ""), if_else(!is.na(STUSPS), paste0(", ", STUSPS), "")),
    tech, tech_pot
  )

# State level tech sums
tech_by_state <- techpot_county_long %>%
  group_by(state_fips, tech) %>%
  summarize(tech_pot = sum(tech_gen, na.rm = TRUE), .groups = "drop") %>%
  left_join(state_lookup, by = "state_fips") %>%
  transmute(
    geo = "State",
    geo_code = state_fips,
    geo_name = coalesce(State.Name, ""),
    tech, tech_pot
  )

# CBSA level
tech_by_cbsa <- techpot_county_long %>%
  filter(!is.na(cbsa_code) & cbsa_code != "") %>%
  group_by(cbsa_code, tech) %>%
  summarize(tech_pot = sum(tech_gen, na.rm = TRUE), .groups = "drop") %>%
  left_join(cbsa_lookup, by = c("cbsa_code" = "cbsa_code")) %>%
  transmute(
    geo = "Metro Area",
    geo_code = cbsa_code,
    geo_name = coalesce(CBSA.Title, ""),
    tech, tech_pot
  )

# CD level (CRITICAL: use CT-safe county_cd119_share_full)
tech_by_cd <- techpot_county_long %>%
  select(county_geoid, tech, tech_gen) %>%
  inner_join(county_cd119_share_full, by = "county_geoid", relationship = "many-to-many") %>%
  group_by(cd_119, tech) %>%
  summarize(tech_pot = sum(tech_gen * share, na.rm = TRUE), .groups = "drop") %>%
  transmute(
    geo = "Congressional District",
    geo_code = cd_119,
    geo_name = cd_119,
    tech, tech_pot
  )

# --- CT diagnostics: tech should be >0 for at least one tech in each CT CD
cat("[ct-check] Tech potentials for CT CDs (0901..0905) after allocation:\n")
print(tech_by_cd %>%
        filter(substr(geo_code, 1, 2) == "09") %>%
        group_by(geo_code) %>%
        summarize(across(tech_pot, sum, na.rm = TRUE), .groups = "drop") %>%
        arrange(geo_code))

# PEA level
tech_by_pea <- techpot_county_long %>%
  select(county_geoid, tech, tech_gen) %>%
  inner_join(county_pea_xwalk, by = c("county_geoid" = "county_geoid")) %>%
  group_by(PEA, PEA_Name, tech) %>%
  summarize(tech_pot = sum(tech_gen, na.rm = TRUE), .groups = "drop") %>%
  transmute(
    geo = "Economic Area",
    geo_code = PEA,
    geo_name = if_else(PEA_Name == "" | is.na(PEA_Name), PEA, PEA_Name),
    tech, tech_pot
  )

tech_long_all <- bind_rows(
  tech_by_state,
  tech_by_cd,
  tech_by_pea,
  tech_by_county,
  tech_by_cbsa
)

dbg_dim(tech_long_all, "tech_long_all (NO pop/gdp)")

# =========================== PIVOT TO WIDE (FIXED IDS) ======================
tech_wide_all <- tech_long_all %>%
  pivot_wider(
    id_cols   = c(geo, geo_code, geo_name),
    names_from  = tech,
    values_from = tech_pot,
    values_fn   = sum,
    values_fill = 0
  )

dbg_dim(tech_wide_all, "tech_wide_all (after pivot)")

# =========================== RE-INDEX TO geo_long ===========================
norm_code <- function(geo, code) {
  out <- suppressWarnings(as.integer(code))
  as.character(out)
}

geo_long_norm <- geo_long %>%
  transmute(
    geo = geo_type,
    geo_code = as.character(geo_code),
    geo_name = nz_chr(geo_name),
    code_norm = norm_code(geo_type, geo_code)
  )

tech_wide_norm <- tech_wide_all %>%
  mutate(code_norm = norm_code(geo, geo_code)) %>%
  select(-geo_code, -geo_name)

context_norm <- context_all %>%
  mutate(code_norm = norm_code(geo, geo_code)) %>%
  select(-geo_code, -geo_name)

tech_pot_geo <- geo_long_norm %>%
  left_join(context_norm, by = c("geo", "code_norm")) %>%
  left_join(tech_wide_norm, by = c("geo", "code_norm")) %>%
  mutate(
    `Rooftop Solar Potential (MWh)` = coalesce(`Rooftop Solar Potential (MWh)`, 0),
    `Utility Solar Potential (MWh)` = coalesce(`Utility Solar Potential (MWh)`, 0),
    `Wind Potential (MWh)`          = coalesce(`Wind Potential (MWh)`, 0),
    `Total Potential (MWh)` = `Rooftop Solar Potential (MWh)` +
      `Utility Solar Potential (MWh)` +
      `Wind Potential (MWh)`,
    `Utility Solar MWh per capita` = safe_div(`Utility Solar Potential (MWh)`, pop_2022),
    `Wind MWh per capita`          = safe_div(`Wind Potential (MWh)`,          pop_2022),
    `Rooftop Solar MWh per capita` = safe_div(`Rooftop Solar Potential (MWh)`, pop_2022)
  ) %>%
  group_by(geo) %>%
  mutate(
    Solar_potential_rank         = min_rank(-as.numeric(`Utility Solar Potential (MWh)`)),
    Wind_potential_rank          = min_rank(-as.numeric(`Wind Potential (MWh)`)),
    Rooftop_solar_potential_rank = min_rank(-as.numeric(`Rooftop Solar Potential (MWh)`)),
    Total_potential_rank         = min_rank(-as.numeric(`Total Potential (MWh)`))
  ) %>%
  ungroup() %>%
  transmute(
    geo,
    geo_code,  # from geo_long
    geo_name,  # from geo_long
    pop_2022, gdp_2022,
    `Rooftop Solar Potential (MWh)`,
    `Utility Solar Potential (MWh)`,
    `Wind Potential (MWh)`,
    `Total Potential (MWh)`,
    `Utility Solar MWh per capita`,
    `Wind MWh per capita`,
    `Rooftop Solar MWh per capita`,
    Solar_potential_rank,
    Wind_potential_rank,
    Rooftop_solar_potential_rank,
    Total_potential_rank
  )

# =========================== HEAVY DIAGNOSTICS ==============================
dbg_dim(tech_pot_geo, "tech_pot_geo (FINAL)")
cat(sprintf("[check] geo_long rows: %d | tech_pot_geo rows: %d\n", nrow(geo_long), nrow(tech_pot_geo)))

# Duplicates?
dups <- tech_pot_geo %>% count(geo, geo_code, geo_name) %>% filter(n > 1)
if (nrow(dups) > 0) {
  cat("[ALERT] Duplicated (geo, geo_code, geo_name) in tech_pot_geo:\n"); print(dups)
} else {
  cat("[ok] No duplicate (geo, geo_code, geo_name) rows in tech_pot_geo.\n")
}

# Missing context?
ctx_missing <- tech_pot_geo %>%
  filter(is.na(pop_2022) | is.na(gdp_2022)) %>%
  group_by(geo) %>% summarize(n = n(), .groups = "drop")
if (nrow(ctx_missing) > 0) {
  cat("[warn] Rows missing pop/gdp context:\n"); print(ctx_missing)
  cat("Examples:\n"); print(utils::head(tech_pot_geo %>% filter(is.na(pop_2022) | is.na(gdp_2022)), 10))
} else {
  cat("[ok] All rows have pop/gdp context (or explicitly NA where appropriate).\n")
}

# Coverage by geo type vs skeleton
tally_final    <- tech_pot_geo %>% count(geo, name = "final_n")
tally_skeleton <- geo_long_norm %>% count(geo, name = "skeleton_n")
coverage_cmp   <- tally_skeleton %>%
  left_join(tally_final, by = "geo") %>%
  mutate(diff = final_n - skeleton_n)
cat("[coverage] Rows by geo type (final vs skeleton):\n"); print(coverage_cmp)

# Skeleton anti-join (should be empty)
skeleton_keys <- geo_long_norm %>% transmute(geo, code_norm, skeleton_geo_name = geo_name)
final_keys    <- tech_pot_geo %>% mutate(code_norm = norm_code(geo, geo_code)) %>%
  transmute(geo, code_norm, final_geo_name = geo_name)

missing_in_final <- skeleton_keys %>% anti_join(final_keys, by = c("geo", "code_norm"))
if (nrow(missing_in_final) > 0) {
  cat("[ALERT] Skeleton geos missing in final (should be 0):\n"); print(utils::head(missing_in_final, 20))
} else {
  cat("[ok] No skeleton geos missing in final join.\n")
}

# --- CT final verification: CDs must have context & non-zero totals
ct_cd_final <- tech_pot_geo %>%
  filter(geo == "Congressional District", substr(geo_code, 1, 2) == "09") %>%
  arrange(geo_code) %>%
  transmute(geo_code, geo_name, pop_2022, gdp_2022, `Total Potential (MWh)`)
cat("[ct-check] Final CT CDs (pop/gdp should be non-NA; totals should be >= 0):\n"); print(ct_cd_final)

# Assert overall row equality
if (nrow(tech_pot_geo) != nrow(geo_long)) {
  stop(sprintf("Row-count mismatch: tech_pot_geo (%d) != geo_long (%d). See diagnostics above.",
               nrow(tech_pot_geo), nrow(geo_long)))
}

# =========================== FRIENDLY FINAL NOTES ===========================
unique_states_techpot <- sort(unique(techpot_county_with_geoid$state_name_techpot))
cat("Unique states in TechPot data actually matched (", length(unique_states_techpot), "):\n",
    paste(unique_states_techpot, collapse = ", "), "\n", sep = "")

if (!is.null(xwalk)) {
  cat("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA rows (expected ~3,143): ", nrow(xwalk), "\n", sep = "")
  coverage_by_state <- techpot_county_with_geoid %>%
    st_drop_geometry() %>%
    count(state_name_techpot, name = "tp_n") %>%
    arrange(state_name_techpot)
  cat("[FYI] Example coverage_by_state (first 10 rows):\n"); print(utils::head(coverage_by_state, 10))
}

# --------------------------- Result objects you can use ---------------------
# techpot_county_with_geoid : sf with county geometries & CBSA codes
# techpot_county_long       : long-form county-level potentials (per tech) + keys
# county_cd119_share_full   : county->CD119 allocation shares (CT-safe)
# context_all               : pop/gdp by geo (unique per geo)
# tech_wide_all             : potentials wide (unique per geo)
# tech_pot_geo              : FINAL table, aligned to geo_long (same row count)

================================================================================
# END techpot_geo.R
================================================================================


================================================================================
# BEGIN fed_inv_geo.R
================================================================================

# ====================== PREP: Quick glances at input data already in memory ======================
cat("Glimpse COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA:\n"); glimpse(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA)
cat("Glimpse geo:\n"); glimpse(geo)
cat("Glimpse geo_long:\n"); glimpse(geo_long)
cat("Glimpse tigris_counties_2020_raw:\n"); glimpse(tigris_counties_2020_raw)
cat("Glimpse geocorr_county_2020_cd_119: \n"); glimpse(geocorr_county_2020_cd_119)
cat("Glimpse geocorr_ct_county_cd_119: \n"); glimpse(geocorr_ct_county_cd_119)
cat("Glimpse (tigris_cbsa_2020_raw"); glimpse(tigris_cbsa_2020_raw)
cat("Glimpse tigris_states_2024_raw:\n"); glimpse(tigris_states_2024_raw)
cat("Glimpse tigris_congressional_districts_2024_raw"); glimpse(tigris_congressional_districts_2024_raw)
cat("Glimpse geographies"); glimpse(geographies)
cat("Glimpse geographies_clean"); glimpse(geographies_clean)

#IRA BIL (Federal Funding) Data
ira_bil_raw <- tryCatch(
  readxl::read_excel(file.path(paths$raw_data,"Investment Data - SHARED.xlsx"), sheet=3),
  error=function(e) tibble()
) %>% fix_df(); dbg(ira_bil_raw,"ira_bil_raw")
sf::sf_use_s2(TRUE)

ira_bil_geocoded <- ira_bil_raw %>%
  mutate(
    Longitude = suppressWarnings(as.numeric(Longitude)),
    Latitude  = suppressWarnings(as.numeric(Latitude)),
    Longitude = ifelse(Longitude < -180 | Longitude > 180, NA_real_, Longitude),
    Latitude  = ifelse(Latitude < -90 | Latitude > 90, NA_real_, Latitude)
  ) %>%
  filter(!is.na(Latitude), !is.na(Longitude)) %>%
  st_as_sf(coords = c("Longitude","Latitude"), crs=4326, remove=FALSE) %>%
  st_join(tigris_counties_2020_raw %>% st_transform(4326) %>% transmute(COUNTY_GEOID_2020=GEOID, COUNTY_NAME_2020=NAME, COUNTY_NAMELSAD_2020=NAMELSAD)) %>%
  st_join(tigris_congressional_districts_2024_raw %>% st_transform(4326) %>% transmute(CD119_GEOID=GEOID)) %>%
  st_join(tigris_cbsa_2020_raw %>% st_transform(4326) %>% transmute(CBSA_2020_CODE=GEOID, CBSA_2020_TITLE=NAME)) %>%
  st_join(tigris_states_2024_raw %>% st_transform(4326) %>% transmute(STATE_ABBR=STUSPS, STATE_NAME=NAME, STATE_FIPS=STATEFP)) %>%
  st_join(tigris_counties_2024_raw %>% st_transform(4326) %>% transmute(COUNTY_GEOID_2024=GEOID, COUNTY_NAME_2024=NAME, COUNTY_NAMELSAD_2024=NAMELSAD))
dbg(ira_bil_geocoded,"ira_bil_geocoded")
# ---------------------------
# Federal Grants — finish pipeline (hardened + full coverage to geo_long)
# >> Fix: ensure Metro Area (CBSA) rollups are computed (not just zero-filled)
# >> Adds automatic inclusion/normalization of CBSA column and richer debugging
# ---------------------------

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(janitor)
  library(sf)
  library(stringr)
})

# ---------- Helpers (defined if missing) ----------
if (!exists("%||%")) {
  `%||%` <- function(a, b) if (!is.null(a)) a else b
}

if (!exists("dbg")) {
  dbg <- function(x, label = deparse(substitute(x)), n = 5) {
    cat("\n================================================================\n")
    cat("====", label, "====\n")
    # Glimpse
    suppressWarnings(print(utils::capture.output(dplyr::glimpse(x, width = 80))))
    # Head for data frames / tibbles
    if (inherits(x, "data.frame") || inherits(x, "tbl")) {
      print(utils::head(x, n))
      # NA counts (top 15)
      nas <- tryCatch(sort(colSums(is.na(x)), decreasing = TRUE), error = function(e) NULL)
      if (!is.null(nas)) {
        nas <- nas[nas > 0]
        if (length(nas)) {
          cat("\n---- NA counts (top 15) ----\n")
          print(utils::head(nas, 15))
        }
      }
    }
    cat("================================================================\n")
    invisible(x)
  }
}

percent_fmt <- function(x) sprintf("%0.1f%%", 100 * x)

# ---------- 0) Quick audit of inputs we rely on ----------
dbg(ira_bil_geocoded, "ira_bil_geocoded (input check)")
dbg(geo,                "geo (input check)")
dbg(geo_long,           "geo_long (input check)")
dbg(geographies,        "geographies (input check)")
if (exists("geographies_clean")) dbg(geographies_clean, "geographies_clean (input check)")

# Sanity: expected set of rollup columns
expected_keys <- c("State.Name", "cd_119", "PEA", "CBSA.Title", "GeoName")

missing_in_geographies <- setdiff(expected_keys, geographies %||% character())
extra_in_geographies   <- setdiff(geographies %||% character(), expected_keys)
dbg(list(missing_in_geographies = missing_in_geographies,
         extra_in_geographies   = extra_in_geographies),
    "geographies vs expected (diagnostic)")

# ---------- 1) Normalize/standardize keys coming out of your geocoding joins ----------
# Prefer 2020 county FIPS (to match your `geo`), fall back to 2024 if needed.
ira_bil_keys <- ira_bil_geocoded %>%
  st_drop_geometry() %>%
  clean_names() %>%
  mutate(
    # Standardize funding source and amount
    funding_source  = toupper(trimws(funding_source)),
    funding_amount  = suppressWarnings(as.numeric(funding_amount)),
    
    # Prefer 2020 county FIPS; if missing, use 2024 (both padded to 5)
    county_fips_2020 = coalesce(county_geoid_2020, county_geoid_2024),
    county_fips_2020 = if_else(!is.na(county_fips_2020) & nchar(county_fips_2020) > 0,
                               str_pad(county_fips_2020, width = 5, pad = "0"),
                               NA_character_),
    
    # Congressional district (CD-119): "SSDD" (4 chars)
    district_id = cd119_geoid,
    district_id = if_else(!is.na(district_id) & nchar(district_id) > 0,
                          str_pad(district_id, width = 4, pad = "0"),
                          NA_character_),
    
    # State bits (for sanity checks)
    state_fips_std = if_else(!is.na(state_fips) & nchar(state_fips) > 0, str_pad(state_fips, 2, pad="0"), NA_character_),
    state_name_std = state_name,
    state_abbr_std = state_abbr,
    
    # Reference fields from joins
    cbsa_title_2020  = cbsa_2020_title,        # from tigris_cbsa_2020 join
    county_name_2020 = county_namelsad_2020
  ) %>%
  select(
    objectid, funding_source, program_name, project_name,
    funding_amount, district_id, county_fips_2020,
    cbsa_title_2020, state_name_std, state_abbr_std, state_fips_std
  )

dbg(ira_bil_keys, "ira_bil_keys (after key standardization)")

# ---------- 1a) NA/Validity audits ----------
ira_bil_audit <- list(
  rows_total           = nrow(ira_bil_keys),
  na_funding_amount    = sum(is.na(ira_bil_keys$funding_amount)),
  na_county_fips_2020  = sum(is.na(ira_bil_keys$county_fips_2020)),
  na_district_id       = sum(is.na(ira_bil_keys$district_id)),
  distinct_sources     = paste(sort(unique(ira_bil_keys$funding_source)), collapse=", ")
)
dbg(as.data.frame(ira_bil_audit), "audit: NA/validity counts")

ira_bil_keys %>% count(funding_source, sort = TRUE) %>% dbg("counts by funding_source")
ira_bil_keys %>% filter(is.na(funding_amount)) %>% slice_head(n = 10) %>% dbg("sample rows with NA funding_amount")
ira_bil_keys %>% filter(is.na(county_fips_2020)) %>% slice_head(n = 10) %>% dbg("sample rows with missing county_fips_2020")
ira_bil_keys %>% filter(is.na(district_id)) %>% slice_head(n = 10) %>% dbg("sample rows with missing district_id")

# ---------- 2) Build the county+district summarization (mirrors your legacy approach) ----------
# Keep Funding Source split and sum amounts.
fed_inv_county <- ira_bil_keys %>%
  filter(!is.na(county_fips_2020)) %>%               # must have county to roll up
  mutate(federal_funding = funding_amount) %>%       # ensured numeric above
  group_by(district_id, county_fips_2020, funding_source) %>%
  summarize(
    federal_funds = sum(federal_funding, na.rm = TRUE),
    .groups = "drop"
  )

dbg(fed_inv_county, "fed_inv_county (grouped)")

# ---------- 2a) Join to your geo table; normalize key names (e.g., CBSA Title vs CBSA.Title) ----------
geo_keys <- geo %>%
  select(-any_of("percent_district")) %>% 
  distinct()

# Normalize CBSA column name so it aligns with rollup ("CBSA.Title")
if ("CBSA Title" %in% names(geo_keys) && !("CBSA.Title" %in% names(geo_keys))) {
  names(geo_keys)[names(geo_keys) == "CBSA Title"] <- "CBSA.Title"
}

# Diagnostic: CBSA presence in geo_keys (county view)
cbsa_presence_geo_keys <- geo_keys %>%
  transmute(has_cbsa = !is.na(`CBSA.Title`)) %>%
  summarise(
    n_rows = dplyr::n(),
    n_with_cbsa = sum(has_cbsa, na.rm = TRUE),
    share_with_cbsa = percent_fmt(n_with_cbsa / n_rows)
  )
dbg(cbsa_presence_geo_keys, "geo_keys: CBSA presence diagnostic (county rows)")

# Diagnostic: do we have all requested geographies in geo_keys?
missing_geo_cols_in_geo <- setdiff(expected_keys, names(geo_keys))
dbg(list(missing_geo_cols_in_geo = missing_geo_cols_in_geo),
    "geo_keys: presence of expected geographies (diagnostic)")

dbg(geo_keys, "geo_keys (normalized)")

fed_inv_county_geo <- fed_inv_county %>%
  left_join(
    geo_keys,
    by = c("district_id" = "GEOID_2", "county_fips_2020" = "fips")
  )

# Diagnostic: CBSA presence after join (record-level)
cbsa_presence_after_join <- fed_inv_county_geo %>%
  summarise(
    n_rows = dplyr::n(),
    n_with_cbsa = sum(!is.na(`CBSA.Title`)),
    share_with_cbsa = percent_fmt(n_with_cbsa / n_rows)
  )
dbg(fed_inv_county_geo, "fed_inv_county_geo (after join to geo)")
dbg(cbsa_presence_after_join, "fed_inv_county_geo: CBSA presence diagnostic (post-join)")

# ---------- 2b) Quick join diagnostics ----------
anti_cd <- anti_join(fed_inv_county, geo_keys, by = c("district_id" = "GEOID_2", "county_fips_2020" = "fips"))
dbg(anti_cd, "rows in fed_inv_county that didn't match geo (should be small/zero)")
anti_cd %>% count(funding_source, sort = TRUE) %>% dbg("unmatched by funding_source")

# ---------- 2c) Build EFFECTIVE geography list for rollups (auto-fix CBSA omission) ----------
normalize_clean <- function(v) {
  v <- v %||% character()
  v <- ifelse(v == "State Name", "State.Name", v)
  v <- ifelse(v == "CBSA Title", "CBSA.Title", v)
  v
}

geo_candidates <- unique(c(
  geographies %||% character(),
  normalize_clean(if (exists("geographies_clean")) geographies_clean else character())
))

# Ensure we include CBSA.Title if it exists in joined data but is missing from inputs
if (!("CBSA.Title" %in% geo_candidates) && "CBSA.Title" %in% names(fed_inv_county_geo)) {
  geo_candidates <- c(geo_candidates, "CBSA.Title")
}

# Keep only columns that exist in fed_inv_county_geo
geographies_eff <- intersect(geo_candidates, names(fed_inv_county_geo))

# Final check: we want all expected_keys to be considered if present in data
missing_from_eff_but_available <- setdiff(intersect(expected_keys, names(fed_inv_county_geo)), geographies_eff)
geographies_eff <- unique(c(geographies_eff, missing_from_eff_but_available))

dbg(list(
  original_geographies   = geographies %||% character(),
  geographies_clean_norm = normalize_clean(if (exists("geographies_clean")) geographies_clean else character()),
  geographies_eff        = geographies_eff
), "Geographies selection (pre-rollup)")

# ---------- 3) Rollups across requested geographies ----------
stopifnot(length(geographies_eff) > 0)

results_list <- vector("list", length(geographies_eff))
names(results_list) <- geographies_eff

for (geog in geographies_eff) {
  tmp <- fed_inv_county_geo %>%
    transmute(
      geog_value = .data[[geog]],
      funding_source,
      federal_funds
    ) %>%
    filter(!is.na(geog_value), geog_value != "", geog_value != "NA-NA") %>%
    group_by(geog_value, funding_source) %>%
    summarize(federal_funds = sum(federal_funds, na.rm = TRUE), .groups = "drop") %>%
    mutate(rollup_key = geog, .before = 1)
  
  dbg(tmp, paste0("rollup (normalized) for ", geog))
  results_list[[geog]] <- tmp
}

# Stack (already normalized)
fed_inv_geo_long <- bind_rows(results_list)

dbg(fed_inv_geo_long, "fed_inv_geo_long (normalized & stacked)")

# Quick QA: make sure we didn't lose levels
fed_inv_geo_long %>% count(rollup_key, sort = TRUE) %>% dbg("row counts by rollup_key")
fed_inv_geo_long %>% filter(is.na(geog_value)) %>% count(rollup_key) %>% dbg("NA geog_value by rollup_key (should be 0)")

# Extra: If CBSA rollup exists, show top metros
if ("CBSA.Title" %in% fed_inv_geo_long$rollup_key) {
  fed_inv_geo_long %>%
    filter(rollup_key == "CBSA.Title") %>%
    arrange(desc(federal_funds)) %>%
    slice_head(n = 10) %>%
    dbg("Top 10 metro rollups by federal_funds (long form)")
}

# ---------- 4) De-dupe (safety) and pivot ----------
dups <- fed_inv_geo_long %>%
  count(rollup_key, geog_value, funding_source, name = "n") %>%
  filter(n > 1L)
dbg(dups, "DUP CHECK before pivot")

fed_inv_geo_long_u <- fed_inv_geo_long %>%
  group_by(rollup_key, geog_value, funding_source) %>%
  summarize(federal_funds = sum(federal_funds, na.rm = TRUE), .groups = "drop")

dbg(fed_inv_geo_long_u, "fed_inv_geo_long_u (unique keys)")

fed_inv_geo_wide <- fed_inv_geo_long_u %>%
  mutate(funding_source = case_when(
    funding_source == "BIL" ~ "BIL_grants",
    funding_source == "IRA" ~ "IRA_grants",
    TRUE                    ~ funding_source              # keep any other labels (e.g., "BIL/IRA")
  )) %>%
  tidyr::pivot_wider(
    names_from  = funding_source,
    values_from = federal_funds,
    values_fill = 0
  ) %>%
  mutate(
    total_bil_ira_grants = rowSums(across(any_of(c("BIL_grants","IRA_grants"))), na.rm = TRUE)
  )

dbg(fed_inv_geo_wide, "fed_inv_geo_wide (wide, numeric)")

# ---------- 5) Friendly labels and base final shape ----------
label_map <- c(
  "State.Name" = "State",
  "cd_119"     = "Congressional District",
  "PEA"        = "Economic Area",
  "GeoName"    = "County",
  "CBSA.Title" = "Metro Area"
)

fed_inv_geo_base <- fed_inv_geo_wide %>%
  mutate(
    geo      = recode(rollup_key, !!!label_map, .default = rollup_key),
    geo_name = geog_value
  ) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  select(
    geo, geo_name,
    BIL_grants = any_of("BIL_grants"),
    IRA_grants = any_of("IRA_grants"),
    total_bil_ira_grants
  ) %>%
  arrange(geo, geo_name)

dbg(fed_inv_geo_base, "fed_inv_geo_base (pre-coverage alignment)")
fed_inv_geo_base %>% count(geo, sort = TRUE) %>% dbg("Row counts by geo (base)")

# ---------- 6) Enforce SAME geography coverage as geo_long ----------
# Build expected scaffold from geo_long and left-join base results (zero-fill gaps)
expected_geo <- geo_long %>%
  transmute(
    geo      = geo_type,
    geo_name = geo_name
  ) %>%
  distinct()

dbg(expected_geo, "expected_geo (from geo_long)")

# Coverage diagnostics before enforcing
missing_in_base <- anti_join(expected_geo, fed_inv_geo_base, by = c("geo","geo_name"))
extra_in_base   <- anti_join(fed_inv_geo_base, expected_geo, by = c("geo","geo_name"))
dbg(missing_in_base, "expected but missing in base (will be zero-filled)")
dbg(extra_in_base,   "present in base but not in expected (will be dropped)")

coverage_summary <- tibble(
  expected_rows     = nrow(expected_geo),
  missing_rows      = nrow(missing_in_base),
  extra_rows        = nrow(extra_in_base),
  expected_coverage = percent_fmt((nrow(expected_geo) - nrow(missing_in_base)) / max(1, nrow(expected_geo)))
)
dbg(coverage_summary, "Coverage summary vs geo_long (pre-enforcement)")

# Enforce coverage (left join and zero-fill)
fed_inv_geo <- expected_geo %>%
  left_join(fed_inv_geo_base, by = c("geo","geo_name")) %>%
  mutate(
    BIL_grants            = coalesce(BIL_grants, 0),
    IRA_grants            = coalesce(IRA_grants, 0),
    total_bil_ira_grants  = coalesce(total_bil_ira_grants, 0)
  ) %>%
  arrange(geo, geo_name)

dbg(fed_inv_geo, "fed_inv_geo (final, coverage-aligned to geo_long)")
fed_inv_geo %>% count(geo, sort = TRUE) %>% dbg("Row counts by geo (final)")

# Post-enforcement coverage check (should be 100%)
still_missing <- anti_join(expected_geo, fed_inv_geo, by = c("geo","geo_name"))
dbg(still_missing, "Post-enforcement: still missing (should be empty)")

# ---------- 7) Optional extra QA ----------
# a) Top by total (overall)
fed_inv_geo %>%
  arrange(desc(total_bil_ira_grants)) %>%
  slice_head(n = 10) %>%
  dbg("Top 10 by total_bil_ira_grants (all geos)")

# b) Totals by funding source — raw vs county rollup sanity check
raw_source_totals <- ira_bil_keys %>%
  filter(!is.na(county_fips_2020)) %>%                     # apples-to-apples with rollup needing county
  mutate(source = case_when(
    funding_source == "BIL" ~ "BIL_grants",
    funding_source == "IRA" ~ "IRA_grants",
    TRUE                    ~ funding_source
  )) %>%
  group_by(source) %>%
  summarize(raw_total = sum(funding_amount, na.rm = TRUE), .groups = "drop")

county_source_totals <- fed_inv_geo %>%
  filter(geo == "County") %>%
  summarize(
    BIL_grants = sum(BIL_grants, na.rm = TRUE),
    IRA_grants = sum(IRA_grants, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  tidyr::pivot_longer(everything(), names_to = "source", values_to = "county_rollup_total")

source_total_check <- raw_source_totals %>%
  full_join(county_source_totals, by = "source") %>%
  mutate(delta = county_rollup_total - raw_total)

dbg(raw_source_totals,   "Raw source totals (county-addressable)")
dbg(county_source_totals,"County rollup totals (should be close to raw source totals)")
dbg(source_total_check,  "Source totals check (delta)")

# c) Per-geo-type sanity: expected vs final
per_type_expected <- expected_geo %>% count(geo, name = "n_expected")
per_type_final    <- fed_inv_geo   %>% count(geo, name = "n_final")
dbg(per_type_expected, "Expected per geo type (from geo_long)")
dbg(per_type_final,    "Final per geo type (after enforcement)")

# d) Metro-specific QA (now that CBSA rollups are included)
if ("Metro Area" %in% fed_inv_geo$geo) {
  fed_inv_geo %>%
    filter(geo == "Metro Area") %>%
    summarise(
      n_metros = dplyr::n(),
      total_bil  = sum(BIL_grants, na.rm = TRUE),
      total_ira  = sum(IRA_grants, na.rm = TRUE),
      total_all  = sum(total_bil_ira_grants, na.rm = TRUE)
    ) %>%
    dbg("Metro Area grand totals (counts & sums)")
  
  fed_inv_geo %>%
    filter(geo == "Metro Area") %>%
    arrange(desc(total_bil_ira_grants)) %>%
    slice_head(n = 10) %>%
    dbg("Top 10 Metro Areas by total_bil_ira_grants (final)")
}

================================================================================
# END fed_inv_geo.R
================================================================================


================================================================================
# BEGIN demographics.R
================================================================================

###############################################################
# ACS 5-year 2023 Vitality + Life Expectancy — RIGOROUS & FLEXIBLE
# - Pulls 2023 ACS 5-year county data (via getCensus)
# - Robust NA/sentinel handling
# - CT 2023 Planning Region -> Legacy County reallocation (auto-detect shares)
# - Uses *deduplicated* county attributes (no spurious row multiplication)
# - CD-119 aggregation uses official GEOCORR county<->CD shares (not ad‑hoc)
# - CBSA/PEA/State/County aggregations built cleanly
# - Life Expectancy (IHME 2019) aggregated with ACS-2023 weights
# - Heavy debugging & assertions at each step; nothing omitted
###############################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(tidyr)
  library(purrr)
  library(rlang)
  if (requireNamespace("sf", quietly = TRUE)) library(sf)
})

options(dplyr.summarise.inform = FALSE)
options(stringsAsFactors = FALSE)

# ============================================================
# 0) Helpers (debug, column-finding, assertions)
# ============================================================

if (!exists("dbg")) {
  dbg <- function(x, name = deparse(substitute(x)), n = 8) {
    cat("\n====", name, "====\n")
    cat("Rows:", tryCatch(nrow(x), error = function(e) NA_integer_),
        " Cols:", tryCatch(ncol(x), error = function(e) NA_integer_), "\n")
    if (is.data.frame(x) || inherits(x, "tbl")) {
      print(dplyr::as_tibble(x) %>% head(n))
    } else {
      print(utils::head(x, n))
    }
    invisible(x)
  }
}

dbg_names <- function(df, name = deparse(substitute(df))) {
  cat("\n---- COLNAMES:", name, "----\n")
  print(names(df))
  invisible(df)
}

dbg_na_counts <- function(df, top = 20, name = deparse(substitute(df))) {
  cs <- tryCatch(sapply(df, function(col) sum(is.na(col))), error = function(e) NULL)
  if (!is.null(cs)) {
    cs <- sort(cs, decreasing = TRUE)
    cat("\n---- NA counts (top", top, ") for", name, "----\n")
    print(utils::head(cs, top))
  }
  invisible(cs)
}

approx_equal <- function(a, b, tol = 1e-6) isTRUE(all.equal(as.numeric(a), as.numeric(b), tolerance = tol))
assert_approx_equal <- function(a, b, what, tol = 1e-6) {
  if (!approx_equal(a, b, tol)) stop(sprintf("[CHECK FAILED] %s not conserved (%.6f vs %.6f)", what, as.numeric(a), as.numeric(b)))
}
assert_between <- function(x, lo, hi, what = deparse(substitute(x))) {
  bad <- sum(!(x >= lo & x <= hi), na.rm = TRUE)
  if (bad > 0) warning(sprintf("[RANGE WARN] %s has %s values outside [%g, %g]", what, bad, lo, hi))
  invisible(bad)
}

if (!exists("fix_df")) fix_df <- function(x) x

canonize <- function(x) tolower(gsub("[^a-z0-9]", "", x))

find_col <- function(df, aliases, required = TRUE, verbose = TRUE) {
  nms <- names(df) %||% character()
  cn  <- canonize(nms)
  
  alias_all <- unique(unlist(lapply(aliases, function(a) {
    a <- as.character(a)
    c(
      a,
      gsub("[ _]+", ".", a),
      gsub("[ .]+", "_", a),
      gsub("[._]+", " ", a)
    )
  })))
  alias_can <- canonize(alias_all)
  
  hits <- which(cn %in% alias_can)
  if (length(hits) == 1L) {
    picked <- nms[hits]
    if (verbose) cat(sprintf("  [find_col] matched '%s' -> '%s'\n", paste(aliases, collapse = " | "), picked))
    return(picked)
  } else if (length(hits) > 1L) {
    picked <- nms[hits[1]]
    if (verbose) cat(sprintf("  [find_col] multiple matches for '%s' -> picking '%s' (candidates: %s)\n",
                             paste(aliases, collapse=" | "), picked, paste(nms[hits], collapse=", ")))
    return(picked)
  } else {
    if (required) stop(sprintf("[find_col] Could not find any of: %s", paste(aliases, collapse=", ")))
    if (verbose) cat(sprintf("  [find_col] none matched for '%s'\n", paste(aliases, collapse=" | ")))
    return(NULL)
  }
}

alias_into <- function(df, target, aliases, verbose = TRUE) {
  if (target %in% names(df)) return(df)
  src <- find_col(df, aliases, required = FALSE, verbose = verbose)
  if (!is.null(src)) {
    df[[target]] <- df[[src]]
    if (verbose) cat(sprintf("  [alias_into] created '%s' from '%s'\n", target, src))
  } else {
    df[[target]] <- NA
    if (verbose) cat(sprintf("  [alias_into] created '%s' as NA (no alias found)\n", target))
  }
  df
}

`%||%` <- function(a, b) if (!is.null(a)) a else b

geo_label_map <- c(
  "State.Name" = "State",
  "cd_119"     = "Congressional District",
  "PEA"        = "Economic Area",
  "CBSA Title" = "Metro Area",
  "GeoName"    = "County"
)

first_non_na <- function(x) {
  ix <- which(!is.na(x))
  if (length(ix)) x[ix[1]] else NA
}

safe_n_distinct <- function(x) tryCatch(dplyr::n_distinct(x), error = function(e) NA_integer_)

# ============================================================
# 1) Pull ACS 5-year 2023 (county)
# ============================================================

acs_vars <- c("NAME","B19013_001E","B17020_001E","B99172_001E","C18120_003E","C18120_002E","B23025_001E","B25004_001E","B01003_001E")

acs_5yr_23_raw <- getCensus(
  name     = "acs/acs5",
  vars     = acs_vars,
  region   = "county:*",
  regionin = "state:*",
  vintage  = 2023
) %>% fix_df()

dbg(acs_5yr_23_raw, "acs_5yr_23_raw")
dbg_names(acs_5yr_23_raw, "acs_5yr_23_raw")

acs_5yr_23 <- acs_5yr_23_raw %>%
  mutate(
    state        = stringr::str_pad(state, 2, pad = "0"),
    county       = stringr::str_pad(county, 3, pad = "0"),
    county_geoid = paste0(state, county)
  ) %>%
  fix_df()

dbg(acs_5yr_23, "acs_5yr_23 (with county_geoid)")

# Sentinels -> NA
acs_sentinels <- c(-666666666, -222222222, -888888888, -999999999)
to_na <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  dplyr::na_if(x, acs_sentinels[1]) |>
    dplyr::na_if(acs_sentinels[2]) |>
    dplyr::na_if(acs_sentinels[3]) |>
    dplyr::na_if(acs_sentinels[4])
}

acs_5yr_23 <- acs_5yr_23 %>%
  mutate(
    across(c(B19013_001E, B17020_001E, B99172_001E, C18120_003E, C18120_002E,
             B23025_001E, B25004_001E, B01003_001E), to_na)
  )
dbg(acs_5yr_23 %>% select(NAME, starts_with("B")), "acs_5yr_23 (after sentinel NA handling)")
dbg_na_counts(acs_5yr_23, name = "acs_5yr_23 (post-sentinel)")

# ============================================================
# 2) CT fix — convert 2023 Planning Regions back to legacy 8 counties
# ============================================================

if (!exists("ct_fips_changes_raw")) {
  warning("[CT xwalk] 'ct_fips_changes_raw' not found; CT reallocation will be skipped.")
  ct_fips_changes_raw <- tibble::tibble()
}
if (!exists("tigris_counties_2020_raw")) {
  warning("[CT names] 'tigris_counties_2020_raw' not found; will synthesize names.")
  tigris_counties_2020_raw <- tibble::tibble(STATEFP = character(), GEOID = character(), NAMELSAD = character())
}

dbg_names(ct_fips_changes_raw, "ct_fips_changes_raw")
dbg(ct_fips_changes_raw, "ct_fips_changes_raw (preview)")

stopifnot("county_geoid" %in% names(acs_5yr_23))
acs_ct  <- acs_5yr_23 %>% filter(state == "09")
acs_non <- acs_5yr_23 %>% filter(state != "09")
dbg(acs_ct,  "acs_ct (CT rows as returned by ACS)")
dbg(acs_non, "acs_non (non-CT rows)")

n_ct_units <- dplyr::n_distinct(acs_ct$county)
ct_is_planning_regions <- FALSE
if (nrow(acs_ct) > 0) {
  has_pr_phrase <- tryCatch(any(grepl("Planning Region", acs_ct$NAME, ignore.case = TRUE)), error = function(e) FALSE)
  ct_is_planning_regions <- has_pr_phrase || n_ct_units >= 9
}
cat(sprintf("\n[CT DETECT] n_distinct(CT units) = %s; planning_regions? %s\n",
            n_ct_units, as.character(ct_is_planning_regions)))

# CT county names from 2020 counties
if (nrow(tigris_counties_2020_raw) > 0) {
  tigris_counties_2020_raw <- tigris_counties_2020_raw %>%
    alias_into("STATEFP", c("STATEFP","statefp","state_fp")) %>%
    alias_into("GEOID",   c("GEOID","geoid","GEOID10","geoid10")) %>%
    alias_into("NAMELSAD",c("NAMELSAD","namelsad","NAME","name"))
  ct_names_ref <- (if ("sf" %in% class(tigris_counties_2020_raw)) sf::st_drop_geometry(tigris_counties_2020_raw) else tigris_counties_2020_raw) %>%
    filter(STATEFP == "09") %>%
    transmute(ct_county_geoid = GEOID,
              NAME_ref = paste0(NAMELSAD, ", Connecticut"))
} else {
  ct_names_ref <- tibble::tibble(ct_county_geoid = character(), NAME_ref = character())
}
dbg(ct_names_ref, "ct_names_ref (from 2020 tigris counties)")

ct_fix_applied <- FALSE
if (ct_is_planning_regions && nrow(ct_fips_changes_raw) > 0) {
  
  # Flexible column detection (works with dotted or spaced headers)
  county_col <- find_col(
    ct_fips_changes_raw,
    c("CT county geoid","CT county FIPS","county fips","ct county code","ct_county_geoid","ctcountyfips",
      "County.code"),
    required = FALSE
  )
  pr_col <- find_col(
    ct_fips_changes_raw,
    c("planning region geoid","planning region fips","PR geoid","PR fips","pr_geoid","prfips","planningregionfips",
      "Connecticut.planning.region"),
    required = FALSE
  )
  pr_name_col <- find_col(
    ct_fips_changes_raw,
    c("Planning Region name","PR name","Region name","County name","County.name"),
    required = FALSE
  )
  ct_name_pre_col <- find_col(
    ct_fips_changes_raw,
    c("CT county name pre-2023","CT county name pre 2023","legacy ct county name","ct county name (pre 2023)",
      "CT.county.name.pre.2023"),
    required = FALSE
  )
  pop20_col <- find_col(
    ct_fips_changes_raw,
    c("Total population (2020 Census)","Pop 2020","Population 2020","pop20","total_pop_2020",
      "Total.population..2020.Census."),
    required = FALSE
  )
  share_a_col <- find_col(
    ct_fips_changes_raw,
    c("share PR to CT","share PR->CT","share pr to county","share_pr_to_ct",
      "CTcounty.to.county.allocation.factor"),
    required = FALSE
  )
  share_b_col <- find_col(
    ct_fips_changes_raw,
    c("share CT to PR","share CT->PR","share county to PR","share_ct_to_pr",
      "county.to.CTcounty.allocation.factor"),
    required = FALSE
  )
  
  # If both point to same, try alternate
  if (!is.null(share_a_col) && !is.null(share_b_col) && share_a_col == share_b_col) {
    candidates <- grep("allocation\\.factor|share", names(ct_fips_changes_raw), value = TRUE)
    alt <- setdiff(candidates, share_a_col)
    if (length(alt) > 0) {
      cat(sprintf("[CT XWALK] share_a_col == share_b_col (%s). Selecting alternate '%s' for share_b.\n",
                  share_a_col, alt[1]))
      share_b_col <- alt[1]
    }
  }
  
  cat("\n[CT XWALK DETECT]\n",
      " county_col:  ", county_col,  "\n",
      " pr_col:      ", pr_col,      "\n",
      " pr_name_col: ", pr_name_col, "\n",
      " ct_name_pre: ", ct_name_pre_col, "\n",
      " pop20_col:   ", pop20_col,   "\n",
      " share_a_col: ", share_a_col, "\n",
      " share_b_col: ", share_b_col, "\n", sep = "")
  
  need_cols <- c(county_col, pr_col, share_a_col, share_b_col)
  if (any(is.na(need_cols) | need_cols == "")) {
    warning("[CT xwalk] Required columns not found; skipping CT reallocation.")
  } else {
    ct_xwalk0 <- ct_fips_changes_raw %>%
      mutate(
        county_col_chr = stringr::str_pad(as.character(.data[[county_col]]), 5, pad = "0"),
        pr_col_chr     = stringr::str_pad(as.character(.data[[pr_col]]),     5, pad = "0")
      ) %>%
      filter(str_detect(county_col_chr, "^09\\d{3}$")) %>%
      transmute(
        ct_county_geoid = county_col_chr,
        pr_geoid        = pr_col_chr,
        pr_name         = if (!is.null(pr_name_col)) .data[[pr_name_col]] else NA_character_,
        ct_county_name0 = if (!is.null(ct_name_pre_col)) .data[[ct_name_pre_col]] else NA_character_,
        pop20           = suppressWarnings(as.integer(.data[[pop20_col]])) %||% NA_integer_,
        share_a         = suppressWarnings(as.numeric(.data[[share_a_col]])),
        share_b         = suppressWarnings(as.numeric(.data[[share_b_col]]))
      ) %>%
      mutate(ct_county_name = str_replace(ct_county_name0 %||% "", " CT$", " County, Connecticut"))
    
    dbg(ct_xwalk0, "ct_xwalk0 (raw)")
    
    # Auto-detect share directions
    pr_sums_a <- ct_xwalk0 %>% group_by(pr_geoid)        %>% summarise(sum_a = sum(share_a, na.rm = TRUE), .groups="drop")
    pr_sums_b <- ct_xwalk0 %>% group_by(pr_geoid)        %>% summarise(sum_b = sum(share_b, na.rm = TRUE), .groups="drop")
    ct_sums_a <- ct_xwalk0 %>% group_by(ct_county_geoid) %>% summarise(sum_a = sum(share_a, na.rm = TRUE), .groups="drop")
    ct_sums_b <- ct_xwalk0 %>% group_by(ct_county_geoid) %>% summarise(sum_b = sum(share_b, na.rm = TRUE), .groups="drop")
    
    dev_pr_a <- mean(abs(pr_sums_a$sum_a - 1), na.rm = TRUE)
    dev_pr_b <- mean(abs(pr_sums_b$sum_b - 1), na.rm = TRUE)
    dev_ct_a <- mean(abs(ct_sums_a$sum_a - 1), na.rm = TRUE)
    dev_ct_b <- mean(abs(ct_sums_b$sum_b - 1), na.rm = TRUE)
    
    cat(sprintf("\n[CT SHARE AUTODETECT]\n  mean|PR(sum(a)-1)| = %.6f\n  mean|PR(sum(b)-1)| = %.6f\n  mean|CT(sum(a)-1)| = %.6f\n  mean|CT(sum(b)-1)| = %.6f\n",
                dev_pr_a, dev_pr_b, dev_ct_a, dev_ct_b))
    
    if (dev_pr_a <= dev_pr_b && dev_ct_b <= dev_ct_a) {
      map <- c(pr2ct = "share_a", ct2pr = "share_b")
    } else if (dev_pr_b <= dev_pr_a && dev_ct_a <= dev_ct_b) {
      map <- c(pr2ct = "share_b", ct2pr = "share_a")
    } else {
      if (dev_pr_a <= dev_pr_b) map <- c(pr2ct = "share_a", ct2pr = "share_b") else map <- c(pr2ct = "share_b", ct2pr = "share_a")
      warning("[CT share autodetect] Ambiguous, chose best-by-PR heuristic.")
    }
    cat(sprintf("[CT SHARE MAP] PR→CT = %s ; CT→PR = %s\n", map["pr2ct"], map["ct2pr"]))
    
    ct_xwalk <- ct_xwalk0 %>%
      transmute(
        ct_county_geoid, pr_geoid, pr_name, ct_county_name0, ct_county_name, pop20,
        share_pr_to_ct = .data[[map["pr2ct"]]],
        share_ct_to_pr = .data[[map["ct2pr"]]]
      )
    
    # Sanity
    xw_check1 <- ct_xwalk %>% group_by(pr_geoid)        %>% summarise(sum_share = sum(share_pr_to_ct, na.rm = TRUE), .groups="drop")
    xw_check2 <- ct_xwalk %>% group_by(ct_county_geoid) %>% summarise(sum_share = sum(share_ct_to_pr, na.rm = TRUE), .groups="drop")
    dbg(xw_check1 %>% mutate(dev=abs(sum_share-1)) %>% arrange(desc(dev)) %>% head(10), "xw_check1 PR->CT sum shares (top deviances)")
    dbg(xw_check2 %>% mutate(dev=abs(sum_share-1)) %>% arrange(desc(dev)) %>% head(10), "xw_check2 CT->PR sum shares (top deviances)")
    if (any(abs(xw_check1$sum_share - 1) > 1e-3)) warning("Some PR -> county shares do not sum to ~1")
    if (any(abs(xw_check2$sum_share - 1) > 1e-3)) warning("Some CT county -> PR shares do not sum to ~1")
    
    # Allocate PR values back to legacy CT counties
    count_vars  <- c("B17020_001E","B99172_001E","C18120_003E","C18120_002E","B23025_001E","B25004_001E","B01003_001E")
    median_like <- "B19013_001E"
    
    n_ct_before <- nrow(acs_ct)
    cat(sprintf("[CT] n_ct_before (rows) = %s\n", n_ct_before))
    
    acs_ct_alloc_expanded <- acs_ct %>%
      inner_join(ct_xwalk %>% select(pr_geoid, ct_county_geoid, share_pr_to_ct),
                 by = c("county_geoid" = "pr_geoid")) %>%
      mutate(w_pop_alloc = B01003_001E * share_pr_to_ct) %>%
      mutate(across(all_of(count_vars), ~ .x * share_pr_to_ct))
    
    dbg(acs_ct_alloc_expanded, "acs_ct_alloc_expanded (CT only, expanded by shares)")
    
    acs_ct_counties <- acs_ct_alloc_expanded %>%
      group_by(ct_county_geoid) %>%
      summarise(
        across(all_of(count_vars), \(x) sum(x, na.rm = TRUE)),
        B19013_001E = if (sum(w_pop_alloc, na.rm = TRUE) > 0)
          sum(B19013_001E * w_pop_alloc, na.rm = TRUE) / sum(w_pop_alloc, na.rm = TRUE)
        else NA_real_,
        .groups = "drop"
      ) %>%
      mutate(
        state        = "09",
        county       = str_sub(ct_county_geoid, 3, 5),
        county_geoid = ct_county_geoid
      ) %>%
      left_join(ct_names_ref, by = "ct_county_geoid") %>%
      mutate(NAME = coalesce(NAME_ref, paste0(ct_county_geoid, " (CT legacy county)"))) %>%
      select(state, county, NAME, all_of(c(median_like, count_vars)), county_geoid)
    
    dbg(acs_ct_counties, "acs_ct_counties (legacy 8 counties reconstructed)")
    
    # Conservation on additive counts
    if (nrow(acs_ct_counties) == 8L) {
      ct_tot_before <- acs_ct  %>% summarise(across(all_of(count_vars), ~ sum(.x, na.rm = TRUE)))
      ct_tot_after  <- acs_ct_counties %>% summarise(across(all_of(count_vars), ~ sum(.x, na.rm = TRUE)))
      for (v in count_vars) assert_approx_equal(ct_tot_before[[v]], ct_tot_after[[v]], paste0("CT total for ", v), tol = 1e-4)
      
      acs_5yr_23 <- bind_rows(acs_non, acs_ct_counties) %>% arrange(state, county)
      dbg(acs_5yr_23, "acs_5yr_23 (CT legacy counties in place)")
      ct_fix_applied <- TRUE
    } else {
      warning(sprintf("[CT] Expected 8 legacy counties after reallocation, got %s; leaving CT rows unchanged.", nrow(acs_ct_counties)))
      ct_fix_applied <- FALSE
    }
  }
} else {
  cat("[CT] Reallocation not needed (already legacy counties) or crosswalk missing.\n")
}

# ============================================================
# 3) Metrics (legacy definitions)
# ============================================================

acs_5yr_23 <- acs_5yr_23 %>%
  rename(
    med_house_inc = B19013_001E,
    pov_tot       = B17020_001E,
    pov_family    = B99172_001E,
    empl          = C18120_003E,
    lab_force     = C18120_002E,
    emp_21        = B23025_001E,
    vacancy       = B25004_001E,
    pop           = B01003_001E
  ) %>%
  mutate(
    unemp        = (1 - empl / lab_force) * 100,
    pov_rate     = (1 - pov_tot / pop) * 100,
    emp_pop      = (empl / pop) * 100,
    med_inc_perc = med_house_inc / median(med_house_inc, na.rm = TRUE) * 100
  )

dbg(acs_5yr_23 %>% select(NAME, pop, med_house_inc, unemp, pov_rate) %>% head(10),
    "acs_5yr_23 (post metrics)")
assert_between(acs_5yr_23$unemp,    -50, 50,  "unemp")
assert_between(acs_5yr_23$pov_rate, -50, 100, "pov_rate")
dbg_na_counts(acs_5yr_23, name = "acs_5yr_23 (post metrics)")

# ============================================================
# 4) County attributes join (strict one-row-per-county)
#    - We dedupe attributes first to avoid row multipliers
#    - CD mapping handled separately by GEOCORR in step 5
# ============================================================

if (!exists("geo")) {
  warning("[geo] 'geo' not found; will rely on COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA if available.")
  geo <- tibble::tibble(fips = character())
}
if (!exists("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA")) {
  warning("[crosswalk] 'COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA' not found; may have fewer attributes.")
  COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA <- tibble::tibble()
}

# Build a single-row-per-county attribute frame using best available source(s)
make_geo_attrs_from_geo <- function(geo_df) {
  if (!nrow(geo_df)) return(tibble::tibble(fips=character()))
  # prefer rows with higher percent_district (if present), then those with CBSA/PEA present
  geo_df %>%
    mutate(
      pd_rank = if ("percent_district" %in% names(.)) coalesce(percent_district, 0) else 0,
      has_cbsa = if ("CBSA Title" %in% names(.)) !is.na(`CBSA Title`) else FALSE,
      has_pea  = if ("PEA" %in% names(.)) !is.na(PEA) else FALSE
    ) %>%
    arrange(fips, desc(pd_rank), desc(has_cbsa), desc(has_pea)) %>%
    group_by(fips) %>%
    slice_head(n = 1) %>%
    ungroup() %>%
    select(any_of(c(
      "fips","Region","Division","State.Name","state_abbr","state.fips",
      "GeoName","CBSA Title","CBSA Code","PEA","FCC_PEA_Number","gdp","msa_gdp_2022"
    )))
}

make_geo_attrs_from_ccs <- function(ccs) {
  if (!nrow(ccs)) return(tibble::tibble(fips=character()))
  ccs %>%
    alias_into("STATE_NAME",         c("STATE_NAME","State.Name","state_name")) %>%
    alias_into("STATE_ABBREVIATION", c("STATE_ABBREVIATION","state_abbr","STUSPS")) %>%
    alias_into("COUNTY_GEOID",       c("COUNTY_GEOID","fips","county_geoid","GEOID")) %>%
    alias_into("COUNTY_NAME",        c("COUNTY_NAME","GeoName","County Name","County.Name","NAME")) %>%
    alias_into("CBSA_NAME",          c("CBSA_NAME","CBSA Title","cbsa name","CBSA")) %>%
    alias_into("CBSA_GEOID",         c("CBSA_GEOID","CBSA Code","CBSAFP")) %>%
    alias_into("PEA_NAME",           c("PEA_NAME","PEA")) %>%
    alias_into("PEA_NUMBER",         c("PEA_NUMBER","FCC_PEA_Number")) %>%
    alias_into("COUNTY_GDP_2022",    c("COUNTY_GDP_2022","gdp")) %>%
    alias_into("2022_COUNTY_POPULATION", c("2022_COUNTY_POPULATION","pop")) %>%
    transmute(
      fips          = stringr::str_pad(COUNTY_GEOID, 5, pad="0"),
      State.Name    = STATE_NAME,
      state_abbr    = STATE_ABBREVIATION,
      GeoName       = COUNTY_NAME,
      `CBSA Title`  = CBSA_NAME,
      `CBSA Code`   = CBSA_GEOID,
      PEA           = PEA_NAME,
      FCC_PEA_Number= PEA_NUMBER,
      gdp           = COUNTY_GDP_2022,
      msa_gdp_2022  = NA_real_
    ) %>% distinct(fips, .keep_all = TRUE)
}

geo_attrs_geo <- try(make_geo_attrs_from_geo(geo), silent = TRUE)
if (inherits(geo_attrs_geo, "try-error")) geo_attrs_geo <- tibble::tibble(fips=character())

geo_attrs_ccs <- try(make_geo_attrs_from_ccs(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA), silent = TRUE)
if (inherits(geo_attrs_ccs, "try-error")) geo_attrs_ccs <- tibble::tibble(fips=character())

# Merge sources, prefer 'geo' then fill from 'ccs'
geo_attrs <- full_join(geo_attrs_geo, geo_attrs_ccs, by = "fips", suffix = c("", ".ccs"))

# Robust coalesce from *.ccs if present (avoid referencing non-existent columns)
fill_cols <- c("Region","Division","State.Name","state_abbr","GeoName","CBSA Title","CBSA Code","PEA","FCC_PEA_Number","gdp","msa_gdp_2022")
for (col in fill_cols) {
  col_ccs <- paste0(col, ".ccs")
  if (!col %in% names(geo_attrs)) geo_attrs[[col]] <- NA
  if (col_ccs %in% names(geo_attrs)) {
    geo_attrs[[col]] <- dplyr::coalesce(geo_attrs[[col]], geo_attrs[[col_ccs]])
  }
}
geo_attrs <- geo_attrs %>%
  select(fips, all_of(fill_cols)) %>%
  distinct(fips, .keep_all = TRUE)

dbg(head(geo_attrs, 10), "geo_attrs (deduped single-row county attributes)")
cat("[GEO ATTRS] n counties in attrs:", nrow(geo_attrs), "\n")

# Strict one-to-one join
n_before_join <- nrow(acs_5yr_23)
acs_5yr_23 <- acs_5yr_23 %>%
  mutate(fips = county_geoid) %>%
  left_join(geo_attrs, by = "fips")

n_after_join <- nrow(acs_5yr_23)
cat(sprintf("[JOIN CHECK] rows before: %s | after: %s | delta: %+d (should be 0)\n",
            n_before_join, n_after_join, n_after_join - n_before_join))
if (n_after_join != n_before_join) warning("[JOIN CHECK] County attribute join changed row count; investigate upstream dedupe.")

dbg_names(acs_5yr_23, "acs_5yr_23 (after geo attrs join)")
dbg_na_counts(acs_5yr_23, name = "acs_5yr_23 (post geo attrs join)")

# Normalize geography column names in case callers pass their own lists
acs_5yr_23 <- acs_5yr_23 %>%
  alias_into("State.Name", c("State.Name","state_name","STATE_NAME","state")) %>%
  alias_into("PEA",        c("PEA","pea","Economic Area","economic_area")) %>%
  alias_into("CBSA Title", c("CBSA Title","cbsa title","CBSA.Title","cbsa_title","CBSA")) %>%
  alias_into("GeoName",    c("GeoName","geo_name","County name","County Name","County.Name","county_name"))

# ==========================================================================
# 5) Aggregation geographies selection + CD mapping (GEOCORR official table)
# ==========================================================================

# Geographies input(s)
geo_default <- c("State.Name","cd_119","PEA","CBSA Title","GeoName")
geographies <- unique(c(
  get0("geographies", ifnotfound = character()),
  get0("geographies_clean", ifnotfound = character()),
  geo_default
))
geographies <- geographies[geographies %in% c("State.Name","cd_119","PEA","CBSA Title","GeoName")]
geographies <- unique(geographies)
cat("\n[GEOGRAPHIES REQUESTED] ", paste(geographies, collapse = ", "), "\n", sep = "")

# Build CD mapping from GEOCORR (do NOT rely on 'geo' percent_district)
if (!exists("geocorr_county_2020_cd_119")) {
  warning("[GEOCORR] 'geocorr_county_2020_cd_119' not found; CD aggregation will be skipped.")
  geocorr_county_2020_cd_119 <- tibble::tibble()
}
if (!exists("geocorr_ct_county_cd_119")) {
  warning("[GEOCORR-CT] 'geocorr_ct_county_cd_119' not found; CT CD shares may be incomplete.")
  geocorr_ct_county_cd_119 <- tibble::tibble()
}

# General US county->CD map
if (nrow(geocorr_county_2020_cd_119)) {
  gc <- geocorr_county_2020_cd_119 %>%
    alias_into("County.code",  c("County code","COUNTY_GEOID","County GEOID","County geoid")) %>%
    alias_into("State.abbr",   c("State abbr.","STATE_ABBREVIATION","state_abbr","STUSPS")) %>%
    alias_into("CD.code",      c("Congressional district code (119th Congress)","CD119_code","CD119FP")) %>%
    alias_into("CTY2CD",       c("county-to-cd119 allocation factor","county to cd119 allocation factor","cty2cd")) %>%
    transmute(
      fips   = stringr::str_pad(`County.code`, 5, pad = "0"),
      cd_num = stringr::str_pad(`CD.code`, 2, pad = "0"),
      state_abbr = `State.abbr`,
      cd_119 = paste0(state_abbr, "-", cd_num),
      county_to_cd_share = as.numeric(CTY2CD)
    )
} else gc <- tibble::tibble()

# CT legacy county->CD map (overrides, if present)
if (nrow(geocorr_ct_county_cd_119)) {
  gc_ct <- geocorr_ct_county_cd_119 %>%
    alias_into("County.code", c("County code","COUNTY_GEOID","County GEOID")) %>%
    alias_into("CD.code",     c("Congressional district code (119th Congress)","CD119_code")) %>%
    alias_into("CTY2CD",      c("CTcounty-to-cd119 allocation factor","ct county to cd119 allocation factor","CTcounty.to.cd119.allocation.factor")) %>%
    mutate(
      fips   = stringr::str_pad(`County.code`, 5, pad = "0"),
      cd_num = stringr::str_pad(`CD.code`, 2, pad = "0")
    ) %>%
    transmute(
      fips,
      cd_119 = paste0("CT-", cd_num),
      county_to_cd_share = as.numeric(CTY2CD)
    )
} else gc_ct <- tibble::tibble()

# Combine US + CT override (replace rows where fips starts with "09")
cd_map <- bind_rows(
  gc %>% filter(!stringr::str_detect(fips, "^09")),
  gc_ct
) %>%
  filter(!is.na(fips), !is.na(cd_119), !is.na(county_to_cd_share)) %>%
  group_by(fips) %>%
  mutate(sum_share_by_county = sum(county_to_cd_share, na.rm = TRUE)) %>%
  ungroup()

dbg(cd_map %>% head(12), "cd_map (county -> CD shares)")

# Sanity: by-county shares ~ 1
cd_county_devs <- cd_map %>%
  distinct(fips, sum_share_by_county) %>%
  mutate(dev = abs(sum_share_by_county - 1)) %>%
  arrange(desc(dev))
dbg(cd_county_devs %>% head(25), "cd_map by-county sum of shares (largest devs)")
if (any(cd_county_devs$dev > 1e-3, na.rm = TRUE)) warning("[GEOCORR] Some county->CD share rows deviate notably from 1")

# ============================================================
# 6) Vitality Stats aggregation (State, CD, PEA, CBSA, County)
#    - CD uses cd_map to avoid duplicate rows or ad-hoc weights
# ============================================================

metric_cols <- c("med_house_inc", "med_inc_perc", "pov_rate", "emp_pop", "vacancy", "unemp")

agg_state <- function(df) {
  df %>%
    filter(!is.na(State.Name), State.Name != "") %>%
    distinct(State.Name, county_geoid, pop, !!!syms(metric_cols)) %>%
    group_by(State.Name) %>%
    summarise(across(all_of(metric_cols), ~ weighted.mean(.x, w = pop, na.rm = TRUE)), .groups = "drop") %>%
    mutate(geo = "State", geo_name = State.Name) %>%
    select(geo, geo_name, all_of(metric_cols))
}

agg_cbsa <- function(df) {
  if (!"CBSA Title" %in% names(df)) return(tibble::tibble())
  df %>%
    filter(!is.na(`CBSA Title`), `CBSA Title` != "") %>%
    distinct(`CBSA Title`, county_geoid, pop, !!!syms(metric_cols)) %>%
    group_by(`CBSA Title`) %>%
    summarise(across(all_of(metric_cols), ~ weighted.mean(.x, w = pop, na.rm = TRUE)), .groups = "drop") %>%
    mutate(geo = "Metro Area", geo_name = `CBSA Title`) %>%
    select(geo, geo_name, all_of(metric_cols))
}

agg_pea <- function(df) {
  if (!"PEA" %in% names(df)) return(tibble::tibble())
  df %>%
    filter(!is.na(PEA), PEA != "") %>%
    distinct(PEA, county_geoid, pop, !!!syms(metric_cols)) %>%
    group_by(PEA) %>%
    summarise(across(all_of(metric_cols), ~ weighted.mean(.x, w = pop, na.rm = TRUE)), .groups = "drop") %>%
    mutate(geo = "Economic Area", geo_name = PEA) %>%
    select(geo, geo_name, all_of(metric_cols))
}

agg_county <- function(df) {
  # Prefer GeoName title if present; fallback to NAME
  nm <- if ("GeoName" %in% names(df)) "GeoName" else "NAME"
  df %>%
    filter(!is.na(.data[[nm]]), .data[[nm]] != "") %>%
    distinct(.data[[nm]], county_geoid, !!!syms(metric_cols)) %>%
    mutate(geo = "County", geo_name = .data[[nm]]) %>%
    select(geo, geo_name, all_of(metric_cols))
}

agg_cd <- function(df, cd_map) {
  if (!nrow(cd_map)) return(tibble::tibble())
  # Join mapping and compute weights
  base_cd <- df %>%
    select(county_geoid, pop, !!!syms(metric_cols)) %>%
    mutate(fips = county_geoid) %>%
    inner_join(cd_map %>% select(fips, cd_119, county_to_cd_share), by = "fips") %>%
    mutate(w = pop * county_to_cd_share)
  dbg(base_cd %>% group_by(cd_119) %>% summarise(n=n(), w_sum=sum(w, na.rm=TRUE), .groups="drop") %>% head(12),
      "base_cd (from GEOCORR) summary")
  base_cd %>%
    group_by(cd_119) %>%
    summarise(
      across(all_of(metric_cols), \(x) if (sum(w, na.rm = TRUE) > 0) weighted.mean(x, w = w, na.rm = TRUE) else NA_real_),
      .groups = "drop"
    ) %>%
    mutate(geo = "Congressional District", geo_name = cd_119) %>%
    select(geo, geo_name, all_of(metric_cols))
}

vit_parts <- list()
if ("State.Name" %in% geographies) vit_parts[["State.Name"]] <- agg_state(acs_5yr_23)
if ("CBSA Title" %in% geographies) vit_parts[["CBSA Title"]] <- agg_cbsa(acs_5yr_23)
if ("PEA"        %in% geographies) vit_parts[["PEA"]]        <- agg_pea(acs_5yr_23)
if ("GeoName"    %in% geographies) vit_parts[["GeoName"]]    <- agg_county(acs_5yr_23)
if ("cd_119"     %in% geographies) vit_parts[["cd_119"]]     <- agg_cd(acs_5yr_23, cd_map)

# Show sample of each part
invisible(imap(vit_parts, ~ dbg(head(.x, 6), paste0("vit_g (", .y, ") sample"))))

vit <- bind_rows(vit_parts) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
  group_by(geo) %>%
  mutate(med_inc_rank = rank(-med_house_inc, ties.method = "min")) %>%
  ungroup()

dbg(vit, "vit (final)")
dbg_na_counts(vit, name = "vit (final)")

# ============================================================
# 7) Population table (parallel to vit, CD uses GEOCORR)
# ============================================================

pop_parts <- list()

if ("State.Name" %in% geographies) {
  pop_parts[["State.Name"]] <- acs_5yr_23 %>%
    distinct(State.Name, county_geoid, pop) %>%
    group_by(State.Name) %>%
    summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop") %>%
    mutate(geo = "State", geo_name = State.Name) %>%
    select(geo, geo_name, pop)
}

if ("CBSA Title" %in% geographies) {
  pop_parts[["CBSA Title"]] <- acs_5yr_23 %>%
    filter(!is.na(`CBSA Title`), `CBSA Title` != "") %>%
    distinct(`CBSA Title`, county_geoid, pop) %>%
    group_by(`CBSA Title`) %>%
    summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop") %>%
    mutate(geo = "Metro Area", geo_name = `CBSA Title`) %>%
    select(geo, geo_name, pop)
}

if ("PEA" %in% geographies) {
  pop_parts[["PEA"]] <- acs_5yr_23 %>%
    filter(!is.na(PEA), PEA != "") %>%
    distinct(PEA, county_geoid, pop) %>%
    group_by(PEA) %>%
    summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop") %>%
    mutate(geo = "Economic Area", geo_name = PEA) %>%
    select(geo, geo_name, pop)
}

if ("GeoName" %in% geographies) {
  nm <- if ("GeoName" %in% names(acs_5yr_23)) "GeoName" else "NAME"
  pop_parts[["GeoName"]] <- acs_5yr_23 %>%
    distinct(.data[[nm]], county_geoid, pop) %>%
    group_by(.data[[nm]]) %>%
    summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop") %>%
    mutate(geo = "County", geo_name = .data[[nm]]) %>%
    select(geo, geo_name, pop)
}

if ("cd_119" %in% geographies && nrow(cd_map)) {
  pop_parts[["cd_119"]] <- acs_5yr_23 %>%
    select(county_geoid, pop) %>%
    mutate(fips = county_geoid) %>%
    inner_join(cd_map %>% select(fips, cd_119, county_to_cd_share), by = "fips") %>%
    group_by(cd_119) %>%
    summarise(pop = sum(pop * county_to_cd_share, na.rm = TRUE), .groups = "drop") %>%
    mutate(geo = "Congressional District", geo_name = cd_119) %>%
    select(geo, geo_name, pop)
}

invisible(imap(pop_parts, ~ dbg(head(.x, 6), paste0("pop_df (", .y, ") sample"))))

pop <- bind_rows(pop_parts) %>%
  filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA")
dbg(pop, "pop (final)")

# ============================================================
# 8) Life Expectancy (IHME 2019) with ACS 2023 weights
#     - County weights: ACS 2023 population
#     - cd_119 aggregation uses GEOCORR shares
# ============================================================

life_expectancy_raw <- suppressWarnings(
  read.csv(
    file.path(
      paths$us_maps,"Econ","Life Expectancy","IHME_county_life_expectancy",
      "IHME_USA_LE_COUNTY_RACE_ETHN_2000_2019_LT_2019_BOTH_Y2022M06D16.csv"
    ),
    check.names = FALSE
  )
) %>% fix_df()

dbg_names(life_expectancy_raw, "life_expectancy_raw")
dbg(life_expectancy_raw, "life_expectancy_raw (preview)")
dbg_na_counts(life_expectancy_raw, name = "life_expectancy_raw")

life_expectancy_2019 <- life_expectancy_raw %>%
  alias_into("year",      c("year","Year","YEAR")) %>%
  alias_into("age_name",  c("age_name","age name","Age name")) %>%
  alias_into("race_name", c("race_name","race name","Race name")) %>%
  alias_into("sex_name",  c("sex_name","sex name","Sex name")) %>%
  alias_into("val",       c("val","Value","value")) %>%
  alias_into("fips",      c("fips","FIPS","county_fips","GEOID")) %>%
  alias_into("location_name", c("location_name","Location name","location")) %>%
  filter(
    year == 2019,
    age_name == "<1 year",
    race_name == "Total",
    sex_name == "Both"
  )

dbg(life_expectancy_2019, "life_expectancy_2019 (filtered: 2019/<1yr/Total/Both)")
dbg_na_counts(life_expectancy_2019, name = "life_expectancy_2019")

life_expectancy_county <- life_expectancy_2019 %>%
  filter(grepl("\\(", location_name)) %>%
  mutate(
    county_geoid = ifelse(nchar(as.character(fips)) == 4, paste0("0", fips), as.character(fips)),
    county_geoid = str_pad(county_geoid, 5, pad = "0")
  ) %>%
  filter(nchar(county_geoid) == 5) %>%
  select(county_geoid, le_val = val, location_name)

dbg(life_expectancy_county, "life_expectancy_county (2019 slice + FIPS padded)")
dbg_na_counts(life_expectancy_county, name = "life_expectancy_county")

# Unique ACS weights per county (avoid many-to-many)
acs_5yr_23_norm <- acs_5yr_23 %>%
  mutate(fips = county_geoid) %>%
  select(fips, pop, State.Name, `CBSA Title`, PEA, GeoName) %>%
  group_by(fips) %>%
  summarise(across(everything(), ~ first_non_na(.x)), .groups = "drop")

# Diagnostics: duplication in ACS (should be 1 row per fips now)
dup_fips <- acs_5yr_23_norm %>% count(fips) %>% filter(n>1)
if (nrow(dup_fips)) warning("[ACS canonical] Found duplicated fips in canonical weights table; check upstream joins.")
dbg(acs_5yr_23_norm %>% head(10), "acs_5yr_23_norm (weights + descriptors)")

# Legacy FIPS remap examples (rare; keep list short & explicit)
legacy_fips_remap <- tibble::tribble(
  ~from,   ~to,     ~note,
  "12025", "12086", "Dade -> Miami-Dade (FL) legacy alignment"
)
life_expectancy_county <- life_expectancy_county %>%
  left_join(legacy_fips_remap, by = c("county_geoid" = "from")) %>%
  mutate(county_geoid = if_else(!is.na(to), to, county_geoid)) %>%
  select(-to, -note)

# Gaps diagnostics
ihme_not_in_acs <- life_expectancy_county %>%
  distinct(county_geoid, location_name) %>%
  anti_join(acs_5yr_23_norm %>% distinct(fips), by = c("county_geoid" = "fips"))
dbg(ihme_not_in_acs, "IHME county FIPS not in ACS 5yr 2023 (post-remap)")

acs_not_in_ihme <- acs_5yr_23_norm %>%
  distinct(fips, GeoName) %>%
  anti_join(life_expectancy_county %>% distinct(county_geoid), by = c("fips" = "county_geoid"))
dbg(acs_not_in_ihme %>% head(25), "ACS 2023 FIPS not in IHME (head)")

# Join weights+descriptors
life_expectancy_weighted <- life_expectancy_county %>%
  left_join(
    acs_5yr_23_norm,
    by = c("county_geoid" = "fips")
  ) %>%
  rename(weight_pop = pop)

dbg(life_expectancy_weighted, "life_expectancy_weighted (IHME + ACS23 joined)")
dbg(life_expectancy_weighted %>% filter(is.na(weight_pop) | weight_pop <= 0),
    "Rows with missing/zero weights (ACS23)")
dbg(life_expectancy_weighted %>% filter(is.na(le_val)),
    "Rows with missing life expectancy values (IHME)")

# ---------- Life Expectancy aggregations ----------
agg_le_generic <- function(df, group_col) {
  gsym <- sym(group_col)
  clean <- df %>%
    filter(!is.na(!!gsym), !!gsym != "") %>%
    mutate(usable = !is.na(le_val) & !is.na(weight_pop) & weight_pop > 0)
  
  coverage <- clean %>%
    group_by(!!gsym) %>%
    summarise(
      pop_total  = sum(weight_pop %||% 0, na.rm = TRUE),
      pop_usable = sum(if_else(usable, weight_pop, 0), na.rm = TRUE),
      coverage   = if_else(pop_total > 0, pop_usable / pop_total, NA_real_),
      .groups = "drop"
    )
  
  estimates <- clean %>%
    filter(usable) %>%
    group_by(!!gsym) %>%
    summarise(
      life_expectancy = weighted.mean(le_val, w = weight_pop, na.rm = TRUE),
      n_counties      = n(),
      .groups = "drop"
    ) %>%
    left_join(coverage, by = as.character(gsym))
  
  low_cov <- estimates %>% filter(!is.na(coverage) & coverage < 0.98)
  if (nrow(low_cov) > 0) {
    dbg(low_cov %>% arrange(coverage) %>% head(25),
        paste0("LOW COVERAGE in ", group_col, " (≤98%; top 25 worst)"))
  }
  
  out <- estimates %>%
    transmute(
      geo      = unname(geo_label_map[[group_col]] %||% group_col),
      geo_name = !!gsym,
      life_expectancy, pop_total, pop_usable, coverage, n_counties
    ) %>%
    filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA")
  dbg(out %>% arrange(coverage) %>% head(10),
      paste0("Sample aggregated results: ", group_col))
  out
}

# State/PEA/CBSA/County directly
le_parts <- list()
if ("State.Name" %in% geographies) le_parts[["State.Name"]] <- agg_le_generic(life_expectancy_weighted, "State.Name")
if ("PEA"        %in% geographies) le_parts[["PEA"]]        <- agg_le_generic(life_expectancy_weighted, "PEA")
if ("CBSA Title" %in% geographies) le_parts[["CBSA Title"]] <- agg_le_generic(life_expectancy_weighted, "CBSA Title")
if ("GeoName"    %in% geographies) le_parts[["GeoName"]]    <- agg_le_generic(life_expectancy_weighted, "GeoName")

# CD: build county->CD weights (GEOCORR) then aggregate
if ("cd_119" %in% geographies && nrow(cd_map)) {
  le_cd_base <- life_expectancy_weighted %>%
    mutate(fips = county_geoid) %>%
    inner_join(cd_map %>% select(fips, cd_119, county_to_cd_share), by = "fips") %>%
    mutate(w = weight_pop * county_to_cd_share,
           usable = !is.na(le_val) & !is.na(w) & w > 0)
  
  dbg(le_cd_base %>% group_by(cd_119) %>% summarise(n=n(), w_sum=sum(w, na.rm=TRUE), .groups="drop") %>% head(12),
      "le_cd_base summary (GEOCORR shares)")
  
  le_cd_cov <- le_cd_base %>%
    group_by(cd_119) %>%
    summarise(
      pop_total  = sum(weight_pop * county_to_cd_share, na.rm = TRUE),         # share-weighted CD population
      pop_usable = sum(if_else(usable, weight_pop * county_to_cd_share, 0), na.rm = TRUE),
      coverage   = if_else(pop_total > 0, pop_usable / pop_total, NA_real_),
      .groups = "drop"
    )
  
  le_cd_est <- le_cd_base %>%
    filter(usable) %>%
    group_by(cd_119) %>%
    summarise(
      life_expectancy = weighted.mean(le_val, w = w, na.rm = TRUE),
      n_counties      = n_distinct(county_geoid),
      .groups = "drop"
    ) %>%
    left_join(le_cd_cov, by = "cd_119") %>%
    transmute(
      geo = "Congressional District",
      geo_name = cd_119,
      life_expectancy, pop_total, pop_usable, coverage, n_counties
    )
  
  le_parts[["cd_119"]] <- le_cd_est
  dbg(le_cd_est %>% arrange(coverage) %>% head(12), "le_cd_est (coverage sample)")
}

life_geo <- bind_rows(le_parts)
dbg(life_geo, "life_geo (aggregated across geographies)")

# ---------- 8b) Ensure final LE includes ALL geographies in `geo_long` ----------
life_geo_final_raw <- life_geo %>%
  select(geo, geo_name, life_expectancy)

dbg(life_geo_final_raw, "life_geo_final_raw (pre-padding)")

geo_long_norm <- get0("geo_long", ifnotfound = tibble::tibble())

if (nrow(geo_long_norm) > 0) {
  geo_long_norm <- geo_long_norm %>%
    alias_into("geo_type", c("geo_type","geo","type","geo.kind","geo_kind")) %>%
    alias_into("geo_name", c("geo_name","name","geo.name")) %>%
    transmute(geo = .data$geo_type, geo_name = .data$geo_name) %>%
    distinct()
  
  dbg(geo_long_norm %>% head(10), "geo_long_norm (distinct keys from caller)")
  
  # Rows in geo_long not present in life_geo_final_raw (to be added with NA LE)
  geo_long_only <- anti_join(geo_long_norm, life_geo_final_raw, by = c("geo","geo_name"))
  # Rows in life_geo_final_raw not present in geo_long (will be dropped by the pad-to-geo_long left join)
  le_only <- anti_join(life_geo_final_raw, geo_long_norm, by = c("geo","geo_name"))
  
  cat(sprintf("[PADDING] geo_long-only rows to add: %d | LE-only rows to drop when aligning to geo_long: %d\n",
              nrow(geo_long_only), nrow(le_only)))
  
  life_geo_final <- geo_long_norm %>%
    left_join(life_geo_final_raw, by = c("geo","geo_name")) %>%
    arrange(geo, geo_name)
  
  dbg(life_geo_final, "life_geo_final (padded to include ALL geo_long keys)")
} else {
  warning("[geo_long] Not found or empty; life_geo_final = life_geo_final_raw (no padding applied).")
  life_geo_final <- life_geo_final_raw
}

overall_coverage <- life_expectancy_weighted %>%
  summarize(
    counties_total        = n_distinct(county_geoid),
    counties_with_weights = n_distinct(county_geoid[!is.na(weight_pop) & weight_pop > 0]),
    counties_with_values  = n_distinct(county_geoid[!is.na(le_val)]),
    counties_usable       = n_distinct(county_geoid[!is.na(le_val) & !is.na(weight_pop) & weight_pop > 0])
  )
dbg(overall_coverage, "overall_coverage (county foundations, ACS23 weighting)")

worst_coverage <- life_geo %>%
  arrange(coverage) %>%
  filter(!is.na(coverage)) %>%
  head(15)
dbg(worst_coverage, "Worst coverage groups across all geographies (top 15)")

# ============================================================
# 9) Post-hoc QA snapshots
# ============================================================

cat("\n[QA COUNTS]\n",
    " unique counties (ACS after CT fix): ", safe_n_distinct(acs_5yr_23$county_geoid), "\n",
    " unique states: ", safe_n_distinct(acs_5yr_23$State.Name), "\n",
    " unique CBSAs (non-NA): ", safe_n_distinct(acs_5yr_23$`CBSA Title`[!is.na(acs_5yr_23$`CBSA Title`)]), "\n",
    " unique PEAs (non-NA): ", safe_n_distinct(acs_5yr_23$PEA[!is.na(acs_5yr_23$PEA)]), "\n",
    sep = "")

if ("cd_119" %in% geographies && nrow(cd_map)) {
  by_cd <- cd_map %>% group_by(cd_119) %>%
    summarise(counties = n_distinct(fips),
              sum_share   = sum(county_to_cd_share, na.rm = TRUE),
              .groups="drop")
  dbg(by_cd %>% arrange(desc(abs(sum_share - round(sum_share)))) %>% head(12),
      "cd_map (by CD summary: count of counties, sum-shares)")
}

# ============================================================
# 10) Summary
# ============================================================

cat("\n[SUMMARY]\n",
    " CT fix applied: ", ct_fix_applied, "\n",
    " Geographies used (vit/pop/le): ", paste(geographies, collapse = ", "), "\n",
    " vit rows: ", nrow(vit), " | pop rows: ", nrow(pop),
    " | life_geo rows (pre-pad): ", nrow(life_geo_final_raw),
    " | life_geo_final rows (aligned to geo_long if present): ", nrow(life_geo_final), "\n",
    sep = "")

cat("\n[SESSION INFO]\n")
print(sessionInfo())
# ------------------------------ End of Script ------------------------------

================================================================================
# END demographics.R
================================================================================


================================================================================
# BEGIN innovation_geo.R
================================================================================

# ====================== PREP: Quick glances at input data already in memory ======================
suppressPackageStartupMessages({
  library(dplyr); library(readr); library(tidyr); library(stringr); library(purrr)
  library(tibble); library(janitor); library(rlang)
})

options(warn = 1, scipen = 999)

# ---------- Small debug helpers ----------
debug_header <- function(msg) cat("\n", paste0(rep("=", 4), collapse=""), " ", msg, " ", paste0(rep("=", 4), collapse=""), "\n", sep = "")
debug_ok <- function(msg) cat("[OK] ", msg, "\n", sep = "")
debug_warn <- function(msg) cat("[WARN] ", msg, "\n", sep = "")
debug_info <- function(msg) cat("[INFO] ", msg, "\n", sep = "")

# Safe glimpses (only if objects exist)
safe_glimpse <- function(obj, label){
  cat(sprintf("Glimpse %s:\n", label))
  if (exists(obj, inherits = TRUE)) {
    tryCatch(glimpse(get(obj, inherits = TRUE)), error = function(e) print(e))
  } else {
    cat("  <object not found in environment>\n")
  }
}

safe_glimpse("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA", "COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA")
safe_glimpse("geo",                              "geo")
safe_glimpse("geo_long",                         "geo_long")
safe_glimpse("tigris_counties_2020_raw",         "tigris_counties_2020_raw")
safe_glimpse("geocorr_county_2020_cd_119",       "geocorr_county_2020_cd_119")
safe_glimpse("geocorr_ct_county_cd_119",         "geocorr_ct_county_cd_119")

# ====================== Helpers (utilities + diagnostics) ======================

sanitize_names <- function(x) gsub("\\.+","_", make.names(x, unique = TRUE))
coerce_numeric <- function(x) suppressWarnings(as.numeric(x))
pad_fips_2     <- function(x) ifelse(nchar(x) == 1, paste0("0", x), x)
pad_fips_5     <- function(x) ifelse(nchar(x) == 4, paste0("0", x), x)
strip_digits   <- function(x) gsub("[^0-9]", "", x %||% "")
nzchar1        <- function(x) !is.na(x) & x != "" & x != "NA" & x != "NA-NA"

# robust 2-digit state fips extractor from SA "xxxxx0" codes like "01000"
extract_state_fips2 <- function(x) {
  y <- strip_digits(as.character(x))
  ifelse(nchar(y) >= 2, substr(y, 1, 2), pad_fips_2(y))
}
# also an unpadded "geo_long compatible" version ("01" -> "1")
to_unpadded_state_code <- function(x) as.character(suppressWarnings(as.integer(x)))

print_df_issues <- function(df, label, top_na = 12){
  cat(sprintf("\n==== Diagnostics — %s ====\n", label))
  if (requireNamespace("readr", quietly = TRUE) && inherits(df, "tbl_df")) {
    p <- try(readr::problems(df), silent = TRUE)
    if (!inherits(p, "try-error") && nrow(p)) print(utils::head(p, 12)) else cat("No readr parsing problems.\n")
  } else cat("Skipping readr::problems().\n")
  nac <- sort(sapply(df, function(v) sum(is.na(v))), decreasing = TRUE)
  cat("\n---- NA counts (top 20) ----\n")
  if (length(nac)) print(utils::head(nac, 20)) else cat("No columns.\n")
}

coverage_report <- function(df, label, threshold = 0.85){
  cat(sprintf("\n==== Coverage — %s ====\n", label))
  if (!nrow(df)) return(invisible(NULL))
  cov <- sapply(df, function(v) mean(!is.na(v)))
  cov_df <- tibble(column = names(cov), coverage = as.numeric(cov)) %>% arrange(coverage)
  print(utils::head(cov_df, 20))
  bad <- dplyr::filter(cov_df, coverage < threshold)
  if (nrow(bad)) {
    cat(sprintf("\n-- Columns below %.0f%% coverage --\n", threshold*100))
    print(bad)
  }
  invisible(cov_df)
}

wmean_safe <- function(x, w){
  w <- tidyr::replace_na(as.numeric(w), 0)
  if (sum(w) <= 0) return(NA_real_)
  stats::weighted.mean(as.numeric(x), w, na.rm = TRUE)
}

# Weight rule-of-thumb for additive measures (used only for additive vars)
choose_weight <- function(ind_name){
  nm <- tolower(ind_name %||% "")
  if (str_detect(nm, "per 1,000 population|per capita|population|attainment|unemployment|poverty|migration|broadband|inequality|some college|high school|associate|bachelor|graduate|stem"))
    return("population")
  if (str_detect(nm, "per worker|employment share|jobs|per 10,000 workers|establishments|proprietorship|wage|earnings|compensation"))
    return("population")
  if (str_detect(nm, "gdp|venture|vc|dollar|investment"))
    return("gdp")
  if (str_detect(nm, "index|industry cluster|latent innovation|business dynamics|economic well-being|educational attainment|industry performance|knowledge|patent|diversity|spillovers|strength|performance|profile|formation|dynamics"))
    return("population")
  "gdp"
}

choose_weight_basis <- function(colname){
  if (exists("choose_weight")) {
    w <- tryCatch(choose_weight(colname), error = function(e) "population")
    if (!w %in% c("population","gdp")) w <- "population"
    return(w)
  }
  nm <- tolower(colname %||% "")
  if (str_detect(nm, "gdp|dollar|investment")) return("gdp")
  "population"
}

is_additive_measure <- function(colname){
  nm <- tolower(colname %||% "")
  stringr::str_detect(
    nm,
    "(^|\\b)(total|count|number|dollar|dollars|gdp(?!.*per)|investment(?!.*scaled|per)|patent count|establishments?\\b(?!.*per)|jobs?\\b(?!.*ratio|share)|employment\\b(?!.*share))"
  )
}

prep_common <- function(df){
  if (!nrow(df)) return(tibble())
  df %>%
    as_tibble() %>%
    dplyr::rename(
      area_name = dplyr::any_of(c("description","description...3")),
      indicator = dplyr::any_of(c("description...5"))
    ) %>%
    dplyr::mutate(
      geo_id  = as.character(.data$geo_id),
      time_id = coerce_numeric(.data$time_id)
    ) %>%
    { stats::setNames(., sanitize_names(names(.))) }
}

keep_latest     <- function(df){ if(!nrow(df)||!"time_id"%in%names(df)) return(df); df %>% dplyr::group_by(geo_id) %>% dplyr::filter(time_id==max(time_id,na.rm=TRUE)) %>% dplyr::ungroup() }
normalize_metro <- function(df){ if(!nrow(df)||!"geo_id"%in%names(df)) return(df); df %>% dplyr::mutate(geo_id=ifelse(grepl("^2",geo_id),substr(geo_id,2,nchar(geo_id)),geo_id)) }

add_geo_type <- function(df,type){
  if(!nrow(df)) return(df)
  df %>%
    dplyr::mutate(
      statsamerica_geo_type = type,
      statsamerica_geo_name = area_name,
      statsamerica_geo_id   = geo_id
    ) %>%
    dplyr::select(statsamerica_geo_type, statsamerica_geo_name, statsamerica_geo_id,
                  dplyr::everything(), -area_name, -geo_id)
}

# convenience: check & clip a desired set of columns to those that exist in df
ensure_existing_cols <- function(df, desired, label = "") {
  existing <- intersect(desired, names(df))
  missing  <- setdiff(desired, names(df))
  if (length(missing)) {
    debug_warn(sprintf("Some requested columns were not present in %s (showing up to 12): %s",
                       label, paste(utils::head(missing, 12), collapse = " | ")))
  }
  existing
}

# safe left_join that tries to set relationship="many-to-many" when supported
left_join_m2m <- function(x, y, by, ...) {
  f <- get("left_join", asNamespace("dplyr"))
  # try with relationship= if available
  res <- try(f(x, y, by = by, relationship = "many-to-many", ...), silent = TRUE)
  if (inherits(res, "try-error")) {
    debug_info("dplyr::left_join without 'relationship' (older dplyr). You may see benign many-to-many warnings.")
    res <- f(x, y, by = by, ...)
  }
  res
}

# ====================== Disaggregation engine (FIXED & DEFENSIVE) ======================

disaggregate_to_crosswalk <- function(df_in, xwalk, weights_tbl, assume_all_replicates = FALSE){
  # --- guards & normalization ---
  if (!nrow(df_in)) return(df_in)
  stopifnot(all(c("COUNTY_GEOID","COUNTY_NAME","STATE_ABBREVIATION") %in% names(xwalk)))
  
  xwalk <- xwalk %>%
    dplyr::mutate(
      COUNTY_GEOID = as.character(COUNTY_GEOID),
      COUNTY_NAME  = as.character(COUNTY_NAME),
      STATE_ABBREVIATION = as.character(STATE_ABBREVIATION)
    )
  weights_tbl <- weights_tbl %>%
    dplyr::mutate(
      sa_id = as.character(sa_id),
      to_id = as.character(to_id)
    )
  
  meta_names <- c("statsamerica_geo_type","statsamerica_geo_name","statsamerica_geo_id")
  key_cols   <- c("COUNTY_GEOID","COUNTY_NAME","STATE_ABBREVIATION")
  stopifnot("statsamerica_geo_id" %in% names(df_in))
  meta_cols  <- intersect(meta_names, names(df_in))
  value_cols <- setdiff(names(df_in), c(meta_cols, key_cols))  # indicators only
  
  purge_meta   <- function(df) df %>% dplyr::select(-dplyr::any_of(meta_names))
  ensure_schema <- function(df, key_cols, ind_cols){
    df %>%
      purge_meta() %>%
      { for (cn in setdiff(ind_cols, names(.))) .[[cn]] <- NA_real_; . } %>%
      dplyr::select(dplyr::all_of(c(key_cols, ind_cols))) %>%
      dplyr::mutate(dplyr::across(dplyr::all_of(ind_cols), ~ suppressWarnings(as.numeric(.))))
  }
  
  # --- partition input ---
  already_ok <- df_in %>% dplyr::filter(statsamerica_geo_id %in% xwalk$COUNTY_GEOID)
  needs_map  <- df_in %>% dplyr::filter(statsamerica_geo_id %in% weights_tbl$sa_id)
  ignored    <- df_in %>%
    dplyr::filter(!statsamerica_geo_id %in% xwalk$COUNTY_GEOID,
                  !statsamerica_geo_id %in% weights_tbl$sa_id)
  
  cat("\n[disaggregate] Input rows:", nrow(df_in),
      " | pass-through rows:", nrow(already_ok),
      " | composite rows:", nrow(needs_map),
      " | ignored rows:", nrow(ignored), "\n")
  if (nrow(ignored)) {
    ig <- sort(unique(ignored$statsamerica_geo_id))
    cat("[disaggregate] Note: ignoring", length(ig),
        "IDs not in crosswalk nor in composite map.\n")
  }
  
  # --- variable classification ---
  var_class <- tibble::tibble(indicator = value_cols) %>%
    dplyr::mutate(
      is_additive = if (assume_all_replicates) FALSE else vapply(indicator, is_additive_measure, logical(1)),
      weight_key  = vapply(indicator, choose_weight_basis,  character(1))
    )
  
  # --- trivial path (no composites present) ---
  if (!nrow(needs_map)) {
    wide_ok <- already_ok %>%
      dplyr::rename(COUNTY_GEOID = statsamerica_geo_id) %>%
      left_join_m2m(xwalk %>% dplyr::select(dplyr::all_of(key_cols)), by = "COUNTY_GEOID") %>%
      dplyr::relocate(dplyr::all_of(key_cols))
    
    indicator_cols <- setdiff(names(purge_meta(wide_ok)), key_cols)
    indicator_cols <- indicator_cols[!grepl("^statsamerica_", indicator_cols)]
    indicator_cols <- sort(indicator_cols)
    desired_cols   <- c(key_cols, indicator_cols)
    
    out <- wide_ok %>%
      dplyr::select(dplyr::all_of(desired_cols)) %>%
      dplyr::mutate(dplyr::across(dplyr::all_of(indicator_cols), ~ suppressWarnings(as.numeric(.)))) %>%
      dplyr::distinct(COUNTY_GEOID, .keep_all = TRUE)
    
    target_ids <- sort(unique(xwalk$COUNTY_GEOID))
    out_ids    <- sort(unique(out$COUNTY_GEOID))
    cat("[disaggregate] (no composites) rows:", nrow(out),
        " | missing:", length(setdiff(target_ids, out_ids)),
        " | extra:",   length(setdiff(out_ids, target_ids)), "\n")
    stopifnot(setequal(out_ids, target_ids))
    stopifnot(!anyDuplicated(out$COUNTY_GEOID))
    return(out)
  }
  
  # --- composite path ---
  missing_w <- setdiff(unique(needs_map$statsamerica_geo_id), unique(weights_tbl$sa_id))
  if (length(missing_w)) {
    warning("[disaggregate] No weights for composite IDs: ", paste(missing_w, collapse = ", "))
  }
  
  long_comp <- needs_map %>%
    tidyr::pivot_longer(cols = dplyr::all_of(value_cols),
                        names_to = "indicator", values_to = "value") %>%
    dplyr::left_join(var_class, by = "indicator") %>%
    left_join_m2m(weights_tbl, by = c("statsamerica_geo_id" = "sa_id")) %>%
    dplyr::mutate(
      value  = suppressWarnings(as.numeric(value)),
      w_pop  = tidyr::replace_na(w_population, 0),
      w_gdp2 = tidyr::replace_na(w_gdp,        0),
      weight = dplyr::case_when(
        is_additive & weight_key == "gdp"        ~ w_gdp2,
        is_additive & weight_key == "population" ~ w_pop,
        TRUE                                     ~ 1
      ),
      value_adj    = dplyr::if_else(is_additive, value * weight, value),
      COUNTY_GEOID = to_id
    )
  
  if (anyNA(long_comp$COUNTY_GEOID)) {
    bad <- long_comp %>% dplyr::filter(is.na(COUNTY_GEOID)) %>%
      dplyr::distinct(statsamerica_geo_id) %>% dplyr::arrange(statsamerica_geo_id)
    stop("[disaggregate] Missing to_id for the following composite IDs: ",
         paste(bad$statsamerica_geo_id, collapse = ", "))
  }
  
  wide_comp <- long_comp %>%
    dplyr::group_by(COUNTY_GEOID, indicator) %>%
    dplyr::summarise(
      value = if (any(is_additive, na.rm = TRUE)) {
        sum(value_adj[is_additive], na.rm = TRUE)
      } else {
        wmean_safe(value_adj, weight)
      },
      .groups = "drop"
    ) %>%
    tidyr::pivot_wider(names_from = indicator, values_from = value) %>%
    left_join_m2m(xwalk %>% dplyr::select(dplyr::all_of(key_cols)), by = "COUNTY_GEOID") %>%
    dplyr::relocate(dplyr::all_of(key_cols))
  
  wide_ok <- already_ok %>%
    dplyr::rename(COUNTY_GEOID = statsamerica_geo_id) %>%
    left_join_m2m(xwalk %>% dplyr::select(dplyr::all_of(key_cols)), by = "COUNTY_GEOID") %>%
    dplyr::relocate(dplyr::all_of(key_cols))
  
  indicator_cols <- setdiff(
    names(dplyr::bind_rows(purge_meta(wide_ok), purge_meta(wide_comp))),
    key_cols
  )
  indicator_cols <- sort(indicator_cols[!grepl("^statsamerica_", indicator_cols)])
  desired_cols   <- c(key_cols, indicator_cols)
  
  wide_ok_sel   <- ensure_schema(wide_ok,   key_cols, indicator_cols)
  wide_comp_sel <- ensure_schema(wide_comp, key_cols, indicator_cols)
  
  out_all <- dplyr::bind_rows(
    wide_ok_sel   %>% dplyr::select(dplyr::all_of(desired_cols)),
    wide_comp_sel %>% dplyr::select(dplyr::all_of(desired_cols))
  ) %>%
    dplyr::distinct(COUNTY_GEOID, .keep_all = TRUE)
  
  if (length(intersect(names(out_all), meta_names))) {
    warning("[disaggregate] Unexpected meta columns leaked into output: ",
            paste(intersect(names(out_all), meta_names), collapse = ", "))
    out_all <- out_all %>% dplyr::select(-dplyr::any_of(meta_names))
  }
  bad_types <- indicator_cols[vapply(out_all[indicator_cols], function(v) !is.numeric(v), logical(1))]
  if (length(bad_types)) {
    warning("[disaggregate] Non-numeric indicator columns after coercion: ",
            paste(bad_types, collapse = ", "))
  }
  
  target_ids  <- sort(unique(xwalk$COUNTY_GEOID))
  out_ids     <- sort(unique(out_all$COUNTY_GEOID))
  missing_ids <- setdiff(target_ids, out_ids)
  extra_ids   <- setdiff(out_ids, target_ids)
  
  cat("\n[Disaggregate] Output rows (pre-fill):", nrow(out_all),
      "\n  Missing crosswalk counties:", length(missing_ids),
      "\n  Extra (non-crosswalk) counties:", length(extra_ids), "\n")
  
  if (length(missing_ids)) {
    fillers <- xwalk %>%
      dplyr::filter(COUNTY_GEOID %in% missing_ids) %>%
      dplyr::select(dplyr::all_of(key_cols))
    for (cn in indicator_cols) fillers[[cn]] <- NA_real_
    fillers <- fillers %>% dplyr::select(dplyr::all_of(desired_cols))
    
    out_all <- out_all %>% dplyr::select(dplyr::all_of(desired_cols))
    stopifnot(identical(names(out_all), names(fillers)))
    
    out_all <- dplyr::bind_rows(out_all, fillers) %>%
      dplyr::distinct(COUNTY_GEOID, .keep_all = TRUE)
    
    cat("[Disaggregate] Added NA filler rows for ", length(missing_ids),
        " counties to match crosswalk.\n", sep = "")
  }
  
  out_ids_final <- sort(unique(out_all$COUNTY_GEOID))
  still_missing <- setdiff(target_ids, out_ids_final)
  still_extra   <- setdiff(out_ids_final, target_ids)
  
  cat("[Disaggregate] Output rows (final):", nrow(out_all),
      "\n  Remaining missing:", length(still_missing),
      "\n  Remaining extra:",   length(still_extra), "\n")
  
  if (length(still_missing)) {
    warning("[disaggregate] Still missing crosswalk counties: ",
            paste(still_missing, collapse = ", "))
  }
  if (length(still_extra)) {
    warning("[disaggregate] Output contains non-crosswalk counties: ",
            paste(still_extra, collapse = ", "))
  }
  
  stopifnot(setequal(out_ids_final, target_ids))
  stopifnot(!anyDuplicated(out_all$COUNTY_GEOID))
  stopifnot(all(nchar(out_all$COUNTY_GEOID) == 5))
  
  # --- mass-balance audit (additive only) ---
  if (nrow(needs_map) && any(var_class$is_additive)) {
    tol <- 1e-6
    
    audit_orig <- needs_map %>%
      tidyr::pivot_longer(cols = dplyr::all_of(var_class$indicator),
                          names_to = "indicator", values_to = "orig_value") %>%
      dplyr::inner_join(var_class %>% dplyr::filter(is_additive) %>% dplyr::select(indicator),
                        by = "indicator") %>%
      dplyr::mutate(orig_value = suppressWarnings(as.numeric(orig_value))) %>%
      dplyr::group_by(statsamerica_geo_id, indicator) %>%
      dplyr::summarise(orig = sum(orig_value, na.rm = TRUE), .groups = "drop")
    
    audit_alloc <- long_comp %>%
      dplyr::filter(is_additive) %>%
      dplyr::group_by(statsamerica_geo_id, indicator) %>%
      dplyr::summarise(alloc = sum(value_adj, na.rm = TRUE), .groups = "drop")
    
    audit <- audit_orig %>%
      dplyr::left_join(audit_alloc, by = c("statsamerica_geo_id","indicator")) %>%
      dplyr::mutate(diff = alloc - orig, ok = tidyr::replace_na(abs(diff) <= tol, FALSE))
    
    if (any(!audit$ok)) {
      warning("Mass-balance audit FAILED for additive indicators:\n",
              paste(utils::capture.output(
                print(audit %>% dplyr::filter(!ok) %>% dplyr::arrange(dplyr::desc(abs(diff))) %>% utils::head(20))
              ), collapse = "\n"))
    } else {
      cat("[Disaggregate] Mass-balance audit passed for additive measures.\n")
    }
  }
  
  out_all
}

# ====================== Base reference (crosswalk counties) ======================
stopifnot(exists("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA"))
county_crosswalk <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
  mutate(
    COUNTY_GEOID = pad_fips_5(strip_digits(COUNTY_GEOID)),
    STATE_FIPS   = pad_fips_2(strip_digits(STATE_FIPS))
  )
cat("\nRows in crosswalk:", nrow(county_crosswalk), "\n")
stopifnot(all(nchar(county_crosswalk$COUNTY_GEOID) == 5))

# ====================== StatsAmerica: download + read ======================
debug_header("Download StatsAmerica bundle")
sa_zip <- tempfile(fileext = ".zip")
sa_ok  <- tryCatch({
  download.file("https://www.statsamerica.org/downloads/Innovation-Intelligence.zip", sa_zip, mode = "wb", quiet = TRUE)
  file.exists(sa_zip)
}, error = function(e) FALSE)
if (!isTRUE(sa_ok)) stop("Could not download the StatsAmerica zip. Aborting.")

sa_list <- try(unzip(sa_zip, list = TRUE), silent = TRUE)
if (inherits(sa_list, "try-error")) stop("Could not list files inside the StatsAmerica zip. Aborting.")
tmp_sa  <- tempdir()
debug_ok("StatsAmerica zip downloaded and listed.")

sa_read <- function(regex){
  f <- sa_list$Name[grepl(regex, sa_list$Name, ignore.case = TRUE)]
  f <- f[!grepl("EDD", f, ignore.case = TRUE)]
  if (!length(f)) {
    message("sa_read() - no match for: ", regex)
    return(tibble())
  }
  unzip(sa_zip, files = f[1], exdir = tmp_sa)
  readr::read_csv(file.path(tmp_sa, f[1]),
                  show_col_types = FALSE,
                  na = c("", "NULL", "NA", "N/A"),
                  guess_max = 100000)
}

idx_sc_raw     <- sa_read("Index Values.*States and Counties\\.csv$");     print_df_issues(idx_sc_raw,    "idx_sc_raw")
meas_sc_raw    <- sa_read("Measures.*States and Counties\\.csv$");         print_df_issues(meas_sc_raw,   "meas_sc_raw")
idx_metro_raw  <- sa_read("Index Values.*Metros\\.csv$") %>% normalize_metro(); print_df_issues(idx_metro_raw, "idx_metro_raw (normalized)")
meas_metro_raw <- sa_read("Measures.*Metros\\.csv$")     %>% normalize_metro(); print_df_issues(meas_metro_raw,"meas_metro_raw (normalized)")

# ====================== Pivot-wide (States+Counties, Metros) ======================
debug_header("Pivot SA long -> wide")

idx_sc <- idx_sc_raw %>%
  prep_common() %>% keep_latest() %>%
  rename(value = dplyr::any_of(c("Index.Value","Index_Value"))) %>%
  mutate(value = coerce_numeric(value)) %>%
  group_by(geo_id, area_name, indicator) %>%
  summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
  tidyr::pivot_wider(names_from = "indicator", values_from = "value", names_repair = "unique")
coverage_report(idx_sc, "idx_sc")

# ID hygiene & debug
n2_before <- sum(nchar(strip_digits(as.character(idx_sc$geo_id))) == 2)
n4_before <- sum(nchar(strip_digits(as.character(idx_sc$geo_id))) == 4)
n5_before <- sum(nchar(strip_digits(as.character(idx_sc$geo_id))) == 5)
cat(sprintf("\nID length check BEFORE padding in idx_sc: 2-digit=%d, 4-digit=%d, 5-digit=%d\n", n2_before, n4_before, n5_before))
idx_sc <- idx_sc %>%
  mutate(
    geo_id = strip_digits(as.character(geo_id)),
    geo_id = ifelse(nchar(geo_id) == 4, paste0("0", geo_id), geo_id)  # fix dropped leading zero in counties
  )
cat("\nID length table AFTER padding in idx_sc:\n"); print(table(nchar(idx_sc$geo_id)))
cat("Pseudo-state rows (xxxxx000) present in idx_sc: ", sum(grepl("000$", idx_sc$geo_id)), "\n", sep="")

meas_sc <- meas_sc_raw %>%
  prep_common() %>%
  rename(value = dplyr::any_of(c("Measure.Value","Measure_Value")),
         indicator = dplyr::any_of(c("Code.Description","Code_Description")),
         Summary_Level = dplyr::any_of(c("Summary.Level","Summary_Level"))) %>%
  mutate(value = coerce_numeric(value), Summary_Level = as.character(Summary_Level)) %>%
  keep_latest() %>%
  group_by(geo_id, area_name, indicator, Summary_Level) %>%
  summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
  tidyr::pivot_wider(names_from = "indicator", values_from = "value", names_repair = "unique")
coverage_report(meas_sc, "meas_sc")

idx_metro <- idx_metro_raw %>%
  prep_common() %>% keep_latest() %>%
  rename(value = dplyr::any_of(c("Index.Value","Index_Value"))) %>%
  mutate(value = coerce_numeric(value)) %>%
  group_by(geo_id, area_name, indicator) %>%
  summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
  tidyr::pivot_wider(names_from = "indicator", values_from = "value", names_repair = "unique")
coverage_report(idx_metro, "idx_metro")

meas_metro <- meas_metro_raw %>%
  prep_common() %>%
  rename(value = dplyr::any_of(c("Measure.Value","Measure_Value")),
         indicator = dplyr::any_of(c("Code.Description","Code_Description")),
         Summary_Level = dplyr::any_of(c("Summary.Level","Summary_Level"))) %>%
  mutate(value = coerce_numeric(value), Summary_Level = as.character(Summary_Level)) %>%
  keep_latest() %>%
  group_by(geo_id, area_name, indicator, Summary_Level) %>%
  summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
  tidyr::pivot_wider(names_from = "indicator", values_from = "value", names_repair = "unique")
coverage_report(meas_metro, "meas_metro")

# ====================== Canonical mapping & weights ======================
debug_header("Composite ID mapping weights (AK/HI/VA)")
# AK historical changes, HI Kalawao+Maui composite, VA independent-city combos
sa_combo_pairs <- tribble(
  ~sa_id, ~to_id,
  # Alaska historical / composite units
  "02232","02105",
  "02232","02230",
  "02261","02063",
  "02261","02066",
  "02270","02158",
  "02280","02195",
  "02280","02275",
  "02901","02130",
  "02901","02198",
  # Hawaii composite
  "15901","15005",
  "15901","15009",
  # Virginia independent-city composites
  "51901","51003", "51901","51540",
  "51903","51005", "51903","51580",
  "51907","51015", "51907","51790", "51907","51820",
  "51911","51031", "51911","51680",
  "51913","51035", "51913","51640",
  "51918","51053", "51918","51570", "51918","51730",
  "51919","51059", "51919","51600", "51919","51610",
  "51921","51069", "51921","51840",
  "51923","51081", "51923","51595",
  "51929","51089", "51929","51690",
  "51931","51095", "51931","51830",
  "51933","51121", "51933","51750",
  "51939","51143", "51939","51590",
  "51941","51149", "51941","51670",
  "51942","51153", "51942","51683", "51942","51685",
  "51944","51161", "51944","51775",
  "51945","51163", "51945","51530", "51945","51678",
  "51947","51165", "51947","51660",
  "51949","51175", "51949","51620",
  "51951","51177", "51951","51630",
  "51953","51191", "51953","51520",
  "51955","51195", "51955","51720",
  "51958","51199", "51958","51735"
) %>% distinct()

stopifnot(all(sa_combo_pairs$to_id %in% county_crosswalk$COUNTY_GEOID))

xwalk_keep_cols <- c("COUNTY_GEOID","COUNTY_NAME","STATE_ABBREVIATION","2022_COUNTY_POPULATION","COUNTY_GDP_2022")

sa_combo_weights <- sa_combo_pairs %>%
  left_join(county_crosswalk %>% select(all_of(xwalk_keep_cols)), by = c("to_id" = "COUNTY_GEOID")) %>%
  group_by(sa_id) %>%
  mutate(
    pop_sum      = sum(tidyr::replace_na(`2022_COUNTY_POPULATION`, 0), na.rm = TRUE),
    gdp_sum      = sum(tidyr::replace_na(COUNTY_GDP_2022, 0), na.rm = TRUE),
    w_population = if_else(pop_sum > 0, `2022_COUNTY_POPULATION`/pop_sum, 1/n()),
    w_gdp        = if_else(gdp_sum > 0, COUNTY_GDP_2022/gdp_sum, w_population)
  ) %>%
  ungroup() %>%
  select(sa_id, to_id, COUNTY_NAME, STATE_ABBREVIATION, w_population, w_gdp)

# Debug: are composite SA IDs present in idx_sc?
sa_composite_ids <- unique(sa_combo_pairs$sa_id)
comp_found   <- sort(intersect(sa_composite_ids, idx_sc$geo_id))
comp_missing <- sort(setdiff(sa_composite_ids, idx_sc$geo_id))
cat(sprintf("\nComposite SA IDs found in idx_sc: %d / %d\n", length(comp_found), length(sa_composite_ids)))
if (length(comp_missing)) {
  cat("Composite IDs missing from this vintage (OK if not published):\n  ", paste(comp_missing, collapse = ", "), "\n", sep="")
}

# ====================== Tag geo_type & split State/County ======================
debug_header("Split state & county tables")
# Derive state + county IDs from measures (authoritative summary levels)
state_ids_meas <- meas_sc %>%
  filter(Summary_Level == "40") %>%
  transmute(geo_id = pad_fips_2(strip_digits(as.character(geo_id)))) %>%
  pull(geo_id) %>% unique()

county_ids_meas <- meas_sc %>%
  filter(Summary_Level == "50") %>%
  transmute(geo_id = pad_fips_5(strip_digits(as.character(geo_id)))) %>%
  pull(geo_id) %>% unique()

cat("\nCounts from measures — states:", length(state_ids_meas), " | counties:", length(county_ids_meas), "\n")

# Build states_idx from idx_sc by pattern (ends with '000')
states_idx <- idx_sc %>%
  dplyr::filter(stringr::str_detect(geo_id, "000$")) %>%
  add_geo_type("State")
cat("\nCounts — states_idx (should be 51): ", nrow(states_idx), "\n", sep = ""); glimpse(states_idx)

# Build counties_idx directly from idx_sc (5-digit county-like; exclude pseudo-state x000)
counties_idx <- idx_sc %>%
  dplyr::filter(stringr::str_detect(geo_id, "^[0-9]{5}$"),
                !stringr::str_detect(geo_id, "000$")) %>%
  add_geo_type("County") %>%
  dplyr::mutate(statsamerica_geo_id = pad_fips_5(statsamerica_geo_id))

# Debug: health-check by state
by_state_idx <- counties_idx %>%
  dplyr::transmute(st = substr(statsamerica_geo_id, 1, 2)) %>%
  dplyr::count(st, name = "n_rows") %>%
  dplyr::arrange(st)
print(by_state_idx, n = Inf)
va_have <- sum(substr(counties_idx$statsamerica_geo_id, 1, 2) == "51")
cat("Virginia rows in counties_idx: ", va_have, " (expected ~133)\n", sep = "")

# ====================== Disaggregate to crosswalk (FIXED) ======================
debug_header("Disaggregate counties to crosswalk")
# Index tables: replicate (non-additive)
counties_idx_xwalk <- disaggregate_to_crosswalk(
  df_in = counties_idx,
  xwalk = county_crosswalk,
  weights_tbl = sa_combo_weights,
  assume_all_replicates = TRUE
)

# Measures tables: additive vs non-additive classification
states_meas <- add_geo_type(meas_sc %>% filter(Summary_Level=="40") %>% select(-Summary_Level), "State")
cat("\nCounts — states_meas:", nrow(states_meas), "\n"); glimpse(states_meas)

counties_meas <- add_geo_type(meas_sc %>% filter(Summary_Level=="50") %>% select(-Summary_Level), "County") %>%
  mutate(statsamerica_geo_id = pad_fips_5(statsamerica_geo_id))
cat("\nCounts — counties_meas:", nrow(counties_meas), "\n"); glimpse(counties_meas)

counties_meas_xwalk <- disaggregate_to_crosswalk(
  df_in = counties_meas,
  xwalk = county_crosswalk,
  weights_tbl = sa_combo_weights,
  assume_all_replicates = FALSE
)

# Metros (untouched by crosswalk)
metros_idx  <- add_geo_type(idx_metro,  "Metro")
cat("\nCounts — metros_idx:", nrow(metros_idx), "\n"); glimpse(metros_idx)

metros_meas <- add_geo_type(meas_metro, "Metro")
cat("\nCounts — metros_meas:", nrow(metros_meas), "\n"); glimpse(metros_meas)

# ====================== Inventory: SA county IDs vs Crosswalk ======================
debug_header("Inventory: StatsAmerica county IDs vs crosswalk")
all_statsamerica_county_ids <- union(unique(counties_idx$statsamerica_geo_id),
                                     unique(counties_meas$statsamerica_geo_id))
cat("\nUnique SA county-like IDs (idx + meas):", length(all_statsamerica_county_ids), "\n")
stopifnot(all(nchar(all_statsamerica_county_ids) == 5))

statsamerica_county_matrix <- tibble(statsamerica_geo_id = all_statsamerica_county_ids) %>%
  left_join(counties_idx  %>% select(statsamerica_geo_id, statsamerica_geo_name) %>% distinct(), by = "statsamerica_geo_id") %>%
  left_join(counties_meas %>% select(statsamerica_geo_id, statsamerica_geo_name) %>% distinct(), by = "statsamerica_geo_id", suffix = c(".idx", ".meas")) %>%
  mutate(
    statsamerica_geo_name = coalesce(statsamerica_geo_name.idx, statsamerica_geo_name.meas),
    statsamerica_geo_name = ifelse(is.na(statsamerica_geo_name), "<missing name>", statsamerica_geo_name)
  ) %>%
  select(statsamerica_geo_id, statsamerica_geo_name) %>%
  arrange(statsamerica_geo_id)
glimpse(statsamerica_county_matrix)

statsamerica_county_matrix_xwalk_reference <- statsamerica_county_matrix %>%
  mutate(present_in_county_crosswalk = statsamerica_geo_id %in% county_crosswalk$COUNTY_GEOID)
glimpse(statsamerica_county_matrix_xwalk_reference)

missing_in_crosswalk <- dplyr::filter(statsamerica_county_matrix_xwalk_reference, !present_in_county_crosswalk)
cat("\nSA county IDs not found in crosswalk (n=", nrow(missing_in_crosswalk), "):\n", sep = "")
if(nrow(missing_in_crosswalk)) print(missing_in_crosswalk, n = min(Inf, nrow(missing_in_crosswalk)), width = Inf)

county_Xwalk_statsamerica_reference <- county_crosswalk %>%
  mutate(present_in_statsamerica = COUNTY_GEOID %in% statsamerica_county_matrix$statsamerica_geo_id)
glimpse(county_Xwalk_statsamerica_reference)

missing_in_statsamerica <- dplyr::filter(county_Xwalk_statsamerica_reference, !present_in_statsamerica)
cat("\nCrosswalk counties not found in SA IDs (n=", nrow(missing_in_statsamerica), "):\n", sep = "")
if(nrow(missing_in_statsamerica)) print(missing_in_statsamerica, n = min(Inf, nrow(missing_in_statsamerica)), width = Inf)

# ====================== Final QA / footprint ======================
debug_header("Quick footprint counts")
cat("Crosswalk target rows:         ", nrow(county_crosswalk), "\n", sep="")
cat("Index (aligned) output rows:   ", nrow(counties_idx_xwalk), "\n", sep="")
cat("Measures (aligned) output rows:", nrow(counties_meas_xwalk), "\n", sep="")

stopifnot(nrow(counties_idx_xwalk)  >= 3143, nrow(counties_meas_xwalk) >= 3143)

target_set <- sort(unique(county_crosswalk$COUNTY_GEOID))
idx_set    <- sort(unique(counties_idx_xwalk$COUNTY_GEOID))
meas_set   <- sort(unique(counties_meas_xwalk$COUNTY_GEOID))

cat("\n== Footprint QA ==\n",
    "Crosswalk counties:     ", length(target_set), "\n",
    "Index (aligned) rows:   ", length(idx_set),    " (missing:", length(setdiff(target_set, idx_set)),  ")\n",
    "Measures (aligned) rows:", length(meas_set),   " (missing:", length(setdiff(target_set, meas_set)), ")\n", sep="")

if (length(setdiff(target_set, idx_set)))  message("Index missing IDs: ", paste(setdiff(target_set, idx_set), collapse=", "))
if (length(setdiff(target_set, meas_set))) message("Measures missing IDs: ", paste(setdiff(target_set, meas_set), collapse=", "))

stopifnot(
  setequal(unique(counties_idx_xwalk$COUNTY_GEOID),  unique(county_crosswalk$COUNTY_GEOID)),
  setequal(unique(counties_meas_xwalk$COUNTY_GEOID), unique(county_crosswalk$COUNTY_GEOID))
)

cat("\nAll good: aligned county footprints exactly match the crosswalk.\n")
glimpse(counties_idx_xwalk)
glimpse(counties_meas_xwalk)
glimpse(states_idx)
glimpse(states_meas)
glimpse(metros_idx)
glimpse(metros_meas)

# ====================== Enrich county-aligned tables w/ crosswalk fields ======================
debug_header("Enrich county outputs with crosswalk fields (CBSA, PEA, etc.)")

counties_idx_xwalk_enriched <- counties_idx_xwalk %>%
  select(-COUNTY_NAME, -STATE_ABBREVIATION) %>%
  left_join(
    COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA,
    by = c("COUNTY_GEOID" = "COUNTY_GEOID")
  )
glimpse(counties_idx_xwalk_enriched)

counties_meas_xwalk_enriched <- counties_meas_xwalk %>%
  select(-COUNTY_NAME, -STATE_ABBREVIATION) %>%
  left_join(
    COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA,
    by = c("COUNTY_GEOID" = "COUNTY_GEOID")
  )
glimpse(counties_meas_xwalk_enriched)

# Small sanity checks on enrichment
cat("\nCBSA-attached counties (idx/meas):",
    sum(!is.na(counties_idx_xwalk_enriched$CBSA_GEOID)), "/",
    sum(!is.na(counties_meas_xwalk_enriched$CBSA_GEOID)), "\n")
cat("PEA-attached counties (idx/meas):",
    sum(!is.na(counties_idx_xwalk_enriched$PEA_NUMBER)), "/",
    sum(!is.na(counties_meas_xwalk_enriched$PEA_NUMBER)), "\n")

# ====================== Innovation Geo (final assembly) ======================
suppressPackageStartupMessages({ library(dplyr); library(tidyr); library(stringr); library(purrr) })
debug_header("Assembling Innovation Geo layers")

# ---------- 1) Identify indicator columns we’ll carry ----------
xwalk_cols <- c("CENSUS_REGION_GEOID","CENSUS_REGION_NAME","CENSUS_DIVISION_GEOID","CENSUS_DIVISION_NAME",
                "STATE_FIPS","STATE_NAME","STATE_ABBREVIATION","COUNTY_NAME","IN_CBSA","CBSA_GEOID","CBSA_NAME",
                "2022_COUNTY_POPULATION","COUNTY_GDP_2022","PEA_NAME","PEA_NUMBER")

idx_cols_county  <- setdiff(names(counties_idx_xwalk_enriched),  c("COUNTY_GEOID", xwalk_cols))
meas_cols_county <- setdiff(names(counties_meas_xwalk_enriched), c("COUNTY_GEOID", xwalk_cols))
# Keep a consistent union (some sets may differ slightly by vintage)
idx_cols_union   <- idx_cols_county
meas_cols_union  <- meas_cols_county

debug_info(paste("Index columns (county) =", length(idx_cols_union)))
debug_info(paste("Measure columns (county) =", length(meas_cols_union)))
debug_info(paste("Indicator overlap (meas ∩ idx) =", length(intersect(meas_cols_union, idx_cols_union))))

# ---------- 2) Helpers to aggregate from counties ----------
weighted_block <- function(df, group_cols, cols, basis = c("population","gdp")) {
  basis <- match.arg(basis)
  wcol  <- if (basis == "gdp") "COUNTY_GDP_2022" else "2022_COUNTY_POPULATION"
  cols_present <- ensure_existing_cols(df, cols, label = paste("weighted_block(", basis, ")"))
  df %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(across(all_of(cols_present), ~ wmean_safe(.x, .data[[wcol]]), .names = "{.col}"),
              .groups = "drop")
}

sum_block <- function(df, group_cols, cols) {
  cols_present <- ensure_existing_cols(df, cols, label = "sum_block")
  df %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(across(all_of(cols_present), ~ sum(as.numeric(.x), na.rm = TRUE), .names = "{.col}"),
              .groups = "drop")
}

aggregate_measures_from_counties <- function(df_counties, group_cols) {
  # Classify measures:
  vc <- tibble(indicator = meas_cols_union) %>%
    mutate(
      is_additive = vapply(indicator, is_additive_measure, logical(1)),
      basis       = vapply(indicator, choose_weight_basis, character(1))
    )
  add_cols        <- vc %>% filter(is_additive) %>% pull(indicator)
  rep_pop_cols    <- vc %>% filter(!is_additive & basis == "population") %>% pull(indicator)
  rep_gdp_cols    <- vc %>% filter(!is_additive & basis == "gdp")         %>% pull(indicator)
  
  out_add     <- if (length(add_cols))     sum_block(df_counties, group_cols, add_cols) else NULL
  out_rep_pop <- if (length(rep_pop_cols)) weighted_block(df_counties, group_cols, rep_pop_cols, "population") else NULL
  out_rep_gdp <- if (length(rep_gdp_cols)) weighted_block(df_counties, group_cols, rep_gdp_cols, "gdp") else NULL
  
  reduce(compact(list(out_add, out_rep_pop, out_rep_gdp)), ~ full_join(.x, .y, by = group_cols))
}

aggregate_indices_from_counties <- function(df_counties, group_cols) {
  # Treat all index columns as replicate, default weight basis chosen by choose_weight_basis()
  vc <- tibble(indicator = idx_cols_union) %>%
    mutate(basis = vapply(indicator, choose_weight_basis, character(1)))
  idx_pop <- vc %>% filter(basis == "population") %>% pull(indicator)
  idx_gdp <- vc %>% filter(basis == "gdp")         %>% pull(indicator)
  
  out_pop <- if (length(idx_pop)) weighted_block(df_counties, group_cols, idx_pop, "population") else NULL
  out_gdp <- if (length(idx_gdp)) weighted_block(df_counties, group_cols, idx_gdp, "gdp")         else NULL
  
  reduce(compact(list(out_pop, out_gdp)), ~ full_join(.x, .y, by = group_cols))
}

# ---------- 3) Counties (already aligned 1:1 to crosswalk) ----------
debug_header("Assemble county layer")
counties_meas_out <- counties_meas_xwalk_enriched %>%
  select(COUNTY_GEOID, all_of(ensure_existing_cols(., meas_cols_union, "counties_meas_out")))
counties_idx_out  <- counties_idx_xwalk_enriched  %>%
  select(COUNTY_GEOID, all_of(ensure_existing_cols(., idx_cols_union,  "counties_idx_out")))

# --- Build column universe & overlaps ---
all_indicators <- sort(unique(c(meas_cols_union, idx_cols_union)))
overlap        <- intersect(meas_cols_union, idx_cols_union)

# --- Join with explicit suffixes so collisions are predictable ---
counties_joined <- counties_meas_out %>%
  dplyr::full_join(counties_idx_out,
                   by = "COUNTY_GEOID",
                   suffix = c(".meas", ".idx"))

# --- Resolve collisions: prefer MEAS values, fallback to IDX ---
if (length(overlap)) {
  for (nm in overlap) {
    m <- paste0(nm, ".meas")
    i <- paste0(nm, ".idx")
    if (m %in% names(counties_joined) || i %in% names(counties_joined)) {
      counties_joined[[nm]] <- dplyr::coalesce(counties_joined[[m]], counties_joined[[i]])
      if (m %in% names(counties_joined)) counties_joined[[m]] <- NULL
      if (i %in% names(counties_joined)) counties_joined[[i]] <- NULL
    }
  }
}

# --- Final County output ---
counties_out <- counties_joined %>%
  dplyr::left_join(
    COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
      dplyr::select(COUNTY_GEOID, COUNTY_NAME, STATE_ABBREVIATION),
    by = "COUNTY_GEOID"
  ) %>%
  dplyr::transmute(
    geo_type = "County",
    geo_code = COUNTY_GEOID,
    geo_name = paste0(COUNTY_NAME, ", ", STATE_ABBREVIATION),
    dplyr::across(dplyr::any_of(all_indicators))
  )
debug_info(paste("County output rows =", nrow(counties_out)))

# ---------- 4) States -> prefer direct StatsAmerica else county fallback ----------
debug_header("Assemble state layer")
states_meas_direct <- states_meas %>%
  rename(STATE_NAME = statsamerica_geo_name,
         STATE_FIPS_raw = statsamerica_geo_id) %>%
  mutate(STATE_FIPS = extract_state_fips2(STATE_FIPS_raw)) %>%
  select(STATE_FIPS, all_of(ensure_existing_cols(., meas_cols_union, "states_meas_direct")))
states_idx_direct <- states_idx %>%
  rename(STATE_NAME = statsamerica_geo_name,
         STATE_FIPS_raw = statsamerica_geo_id) %>%
  mutate(STATE_FIPS = extract_state_fips2(STATE_FIPS_raw)) %>%
  select(STATE_FIPS, all_of(ensure_existing_cols(., idx_cols_union, "states_idx_direct")))

# County-based fallback (measures)
counties_for_states_meas <- counties_meas_xwalk_enriched %>%
  select(STATE_FIPS, `2022_COUNTY_POPULATION`, COUNTY_GDP_2022,
         all_of(ensure_existing_cols(., meas_cols_union, "counties_for_states_meas"))) %>%
  mutate(STATE_FIPS = pad_fips_2(STATE_FIPS))

meas_states_fallback <- aggregate_measures_from_counties(
  df_counties = counties_for_states_meas,
  group_cols = c("STATE_FIPS")
) %>% select(STATE_FIPS, all_of(ensure_existing_cols(., meas_cols_union, "meas_states_fallback")))

# County-based fallback (indices)
idx_states_fallback <- aggregate_indices_from_counties(
  df_counties = counties_idx_xwalk_enriched %>%
    select(STATE_FIPS, `2022_COUNTY_POPULATION`, COUNTY_GDP_2022,
           all_of(ensure_existing_cols(., idx_cols_union, "idx_states_fallback"))) %>%
    mutate(STATE_FIPS = pad_fips_2(STATE_FIPS)),
  group_cols  = c("STATE_FIPS")
) %>% select(STATE_FIPS, all_of(ensure_existing_cols(., idx_cols_union, "idx_states_fallback_out")))

states_meas_final <- states_meas_direct %>%
  bind_rows(anti_join(meas_states_fallback, states_meas_direct, by = "STATE_FIPS"))
states_idx_final  <- states_idx_direct %>%
  bind_rows(anti_join(idx_states_fallback,  states_idx_direct,  by = "STATE_FIPS"))

# use unpadded state code to match geo_long (which shows "1","2","4", etc.)
states_out <- states_meas_final %>%
  full_join(states_idx_final, by = "STATE_FIPS") %>%
  left_join(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>% distinct(STATE_FIPS, STATE_NAME), by = "STATE_FIPS") %>%
  mutate(geo_code_state = to_unpadded_state_code(STATE_FIPS)) %>%
  transmute(
    geo_type = "State",
    geo_code = geo_code_state,  # <-- matches geo_long$geo_code style
    geo_name = STATE_NAME,
    across(all_of(ensure_existing_cols(., c(meas_cols_union, idx_cols_union), "states_out")))
  ) %>% distinct(geo_code, .keep_all = TRUE)
debug_info(paste("State output rows =", nrow(states_out)))

# ---------- 5) CBSA (metropolitan & micropolitan)
debug_header("Assemble CBSA (Metro/Micro) layer")
county_cbsa_base <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
  select(COUNTY_GEOID, CBSA_GEOID, CBSA_NAME) %>%
  filter(!is.na(CBSA_GEOID)) %>%
  distinct()

# Direct StatsAmerica: retain CBSA_NAME so geo_name is available
metros_meas_direct <- metros_meas %>%
  rename(CBSA_GEOID = statsamerica_geo_id,
         CBSA_NAME  = statsamerica_geo_name) %>%
  select(CBSA_GEOID, CBSA_NAME,
         all_of(ensure_existing_cols(., meas_cols_union, "metros_meas_direct")))
metros_idx_direct <- metros_idx %>%
  rename(CBSA_GEOID = statsamerica_geo_id,
         CBSA_NAME  = statsamerica_geo_name) %>%
  select(CBSA_GEOID, CBSA_NAME,
         all_of(ensure_existing_cols(., idx_cols_union, "metros_idx_direct")))

# County aggregation for all CBSA in crosswalk
# Measures
counties_for_cbsa_meas <- counties_meas_xwalk_enriched %>%
  filter(!is.na(CBSA_GEOID)) %>%
  select(
    CBSA_GEOID, CBSA_NAME, `2022_COUNTY_POPULATION`, COUNTY_GDP_2022,
    all_of(ensure_existing_cols(., meas_cols_union, "counties_for_cbsa_meas"))
  )
meas_cbsa_from_counties <- aggregate_measures_from_counties(
  df_counties = counties_for_cbsa_meas,
  group_cols  = c("CBSA_GEOID","CBSA_NAME")
)

counties_for_cbsa_idx <- counties_idx_xwalk_enriched %>%
  filter(!is.na(CBSA_GEOID)) %>%
  select(
    CBSA_GEOID, CBSA_NAME, `2022_COUNTY_POPULATION`, COUNTY_GDP_2022,
    all_of(ensure_existing_cols(., idx_cols_union, "counties_for_cbsa_idx"))
  )
idx_cbsa_from_counties <- aggregate_indices_from_counties(
  df_counties = counties_for_cbsa_idx,
  group_cols  = c("CBSA_GEOID","CBSA_NAME")
)

# Use StatsAmerica when present; otherwise use county aggregates (which includes micros)
cbsa_meas_final <- metros_meas_direct %>%
  bind_rows(anti_join(meas_cbsa_from_counties, metros_meas_direct, by = "CBSA_GEOID"))
cbsa_idx_final <- metros_idx_direct %>%
  bind_rows(anti_join(idx_cbsa_from_counties,  metros_idx_direct,  by = "CBSA_GEOID"))

cbsa_out <- cbsa_meas_final %>%
  full_join(cbsa_idx_final, by = c("CBSA_GEOID","CBSA_NAME")) %>%
  transmute(
    geo_type = "Metro Area",
    geo_code = CBSA_GEOID,
    geo_name = CBSA_NAME,
    across(all_of(ensure_existing_cols(., c(meas_cols_union, idx_cols_union), "cbsa_out")))
  ) %>% distinct(geo_code, .keep_all = TRUE)
debug_info(paste("CBSA output rows =", nrow(cbsa_out)))

# ---------- 6) Economic Areas (PEA) from counties — **FIXED to use crosswalk columns directly**
debug_header("Assemble Economic Area (PEA) layer")
# Use the PEA columns already present in the enriched county tables to avoid name collisions.
pea_meas <- counties_meas_xwalk_enriched %>%
  filter(!is.na(PEA_NUMBER)) %>%
  mutate(PEA_CODE = as.character(PEA_NUMBER)) %>%
  select(PEA_CODE, PEA_NAME, `2022_COUNTY_POPULATION`, COUNTY_GDP_2022,
         all_of(ensure_existing_cols(., meas_cols_union, "pea_meas"))) %>%
  aggregate_measures_from_counties(group_cols = c("PEA_CODE","PEA_NAME"))

pea_idx  <- counties_idx_xwalk_enriched %>%
  filter(!is.na(PEA_NUMBER)) %>%
  mutate(PEA_CODE = as.character(PEA_NUMBER)) %>%
  select(PEA_CODE, PEA_NAME, `2022_COUNTY_POPULATION`, COUNTY_GDP_2022,
         all_of(ensure_existing_cols(., idx_cols_union, "pea_idx"))) %>%
  aggregate_indices_from_counties(group_cols = c("PEA_CODE","PEA_NAME"))

pea_out <- pea_meas %>%
  full_join(pea_idx, by = c("PEA_CODE","PEA_NAME")) %>%
  transmute(
    geo_type = "Economic Area",
    geo_code = PEA_CODE,
    geo_name = PEA_NAME,
    across(all_of(ensure_existing_cols(., c(meas_cols_union, idx_cols_union), "pea_out")))
  ) %>% distinct(geo_code, .keep_all = TRUE)
debug_info(paste("PEA output rows =", nrow(pea_out)))

# ---------- 7) Congressional Districts (119th) ----------
debug_header("Assemble Congressional District (119th) layer")
cd_alloc_general <- geocorr_county_2020_cd_119 %>%
  transmute(
    COUNTY_GEOID = pad_fips_5(`COUNTY_GEOID`),
    CD119_GEOID  = paste0(pad_fips_2(`State_code`), pad_fips_2(`CD119_code`)),
    alloc        = as.numeric(`county-to-cd119 allocation factor`)
  )

cd_alloc_ct <- geocorr_ct_county_cd_119 %>%
  transmute(
    COUNTY_GEOID = pad_fips_5(`County code`),
    CD119_GEOID  = paste0("09", pad_fips_2(`Congressional district code (119th Congress)`)),
    alloc        = as.numeric(`CTcounty-to-cd119 allocation factor`)
  )

cd_alloc <- bind_rows(cd_alloc_general, cd_alloc_ct) %>%
  filter(!is.na(alloc) & alloc > 0) %>%
  group_by(COUNTY_GEOID) %>%
  mutate(alloc = alloc / sum(alloc)) %>%  # normalize (defensive)
  ungroup()

# Allocation QA: each county's CD shares should sum to ~1
alloc_check <- cd_alloc %>% group_by(COUNTY_GEOID) %>% summarise(s=sum(alloc), .groups="drop")
bad_alloc <- alloc_check %>% filter(abs(s-1) > 1e-6)
if (nrow(bad_alloc)) {
  debug_warn(paste("Allocation rows not summing to 1 (n=", nrow(bad_alloc), ") — will still proceed.", sep=""))
  print(utils::head(bad_alloc, 12))
} else {
  debug_ok("CD allocation per county sums to 1 (within tolerance).")
}

# County -> CD for MEASURES
cd_meas_long <- counties_meas_xwalk_enriched %>%
  select(COUNTY_GEOID, `2022_COUNTY_POPULATION`, COUNTY_GDP_2022,
         all_of(ensure_existing_cols(., meas_cols_union, "cd_meas_long"))) %>%
  inner_join(cd_alloc, by = "COUNTY_GEOID") %>%
  mutate(
    w_pop = `2022_COUNTY_POPULATION` * alloc,
    w_gdp = COUNTY_GDP_2022 * alloc
  )

vc_meas <- tibble(indicator = meas_cols_union) %>%
  mutate(is_additive = vapply(indicator, is_additive_measure, logical(1)),
         basis       = vapply(indicator, choose_weight_basis, character(1)))
meas_add_cols     <- vc_meas %>% filter(is_additive) %>% pull(indicator)
meas_rep_pop_cols <- vc_meas %>% filter(!is_additive & basis == "population") %>% pull(indicator)
meas_rep_gdp_cols <- vc_meas %>% filter(!is_additive & basis == "gdp")         %>% pull(indicator)

cd_meas_add <- if (length(meas_add_cols)) {
  cols_present <- ensure_existing_cols(cd_meas_long, meas_add_cols, "cd_meas_add")
  cd_meas_long %>%
    group_by(CD119_GEOID) %>%
    summarise(across(all_of(cols_present), ~ sum(as.numeric(.x) * alloc, na.rm = TRUE), .names = "{.col}"), .groups = "drop")
} else NULL

cd_meas_rep_pop <- if (length(meas_rep_pop_cols)) {
  cols_present <- ensure_existing_cols(cd_meas_long, meas_rep_pop_cols, "cd_meas_rep_pop")
  cd_meas_long %>%
    group_by(CD119_GEOID) %>%
    summarise(across(all_of(cols_present), ~ wmean_safe(.x, w_pop), .names = "{.col}"), .groups = "drop")
} else NULL

cd_meas_rep_gdp <- if (length(meas_rep_gdp_cols)) {
  cols_present <- ensure_existing_cols(cd_meas_long, meas_rep_gdp_cols, "cd_meas_rep_gdp")
  cd_meas_long %>%
    group_by(CD119_GEOID) %>%
    summarise(across(all_of(cols_present), ~ wmean_safe(.x, w_gdp), .names = "{.col}"), .groups = "drop")
} else NULL

cd_meas <- reduce(compact(list(cd_meas_add, cd_meas_rep_pop, cd_meas_rep_gdp)), ~ full_join(.x, .y, by = "CD119_GEOID"))

# County -> CD for INDICES (replicates; basis-driven)
cd_idx_long <- counties_idx_xwalk_enriched %>%
  select(COUNTY_GEOID, `2022_COUNTY_POPULATION`, COUNTY_GDP_2022,
         all_of(ensure_existing_cols(., idx_cols_union, "cd_idx_long"))) %>%
  inner_join(cd_alloc, by = "COUNTY_GEOID") %>%
  mutate(
    w_pop = `2022_COUNTY_POPULATION` * alloc,
    w_gdp = COUNTY_GDP_2022 * alloc
  )

vc_idx <- tibble(indicator = idx_cols_union) %>%
  mutate(basis = vapply(indicator, choose_weight_basis, character(1)))
idx_rep_pop_cols <- vc_idx %>% filter(basis == "population") %>% pull(indicator)
idx_rep_gdp_cols <- vc_idx %>% filter(basis == "gdp")         %>% pull(indicator)

cd_idx_pop <- if (length(idx_rep_pop_cols)) {
  cols_present <- ensure_existing_cols(cd_idx_long, idx_rep_pop_cols, "cd_idx_pop")
  cd_idx_long %>%
    group_by(CD119_GEOID) %>%
    summarise(across(all_of(cols_present), ~ wmean_safe(.x, w_pop), .names = "{.col}"), .groups = "drop")
} else NULL

cd_idx_gdp <- if (length(idx_rep_gdp_cols)) {
  cols_present <- ensure_existing_cols(cd_idx_long, idx_rep_gdp_cols, "cd_idx_gdp")
  cd_idx_long %>%
    group_by(CD119_GEOID) %>%
    summarise(across(all_of(cols_present), ~ wmean_safe(.x, w_gdp), .names = "{.col}"), .groups = "drop")
} else NULL

cd_idx <- reduce(compact(list(cd_idx_pop, cd_idx_gdp)), ~ full_join(.x, .y, by = "CD119_GEOID"))

# Final CD table
cd_out <- cd_meas %>%
  full_join(cd_idx, by = "CD119_GEOID") %>%
  transmute(
    geo_type = "Congressional District",
    geo_code = CD119_GEOID,
    geo_name = paste0(substr(CD119_GEOID, 1, 2), "-", substr(CD119_GEOID, 3, 4)),  # e.g., "06-01"
    across(all_of(ensure_existing_cols(., c(meas_cols_union, idx_cols_union), "cd_out")))
  ) %>% distinct(geo_code, .keep_all = TRUE)
debug_info(paste("CD output rows =", nrow(cd_out)))

# ---------- 8) Bind all, align with geo_long footprint ----------
debug_header("Bind all layers & align to geo_long")
innovation_geo_raw <- bind_rows(
  states_out,
  counties_out,
  cbsa_out,
  pea_out,
  cd_out
)

# Show counts by type for sanity
debug_header("Counts in innovation_geo_raw by geo_type")
print(innovation_geo_raw %>% count(geo_type), n=Inf)

# Align with geo_long; ensure every geo_long row is present (fill missing with NA indicators)
# Expect geo_long$geo_type to use the same values used above (State, County, Metro Area, Economic Area, Congressional District)
innovation_geo <- geo_long %>%
  rename(geo_type = geo_type, geo_name_target = geo_name, geo_code = geo_code) %>%
  left_join(innovation_geo_raw, by = c("geo_type","geo_code")) %>%
  mutate(
    geo_name = coalesce(geo_name, geo_name_target)
  ) %>%
  select(geo_type, geo_code, geo_name, everything(), -geo_name_target) %>%
  distinct(geo_type, geo_code, .keep_all = TRUE)

# ---------- 9) Quick QA ----------
message("\n== innovation_geo coverage check ==")
by_type_target <- geo_long %>% count(geo_type, name = "target_n")
by_type_have   <- innovation_geo %>% count(geo_type, name = "have_n")
print(by_type_target %>% left_join(by_type_have, by = "geo_type"))

missing_any <- anti_join(geo_long %>% rename(gt = geo_type, gc = geo_code),
                         innovation_geo %>% select(geo_type, geo_code) %>% rename(gt = geo_type, gc = geo_code),
                         by = c("gt"="gt","gc"="gc"))
if (nrow(missing_any)) {
  message("WARNING: Missing rows in innovation_geo for some geo_long codes (showing first 20):")
  print(utils::head(missing_any, 20))
} else {
  message("All geo_long codes are present in innovation_geo.")
}

#Replace any "NaN" values with true blank/NA
innovation_geo <- innovation_geo %>%
  mutate(across(where(is.numeric), ~ ifelse(is.nan(.), NA, .)))


debug_ok("Script completed.")

================================================================================
# END innovation_geo.R
================================================================================


================================================================================
# BEGIN feas.R
================================================================================

###############################################################################
# ENHANCED FEASIBILITY PIPELINE - Fully Robust with Comprehensive Debugging
# Version 2.0 - Complete rewrite with extensive validation and error handling
###############################################################################

cat("\n========================================================================\n")
cat("FEASIBILITY PIPELINE v2.0 - STARTING", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================================================\n")

# ========================== 0) Libraries & Setup ==========================
suppressPackageStartupMessages({
  required_packages <- c("dplyr", "tidyr", "stringr", "purrr", "readr", 
                         "janitor", "tibble", "rlang")
  
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop(sprintf("Required package '%s' is not installed. Please install it first.", pkg))
    }
    library(pkg, character.only = TRUE, quietly = TRUE)
  }
  
  # Optional parallel processing
  if (!requireNamespace("multidplyr", quietly = TRUE)) {
    message("[PARALLEL] Package 'multidplyr' not installed; using sequential processing")
  } else {
    library(multidplyr, quietly = TRUE)
  }
})

# Set global options for better debugging
options(
  dplyr.summarise.inform = FALSE,
  warn = 1,  # Print warnings as they occur
  stringsAsFactors = FALSE
)

# ---- Enhanced Debug Helpers ----
with_timing <- function(label, expr, verbose = TRUE) {
  if (verbose) cat("\n[TIMING] START:", label, "at", format(Sys.time(), "%H:%M:%S"), "\n")
  start_mem <- gc(reset = TRUE, full = FALSE)[2, 2]  # Used memory in MB
  
  t <- system.time({ 
    res <- tryCatch(
      force(expr),
      error = function(e) {
        cat("\n[ERROR] in", label, ":", conditionMessage(e), "\n")
        stop(e)
      }
    )
  })
  
  end_mem <- gc(full = FALSE)[2, 2]
  mem_diff <- end_mem - start_mem
  
  if (verbose) {
    cat(sprintf("[TIMING] END: %s | elapsed=%.2fs | mem_delta=%.1fMB\n",
                label, t[["elapsed"]], mem_diff))
  }
  invisible(res)
}

mem_usage <- function() {
  m <- gc(full = FALSE)
  cat(sprintf("[MEMORY] Used: %.1f MB | Max: %.1f MB\n", 
              m[2, 2], m[2, 6]))
}

# ---- Enhanced fix_df with validation ----
fix_df <- function(df, name = "data") {
  cat("[FIX_DF] Processing:", name, "\n")
  
  if (is.character(df) && length(df) == 1L) {
    if (!file.exists(df)) {
      stop(sprintf("File not found: %s", df))
    }
    cat("[FIX_DF] Reading from file:", df, "\n")
    df <- readr::read_csv(df, show_col_types = FALSE, 
                          name_repair = "unique_quiet",
                          progress = FALSE, 
                          guess_max = 100000)
  }
  
  # Convert to tibble and clean names
  df <- as_tibble(df)
  orig_rows <- nrow(df)
  orig_cols <- ncol(df)
  
  # Fix column names
  nm <- names(df)
  if (is.null(nm)) nm <- rep("", ncol(df))
  empty <- which(is.na(nm) | trimws(nm) == "")
  if (length(empty) > 0) {
    nm[empty] <- paste0("col_", empty)
    cat("[FIX_DF] Fixed", length(empty), "empty column names\n")
  }
  names(df) <- make.unique(nm, sep = "_")
  
  # Remove empty rows and columns
  df <- janitor::remove_empty(df, c("rows", "cols"), quiet = FALSE)
  
  cat(sprintf("[FIX_DF] %s: %d→%d rows, %d→%d cols\n", 
              name, orig_rows, nrow(df), orig_cols, ncol(df)))
  
  df
}

# ---- Enhanced dbg with more details ----
dbg <- function(df, name = deparse(substitute(df)), show_sample = TRUE) {
  cat("\n╔════════════════════════════════════════╗\n")
  cat("║", sprintf("%-38s", paste("DEBUG:", name)), "║\n")
  cat("╚════════════════════════════════════════╝\n")
  
  if (inherits(df, "data.frame")) {
    cat(sprintf("• Dimensions: %d rows × %d columns\n", nrow(df), ncol(df)))
    cat(sprintf("• Memory size: %s\n", format(object.size(df), units = "auto")))
    
    # Data types summary
    type_summary <- table(sapply(df, class))
    cat("• Column types:", paste(names(type_summary), type_summary, sep = ":", collapse = ", "), "\n")
    
    if (nrow(df) > 0 && ncol(df) > 0) {
      # NA analysis
      na_counts <- colSums(is.na(df))
      na_pct <- round(100 * na_counts / nrow(df), 1)
      high_na <- which(na_pct > 50)
      if (length(high_na) > 0) {
        cat("• High NA columns (>50%):\n")
        for (i in head(high_na, 5)) {
          cat(sprintf("  - %s: %.1f%%\n", names(df)[i], na_pct[i]))
        }
      }
      
      # Show sample
      if (show_sample) {
        cat("\n• Sample data:\n")
        print(head(df, 3))
      }
      
      # Check for duplicate rows
      n_dups <- sum(duplicated(df))
      if (n_dups > 0) {
        cat(sprintf("\n⚠ WARNING: %d duplicate rows detected\n", n_dups))
      }
    }
  } else {
    str(df, max.level = 2)
  }
  
  invisible(df)
}

# ---- Enhanced join diagnostics ----
check_join <- function(x, y, by, nm_x = "x", nm_y = "y", 
                       show_unmatched = 5, validate_keys = TRUE) {
  cat("\n┌─ JOIN DIAGNOSTIC ─────────────────────┐\n")
  cat(sprintf("│ Joining %s → %s\n", nm_x, nm_y))
  cat(sprintf("│ By: %s\n", paste(names(by), by, sep = "=", collapse = ", ")))
  cat(sprintf("│ Rows: %s=%d, %s=%d\n", nm_x, nrow(x), nm_y, nrow(y)))
  
  # Validate join keys exist
  if (validate_keys) {
    for (key in names(by)) {
      if (!key %in% names(x)) {
        stop(sprintf("Join key '%s' not found in %s", key, nm_x))
      }
    }
    for (key in by) {
      if (!key %in% names(y)) {
        stop(sprintf("Join key '%s' not found in %s", key, nm_y))
      }
    }
  }
  
  # Check for duplicate keys
  x_keys <- x[names(by)]
  y_keys <- y[by]
  names(y_keys) <- names(by)
  
  x_dups <- sum(duplicated(x_keys))
  y_dups <- sum(duplicated(y_keys))
  
  if (x_dups > 0) cat(sprintf("│ ⚠ %s has %d duplicate key combinations\n", nm_x, x_dups))
  if (y_dups > 0) cat(sprintf("│ ⚠ %s has %d duplicate key combinations\n", nm_y, y_dups))
  
  # Perform join
  result <- suppressWarnings(
    left_join(x, y, by = by, relationship = "many-to-many")
  )
  
  cat(sprintf("│ Result: %d rows (%.1fx)\n", 
              nrow(result), nrow(result) / nrow(x)))
  
  # Analyze unmatched
  unmatched <- suppressWarnings(anti_join(x, y, by = by))
  n_unmatched <- nrow(unmatched)
  pct_unmatched <- 100 * n_unmatched / nrow(x)
  
  cat(sprintf("│ Unmatched: %d rows (%.1f%%)\n", n_unmatched, pct_unmatched))
  
  if (n_unmatched > 0 && show_unmatched > 0) {
    cat("│ Sample unmatched:\n")
    sample_unmatched <- head(unmatched[names(by)], show_unmatched)
    for (i in 1:nrow(sample_unmatched)) {
      cat(sprintf("│   • %s\n", 
                  paste(sample_unmatched[i,], collapse = " | ")))
    }
  }
  
  cat("└───────────────────────────────────────┘\n")
  
  invisible(result)
}

# ---- Safe weighted mean with validation ----
wtd_mean <- function(x, w, na.rm = TRUE) {
  if (length(x) != length(w)) {
    stop("Length mismatch: x and w must have same length")
  }
  
  if (all(is.na(x)) || all(is.na(w))) {
    return(NA_real_)
  }
  
  if (na.rm) {
    ok <- !is.na(x) & !is.na(w) & w > 0
    if (!any(ok)) return(NA_real_)
    x <- x[ok]
    w <- w[ok]
  }
  
  if (sum(w) == 0) return(NA_real_)
  
  weighted.mean(x, w, na.rm = FALSE)
}

# ---- FIPS utilities with validation ----
to_fips5 <- function(x) {
  x <- as.character(x)
  x <- gsub("[^0-9]", "", x)  # Remove non-numeric characters
  
  # Handle special cases
  x[x == ""] <- NA_character_
  
  # Pad to 5 digits
  result <- stringr::str_pad(x, width = 5, pad = "0", side = "left")
  
  # Validate
  invalid <- !is.na(result) & !grepl("^[0-9]{5}$", result)
  if (any(invalid)) {
    warning(sprintf("Invalid FIPS codes detected: %s", 
                   paste(head(result[invalid], 5), collapse = ", ")))
  }
  
  result
}

# ---- Enhanced column resolver ----
resolve_col <- function(df, col, stop_on_error = TRUE) {
  # Direct match
  if (col %in% names(df)) return(col)
  
  # Case-insensitive match
  lower_match <- which(tolower(names(df)) == tolower(col))
  if (length(lower_match) == 1) return(names(df)[lower_match])
  
  # Space/dot/underscore variations
  variations <- c(
    gsub(" ", ".", col, fixed = TRUE),
    gsub("\\.", " ", col),
    gsub("_", ".", col),
    gsub("\\.", "_", col),
    gsub(" ", "_", col),
    gsub("_", " ", col),
    gsub("[^A-Za-z0-9]", ".", col),
    gsub("[^A-Za-z0-9]", "_", col)
  )
  
  for (var in unique(variations)) {
    if (var %in% names(df)) return(var)
  }
  
  # Fuzzy matching as last resort
  distances <- stringdist::stringdist(col, names(df), method = "jw")
  if (min(distances) < 0.1) {
    best_match <- names(df)[which.min(distances)]
    warning(sprintf("Column '%s' not found, using fuzzy match: '%s'", 
                   col, best_match))
    return(best_match)
  }
  
  if (stop_on_error) {
    stop(sprintf("Column '%s' not found. Available columns: %s",
                col, paste(head(names(df), 10), collapse = ", ")))
  } else {
    warning(sprintf("Column '%s' not found in dataframe", col))
    return(NULL)
  }
}

# ========================== 1) Data Loading & Validation ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 1: DATA LOADING & INITIAL VALIDATION\n")
cat("═══════════════════════════════════════════════════════════════════\n")

# Validate required objects exist
required_objects <- c("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA", "geo", "geo_long",
                     "tigris_counties_2020_raw", "geocorr_county_2020_cd_119",
                     "geocorr_ct_county_cd_119", "tigris_cbsa_2020_raw",
                     "tigris_states_2024_raw", "tigris_congressional_districts_2024_raw")

missing_objects <- required_objects[!sapply(required_objects, exists)]
if (length(missing_objects) > 0) {
  stop(sprintf("Required objects missing: %s", 
              paste(missing_objects, collapse = ", ")))
}

# Load CGT data with validation
if (!exists("cgt_county_raw")) {
  if (!exists("DATA_DIR")) {
    stop("DATA_DIR not defined. Please set the data directory path.")
  }
  
  cgt_file <- file.path(DATA_DIR, "CGT_county_data", "cgt_county_data_08_29_2024.csv")
  if (!file.exists(cgt_file)) {
    stop(sprintf("CGT data file not found: %s", cgt_file))
  }
  
  cgt_county_raw <- with_timing(
    "Load CGT county data",
    readr::read_csv(cgt_file, show_col_types = FALSE, 
                   progress = TRUE, guess_max = 100000) %>%
      fix_df("cgt_county_raw")
  )
}

dbg(cgt_county_raw, "cgt_county_raw")
mem_usage()

# Load optional supplementary data
if (exists("paths") && !is.null(paths$raw_data)) {
  supp_files <- list(
    cim_eco_eti = "CIM_eco_eti_invest_categories.csv",
    eco_rmi_crosswalk = "eco_rmi_updated_crosswalk.csv", 
    clean_industry_naics = "clean_industry_naics.csv"
  )
  
  for (name in names(supp_files)) {
    obj_name <- paste0(name, "_raw")
    if (!exists(obj_name)) {
      file_path <- file.path(paths$raw_data, supp_files[[name]])
      if (file.exists(file_path)) {
        assign(obj_name, fix_df(file_path, name), envir = .GlobalEnv)
        cat(sprintf("✓ Loaded %s\n", name))
      } else {
        cat(sprintf("⚠ File not found: %s\n", supp_files[[name]]))
      }
    }
  }
}

# Define geographies with validation
if (!exists("geographies")) {
  geographies <- c("State.Name", "cd_119", "PEA", "CBSA.Title", "GeoName")
  cat("ℹ Using default geographies:", paste(geographies, collapse = ", "), "\n")
}

if (!exists("geographies_clean")) {
  geographies_clean <- c("State Name", "cd_119", "PEA", "CBSA Title", "GeoName")
  cat("ℹ Using default clean geography labels\n")
}

# ========================== 2) County Normalization ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 2: COUNTY FIPS NORMALIZATION (2018 → 2020)\n") 
cat("═══════════════════════════════════════════════════════════════════\n")

# Prepare states reference
states_2024 <- with_timing(
  "Prepare states reference",
  tigris_states_2024_raw %>%
    as_tibble() %>%
    transmute(
      STATEFP = GEOID,
      STUSPS,
      STATE_NAME = NAME
    ) %>%
    filter(!is.na(STATEFP))
)

# Prepare counties reference with validation
counties_2020 <- with_timing(
  "Prepare counties 2020 reference",
  tigris_counties_2020_raw %>%
    as_tibble() %>%
    select(STATEFP, COUNTYFP, GEOID, NAME, NAMELSAD) %>%
    left_join(states_2024, by = "STATEFP") %>%
    mutate(fips5 = GEOID) %>%
    filter(!is.na(fips5)) %>%
    select(fips5, STATEFP, COUNTYFP, STUSPS, STATE_NAME, NAME, NAMELSAD) %>%
    distinct()
)

cat(sprintf("✓ Counties 2020 reference: %d counties\n", nrow(counties_2020)))

# Manual FIPS recodes (legacy → 2020)
manual_county_recode <- tribble(
  ~from,  ~to,     ~note,
  "51515", "51019", "Bedford city VA → Bedford County",
  "02270", "02158", "Wade Hampton AK → Kusilvak Census Area", 
  "46113", "46102", "Shannon SD → Oglala Lakota County",
  "02261", "02063", "Valdez-Cordova AK → Chugach (split)",
  "02261", "02066", "Valdez-Cordova AK → Copper River (split)"
)

# Process CGT county data with enhanced validation
cgt_county <- with_timing(
  "Process and normalize CGT county data",
  {
    df <- cgt_county_raw %>%
      clean_names() %>%
      mutate(
        county_fips5_orig = to_fips5(county),
        county_fips5 = county_fips5_orig
      )
    
    # Apply manual recodes
    cat("Applying manual FIPS recodes...\n")
    for (i in 1:nrow(manual_county_recode)) {
      from_code <- manual_county_recode$from[i]
      to_code <- manual_county_recode$to[i]
      note <- manual_county_recode$note[i]
      
      matches <- sum(df$county_fips5 == from_code, na.rm = TRUE)
      if (matches > 0) {
        df$county_fips5[df$county_fips5 == from_code] <- to_code
        cat(sprintf("  • %s: %d rows updated\n", note, matches))
      }
    }
    
    # Extract state info from county_name
    df <- df %>%
      mutate(
        # Extract state abbreviation from "County, ST" format
        county_state_abbr = stringr::str_extract(county_name, ", ([A-Z]{2})$") %>%
          stringr::str_remove(", "),
        
        # Clean county label
        county_label = stringr::str_remove(county_name, ",\\s*[A-Z]{2}$") %>%
          stringr::str_trim()
      )
    
    # Special handling for Alaska split (Valdez-Cordova)
    if ("02261" %in% unique(df$county_fips5_orig)) {
      cat("\nSpecial case: Splitting Valdez-Cordova (02261)...\n")
      
      vc_rows <- df %>% filter(county_fips5_orig == "02261")
      n_vc <- nrow(vc_rows)
      
      if (n_vc > 0) {
        # Create Chugach records
        vc_chugach <- vc_rows %>%
          mutate(
            county_fips5 = "02063",
            county_name = "Chugach Census Area, AK",
            county_label = "Chugach Census Area",
            county_state_abbr = "AK"
          )
        
        # Create Copper River records
        vc_copper <- vc_rows %>%
          mutate(
            county_fips5 = "02066",
            county_name = "Copper River Census Area, AK",
            county_label = "Copper River Census Area",
            county_state_abbr = "AK"
          )
        
        # Remove original and add splits
        df <- df %>%
          filter(county_fips5_orig != "02261") %>%
          bind_rows(vc_chugach, vc_copper)
        
        cat(sprintf("  • Split %d rows → %d Chugach + %d Copper River\n",
                   n_vc, nrow(vc_chugach), nrow(vc_copper)))
      }
    }
    
    df
  }
)

# Validation: Check unmatched counties
unmatched_analysis <- with_timing(
  "Analyze unmatched counties",
  {
    unmatched <- cgt_county %>%
      filter(!county_fips5 %in% counties_2020$fips5) %>%
      group_by(county_fips5, county_name, county_state_abbr) %>%
      summarise(n_records = n(), .groups = "drop") %>%
      arrange(desc(n_records))
    
    # Separate 999 codes (not reported) from real unmatched
    not_reported <- unmatched %>% filter(grepl("999$", county_fips5))
    real_unmatched <- unmatched %>% filter(!grepl("999$", county_fips5))
    
    cat(sprintf("\nUnmatched Analysis:\n"))
    cat(sprintf("  • Not reported (XXX999): %d codes, %d total records\n",
               nrow(not_reported), sum(not_reported$n_records)))
    cat(sprintf("  • Real unmatched: %d codes, %d total records\n",
               nrow(real_unmatched), sum(real_unmatched$n_records)))
    
    if (nrow(real_unmatched) > 0) {
      cat("\n  Real unmatched counties (top 10):\n")
      print(head(real_unmatched, 10))
    }
    
    list(not_reported = not_reported, real_unmatched = real_unmatched)
  }
)

# Try secondary matching by name for real unmatched
if (nrow(unmatched_analysis$real_unmatched) > 0) {
  cat("\nAttempting secondary matching by county name + state...\n")
  
  counties_2020_lookup <- counties_2020 %>%
    mutate(
      lookup_name = NAMELSAD,
      lookup_state = STUSPS
    ) %>%
    select(fips5, lookup_name, lookup_state)
  
  remapping <- unmatched_analysis$real_unmatched %>%
    left_join(
      counties_2020_lookup,
      by = c("county_label" = "lookup_name", 
             "county_state_abbr" = "lookup_state")
    ) %>%
    filter(!is.na(fips5)) %>%
    select(county_fips5, new_fips5 = fips5)
  
  if (nrow(remapping) > 0) {
    cat(sprintf("  • Found %d remappings by name\n", nrow(remapping)))
    
    # Apply remappings
    cgt_county <- cgt_county %>%
      left_join(remapping, by = "county_fips5") %>%
      mutate(county_fips5 = coalesce(new_fips5, county_fips5)) %>%
      select(-new_fips5)
  }
}

dbg(cgt_county, "cgt_county (normalized)")

# ========================== 3) Geographic Keys & Mapping ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 3: BUILDING GEOGRAPHIC KEYS AND MAPPINGS\n")
cat("═══════════════════════════════════════════════════════════════════\n")

# Standardize geo column names
geo_std <- with_timing(
  "Standardize geo dataframe",
  {
    df <- geo
    
    # Handle both dotted and spaced versions
    possible_renames <- list(
      c("CBSA.Title", "CBSA Title"),
      c("CBSA.Code", "CBSA Code"),
      c("State.Name", "State Name")
    )
    
    for (pair in possible_renames) {
      target <- pair[1]
      alternatives <- pair[-1]
      
      if (!target %in% names(df)) {
        for (alt in alternatives) {
          if (alt %in% names(df)) {
            df <- df %>% rename(!!target := !!alt)
            cat(sprintf("  • Renamed '%s' → '%s'\n", alt, target))
            break
          }
        }
      }
    }
    
    df
  }
)

# Build comprehensive geo keys
geo_keys <- with_timing(
  "Build geographic keys",
  {
    keys_list <- list(
      # States
      geo_std %>%
        filter(!is.na(State.Name), !is.na(state.fips)) %>%
        distinct(
          geo_type = "State",
          geo_name = State.Name,
          geo_code = sprintf("%02d", as.integer(state.fips))
        ),
      
      # Congressional Districts
      geo_std %>%
        filter(!is.na(cd_119), !is.na(GEOID_2)) %>%
        distinct(
          geo_type = "Congressional District",
          geo_name = cd_119,
          geo_code = GEOID_2
        ),
      
      # Economic Areas
      geo_std %>%
        filter(!is.na(PEA), !is.na(FCC_PEA_Number)) %>%
        distinct(
          geo_type = "Economic Area",
          geo_name = PEA,
          geo_code = as.character(FCC_PEA_Number)
        ),
      
      # Metro Areas
      geo_std %>%
        filter(!is.na(CBSA.Title), !is.na(CBSA.Code)) %>%
        distinct(
          geo_type = "Metro Area",
          geo_name = CBSA.Title,
          geo_code = CBSA.Code
        ),
      
      # Counties
      geo_std %>%
        filter(!is.na(GeoName), !is.na(fips)) %>%
        distinct(
          geo_type = "County",
          geo_name = GeoName,
          geo_code = fips
        )
    )
    
    result <- bind_rows(keys_list) %>% distinct()
    
    # Validation
    cat("\nGeo keys summary:\n")
    result %>%
      group_by(geo_type) %>%
      summarise(
        n = n(),
        n_missing_code = sum(is.na(geo_code)),
        .groups = "drop"
      ) %>%
      print()
    
    result
  }
)

# ========================== 4) GDP Weights & County Geography ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 4: GDP WEIGHTS AND COUNTY GEOGRAPHY ENRICHMENT\n")
cat("═══════════════════════════════════════════════════════════════════\n")

# Build GDP weights with fallback
gdp_weights <- with_timing(
  "Build GDP weights",
  {
    # Primary GDP from geo
    primary_gdp <- geo_std %>%
      filter(!is.na(fips), !is.na(gdp)) %>%
      distinct(county_fips5 = fips, gdp_primary = gdp)
    
    # Fallback GDP from crosswalk
    fallback_gdp <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
      transmute(
        county_fips5 = COUNTY_GEOID,
        gdp_fallback = COUNTY_GDP_2022
      )
    
    # Combine with validation
    weights <- primary_gdp %>%
      full_join(fallback_gdp, by = "county_fips5") %>%
      mutate(
        gdp_weight = coalesce(gdp_primary, gdp_fallback),
        # Zero weight for "not reported" counties
        gdp_weight = if_else(grepl("999$", county_fips5), 0, gdp_weight),
        gdp_weight = if_else(is.na(gdp_weight), 0, gdp_weight)
      ) %>%
      select(county_fips5, gdp_weight)
    
    # Validation
    cat(sprintf("GDP weights summary:\n"))
    cat(sprintf("  • Total counties: %d\n", nrow(weights)))
    cat(sprintf("  • With positive weight: %d\n", sum(weights$gdp_weight > 0)))
    cat(sprintf("  • Zero weight: %d\n", sum(weights$gdp_weight == 0)))
    cat(sprintf("  • Total GDP: $%.1f trillion\n", 
               sum(weights$gdp_weight, na.rm = TRUE) / 1e6))
    
    weights
  }
)

# Build county geography enrichment
county_geo <- with_timing(
  "Build county geography",
  geo_std %>%
    select(
      State.Name, state_abbr, state.fips,
      GeoName, fips, 
      PEA, FCC_PEA_Number,
      CBSA.Title, CBSA.Code,
      cd_119, GEOID_2, 
      percent_district, gdp
    ) %>%
    rename(county_fips5 = fips) %>%
    filter(!is.na(county_fips5))
)

# Congressional district allocation with Connecticut handling
cd_alloc_all <- with_timing(
  "Build CD allocation factors",
  {
    # Standard allocation
    standard <- geocorr_county_2020_cd_119 %>%
      transmute(
        COUNTY_GEOID = COUNTY_GEOID,
        CD119_GEOID = CD119_GEOID,
        cd_to_county = `cd119-to-county allocation factor`
      )
    
    # Connecticut special handling
    ct_special <- geocorr_ct_county_cd_119 %>%
      transmute(
        COUNTY_GEOID = `County code`,
        CD119_GEOID = paste0("09", `Congressional district code (119th Congress)`),
        cd_to_county = `cd119-to-CTcounty allocation factor`
      )
    
    # Combine (CT overrides standard)
    result <- standard %>%
      anti_join(ct_special, by = c("COUNTY_GEOID", "CD119_GEOID")) %>%
      bind_rows(ct_special) %>%
      filter(!is.na(cd_to_county), cd_to_county > 0)
    
    cat(sprintf("\nCD allocation summary:\n"))
    cat(sprintf("  • Standard allocations: %d\n", nrow(standard)))
    cat(sprintf("  • Connecticut overrides: %d\n", nrow(ct_special)))
    cat(sprintf("  • Final allocations: %d\n", nrow(result)))
    
    result
  }
)

# Join county geography with CD allocations
county_geo_cd <- check_join(
  county_geo, cd_alloc_all,
  by = c("county_fips5" = "COUNTY_GEOID", "GEOID_2" = "CD119_GEOID"),
  nm_x = "county_geo", nm_y = "cd_alloc"
) %>%
  mutate(
    percent_district = coalesce(100 * cd_to_county, percent_district, 0)
  )

# ========================== 5) Main Feasibility Base ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 5: BUILD MAIN FEASIBILITY BASE TABLE\n")
cat("═══════════════════════════════════════════════════════════════════\n")

county_feas_base <- with_timing(
  "Build county feasibility base",
  {
    # First join: CGT + county geography
    temp1 <- check_join(
      cgt_county, county_geo_cd,
      by = "county_fips5",
      nm_x = "cgt_county", nm_y = "county_geo_cd"
    )
    
    # Second join: Add GDP weights
    result <- check_join(
      temp1, gdp_weights,
      by = "county_fips5",
      nm_x = "temp1", nm_y = "gdp_weights"
    ) %>%
      mutate(
        gdp_weight = coalesce(gdp_weight, 0)
      )
    
    # Validation
    missing_all_geo <- result %>%
      filter(
        is.na(State.Name) & 
        is.na(CBSA.Title) & 
        is.na(PEA) & 
        is.na(cd_119)
      )
    
    cat(sprintf("\nGeography coverage:\n"))
    cat(sprintf("  • Total records: %d\n", nrow(result)))
    cat(sprintf("  • With State: %.1f%%\n", 
               100 * mean(!is.na(result$State.Name))))
    cat(sprintf("  • With CD: %.1f%%\n", 
               100 * mean(!is.na(result$cd_119))))
    cat(sprintf("  • With PEA: %.1f%%\n", 
               100 * mean(!is.na(result$PEA))))
    cat(sprintf("  • With CBSA: %.1f%%\n", 
               100 * mean(!is.na(result$CBSA.Title))))
    cat(sprintf("  • With County: %.1f%%\n", 
               100 * mean(!is.na(result$GeoName))))
    cat(sprintf("  • Missing all geography: %d (%.1f%%)\n",
               nrow(missing_all_geo), 
               100 * nrow(missing_all_geo) / nrow(result)))
    
    result
  }
)

# Create slim version for aggregation
county_feas_base_slim <- county_feas_base %>%
  select(
    State.Name, cd_119, PEA, CBSA.Title, GeoName,
    aggregation_level, aggregation_level_desc,
    industry_desc, industry_code, 
    density, pci,
    gdp_weight, percent_district
  )

mem_usage()

# ========================== 6) Parallel Processing Setup ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 6: PARALLEL PROCESSING SETUP\n")
cat("═══════════════════════════════════════════════════════════════════\n")

HAS_MULTIDPLYR <- requireNamespace("multidplyr", quietly = TRUE)

N_WORKERS <- {
  override <- getOption("NCORES_OVERRIDE", default = NA)
  env <- Sys.getenv("R_FUTURE_WORKERS", unset = NA)
  n <- suppressWarnings(as.integer(if (!is.na(override)) override else env))
  if (is.na(n) || n < 1L) n <- max(1L, parallel::detectCores() - 1L)
  min(n, 7)  # Cap at 7 workers to avoid memory issues
}

cat(sprintf("System info:\n"))
cat(sprintf("  • Hostname: %s\n", Sys.info()[["nodename"]]))
cat(sprintf("  • Platform: %s\n", R.version$platform))
cat(sprintf("  • Cores detected: %d\n", parallel::detectCores()))
cat(sprintf("  • Workers to use: %d\n", N_WORKERS))
cat(sprintf("  • multidplyr available: %s\n", HAS_MULTIDPLYR))

# Cluster management
CL <- NULL

# ========================== 7) Aggregation Functions ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 7: DEFINE AGGREGATION FUNCTIONS\n")
cat("═══════════════════════════════════════════════════════════════════\n")

# Main feasibility aggregation
agg_one_geog <- function(df, geog_col, try_multidplyr = FALSE, n_workers = N_WORKERS) {
  geog_col <- resolve_col(df, geog_col)
  gsym <- rlang::sym(geog_col)
  
  cat(sprintf("\n[AGG] Processing: %s\n", geog_col))
  
  # Filter to non-missing geography
  base <- df %>% filter(!is.na(!!gsym))
  
  nrows <- nrow(base)
  nuniq <- n_distinct(base[[geog_col]])
  
  cat(sprintf("  • Input: %s rows, %d unique groups\n", 
             format(nrows, big.mark = ","), nuniq))
  
  # Pre-compute weights
  if (geog_col == "cd_119") {
    base <- base %>%
      mutate(.weight = gdp_weight * coalesce(percent_district, 0) / 100)
  } else {
    base <- base %>%
      distinct(
        !!gsym, 
        aggregation_level, aggregation_level_desc,
        industry_desc, industry_code, 
        density, pci, gdp_weight
      )
  }
  
  # Perform aggregation (sequential for stability)
  cat("  • Aggregating (sequential)...")
  
  if (geog_col == "cd_119") {
    result <- base %>%
      group_by(!!gsym, aggregation_level, aggregation_level_desc, 
               industry_desc, industry_code) %>%
      summarise(
        density = wtd_mean(density, .weight),
        pci = wtd_mean(pci, .weight),
        .groups = "drop"
      )
  } else {
    result <- base %>%
      group_by(!!gsym, aggregation_level, aggregation_level_desc,
               industry_desc, industry_code) %>%
      summarise(
        density = wtd_mean(density, gdp_weight),
        pci = wtd_mean(pci, gdp_weight),
        .groups = "drop"
      )
  }
  
  cat(" done\n")
  cat(sprintf("  • Output: %d rows\n", nrow(result)))
  
  result
}

# Strategic feasibility aggregation
agg_one_geog_strat <- function(df, geog_col, try_multidplyr = FALSE, n_workers = N_WORKERS) {
  geog_col <- resolve_col(df, geog_col)
  gsym <- rlang::sym(geog_col)
  
  cat(sprintf("\n[AGG-STRAT] Processing: %s\n", geog_col))
  
  base <- df %>% filter(!is.na(!!gsym))
  
  nrows <- nrow(base)
  nuniq <- n_distinct(base[[geog_col]])
  
  cat(sprintf("  • Input: %s rows, %d unique groups\n",
             format(nrows, big.mark = ","), nuniq))
  
  # Pre-compute weights
  if (geog_col == "cd_119") {
    base <- base %>%
      mutate(.weight = gdp_weight * coalesce(percent_district, 0) / 100)
  } else {
    base <- base %>%
      distinct(!!gsym, clean_industry, production_phase, density, gdp_weight)
  }
  
  # Aggregate
  cat("  • Aggregating (sequential)...")
  
  if (geog_col == "cd_119") {
    result <- base %>%
      group_by(!!gsym, clean_industry, production_phase) %>%
      summarise(
        density = wtd_mean(density, .weight),
        .groups = "drop"
      )
  } else {
    result <- base %>%
      group_by(!!gsym, clean_industry, production_phase) %>%
      summarise(
        density = wtd_mean(density, gdp_weight),
        .groups = "drop"
      )
  }
  
  cat(" done\n")
  cat(sprintf("  • Output: %d rows\n", nrow(result)))
  
  result
}

# ECI aggregation
agg_one_geog_eci <- function(df, geog_col, try_multidplyr = FALSE, n_workers = N_WORKERS) {
  geog_col <- resolve_col(df, geog_col)
  gsym <- rlang::sym(geog_col)
  
  cat(sprintf("\n[AGG-ECI] Processing: %s\n", geog_col))
  
  base <- df %>% filter(!is.na(!!gsym))
  
  nrows <- nrow(base)
  nuniq <- n_distinct(base[[geog_col]])
  
  cat(sprintf("  • Input: %s rows, %d unique groups\n",
             format(nrows, big.mark = ","), nuniq))
  
  # Pre-compute weights
  if (geog_col == "cd_119") {
    base <- base %>%
      mutate(.weight = gdp_weight * coalesce(percent_district, 0) / 100)
  } else {
    base <- base %>%
      distinct(!!gsym, eci, gdp_weight)
  }
  
  # Aggregate
  cat("  • Aggregating (sequential)...")
  
  if (geog_col == "cd_119") {
    result <- base %>%
      group_by(!!gsym) %>%
      summarise(
        eci = wtd_mean(eci, .weight),
        .groups = "drop"
      )
  } else {
    result <- base %>%
      group_by(!!gsym) %>%
      summarise(
        eci = wtd_mean(eci, gdp_weight),
        .groups = "drop"
      )
  }
  
  cat(" done\n")
  cat(sprintf("  • Output: %d rows\n", nrow(result)))
  
  result
}

# ========================== 8) Main Feasibility Aggregation ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 8: MAIN FEASIBILITY AGGREGATION\n")
cat("═══════════════════════════════════════════════════════════════════\n")

results_list <- list()

for (geog in geographies) {
  results_list[[geog]] <- with_timing(
    sprintf("Aggregate feasibility for %s", geog),
    agg_one_geog(county_feas_base_slim, geog, try_multidplyr = FALSE)
  )
}

# Combine results
county_feas <- with_timing(
  "Combine feasibility results",
  bind_rows(results_list, .id = "geog_col")
)

cat(sprintf("\n✓ Combined feasibility: %d rows\n", nrow(county_feas)))

# Label and clean
county_feas_labeled <- with_timing(
  "Label feasibility results",
  county_feas %>%
    ungroup() %>%
    mutate(
      geo = case_when(
        geog_col == "State.Name" ~ "State",
        geog_col == "cd_119" ~ "Congressional District",
        geog_col == "PEA" ~ "Economic Area",
        geog_col == "GeoName" ~ "County",
        geog_col == "CBSA.Title" ~ "Metro Area",
        TRUE ~ "Unknown"
      ),
      geo_name = case_when(
        geog_col == "State.Name" ~ State.Name,
        geog_col == "cd_119" ~ cd_119,
        geog_col == "PEA" ~ PEA,
        geog_col == "GeoName" ~ GeoName,
        geog_col == "CBSA.Title" ~ CBSA.Title,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(geo_name), geo_name != "", !grepl("NA-NA", geo_name)) %>%
    select(geo, geo_name, aggregation_level, aggregation_level_desc,
           industry_desc, industry_code, density, pci)
)

# Calculate percentile ranks
feas <- with_timing(
  "Calculate percentile ranks",
  county_feas_labeled %>%
    group_by(geo, aggregation_level, industry_desc) %>%
    mutate(
      industry_feas_perc = percent_rank(density),
      industry_feas_perc = round(industry_feas_perc, 4)
    ) %>%
    group_by(geo, geo_name, aggregation_level) %>%
    mutate(
      region_feas_rank = rank(-industry_feas_perc, ties.method = "first")
    ) %>%
    ungroup()
)

dbg(feas, "feas (final)", show_sample = FALSE)

# ========================== 9) Strategic Feasibility ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 9: STRATEGIC FEASIBILITY\n")
cat("═══════════════════════════════════════════════════════════════════\n")

# Prepare clean industry NAICS mapping
if (exists("clean_industry_naics_raw") || exists("eco_rmi_crosswalk_raw")) {
  clean_industry_naics <- with_timing(
    "Prepare clean industry mapping",
    {
      parts <- list()
      
      if (exists("clean_industry_naics_raw")) {
        parts$clean <- clean_industry_naics_raw %>%
          clean_names() %>%
          rename_with(~gsub("x6_digit_code", "industry_code_6", .x)) %>%
          select(any_of(c("clean_industry", "production_phase", 
                         "industry_code_6", "naics_desc")))
      }
      
      if (exists("eco_rmi_crosswalk_raw")) {
        parts$eco <- eco_rmi_crosswalk_raw %>%
          clean_names() %>%
          rename_with(~gsub("x6[._]digit[._]code", "industry_code_6", .x)) %>%
          select(any_of(c("clean_industry", "production_phase",
                         "industry_code_6", "naics_desc")))
      }
      
      result <- bind_rows(parts) %>%
        distinct() %>%
        mutate(industry_code_6 = as.integer(industry_code_6)) %>%
        filter(!is.na(industry_code_6))
      
      cat(sprintf("  • Clean industries: %d\n", n_distinct(result$clean_industry)))
      cat(sprintf("  • NAICS codes: %d\n", n_distinct(result$industry_code_6)))
      
      result
    }
  )
  
  # Build strategic feasibility base
  county_feas_strategic_base <- with_timing(
    "Build strategic feasibility base",
    {
      temp <- cgt_county %>%
        mutate(industry_code_6 = as.integer(industry_code)) %>%
        inner_join(
          clean_industry_naics,
          by = "industry_code_6",
          relationship = "many-to-many"
        )
      
      cat(sprintf("  • Matched records: %d\n", nrow(temp)))
      
      # Add geography
      temp <- check_join(
        temp, county_geo_cd,
        by = "county_fips5",
        nm_x = "cgt_strategic", nm_y = "county_geo_cd",
        show_unmatched = 3
      )
      
      # Add GDP weights
      result <- check_join(
        temp, gdp_weights,
        by = "county_fips5",
        nm_x = "temp", nm_y = "gdp_weights",
        show_unmatched = 3
      ) %>%
        mutate(gdp_weight = coalesce(gdp_weight, 0))
      
      result
    }
  )
  
  # Create slim version
  county_feas_strategic_base_slim <- county_feas_strategic_base %>%
    select(
      State.Name, cd_119, PEA, CBSA.Title, GeoName,
      clean_industry, production_phase, density,
      gdp_weight, percent_district
    )
  
  # Aggregate strategic feasibility
  results_list_strat <- list()
  
  for (geog in geographies) {
    results_list_strat[[geog]] <- with_timing(
      sprintf("Aggregate strategic for %s", geog),
      agg_one_geog_strat(county_feas_strategic_base_slim, geog)
    )
  }
  
  # Process strategic results
  feas_strategic <- with_timing(
    "Process strategic feasibility",
    {
      combined <- bind_rows(results_list_strat, .id = "geog_col")
      
      result <- combined %>%
        ungroup() %>%
        mutate(
          geo = case_when(
            geog_col == "State.Name" ~ "State",
            geog_col == "cd_119" ~ "Congressional District",
            geog_col == "PEA" ~ "Economic Area",
            geog_col == "GeoName" ~ "County",
            geog_col == "CBSA.Title" ~ "Metro Area",
            TRUE ~ "Unknown"
          ),
          geo_name = case_when(
            geog_col == "State.Name" ~ State.Name,
            geog_col == "cd_119" ~ cd_119,
            geog_col == "PEA" ~ PEA,
            geog_col == "GeoName" ~ GeoName,
            geog_col == "CBSA.Title" ~ CBSA.Title,
            TRUE ~ NA_character_
          )
        ) %>%
        filter(!is.na(geo_name), geo_name != "", !grepl("NA-NA", geo_name)) %>%
        mutate(
          industry = case_when(
            production_phase == "Manufacturing" ~ paste0(clean_industry, " Manufacturing"),
            production_phase == "Operations" ~ clean_industry,
            TRUE ~ NA_character_
          )
        ) %>%
        filter(!is.na(industry)) %>%
        select(geo, geo_name, industry, density) %>%
        distinct() %>%
        pivot_wider(
          names_from = industry,
          names_prefix = "Feasibility_",
          values_from = density,
          values_fill = NA
        ) %>%
        rowwise() %>%
        mutate(
          Strategic_Feasibility = mean(c_across(where(is.numeric)), na.rm = TRUE)
        ) %>%
        ungroup()
      
      cat(sprintf("  • Strategic feasibility: %d geographies\n", nrow(result)))
      
      result
    }
  )
  
  dbg(feas_strategic, "feas_strategic", show_sample = FALSE)
} else {
  cat("⚠ Skipping strategic feasibility (missing data files)\n")
  feas_strategic <- NULL
}

# ========================== 10) Economic Complexity Index ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 10: ECONOMIC COMPLEXITY INDEX (ECI)\n")
cat("═══════════════════════════════════════════════════════════════════\n")

# Build ECI base
county_eci_base <- with_timing(
  "Build ECI base",
  {
    # Join CGT with geography
    temp <- check_join(
      cgt_county, county_geo_cd,
      by = "county_fips5",
      nm_x = "cgt_county", nm_y = "county_geo_cd",
      show_unmatched = 3
    )
    
    # Add GDP weights
    result <- check_join(
      temp, gdp_weights,
      by = "county_fips5",
      nm_x = "temp", nm_y = "gdp_weights",
      show_unmatched = 3
    ) %>%
      mutate(gdp_weight = coalesce(gdp_weight, 0))
    
    result
  }
)

# Create slim version for ECI
county_eci_base_slim <- county_eci_base %>%
  select(
    State.Name, cd_119, PEA, CBSA.Title, GeoName,
    eci, gdp_weight, percent_district
  )

# Aggregate ECI
results_list_eci <- list()

for (geog in geographies) {
  results_list_eci[[geog]] <- with_timing(
    sprintf("Aggregate ECI for %s", geog),
    agg_one_geog_eci(county_eci_base_slim, geog)
  )
}

# Process ECI results
county_eci <- with_timing(
  "Process ECI results",
  {
    combined <- bind_rows(results_list_eci, .id = "geog_col")
    
    result <- combined %>%
      mutate(
        geo = case_when(
          geog_col == "State.Name" ~ "State",
          geog_col == "cd_119" ~ "Congressional District",
          geog_col == "PEA" ~ "Economic Area",
          geog_col == "GeoName" ~ "County",
          geog_col == "CBSA.Title" ~ "Metro Area",
          TRUE ~ "Unknown"
        ),
        geo_name = case_when(
          geog_col == "State.Name" ~ State.Name,
          geog_col == "cd_119" ~ cd_119,
          geog_col == "PEA" ~ PEA,
          geog_col == "GeoName" ~ GeoName,
          geog_col == "CBSA.Title" ~ CBSA.Title,
          TRUE ~ NA_character_
        )
      ) %>%
      filter(!is.na(geo_name), geo_name != "", !grepl("NA-NA", geo_name)) %>%
      select(geo, geo_name, eci) %>%
      rename(economic_complexity = eci)
    
    cat(sprintf("  • ECI results: %d geographies\n", nrow(result)))
    
    result
  }
)

dbg(county_eci, "county_eci", show_sample = FALSE)

# ========================== 11) Geo Code Mapping ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 11: ATTACH GEO CODES FOR MAPPING\n")
cat("═══════════════════════════════════════════════════════════════════\n")

# Enhanced mapping function
attach_geo_codes <- function(df, geo_keys, geo_long, name = "data") {
  cat(sprintf("\nMapping geo codes for %s...\n", name))
  
  # First pass: direct join with geo_keys
  result <- df %>%
    left_join(
      geo_keys,
      by = c("geo" = "geo_type", "geo_name" = "geo_name"),
      relationship = "many-to-many"
    )
  
  n_missing_1 <- sum(is.na(result$geo_code))
  cat(sprintf("  • After geo_keys join: %d missing codes\n", n_missing_1))
  
  # Second pass: try geo_long as fallback
  if (n_missing_1 > 0) {
    result <- result %>%
      left_join(
        geo_long %>%
          rename(gl_type = geo_type, gl_name = geo_name, gl_code = geo_code),
        by = c("geo" = "gl_type", "geo_name" = "gl_name")
      ) %>%
      mutate(geo_code = coalesce(geo_code, gl_code)) %>%
      select(-gl_code)
    
    n_missing_2 <- sum(is.na(result$geo_code))
    cat(sprintf("  • After geo_long join: %d missing codes\n", n_missing_2))
  }
  
  # Report any remaining unmapped
  if (sum(is.na(result$geo_code)) > 0) {
    unmapped <- result %>%
      filter(is.na(geo_code)) %>%
      distinct(geo, geo_name) %>%
      head(10)
    
    cat("  ⚠ Sample unmapped:\n")
    print(unmapped)
  } else {
    cat("  ✓ All records mapped successfully\n")
  }
  
  result
}

# Map all outputs
feas_long_mapped <- attach_geo_codes(feas, geo_keys, geo_long, "feasibility")

if (!is.null(feas_strategic)) {
  feas_strategic_long_mapped <- attach_geo_codes(
    feas_strategic, geo_keys, geo_long, "strategic feasibility"
  )
} else {
  feas_strategic_long_mapped <- NULL
}

eci_long_mapped <- attach_geo_codes(county_eci, geo_keys, geo_long, "ECI")

# ========================== 12) Final Validation & Cleanup ==========================
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("STEP 12: FINAL VALIDATION AND CLEANUP\n")
cat("═══════════════════════════════════════════════════════════════════\n")

# Validate outputs
validate_output <- function(df, name) {
  cat(sprintf("\nValidating %s:\n", name))
  cat(sprintf("  • Rows: %d\n", nrow(df)))
  cat(sprintf("  • Columns: %d\n", ncol(df)))
  cat(sprintf("  • Unique geographies: %d\n", n_distinct(df$geo_name)))
  cat(sprintf("  • Missing geo_codes: %d\n", sum(is.na(df$geo_code))))
  
  # Check for duplicates
  key_cols <- c("geo", "geo_name")
  if ("aggregation_level" %in% names(df)) {
    key_cols <- c(key_cols, "aggregation_level", "industry_desc")
  }
  
  n_dups <- df %>%
    group_by(across(all_of(key_cols))) %>%
    filter(n() > 1) %>%
    nrow()
  
  if (n_dups > 0) {
    cat(sprintf("  ⚠ Duplicate keys: %d\n", n_dups))
  } else {
    cat("  ✓ No duplicate keys\n")
  }
}

validate_output(feas_long_mapped, "feasibility")
if (!is.null(feas_strategic_long_mapped)) {
  validate_output(feas_strategic_long_mapped, "strategic feasibility")
}
validate_output(eci_long_mapped, "ECI")

# Clean up cluster if created
if (!is.null(CL)) {
  try(parallel::stopCluster(CL), silent = TRUE)
  cat("\n✓ Parallel cluster stopped\n")
}

# Final memory report
cat("\n")
mem_usage()

# Summary
cat("\n╔════════════════════════════════════════════════════════════════════╗\n")
cat("║                     PIPELINE COMPLETE                               ║\n")
cat("╚════════════════════════════════════════════════════════════════════╝\n")

cat("\nFinal outputs:\n")
cat(sprintf("  • feas_long_mapped: %d rows\n", nrow(feas_long_mapped)))
if (!is.null(feas_strategic_long_mapped)) {
  cat(sprintf("  • feas_strategic_long_mapped: %d rows\n", 
             nrow(feas_strategic_long_mapped)))
}
cat(sprintf("  • eci_long_mapped: %d rows\n", nrow(eci_long_mapped)))
cat(sprintf("\nCompleted at %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))

# Display final glimpse
cat("\n═══════════════════════════════════════════════════════════════════\n")
cat("FINAL OUTPUT GLIMPSE - COUNTY ECI\n")
cat("═══════════════════════════════════════════════════════════════════\n")
glimpse(county_eci)

================================================================================
# END feas.R
================================================================================


================================================================================
# BEGIN geo_credits.R
================================================================================

# =================================================================================================
# FULL PIPELINE — LOAD + ENRICH + CREDIT APPORTIONMENT (START TO FINISH)
# - Uses your TOP "base" allocation section (GDP-weighted + CD split handling + 30D)
# - Ensures all required data frames/files are loaded & prepared (robust lower script pieces)
# =================================================================================================

suppressPackageStartupMessages({
  library(dplyr); library(tidyr); library(stringr); library(purrr); library(janitor)
  library(lubridate); library(zoo); library(rlang); library(sf); library(cli)
})

options(sf_max_plot = 1, scipen = 999)
script_start_time <- Sys.time()

# -------------------------------------------------------------------------------------------------
# Helpers: logging, validation, cleaning
# -------------------------------------------------------------------------------------------------

dbg <- function(x, title = deparse(substitute(x)), n = 5, width = 100){
  cat("\n====", title, "====\n", sep = " ")
  if (inherits(x,"data.frame")) {
    cat(sprintf("Rows: %s   Cols: %s \n", nrow(x), ncol(x)))
    print(dplyr::glimpse(x, width = width))
    cat("Head:\n"); print(utils::head(x, n))
    na_ct <- colSums(is.na(x))
    if (any(na_ct>0)) {
      top_na <- sort(na_ct[na_ct>0], decreasing = TRUE)
      cat("\n---- NA counts (top 20) ----\n")
      print(utils::head(top_na, 20))
    }
  } else {
    str(x)
  }
}

cli_msg  <- function(...) cli::cli_inform(list(message = sprintf(...)))
cli_ok   <- function(...) cli::cli_alert_success(sprintf(...))
cli_warn <- function(...) cli::cli_alert_warning(sprintf(...))
cli_oops <- function(...) cli::cli_alert_danger(sprintf(...))

require_cols <- function(df, cols, df_name = deparse(substitute(df))) {
  miss <- setdiff(cols, names(df))
  if (length(miss)) stop(sprintf("[%s] is missing required columns: %s", df_name, paste(miss, collapse = ", ")), call. = FALSE)
}

safe_left_join <- function(x, y, by, suffix = c(".x", ".y")){
  before <- nrow(x)
  out <- suppressMessages(left_join(x, y, by = by, suffix = suffix))
  after <- nrow(out)
  if (after < before) cli_warn("Left-join reduced rows from %s -> %s. Keys: %s", before, after, paste(by, collapse=", "))
  if (after > before) cli_msg  ("Left-join expanded rows from %s -> %s (many-to-one?) Keys: %s", before, after, paste(by, collapse=", "))
  out
}

# Select only existing columns; avoid tidyselect collisions with objects in the env
safe_select <- function(.data, cols){
  cols <- as.character(cols)
  exist <- intersect(cols, names(.data))
  miss  <- setdiff(cols, names(.data))
  if (length(miss)) cli_warn("safe_select: missing columns: %s", paste(miss, collapse=", "))
  dplyr::select(.data, dplyr::all_of(exist))
}

# Coalesce across any present columns into a canonical output column, and drop the dupes
coalesce_cols <- function(df, out_col, candidates){
  present <- intersect(candidates, names(df))
  if (length(present) == 0) {
    cli_warn("coalesce_cols: none of the candidates found for %s: %s",
             out_col, paste(candidates, collapse=", "))
    return(df)
  }
  df %>%
    mutate("{out_col}" := coalesce(!!! rlang::syms(present))) %>%
    select(-any_of(setdiff(present, out_col)))
}

norm_name <- function(x) stringr::str_squish(as.character(x))

# Multi-format date parser with fallbacks + NA safety
parse_date_multi <- function(x){
  x_chr <- as.character(x)
  d1 <- suppressWarnings(lubridate::mdy(x_chr))
  d2 <- suppressWarnings(lubridate::ymd(x_chr))
  d3 <- suppressWarnings(lubridate::dmy(x_chr))
  out <- coalesce(d1, d2, d3)
  still_na <- is.na(out) & !is.na(x_chr) & nzchar(x_chr)
  if (any(still_na)) {
    try2 <- suppressWarnings(as.Date(x_chr[still_na], format = "%m/%d/%y"))
    out[still_na] <- try2
  }
  out
}

# Status normalization — accepts long/short codes and synonyms
normalize_status <- function(x){
  x0 <- tolower(trimws(as.character(x)))
  case_when(
    x0 %in% c("o","operating","operational","in operation","operating facility") ~ "Operating",
    x0 %in% c("uc","u/c","under construction","construction","building") ~ "Under Construction",
    x0 %in% c("a","announced","planned","proposed") ~ "Announced",
    x0 %in% c("c","cancelled","canceled","abandoned","shelved") ~ "Cancelled",
    TRUE ~ NA_character_
  )
}

# Technology normalization (minimal; extend as needed)
normalize_tech <- function(x){
  x0 <- str_to_lower(str_squish(as.character(x)))
  rec <- case_when(
    x0 %in% c("battery","batteries","ev batteries","battery cells","battery manufacturing") ~ "Batteries",
    x0 %in% c("solar","pv","photovoltaics","solar manufacturing","solar modules","modules") ~ "Solar",
    x0 %in% c("wind","wind manufacturing","wind turbine","blades","towers") ~ "Wind",
    x0 %in% c("critical minerals","critical materials","minerals","processing") ~ "Critical Minerals",
    x0 %in% c("hydrogen","clean hydrogen","h2","green hydrogen","blue hydrogen","ammonia (hydrogen)") ~ "Hydrogen",
    x0 %in% c("carbon management","ccs","ccus","carbon capture","direct air capture","dac") ~ "Carbon Management",
    x0 %in% c("cement") ~ "Cement",
    x0 %in% c("iron & steel","steel","iron","iron and steel") ~ "Iron & Steel",
    x0 %in% c("pulp & paper","pulp and paper","paper","pulp") ~ "Pulp & Paper",
    x0 %in% c("sustainable aviation fuels","saf") ~ "Sustainable Aviation Fuels",
    x0 %in% c("clean fuels","renewable fuels","biofuels","rni","rng") ~ "Clean Fuels",
    x0 %in% c("hydroelectric","hydro","conventional hydroelectricity") ~ "Hydroelectric",
    x0 %in% c("geothermal") ~ "Geothermal",
    x0 %in% c("nuclear") ~ "Nuclear",
    x0 %in% c("storage","energy storage","battery storage") ~ "Storage",
    TRUE ~ str_to_title(x0)
  )
  rec
}

# Segment normalization (optional)
normalize_segment <- function(x){
  x0 <- tolower(trimws(as.character(x)))
  case_when(
    x0 %in% c("manufacturing","clean tech manufacturing","clean-tech manufacturing","cleantech manufacturing") ~ "Manufacturing",
    x0 %in% c("energy and industry","energy & industry","industry") ~ "Energy and Industry",
    x0 %in% c("retail") ~ "Retail",
    TRUE ~ str_to_title(x0)
  )
}

# Geography label function
geo_label <- function(geog) switch(geog,
                                   "State.Name" = "State",
                                   "cd_119"      = "Congressional District",
                                   "PEA"         = "Economic Area",
                                   "GeoName"     = "County",
                                   "CBSA Title"  = "Metro Area",
                                   geog)

rank_or_na <- function(x){
  x <- replace_na(x, 0)
  if (all(x == 0)) rep(NA_real_, length(x)) else rank(-x, ties.method = "min")
}

# Zero-pad helper
padr <- function(x, width) stringr::str_pad(as.character(x), width = width, pad = "0")

# State mapping helpers
state_lookup <- tibble(State.Name = c(state.name, "District of Columbia"),
                       state_abbr = c(state.abb,  "DC"))

to_state_abbr <- function(x){
  x0 <- str_squish(as.character(x))
  out <- state_lookup$state_abbr[match(x0, state_lookup$State.Name)]
  pick2 <- str_match(x0, "([A-Z]{2})\\s*$")[,2]
  out <- coalesce(out, pick2)
  hy <- str_match(x0, "([A-Z]{2})(?:-[A-Z]{2})+\\s*$")[,2]
  out <- coalesce(out, hy)
  out <- coalesce(out, state_lookup$state_abbr[match(str_to_title(x0), state_lookup$State.Name)])
  out
}

# Extract first recognizable state abbr from a generic geo_name string
extract_first_state_abbr_from_text <- function(geo_name){
  g <- as.character(geo_name)
  ab <- str_match(g, ",\\s*([A-Z]{2})\\s*$")[,2]
  ab <- coalesce(ab, str_match(g, "^([A-Z]{2})-\\d{2}$")[,2])
  if (any(is.na(ab))){
    tail_name <- str_match(g, ",\\s*([^,]+)$")[,2]
    ab2 <- state_lookup$state_abbr[match(tail_name, state_lookup$State.Name)]
    ab <- coalesce(ab, ab2)
  }
  if (any(is.na(ab))){
    ab3 <- str_match(g, "([A-Z]{2})(?:-[A-Z]{2})+")[,2]
    ab <- coalesce(ab, ab3)
  }
  ab
}

# Avoid tidyselect collisions with any object named cd_119 in env
if (exists("cd_119", inherits = TRUE) && !is.atomic(get("cd_119", inherits = TRUE))) {
  cli_warn("An object named `cd_119` exists in the environment (type: %s). Using all_of()/safe_select to avoid column-selection collisions.",
           class(get("cd_119", inherits = TRUE))[1])
}

# ---- hotfix: safely (re)define fix_df if the binding is locked or missing ----
suppressWarnings({
  ge <- globalenv()
  if (exists("fix_df", envir = ge, inherits = FALSE) && bindingIsLocked("fix_df", ge)) {
    unlockBinding("fix_df", ge)
    on.exit(try(lockBinding("fix_df", ge), silent = TRUE), add = TRUE)  # re-lock when done
  }
})
if (!exists("fix_df", inherits = FALSE)) {
  fix_df <- function(df){
    df %>% tibble::as_tibble() %>% janitor::remove_empty(c("rows","cols"))
  }
}

# -------------------------------------------------------------------------------------------------
# 0) Read CIM inputs (robust) — uses cim_dir (must be set)  [**KEEP YOUR TOP READS**]
# -------------------------------------------------------------------------------------------------

if (!exists("cim_dir") || !dir.exists(cim_dir)) {
  cli_oops("cim_dir not set or directory not found. Please set cim_dir <- 'path/to/CIM' before running.")
  stop("Missing cim_dir", call. = FALSE)
}

cli::cli_h1("Reading CIM CSVs")

investment_raw   <- suppressWarnings(read.csv(file.path(cim_dir,"quarterly_actual_investment.csv"),          skip = 4, check.names = FALSE)) %>% fix_df()
socioecon_raw    <- suppressWarnings(read.csv(file.path(cim_dir,"socioeconomics.csv"),                        skip = 4, check.names = FALSE)) %>% fix_df()
tax_inv_cat_raw  <- suppressWarnings(read.csv(file.path(cim_dir,"federal_actual_investment_by_category.csv"), skip = 4, check.names = FALSE)) %>% fix_df()
tax_inv_state_raw<- suppressWarnings(read.csv(file.path(cim_dir,"federal_actual_investment_by_state.csv"),    skip = 4, check.names = FALSE)) %>% fix_df()

# Baseline checks
require_cols(investment_raw,  c("Segment","State","Technology","Subcategory","quarter","Estimated_Actual_Quarterly_Expenditure","Decarb_Sector"), "investment_raw")
require_cols(tax_inv_cat_raw, c("Segment","Category","quarter","Total Federal Investment"), "tax_inv_cat_raw")
require_cols(tax_inv_state_raw, c("State","quarter","Total Federal Investment"), "tax_inv_state_raw")

# Normalize core inputs
investment  <- investment_raw  %>%
  mutate(
    Segment       = normalize_segment(Segment),
    Technology    = normalize_tech(Technology),
    Decarb_Sector = str_to_title(str_squish(Decarb_Sector))
  )

tax_inv_cat <- tax_inv_cat_raw %>%
  mutate(
    Segment  = normalize_segment(Segment),
    Category = str_squish(as.character(Category))
  )

tax_inv_state <- tax_inv_state_raw

cli::cli_h1("Inputs overview")
dbg(investment,   "investment")
dbg(tax_inv_cat,  "tax_inv_cat")
dbg(tax_inv_state,"tax_inv_state")

# -------------------------------------------------------------------------------------------------
# 1) Ensure TIGRIS + CROSSWALKS available (if not found in memory, fetch via tigris/geocorr fallback)
# -------------------------------------------------------------------------------------------------

# Counties 2020
if (!exists("tigris_counties_2020_raw")) {
  cli_warn("tigris_counties_2020_raw not found in memory. Attempting to download via {tigris} (2020).")
  tigris_counties_2020_raw <- tigris::counties(year = 2020, cb = TRUE, class = "sf") %>% st_transform(4326)
}
require_cols(tigris_counties_2020_raw, c("GEOID","STATEFP","NAMELSAD","CBSAFP","geometry"), "tigris_counties_2020_raw")

# CBSA 2020
if (!exists("tigris_cbsa_2020_raw")) {
  cli_warn("tigris_cbsa_2020_raw not found. Attempting to download via {tigris} (2020).")
  tigris_cbsa_2020_raw <- tigris::core_based_statistical_areas(year = 2020, cb = TRUE, class = "sf") %>% st_transform(4326)
}
require_cols(tigris_cbsa_2020_raw, c("GEOID","NAME","geometry"), "tigris_cbsa_2020_raw")

# States 2024
if (!exists("tigris_states_2024_raw")) {
  cli_warn("tigris_states_2024_raw not found. Attempting to download via {tigris} (2024).")
  tigris_states_2024_raw <- tigris::states(year = 2024, cb = TRUE, class = "sf") %>% st_transform(4326)
}
require_cols(tigris_states_2024_raw, c("GEOID","NAME","STUSPS","geometry"), "tigris_states_2024_raw")

# --- CRS + validity normalizers ---
ensure_wgs84 <- function(x){
  if (!inherits(x, "sf")) stop("ensure_wgs84 expects an sf object.")
  x <- sf::st_make_valid(x)
  if (is.na(sf::st_crs(x))) {
    x <- sf::st_set_crs(x, 4326)
  } else if (!isTRUE(sf::st_crs(x)$epsg == 4326)) {
    x <- sf::st_transform(x, 4326)
  }
  x
}
tigris_counties_2020_raw <- ensure_wgs84(tigris_counties_2020_raw)
tigris_cbsa_2020_raw     <- ensure_wgs84(tigris_cbsa_2020_raw)
tigris_states_2024_raw   <- ensure_wgs84(tigris_states_2024_raw)

# County→PEA crosswalk
if (!exists("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA")) {
  cli_oops("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA not found in memory. Please load it.")
  stop("Missing COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA", call. = FALSE)
}
require_cols(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA,
             c("COUNTY_GEOID","PEA_NAME","PEA_NUMBER","STATE_ABBREVIATION","STATE_NAME","CBSA_NAME","CBSA_GEOID"),
             "COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA")

# County→CD119 crosswalk
if (!exists("geocorr_county_2020_cd_119")) {
  cli_oops("geocorr_county_2020_cd_119 not found in memory. Please load it.")
  stop("Missing geocorr_county_2020_cd_119", call. = FALSE)
}
require_cols(geocorr_county_2020_cd_119,
             c("County code","CD119_code","CD119_GEOID","State abbr.","cd119-to-county allocation factor"),
             "geocorr_county_2020_cd_119")

# CT special-case crosswalk (optional but used if present)
has_ct_xw <- exists("geocorr_ct_county_cd_119")
if (has_ct_xw) {
  require_cols(geocorr_ct_county_cd_119,
               c("County code","Congressional district code (119th Congress)","CTcounty-to-cd119 allocation factor","CT county name pre-2023"),
               "geocorr_ct_county_cd_119")
}

# -------------------------------------------------------------------------------------------------
# 2) Build crosswalks
# -------------------------------------------------------------------------------------------------

cty_to_cbsa <- tigris_counties_2020_raw %>%
  st_drop_geometry() %>%
  transmute(
    COUNTY_GEOID = padr(GEOID, 5),
    COUNTY_NAME  = NAMELSAD,
    STATEFP      = STATEFP,
    CBSA_CODE    = ifelse(!is.na(CBSAFP) & nzchar(CBSAFP), padr(CBSAFP, 5), NA_character_)
  ) %>%
  left_join(
    tigris_cbsa_2020_raw %>% st_drop_geometry() %>% transmute(CBSA_CODE = padr(GEOID, 5), CBSA_TITLE = NAME),
    by = "CBSA_CODE"
  )

cty_to_pea <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
  transmute(
    COUNTY_GEOID = padr(COUNTY_GEOID, 5),
    PEA_NAME     = PEA_NAME,
    PEA_NUMBER   = as.integer(PEA_NUMBER)
  ) %>% distinct()

# Dominant CD per county (largest allocation factor)
cty_to_cd <- geocorr_county_2020_cd_119 %>%
  transmute(
    COUNTY_GEOID = padr(`County code`, 5),
    STATE_ABBR   = `State abbr.`,
    CD119_CODE   = padr(`CD119_code`, 2),
    CD119_GEOID  = `CD119_GEOID`,
    alloc        = `cd119-to-county allocation factor`
  ) %>%
  group_by(COUNTY_GEOID) %>% slice_max(order_by = alloc, with_ties = FALSE) %>% ungroup() %>%
  mutate(cd_119 = paste0(STATE_ABBR, "-", CD119_CODE))

if (has_ct_xw) {
  ct_adj <- geocorr_ct_county_cd_119 %>%
    transmute(
      COUNTY_GEOID = padr(`County code`, 5),
      cd_119_ct    = paste0("CT-", padr(`Congressional district code (119th Congress)`, 2))
    ) %>% distinct(COUNTY_GEOID, .keep_all = TRUE)
  
  ct_unmatched <- anti_join(ct_adj, cty_to_cd, by = "COUNTY_GEOID")
  if (nrow(ct_unmatched) > 0) {
    cli_warn("%s CT county rows in CT override have no match in base county→CD crosswalk; they will be ignored.\n%s",
             nrow(ct_unmatched),
             utils::capture.output(utils::head(ct_unmatched, 5)) |> paste(collapse = "\n"))
  }
  
  cty_to_cd <- cty_to_cd %>%
    left_join(ct_adj, by = "COUNTY_GEOID") %>%
    mutate(
      cd_119     = coalesce(cd_119_ct, cd_119),
      CD119_CODE = if_else(!is.na(cd_119_ct), stringr::str_sub(cd_119, -2), CD119_CODE)
    ) %>%
    select(-cd_119_ct)
}

dbg(cty_to_cbsa, "cty_to_cbsa")
dbg(cty_to_pea,  "cty_to_pea")
dbg(cty_to_cd,   "cty_to_cd")

# -------------------------------------------------------------------------------------------------
# 3) Facilities cleanup + GEO enrichment
# -------------------------------------------------------------------------------------------------

if (!exists("fac_geo")) {
  cli_oops("fac_geo not found in memory. Load it before running.")
  stop("Missing fac_geo", call. = FALSE)
}

core_fac_cols <- c("unique_id","Segment","Technology","Subcategory","Decarb_Sector","Investment_Status",
                   "Current_Facility_Status","State","State.Name",
                   "Latitude","Longitude","Estimated_Total_Facility_CAPEX",
                   "Announcement_Date","Production_Date",
                   "county_2020_geoid","GEOID","STATEFP","NAMELSAD","CBSAFP",
                   "Company","Region","Division","ann_date","year_str","status_now",
                   "cd_119","GeoName","PEA","CBSA.Title")
miss_core <- setdiff(core_fac_cols, names(fac_geo))
if (length(miss_core)) cli_warn("fac_geo missing some expected columns: %s", paste(miss_core, collapse=", "))

facilities_clean <- fac_geo %>%
  as_tibble() %>%
  mutate(
    unique_id = if ("unique_id" %in% names(.)) unique_id else as.character(row_number()),
    Segment   = normalize_segment(Segment),
    Technology= normalize_tech(Technology),
    Decarb_Sector = str_to_title(str_squish(Decarb_Sector)),
    Investment_Status = coalesce(normalize_status(Investment_Status), normalize_status(status_now)),
    Current_Facility_Status = coalesce(normalize_status(Current_Facility_Status), normalize_status(status_now)),
    state_abbr_input = case_when(!is.na(State) & nchar(State)==2 ~ toupper(State), TRUE ~ NA_character_),
    State.Name = coalesce(State.Name, state_lookup$State.Name[match(state_abbr_input, state_lookup$state_abbr)]),
    State      = coalesce(state_abbr_input, to_state_abbr(State.Name)),
    Production_Date = coalesce(Production_Date, as.character(Announcement_Date)),
    Production_Date_parsed = parse_date_multi(Production_Date),
    ann_date = if ("ann_date" %in% names(.)) ann_date else as.Date(NA)
  ) %>%
  mutate(
    county_geoid_from_col   = if ("county_2020_geoid" %in% names(.)) padr(county_2020_geoid, 5) else NA_character_,
    county_geoid_from_GEOID = if ("GEOID" %in% names(.)) ifelse(nchar(GEOID)==5, padr(GEOID,5), NA_character_) else NA_character_,
    county_geoid = coalesce(county_geoid_from_col, county_geoid_from_GEOID),
    LatLon_Valid = is.finite(Latitude) & is.finite(Longitude) &
      !is.na(Latitude) & !is.na(Longitude) &
      Latitude >= -90 & Latitude <= 90 &
      Longitude >= -180 & Longitude <= 180 &
      !(abs(Latitude) < 1e-6 & abs(Longitude) < 1e-6)
  )

cli::cli_h1("Facilities — initial normalization")
dbg(
  facilities_clean %>%
    safe_select(c("unique_id","Segment","Technology","Decarb_Sector","Investment_Status",
                  "Current_Facility_Status","State","State.Name","Production_Date",
                  "Production_Date_parsed","Latitude","Longitude","county_geoid")) %>%
    head(20),
  "facilities_clean (key cols)"
)

# Spatial backfill county_geoid where needed
need_spatial <- is.na(facilities_clean$county_geoid) & facilities_clean$LatLon_Valid
if (any(need_spatial)) {
  cli_msg("Running point-in-polygon join for %s facilities lacking county_geoid", sum(need_spatial))
  pts <- sf::st_as_sf(facilities_clean[need_spatial,], coords = c("Longitude","Latitude"), crs = 4326, remove = FALSE)
  hit <- suppressWarnings(sf::st_join(pts, tigris_counties_2020_raw[, "GEOID"], left = TRUE))
  facilities_clean$county_geoid[need_spatial] <- padr(hit$GEOID, 5)
}

# Enrich with CBSA/PEA/CD/state/county labels
facilities_enriched <- facilities_clean %>%
  safe_left_join(cty_to_cbsa, by = c("county_geoid" = "COUNTY_GEOID")) %>%
  safe_left_join(cty_to_pea,  by = c("county_geoid" = "COUNTY_GEOID")) %>%
  safe_left_join(cty_to_cd  %>% dplyr::select(dplyr::all_of(c("COUNTY_GEOID","cd_119"))),
                 by = c("county_geoid" = "COUNTY_GEOID")) %>%
  coalesce_cols("cd_119", c("cd_119","cd_119.x","cd_119.y")) %>%
  safe_left_join(
    tigris_counties_2020_raw %>%
      sf::st_drop_geometry() %>%
      dplyr::transmute(
        GEOID_cty     = stringr::str_pad(GEOID, 5, pad = "0"),
        NAMELSAD_cty  = NAMELSAD,
        STATEFP_cty   = STATEFP
      ),
    by = c("county_geoid" = "GEOID_cty")
  ) %>%
  safe_left_join(
    tigris_states_2024_raw %>%
      sf::st_drop_geometry() %>%
      dplyr::transmute(
        STATEFP_cty   = GEOID,
        STUSPS_cty    = STUSPS,
        STATE_NAME_cty= NAME
      ),
    by = "STATEFP_cty"
  ) %>%
  dplyr::mutate(
    GeoName      = dplyr::coalesce(
      .data$GeoName,
      dplyr::if_else(!is.na(NAMELSAD_cty) & !is.na(STATE_NAME_cty),
                     paste0(NAMELSAD_cty, ", ", STATE_NAME_cty),
                     NA_character_)
    ),
    `CBSA Title` = dplyr::coalesce(CBSA_TITLE, `CBSA.Title`),
    `CBSA Code`  = dplyr::coalesce(CBSA_CODE,  `CBSA.Code`),
    PEA          = dplyr::coalesce(.data$PEA, PEA_NAME),
    State.Name   = dplyr::coalesce(.data$State.Name, STATE_NAME_cty),
    State        = dplyr::coalesce(.data$State, STUSPS_cty),
    `CBSA Title` = dplyr::if_else(is.na(`CBSA Title`) | !nzchar(`CBSA Title`),
                                  "Nonmetro (no CBSA)", `CBSA Title`)
  )

cli::cli_h1("Facilities — enriched geographies")
dbg(
  facilities_enriched %>%
    safe_select(c("unique_id","State","State.Name","GeoName","PEA","CBSA Title","cd_119","Estimated_Total_Facility_CAPEX")) %>%
    head(20),
  "facilities_enriched (geo cols sample)"
)

# Missingness by geo (sanity)
geo_req <- c("State.Name","cd_119","PEA","CBSA Title","GeoName")
miss_by_geo <- purrr::map_dfr(
  geo_req,
  ~tibble(geo = .x, missing = sum(is.na(facilities_enriched[[.x]]) | facilities_enriched[[.x]]==""))
)
cli_msg("Missingness by geo in facilities_enriched:")
print(miss_by_geo)

# Final facilities_use
facilities_use <- facilities_enriched %>%
  mutate(
    Estimated_Total_Facility_CAPEX = suppressWarnings(as.numeric(Estimated_Total_Facility_CAPEX)),
    Estimated_Total_Facility_CAPEX = replace_na(Estimated_Total_Facility_CAPEX, 0),
    Project_Type = case_when(
      !is.na(Segment) & !is.na(Technology) ~ paste(Segment, Technology, sep = " - "),
      !is.na(Technology) ~ Technology,
      !is.na(Segment) ~ Segment,
      TRUE ~ "Unknown"
    )
  ) %>%
  filter(!is.na(Technology) | Estimated_Total_Facility_CAPEX > 0) %>%
  distinct()

cli::cli_h1("Facilities — final")
dbg(
  facilities_use %>%
    safe_select(c("unique_id","Segment","Technology","Decarb_Sector","Investment_Status","Current_Facility_Status",
                  "State","State.Name","GeoName","PEA","CBSA Title","cd_119",
                  "Estimated_Total_Facility_CAPEX","Production_Date_parsed")) %>%
    head(30),
  "facilities_use (selected cols)"
)
cli_msg("Final facilities_use rows: %s", nrow(facilities_use))

# -------------------------------------------------------------------------------------------------
# 5) Credit totals
# -------------------------------------------------------------------------------------------------

tax_inv_cat_tot <- tax_inv_cat %>%
  group_by(Category) %>%
  summarise(Total_Federal_Investment = sum(`Total Federal Investment`, na.rm = TRUE), .groups = "drop")

tax_inv_cat_by_seg <- tax_inv_cat %>%
  group_by(Category, Segment) %>%
  summarise(Total_Federal_Investment = sum(`Total Federal Investment`, na.rm = TRUE), .groups = "drop")

cli::cli_h1("Federal tax credit category totals")
dbg(tax_inv_cat_tot,    "tax_inv_cat_tot (category totals)")
dbg(tax_inv_cat_by_seg, "tax_inv_cat_by_seg (purely diagnostic)")

# =================================================================================================
# FIXED GEO_CREDITS SECTION - Tax Credit Allocations   [YOUR TOP/BASE LOGIC]
# =================================================================================================

# Set IRA start date
ira_start <- as.Date("2022-08-15")

# Enhanced compute_shares with GDP weighting for CDs (and accepts character geog_col directly)
compute_shares <- function(df, geog_col, use_gdp_weight = FALSE) {
  if (!geog_col %in% names(df)) {
    cli_warn("Column '%s' not found in dataframe", geog_col)
    return(tibble(geo_name = character(), capex_sum = numeric(), n_fac = integer(), share = numeric()))
  }
  
  # For Congressional Districts, allow county splits via percent_district
  if (geog_col == "cd_119" && "percent_district" %in% names(df)) {
    grp <- df %>%
      filter(!is.na(.data[[geog_col]]), .data[[geog_col]] != "", .data[[geog_col]] != "NA-NA") %>%
      mutate(
        weighted_capex = Estimated_Total_Facility_CAPEX * (percent_district / 100)
      ) %>%
      group_by(.data[[geog_col]]) %>%
      summarise(
        capex_sum = sum(weighted_capex, na.rm = TRUE),
        n_fac = n(),
        .groups = "drop"
      ) %>%
      rename(geo_name = all_of(geog_col))
  } else {
    # Standard allocation
    grp <- df %>%
      filter(!is.na(.data[[geog_col]]), .data[[geog_col]] != "", .data[[geog_col]] != "NA-NA") %>%
      group_by(.data[[geog_col]]) %>%
      summarise(
        capex_sum = sum(Estimated_Total_Facility_CAPEX, na.rm = TRUE),
        n_fac = n(),
        .groups = "drop"
      ) %>%
      rename(geo_name = all_of(geog_col))
  }
  
  tot_capex <- sum(grp$capex_sum, na.rm = TRUE)
  
  if (tot_capex > 0) {
    grp <- grp %>% mutate(share = capex_sum / tot_capex)
  } else if (use_gdp_weight && "gdp" %in% names(df)) {
    # GDP-based fallback
    cli_warn("No facilities with CAPEX for '%s' - using GDP-based allocation", geog_col)
    grp <- df %>%
      filter(!is.na(.data[[geog_col]]), !is.na(gdp)) %>%
      group_by(.data[[geog_col]]) %>%
      summarise(
        gdp_sum = sum(gdp * if_else("percent_district" %in% names(.) & geog_col == "cd_119",
                                    percent_district / 100, 1), na.rm = TRUE),
        n_fac = 0,
        .groups = "drop"
      ) %>%
      rename(geo_name = all_of(geog_col)) %>%
      mutate(
        capex_sum = 0,
        share = gdp_sum / sum(gdp_sum, na.rm = TRUE)
      ) %>%
      select(-gdp_sum)
  } else {
    cli_warn("Total CAPEX is zero for '%s' - using equal shares", geog_col)
    tot_n <- sum(grp$n_fac)
    grp <- grp %>% mutate(share = if_else(tot_n > 0, n_fac / tot_n, 0))
  }
  
  grp
}

# Allocate a category total across a single geography (supports GDP option via compute_shares)
allocate_category_by_geo <- function(fac_df, geog_col, category_total, use_gdp_weight = FALSE) {
  shares <- compute_shares(fac_df, geog_col, use_gdp_weight)
  
  if (nrow(shares) == 0) {
    cli_warn("No facilities for geography: %s", geog_col)
    return(tibble(geo = character(), geo_name = character(), allocated = numeric()))
  }
  
  shares %>%
    mutate(
      geo = geo_label(geog_col),
      allocated = share * category_total
    ) %>%
    select(geo, geo_name, allocated)
}

# Master: allocate across ALL geos for a filtered facility subset
allocate_credit_by_all_geos <- function(filter_fn, category_total, out_name, use_gdp_weight = FALSE) {
  geos <- c("State.Name", "cd_119", "PEA", "CBSA Title", "GeoName")
  
  fac_sub <- filter_fn(facilities_use)
  
  if (nrow(fac_sub) == 0) {
    cli_warn("No facilities matched for %s", out_name)
    return(tibble(geo = character(), geo_name = character(), !!out_name := numeric()))
  }
  
  cli_msg("Matched %s facilities for %s", nrow(fac_sub), out_name)
  
  # Ensure we have GDP data if requested
  if (use_gdp_weight && !"gdp" %in% names(fac_sub)) {
    cli_warn("GDP weighting requested but 'gdp' column not found in facilities data")
    use_gdp_weight <- FALSE
  }
  
  results <- list()
  for (g in geos) {
    if (!g %in% names(fac_sub)) {
      cli_warn("Column '%s' not in facilities data", g)
      next
    }
    
    alloc <- allocate_category_by_geo(fac_sub, g, category_total, use_gdp_weight)
    if (nrow(alloc) > 0) {
      alloc <- alloc %>% rename(!!out_name := allocated)
      results[[g]] <- alloc
    }
  }
  
  bind_rows(results)
}

# Filter helper
is_operating <- function(x) {
  x_norm <- tolower(trimws(as.character(x)))
  x_norm %in% c("o", "operating", "operational", "in operation")
}

# 45X - Advanced Manufacturing
filter_45x <- function(.data) {
  .data %>%
    filter(
      Decarb_Sector %in% c("Clean Tech Manufacturing", "Clean Tech Manufacturing & Supply Chain", "Manufacturing"),
      Technology %in% c("Batteries", "Solar", "Wind", "Critical Minerals"),
      is_operating(Investment_Status) | is_operating(Current_Facility_Status),
      !is.na(State.Name), State.Name != ""
    )
}

# 45V/Q - Emerging Climate Tech
filter_45vq <- function(.data) {
  .data %>%
    filter(
      Segment %in% c("Energy and Industry", "Manufacturing"),
      Technology %in% c("Hydrogen", "Carbon Management", "Cement", "Iron & Steel", "Pulp & Paper"),
      is_operating(Investment_Status) | is_operating(Current_Facility_Status),
      !is.na(State.Name), State.Name != ""
    )
}

# 45Z/40B - Clean Fuels
filter_45z_40b <- function(.data) {
  .data %>%
    filter(
      Segment %in% c("Energy and Industry", "Manufacturing"),
      Technology %in% c("Sustainable Aviation Fuels", "Clean Fuels"),
      is_operating(Investment_Status) | is_operating(Current_Facility_Status),
      !is.na(State.Name), State.Name != ""
    )
}

# 45 - Clean Electricity
filter_45 <- function(.data) {
  .data %>%
    filter(
      Decarb_Sector %in% c("Power", "Electric Power", "Electricity"),
      (
        Technology %in% c("Solar", "Wind", "Nuclear", "Storage", "Hydroelectric", "Geothermal")
      ),
      is_operating(Investment_Status) | is_operating(Current_Facility_Status),
      !is.na(Production_Date_parsed), Production_Date_parsed > ira_start
    )
}

# Get category totals from tax_inv_cat_tot
cat_45x_total <- tax_inv_cat_tot %>%
  filter(Category == "Advanced Manufacturing Tax Credits") %>%
  pull(Total_Federal_Investment) %>%
  sum(na.rm = TRUE)

cat_45_total <- tax_inv_cat_tot %>%
  filter(Category == "Clean Electricity Tax Credits") %>%
  pull(Total_Federal_Investment) %>%
  sum(na.rm = TRUE)

cat_emerging_total <- tax_inv_cat_tot %>%
  filter(Category == "Emerging Climate Technology Tax Credits") %>%
  pull(Total_Federal_Investment) %>%
  sum(na.rm = TRUE)

# Split emerging between 45V/Q and 45Z/40B based on facility CAPEX
cap_vq <- filter_45vq(facilities_use) %>%
  summarise(s = sum(Estimated_Total_Facility_CAPEX, na.rm = TRUE)) %>%
  pull(s)

cap_z40 <- filter_45z_40b(facilities_use) %>%
  summarise(s = sum(Estimated_Total_Facility_CAPEX, na.rm = TRUE)) %>%
  pull(s)

cap_sum <- cap_vq + cap_z40
if (cap_sum == 0) {
  share_vq <- 0.5
  share_z40 <- 0.5
} else {
  share_vq <- cap_vq / cap_sum
  share_z40 <- 1 - share_vq
}

cat_45vq_total    <- cat_emerging_total * share_vq
cat_45z_40b_total <- cat_emerging_total * share_z40

cli::cli_h1("Allocating tax credits across geographies")

# Run allocations (no GDP weighting by default; set use_gdp_weight = TRUE to enable fallback)
geo_45x      <- allocate_credit_by_all_geos(filter_45x,     cat_45x_total,      "local_45x",     use_gdp_weight = FALSE)
geo_45vq     <- allocate_credit_by_all_geos(filter_45vq,    cat_45vq_total,     "local_45vq",    use_gdp_weight = FALSE)
geo_45z_40b  <- allocate_credit_by_all_geos(filter_45z_40b, cat_45z_40b_total,  "local_45z_40b", use_gdp_weight = FALSE)
geo_45       <- allocate_credit_by_all_geos(filter_45,      cat_45_total,       "local_45",      use_gdp_weight = FALSE)

# -------------------------------------------------------------------------------------------------
# 30D - Zero Emission Vehicles (state-based allocation)
# -------------------------------------------------------------------------------------------------

inv_30d_state <- investment %>%
  mutate(qtr = zoo::as.yearqtr(quarter, format = "%Y-Q%q")) %>%
  filter(
    Segment == "Retail",
    Technology == "Zero Emission Vehicles",
    qtr > zoo::as.yearqtr("2022 Q1")
  ) %>%
  group_by(State) %>%
  summarise(
    total_exp = sum(Estimated_Actual_Quarterly_Expenditure, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(cap_share = total_exp / sum(total_exp, na.rm = TRUE))

cat_30d_total <- tax_inv_cat_tot %>%
  filter(Category == "Zero Emission Vehicle Tax Credits") %>%
  pull(Total_Federal_Investment) %>%
  sum(na.rm = TRUE)

state_30d <- inv_30d_state %>%
  mutate(state_30d = cap_share * cat_30d_total) %>%
  select(State, state_30d)

# Pop-based distribution for 30D
if (!exists("pop")) stop("pop dataframe required for 30D allocation")

pop_expanded <- pop %>%
  mutate(
    geo_name = norm_name(geo_name),
    state_abbr = case_when(
      geo == "State" ~ to_state_abbr(geo_name),
      str_detect(geo_name, "^[A-Z]{2}-") ~ str_sub(geo_name, 1, 2),
      TRUE ~ extract_first_state_abbr_from_text(geo_name)
    )
  ) %>%
  filter(!is.na(state_abbr))

# Calculate state population shares for each geography
fac_30d_down <- pop_expanded %>%
  filter(geo != "State") %>%
  group_by(state_abbr) %>%
  mutate(
    state_pop = sum(pop, na.rm = TRUE),
    pop_share = if_else(state_pop > 0, pop / state_pop, 0)
  ) %>%
  ungroup() %>%
  left_join(state_30d %>% rename(state_abbr = State), by = "state_abbr") %>%
  mutate(local_30d = replace_na(state_30d * pop_share, 0)) %>%
  select(geo, geo_name, local_30d)

# Add state-level 30D
fac_30d_states <- state_30d %>%
  left_join(
    tibble(
      state_abbr = c(state.abb, "DC"),
      State.Name = c(state.name, "District of Columbia")
    ),
    by = c("State" = "state_abbr")
  ) %>%
  transmute(geo = "State", geo_name = State.Name, local_30d = state_30d) %>%
  filter(!is.na(geo_name))

fac_30d_geo <- bind_rows(fac_30d_down, fac_30d_states)

# -------------------------------------------------------------------------------------------------
# Build final geo spine
# -------------------------------------------------------------------------------------------------

if (exists("geo_long")) {
  geo_spine <- geo_long %>%
    transmute(
      geo = norm_name(geo_type),
      geo_name = norm_name(geo_name)
    ) %>%
    filter(!is.na(geo), !is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
    distinct()
} else {
  # Build from available data
  geo_spine <- bind_rows(
    facilities_use %>% transmute(geo = "State",                  geo_name = State.Name),
    facilities_use %>% transmute(geo = "Congressional District", geo_name = cd_119),
    facilities_use %>% transmute(geo = "Economic Area",          geo_name = PEA),
    facilities_use %>% transmute(geo = "Metro Area",             geo_name = `CBSA Title`),
    facilities_use %>% transmute(geo = "County",                 geo_name = GeoName)
  ) %>%
    filter(!is.na(geo_name), geo_name != "", geo_name != "NA-NA") %>%
    distinct()
}

# -------------------------------------------------------------------------------------------------
# Combine all allocations
# -------------------------------------------------------------------------------------------------

geo_credits <- geo_spine %>%
  left_join(geo_45x     %>% select(geo, geo_name, local_45x),      by = c("geo", "geo_name")) %>%
  left_join(geo_45vq    %>% select(geo, geo_name, local_45vq),     by = c("geo", "geo_name")) %>%
  left_join(geo_45z_40b %>% select(geo, geo_name, local_45z_40b),  by = c("geo", "geo_name")) %>%
  left_join(geo_45      %>% select(geo, geo_name, local_45),       by = c("geo", "geo_name")) %>%
  left_join(fac_30d_geo %>% select(geo, geo_name, local_30d),      by = c("geo", "geo_name")) %>%
  mutate(across(starts_with("local_"), ~replace_na(.x, 0))) %>%
  group_by(geo) %>%
  mutate(
    rank_45x     = rank(-local_45x,     ties.method = "min"),
    rank_45vq    = rank(-local_45vq,    ties.method = "min"),
    rank_45      = rank(-local_45,      ties.method = "min"),
    rank_30d     = rank(-local_30d,     ties.method = "min"),
    rank_45z_40b = rank(-local_45z_40b, ties.method = "min")
  ) %>%
  ungroup()

# -------------------------------------------------------------------------------------------------
# Summary diagnostics
# -------------------------------------------------------------------------------------------------

cli::cli_h1("geo_credits summary")
cli_msg("Total rows: %s", nrow(geo_credits))
cli_msg("Geographies: %s", paste(unique(geo_credits$geo), collapse = ", "))

summary_stats <- geo_credits %>%
  summarise(
    across(starts_with("local_"), 
           list(total = ~sum(.x, na.rm = TRUE),
                nonzero = ~sum(.x > 0, na.rm = TRUE)),
           .names = "{.col}_{.fn}")
  )

cli_msg("45X total allocated: $%.1fM (%.0f geos)", 
        summary_stats$local_45x_total, summary_stats$local_45x_nonzero)
cli_msg("45 total allocated: $%.1fM (%.0f geos)", 
        summary_stats$local_45_total, summary_stats$local_45_nonzero)
cli_msg("30D total allocated: $%.1fM (%.0f geos)", 
        summary_stats$local_30d_total, summary_stats$local_30d_nonzero)

# Done
cli_ok("Pipeline finished in %.2f seconds.", as.numeric(difftime(Sys.time(), script_start_time, units = "secs")))

================================================================================
# END geo_credits.R
================================================================================


================================================================================
# BEGIN politics.R
================================================================================

# ================================================
# Robust Alaska (AK) ED -> County/Borough Mapping
# and CBS County-level Output (with AK fixed)
# + PRES 2024 rollups (State / CD119 / PEA / CBSA / County)
# ================================================
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(janitor)
  library(readr)
  library(sf)
  library(tigris)
  library(tidycensus)
  library(purrr)
})

options(tigris_use_cache = TRUE)
sf::sf_use_s2(FALSE)   # Geometry ops in AK can be finicky with s2; disable for robust joins

# -------------------------
# Debug helpers
# -------------------------
log_section <- function(title) {
  cat("\n", strrep("=", 20), " ", title, " ", strrep("=", 20), "\n", sep = "")
}

safe_glimpse <- function(x, nm = deparse(substitute(x))) {
  cat("Glimpse", nm, ":\n")
  if (is.data.frame(x) || inherits(x, "tbl_df")) {
    cat("  nrow:", nrow(x), " ncol:", ncol(x), "\n  cols: ",
        paste(names(x), collapse = ", "), "\n", sep = "")
    print(utils::head(x, 3))
  } else {
    print(utils::str(x, max.level = 1))
  }
}

null_or_empty <- function(x) is.null(x) || (length(x) == 0)

rescale_to_one <- function(w) {
  s <- sum(w, na.rm = TRUE)
  if (is.na(s) || s == 0) return(w)
  w / s
}

require_cols <- function(df, cols, where = "object") {
  missing <- setdiff(cols, names(df))
  if (length(missing)) stop(sprintf("Missing required columns in %s: %s", where, paste(missing, collapse=", ")))
}

# Helper: compact count summary
log_dim <- function(df, lbl = deparse(substitute(df))) {
  cat(sprintf("Rows x Cols for %s: %s x %s\n", lbl, nrow(df), ncol(df)))
}

# -------------------------
# 1) Load / prepare CBS input
# -------------------------
log_section("1) Load CBS county/ED input")

# Use in-memory `cbs` if present; otherwise fetch from source
if (!exists("cbs")) {
  cat("`cbs` not found in memory; downloading from CBS source...\n")
  cbs <- readr::read_csv(
    "https://raw.githubusercontent.com/cbs-news-data/election-2024-maps/refs/heads/master/output/all_counties_clean_2024.csv",
    show_col_types = FALSE
  )
}
cbs <- cbs %>% mutate(fips = sprintf("%05s", as.character(fips)))
safe_glimpse(cbs, "cbs"); log_dim(cbs, "cbs")

# -------------------------
# 2) Identify AK ED rows
# -------------------------
log_section("2) Identify Alaska ED rows (02001–02040) vs normal counties")

AK_ED_FIPS <- sprintf("02%03d", 1:40)  # 02001 .. 02040

cbs_ak_ed <- cbs %>% filter(state == "AK", fips %in% AK_ED_FIPS)
cbs_non_ak_or_regular <- cbs %>% filter(!(state == "AK" & fips %in% AK_ED_FIPS))

cat("AK ED rows found:", nrow(cbs_ak_ed), "of 40 expected.\n")
missing_eds <- setdiff(AK_ED_FIPS, unique(cbs_ak_ed$fips))
if (length(missing_eds)) cat("Warning: Missing AK ED rows in `cbs` for fips:", paste(missing_eds, collapse = ", "), "\n")

# -------------------------
# 3) County reference (names/FIPS)
# -------------------------
log_section("3) Load county shapes (2020) for names/FIPS reference")

if (exists("tigris_counties_2020_raw") && inherits(tigris_counties_2020_raw, "sf")) {
  counties_2020 <- tigris_counties_2020_raw %>%
    st_drop_geometry() %>%
    select(STATEFP, COUNTYFP, GEOID, NAME, NAMELSAD)
  cat("Using in-memory `tigris_counties_2020_raw`.\n")
} else {
  cat("`tigris_counties_2020_raw` not in memory; downloading AK counties (2020)...\n")
  counties_2020 <- tigris::counties(state = "AK", year = 2020, cb = FALSE, class = "sf") %>%
    st_drop_geometry() %>%
    select(STATEFP, COUNTYFP, GEOID, NAME, NAMELSAD)
}
safe_glimpse(counties_2020, "counties_2020 (national or AK subset)")

ak_county_ref <- counties_2020 %>%
  filter(STATEFP == "02") %>%
  transmute(COUNTYFP = stringr::str_pad(COUNTYFP, 3, side = "left", pad = "0"),
            COUNTY_GEOID = GEOID,
            COUNTY_NAME  = NAMELSAD)

safe_glimpse(ak_county_ref, "ak_county_ref"); require_cols(ak_county_ref, c("COUNTYFP","COUNTY_GEOID","COUNTY_NAME"), "ak_county_ref")

# -------------------------
# 4) Alaska VTDs (precincts) → derive ED codes
# -------------------------
log_section("4) Load Alaska voting districts (VTDs) and derive ED codes")

if (exists("ak_voting_districts") && inherits(ak_voting_districts, "sf")) {
  vtd_ak <- ak_voting_districts
  cat("Using in-memory `ak_voting_districts`.\n")
} else {
  cat("Downloading `tigris::voting_districts(state='AK', year=2024)`...\n")
  vtd_ak <- tigris::voting_districts(state = "AK", year = 2024, cb = FALSE, class = "sf")
}

# Derive ED code from "01-xxx" prefix
vtd_ak <- vtd_ak %>%
  mutate(
    state_election_district = ifelse(grepl("-", VTDST20),
                                     sub("-.*", "", VTDST20),
                                     NA_character_),
    state_election_district = stringr::str_pad(state_election_district, width = 2, side = "left", pad = "0"),
    COUNTYFP = COUNTYFP20  # normalize naming
  ) %>%
  select(STATEFP20, COUNTYFP20, COUNTYFP, VTDST20, GEOID20, ALAND20, AWATER20,
         state_election_district, geometry)

n_ed_vtd <- length(unique(na.omit(vtd_ak$state_election_district)))
cat("Unique ED codes present in VTDs:", n_ed_vtd, " | Sample EDs:",
    paste(head(sort(unique(na.omit(vtd_ak$state_election_district))), 10), collapse = ", "), "\n")

# -------------------------
# 5) Build AK ED -> County crosswalk
#    Primary: POPULATION-weighted using 2020 PL blocks
#    Fallback: AREA-weighted using VTD ALAND20 (with equal-weight rescue)
# -------------------------
log_section("5) Build AK ED -> County crosswalk (pop-weighted with area fallback)")

can_use_pop <- TRUE
pop_error <- NULL
xwalk_pop <- NULL
xwalk_area_all <- NULL

# --- Attempt population-weighted crosswalk
tryCatch({
  api_present <- !is.na(Sys.getenv("CENSUS_API_KEY")) && nzchar(Sys.getenv("CENSUS_API_KEY"))
  if (!api_present) stop("No CENSUS_API_KEY set; skipping population-weighted method.")
  
  cat("Fetching 2020 PL 94-171 total population for AK census blocks via tidycensus...\n")
  blk_pop <- tidycensus::get_decennial(
    geography = "block",
    variables = "P1_001N",
    state = "AK",
    year = 2020,
    sumfile = "pl",
    geometry = TRUE,
    cache_table = TRUE
  ) %>%
    rename(BLOCK_GEOID = GEOID, pop2020 = value) %>%
    select(BLOCK_GEOID, pop2020, geometry)
  
  cat("Blocks retrieved:", nrow(blk_pop), "\n")
  # Reproject to Alaska Albers (EPSG:3338) for robust spatial join
  blk_pop  <- st_transform(blk_pop, 3338)
  vtd_slim <- vtd_ak %>% select(state_election_district, COUNTYFP = COUNTYFP20, geometry) %>% st_transform(3338)
  
  if (!all(st_is_valid(vtd_slim))) vtd_slim <- st_make_valid(vtd_slim)
  if (!all(st_is_valid(blk_pop)))  blk_pop  <- st_make_valid(blk_pop)
  
  cat("Joining blocks to VTDs (st_within)...\n")
  blk_vtd <- st_join(blk_pop, vtd_slim, join = st_within, left = FALSE)
  cat("Blocks matched to VTDs:", nrow(blk_vtd), "\n")
  
  xwalk_pop <- blk_vtd %>%
    st_drop_geometry() %>%
    mutate(COUNTYFP = stringr::str_pad(COUNTYFP, 3, side = "left", pad = "0"),
           state_election_district = stringr::str_pad(state_election_district, 2, side = "left", pad = "0")) %>%
    group_by(state_election_district, COUNTYFP) %>%
    summarise(pop = sum(pop2020, na.rm = TRUE), .groups = "drop_last") %>%
    group_by(state_election_district) %>%
    mutate(weight = rescale_to_one(pop)) %>%
    ungroup() %>%
    arrange(state_election_district, COUNTYFP)
  
  chk <- xwalk_pop %>%
    group_by(state_election_district) %>%
    summarise(wsum = sum(weight), n_cnty = n_distinct(COUNTYFP), .groups = "drop")
  cat("POP weight sum check (should be 1.0 per ED):\n"); print(utils::head(chk, 10))
  if (any(abs(chk$wsum - 1) > 1e-6)) {
    bad <- chk %>% filter(abs(wsum - 1) > 1e-6)
    cat("WARNING: POP shares not summing to 1 for EDs:\n"); print(bad)
  }
}, error = function(e) {
  can_use_pop <<- FALSE
  pop_error <<- conditionMessage(e)
  cat("Population-weighted crosswalk failed / unavailable. Reason:\n", pop_error, "\n")
})

# --- AREA-weighted (with equal-weight rescue if ALAND unavailable)
xwalk_area_all <- vtd_ak %>%
  st_drop_geometry() %>%
  mutate(COUNTYFP = stringr::str_pad(coalesce(COUNTYFP, COUNTYFP20), 3, side = "left", pad = "0"),
         state_election_district = stringr::str_pad(state_election_district, 2, side = "left", pad = "0")) %>%
  group_by(state_election_district, COUNTYFP) %>%
  summarise(ALAND = sum(ALAND20, na.rm = TRUE), .groups = "drop_last") %>%
  group_by(state_election_district) %>%
  mutate(
    .w_raw = ifelse(ALAND > 0, ALAND, NA_real_),
    weight = if (all(is.na(.w_raw))) 1 / n() else .w_raw / sum(.w_raw, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  select(state_election_district, COUNTYFP, weight) %>%
  arrange(state_election_district, COUNTYFP)

chk_area <- xwalk_area_all %>%
  group_by(state_election_district) %>%
  summarise(wsum = sum(weight), n_cnty = n_distinct(COUNTYFP), .groups = "drop")
cat("AREA weight sum check (should be 1.0 per ED):\n"); print(utils::head(chk_area, 10))
if (any(abs(chk_area$wsum - 1) > 1e-6)) {
  bad <- chk_area %>% filter(abs(wsum - 1) > 1e-6)
  cat("WARNING: AREA shares not summing to 1 for EDs (after rescue):\n"); print(bad)
}

# --- Choose base crosswalk and backfill missing EDs (hybrid)
if (can_use_pop && !is.null(xwalk_pop)) {
  base_xwalk <- xwalk_pop
  ed_in_vtd  <- sort(unique(na.omit(vtd_ak$state_election_district)))
  ed_in_pop  <- sort(unique(na.omit(xwalk_pop$state_election_district)))
  ed_missing <- setdiff(ed_in_vtd, ed_in_pop)
  if (length(ed_missing)) {
    cat("Backfilling EDs missing from POP weights (using AREA weights):", paste(ed_missing, collapse = ", "), "\n")
    area_backfill <- xwalk_area_all %>% filter(state_election_district %in% ed_missing)
    base_xwalk <- bind_rows(base_xwalk, area_backfill)
  }
  method_used <- "POPULATION-weighted (with area backfill if needed)"
} else {
  base_xwalk <- xwalk_area_all
  method_used <- "AREA-weighted (population method unavailable)"
}

# --- Attach county names/geoids from reference
ak_xwalk <- base_xwalk %>%
  left_join(ak_county_ref, by = "COUNTYFP", suffix = c("", ".ref")) %>%
  mutate(
    COUNTY_GEOID = dplyr::coalesce(COUNTY_GEOID, paste0("02", COUNTYFP)),
    COUNTY_NAME  = dplyr::coalesce(COUNTY_NAME,  paste0("FIPS 02", COUNTYFP))
  ) %>%
  select(state_election_district, COUNTYFP, COUNTY_GEOID, COUNTY_NAME, weight)

cat("\nUsing AK ED -> County crosswalk method: ", method_used, "\n", sep = "")
safe_glimpse(ak_xwalk, "ak_xwalk (final crosswalk)"); log_dim(ak_xwalk, "ak_xwalk")
require_cols(ak_xwalk, c("state_election_district","COUNTYFP","COUNTY_GEOID","COUNTY_NAME","weight"), "ak_xwalk")

# -------------------------
# 6) Prepare AK ED rows from CBS and attach ED code
# -------------------------
log_section("6) Prepare AK ED rows from CBS and attach ED code")

if (nrow(cbs_ak_ed) == 0) stop("No Alaska ED rows found in `cbs`. Cannot proceed with AK mapping.")

cbs_ak_ed <- cbs_ak_ed %>%
  mutate(
    ed_num = as.integer(sub("^02", "", fips)),             # 1..40
    ed_code = stringr::str_pad(ed_num, 2, side = "left", pad = "0")     # "01".."40"
  )

safe_glimpse(cbs_ak_ed, "cbs_ak_ed"); log_dim(cbs_ak_ed, "cbs_ak_ed")

# -------------------------
# 7) Allocate ED votes to counties using crosswalk weights
# -------------------------
log_section("7) Allocate AK ED values to county/borough level")

num_cols <- intersect(
  c("totalExpVote", "totalVote", "vote_Harris", "vote_Trump", "vote_Other"),
  names(cbs_ak_ed)
)
if (length(num_cols) == 0) stop("No numeric vote columns found to allocate in `cbs_ak_ed`.")
cat("Numeric columns allocated:", paste(num_cols, collapse = ", "), "\n")

# Join should be complete—debug any missing EDs
missing_ed_in_xwalk <- anti_join(cbs_ak_ed %>% distinct(ed_code), ak_xwalk %>% distinct(state_election_district),
                                 by = c("ed_code" = "state_election_district"))
if (nrow(missing_ed_in_xwalk)) {
  cat("WARNING: The following ED codes in `cbs_ak_ed` were not found in crosswalk:\n")
  print(missing_ed_in_xwalk)
}

ak_alloc_long <- cbs_ak_ed %>%
  inner_join(ak_xwalk, by = c("ed_code" = "state_election_district")) %>%
  mutate(across(all_of(num_cols), ~ .x * weight, .names = "{.col}_alloc"))

safe_glimpse(ak_alloc_long, "ak_alloc_long (post-join and weighted)"); log_dim(ak_alloc_long, "ak_alloc_long")
require_cols(ak_alloc_long, c("COUNTY_GEOID","COUNTY_NAME","weight"), "ak_alloc_long")
ak_county_alloc <- ak_alloc_long %>%
  group_by(COUNTY_GEOID) %>%
  summarise(
    state              = "AK",
    fips               = first(COUNTY_GEOID),
    NAME               = first(COUNTY_NAME),
    timeStamp          = suppressWarnings(max(timeStamp, na.rm = TRUE)),
    across(all_of(paste0(num_cols, "_alloc")), ~ sum(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  rename_with(~ gsub("_alloc$", "", .x), .cols = ends_with("_alloc")) %>%
  mutate(
    pctExpVote     = ifelse(!is.na(totalExpVote) & totalExpVote > 0, 100, NA_real_),
    pctExpVote_NEW = pctExpVote,
    pct_Harris     = ifelse(totalVote > 0, 100 * vote_Harris / totalVote, NA_real_),
    pct_Trump      = ifelse(totalVote > 0, 100 * vote_Trump  / totalVote, NA_real_),
    leader = case_when(
      is.na(totalVote) | totalVote == 0 ~ NA_character_,
      pmax(vote_Harris, vote_Trump, vote_Other, na.rm = TRUE) == vote_Harris ~ "Harris",
      pmax(vote_Harris, vote_Trump, vote_Other, na.rm = TRUE) == vote_Trump  ~ "Trump",
      TRUE ~ "Other"
    ),
    .pct_H = ifelse(totalVote > 0, 100 * vote_Harris / totalVote, NA_real_),
    .pct_T = ifelse(totalVote > 0, 100 * vote_Trump  / totalVote, NA_real_),
    .pct_O = ifelse(totalVote > 0, 100 * vote_Other  / totalVote, NA_real_),
    .top1 = pmax(.pct_H, .pct_T, .pct_O, na.rm = TRUE),
    .top2 = pmax(
      ifelse(.pct_H == .top1, -Inf, .pct_H),
      ifelse(.pct_T == .top1, -Inf, .pct_T),
      ifelse(.pct_O == .top1, -Inf, .pct_O),
      na.rm = TRUE
    ),
    leader_margin      = .top1 - .top2,
    leader_margin_safe = leader_margin,
    leader_margin_abs  = abs(leader_margin),
    at_least_20pct_in  = "20pctExpVoteIn",
    ak_district        = NA_character_,
    ts_pretty          = as.character(timeStamp)  # ADD THIS LINE HERE
  ) %>%
  select(
    state, totalExpVote, pctExpVote, totalVote, timeStamp,
    vote_Harris, vote_Trump, NAME, vote_Other, pctExpVote_NEW,
    pct_Harris, pct_Trump, ts_pretty,  # SIMPLIFIED: just select ts_pretty
    fips, leader, at_least_20pct_in, leader_margin, leader_margin_safe, leader_margin_abs, ak_district
  )
safe_glimpse(ak_county_alloc, "ak_county_alloc (allocated AK counties)"); log_dim(ak_county_alloc, "ak_county_alloc")

# -------------------------
# 8) Combine with the rest of CBS
# -------------------------
log_section("8) Combine AK county allocations with the rest of CBS")

# Drop any remaining 020xx EDs from the non-AK set (guard)
cbs_non_ak_or_regular <- cbs_non_ak_or_regular %>%
  filter(!(state == "AK" & stringr::str_detect(fips, "^02\\d{3}$") & !fips %in% AK_ED_FIPS))

# CRITICAL FIX: remove national '00000' row before final bind
cbs_non_ak_or_regular <- cbs_non_ak_or_regular %>% filter(fips != "00000")

cbs_county_robust <- bind_rows(
  cbs_non_ak_or_regular,
  ak_county_alloc
) %>% arrange(state, fips)

safe_glimpse(cbs_county_robust, "cbs_county_robust (final)"); log_dim(cbs_county_robust, "cbs_county_robust")

# -------------------------
# 9) Sanity checks vs your county crosswalk (if present)
# -------------------------
log_section("9) Sanity checks: FIPS coverage in COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA")

if (exists("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA")) {
  missing_fips <- setdiff(cbs_county_robust$fips, COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA$COUNTY_GEOID)
  if (length(missing_fips)) {
    cat("WARNING: The following fips in `cbs_county_robust` are missing from COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA:\n")
    print(sort(unique(missing_fips)))
    if (any(stringr::str_detect(missing_fips, "^02"))) {
      cat("Note: Check Alaska county/borough FIPS vs your crosswalk vintage (2020 vs later reorganizations).\n")
    }
  } else {
    cat("All fips in `cbs_county_robust` are found in COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA.\n")
  }
} else {
  cat("Note: `COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA` not in memory; skipping FIPS coverage check.\n")
}

# -------------------------
# 10) Consistency check: AK ED sums vs allocated AK counties
# -------------------------
log_section("10) Consistency check: sums AK ED -> AK counties")

num_cols <- c("totalExpVote", "totalVote", "vote_Harris", "vote_Trump", "vote_Other")

ed_totals <- cbs_ak_ed %>% summarise(across(all_of(num_cols), ~ sum(.x, na.rm = TRUE)))
ak_county_totals <- ak_county_alloc %>% summarise(across(all_of(num_cols), ~ sum(.x, na.rm = TRUE)))

comp <- bind_rows(
  ed_totals %>% mutate(source = "AK_ED_sum"),
  ak_county_totals %>% mutate(source = "AK_alloc_counties_sum")
) %>% relocate(source)
print(comp)

tol <- 1e-4
mismatch <- purrr::map_lgl(num_cols, function(col) {
  abs(ed_totals[[col]] - ak_county_totals[[col]]) > tol
})
if (any(mismatch)) {
  cat("WARNING: Minor mismatches between ED sums and allocated county sums (check rounding/weights):\n")
  print(num_cols[mismatch])
} else {
  cat("OK: ED sums and allocated county sums match within tolerance.\n")
}

# ====================== PREP: Quick glances at input data already in memory ======================
if (exists("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA")) { cat("Glimpse COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA:\n"); glimpse(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA) }
if (exists("geo")) { cat("Glimpse geo:\n"); glimpse(geo) }
if (exists("geo_long")) { cat("Glimpse geo_long:\n"); glimpse(geo_long) }
if (exists("tigris_counties_2020_raw")) { cat("Glimpse tigris_counties_2020_raw:\n"); glimpse(tigris_counties_2020_raw) }
if (exists("geocorr_county_2020_cd_119")) { cat("Glimpse geocorr_county_2020_cd_119: \n"); glimpse(geocorr_county_2020_cd_119) }
if (exists("geocorr_ct_county_cd_119")) { cat("Glimpse geocorr_ct_county_cd_119: \n"); glimpse(geocorr_ct_county_cd_119) }
if (exists("tigris_cbsa_2020_raw")) { cat("Glimpse tigris_cbsa_2020_raw:\n"); glimpse(tigris_cbsa_2020_raw) }
if (exists("tigris_states_2024_raw")) { cat("Glimpse tigris_states_2024_raw:\n"); glimpse(tigris_states_2024_raw) }
if (exists("tigris_congressional_districts_2024_raw")) { cat("Glimpse tigris_congressional_districts_2024_raw:\n"); glimpse(tigris_congressional_districts_2024_raw) }
if (exists("geographies")) { cat("Glimpse geographies:\n"); glimpse(geographies) }
if (exists("geographies_clean")) { cat("Glimpse geographies_clean:\n"); glimpse(geographies_clean) }

# Optionally (re)load presidential vote by Congressional district if path available
if (!exists("presvote_by_cd") && exists("paths") && !is.null(paths$raw_data)) {
  presvote_by_cd <- read_csv(file.path(paths$raw_data,"2020_2024_presvote_by_cd.csv"), skip = 1, show_col_types = FALSE)
}
if (exists("presvote_by_cd")) { cat("Glimpse presvote_by_cd:\n"); glimpse(presvote_by_cd) }

# =========================================================
# PRES 2024 aggregation to State / CD119 / PEA / CBSA / County
# Returns exactly one row per entry in `geo_long`
# =========================================================

log_section("A) Build pres2024 (aligned to geo_long)")

# ---------- 1) Choose vote base (county-level, post-AK fix if available) ----------
votes_base <- NULL
if (exists("cbs_county_robust")) {
  votes_base <- cbs_county_robust
  cat("Using `cbs_county_robust` (includes Alaska county allocations).\n")
} else if (exists("cbs")) {
  votes_base <- cbs %>% filter(!is.na(fips), fips != "00000")
  cat("`cbs_county_robust` not found; falling back to raw `cbs`.\n")
} else {
  stop("Neither `cbs_county_robust` nor `cbs` is present.")
}

# Minimal schema standardization
votes_base <- votes_base %>%
  mutate(
    fips        = sprintf("%05s", as.character(fips)),
    vote_Harris = as.numeric(vote_Harris),
    vote_Trump  = as.numeric(vote_Trump),
    vote_Other  = as.numeric(vote_Other),
    totalVote   = as.numeric(totalVote)
  ) %>%
  mutate(
    totalVote = ifelse(is.na(totalVote) & rowSums(cbind(vote_Harris, vote_Trump, vote_Other), na.rm = TRUE) > 0,
                       rowSums(cbind(vote_Harris, vote_Trump, vote_Other), na.rm = TRUE),
                       totalVote)
  )

require_cols(votes_base, c("fips","state","vote_Harris","vote_Trump","vote_Other","totalVote"), "votes_base")
safe_glimpse(votes_base, "votes_base (county-level)"); log_dim(votes_base, "votes_base")

# ---------- 2) Helper: demshare + partisan bins ----------
as_partisan <- function(df, h_col = "vote_Harris", t_col = "totalVote") {
  h <- rlang::sym(h_col); tv <- rlang::sym(t_col)
  df %>%
    mutate(
      demshare = dplyr::if_else(!!tv > 0, (!!h) / (!!tv), NA_real_),
      partisan = dplyr::case_when(
        demshare < 0.40 ~ 1,
        demshare < 0.45 ~ 2,
        demshare < 0.55 ~ 3,
        demshare < 0.65 ~ 4,
        demshare >= 0.65 ~ 5,
        TRUE ~ NA_real_
      )
    ) %>%
    mutate(
      partisan = factor(partisan, levels = 1:5,
                        labels = c("Strong Republican", "Leans Republican",
                                   "Battleground", "Leans Democratic", "Strong Democratic"))
    )
}

sum_vote_cols <- function(df, grp_vars) {
  df %>%
    group_by(across(all_of(grp_vars))) %>%
    summarise(
      vote_Harris = sum(vote_Harris, na.rm = TRUE),
      vote_Trump  = sum(vote_Trump,  na.rm = TRUE),
      vote_Other  = sum(vote_Other,  na.rm = TRUE),
      totalVote   = sum(totalVote,   na.rm = TRUE),
      .groups = "drop"
    )
}

# ---------- 3) Build each geography ----------

# --- 3a) County (straight join via fips -> GeoName)
require_cols(geo, c("fips","GeoName"), "geo (for County)")
county_lookup <- geo %>% select(fips, GeoName) %>% distinct()
county_df <- votes_base %>%
  inner_join(county_lookup, by = "fips") %>%
  sum_vote_cols("GeoName") %>%
  as_partisan() %>%
  transmute(geo = "County", geo_name = GeoName, demshare, partisan)

cat("DEBUG: county_df has", nrow(county_df), "rows\n")

# --- 3b) State (group counties to State.Name)
require_cols(geo, c("state_abbr","State.Name"), "geo (for State)")
state_lookup <- geo %>% select(state_abbr, State.Name) %>% distinct()
state_df <- votes_base %>%
  inner_join(state_lookup, by = c("state" = "state_abbr")) %>%
  sum_vote_cols("State.Name") %>%
  as_partisan() %>%
  transmute(geo = "State", geo_name = State.Name, demshare, partisan)

cat("DEBUG: state_df has", nrow(state_df), "rows\n")

# --- 3c) Metro Area (CBSA Title). Support both "CBSA Title" and "CBSA.Title".
cbsa_df <- NULL
cbsa_col <- if ("CBSA Title" %in% names(geo)) "CBSA Title" else if ("CBSA.Title" %in% names(geo)) "CBSA.Title" else NA_character_
if (!is.na(cbsa_col)) {
  cbsa_lookup <- geo %>% select(fips, CBSA_Title = all_of(cbsa_col)) %>% distinct()
  cbsa_df <- votes_base %>%
    inner_join(cbsa_lookup, by = "fips") %>%
    filter(!is.na(CBSA_Title) & CBSA_Title != "") %>%
    sum_vote_cols("CBSA_Title") %>%
    as_partisan() %>%
    transmute(geo = "Metro Area", geo_name = CBSA_Title, demshare, partisan)
  cat("CBSA rollup built using column:", cbsa_col, "\n")
  cat("DEBUG: cbsa_df has", nrow(cbsa_df), "rows\n")
} else {
  cbsa_df <- tibble(geo = character(), geo_name = character(), demshare = double(), partisan = factor())
  cat("Note: No CBSA column (`CBSA Title` or `CBSA.Title`) found on `geo`; Metro Area rollups skipped.\n")
}

# --- 3d) Economic Area (PEA). Prefer COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA; fallback to geo$PEA.
pea_df <- NULL
if (exists("COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA")) {
  require_cols(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA, c("COUNTY_GEOID","PEA_NAME"), "COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA")
  pea_lookup <- COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA %>%
    select(COUNTY_GEOID, PEA_NAME) %>%
    filter(!is.na(PEA_NAME) & PEA_NAME != "") %>%
    distinct()
  pea_df <- votes_base %>%
    inner_join(pea_lookup, by = c("fips" = "COUNTY_GEOID")) %>%
    sum_vote_cols("PEA_NAME") %>%
    as_partisan() %>%
    transmute(geo = "Economic Area", geo_name = PEA_NAME, demshare, partisan)
  cat("PEA rollup built from COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA.\n")
  cat("DEBUG: pea_df has", nrow(pea_df), "rows\n")
} else if (all(c("fips","PEA") %in% names(geo))) {
  pea_lookup <- geo %>% select(fips, PEA_NAME = PEA) %>% filter(!is.na(PEA_NAME) & PEA_NAME != "") %>% distinct()
  pea_df <- votes_base %>%
    inner_join(pea_lookup, by = "fips") %>%
    sum_vote_cols("PEA_NAME") %>%
    as_partisan() %>%
    transmute(geo = "Economic Area", geo_name = PEA_NAME, demshare, partisan)
  cat("PEA rollup built from `geo$PEA` fallback.\n")
  cat("DEBUG: pea_df has", nrow(pea_df), "rows\n")
} else {
  pea_df <- tibble(geo = character(), geo_name = character(), demshare = double(), partisan = factor())
  cat("Note: No PEA crosswalk found; Economic Area rollups skipped.\n")
}

# --- 3e) Congressional District (119th) — county→CD allocation by geocorr, with robust CD labels
cd_df <- NULL
if (exists("geocorr_county_2020_cd_119")) {
  cat("DEBUG: Processing Congressional Districts using geocorr_county_2020_cd_119\n")
  
  g <- geocorr_county_2020_cd_119 %>%
    janitor::clean_names()
  
  # Harmonize variable names
  if (!"county_geoid" %in% names(g)) g <- g %>% rename(county_geoid = `county_code`)
  if (!"cd119_geoid"  %in% names(g)) g <- g %>% rename(cd119_geoid  = `cd119_geoid`)
  
  cat("DEBUG: geocorr columns after cleaning:", paste(names(g), collapse=", "), "\n")
  
  alloc_col <- dplyr::case_when(
    "county_to_cd119_allocation_factor" %in% names(g) ~ "county_to_cd119_allocation_factor",
    "cd119_to_county_allocation_factor" %in% names(g) ~ "cd119_to_county_allocation_factor",
    TRUE ~ NA_character_
  )
  if (is.na(alloc_col)) stop("Could not find allocation factor in `geocorr_county_2020_cd_119`.")
  
  cat("DEBUG: Using allocation column:", alloc_col, "\n")
  
  g <- g %>%
    mutate(w_raw = .data[[alloc_col]]) %>%
    group_by(county_geoid) %>%
    mutate(w = rescale_to_one(as.numeric(w_raw))) %>%
    ungroup() %>%
    select(county_geoid, cd119_geoid, w) %>%
    filter(!is.na(county_geoid), !is.na(cd119_geoid), !is.na(w))
  
  cat("DEBUG: Prepared geocorr has", nrow(g), "rows\n")
  
  # CD label lookup: map CD119FP to "ST-##", with at-large → "ST-AL"
  require_cols(tigris_congressional_districts_2024_raw, c("STATEFP","CD119FP","GEOID"), "tigris_congressional_districts_2024_raw")
  require_cols(tigris_states_2024_raw, c("STATEFP","STUSPS"), "tigris_states_2024_raw")
  
  # DEBUG: Check CD119FP values before processing
  cat("DEBUG: Sample CD119FP values from tigris_congressional_districts_2024_raw:\n")
  print(head(unique(tigris_congressional_districts_2024_raw$CD119FP), 20))
  
  cd_lookup <- tigris_congressional_districts_2024_raw %>%
    sf::st_drop_geometry() %>%
    left_join(
      tigris_states_2024_raw %>% sf::st_drop_geometry() %>% select(STATEFP, STUSPS),
      by = "STATEFP"
    ) %>%
    mutate(
      cd_clean = dplyr::case_when(
        CD119FP %in% c("00","98","99") ~ "AL",
        grepl("^[0-9]+$", CD119FP) ~ stringr::str_pad(as.integer(CD119FP), width = 2, side = "left", pad = "0"),  # FIXED
        TRUE ~ "AL"  # any odd codes → at-large
      ),
      cd_119 = paste0(STUSPS, "-", cd_clean)
    ) %>%
    select(cd119_geoid = GEOID, cd_119) %>%
    distinct()
  
  cat("DEBUG: cd_lookup has", nrow(cd_lookup), "rows. Sample:\n")
  print(head(cd_lookup, 10))
  
  # Allocate county votes to CDs
  cd_alloc <- votes_base %>%
    inner_join(g, by = c("fips" = "county_geoid")) %>%
    mutate(
      vote_Harris = vote_Harris * w,
      vote_Trump  = vote_Trump  * w,
      vote_Other  = vote_Other  * w,
      totalVote   = totalVote   * w
    ) %>%
    sum_vote_cols("cd119_geoid") %>%
    inner_join(cd_lookup, by = "cd119_geoid") %>%
    as_partisan() %>%
    transmute(geo = "Congressional District", geo_name = cd_119, demshare, partisan)
  
  cd_df <- cd_alloc
  cat("DEBUG: cd_df has", nrow(cd_df), "rows\n")
  
} else {
  if (all(c("fips","cd_119","percent_district") %in% names(geo))) {
    cat("DEBUG: Using fallback CD calculation from geo dataframe\n")
    cd_alloc <- votes_base %>%
      inner_join(geo %>% select(fips, cd_119, percent_district), by = "fips") %>%
      mutate(w = as.numeric(percent_district) / 100) %>%
      group_by(cd_119) %>%
      summarise(
        vote_Harris = sum(vote_Harris * w, na.rm = TRUE),
        vote_Trump  = sum(vote_Trump  * w, na.rm = TRUE),
        vote_Other  = sum(vote_Other  * w, na.rm = TRUE),
        totalVote   = sum(totalVote   * w, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      as_partisan() %>%
      transmute(geo = "Congressional District", geo_name = cd_119, demshare, partisan)
    cd_df <- cd_alloc
    cat("DEBUG: cd_df (fallback) has", nrow(cd_df), "rows\n")
  } else {
    cd_df <- tibble(geo = character(), geo_name = character(), demshare = double(), partisan = factor())
    cat("Note: No CD crosswalk found; Congressional District rollups skipped.\n")
  }
}

# ---------- 4) Optional override for CD using `presvote_by_cd` ----------
# Robust to "2024" column being named '2024' (raw) or 'x2024' (after clean_names),
# and Trump/Total columns named '...5','...6' or 'x5','x6'.
extract_presvote_cd <- function(df_raw) {
  df1 <- df_raw
  nm1 <- names(df1)
  
  cat("DEBUG: Column names in presvote_by_cd:", paste(nm1, collapse=", "), "\n")
  
  # Identify key columns under either naming scheme
  col_district <- dplyr::case_when(
    "District" %in% nm1 ~ "District",
    "district" %in% nm1 ~ "district",
    TRUE ~ nm1[grepl("^district$", nm1, ignore.case = TRUE)][1]
  )
  if (is.na(col_district) || is.na(nchar(col_district))) stop("Could not find 'District' column in presvote_by_cd.")
  
  # Harris (2024)
  col_harris <- dplyr::case_when(
    "2024" %in% nm1 ~ "2024",
    "x2024" %in% nm1 ~ "x2024",
    TRUE ~ nm1[grepl("(^|_)2024$", nm1)][1]
  )
  # Trump (2024)
  col_trump <- dplyr::case_when(
    "...5" %in% nm1 ~ "...5",
    "x5" %in% nm1 ~ "x5",
    TRUE ~ nm1[grepl("(^|_)5$", nm1)][1]
  )
  # Total (2024)
  col_total <- dplyr::case_when(
    "...6" %in% nm1 ~ "...6",
    "x6" %in% nm1 ~ "x6",
    TRUE ~ nm1[grepl("(^|_)6$", nm1)][1]
  )
  
  cat("DEBUG: Detected columns - District:", col_district, ", Harris:", col_harris, ", Trump:", col_trump, ", Total:", col_total, "\n")
  
  need <- c(col_district, col_harris, col_trump, col_total)
  if (any(is.na(need))) stop("Could not robustly detect 2024 Harris/Trump/Total columns in presvote_by_cd.")
  
  out <- df1 %>%
    # keep as-is (do NOT clean names here; we already detected robustly)
    filter(!is.na(.data[[col_district]])) %>%
    transmute(
      geo = "Congressional District",
      geo_name = stringr::str_trim(.data[[col_district]]),
      vote_Harris = readr::parse_number(.data[[col_harris]]),
      vote_Trump  = readr::parse_number(.data[[col_trump]]),
      totalVote   = readr::parse_number(.data[[col_total]])
    ) %>%
    mutate(
      vote_Other = pmax(0, totalVote - (vote_Harris + vote_Trump))
    ) %>%
    # Only well-formed districts like "AL-01" or "AK-AL"
    filter(grepl("^[A-Z]{2}-(AL|\\d{2})$", geo_name)) %>%
    as_partisan() %>%
    select(geo, geo_name, demshare, partisan)
  
  cat("DEBUG: Extracted", nrow(out), "rows from presvote_by_cd\n")
  out
}

if (exists("presvote_by_cd")) {
  cat("Found `presvote_by_cd`; attempting to clean and override CD results...\n")
  cd_direct <- NULL
  try({
    cd_direct <- extract_presvote_cd(presvote_by_cd)
  }, silent = FALSE)  # Changed to FALSE to see any error messages
  if (!is.null(cd_direct) && nrow(cd_direct)) {
    cd_df <- cd_direct
    cat("CD results overridden by `presvote_by_cd` (2024). Rows:", nrow(cd_df), "\n")
  } else {
    cat("`presvote_by_cd` present but could not be parsed into rows; keeping allocation-based CD.\n")
  }
}

# ---------- 5) Combine and align to geo_long ----------
results_list <- list(
  county_df,
  state_df,
  cd_df,
  pea_df,
  cbsa_df
)

cat("DEBUG: Combining results lists:\n")
for(i in 1:length(results_list)) {
  if(!is.null(results_list[[i]])) {
    cat("  List", i, "has", nrow(results_list[[i]]), "rows\n")
  } else {
    cat("  List", i, "is NULL\n")
  }
}

pres2024_raw <- bind_rows(results_list)
cat("DEBUG: pres2024_raw has", nrow(pres2024_raw), "rows total\n")

require_cols(geo_long, c("geo_type","geo_name"), "geo_long")
pres2024 <- geo_long %>%
  rename(geo = geo_type) %>%
  left_join(pres2024_raw, by = c("geo","geo_name")) %>%
  select(geo, geo_name, demshare, partisan)

# ---------- 6) Sanity: exact rowcount + quick coverage stats ----------
cat("geo_long rows:", nrow(geo_long), " | pres2024 rows:", nrow(pres2024), "\n")
if (nrow(pres2024) != nrow(geo_long)) {
  warning("Rowcount mismatch after join to geo_long.")
}

# Coverage by geo type
cov <- pres2024 %>%
  group_by(geo) %>%
  summarise(
    n = n(),
    n_filled = sum(!is.na(demshare)),
    coverage_pct = round(100 * n_filled / n, 2),
    .groups = "drop"
  )
cat("\nCoverage summary by geography type:\n")
print(cov)

# Check for duplicates in geo_long
dup_check <- geo_long %>%
  group_by(geo_type, geo_name) %>%
  summarise(n = n(), .groups = "drop") %>%
  filter(n > 1)
if(nrow(dup_check) > 0) {
  cat("\nWARNING: Duplicates found in geo_long:\n")
  print(dup_check)
}

# Peek a few
safe_glimpse(pres2024, "pres2024 (final, aligned to geo_long)")

================================================================================
# END politics.R
================================================================================


================================================================================
# BEGIN state_variables.R
================================================================================

# ======================================================================
# state_variables.R  — consolidated, defensive, ~350+ lines with debugging
# - Fixes pres2024 filter scoping bug (uses .data[["geo"]] to avoid env clash)
# - Adds robust, instrumented left_join_dbg() with state coverage checks
# - Extra diagnostics at each major step
# ======================================================================

# ---- 0) Libraries & global options -----------------------------------
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(readr)
  library(readxl)
  library(stringr)
  library(purrr)
  library(lubridate)
  library(httr)
  library(rvest)
  library(xml2)
  library(fs)
  library(furrr)
  library(parallel)
  library(tibble)
})

options(
  dplyr.summarise.inform = FALSE,
  readr.show_col_types   = FALSE
)

# ---- 1) Small helpers -------------------------------------------------
dbg <- function(x, title = deparse(substitute(x)), n = 5) {
  cat(sprintf("\n==== %s ====\n", title))
  if (inherits(x, "try-error")) {
    cat("Try-error object\n"); return(invisible(x))
  }
  if (!is.data.frame(x)) {
    print(x); return(invisible(x))
  }
  cat(sprintf("Rows: %s  Cols: %s \n", nrow(x), ncol(x)))
  print(head(x, n))
  invisible(x)
}

fix_df <- function(df) {
  # Keep original names (to preserve BEA "YYYY:Qn") and just coerce to tibble
  if (is.null(df)) return(tibble())
  as_tibble(df)
}

safe_pct <- function(new, old) ifelse(is.na(old) | old == 0, NA_real_, (new / old - 1) * 100)
safe_div <- function(num, den) ifelse(is.na(den) | den == 0, NA_real_, num / den)

money_to_num <- function(x) as.numeric(gsub("[^0-9\\.\\-]", "", x %||% NA_character_))

`%||%` <- function(x, y) if (is.null(x)) y else x

first_existing_col <- function(df, candidates) {
  hit <- intersect(candidates, names(df))
  if (length(hit) == 0) stop("None of the candidate columns exist: ", paste(candidates, collapse = ", "))
  hit[1L]
}

coerce_num_na_bad <- function(df) {
  df %>% mutate(across(where(is.numeric), ~ ifelse(is.nan(.x) | is.infinite(.x), NA_real_, .x)))
}

log_step <- function(msg) cat(sprintf("\n--- %s @ %s ---\n", msg, format(Sys.time(), "%H:%M:%S")))

# Instrumented left join with diagnostics on coverage (by State if present)
left_join_dbg <- function(x, y, by, name = deparse(substitute(y))) {
  cat(sprintf("\n>>> LEFT JOIN: %s  by = %s\n", name, paste(by, collapse = ", ")))
  cat(sprintf("    LHS dim: %d x %d | RHS dim: %d x %d\n", nrow(x), ncol(x), nrow(y), ncol(y)))
  # Coverage check if joining by State
  if (all(by == "State") && "State" %in% names(x) && "State" %in% names(y)) {
    lhs_states <- x %>% distinct(State) %>% arrange(State)
    rhs_states <- y %>% distinct(State) %>% arrange(State)
    missing_in_rhs <- anti_join(lhs_states, rhs_states, by = "State")
    if (nrow(missing_in_rhs)) {
      cat("    WARNING: States in LHS with no match in RHS:", paste(missing_in_rhs$State, collapse = ", "), "\n")
    } else {
      cat("    Coverage: RHS covers all LHS states by 'State'.\n")
    }
  }
  out <- suppressMessages(left_join(x, y, by = by))
  cat(sprintf("    OUT dim: %d x %d (after join: %s)\n", nrow(out), ncol(out), name))
  out
}

# ---- 2) Paths & prerequisites ----------------------------------------
log_step("Paths / prerequisites")

# Expect user to have these in environment; provide gentle fallbacks
if (!exists("paths")) {
  paths <- list(
    raw_data    = "data/raw",
    states_data = "data/states"
  )
  message("`paths` not found; using default ./data/{raw,states}")
}

if (!exists("DATA_DIR")) {
  DATA_DIR <- "."
  message("`DATA_DIR` not found; using '.'")
}

# census_divisions and CAGDP2_LONG_STATE are used downstream.
# Fail early if they are missing (these are core joins).
if (!exists("census_divisions")) {
  stop("`census_divisions` not found in environment. It must include State.Name, state_abbr, Region, Division.")
}
if (!all(c("State.Name", "state_abbr", "Region", "Division") %in% names(census_divisions))) {
  stop("`census_divisions` is missing required columns: State.Name, state_abbr, Region, Division.")
}

if (!exists("CAGDP2_LONG_STATE")) {
  stop("`CAGDP2_LONG_STATE` (BEA annual GDP long table) not found in environment.")
}

# Build a clean state key
state_key <- census_divisions %>%
  transmute(
    State      = State.Name,
    State.Code = state_abbr,
    Region, Division
  ) %>% distinct() %>%
  filter(!is.na(State) & State != "United States")
stopifnot(all(c("State","State.Code") %in% names(state_key)))

dbg(state_key, "state_key")

# ---- 3) EPS (Electric Power Sector scenarios) -------------------------
log_step("EPS read & build indices")

eps_path <- file.path(paths$raw_data, "eps.csv")
if (!file.exists(eps_path)) stop("Missing file: ", eps_path)
eps_raw <- read_csv(eps_path) %>%
  rename(rowid = `...1`)
dbg(eps_raw, "eps_raw (patched names)")

# Your original "electricity generation by type" subset — index to 2021
elec_keep <- c("onshore wind", "solar PV", "hard coal",
               "natural gas combined cycle", "natural gas peaker")

eps_elec <- eps_raw %>%
  filter(
    scenario == "BAU",
    var1 == "Electricity Generation by Type",
    var2 %in% elec_keep
  ) %>%
  group_by(Division, State, var2) %>%
  mutate(
    ref_2021 = sum(Value[Year == 2021], na.rm = TRUE),
    ref_2021 = if_else(is.na(ref_2021) | ref_2021 <= 1e-12, NA_real_, ref_2021),
    index    = round(100 * Value / ref_2021, 1)
  ) %>%
  ungroup() %>%
  select(Division, State, var2, Year, index)

stopifnot(!any(is.infinite(eps_elec$index), na.rm = TRUE))
dbg(eps_elec, "eps_elec (2021=100)")

# Also compute Top‑7 var2 types by total gen (BAU)
eps_top7_types <- eps_raw %>%
  filter(scenario == "BAU", var1 == "Electricity Generation by Type", !is.na(var2)) %>%
  group_by(var2) %>% summarise(total_gen = sum(Value, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(total_gen)) %>% slice_head(n = 7) %>% pull(var2)
cat("\nTop-7 EPS types (by total gen): ", paste(eps_top7_types, collapse = " | "), "\n")

eps_elec_top7 <- eps_raw %>%
  filter(scenario == "BAU", var1 == "Electricity Generation by Type", var2 %in% eps_top7_types) %>%
  group_by(State, var2) %>%
  mutate(
    base_2021 = Value[Year == 2021][1],
    index     = ifelse(is.na(base_2021) | base_2021 == 0, NA_real_, round(100 * Value / base_2021, 1))
  ) %>%
  ungroup() %>%
  select(Division, State, var2, Year, index)

eps_elec_latest <- eps_elec_top7 %>%
  filter(Year == max(Year, na.rm = TRUE)) %>%
  select(State, var2, index) %>%
  pivot_wider(names_from = var2, values_from = index, names_prefix = "eps_idx_") %>%
  coerce_num_na_bad()

dbg(eps_elec_latest, "eps_elec_latest (wide, Top7)")

# ---- 4) BEA Quarterly GDP (SQGDP) ------------------------------------
log_step("BEA SQGDP download -> latest quarter growths")

sqgdp_zip <- tempfile(fileext = ".zip")
download.file("https://apps.bea.gov/regional/zip/SQGDP.zip", sqgdp_zip, mode = "wb", quiet = TRUE)
sq_list <- try(unzip(sqgdp_zip, list = TRUE), silent = TRUE)
sqf <- if (!inherits(sq_list, "try-error")) {
  sq_list$Name[grepl("^SQGDP1__ALL_AREAS_\\d{4}_\\d{4}\\.csv$", sq_list$Name)][1]
} else NA_character_
tmp_sg <- tempdir()
if (!is.na(sqf)) unzip(sqgdp_zip, files = sqf, exdir = tmp_sg)
sqgdp1_raw <- if (!is.na(sqf)) read.csv(file.path(tmp_sg, sqf), check.names = FALSE) else tibble()
sqgdp1_raw <- fix_df(sqgdp1_raw); dbg(sqgdp1_raw, "sqgdp1_raw")

# Dynamic 1‑yr & 5‑yr growth vs same quarter (latest YYYY:Qn)
state_gdp_quarterly_wide <- {
  df <- sqgdp1_raw %>%
    filter(LineCode == 1, GeoName != "United States") %>%
    filter(!is.na(GeoName), GeoName != "") %>%
    mutate(GeoFIPS = str_trim(GeoFIPS)) %>%
    filter(!startsWith(GeoFIPS, "9"))
  
  qcols <- grep("^\\d{4}:Q[1-4]$", names(df), value = TRUE)
  stopifnot(length(qcols) > 0)
  latest_q  <- max(qcols)
  latest_y  <- as.integer(substr(latest_q, 1, 4))
  latest_qn <- as.integer(sub(".*:Q", "", latest_q))
  q_1yr <- sprintf("%d:Q%d", latest_y - 1, latest_qn)
  q_5yr <- sprintf("%d:Q%d", latest_y - 5, latest_qn)
  has_1yr <- q_1yr %in% names(df); has_5yr <- q_5yr %in% names(df)
  
  df %>%
    mutate(
      latest_val     = .data[[latest_q]],
      one_yr_val     = if (has_1yr) .data[[q_1yr]] else NA_real_,
      five_yr_val    = if (has_5yr) .data[[q_5yr]] else NA_real_,
      state_gdp_1_yr = round((latest_val / dplyr::na_if(one_yr_val, 0)  - 1) * 100, 1),
      state_gdp_5_yr = round((latest_val / dplyr::na_if(five_yr_val, 0) - 1) * 100, 1)
    ) %>%
    select(
      GeoFIPS, GeoName, Region, TableName, LineCode, IndustryClassification,
      Description, Unit,
      state_gdp_1_yr, state_gdp_5_yr,
      all_of(latest_q), any_of(c(q_1yr, q_5yr)), everything()
    ) %>%
    rename(latest_quarter = all_of(latest_q))
}
dbg(state_gdp_quarterly_wide, "state_gdp_quarterly_wide")

state_gdp_quarterly <- state_gdp_quarterly_wide %>%
  transmute(GeoFIPS, State = GeoName, latest_quarter, state_gdp_1_yr, state_gdp_5_yr)
dbg(state_gdp_quarterly, "state_gdp_quarterly")

# ---- 5) XChange climate policy index ---------------------------------
log_step("XChange climate policy index (CSV)")

xchange_pol_index <- read_csv(
  fs::path(DATA_DIR, "US Maps etc", "Policy", "xchange_climate_policy_index.csv")
) %>%
  select(-any_of(c("...1", "X1"))) %>%
  rename(State.Code = abbr)
dbg(xchange_pol_index, "xchange_pol_index")

# ---- 6) EIA state emissions (levels + 17->22 change) -----------------
log_step("EIA state emissions")

eia_state_ems_tmp <- tempfile(fileext = ".xlsx")
GET("https://www.eia.gov/environment/emissions/state/excel/table1.xlsx",
    write_disk(eia_state_ems_tmp, overwrite = TRUE))
eia_state_emissions_raw <- tryCatch(
  readxl::read_excel(eia_state_ems_tmp, sheet = 1, skip = 4),
  error = function(e) tibble()
) %>% fix_df()
dbg(eia_state_emissions_raw, "eia_state_emissions_raw")

state_ems <- eia_state_emissions_raw %>%
  mutate(state_ems_change_1722 = round((`2022` - `2017`) / `2017` * 100, 3)) %>%
  select(State, emissions_2022 = `2022`, state_ems_change_1722)
dbg(state_ems, "state_ems")

# ---- 7) Corporate tax (state) ----------------------------------------
log_step("Corporate tax rates (state)")

corp_tax_path <- file.path(paths$raw_data, "2024 State Corporate Income Tax Rates Brackets.csv")
if (!file.exists(corp_tax_path)) stop("Missing file: ", corp_tax_path)
corp_tax_raw <- read_csv(corp_tax_path)
dbg(corp_tax_raw, "corp_tax_raw")

corporate_tax <- corp_tax_raw %>%
  mutate(
    State = State %>% gsub("\\([A-Za-z]\\)", "", .) %>% str_trim(),
    rate_num = suppressWarnings(readr::parse_number(Rates))  # numeric %
  ) %>%
  group_by(State) %>%
  summarise(
    corporate_tax = {
      mx <- suppressWarnings(max(rate_num, na.rm = TRUE))
      if (is.infinite(mx)) 0 else mx
    },
    .groups = "drop"
  )
dbg(corporate_tax, "corporate_tax")

# ---- 8) EIA Natural Gas — industrial price (avg 2024) ----------------
log_step("EIA NG industrial price")

eia_ng_tmp <- tempfile(fileext = ".xls")
download.file("https://www.eia.gov/dnav/ng/xls/NG_PRI_SUM_A_EPG0_PIN_DMCF_M.xls",
              eia_ng_tmp, mode = "wb", quiet = TRUE)
eia_ng_industrial_price_raw <- tryCatch(
  readxl::read_excel(eia_ng_tmp, sheet = 2, skip = 2),
  error = function(e) tibble()
) %>% fix_df()
dbg(eia_ng_industrial_price_raw, "eia_ng_industrial_price_raw")

clean_colnames <- function(nms) nms %>%
  str_remove_all(" Natural Gas Industrial Price \\(Dollars per Thousand Cubic Feet\\)") %>%
  str_trim()

colnames(eia_ng_industrial_price_raw) <- c("Date", clean_colnames(colnames(eia_ng_industrial_price_raw)[-1]))

eia_gas <- eia_ng_industrial_price_raw %>%
  pivot_longer(cols = -Date, names_to = "state", values_to = "dollars_mcf") %>%
  left_join(census_divisions, by = c("state" = "State.Name"))

eia_gas_2024 <- eia_gas %>%
  mutate(year = year(Date)) %>%
  group_by(state, year) %>%
  summarise(gas_price = mean(dollars_mcf, na.rm = TRUE), .groups = "drop") %>%
  filter(year == 2024) %>%
  select(-year) %>%
  filter(state != "United States") %>%
  rename(State = state, gas_price_mcf_2024 = gas_price) %>%
  coerce_num_na_bad()
dbg(eia_gas_2024, "eia_gas_2024")

# ---- 9) EIA 861M — Industrial electricity price ----------------------
log_step("EIA 861M industrial price (avg by year)")

eia861m_path <- file.path(paths$states_data, "eia_sales.xlsx")
invisible(httr::RETRY("GET",
                      "https://www.eia.gov/electricity/data/eia861m/xls/sales_revenue.xlsx",
                      write_disk(eia861m_path, overwrite = TRUE), times = 3
))
eia861m_sales_raw <- tryCatch(
  readxl::read_excel(eia861m_path, sheet = 1, skip = 2),
  error = function(e) tibble()
) %>% fix_df()
dbg(eia861m_sales_raw, "eia861m_sales_raw")

# Pick the 4th "Cents/kWh" column (Industrial)
ck_cols <- grep("^Cents/kWh", names(eia861m_sales_raw), value = TRUE)
stopifnot(length(ck_cols) >= 4)
ind_ck_col <- ck_cols[4]
cat("Using column for Industrial Cents/kWh: ", ind_ck_col, "\n")

ind_price_m <- eia861m_sales_raw %>%
  transmute(State, Year, Month, ind_price_m = .data[[ind_ck_col]])
dbg(ind_price_m, "ind_price_m")

ind_price <- ind_price_m %>%
  group_by(State, Year) %>%
  summarise(ind_price = mean(ind_price_m, na.rm = TRUE), .groups = "drop") %>%
  filter(Year %in% c(2014L, 2019L, 2024L)) %>%
  pivot_wider(names_from = Year, values_from = ind_price) %>%
  mutate(
    ind_price_10yr      = safe_pct(`2024`, `2014`),
    ind_price_5yr       = safe_pct(`2024`, `2019`),
    ind_price_cents_kwh = `2024`
  ) %>%
  select(State, ind_price_10yr, ind_price_5yr, ind_price_cents_kwh) %>%
  coerce_num_na_bad()
dbg(ind_price, "ind_price")

# ---- 10) CNBC Top States — percentiles -------------------------------
log_step("CNBC Top States (scrape -> percentiles)")

rank_to_percentile <- function(rank, n = 50) round(((n - as.numeric(rank) + 1) / n) * 100, 1)

cnbc_url <- "https://www.cnbc.com/2025/07/10/top-states-for-business-americas-2025-the-full-rankings.html"
cnbc_page <- tryCatch(read_html(cnbc_url), error = function(e) NULL)
states_business_percentiles <- if (!is.null(cnbc_page)) {
  tbls <- html_table(cnbc_page)
  tbl  <- tbls[[1]]
  tbl$STATE <- str_extract(tbl$STATE, "[A-Za-z ]+") %>% trimws()
  map_names <- c(
    "OVERALL" = "Overall Percentile", "STATE" = "State",
    "ECONOMY" = "Economy", "INFRA-\nSTRUCTURE" = "Infrastructure",
    "WORKFORCE" = "Workforce", "COST OF \nDOING\nBUSINESS" = "Cost of Doing Business",
    "BUSINESS\nFRIENDLINESS" = "Business Friendliness",
    "QUALITY\nOF LIFE" = "Quality of Life",
    "TECHNOLOGY \n&\nINNOVATION" = "Technology & Innovation",
    "EDUCATION" = "Education", "ACCESS \nTO\nCAPITAL" = "Access to Capital",
    "COST \nOF\nLIVING" = "Cost of Living"
  )
  names(tbl) <- map_names[names(tbl)]
  pct <- tbl
  ranking_cols <- setdiff(names(pct), "State")
  pct[ranking_cols] <- lapply(pct[ranking_cols], rank_to_percentile)
  pct
} else {
  tibble(State = character(), `Overall Percentile` = numeric())
}
dbg(states_business_percentiles, "states_business_percentiles")

cnbc_rankings <- states_business_percentiles %>%
  transmute(
    State,
    cnbc_overall_pct = `Overall Percentile`,
    cnbc_economy_pct = Economy,
    cnbc_infra_pct   = Infrastructure
  )
dbg(cnbc_rankings, "cnbc_rankings")

# ---- 11) Good Jobs First (GJF) ---------------------------------------
log_step("Good Jobs First – combine & clean")

read_and_clean_gjf <- function(path) {
  readr::read_csv(path) %>%
    mutate(across(everything(), as.character)) %>%
    mutate(Year = suppressWarnings(as.integer(Year))) %>%
    filter(!is.na(Year) & Year >= 2019)
}

gjf_folder            <- file.path(paths$raw_data, "Good Jobs First")
gjf_march_path        <- file.path(gjf_folder, "gjf_complete.csv")
gjf_july_2025_folder  <- file.path(gjf_folder, "Good Jobs First Downloads 22 July 2025")

gjf_march_2025 <- if (file.exists(gjf_march_path)) read_and_clean_gjf(gjf_march_path) else tibble()
dbg(gjf_march_2025, "gjf_march_2025")

july_files <- if (dir_exists(gjf_july_2025_folder)) list.files(gjf_july_2025_folder, pattern = "\\.csv$", full.names = TRUE) else character()
future::plan(future::multisession, workers = max(1, parallel::detectCores(logical = TRUE) - 1))
on.exit(future::plan(future::sequential), add = TRUE)
gjf_july_2025 <- if (length(july_files)) future_map_dfr(july_files, read_and_clean_gjf, .progress = TRUE) else tibble()
dbg(gjf_july_2025, "gjf_july_2025")

gjf_combined <- bind_rows(gjf_march_2025, gjf_july_2025) %>% distinct()
dbg(gjf_combined, "gjf_combined")

# Normalize 'state_name' and 'subs_m' (millions)
state_col <- first_existing_col(
  gjf_combined,
  c("Location", "State in Which Facility Is Located", "State.in.Which.Facility.Is.Located")
)

gjf_unified <- gjf_combined %>%
  mutate(
    state_name   = .data[[state_col]] %>% str_trim(),
    subs_m_input = coalesce(
      suppressWarnings(as.numeric(.data$subs_m)),                       # already in millions in some sets
      money_to_num(.data$Subsidy.Value.Adjusted.For.Megadeal),
      money_to_num(.data$`Subsidy Value Adjusted For Megadeal`),
      money_to_num(.data$Subsidy.Value),
      money_to_num(.data$`Subsidy Value`),
      money_to_num(.data$Megadeal.Subsidy.Value.in.2023.Dollars),
      money_to_num(.data$`Megadeal Subsidy Value in 2023 Dollars`)
    ),
    subs_m = ifelse(is.na(subs_m_input), NA_real_,
                    ifelse(subs_m_input > 1e3, subs_m_input / 1e6, subs_m_input))
  ) %>%
  filter(!is.na(state_name) & state_name != "", !is.na(Year) & Year >= 2020)
dbg(gjf_unified, "gjf_unified (state & subs_m)")

# Annual GDP 2022 (millions) from your long table
state_gdp_2022_m <- CAGDP2_LONG_STATE %>%
  filter((LineCode %in% c(1, "1")), Year == 2022) %>%
  transmute(State = GeoName, gdp_2022_m = as.numeric(Value) / 1e3) %>%
  distinct(State, .keep_all = TRUE)
dbg(state_gdp_2022_m, "state_gdp_2022_m")

gjf_statetotal_2020_24 <- gjf_unified %>%
  group_by(state_name) %>%
  summarise(
    subs_m = sum(subs_m, na.rm = TRUE),
    deals  = sum(!is.na(subs_m)),
    .groups = "drop"
  ) %>%
  rename(State = state_name) %>%
  left_join(state_gdp_2022_m, by = "State") %>%
  mutate(
    incent_gdp      = (subs_m / gdp_2022_m) * 100,
    incent_gdp      = ifelse(is.na(gdp_2022_m) | gdp_2022_m == 0, NA_real_, incent_gdp),
    incent_gdp_rank = ifelse(is.na(incent_gdp), NA_real_, rank(-incent_gdp, ties.method = "min"))
  ) %>%
  coerce_num_na_bad()
dbg(gjf_statetotal_2020_24, "gjf_statetotal_2020_24")

================================================================================
# END state_variables.R
================================================================================


================================================================================
# APPENDED END SECTION (GLANCES + DATA FRAME CHECK)
================================================================================


# ====================== PREP: Quick glances at input data already in memory ======================
cat("Glimpse COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA:\n"); glimpse(COUNTY_CROSSWALK_SUPPLEMENT_GDP_PEA)
cat("Glimpse geo:\n"); glimpse(geo)
cat("Glimpse geo_long:\n"); glimpse(geo_long)
cat("Glimpse tigris_counties_2020_raw:\n"); glimpse(tigris_counties_2020_raw)
cat("Glimpse geocorr_county_2020_cd_119: \n"); glimpse(geocorr_county_2020_cd_119)
cat("Glimpse geocorr_ct_county_cd_119: \n"); glimpse(geocorr_ct_county_cd_119)
cat("Glimpse tigris_cbsa_2020_raw\n"); glimpse(tigris_cbsa_2020_raw)
cat("Glimpse tigris_states_2024_raw:\n"); glimpse(tigris_states_2024_raw)
cat("Glimpse tigris_congressional_districts_2024_raw\n"); glimpse(tigris_congressional_districts_2024_raw)
cat("Glimpse geographies\n"); glimpse(geographies)
cat("Glimpse geographies_clean\n"); glimpse(geographies_clean)
cat("Glimpse ct_fips_changes_raw\n"); glimpse(ct_fips_changes_raw)

# Content data frames
data_frames_all <- c(
  "rengen", "facilities_all", "geo_credits", "fed_inv_geo", 
  "elec_grid", "supplycurve_geo", "tech_pot_geo", "county_gdp_ind", 
  "county_gdp_ind_final", "manpay_geo", "manshare", "vit", 
  "pop", "prop", "life_geo", "pres2024", "innovation_geo",
  "gjf_statetotal_2020_24", "ind_price", "cnbc_rankings", 
  "eia_gas_2024", "state_ems", "xchange_pol_index", "state_gdp_quarterly",
  "feas_strategic", "county_eci"
)

# Loop through once
for (df_name in data_frames_all) {
  if (exists(df_name, envir = .GlobalEnv)) {
    cat(paste0("✅ ", df_name, " exists\n"))
    glimpse(get(df_name, envir = .GlobalEnv))
  } else {
    cat(paste0("❌ ", df_name, " does NOT exist\n"))
  }
}
