# ============================================================
# In-Queue vs. Operational Capacity (%), by State & by Region
# Source A: LBNL "Queued Up 2025" (Active = In-Queue)
# Source B: EIA-860/EIA-860M "Operating" (denominator)
# Geography: Contiguous U.S. (48 states; excludes AK & HI)
# Output: queued_pct_by_state, queued_pct_by_region
# Debugging: extensive messages, checks, and summaries
# ============================================================

# --- Libraries ---------------------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(readr)
  library(readxl)
  library(stringr)
  library(sf)
  library(tigris)
})

options(tigris_use_cache = TRUE)
options(dplyr.summarise.inform = FALSE)

# --- Helper utilities --------------------------------------------------------

dbg <- function(...) {
  msg <- paste0("[DEBUG ", format(Sys.time(), "%H:%M:%S"), "] ", paste(..., collapse = " "))
  message(msg)
}

require_cols <- function(df, cols, df_name = "data.frame") {
  missing <- setdiff(cols, names(df))
  if (length(missing) > 0) stop(df_name, " is missing required columns: ", paste(missing, collapse = ", "))
}

# Excel date cleaner (accepts serials or dates)
clean_excel_date <- function(x) {
  suppressWarnings({
    as.Date(as.numeric(x), origin = "1899-12-30")
  })
}

# Coalesce numeric safely
num_coalesce <- function(...) {
  suppressWarnings(as.numeric(dplyr::coalesce(...)))
}

# Normalize 2-letter state codes (also handles full names if ever present)
normalize_state <- function(x) {
  x <- trimws(x)
  # If already 2-letter uppercase, keep
  keep <- nchar(x) == 2 & grepl("^[A-Z]{2}$", x)
  out <- x
  # Convert full names to abbreviations if needed
  if (any(!keep)) {
    nm2abb <- setNames(state.abb, state.name)
    nm2abb <- c(nm2abb, "District of Columbia" = "DC")
    out[!keep] <- nm2abb[out[!keep]]
  }
  toupper(out)
}

# --- File paths (edit if needed) ---------------------------------------------
LBNL_PATH <- "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/LBNL Queued Up 2025.xlsx"
EIA_OPERATING_PATH <- "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/august_generator2025 (3).xlsx"

if (!file.exists(LBNL_PATH)) stop("LBNL file not found: ", LBNL_PATH)
if (!file.exists(EIA_OPERATING_PATH)) stop("EIA-860M file not found: ", EIA_OPERATING_PATH)

# --- Define geography: Contiguous U.S. (CONUS) -------------------------------
CONUS_STATES <- setdiff(state.abb, c("AK", "HI"))
dbg("CONUS (48 states): ", paste(CONUS_STATES, collapse = ", "))

# --- Load LBNL: raw & codebook ----------------------------------------------
dbg("Reading LBNL 'Queued Up 2025' (raw + codebook)...")
LBNL_Queued_Up_2025_Raw <- read_excel(
  LBNL_PATH,
  sheet = "03. Complete Queue Data",
  skip = 1
)

LBNL_Queued_Up_2025_Codebook <- read_excel(
  LBNL_PATH,
  sheet = "04. Data Codebook",
  skip = 1
)

# Validate codebook columns
require_cols(LBNL_Queued_Up_2025_Codebook, c("Field Name", "Description"), "LBNL Codebook")

# --- Build mapping and rename LBNL columns -----------------------------------
rename_map <- LBNL_Queued_Up_2025_Codebook %>%
  select(`Field Name`, Description) %>%
  filter(`Field Name` %in% names(LBNL_Queued_Up_2025_Raw)) %>%
  tibble::deframe()

LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025_Raw %>%
  rename_with(~ rename_map[.x], .cols = intersect(names(.), names(rename_map)))

# --- Fix LBNL dates & numeric coercions --------------------------------------
date_cols <- c(
  "interconnection request date (date project entered queue)",
  "proposed online date from interconnection application",
  "date project became operational (if applicable)",
  "date project withdrawn from queue (if applicable)",
  "date of signed interconnection agreement (if applicable)"
)

LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025 %>%
  mutate(
    across(all_of(date_cols), clean_excel_date),
    `year project entered queue` = suppressWarnings(as.integer(`year project entered queue`)),
    `proposed online year from interconnection application` =
      suppressWarnings(as.integer(`proposed online year from interconnection application`))
  )

# Resource & capacity fields -> clean
to_missing <- function(x) ifelse(is.na(x) | x == "NA" | x == "", NA, x)

LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025 %>%
  mutate(
    `resource type 1` = to_missing(`resource type 1`),
    `resource type 2` = to_missing(`resource type 2`),
    `resource type 3` = to_missing(`resource type 3`),
    `capacity of type 1 (MW)` = suppressWarnings(as.numeric(to_missing(`capacity of type 1 (MW)`))),
    `capacity of type 2 (MW)` = suppressWarnings(as.numeric(to_missing(`capacity of type 2 (MW)`))),
    `capacity of type 3 (MW)` = suppressWarnings(as.numeric(to_missing(`capacity of type 3 (MW)`)))
  )

# --- Filter LBNL to ACTIVE queue entries (in-queue = "active") ----------------
status_col <- "current queue status (active, withdrawn, suspended, or operational)"
if (!status_col %in% names(LBNL_Queued_Up_2025)) {
  stop("Expected LBNL status column not found: ", status_col)
}

df_active_gen <- LBNL_Queued_Up_2025 %>%
  filter(tolower(.data[[status_col]]) == "active")

dbg("LBNL active rows: ", nrow(df_active_gen), " / original: ", nrow(LBNL_Queued_Up_2025))

# --- Long-ify resource/capacity pairs ----------------------------------------
by_state_resource_long <- df_active_gen %>%
  transmute(
    state    = normalize_state(`state where project is located`),
    req_year = `year project entered queue`,
    rt1      = `resource type 1`, cap1 = `capacity of type 1 (MW)`,
    rt2      = `resource type 2`, cap2 = `capacity of type 2 (MW)`,
    rt3      = `resource type 3`, cap3 = `capacity of type 3 (MW)`
  ) %>%
  tidyr::pivot_longer(
    cols = c(rt1, cap1, rt2, cap2, rt3, cap3),
    names_to = c(".value", "slot"),
    names_pattern = "([a-z]+)([123])"
  ) %>%
  rename(resource_type = rt, capacity_mw = cap) %>%
  filter(!is.na(state), !is.na(resource_type), !is.na(capacity_mw), capacity_mw > 0)

dbg("Unique LBNL resource types (active): ", paste(sort(unique(by_state_resource_long$resource_type)), collapse = ", "))
dbg("Unique LBNL states (pre-CONUS): ", paste(sort(unique(by_state_resource_long$state)), collapse = ", "))

# --- Restrict LBNL to CONUS ---------------------------------------------------
by_state_resource_long <- by_state_resource_long %>%
  filter(state %in% CONUS_STATES)

dbg("LBNL (active, CONUS) rows: ", nrow(by_state_resource_long))
dbg("LBNL states (CONUS): ", paste(sort(unique(by_state_resource_long$state)), collapse = ", "))

# --- Summaries (LBNL) ---------------------------------------------------------
capacity_by_state_resource <- by_state_resource_long %>%
  group_by(state, resource_type) %>%
  summarise(total_capacity_mw = sum(capacity_mw, na.rm = TRUE), .groups = "drop") %>%
  arrange(state, desc(total_capacity_mw))

capacity_by_state_total_queued <- capacity_by_state_resource %>%
  group_by(state) %>%
  summarise(queued_total_mw = sum(total_capacity_mw, na.rm = TRUE), .groups = "drop") %>%
  mutate(queued_total_gw = queued_total_mw / 1000)

dbg("Check LBNL queued totals (first 10):")
print(head(capacity_by_state_total_queued, 10), row.names = FALSE)

# --- EIA-860M: Operating sheet (denominator) ----------------------------------
dbg("Reading EIA-860/860M 'Operating' worksheet...")
eia_opgen <- read_excel(EIA_OPERATING_PATH, sheet = "Operating", skip = 2)

# Validate key columns exist
eia_req_cols <- c(
  "Plant State", "Status",
  "Nameplate Capacity (MW)", "Net Summer Capacity (MW)"
)
require_cols(eia_opgen, eia_req_cols, "EIA Operating")

# Clean & restrict to CONUS, keep OP + SB statuses only
dbg("Unique EIA status examples (first 10): ", paste(utils::head(unique(eia_opgen$Status), 10), collapse = " | "))

eia_conus <- eia_opgen %>%
  mutate(
    PlantState = normalize_state(`Plant State`),
    status_code = toupper(stringr::str_match(Status, "^\\(([A-Za-z]+)\\)")[,2]),
    # Use Net Summer (AC) when available; fallback to Nameplate
    denom_capacity_mw = num_coalesce(`Net Summer Capacity (MW)`, `Nameplate Capacity (MW)`)
  ) %>%
  filter(
    PlantState %in% CONUS_STATES,
    status_code %in% c("OP", "SB"),                 # Operating + Standby
    !is.na(denom_capacity_mw),
    denom_capacity_mw > 0
  ) %>%
  select(PlantState, status_code, denom_capacity_mw)

dbg("EIA rows after CONUS + status filter: ", nrow(eia_conus))
dbg("Statuses kept (counts):")
print(as.data.frame(table(eia_conus$status_code)))

operational_by_state <- eia_conus %>%
  group_by(state = PlantState) %>%
  summarise(operational_total_mw = sum(denom_capacity_mw, na.rm = TRUE), .groups = "drop") %>%
  mutate(operational_total_gw = operational_total_mw / 1000)

dbg("Check EIA operational totals (first 10):")
print(head(operational_by_state, 10), row.names = FALSE)

# --- Harmonize & compute % by state ------------------------------------------
# Ensure we evaluate across the same set of states (CONUS), and fill missing with zeros
state_frame <- tibble(state = CONUS_STATES)

queued_state <- state_frame %>%
  left_join(capacity_by_state_total_queued, by = "state") %>%
  mutate(queued_total_mw = coalesce(queued_total_mw, 0),
         queued_total_gw = queued_total_mw / 1000)

oper_state <- state_frame %>%
  left_join(operational_by_state, by = "state") %>%
  mutate(operational_total_mw = coalesce(operational_total_mw, 0),
         operational_total_gw = operational_total_mw / 1000)

queued_pct_by_state <- queued_state %>%
  left_join(select(oper_state, state, operational_total_mw, operational_total_gw), by = "state") %>%
  mutate(
    queued_to_operational_pct = dplyr::if_else(
      operational_total_mw > 0,
      100 * queued_total_mw / operational_total_mw,
      NA_real_
    )
  ) %>%
  arrange(state)

dbg("Sanity check: states with zero denominator (operational_total_mw == 0): ",
    paste(queued_pct_by_state$state[queued_pct_by_state$operational_total_mw == 0], collapse = ", "))

dbg("Preview: queued_pct_by_state (first 10):")
print(head(queued_pct_by_state, 10), row.names = FALSE)

# --- Map states -> Census regions --------------------------------------------
dbg("Fetching Census regions & states (tigris 2024)...")
regions <- tigris::regions(year = 2024) %>%
  st_drop_geometry() %>%
  transmute(
    REGION_GEOID = as.character(GEOID),
    CENSUS_REGION_NAME = NAMELSAD
  )

states_tbl <- tigris::states(year = 2024, cb = FALSE) %>%
  st_drop_geometry() %>%
  mutate(REGION = as.character(REGION)) %>%
  filter(STUSPS %in% CONUS_STATES) %>%           # **CONUS filter here**
  rename(STATE_NAME = NAME) %>%
  select(STUSPS, STATE_NAME, STATEFP, REGION) %>%
  left_join(regions, by = c("REGION" = "REGION_GEOID"))

require_cols(states_tbl, c("STUSPS", "CENSUS_REGION_NAME"), "tigris states join")

dbg("Census regions present: ", paste(unique(states_tbl$CENSUS_REGION_NAME), collapse = " | "))

# Attach region to state-level % table
queued_pct_by_state <- queued_pct_by_state %>%
  left_join(states_tbl %>% select(STUSPS, CENSUS_REGION_NAME), by = c("state" = "STUSPS")) %>%
  relocate(CENSUS_REGION_NAME, .after = state)

# --- Aggregate to Census regions ---------------------------------------------
# Regional numerator & denominator are sums of state totals (GW and MW provided)
queued_pct_by_region <- queued_pct_by_state %>%
  group_by(CENSUS_REGION_NAME) %>%
  summarise(
    queued_total_mw       = sum(queued_total_mw, na.rm = TRUE),
    operational_total_mw  = sum(operational_total_mw, na.rm = TRUE),
    queued_total_gw       = sum(queued_total_gw, na.rm = TRUE),
    operational_total_gw  = sum(operational_total_gw, na.rm = TRUE),
    queued_to_operational_pct =
      ifelse(operational_total_mw > 0, 100 * queued_total_mw / operational_total_mw, NA_real_),
    .groups = "drop"
  ) %>%
  arrange(CENSUS_REGION_NAME)

dbg("Preview: queued_pct_by_region:")
print(queued_pct_by_region, row.names = FALSE)

# --- Optional: resource-wide tables you already had (now CONUS-scoped) -------
# Wide (GW) by state × resource (CONUS only)
capacity_by_state_resource_wide <- capacity_by_state_resource %>%
  mutate(total_capacity_gw = total_capacity_mw / 1000) %>%
  select(-total_capacity_mw) %>%
  tidyr::pivot_wider(
    names_from  = resource_type,
    values_from = total_capacity_gw,
    values_fill = 0
  ) %>%
  arrange(state) %>%
  mutate(Total = rowSums(select(., -state), na.rm = TRUE))

# --- Exports (optional) -------------------------------------------------------
readr::write_csv(queued_pct_by_state,  "queued_percent_by_state_conus.csv")
readr::write_csv(queued_pct_by_region, "queued_percent_by_region_conus.csv")
readr::write_csv(capacity_by_state_resource_wide, "capacity_by_state_resource_wide_gw_conus.csv")

# --- Final debug summaries ----------------------------------------------------
dbg("Final rows: queued_pct_by_state = ", nrow(queued_pct_by_state),
    " (expect 48), queued_pct_by_region = ", nrow(queued_pct_by_region), " (expect 4)")

dbg("Total queued (GW, CONUS): ",
    round(sum(queued_pct_by_state$queued_total_gw, na.rm = TRUE), 3))

dbg("Total operational (GW, CONUS): ",
    round(sum(queued_pct_by_state$operational_total_gw, na.rm = TRUE), 3))

dbg("All done. Objects available in workspace:",
    "queued_pct_by_state, queued_pct_by_region, capacity_by_state_resource_wide")
