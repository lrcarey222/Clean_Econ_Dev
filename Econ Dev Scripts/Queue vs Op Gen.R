# ================================
# Queued vs Operating Capacity (%)
# By State and By Census Region
# Scope: Continental US (48 states)
# ================================

# --- Libraries ---
suppressPackageStartupMessages({
  library(dplyr)
  library(readxl)
  library(tidyr)
  library(readr)
  library(tigris)
  library(sf)
  library(stringr)
  library(rlang)
})

options(stringsAsFactors = FALSE)
options(tigris_use_cache = TRUE)

# --- Helper: debug printer ---
dbg <- function(..., .sep = " ") {
  msg <- paste0("[DEBUG ", format(Sys.time(), "%H:%M:%S"), "] ", paste(..., collapse = .sep))
  message(msg)
}

# --- File paths (EDIT if needed) ---
path_lbnl <- "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/LBNL Queued Up 2025.xlsx"
path_eia  <- "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/august_generator2025 (3).xlsx"

# --- Continental US state set (48) ---
continental_states <- setdiff(state.abb, c("AK", "HI"))
dbg("Continental states count:", length(continental_states))
stopifnot(length(continental_states) == 48)

# ==========================
# 1) LBNL: Load + Clean
# ==========================
dbg("Loading LBNL queued-up workbook...")
LBNL_Queued_Up_2025_Raw <- read_excel(
  path_lbnl,
  sheet = "03. Complete Queue Data",
  skip  = 1
)

dbg("Loading LBNL codebook...")
LBNL_Queued_Up_2025_Codebook <- read_excel(
  path_lbnl,
  sheet = "04. Data Codebook",
  skip  = 1
)

# Build mapping old name -> full description (only those present)
rename_map <- LBNL_Queued_Up_2025_Codebook %>%
  dplyr::select(`Field Name`, Description) %>%
  dplyr::filter(`Field Name` %in% names(LBNL_Queued_Up_2025_Raw)) %>%
  tibble::deframe()

dbg("Columns before rename:", paste(names(LBNL_Queued_Up_2025_Raw), collapse = ", "))
LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025_Raw %>%
  dplyr::rename_with(~ rename_map[.x], .cols = intersect(names(.), names(rename_map)))
dbg("Columns after rename:", paste(names(LBNL_Queued_Up_2025), collapse = ", "))

# Fix dates + coerce years
date_cols <- c(
  "interconnection request date (date project entered queue)",
  "proposed online date from interconnection application",
  "date project became operational (if applicable)",
  "date project withdrawn from queue (if applicable)",
  "date of signed interconnection agreement (if applicable)"
)

dbg("Coercing Excel date serials (if any) and year fields...")
LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025 %>%
  dplyr::mutate(
    dplyr::across(
      tidyselect::any_of(date_cols),
      ~ as.Date(suppressWarnings(as.numeric(.x)), origin = "1899-12-30")
    ),
    `year project entered queue` =
      suppressWarnings(as.integer(`year project entered queue`)),
    `proposed online year from interconnection application` =
      suppressWarnings(as.integer(`proposed online year from interconnection application`))
  )

# Clean resource types + capacities
to_missing <- function(x) ifelse(is.na(x) | x == "NA" | x == "", NA, x)

cap_cols <- c("capacity of type 1 (MW)", "capacity of type 2 (MW)", "capacity of type 3 (MW)")
dbg("Ensuring capacity columns numeric:", paste(cap_cols, collapse = ", "))

LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025 %>%
  dplyr::mutate(
    `resource type 1` = to_missing(`resource type 1`),
    `resource type 2` = to_missing(`resource type 2`),
    `resource type 3` = to_missing(`resource type 3`),
    `capacity of type 1 (MW)` = suppressWarnings(as.numeric(to_missing(`capacity of type 1 (MW)`))),
    `capacity of type 2 (MW)` = suppressWarnings(as.numeric(to_missing(`capacity of type 2 (MW)`))),
    `capacity of type 3 (MW)` = suppressWarnings(as.numeric(to_missing(`capacity of type 3 (MW)`)))
  )

# Filter to ACTIVE queue status (in-queue = active)
status_col <- "current queue status (active, withdrawn, suspended, or operational)"
stopifnot(status_col %in% names(LBNL_Queued_Up_2025))
dbg("Filtering to ACTIVE queue records...")
df_active_gen <- LBNL_Queued_Up_2025 %>%
  dplyr::filter(tolower(.data[[status_col]]) == "active")

dbg("Active queue rows:", nrow(df_active_gen))

# Keep only continental US states (match LBNL scope to EIA scope)
state_col_lbnl <- "state where project is located"
stopifnot(state_col_lbnl %in% names(df_active_gen))
df_active_gen <- df_active_gen %>%
  dplyr::filter(.data[[state_col_lbnl]] %in% continental_states)

dbg("Active queue rows after continental filter:", nrow(df_active_gen))
dbg("Unique queue states:", paste(sort(unique(df_active_gen[[state_col_lbnl]])), collapse = ", "))

# Long-ify resource/capacity pairs
dbg("Pivoting resource/capacity pairs to long format...")
by_state_resource_long <- df_active_gen %>%
  dplyr::transmute(
    state    = .data[[state_col_lbnl]],
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
  dplyr::rename(resource_type = rt, capacity_mw = cap) %>%
  dplyr::filter(!is.na(state), !is.na(resource_type), !is.na(capacity_mw), capacity_mw > 0)

dbg("Rows in long queue table:", nrow(by_state_resource_long))

# Summaries in MW (keep MW for % calc vs EIA MW)
dbg("Summarizing queued capacity (MW) by state...")
queued_by_state_mw <- by_state_resource_long %>%
  dplyr::group_by(state) %>%
  dplyr::summarise(queued_mw = sum(capacity_mw, na.rm = TRUE), .groups = "drop") %>%
  dplyr::arrange(state)

dbg("Queue MW totals — sample:")
print(head(queued_by_state_mw, 10))

# ==========================
# 2) EIA: Load + Clean
# ==========================
dbg("Loading EIA 860/860M Operating sheet...")
eia_opgen <- read_excel(path_eia, sheet = "Operating", skip = 2)

dbg("EIA columns:", paste(names(eia_opgen), collapse = ", "))

# Basic sanity: require columns
req_eia_cols <- c("Plant State", "Nameplate Capacity (MW)", "Status")
missing_eia <- setdiff(req_eia_cols, names(eia_opgen))
if (length(missing_eia) > 0) {
  stop("Missing required EIA columns: ", paste(missing_eia, collapse = ", "))
}

# Keep plausible numeric capacity and non-missing state
dbg("Filtering EIA rows to valid state/capacity...")
eia_opgen <- eia_opgen %>%
  dplyr::filter(!is.na(`Plant State`),
                !is.na(`Nameplate Capacity (MW)`),
                `Nameplate Capacity (MW)` >= 0)

dbg("EIA rows after NA/>=0 filter:", nrow(eia_opgen))

# Restrict to continental US (48); exclude DC & territories explicitly
dbg("Restricting EIA to continental US (48 states only)...")
eia_opgen_contig <- eia_opgen %>%
  dplyr::filter(`Plant State` %in% continental_states)

dbg("EIA rows after continental filter:", nrow(eia_opgen_contig))
dbg("Unique EIA states:", paste(sort(unique(eia_opgen_contig$`Plant State`)), collapse = ", "))

# (Optional) If you need to further restrict to true operating status codes, you can filter here.
# The "Operating" sheet should already reflect operating units; we maintain all rows on this tab.
# Example (commented):
# eia_opgen_contig <- eia_opgen_contig %>%
#   dplyr::filter(str_detect(Status, regex("OP|Operating", ignore_case = TRUE)))

# Summarize operating capacity by state (Nameplate MW)
dbg("Summarizing operating (Nameplate MW) by state...")
operating_by_state_mw <- eia_opgen_contig %>%
  dplyr::group_by(state = `Plant State`) %>%
  dplyr::summarise(operating_mw = sum(`Nameplate Capacity (MW)`, na.rm = TRUE), .groups = "drop") %>%
  dplyr::arrange(state)

dbg("Operating MW totals — sample:")
print(head(operating_by_state_mw, 10))

# ==========================
# 3) Align to Regions
# ==========================
dbg("Fetching Census regions and states from tigris (2024)...")
regions <- tigris::regions(year = 2024) %>%
  sf::st_drop_geometry() %>%
  transmute(
    REGION_GEOID = as.character(GEOID),
    CENSUS_REGION_NAME = NAMELSAD
  )

states_tbl <- tigris::states(year = 2024, cb = FALSE) %>%
  sf::st_drop_geometry() %>%
  mutate(REGION = as.character(REGION)) %>%
  # keep exactly the 50 states, then reduce to 48 in a moment:
  filter(STUSPS %in% state.abb) %>%
  rename(STATE_NAME = NAME) %>%
  select(STUSPS, STATE_NAME, STATEFP, REGION)

# Now reduce to 48 continental for consistent joining
states_tbl <- states_tbl %>% filter(STUSPS %in% continental_states)
dbg("tigris states kept (should be 48):", nrow(states_tbl))
stopifnot(nrow(states_tbl) == 48)

# Attach region names
states_with_regions <- states_tbl %>%
  left_join(regions, by = c("REGION" = "REGION_GEOID"))

dbg("States with regions — sample:")
print(head(states_with_regions, 10))

# ==========================
# 4) Build % Tables
# ==========================
dbg("Joining queued and operating totals by state...")
state_totals <- states_with_regions %>%
  select(state = STUSPS, CENSUS_REGION_NAME) %>%
  left_join(queued_by_state_mw,    by = "state") %>%
  left_join(operating_by_state_mw, by = "state")

# Replace NAs with 0 where appropriate
state_totals <- state_totals %>%
  mutate(
    queued_mw    = coalesce(queued_mw, 0),
    operating_mw = coalesce(operating_mw, 0)
  )

# Sanity: states with zero operating capacity would create Inf/NaN. Guard:
zero_ops <- state_totals %>% filter(operating_mw <= 0)
if (nrow(zero_ops) > 0) {
  warning("States with zero or missing operating capacity: ",
          paste(zero_ops$state, collapse = ", "), ". Percentages set to NA.")
}

inqueue_share_by_state <- state_totals %>%
  mutate(
    inqueue_share_pct = dplyr::if_else(operating_mw > 0,
                                       100 * queued_mw / operating_mw,
                                       NA_real_)
  ) %>%
  arrange(state)

dbg("Final by-state rows:", nrow(inqueue_share_by_state))
dbg("By-state totals (queued MW sum / operating MW sum): ",
    sum(inqueue_share_by_state$queued_mw, na.rm = TRUE), "/",
    sum(inqueue_share_by_state$operating_mw, na.rm = TRUE))

dbg("Creating region-level table (capacity sums across states, then %)...")
region_totals <- inqueue_share_by_state %>%
  group_by(CENSUS_REGION_NAME) %>%
  summarise(
    queued_mw    = sum(queued_mw, na.rm = TRUE),
    operating_mw = sum(operating_mw, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    inqueue_share_pct = dplyr::if_else(operating_mw > 0,
                                       100 * queued_mw / operating_mw,
                                       NA_real_)
  ) %>%
  arrange(CENSUS_REGION_NAME)

inqueue_share_by_region <- region_totals

dbg("Final by-region rows:", nrow(inqueue_share_by_region))
dbg("By-region totals (queued MW sum / operating MW sum): ",
    sum(inqueue_share_by_region$queued_mw, na.rm = TRUE), "/",
    sum(inqueue_share_by_region$operating_mw, na.rm = TRUE))

# ==========================
# 5) Optional: Inspect / Export
# ==========================
dbg("Glimpse (by state):")
glimpse(inqueue_share_by_state)

dbg("Head (by region):")
print(inqueue_share_by_region)

# Optional CSV exports
# readr::write_csv(inqueue_share_by_state,  "inqueue_share_by_state.csv")
# readr::write_csv(inqueue_share_by_region, "inqueue_share_by_region.csv")

# ==========================
# 6) Extra Diagnostics
# ==========================
# Cross-check that the states listed in the LBNL active set are a subset of the 48:
lbnl_states <- sort(unique(df_active_gen[[state_col_lbnl]]))
if (!all(lbnl_states %in% continental_states)) {
  warning("Some LBNL states not in continental set: ",
          paste(setdiff(lbnl_states, continental_states), collapse = ", "))
} else {
  dbg("All LBNL states are within the continental set.")
}

# Cross-check: any continental states missing in queued or operating tables?
missing_in_queue   <- setdiff(continental_states, queued_by_state_mw$state)
missing_in_oper    <- setdiff(continental_states, operating_by_state_mw$state)
if (length(missing_in_queue)) dbg("States with NO queued capacity:", paste(missing_in_queue, collapse = ", "))
if (length(missing_in_oper))  dbg("States with NO operating capacity:", paste(missing_in_oper,  collapse = ", "))

dbg("Script complete. Objects ready:",
    "inqueue_share_by_state, inqueue_share_by_region")
