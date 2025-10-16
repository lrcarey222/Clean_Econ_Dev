#Chart product to edit: https://www.datawrapper.de/_/ZAFRe/

# --- Libraries ---
library(dplyr)
library(readxl)
library(tidyr)
library(readr)

# --- Load data ---
LBNL_Queued_Up_2025_Raw <- read_excel(
  "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/LBNL Queued Up 2025.xlsx",
  sheet = "03. Complete Queue Data", skip = 1
)

LBNL_Queued_Up_2025_Codebook <- read_excel(
  "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/LBNL Queued Up 2025.xlsx",
  sheet = "04. Data Codebook", skip = 1
)

# Optional: inspect full codebook
print(LBNL_Queued_Up_2025_Codebook, n = Inf)

# --- Build mapping from field name to full description (exact) ---
rename_map <- LBNL_Queued_Up_2025_Codebook %>%
  dplyr::select(`Field Name`, Description) %>%
  dplyr::filter(`Field Name` %in% names(LBNL_Queued_Up_2025_Raw)) %>%
  tibble::deframe()  # named vector: old field -> full description

# --- Rename columns to full descriptions ---
LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025_Raw %>%
  dplyr::rename_with(~ rename_map[.x], .cols = intersect(names(.), names(rename_map)))

# --- Fix dates (Excel serials) + coerce year fields ---
date_cols <- c(
  "interconnection request date (date project entered queue)",
  "proposed online date from interconnection application",
  "date project became operational (if applicable)",
  "date project withdrawn from queue (if applicable)",
  "date of signed interconnection agreement (if applicable)"
)

LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025 %>%
  dplyr::mutate(
    dplyr::across(all_of(date_cols),
                  ~ as.Date(suppressWarnings(as.numeric(.x)), origin = "1899-12-30")),
    `year project entered queue` =
      suppressWarnings(as.integer(`year project entered queue`)),
    `proposed online year from interconnection application` =
      suppressWarnings(as.integer(`proposed online year from interconnection application`))
  )

# --- Clean resource + capacity fields ---
to_missing <- function(x) ifelse(is.na(x) | x == "NA" | x == "", NA, x)

LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025 %>%
  dplyr::mutate(
    `resource type 1` = to_missing(`resource type 1`),
    `resource type 2` = to_missing(`resource type 2`),
    `resource type 3` = to_missing(`resource type 3`),
    `capacity of type 1 (MW)` = suppressWarnings(as.numeric(to_missing(`capacity of type 1 (MW)`))),
    `capacity of type 2 (MW)` = suppressWarnings(as.numeric(to_missing(`capacity of type 2 (MW)`))),
    `capacity of type 3 (MW)` = suppressWarnings(as.numeric(to_missing(`capacity of type 3 (MW)`)))
  )

# --- Filter to ACTIVE Generation only (in-queue = active) ---
df_active_gen <- LBNL_Queued_Up_2025 %>%
  dplyr::filter(
    tolower(`current queue status (active, withdrawn, suspended, or operational)`) == "active"
  )

# --- Long-ify resource/capacity pairs (explicit and robust) ---
by_state_resource_long <- df_active_gen %>%
  dplyr::transmute(
    state    = `state where project is located`,
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

# --- Summary: total MW by state & resource (all years) ---
capacity_by_state_resource <- by_state_resource_long %>%
  dplyr::group_by(state, resource_type) %>%
  dplyr::summarise(total_capacity_mw = sum(capacity_mw, na.rm = TRUE), .groups = "drop") %>%
  dplyr::arrange(state, dplyr::desc(total_capacity_mw))

# --- Pivoted ACTIVE in-queue generation capacity by state × resource × interconnection request year (MW) ---
capacity_by_state_resource_year_long <- by_state_resource_long %>%
  dplyr::filter(!is.na(req_year)) %>%
  dplyr::group_by(state, req_year, resource_type) %>%
  dplyr::summarise(total_capacity_mw = sum(capacity_mw, na.rm = TRUE), .groups = "drop") %>%
  dplyr::arrange(state, req_year, dplyr::desc(total_capacity_mw))

capacity_by_state_resource_year_wide_mw <- capacity_by_state_resource_year_long %>%
  tidyr::pivot_wider(
    names_from  = resource_type,
    values_from = total_capacity_mw,
    values_fill = 0
  ) %>%
  dplyr::arrange(state, req_year)

# --- Active in-queue capacity by state × year (MW) ---
capacity_by_state_year_long <- by_state_resource_long %>%
  dplyr::group_by(state, req_year) %>%
  dplyr::summarise(total_capacity_mw = sum(capacity_mw, na.rm = TRUE), .groups = "drop") %>%
  dplyr::arrange(state, req_year)

capacity_by_state_year_wide_mw <- capacity_by_state_year_long %>%
  tidyr::pivot_wider(
    names_from  = req_year,
    values_from = total_capacity_mw,
    values_fill = 0,
    names_sort  = TRUE
  ) %>%
  dplyr::arrange(state)

# =========================
# === Convert to **GW** ===
# =========================

# 1) State × Resource (all years): wide in GW
capacity_by_state_resource_wide <- capacity_by_state_resource %>%
  dplyr::mutate(total_capacity_gw = total_capacity_mw / 1000) %>%
  dplyr::select(-total_capacity_mw) %>%
  tidyr::pivot_wider(
    names_from  = resource_type,
    values_from = total_capacity_gw,
    values_fill = 0
  ) %>%
  dplyr::arrange(state)

# 2) State × Year × Resource: wide in GW (kept if you need it)
capacity_by_state_resource_year_wide <- capacity_by_state_resource_year_wide_mw %>%
  dplyr::mutate(dplyr::across(-c(state, req_year), ~ .x / 1000))

# 3) State × Year: wide in GW (FINAL)
capacity_by_state_year_wide <- capacity_by_state_year_wide_mw %>%
  dplyr::mutate(dplyr::across(-state, ~ .x / 1000))

# --- Inspect (both final tables are in GW) ---
glimpse(capacity_by_state_resource_wide)
glimpse(capacity_by_state_year_wide)

#Add a "total" column to capacity_by_state_resource_wide
capacity_by_state_resource_wide <- capacity_by_state_resource_wide %>%
  dplyr::mutate(Total = rowSums(dplyr::select(., -state), na.rm = TRUE)) %>%
  dplyr::arrange(state)

# --- Optional: Export CSVs (GW) ---
readr::write_csv(capacity_by_state_resource_wide, "capacity_by_state_resource_wide_gw.csv")
readr::write_csv(capacity_by_state_year_wide,    "capacity_by_state_year_wide_gw.csv")

library(tidyverse)
library(tigris); options(tigris_use_cache = TRUE)
library(sf)

# Fetch & prep Census regions (no geometry)
regions <- tigris::regions(year = 2024) %>%
  st_drop_geometry() %>%
  transmute(
    REGION_GEOID = as.character(GEOID),   # ensure character
    CENSUS_REGION_NAME = NAMELSAD
  )

# Fetch & prep states; keep exactly the 50 states
states <- tigris::states(year = 2024, cb = FALSE) %>%
  st_drop_geometry() %>%
  mutate(REGION = as.character(REGION)) %>%  # match type for join
  filter(STUSPS %in% state.abb) %>%          # exactly the 50 states (excludes DC & territories)
  rename(STATE_NAME = NAME) %>%
  select(STUSPS, STATE_NAME, STATEFP, REGION)

# Join: states -> regions (by region code) -> capacity (by state abbr)
state_region_resource <- states %>%
  left_join(regions, by = c("REGION" = "REGION_GEOID")) %>%
  left_join(capacity_by_state_resource_wide, by = c("STUSPS" = "state"))

glimpse(state_region_resource)

#Excluding the "Total" column, aggregate data by CENSUS_REGION_NAME such that we have columns for "REGION", CENSUS_REGION_NAME", and total by each resource
region_resource_summary <- state_region_resource %>%
  select(-Total) %>%
  group_by(REGION, CENSUS_REGION_NAME) %>%
  summarise(across(where(is.numeric), sum, na.rm = TRUE), .groups = "drop") %>%
  arrange(REGION, CENSUS_REGION_NAME) %>%
  select(-REGION)
glimpse(region_resource_summary)

#Export region_resource_summary as CSV
readr::write_csv(region_resource_summary, "region_resource_summary_gw.csv")
