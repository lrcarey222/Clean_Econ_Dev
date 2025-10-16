# --- Libraries ---
library(dplyr)
library(readxl)
library(tidyr)

# --- Load data ---
LBNL_Queued_Up_2025_Raw <- read_excel(
  "Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/LBNL Queued Up 2025.xlsx",
  sheet = "03. Complete Queue Data", skip = 1
)

LBNL_Queued_Up_2025_Codebook <- read_excel(
  "Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/Raw Data/LBNL Queued Up 2025.xlsx",
  sheet = "04. Data Codebook", skip = 1
)

# --- Build mapping from field name to full description (exact) ---
rename_map <- LBNL_Queued_Up_2025_Codebook %>%
  select(`Field Name`, Description) %>%
  filter(`Field Name` %in% names(LBNL_Queued_Up_2025_Raw)) %>%
  deframe()  # named vector: names = old field, values = full description

# --- Rename columns to full descriptions ---
LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025_Raw %>%
  rename_with(~ rename_map[.x], .cols = intersect(names(.), names(rename_map)))

# --- Fix dates (Excel serials) + coerce year fields ---
date_cols <- c(
  "interconnection request date (date project entered queue)",
  "proposed online date from interconnection application",
  "date project became operational (if applicable)",
  "date project withdrawn from queue (if applicable)",
  "date of signed interconnection agreement (if applicable)"
)

LBNL_Queued_Up_2025 <- LBNL_Queued_Up_2025 %>%
  mutate(
    across(all_of(date_cols),
           ~ as.Date(suppressWarnings(as.numeric(.x)), origin = "1899-12-30")),
    `year project entered queue` =
      suppressWarnings(as.integer(`year project entered queue`)),
    `proposed online year from interconnection application` =
      suppressWarnings(as.integer(`proposed online year from interconnection application`))
  )

# --- Clean resource + capacity fields ---
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

# --- Filter to ACTIVE Generation only (in-queue = active) ---
df_active_gen <- LBNL_Queued_Up_2025 %>%
  filter(
    tolower(`current queue status (active, withdrawn, suspended, or operational)`) == "active",
    tolower(`type of project or interconnection request (load/transmission/generation)`) == "generation"
  )

# --- Long-ify resource/capacity pairs (explicit and robust) ---
by_state_resource_long <- df_active_gen %>%
  transmute(
    state   = `state where project is located`,
    req_year = `year project entered queue`,
    rt1     = `resource type 1`, cap1 = `capacity of type 1 (MW)`,
    rt2     = `resource type 2`, cap2 = `capacity of type 2 (MW)`,
    rt3     = `resource type 3`, cap3 = `capacity of type 3 (MW)`
  ) %>%
  pivot_longer(
    cols = c(rt1, cap1, rt2, cap2, rt3, cap3),
    names_to = c(".value", "slot"),
    names_pattern = "([a-z]+)([123])"
  ) %>%
  rename(resource_type = rt, capacity_mw = cap) %>%
  filter(!is.na(state), !is.na(resource_type), !is.na(capacity_mw), capacity_mw > 0)

# --- Summary: total MW by state & resource (all years) ---
capacity_by_state_resource <- by_state_resource_long %>%
  group_by(state, resource_type) %>%
  summarise(total_capacity_mw = sum(capacity_mw, na.rm = TRUE), .groups = "drop") %>%
  arrange(state, desc(total_capacity_mw))

# --- Pivoted ACTIVE in-queue generation capacity by state × resource × interconnection request year ---
# Long version (grouped by year)
capacity_by_state_resource_year_long <- by_state_resource_long %>%
  filter(!is.na(req_year)) %>%
  group_by(state, req_year, resource_type) %>%
  summarise(total_capacity_mw = sum(capacity_mw, na.rm = TRUE), .groups = "drop") %>%
  arrange(state, req_year, desc(total_capacity_mw))

# Wide/pivoted version (resources as columns)
capacity_by_state_resource_year_wide <- capacity_by_state_resource_year_long %>%
  pivot_wider(
    names_from  = resource_type,
    values_from = total_capacity_mw,
    values_fill = 0
  ) %>%
  arrange(state, req_year)

# --- Inspect results ---
# totals by state x resource (all years)
glimpse(capacity_by_state_resource_wide)

# pivoted by state x interconnection request year x resource
glimpse(capacity_by_state_resource_year_wide)

# --- Active in-queue capacity by state x year (wide) ---
# Start from the long table you already built: by_state_resource_long
# (If not available, replace the first line with df_active_gen and sum the three capacity columns directly.)

capacity_by_state_year_long <- by_state_resource_long %>%
  group_by(state, req_year) %>%
  summarise(total_capacity_mw = sum(capacity_mw, na.rm = TRUE), .groups = "drop") %>%
  arrange(state, req_year)

capacity_by_state_year_wide <- capacity_by_state_year_long %>%
  tidyr::pivot_wider(
    names_from  = req_year,
    values_from = total_capacity_mw,
    values_fill = 0,
    names_sort  = TRUE   # ensure year columns are in ascending order
  ) %>%
  arrange(state)

# Inspect
glimpse(capacity_by_state_year_wide)

# (Optional) Save
# readr::write_csv(capacity_by_state_year_wide, "active_gen_capacity_by_state_year_wide.csv")
