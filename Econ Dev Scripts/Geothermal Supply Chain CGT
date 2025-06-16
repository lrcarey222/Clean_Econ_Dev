# ==============================================================================
# Geothermal-Adjacent Supply-Chain Analysis - Optimized for File Size
# Updated to include the full set of six-digit NAICS codes requested
# Industry titles are always taken from the `industry_desc` field in the CGT
# dataset, so no hard-coded names appear in this script.
# Restored: Industries Present, Comparative-Advantage Industries, and 
#           Complexity-Weighted Feasibility columns
# Added: Energy community and closure status columns
# Added: Complexity-Weighted RCA (based on RCA values)
# Added: High-complexity industries analysis
# REVISED: Highest-RCA Comparative-Advantage Industry now only considers top 3 complex industries
# REVISED: Added count columns to high-complexity industries analysis
# ADDED: Comprehensive industry summary including support industries
# ADDED: Automated Datawrapper chart generation for all central industries
# Creates six output files:
#   1. County-level analysis (wide format - full version)
#   2. County-level analysis (trimmed version without individual RCA/density scores)
#   3. Industry-level summary
#   4. County summary with energy community status
#   5. High-complexity industries analysis (includes presence/advantage counts)
#   6. Comprehensive industry summary including support industries
# Creates Datawrapper charts:
#   - One choropleth map for each of the 16 central industries showing comparative advantage
# ==============================================================================

# ------------------------------------------------------------------------------
# 1) Load libraries
# ------------------------------------------------------------------------------
library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(ggplot2)
library(DatawRappr)

# ------------------------------------------------------------------------------
# 2) Read in the data (specify the correct encoding if UTF-8 is wrong for you)
# ------------------------------------------------------------------------------
cgt_county_data <- read_csv(
  "~/Library/CloudStorage/OneDrive-RMI/US Program - Documents/6_Projects/Clean Regional Economic Development/ACRE/Data/CGT_county_data/modified_CGT_files/cgt_county_data_with_CD_and_EC_20250213_130456.csv",
  locale = locale(encoding = "UTF-8")   # change to "Latin1" or "windows-1252" if needed
)

# ------------------------------------------------------------------------------
# 3) Define the complete NAICS code set and check that all codes exist
# ------------------------------------------------------------------------------

# --- Original six codes --------------------------------------
original_codes <- c(
  333132, # Oil & Gas Field Machinery mfg
  331210, # Iron & Steel Pipe & Tube mfg
  334513, # Instruments & Related Products mfg
  333611, # Turbines & Generator Sets mfg
  332410, # Power Boiler & Heat Exchanger mfg
  333914  # Measuring, Dispensing & Other Pumping Equip. mfg
)

# --- Newly added geothermal-specific manufacturing codes (via CATF)----------------------
catf_suggested_additions <- c(
  334519, # Geophysical Instruments mfg
  333998, # Steam Separating Machinery mfg
  327310, # Cement mfg
  332911, # Industrial Valve mfg
  333415, # A/C, Warm-Air, & Commercial Refrigeration Equip. mfg
  334511, # Search, Detection, Navigation & Nautical Sys./Instr. mfg
  335311, # Power, Distribution & Specialty Transformer mfg
  335312, # Motor & Generator mfg
  335313, # Switchgear & Switchboard Apparatus mfg
  335314  # Relay & Industrial Control mfg
)

# --- Broader "stretch" supply-chain codes (via CATF) -------------------------------------
support_codes <- c(
  325180, 325199,                     # Inorganic & Organic Basic Chemicals mfg
  326220, 326291,                     # Rubber & Plastic Hoses/Belting; Rubber Prod. for Mech. Use
  332111, 332312, 332420, 332710,     # Forging; Structural Metal; Heavy-Gauge Tanks; Machine Shops
  332811, 332912, 332996,             # Metal Heat Treating; Fluid Power Valves; Fabricated Pipe
  333120,                             # Construction Machinery mfg
  334416, 334516,                     # Capacitors/Resistors/Coils; Analytical Instruments mfg
  335931                              # Current-Carrying Wiring Devices mfg
)

# --- Combine, deduplicate, and sort (exclude "stretch" codes for now) ------------------------------------------
relevant_codes <- sort(unique(c(original_codes, catf_suggested_additions)))

# --- Debug: confirm presence in the source data ------------------------------
missing_in_source <- setdiff(relevant_codes, unique(cgt_county_data$industry_code))
if (length(missing_in_source) == 0) {
  cat("All specified NAICS codes are present in the source data.\n")
} else {
  warning(
    "The following NAICS codes are missing from the source data: ",
    paste(missing_in_source, collapse = ", "), "\n"
  )
}

# ------------------------------------------------------------------------------
# 4) Calculate normalized PCI scores for complexity weighting
# ------------------------------------------------------------------------------
# Get unique industry PCI scores where aggregation_level is 4
industry_pci <- cgt_county_data %>%
  filter(aggregation_level == 4) %>%
  select(industry_code, industry_desc, pci) %>%
  distinct() %>%
  group_by(industry_code, industry_desc) %>%
  summarise(pci = mean(pci, na.rm = TRUE), .groups = "drop")

# Normalize PCI scores to 0-100 scale
min_pci <- min(industry_pci$pci, na.rm = TRUE)
max_pci <- max(industry_pci$pci, na.rm = TRUE)
industry_pci <- industry_pci %>%
  mutate(
    industry_complexity_normalized = (pci - min_pci) / (max_pci - min_pci) * 100
  )

# ------------------------------------------------------------------------------
# 4a) Identify the three highest-complexity industries
# ------------------------------------------------------------------------------
# Filter for our relevant industries and get top 3 by complexity
top3_complex_industries <- industry_pci %>%
  filter(industry_code %in% relevant_codes) %>%
  arrange(desc(industry_complexity_normalized)) %>%
  slice_head(n = 3)

# Print the three highest-complexity industries
cat("\nThree Highest-Complexity Geothermal-Adjacent Industries:\n")
cat("==================================================\n")
for (i in 1:nrow(top3_complex_industries)) {
  cat(sprintf("%d. %s (NAICS %d) - Complexity Score: %.2f\n",
              i,
              top3_complex_industries$industry_desc[i],
              top3_complex_industries$industry_code[i],
              top3_complex_industries$industry_complexity_normalized[i]))
}
cat("\n")

# Store the codes for later use
top3_complex_codes <- top3_complex_industries$industry_code

# ------------------------------------------------------------------------------
# 5) Filter CGT data for these NAICS codes
# ------------------------------------------------------------------------------
cgt_filtered_geothermal <- cgt_county_data %>%
  filter(industry_code %in% relevant_codes)

# Optional debug: make sure filtering worked
missing_after_filter <- setdiff(relevant_codes, unique(cgt_filtered_geothermal$industry_code))
if (length(missing_after_filter) > 0) {
  warning(
    "After filtering, these NAICS codes are still missing: ",
    paste(missing_after_filter, collapse = ", "), "\n"
  )
}

# ------------------------------------------------------------------------------
# 6) Add presence/advantage flags and round density percentile
# ------------------------------------------------------------------------------
cgt_filtered_geothermal <- cgt_filtered_geothermal %>%
  mutate(
    density_county_percentile_from_rank = round(density_county_percentile_from_rank, 2),
    presence  = rca != 0,
    advantage = rca >= 1
  )

# ------------------------------------------------------------------------------
# 6a) Normalize density scores (per industry_code)
# ------------------------------------------------------------------------------
cgt_filtered_geothermal <- cgt_filtered_geothermal %>%
  mutate(
    density_score_normalized_0_100 = NA_real_
  )

for (code in relevant_codes) {
  # Normalize density scores to 0-100 ------------------------------------------
  idx_density <- cgt_filtered_geothermal$industry_code == code & !is.na(cgt_filtered_geothermal$density)
  if (sum(idx_density) > 0) {
    density_values <- cgt_filtered_geothermal$density[idx_density]
    min_density <- min(density_values, na.rm = TRUE)
    max_density <- max(density_values, na.rm = TRUE)
    
    if (max_density > min_density) {
      cgt_filtered_geothermal$density_score_normalized_0_100[idx_density] <- 
        round(((density_values - min_density) / (max_density - min_density)) * 100, 2)
    } else {
      # If all values are the same, set to 50
      cgt_filtered_geothermal$density_score_normalized_0_100[idx_density] <- 50
    }
  }
}

# ------------------------------------------------------------------------------
# 7) Add complexity scores to filtered data
# ------------------------------------------------------------------------------
cgt_filtered_geothermal <- cgt_filtered_geothermal %>%
  left_join(
    industry_pci %>% select(industry_code, industry_complexity_normalized),
    by = "industry_code"
  )

# ------------------------------------------------------------------------------
# 8) County-level summary with requested metrics including energy community status
# ------------------------------------------------------------------------------
cgt_county_summary <- cgt_filtered_geothermal %>%
  group_by(
    State_Name, County_Name, county_geoid, county_name,
    `119th Congressional District(s)`,
    ffe_qual_status, ec_qual_status,
    contains_tracts_with_mine_closure,
    contains_tracts_with_generator_closure,
    contains_tracts_adjacent_to_closure
  ) %>%
  summarise(
    `Industry Presence Count`           = sum(presence,   na.rm = TRUE),
    `Industries Present` = {
      out <- str_c(
        unique(str_c(industry_desc[presence], " (NAICS ", industry_code[presence], ")")),
        collapse = "; "
      ); ifelse(out == "", NA_character_, out)
    },
    
    `Industry Comparative Advantage Count` = sum(advantage, na.rm = TRUE),
    `Comparative-Advantage Industries` = {
      out <- str_c(
        unique(str_c(industry_desc[advantage], " (NAICS ", industry_code[advantage], ")")),
        collapse = "; "
      ); ifelse(out == "", NA_character_, out)
    },
    
    `Highest-Feasibility-Percentile` = max(density_county_percentile_from_rank, na.rm = TRUE),
    `Highest-Feasibility Industry`      = {
      i <- which.max(density_county_percentile_from_rank)
      if (length(i) && !all(is.na(density_county_percentile_from_rank))) {
        str_c(industry_desc[i], " (NAICS ", industry_code[i], ")")
      } else NA_character_
    },
    
    # Calculate complexity-weighted feasibility (based on density)
    `Complexity-Weighted Feasibility` = {
      # Weighted average of density using normalized complexity scores
      weights <- industry_complexity_normalized[!is.na(density) & !is.na(industry_complexity_normalized)]
      values <- density[!is.na(density) & !is.na(industry_complexity_normalized)]
      if (length(weights) > 0 && sum(weights) > 0) {
        sum(values * weights) / sum(weights)
      } else {
        NA_real_
      }
    },
    
    # Calculate complexity-weighted comparative advantage (based on M)
    `Complexity-Weighted Comparative Advantage` = {
      # Weighted average of M (employment/count) using normalized complexity scores
      weights <- industry_complexity_normalized[!is.na(M) & !is.na(industry_complexity_normalized)]
      values <- M[!is.na(M) & !is.na(industry_complexity_normalized)]
      if (length(weights) > 0 && sum(weights) > 0) {
        sum(values * weights) / sum(weights)
      } else {
        NA_real_
      }
    },
    
    # Calculate complexity-weighted RCA
    `Complexity-Weighted RCA` = {
      # Weighted average of RCA using normalized complexity scores
      weights <- industry_complexity_normalized[!is.na(rca) & !is.na(industry_complexity_normalized)]
      values <- rca[!is.na(rca) & !is.na(industry_complexity_normalized)]
      if (length(weights) > 0 && sum(weights) > 0) {
        sum(values * weights) / sum(weights)
      } else {
        NA_real_
      }
    },
    
    .groups = "drop"
  ) %>%
  # Add normalized Comparative Advantage Score
  mutate(
    `Comparative-Advantage-Score-Normalized` = {
      # Normalize Industry Comparative Advantage Count to 0-100 scale
      min_count <- min(`Industry Comparative Advantage Count`, na.rm = TRUE)
      max_count <- max(`Industry Comparative Advantage Count`, na.rm = TRUE)
      if (max_count > min_count) {
        round(((`Industry Comparative Advantage Count` - min_count) / (max_count - min_count)) * 100, 2)
      } else {
        50  # If all values are the same
      }
    }
  ) %>%
  # Add Complexity-Weighted Feasibility Rank and Percentile
  mutate(
    `Complexity-Weighted Feasibility Rank` = rank(desc(`Complexity-Weighted Feasibility`), 
                                                  ties.method = "min", 
                                                  na.last = TRUE),
    `Complexity-Weighted Feasibility Percentile` = round(
      (rank(`Complexity-Weighted Feasibility`, ties.method = "average", na.last = FALSE) / 
         sum(!is.na(`Complexity-Weighted Feasibility`))) * 100, 
      2
    )
  )

# ------------------------------------------------------------------------------
# 8a) Create high-complexity industries analysis
# ------------------------------------------------------------------------------
high_complexity_analysis <- cgt_filtered_geothermal %>%
  group_by(county_geoid, County_Name, State_Name, county_name) %>%
  summarise(
    # Count number of high-complexity industries present
    `Number of High-Complexity Industries Present` = sum(
      industry_code %in% top3_complex_codes & presence, 
      na.rm = TRUE
    ),
    
    # Count number of high-complexity industries with comparative advantage
    `Number of High-Complexity Comparative-Advantage Industries` = sum(
      industry_code %in% top3_complex_codes & advantage, 
      na.rm = TRUE
    ),
    
    # Check presence in the three high-complexity industries
    `Selected Industries Present` = ifelse(
      any(industry_code %in% top3_complex_codes & presence), "Yes", "No"
    ),
    
    # Check comparative advantage in the three high-complexity industries
    `Comparative Advantage Present in Selected Industries` = ifelse(
      any(industry_code %in% top3_complex_codes & advantage), "Yes", "No"
    ),
    
    # Find highest RCA industry overall
    `Highest-RCA Industry` = {
      if (all(is.na(rca)) || all(rca == 0)) {
        NA_character_
      } else {
        i <- which.max(rca)
        str_c(industry_desc[i], " (NAICS ", industry_code[i], ")")
      }
    },
    
    # Find highest RCA industry among the top 3 complex industries with comparative advantage
    `Highest-RCA Comparative-Advantage Industry` = {
      # Only consider industries that are BOTH in top 3 complex AND have comparative advantage
      top3_advantage_idx <- which(industry_code %in% top3_complex_codes & advantage)
      if (length(top3_advantage_idx) == 0) {
        NA_character_
      } else {
        i <- top3_advantage_idx[which.max(rca[top3_advantage_idx])]
        str_c(industry_desc[i], " (NAICS ", industry_code[i], ")")
      }
    },
    
    # Create detailed description of comparative advantages in selected industries
    `Comparative Advantage Present in Selected Industries (Detail)` = {
      # Check which of the top 3 complex industries have comparative advantage
      has_advantage <- industry_code %in% top3_complex_codes & advantage
      
      if (!any(has_advantage)) {
        "None"
      } else {
        # Get the names of industries with advantage
        advantage_industries <- unique(industry_desc[has_advantage])
        n_advantages <- length(advantage_industries)
        
        if (n_advantages == 1) {
          str_c(advantage_industries, " only")
        } else if (n_advantages == 2) {
          str_c(advantage_industries[1], " and ", advantage_industries[2])
        } else if (n_advantages == 3) {
          "All three high-complexity industries"
        }
      }
    },
    
    .groups = "drop"
  )

# ------------------------------------------------------------------------------
# 9) Pivot the detailed industry metrics wide
# ------------------------------------------------------------------------------
cgt_geothermal_wide <- cgt_filtered_geothermal %>%
  select(
    State_Name, County_Name, county_geoid,
    `119th Congressional District(s)`,
    industry_desc, industry_code,
    rca, density_score_normalized_0_100, density_county_percentile_from_rank,
    presence, advantage
  ) %>%
  distinct() %>%
  pivot_wider(
    id_cols = c(
      State_Name, County_Name, county_geoid,
      `119th Congressional District(s)`
    ),
    names_from  = c(industry_desc, industry_code),
    names_glue  = "{industry_desc} (NAICS {industry_code}): {.value}",
    values_from = c(
      rca, density_score_normalized_0_100, density_county_percentile_from_rank,
      presence, advantage
    )
  ) %>%
  {  # rename presence/advantage columns for readability ------------------------
    colnames(.) <- sub(": presence$",  ": Industry Presence in County",             colnames(.))
    colnames(.) <- sub(": advantage$", ": Existing Comparative Advantage for County", colnames(.))
    .
  }

# ------------------------------------------------------------------------------
# 10) Combine the summary and wide datasets
# ------------------------------------------------------------------------------
final_cgt_geothermal <- cgt_county_summary %>%
  left_join(
    cgt_geothermal_wide,
    by = c(
      "State_Name", "County_Name", "county_geoid",
      "119th Congressional District(s)"
    )
  )

# ------------------------------------------------------------------------------
# 10a) Create trimmed version without individual RCA and density scores
# ------------------------------------------------------------------------------
# Select only the columns we want to keep
final_cgt_geothermal_trimmed <- final_cgt_geothermal %>%
  select(
    # Keep all summary columns including energy community status
    State_Name, County_Name, county_geoid, county_name,
    `119th Congressional District(s)`,
    ffe_qual_status, ec_qual_status,
    contains_tracts_with_mine_closure,
    contains_tracts_with_generator_closure,
    contains_tracts_adjacent_to_closure,
    `Industry Presence Count`,
    `Industries Present`,
    `Industry Comparative Advantage Count`,
    `Comparative-Advantage Industries`,
    `Highest-Feasibility-Percentile`,
    `Highest-Feasibility Industry`,
    `Complexity-Weighted Feasibility`,
    `Complexity-Weighted Feasibility Rank`,
    `Complexity-Weighted Feasibility Percentile`,
    `Complexity-Weighted Comparative Advantage`,
    `Complexity-Weighted RCA`,
    `Comparative-Advantage-Score-Normalized`,
    # Keep only presence and advantage columns for each industry
    contains("Industry Presence in County"),
    contains("Existing Comparative Advantage for County")
  )

# ------------------------------------------------------------------------------
# 10b) Create separate county energy community status export
# ------------------------------------------------------------------------------
county_energy_status_export <- cgt_county_summary %>%
  select(
    county_name,
    `Industry Comparative Advantage Count`,
    `Industry Presence Count`,
    ffe_qual_status,
    ec_qual_status
  ) %>%
  arrange(desc(`Industry Comparative Advantage Count`), desc(`Industry Presence Count`))

# ------------------------------------------------------------------------------
# 11) Create industry-level summary table
# ------------------------------------------------------------------------------
# First, create a helper dataset with top 10 RCA counties for each industry
top_rca_by_industry <- cgt_filtered_geothermal %>%
  filter(rca > 0, !is.na(rca)) %>%
  arrange(industry_code, desc(rca)) %>%
  group_by(industry_code) %>%
  slice_head(n = 10) %>%
  summarise(
    `Top 10 Highest-RCA Counties` = str_c(
      str_c(County_Name, ", ", State_Name), 
      collapse = "; "
    ),
    .groups = "drop"
  )

# Create a dataset with state-level concentration statistics
# Percentage = (counties with M >= 1 in state / total counties in state) × 100
state_concentration_stats <- cgt_filtered_geothermal %>%
  group_by(industry_code, State_Name) %>%
  summarise(
    high_concentration_counties = sum(M >= 1, na.rm = TRUE),
    total_counties = n_distinct(county_geoid),
    .groups = "drop"
  ) %>%
  mutate(
    percentage = (high_concentration_counties / total_counties) * 100
  ) %>%
  filter(high_concentration_counties > 0) %>%  # Only keep states with at least one high-concentration county
  arrange(industry_code, desc(percentage))

# Get the state with highest percentage for each industry
highest_pct_state_by_industry <- state_concentration_stats %>%
  group_by(industry_code) %>%
  slice_head(n = 1) %>%
  mutate(
    `State with Highest % of High-Concentration Counties` = str_c(
      State_Name, " (", round(percentage, 1), "%)"
    )
  ) %>%
  select(industry_code, `State with Highest % of High-Concentration Counties`)

# Get top 5 states with highest percentage for each industry
top5_pct_states_by_industry <- state_concentration_stats %>%
  group_by(industry_code) %>%
  slice_head(n = 5) %>%
  summarise(
    `Top 5 States by % of High-Concentration Counties` = str_c(
      str_c(State_Name, " (", round(percentage, 1), "%)"),
      collapse = "; "
    ),
    .groups = "drop"
  )

# Now create the main industry summary
industry_summary <- cgt_filtered_geothermal %>%
  group_by(industry_code, industry_desc) %>%
  summarise(
    # Count counties where M >= 1 (High-Concentration Counties)
    `High-Concentration Counties` = sum(M >= 1, na.rm = TRUE),
    
    # Get the industry complexity score (should be the same for all counties in an industry)
    `Industry Complexity Score` = round(first(industry_complexity_normalized), 2),
    
    .groups = "drop"
  ) %>%
  # Join with all the additional statistics
  left_join(top_rca_by_industry, by = "industry_code") %>%
  left_join(highest_pct_state_by_industry, by = "industry_code") %>%
  left_join(top5_pct_states_by_industry, by = "industry_code") %>%
  # Create the full industry name with NAICS code
  mutate(
    Industry = str_c(industry_desc, " (NAICS ", industry_code, ")")
  ) %>%
  # Select and reorder columns
  select(
    Industry,
    `High-Concentration Counties`,
    `Industry Complexity Score`,
    `State with Highest % of High-Concentration Counties`,
    `Top 5 States by % of High-Concentration Counties`,
    `Top 10 Highest-RCA Counties`
  ) %>%
  # Sort by industry code for consistent ordering
  arrange(Industry)

# ------------------------------------------------------------------------------
# 11a) Inspect the industry summary table (optional)
# ------------------------------------------------------------------------------
glimpse(industry_summary)
# Should have 16 rows (one per industry) and 6 columns
print(industry_summary, n = 16)

# ------------------------------------------------------------------------------
# 12) Inspect the final county datasets (optional)
# ------------------------------------------------------------------------------
glimpse(final_cgt_geothermal)
# Should have approximately 102 columns: 22 summary columns + 80 industry-specific columns

glimpse(final_cgt_geothermal_trimmed)
# Should have approximately 54 columns: 22 summary columns + 32 presence/advantage columns

glimpse(county_energy_status_export)
# Should have 3,141 rows and 5 columns

glimpse(high_complexity_analysis)
# Should have 3,141 rows and 10 columns

# ------------------------------------------------------------------------------
# 13) Create comprehensive industry analysis including support industries
# ------------------------------------------------------------------------------
# Combine all industry codes (central + support)
all_industry_codes <- sort(unique(c(relevant_codes, support_codes)))

# Filter CGT data for ALL codes (central + support)
cgt_all_industries <- cgt_county_data %>%
  filter(industry_code %in% all_industry_codes)

# Add presence and advantage flags
cgt_all_industries <- cgt_all_industries %>%
  mutate(
    presence = rca != 0,
    advantage = rca >= 1
  )

# Create comprehensive industry summary
comprehensive_industry_summary <- cgt_all_industries %>%
  group_by(industry_code, industry_desc) %>%
  summarise(
    # Count counties with any presence
    `# of Counties with Any Industry Presence` = sum(presence, na.rm = TRUE),
    
    # Count counties with comparative advantage
    `# of Counties with Comparative Advantage` = sum(advantage, na.rm = TRUE),
    
    .groups = "drop"
  ) %>%
  # Add PCI and normalized complexity scores
  left_join(
    industry_pci %>% select(industry_code, pci, industry_complexity_normalized),
    by = "industry_code"
  ) %>%
  # Add category column
  mutate(
    Category = case_when(
      industry_code %in% relevant_codes ~ "Central Industry",
      industry_code %in% support_codes ~ "Support Industry",
      TRUE ~ "Unknown"
    )
  ) %>%
  # Ensure title case for industry descriptions
  mutate(
    industry_desc = str_to_title(industry_desc)
  ) %>%
  # Select and order columns as requested
  select(
    industry_desc,
    industry_code,
    pci,
    Category,
    industry_complexity_normalized,
    `# of Counties with Any Industry Presence`,
    `# of Counties with Comparative Advantage`
  ) %>%
  # Sort by category (Central first) then by industry code
  arrange(desc(Category), industry_code)

# ------------------------------------------------------------------------------
# 13a) Inspect the comprehensive industry summary (optional)
# ------------------------------------------------------------------------------
glimpse(comprehensive_industry_summary)
# Should have 33 rows (16 central + 17 support industries) and 7 columns
print(comprehensive_industry_summary, n = 33)

# ------------------------------------------------------------------------------
# 14) Write all six datasets to disk
# ------------------------------------------------------------------------------
# Full county-level analysis (3,141 counties × ~102 columns)
write_csv(final_cgt_geothermal, "cgt_geothermal_wide_plus_summary_with_industry_lists.csv")

# Trimmed county-level analysis (3,141 counties × ~54 columns)
write_csv(final_cgt_geothermal_trimmed, "cgt_geothermal_wide_trimmed.csv")

# County energy community status summary (3,141 counties × 5 columns)
write_csv(county_energy_status_export, "cgt_geothermal_county_energy_status.csv")

# High-complexity industries analysis (3,141 counties × 10 columns)
write_csv(high_complexity_analysis, "cgt_geothermal_high_complexity_industries.csv")

# Industry-level summary (16 industries × 6 columns)
write_csv(industry_summary, "cgt_geothermal_industry_summary.csv")

# Comprehensive industry summary including support industries (33 industries × 7 columns)
write_csv(comprehensive_industry_summary, "cgt_geothermal_comprehensive_industry_summary.csv")

# ------------------------------------------------------------------------------
# 15) Create Datawrapper charts for all central industries
# ------------------------------------------------------------------------------
# First, verify the initial chart setup
cat("\nVerifying initial Datawrapper chart (vB0aj)...\n")
chart_metadata <- dw_retrieve_chart_metadata("vB0aj", api_key = "environment")
cat("Chart type:", chart_metadata$type, "\n")
cat("Current title:", chart_metadata$title, "\n")
cat("Folder ID:", chart_metadata$folderId, "\n\n")

# Get the data structure from the template chart
template_data <- dw_data_from_chart("vB0aj", api_key = "environment")
cat("Template data dimensions:", nrow(template_data), "rows,", ncol(template_data), "columns\n\n")

# Prepare data for Datawrapper charts
# We need to select only the columns that are relevant for the maps
dw_chart_data <- final_cgt_geothermal_trimmed %>%
  select(
    county_geoid,
    State_Name,
    county_name,
    `119th Congressional District(s)`,
    `Industry Presence Count`,
    `Industries Present`,
    `Industry Comparative Advantage Count`,
    `Comparative-Advantage Industries`,
    # Include all the advantage columns
    contains("Existing Comparative Advantage for County")
  )

# Create a chart for each central industry
cat("Creating Datawrapper charts for all central industries...\n")
cat("==================================================\n")

# Get industry information for chart creation
industry_info <- industry_pci %>%
  filter(industry_code %in% relevant_codes) %>%
  select(industry_code, industry_desc) %>%
  arrange(industry_code)

# Initialize list to store created charts
created_charts <- list()

# Track which industry was used as template
template_industry_code <- 333611  # Based on the original chart title

for (i in 1:nrow(industry_info)) {
  industry_code <- industry_info$industry_code[i]
  industry_desc <- industry_info$industry_desc[i]
  
  # Create column name for this industry
  advantage_col <- paste0(industry_desc, " (NAICS ", industry_code, "): Existing Comparative Advantage for County")
  
  cat(sprintf("\n%d. Creating chart for: %s (NAICS %d)\n", i, industry_desc, industry_code))
  
  # Prepare data for this specific industry
  chart_specific_data <- dw_chart_data %>%
    select(
      county_geoid,
      State_Name, 
      county_name,
      `119th Congressional District(s)`,
      `Industry Presence Count`,
      `Industries Present`,
      `Industry Comparative Advantage Count`,
      `Comparative-Advantage Industries`,
      !!advantage_col
    )
  
  # Create new chart
  if (industry_code == template_industry_code) {
    # For the template industry, we still need to create a proper chart
    # since we'll rename the template later
    new_chart <- dw_copy_chart(
      copy_from = "vB0aj",
      api_key = "environment"
    )
  } else {
    # Copy the template chart
    new_chart <- dw_copy_chart(
      copy_from = "vB0aj",
      api_key = "environment"
    )
  }
  
  cat("  - Created new chart:", new_chart$id, "\n")
  
  # Update the data
  dw_data_to_chart(
    chart_specific_data,
    chart_id = new_chart$id,
    api_key = "environment"
  )
  
  cat("  - Updated data\n")
  
  # Update the chart metadata
  new_title <- paste0(industry_desc, " (NAICS ", industry_code, "): County-Level Employment-Based Comparative Advantage")
  
  dw_edit_chart(
    chart_id = new_chart$id,
    title = new_title,
    axes = list(
      keys = "county_geoid",
      values = advantage_col
    ),
    folderId = chart_metadata$folderId,  # Use same folder as template
    api_key = "environment"
  )
  
  cat("  - Updated metadata\n")
  
  # Update visualization settings to match the template
  dw_edit_chart(
    chart_id = new_chart$id,
    visualize = list(
      `map-key-attr` = "county_geoid",
      tooltip = list(
        title = "<big> <big> {{ county_name }}, {{ State_Name }}",
        enabled = TRUE
      )
    ),
    api_key = "environment"
  )
  
  # Publish the chart
  published_info <- dw_publish_chart(
    chart_id = new_chart$id,
    api_key = "environment",
    return_urls = TRUE
  )
  
  cat("  - Published chart\n")
  
  # Store the chart info - handle different possible response structures
  chart_url <- if (!is.null(published_info$publicUrl)) {
    published_info$publicUrl
  } else if (!is.null(published_info$url)) {
    published_info$url
  } else {
    paste0("https://datawrapper.dwcdn.net/", new_chart$id, "/1/")
  }
  
  created_charts[[i]] <- data.frame(
    industry_code = industry_code,
    industry_desc = industry_desc,
    chart_id = new_chart$id,
    url = chart_url,
    stringsAsFactors = FALSE
  )
  
  # Small delay to avoid API rate limits
  Sys.sleep(1)
}

# Create a summary of created charts
if (length(created_charts) > 0) {
  charts_summary <- do.call(rbind, created_charts)
  write_csv(charts_summary, "cgt_geothermal_datawrapper_charts_summary.csv")
  
  cat("\n==================================================\n")
  cat("Successfully created", nrow(charts_summary), "Datawrapper charts\n")
  cat("Chart summary saved to: cgt_geothermal_datawrapper_charts_summary.csv\n")
} else {
  cat("\nNo charts were created successfully.\n")
}

# ------------------------------------------------------------------------------
# 16) Rename the template chart to "TEMPLATE MAP"
# ------------------------------------------------------------------------------
cat("\nRenaming template chart to 'TEMPLATE MAP'...\n")

dw_edit_chart(
  chart_id = "vB0aj",
  title = "TEMPLATE MAP",
  api_key = "environment"
)

# Republish the template with new name
dw_publish_chart(
  chart_id = "vB0aj",
  api_key = "environment"
)

cat("Template chart renamed successfully.\n")

# ------------------------------------------------------------------------------
# End of Script
# ------------------------------------------------------------------------------
