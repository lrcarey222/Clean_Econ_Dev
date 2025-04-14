#!/usr/bin/env Rscript
###############################################################################
# TEMPLATE R SCRIPT FOR OS DETECTION AND BASE PATH SETUP
#
# This script automatically detects whether it is running on a Windows PC 
# or on a Mac/Linux system and then sets the "base_path" variable 
# accordingly. You can adapt the paths below as needed.
###############################################################################

# Retrieve the operating system name
os_type <- Sys.info()[["sysname"]]

# OS-specific base_path setup
if (grepl("windows", tolower(os_type))) {
  # Running on Windows
  # Get the current Windows username
  user_name <- Sys.getenv("USERNAME")
  
  # If the username is detected, use it to create a dynamic path
  if (nzchar(user_name)) {
    base_path <- file.path(
      "C:", "Users", user_name, "OneDrive - RMI", "Documents - US Program",
      "6_Projects", "Clean Regional Economic Development", "ACRE", "Data"
    )
  } else {
    # Fallback: replace 'YourUser' with your actual username if needed
    base_path <- file.path(
      "C:", "Users", "YourUser", "OneDrive - RMI", "Documents - US Program",
      "6_Projects", "Clean Regional Economic Development", "ACRE", "Data"
    )
  }
} else {
  # Running on macOS (Darwin) or Linux
  base_path <- "~/Library/CloudStorage/OneDrive-RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Data"
}

# Output the determined base_path (for verification purposes)
cat("Operating System Detected:", os_type, "\n")
cat("Base Path set to:", base_path, "\n\n")

###############################################################################
# ADDITIONAL SCRIPT CONTENT BELOW
#
# You may now use 'base_path' in your script for reading/writing data,
# setting working directories, etc.
#
# Example: setting the working directory
#setwd(base_path)
#
# Continue adding your analysis code below.
###############################################################################
