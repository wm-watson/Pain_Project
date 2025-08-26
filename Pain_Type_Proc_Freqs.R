library(tidyverse)
library(data.table)

# File paths
file_path <- "C:/Users/watso/OneDrive - University of Arkansas for Medical Sciences/Pain_Project/NPPES_Data_Dissemination_August_2025_V2/npidata_pfile_20050523-20250810.csv"
mylib_path <- "C:/Users/watso/OneDrive - University of Arkansas for Medical Sciences/Pain_Project/"

#Sample NPPES Data----
# Read first 100,000 rows
cms_pain_prov <- fread("C:/Users/watso/OneDrive - University of Arkansas for Medical Sciences/Pain_Project/Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2021/mdcr_pain_provs_2021.csv")

pain_provs_slim <- cms_pain_prov %>% 
  select(c(Rndrng_NPI, Rndrng_Prvdr_Type, HCPCS_Cd, HCPCS_Desc, Tot_Srvcs, Place_Of_Srvc))

# rm(cms_pain_prov)

pain_provs_no_EM <- pain_provs_slim %>% 
  filter(c)

#Remove E&M Codes, G-Codes, and Imaging codes except 99152/99153
pain_provs_no_EM <- pain_provs_slim %>% 
  filter(
    # Remove office visit E&M codes
    !HCPCS_Cd %in% c("99201", "99202", "99203", "99204", "99205",
                     "99211", "99212", "99213", "99214", "99215",
                     "99221", "99222", "99223", "99231", "99232", "99233",
                     "99238", "99239"),
    # Remove all other E&M codes except 99152 and 99153
    !(grepl("^99", HCPCS_Cd) & !HCPCS_Cd %in% c("99152", "99153")),
    # Remove G-codes
    !grepl("^G", HCPCS_Cd),
    # Remove imaging 7000 series codes
    !grepl("^7[0-9]{4}$", HCPCS_Cd)
  )

# Calculate total providers by type first
provider_counts <- pain_provs_no_EM %>%
  distinct(Rndrng_NPI, Rndrng_Prvdr_Type) %>%
  count(Rndrng_Prvdr_Type, name = "total_providers_in_type")

# Group by provider type and HCPCS code
pain_procs_by_type <- pain_provs_no_EM %>% 
  mutate(Tot_Srvcs = as.numeric(gsub(",", "", Tot_Srvcs))) %>% 
  group_by(Rndrng_Prvdr_Type, HCPCS_Cd, HCPCS_Desc) %>% 
  summarise(
    total_services = sum(Tot_Srvcs, na.rm = TRUE),
    unique_providers = n_distinct(Rndrng_NPI),
    .groups = 'drop'
  ) %>% 
  # Join with provider counts
  left_join(provider_counts, by = "Rndrng_Prvdr_Type") %>%
  group_by(Rndrng_Prvdr_Type) %>%
  mutate(
    # Method 1: Percentage of providers who perform this procedure
    pct_providers_performing = round((unique_providers / total_providers_in_type) * 100, 2),
    # Method 2: Percentage of total services
    pct_of_total_services = round((total_services / sum(total_services)) * 100, 2)
  ) %>% 
  ungroup()

# Split into separate datasets by provider type
pain_mgmt_procs <- pain_procs_by_type %>%
  filter(Rndrng_Prvdr_Type == "Pain Management")

interventional_pain_procs <- pain_procs_by_type %>%
  filter(Rndrng_Prvdr_Type == "Interventional Pain Management")

# Top 25/50 by percentage of providers performing
pain_mgmt_top25_by_providers <- pain_mgmt_procs %>%
  arrange(desc(pct_providers_performing)) %>%
  slice_head(n = 25)

pain_mgmt_top50_by_providers <- pain_mgmt_procs %>%
  arrange(desc(pct_providers_performing)) %>%
  slice_head(n = 50)

interventional_top25_by_providers <- interventional_pain_procs %>%
  arrange(desc(pct_providers_performing)) %>%
  slice_head(n = 25)

interventional_top50_by_providers <- interventional_pain_procs %>%
  arrange(desc(pct_providers_performing)) %>%
  slice_head(n = 50)

# Top 25/50 by percentage of total services
pain_mgmt_top25_by_services <- pain_mgmt_procs %>%
  arrange(desc(pct_of_total_services)) %>%
  slice_head(n = 25)

pain_mgmt_top50_by_services <- pain_mgmt_procs %>%
  arrange(desc(pct_of_total_services)) %>%
  slice_head(n = 50)

interventional_top25_by_services <- interventional_pain_procs %>%
  arrange(desc(pct_of_total_services)) %>%
  slice_head(n = 25)

interventional_top50_by_services <- interventional_pain_procs %>%
  arrange(desc(pct_of_total_services)) %>%
  slice_head(n = 50)


# # Check for non-numeric values
# unique(pain_procs$Tot_Srvcs[!grepl("^[0-9]+$", pain_procs$Tot_Srvcs)])

#Save Procs
fwrite(pain_procs, file.path(mylib_path, "pain_procs.csv"))

# Count unique NPIs by provider type
pain_provs_no_EM %>%
  distinct(Rndrng_NPI, Rndrng_Prvdr_Type) %>%
  count(Rndrng_Prvdr_Type)

# Load the package (install if needed: install.packages("openxlsx"))
library(openxlsx)

# Create a workbook
wb <- createWorkbook()

# Add worksheets and data for each dataset
addWorksheet(wb, "PM_Top25_Providers")
writeData(wb, "PM_Top25_Providers", pain_mgmt_top25_by_providers)

addWorksheet(wb, "PM_Top50_Providers") 
writeData(wb, "PM_Top50_Providers", pain_mgmt_top50_by_providers)

addWorksheet(wb, "PM_Top25_Services")
writeData(wb, "PM_Top25_Services", pain_mgmt_top25_by_services)

addWorksheet(wb, "PM_Top50_Services")
writeData(wb, "PM_Top50_Services", pain_mgmt_top50_by_services)

addWorksheet(wb, "IPM_Top25_Providers")
writeData(wb, "IPM_Top25_Providers", interventional_top25_by_providers)

addWorksheet(wb, "IPM_Top50_Providers")
writeData(wb, "IPM_Top50_Providers", interventional_top50_by_providers)

addWorksheet(wb, "IPM_Top25_Services")
writeData(wb, "IPM_Top25_Services", interventional_top25_by_services)

addWorksheet(wb, "IPM_Top50_Services")
writeData(wb, "IPM_Top50_Services", interventional_top50_by_services)

# Save the workbook
saveWorkbook(wb, file.path(mylib_path, "pain_specialist_procedures_analysis.xlsx"), overwrite = TRUE)

cat("All eight datasets saved to pain_specialist_procedures_analysis.xlsx with separate tabs\n")