library(tidyverse)
library(data.table)

# File paths
file_path <- "/Users/williamwatson/Library/CloudStorage/OneDrive-UniversityofArkansasforMedicalSciences/Pain_Project/Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2023/Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2023.csv"
mylib_path <- "/Users/williamwatson/Library/CloudStorage/OneDrive-UniversityofArkansasforMedicalSciences/Pain_Project/"

#Sample NPPES Data----
# Read first 100,000 rows
cms_pain_prov <- fread("/Users/williamwatson/Library/CloudStorage/OneDrive-UniversityofArkansasforMedicalSciences/Pain_Project/Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2023/Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2023.csv")

pain_provs_slim <- cms_pain_prov %>% 
  select(c(Rndrng_NPI, Rndrng_Prvdr_Type, HCPCS_Cd, HCPCS_Desc, Tot_Srvcs, Place_Of_Srvc))

# rm(cms_pain_prov)


# Define top HCPCS codes for Pain Management and Interventional Pain Management
pm_top10_codes <- c("64493", "64494", "64483", "62323", "27096", 
                    "64635", "20610", "64636", "62321", "64484")

pm_top20_codes <- c("64493", "64494", "64483", "62323", "27096", 
                    "64635", "20610", "64636", "62321", "64484",
                    "64490", "64491", "J1100", "20553", "J1030",
                    "J3301", "99152", "80307", "64633", "64495")

ipm_top10_codes <- c("64493", "64494", "64483", "62323", "64635",
                     "64636", "27096", "20610", "64484", "62321")

ipm_top20_codes <- c("64493", "64494", "64483", "62323", "64635",
                     "64636", "27096", "20610", "64484", "62321",
                     "64490", "64491", "J1100", "J1030", "64633",
                     "J3301", "80307", "99152", "64634", "64495")

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
  ) %>%
  # Add binary indicators for top procedure codes
  mutate(
    pm_top10_indicator = as.integer(HCPCS_Cd %in% pm_top10_codes),
    pm_top20_indicator = as.integer(HCPCS_Cd %in% pm_top20_codes),
    ipm_top10_indicator = as.integer(HCPCS_Cd %in% ipm_top10_codes),
    ipm_top20_indicator = as.integer(HCPCS_Cd %in% ipm_top20_codes),
    Tot_Srvcs = as.numeric(gsub(",", "", Tot_Srvcs))
  )

# Calculate total providers by type first
provider_counts <- pain_provs_no_EM %>%
  distinct(Rndrng_NPI, Rndrng_Prvdr_Type) %>%
  count(Rndrng_Prvdr_Type, name = "total_providers_in_type")

# Group by provider type and HCPCS code
pain_procs_by_type <- pain_provs_no_EM %>% 
  group_by(Rndrng_Prvdr_Type, HCPCS_Cd, HCPCS_Desc) %>% 
  summarise(
    total_services = sum(Tot_Srvcs, na.rm = TRUE),
    unique_providers = n_distinct(Rndrng_NPI),
    pm_top10_services = sum(Tot_Srvcs * pm_top10_indicator, na.rm = TRUE),
    pm_top20_services = sum(Tot_Srvcs * pm_top20_indicator, na.rm = TRUE),
    ipm_top10_services = sum(Tot_Srvcs * ipm_top10_indicator, na.rm = TRUE),
    ipm_top20_services = sum(Tot_Srvcs * ipm_top20_indicator, na.rm = TRUE),
    .groups = 'drop'
  ) %>% 
  # Join with provider counts
  left_join(provider_counts, by = "Rndrng_Prvdr_Type") %>%
  mutate(
    # Method 1: Percentage of providers who perform this procedure
    pct_providers_performing = round((unique_providers / total_providers_in_type) * 100, 2)
  )

# Create summary by specialty showing top procedure usage
specialty_top_procedure_summary <- pain_provs_no_EM %>%
  group_by(Rndrng_Prvdr_Type) %>%
  summarise(
    total_services = sum(Tot_Srvcs, na.rm = TRUE),
    pm_top10_services = sum(Tot_Srvcs * pm_top10_indicator, na.rm = TRUE),
    pm_top20_services = sum(Tot_Srvcs * pm_top20_indicator, na.rm = TRUE),
    ipm_top10_services = sum(Tot_Srvcs * ipm_top10_indicator, na.rm = TRUE),
    ipm_top20_services = sum(Tot_Srvcs * ipm_top20_indicator, na.rm = TRUE),
    .groups = 'drop'
  ) %>%
  mutate(
    pm_top10_pct = round((pm_top10_services / total_services) * 100, 2),
    pm_top20_pct = round((pm_top20_services / total_services) * 100, 2),
    ipm_top10_pct = round((ipm_top10_services / total_services) * 100, 2),
    ipm_top20_pct = round((ipm_top20_services / total_services) * 100, 2)
  )

#Save Procs
fwrite(specialty_top_procedure_summary, file.path(mylib_path, "speciality_summary.csv"))


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