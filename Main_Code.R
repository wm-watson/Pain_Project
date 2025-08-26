library(tidyverse)
library(data.table)

#Sample NPPES Data----
# Read first 100,000 rows
npi_sample <- fread("C:/Users/watso/OneDrive - University of Arkansas for Medical Sciences/Pain_Project/NPPES_Data_Dissemination_August_2025_V2/npidata_pfile_20050523-20250810.csv", 
                    nrows = 100000)


#Get All Pain Specialists from full sample----
# Define pain specialist taxonomy codes and descriptions
pain_codes <- data.table(
  code = c("208VP0000X", "208VP0014X", "2084P0800X", "207VN0000X"),
  description = c("Pain Medicine (Anesthesiology)", 
                  "Pain Medicine (Physical Medicine & Rehabilitation)",
                  "Pain Medicine (Psychiatry & Neurology)", 
                  "Pain Medicine (Neurology)")
)

# File paths
file_path <- "C:/Users/watso/OneDrive - University of Arkansas for Medical Sciences/Pain_Project/NPPES_Data_Dissemination_August_2025_V2/npidata_pfile_20050523-20250810.csv"
output_path <- "C:/Users/watso/OneDrive - University of Arkansas for Medical Sciences/Pain_Project/pain_specialists.csv"

# Initialize result list
pain_specialists <- list()
chunk_size <- 50000
chunk_num <- 1

# Get column positions first
header <- fread(file_path, nrows = 0)
col_positions <- which(names(header) %in% c("NPI", "Provider Credential Text", 
                                            "Provider Business Mailing Address State Name", 
                                            "Healthcare Provider Taxonomy Code_1", 
                                            "Healthcare Provider Taxonomy Code_2"))

cat("Reading file in chunks...\n")

# Process file in chunks
repeat {
  # Calculate skip rows (0 for first chunk, then cumulative)
  skip_rows <- ifelse(chunk_num == 1, 0, (chunk_num - 1) * chunk_size + 1)
  
  # Read chunk - select columns by position to avoid header issues
  chunk <- fread(file_path, nrows = chunk_size, skip = skip_rows, 
                 select = col_positions, showProgress = FALSE)
  
  if (nrow(chunk) == 0) break
  
  # Set proper column names
  setnames(chunk, c("NPI", "Provider Credential Text", "Provider Business Mailing Address State Name", 
                    "Healthcare Provider Taxonomy Code_1", "Healthcare Provider Taxonomy Code_2"))
  
  # Filter for pain specialists
  pain_chunk <- chunk[`Healthcare Provider Taxonomy Code_1` %in% pain_codes$code | 
                        `Healthcare Provider Taxonomy Code_2` %in% pain_codes$code]
  
  if (nrow(pain_chunk) > 0) {
    # Add primary pain flag (1 = primary, 0 = secondary only)
    pain_chunk[, primary_pain := as.integer(`Healthcare Provider Taxonomy Code_1` %in% pain_codes$code)]
    
    # Add taxonomy description (prioritize primary)
    pain_chunk[, taxonomy_description := ""]
    pain_chunk[`Healthcare Provider Taxonomy Code_1` %in% pain_codes$code, 
               taxonomy_description := pain_codes[match(`Healthcare Provider Taxonomy Code_1`, code), description]]
    pain_chunk[`Healthcare Provider Taxonomy Code_2` %in% pain_codes$code & taxonomy_description == "", 
               taxonomy_description := pain_codes[match(`Healthcare Provider Taxonomy Code_2`, code), description]]
    
    pain_specialists[[length(pain_specialists) + 1]] <- pain_chunk
  }
  
  cat("Processed chunk", chunk_num, "- Found", nrow(pain_chunk), "pain specialists\n")
  chunk_num <- chunk_num + 1
}


#The code will break here but it is expected to. Run all the following.
# Combine all chunks
final_pain_specialists <- rbindlist(pain_specialists, fill = TRUE)

# Save result
fwrite(final_pain_specialists, output_path)

cat("Found", nrow(final_pain_specialists), "total pain specialists\n")
cat("Results saved to:", output_path, "\n")