# HTAN to CDS Conversion Pipeline
These scritps will convert an old-style CDS Submission spreadsheet into Data Hub style submission sheets.

**ModelMappingFileGenerator**
This script generates a mapping file between any two MDF models.  Practically, it's limited to producing maps between two differnt versions of the same model

The config file should be in YAML format with the following fields (see **mapping_config.yml** for an example):

- *old_version_files* : A list of the starting model MDF files.  Can be URLs to Github raw files.
- *new_version_files* : A list of the target model MDF files.  Can be URLs to Github raw files.
- *mapping_file* : the full path and filename for the output mapping file.  This will be written as a tab-separated text file


**CDSLiftover**
This script uses the mapping file generated by **ModelMappingFileGenerator** to create Data Hub loading sheets

The config file should be in YAML format with the following fields (see **liftover_config.yml** for an example):

- *liftovermap* : Full path to the mapping file gnerated by **ModelMappingFileGenerator**
- *submission_spreadsheet* : Full path to the old-style CDS Submission spreadsheet.  This should be xlsx format.
- *submission_worksheet* : The name of the worksheet in the submission spreadsheet that has the submission data.
- *output_directory* : Full path to the directory where the Data Hub style load sheets will be written.
- *target_model* : A list of hte target model MDF files.  Cab be URLs to Github raw files.
- *manual* : A dictionary of manual mapping decisions, key is the starting field, value is the target field