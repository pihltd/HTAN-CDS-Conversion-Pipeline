import pandas as pd
import argparse
from crdclib import crdclib
import bento_mdf

def moveIt(old_column, loadsheets, mapping_df, cds_df, mapped=True, original=None):
    # TODO: Handle columns mapping to multiple locations
    mapping_row =mapping_df.loc[mapping_df['lift_from_property'] == old_column]
    new_column = mapping_row.iloc[0]['lift_to_property']
    new_node = mapping_row.iloc[0]['lift_to_node']
    temp_df = loadsheets[new_node]
    if mapped:
        temp_df[new_column] = cds_df[old_column].copy()
    else:
        temp_df[new_column] = cds_df[original].copy()
    loadsheets[new_node] = temp_df
    return loadsheets

def addColumns(cds_df, relations):
    for  rellist in relations.values():
        for column in rellist:
            #print(f"Checking column {column}")
            if column not in cds_df.columns:
                #print(f"Adding column {column} to cds_Df")
                cds_df.loc[:, column] = None
                
    return cds_df

def keyIt2(cds_df, relations):
    # Get the dbgap ID since it's a constant
    dbgap = cds_df.iloc[0]['phs_accession']
    for rellist in relations.values():
        for column in rellist:
            #print(f"Checking {column}")
            for index, row in cds_df.iterrows():
                if column == 'participant.study_participant_id':
                    #print(f"Populating {column}")
                    cds_df.at[index, 'study_participant_id'] = dbgap+"|"+row['participant_id']
                elif column == "study.phs_accession":
                    #print(f"Populating {column}")
                    cds_df.at[index, "study.phs_accession"] = dbgap
                elif column == "sample.sample_id":
                    #print(f"Populating {column}")
                    cds_df.at[index, "sample.sample_id"] = row['sample_id']
                elif column == "file.file_id":
                    #print(f"Populating {column}")
                    cds_df.at[index, "file.file_id"] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum']
                elif column == "image.study_link_id":
                    #print(f"Populating {column}")
                    cds_df.at[index, 'study_link_id'] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum']
                elif column == "program.program_acronym":
                    #print(f"Populating {column}")
                    cds_df.at[index, "program.program_acronym"] = row['study_acronym']
                    
    return cds_df

def keyIt(cds_df):
    #Transformations need to be done BEFORE we break it into individual sheets
    dbgap = cds_df.iloc[0]['phs_accession']
    newcols = ['study_participant_id','file_id', 'study_link_id', 'MultiplexMicroscopy_id', 'study_diagnosis_id', 'treatement_id']
    for col in newcols:
        cds_df.loc[:, col] = None
    for index, row in cds_df.iterrows():
        #study_participant_id
        cds_df.at[index, 'study_participant_id'] = dbgap+"|"+row['participant_id']
        #file_id
        cds_df.at[index, 'file_id'] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum'] 
        #study_link_id uses file_id
        cds_df.at[index, 'study_link_id'] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum']
        #MultiplexMicroscopy_id
        cds_df.at[index, 'MultiplexMicroscopy_id'] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum']
        # study_diagnosis_id
        cds_df.at[index,'study_diagnosis_id'] = row['participant_id']+"|"+str(row['primary_diagnosis'])
        # treatment_id
        cds_df.at[index,'treatement_id'] = row['participant_id']+"|"+str(row['therapeutic_agents'])
    
    return cds_df

def main(args):
    configs = crdclib.readYAML(args.configfile)
    
    #Create a dataframe from the mapping file
    mapping_df = pd.read_csv(configs['liftovermap'], sep="\t", header=0)
    
    #Create a dataframe from the CDS submission excel sheet
    cds_df = pd.read_excel(configs['submission_spreadsheet'], sheet_name=configs['submission_worksheet'])
    # All key fields need to be filled before we break up the sheet
    cds_df = addColumns(cds_df, configs['relationship_columns'])
    cds_df = keyIt2(cds_df, configs['relationship_columns'])
    
    #Read the target model
    target_mdf = bento_mdf.MDF(*configs['target_model'])
    target_nodes = target_mdf.model.nodes
    
    #create the collection of Data Hub style submission sheets
    nodes = list(mapping_df['lift_to_node'].unique())
    # loadsheets will be a dictionary of dataframes.  Key is node, value is dataframe for that node
    loadsheets = {}
    # Orphans is a list of fields with no mapping
    orphans = []
    #Work through each node in the target model creating a dataframe
    for node in nodes:
        props = target_nodes[node].props
        proplist = list(props.keys())
        node_df = pd.DataFrame(columns=proplist)
        loadsheets[node] = node_df
        
        
    #Get a list of the columns in the CDS submission sheet
    cds_columns = list(cds_df.columns)
    #Loop through the columns
    for cds_column in cds_columns:
        print(f"Starting to map {cds_column}")
        # Currently, manual takes precedence over automated
        if cds_column in configs['manual'].keys():
            print(f"{cds_column} is in manual mapping")
            manual_field = configs['manual'][cds_column]
            loadsheets = moveIt(manual_field, loadsheets, mapping_df, cds_df, False, cds_column)
        elif cds_column in mapping_df['lift_from_property'].unique():
            print(f"{cds_column} is in mapped columns")
            loadsheets = moveIt(cds_column, loadsheets, mapping_df, cds_df)
        else:
            print(f"{cds_column} is an orphan")
            orphans.append(cds_column)
            
    #General clean-up
    # Go through the dataframes and drop all rows that are nothing but NaN
    for node, df in loadsheets.items():
        temp_df = df.dropna(how='all')
        loadsheets[node] = temp_df
    # Drop any unpopulated dataframes
    dropkeys = []
    for node, df in loadsheets.items():
        #print(f"Node: {node}\tDataframe lenght:{str(len(df))}")
        if len(df) == 0:
            dropkeys.append(node)
    for node in dropkeys:
        loadsheets.pop(node, None)
    # Drop all duplicate rows and Insert the mandatory type column
    for node, df in loadsheets.items():
            temp_df = df.drop_duplicates()
            temp_df.insert(0, 'type', node)
            #print(f"Node: {node}\tDataFrame:\n{df}")
            loadsheets[node] = temp_df

    
    #And that should do it, just write out the DH style load sheets
    # Ignore any dataframes that are empty
    for node, df in loadsheets.items():
        #print(node)
        filename = configs['output_directory']+"CDS_"+node+"_template.tsv"
        df.to_csv(filename, sep="\t", index=False)
    #Print out the orphan fields
    if len(orphans) > 0:
        orphanfile = configs['output_directory']+"OrphanReport.csv"
        with open(orphanfile,"w") as f:
            for orphan in orphans:
                f.write(f"{orphan}\n")
        
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)