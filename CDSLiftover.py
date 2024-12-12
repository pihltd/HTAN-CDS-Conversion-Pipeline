import pandas as pd
import argparse
from crdclib import crdclib
import bento_mdf
import uuid

def moveIt(old_column, loadsheets, mapping_df, cds_df, mapped=True, original = None, logfile = None):
    if logfile is not None:
        f = open(logfile, "a")
    # Create a new df based on the value of old_column
    df = mapping_df.loc[mapping_df['lift_from_property'] == old_column]
    for index, row in df.iterrows():
        new_column = row['lift_to_property']
        new_node = row['lift_to_node']
        if logfile is not None:
            f.write(f"Old Column: {old_column}\tNew Column: {new_column}\tNew Node: {new_node}\tNumber of rows: {str(len(df))}\n")
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
            if column not in cds_df.columns:
                cds_df.loc[:, column] = None
                
    return cds_df

def keyIt(cds_df, relations):
    # Get the dbgap ID since it's a constant
    dbgap = cds_df.iloc[0]['phs_accession']
    for rellist in relations.values():
        for column in rellist:
            for index, row in cds_df.iterrows():
                if column == 'participant.study_participant_id':
                    cds_df.at[index, 'participant.study_participant_id'] = dbgap+"|"+row['participant_id']
                    cds_df.at[index, 'study_participant_id'] = dbgap+"|"+row['participant_id']
                elif column == "study.phs_accession":
                    cds_df.at[index, "study.phs_accession"] = dbgap
                elif column == "sample.sample_id":
                    cds_df.at[index, "sample.sample_id"] = row['sample_id']
                elif column == "file.file_id":
                    cds_df.at[index, "file.file_id"] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum']
                    cds_df.at[index, "file_id"] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum']
                    #cds_df.at[index, "MultiplexMicroscopy_id"] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum']
                    #cds_df.at[index, "NonDICOMpathologyImages_id"] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum']
                    cds_df.at[index, "MultiplexMicroscopy_id"] = uuid.uuid4()
                    cds_df.at[index, "NonDICOMpathologyImages_id"] = uuid.uuid4()
                elif column == "image.study_link_id":
                    cds_df.at[index, 'image.study_link_id'] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum']
                    cds_df.at[index, 'study_link_id'] = row['file_name']+"|"+str(row['file_size'])+"|"+row['md5sum']
                elif column == "program.program_acronym":
                    cds_df.at[index, "program.program_acronym"] = row['study_acronym']
                #elif column == "study_diagnosis_id":
                #    cds_df.at[index, "study_diagnosis_id"] = dbgap+"|"+row['participant_id']+"|"+row['site_of_resection_or_biopsy']
                    
    return cds_df

def cleanIt(loadsheets, addedfields):
    cleaningreport = r'C:\Users\pihltd\Documents\modeltest\liftover\cleaningReport.tsv'
    f = open(cleaningreport, "a")
    f.write(f"Added Fields:\n")
    for field in addedfields:
        f.write(field+"\t")
    f.write("\n")
    #print(addedfields)
    # The purpose of this is to remove all dataframes that don't have any data
    nullcols = []
    datacols = []
    for node, df in loadsheets.items():
        for column in list(df.columns):
            if len(df[column]) > 0:
                f.write(f"Data:\t{column}\t{len(df[column])}\n")
                datacols.append(column)
            else:
                f.write(f"Null:\t{column}\t{len(df[column])}\n")
                nullcols.append(column)
        if set(datacols) <= set(addedfields):
            #print(f"Only data in addded columns: {datacols}\t Node:{node}")
            #print(f"Only data in addded columns Node:{node}")
            f.write(f"Only data in added columns for node: {node}\n")
            for col in nullcols:
                f.write(col+"\t")
            f.write("\n")
        else:
            #print(f"Has submitted data: {datacols}\tNode: {node}")
            #print(f"Has submitted data  Node: {node}")
            f.write(f"Has submitted data for node: {node}\n")
            for col in datacols:
                f.write(col+"\t")
            f.write("\n")
    return loadsheets
        


def main(args):
    configs = crdclib.readYAML(args.configfile)
    
    #Create a dataframe from the mapping file
    mapping_df = pd.read_csv(configs['liftovermap'], sep="\t", header=0)
    
    #Create a dataframe from the CDS submission excel sheet
    cds_df = pd.read_excel(configs['submission_spreadsheet'], sheet_name=configs['submission_worksheet'])
    # All key fields need to be filled before we break up the sheet
    cds_df = addColumns(cds_df, configs['relationship_columns'])
    cds_df = addColumns(cds_df, configs['required_columns'])
    cds_df = keyIt(cds_df, configs['relationship_columns'])
    cds_df = keyIt(cds_df, configs['required_columns'])
    keyreport = configs['output_directory']+"KeyAdditionReport.csv"
    cds_df.to_csv(keyreport, sep="\t", index=False)
    
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
    # Add the key fields to the dataframes, they're not in the model
    keyfields = configs['relationship_columns']
    for node in nodes:
        props = target_nodes[node].props
        proplist = list(props.keys())
        if node in keyfields.keys():
            keylist = keyfields[node]
            for keyfield in keylist:
                proplist.append(keyfield)
        node_df = pd.DataFrame(columns=proplist)
        loadsheets[node] = node_df
        
        
    #Get a list of the columns in the CDS submission sheet
    cds_columns = list(cds_df.columns)
    #Loop through the columns
    #Temp log file
    movelog = configs['output_directory']+"MoveReport.txt"
    for cds_column in cds_columns:
        # Currently, manual takes precedence over automated
        if cds_column in configs['manual'].keys():
            manual_field = configs['manual'][cds_column]
            loadsheets = moveIt(manual_field, loadsheets, mapping_df, cds_df, False, cds_column,movelog)
        elif cds_column in mapping_df['lift_from_property'].unique():
            loadsheets = moveIt(cds_column, loadsheets, mapping_df, cds_df,True,None,movelog)
        else:
            orphans.append(cds_column)
            
    #General clean-up
    # Go through the dataframes and drop all rows that are nothing but NaN
    for node, df in loadsheets.items():
        temp_df = df.dropna(how='all')
        loadsheets[node] = temp_df
    # Drop any unpopulated dataframes
    # TODO: Figure out how to drop if the relationships are the only thing populated
    dropkeys = []
    for node, df in loadsheets.items():
        if len(df) == 0:
            dropkeys.append(node)
    for node in dropkeys:
        loadsheets.pop(node, None)
    # Drop all duplicate rows and Insert the mandatory type column
    for node, df in loadsheets.items():
            temp_df = df.drop_duplicates()
            temp_df.insert(0, 'type', node)
            loadsheets[node] = temp_df
            
    # Drop out any empty dataframes
    addedfields = ["type"]
    for fields in configs["required_columns"].values():
        for field in fields:
            addedfields.append(field)
    for fields in configs["relationship_columns"].values():
        for field in fields:
            addedfields.append(field)
    loadsheets = cleanIt(loadsheets, addedfields)

    
    #And that should do it, just write out the DH style load sheets
    for node, df in loadsheets.items():
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