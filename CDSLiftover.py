import pandas as pd
import argparse
from crdclib import crdclib
import bento_mdf

def moveIt(old_column, loadsheets, mapping_df, cds_df, mapped=True, original=None):
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

def main(args):
    configs = crdclib.readYAML(args.configfile)
    
    #Create a dataframe from the mapping file
    mapping_df = pd.read_csv(configs['liftovermap'], sep="\t", header=0)
    
    #Create a dataframe from the CDS submission excel sheet
    cds_df = pd.read_excel(configs['submission_spreadsheet'], sheet_name=configs['submission_worksheet'])
    
    #Read the target model
    target_mdf = bento_mdf.MDF(*configs['target_model'])
    target_nodes = target_mdf.model.nodes
    
    #create the collection of Data Hub style submission sheets
    nodes = list(mapping_df['lift_to_node'].unique())
    #This will be a dictionary of dataframes
    loadsheets = {}
    orphans = []
    for node in nodes:
        props = target_nodes[node].props
        proplist = list(props.keys())
        node_df = pd.DataFrame(columns=proplist)
        loadsheets[node] = node_df
        
        
    #Get a list of the columns in the CDS submission sheet
    cds_columns = list(cds_df.columns)
    #Loop through the columns
    for cds_column in cds_columns:
        # Currently, manual takes precedence over automated
        if cds_column in configs['manual'].keys():
            manual_field = configs['manual'][cds_column]
            loadsheets = moveIt(manual_field, loadsheets, mapping_df, cds_df, False, cds_column)
        elif cds_column in mapping_df['lift_from_property'].unique():
            loadsheets = moveIt(cds_column, loadsheets, mapping_df, cds_df)
        else:
            orphans.append(cds_column)
            
    
    #Since the Excel sheet is flattened data, we need to de-dupe the individual loading sheets
    for node, df in loadsheets.items():
        temp_df = df.drop_duplicates()
        loadsheets[node] = temp_df
    
    #And that should do it, just write out the DH style load sheets
    # Ignore any dataframes that are empty
    for node, df in loadsheets.items():
        if len(df) > 0:
            filename = configs['output_directory']+"CDS_"+node+"_template.csv"
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