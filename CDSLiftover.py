import pandas as pd
import argparse
from crdclib import crdclib
import bento_mdf
import sys


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
    for node in nodes:
        props = target_nodes[node].props
        proplist = list(props.keys())
        node_df = pd.DataFrame(columns=proplist)
        loadsheets[node] = node_df
        
    #for node, df in loadsheets.items():
    #    print(f"Node: {node}\nDataFrame: {df}")
    #sys.exit(0)
        
    #Get a list of the columns in the CDS submission sheet
    cds_columns = list(cds_df.columns)
    #Loop through the columns
    #print(list(loadsheets.keys()))
    #sys.exit(0)
    for cds_column in cds_columns:
        #print(f"CDS Column: {cds_column}")
        if cds_column in mapping_df['lift_from_property'].unique():
            mapping_row =mapping_df.loc[mapping_df['lift_from_property'] == cds_column]
            #mapping_row = mapping_df.loc[[mapping_df['lift_from_property'] == cds_column]]
            print(mapping_row)
            new_column = mapping_row.iloc[0]['lift_to_property']
            new_node = mapping_row.iloc[0]['lift_to_node']
            print(f"Old Column: {cds_column}\tNew Column: {new_column}\t New Node: {new_node}")
            #print(loadsheets[new_node])
            #print(mapping_row)
            #loadsheets[new_node].loc[:, new_column] = cds_df.loc[:, cds_column]
            temp_df = loadsheets[new_node]
            #print(temp_df)
            temp_df[new_column] = cds_df[cds_column].copy()
            loadsheets[new_node] = temp_df
            
        else:
            print(f"ORPHAN PROPERTY: {cds_column}")
    #Since the Excel sheet is flattened data, we need to de-dupe the individual loading sheets
    for node, df in loadsheets.items():
        temp_df = df.drop_duplicates()
        loadsheets[node] = temp_df
    
    #And that should do it, just write out the DH style load sheets
    for node, df in loadsheets.items():
        filename = configs['output_directory']+"CDS_"+node+"_template.csv"
        df.to_csv(filename, sep="\t", index=False)
        
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)