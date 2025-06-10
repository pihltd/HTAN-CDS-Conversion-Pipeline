# Reads Data Hub load sheets from an earlier data model and lifts them to a newer model
# Requires a mapping file from ModelMappingFileGenerator

from crdclib import crdclib
import argparse
import pandas as pd



def populateNodeSheets(sourcesheets, sourcedir, sourcefilelist ):
    for entry in sourcefilelist:
        for node, filename in entry.items():
            temp_df = pd.read_csv(sourcedir+filename, sep="\t")
            sourcesheets[node] = temp_df
    return sourcesheets


def main(args):
    configs = crdclib.readYAML(args.configfile)
    #Dictionary:  Key: Node, value: dataframe
    sourcesheets = {}
    sourcesheets = populateNodeSheets(sourcesheets, configs['input_directory'], configs['Nodes'])
    #print(sourcesheets)
    targetsheets = {}
    
    mapping_df = pd.read_csv(configs['liftovermap'], sep="\t", header=0)
    #print(mapping_df.head())
    
    
    for node, df in sourcesheets.items():
        #Make a df of just the mappings for that node
        node_df = mapping_df.query('lift_from_node == @node')
        #print(node_df.head())
        fieldlist = list(df.columns)
        #print(fieldlist)
        for field in fieldlist:
            if field in node_df['lift_from_property']:
                newprop = 
                newnode = 
    



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)