import pandas as pd
import argparse
from crdclib import crdclib
import bento_mdf



def usedNodeLister(cds_df, mapping_df):
    nodelist = []
    cds_props = list(cds_df.columns)
    for prop in cds_props:
        if prop in mapping_df['lift_from_property'].values:
            tempdf = mapping_df.loc[mapping_df['lift_from_property'] == prop]
            for index, row in tempdf.iterrows():
                if row['lift_to_node'] not in nodelist:
                    nodelist.append(row['lift_to_node'])
    return nodelist


    
def moveit(cds_df, mapping_df, loadsheets ):
    cds_columns = list(cds_df.columns)
    for cds_column in cds_columns:
        df = mapping_df.loc[mapping_df['lift_from_property'] == cds_column]
        for index, row in df.iterrows():
            new_column = row['lift_to_property']
            new_node = row['lift_to_node']
            if new_node in loadsheets.keys():
                temp_df = loadsheets[new_node]
                temp_df[new_column] = cds_df[cds_column].copy()
    return loadsheets
       

       
def dropUnpopulated(loadsheets):
    # Remove sheets that don't have any data
    dropkeys = []
    for node, df in loadsheets.items():
        if len(df) == 0:
            dropkeys.append(node)
    for node in dropkeys:
        loadsheets.pop(node, None)
    return loadsheets



def dropDupes(loadsheets):
    #Remove duplicated rows and add Type column
    for node, df in loadsheets.items():
        temp_df = df.drop_duplicates()
        temp_df.insert(0, 'type', node)
        loadsheets[node] = temp_df
    return loadsheets



def generateKey(dfrow, rulelist):
    # Generates a "unique" string from the list of provided fields
    keystring = None
    for rule in rulelist:
        if keystring is None:
            keystring = str(dfrow[rule])
        else:
            keystring = keystring+"|"+str(dfrow[rule])
    return keystring



def addRequired(cds_df, required_columns, usednodes):
    # Adds required column
    for node, fieldlist in required_columns.items():
        if node in usednodes:
            for field in fieldlist:
                cds_df[field] = None
    return cds_df



def populateRequired(cds_df, required_columns, keyrules):
    for index, row in cds_df.iterrows():
        for node, fieldlist in required_columns.items():
            for field in fieldlist:
                if "." in field:
                    temp = field.split(".")
                    keyfield = temp[-1]
                else:
                    keyfield = field
                keystring = generateKey(row, keyrules[keyfield])
                cds_df.loc[index, field] = keystring
    return cds_df
    


def main(args):
    #
    #  Step 1:  Set up the basics.  Get configs, read the mapping file, and read the submission sheet
    #
    
    configs = crdclib.readYAML(args.configfile)
    
    #Create a dataframe from the mapping file
    mapping_df = pd.read_csv(configs['liftovermap'], sep="\t", header=0)
    
    #Create a dataframe from the CDS submission excel sheet
    cds_df = pd.read_excel(configs['submission_spreadsheet'], sheet_name=configs['submission_worksheet'])
    #Drop all columns that have nothing but NaN
    cds_df = cds_df.dropna(axis=1, how='all')
    
    #Read the target model
    target_mdf = bento_mdf.MDF(*configs['target_model'])
    target_nodes = target_mdf.model.nodes
    
    #
    # Step 2:  Get a list of all the nodes that actually have data
    #
    
    usednodes = usedNodeLister(cds_df, mapping_df)
    
    #
    # Step 3, create the collection of load sheets
    #
    
    # loadsheets will be a dictionary of dataframes.  Key is node, value is dataframe for that node
    loadsheets = {}
    # Orphans is a list of fields with no mapping
    orphans = []
    for node in usednodes:
        props = target_nodes[node].props
        loadsheets[node] = pd.DataFrame(columns=list(props.keys()))

    #
    # Step 4: Add the required and relationship columns to the submission sheet.
    #
    
    cds_df = addRequired(cds_df, configs['required_columns'], usednodes)
    cds_df = addRequired(cds_df, configs['relationship_columns'], usednodes)
    
    #
    # Step 5: Now populate all those nodes
    #
    
    cds_df = populateRequired(cds_df, configs['required_columns'], configs['keyrules'])
    cds_df = populateRequired(cds_df, configs['relationship_columns'], configs['keyrules'])
    
    #
    # Step 6: Move the data from the CDS Submission sheet to the DH Load sheet, starting with manually mapped fields
    #
    
    cds_columns = list(cds_df.columns)
    #Loop through the columns
    for cds_column in cds_columns:
        # Currently, manual takes precedence over automated
        if cds_column in configs['manual'].keys():
            manual_field = configs['manual'][cds_column]
            loadsheets = moveit(cds_df, mapping_df, loadsheets)
        elif cds_column in mapping_df['lift_from_property'].unique():
            loadsheets = moveit(cds_df, mapping_df, loadsheets)
        else:
            orphans.append(cds_column)
    
    #
    # Step 7: General clean-up
    #
    
    # Go through the dataframes and drop all rows that are nothing but NaN
    for node, df in loadsheets.items():
        temp_df = df.dropna(how='all')
        loadsheets[node] = temp_df

    # Drop all duplicate rows and Insert the mandatory type column
    loadsheets = dropDupes(loadsheets)
    
    #
    # Step 8: And that should do it, just write out the DH style load sheets
    #
    
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