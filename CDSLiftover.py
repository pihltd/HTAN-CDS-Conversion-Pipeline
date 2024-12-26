import pandas as pd
import argparse
from crdclib import crdclib
import bento_mdf



def usedNodeLister(cds_df, mapping_df):
    # Return a list of the nodes that are actually used, not just defined in the model
    nodelist = []
    # List of the used properties
    cds_props = list(cds_df.columns)
    for prop in cds_props:
        # Find out how many times the property has been mapped
        if prop in mapping_df['lift_from_property'].values:
            tempdf = mapping_df.loc[mapping_df['lift_from_property'] == prop]
            # For each mapping, get the associated node, but only once
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
       

'''    
def dropUnpopulated(loadsheets):
    # Remove sheets that don't have any data
    dropkeys = []
    for node, df in loadsheets.items():
        if len(df) == 0:
            dropkeys.append(node)
    for node in dropkeys:
        loadsheets.pop(node, None)
    return loadsheets
'''

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
    for rule in rulelist['method']:    
        if keystring is None:
            keystring = str(dfrow[rule])
        else:
            keystring = keystring+"|"+str(dfrow[rule])
    return keystring


'''
def addRequired(cds_df, required_columns, usednodes):
    # NOT NEEDED, all required fields are in the model
    # Adds required column (defined in configs)
    # These are generally ID columns that CDS would populate post submission
    for node, fieldlist in required_columns.items():
        if node in usednodes:
            for field in fieldlist:
                cds_df[field] = None
    return cds_df
'''

def addRelationships(cds_df, mdf, usednodes):
    # Add the key fields from the data model
    # This elimnates the need for the relationship_columns in the config file
    keyfields = {}
    for node in usednodes:
        nodekeys = getKeyFields(node, mdf)
        keyfields[node] = nodekeys
        for nodekey in nodekeys:
            cds_df[nodekey] = None
    return cds_df, keyfields    



def populateRequired2(cds_df, required_columns, keyrules, compoud):
    #Need to do this row-by-row
    for index, row in cds_df.iterrows():
        for node, fieldlist in required_columns.items():
            for field in fieldlist:
                if "." in field:
                    temp = field.split(".")
                    keyfield = temp[-1]
                else:
                    keyfield = field
                if keyrules[keyfield]['compound'] == compoud:
                    keystring = generateKey(row, keyrules[keyfield])
                    cds_df.loc[index, field] = keystring

'''
def populateRequired(cds_df, required_columns, keyrules):
    # This adds values to the key fields based on rules in the config file
    # TODO: Adapt to new rule structure, do compound No first
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
'''    


def getKeyFields(node, mdf):
    keylist = []
    edgelist = mdf.model.edges_by_src(mdf.model.nodes[node])
    for edge in edgelist:
        destnode = edge.dst.get_attr_dict()['handle']
        #Filter out this node, no need to self reference
        if destnode != node:
            destprops = mdf.model.nodes[destnode].props
            for destkey, destprop in destprops.items():
                if destprop.get_attr_dict()['is_key'] == 'True':
                    keylist.append(destnode+"."+destprop.get_attr_dict()['handle'])
    return keylist


def main(args):
    #
    #  Step 1:  Set up the basics.  Get configs, read the mapping file, and read the submission sheet
    #
    
    configs = crdclib.readYAML(args.configfile)
    
    #Create a dataframe from the liftover mapping file
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
        proplist = list(props.keys())
        # Drop all Template:No properties
        # NOTE: This eliminates all the Required Columns
        # REDO so that it keeps Template:No AND Key:True fields
        for propname, prop in props.items():
            if 'Template' in prop.tags:
                #if str(prop.tags['Template'].get_attr_dict()['value']) == 'No':
                if (str(prop.tags['Template'].get_attr_dict()['value']) == 'No') and (prop['key'] != 'true'):
                    proplist.remove(propname)
        loadsheets[node] = pd.DataFrame(columns=proplist)

    #
    # Step 4: Add the required and relationship columns to the submission sheet.
    #
    
    # Dont need to add required fields, they're already in the model
    #cds_df = addRequired(cds_df, configs['required_columns'], usednodes)
    #cds_df = addRequired(cds_df, configs['relationship_columns'], usednodes)
    cds_df, keyfields = addRelationships(cds_df, target_mdf, usednodes)
    #re-do adding relationship columns so that they're autocreated, not hard coded

    
    #
    # Step 5: Now populate all those nodes
    #
    
    # THE PROBLEM:  Required columns are usually Template:No
    # TODO: Did they get weeded out earlier.
    #cds_df = populateRequired(cds_df, configs['required_columns'], configs['keyrules'])
    #cds_df = populateRequired(cds_df, configs['relationship_columns'], configs['keyrules'])
    #Need to do this twice, first to get the rules that don't rely on compoud fields, then for those that do
    cds_df = populateRequired2(cds_df, keyfields, configs['keyrules'], "No")
    cds_df = populateRequired2(cds_df, keyfields, configs['keyrules'], "Yes")
    
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