import pandas as pd
import argparse
from crdclib import crdclib
import bento_mdf
import uuid



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


    
def moveIt(cds_df, mapping_df, loadsheets ):
    #NOTE:  Key fields may not be in mapping file.
    cds_columns = list(cds_df.columns)
    for cds_column in cds_columns:
        #Create a new df from mapping_df where the lift from property is the cds column
        df = mapping_df.loc[mapping_df['lift_from_property'] == cds_column]
        for index, row in df.iterrows():
            new_column = row['lift_to_property']
            new_node = row['lift_to_node']
            if new_node in loadsheets.keys():
                temp_df = loadsheets[new_node]
                temp_df[new_column] = cds_df[cds_column].copy()
    return loadsheets
       

def dropDupes(loadsheets):
    #Remove duplicated rows and add Type column
    for node, df in loadsheets.items():
        temp_df = df.drop_duplicates()
        temp_df.insert(0, 'type', node)
        loadsheets[node] = temp_df
    return loadsheets



def generateKey(dfrow, rulelist, field):
    # Generates a "unique" string from the list of provided fields
    keystring = None
    
    if rulelist['compound'] == "No":
        keystring = str(uuid.uuid4())
    elif rulelist['compound'] == 'Exempt':
        if field in dfrow:
            keystring = str(dfrow[field])
    elif rulelist['compound'] == "Yes":
        for rule in rulelist['method']:
            if keystring is None:
                keystring = str(dfrow[rule])
            else:
                keystring = keystring+"_"+str(dfrow[rule])
                # For unknown reasons, CDS uses _ instead of |
    return keystring

     

def populateKey(cds_df,keyfields, keyrules, compoundvalue):
    #cols = list(cds_df.columns)
    #print(cols)
    for keyfieldlist in keyfields.values():
        for keyfield in keyfieldlist:
            if keyrules[keyfield]['compound'] == compoundvalue:
                keystring = None
                for index, row in cds_df.iterrows():
                    keystring = generateKey(row, keyrules[keyfield], keyfield)
                    # Have to change program acronym to study acronym because the spreadsheet uses study, not program.
                    #if keyfield == 'proteomic_info_id':
                    #    print(f"Field: {keyfield}\t Value: {keystring}")
                    if compoundvalue == 'Yes':
                        print(keyfield)
                    if keyfield == 'study_diagnosis_id':
                        print(f"Field: {keyfield}\t Value: {keystring}")
                    if keyfield == 'program_acronym':
                        cds_df.loc[index, 'study_acronym'] = keystring
                    else:
                        cds_df.loc[index, keyfield] = keystring
    return cds_df
            

def populateRelations(cds_df, relation_columns, keyrules, compound):
    #Need to do this row-by-row but first check that we need to do it at all
    #relation_columns is a dict of node:[fieldlist]
    for node, fieldlist in relation_columns.items():
        for field in fieldlist:
            if "." in field:
                temp = field.split(".")
                keyfield = temp[-1]
            else:
                keyfield = field
            if keyrules[keyfield]['compound'] == compound:
                for index, row in cds_df.iterrows():
                    keystring = generateKey(row, keyrules[keyfield], keyfield)
                    cds_df.loc[index, field] = keystring
    return cds_df


def getKeyProps(mdf):
    keyprops = {}
    nodes = mdf.model.nodes
    for node in nodes:
        temp = []
        nodeprops = nodes[node].props
        for propinfo, prop in nodeprops.items():
            if prop.get_attr_dict()['is_key'] == 'True':
                temp.append(propinfo)
        keyprops[node] = temp
    return keyprops

def getRelationFields(mdf):
    #Uses edges to pull out the relation fields for each node
    relationfields = {}
    nodes = mdf.model.nodes
    for node in nodes:
        temp = []
        edgelist = mdf.model.edges_by_src(mdf.model.nodes[node])
        for edge in edgelist:
            destnode = edge.dst.get_attr_dict()['handle']
            #Filter out self-reference to this node
            if destnode != node:
                destprops = mdf.model.nodes[destnode].props
                for destprop in destprops.values():
                    if destprop.get_attr_dict()['is_key'] == 'True':
                        temp.append(destnode+"."+destprop.get_attr_dict()['handle'])
        relationfields[node] = temp
    return relationfields
         


        
def usedKeyFields(keyfields, usednodes):
    temp = {}
    for keynode, keyfieldlist in keyfields.items():
        if keynode in usednodes:
            temp[keynode] = keyfieldlist
    return temp



def addManualKeys(keyfields, manualkeys):
    for node, keylist in manualkeys.items():
        if node in keyfields:
            for key in keylist:
                keyfields[node] = keyfields[node].append(key)
            else:
                keyfields[node] = keylist
    return keyfields



def buildLoadsheets(usednodes, target_nodes, target_props, keyfields, relationfields):
    loadsheets = {}
    for node in usednodes:
        props = target_nodes[node].props
        proplist = list(props.keys())
        #Drop everything with Template:No
        for prop in proplist:
            testprop = target_props[(node,prop)]
            if 'Template' in testprop.tags:
                if str(testprop.tags['Template'].get_attr_dict()['value']) == 'No':
                    proplist.remove(prop)
        #Now go back and add the key and relationships 
        for keyprop in keyfields[node]:
            if keyprop not in proplist:
                proplist.append(keyprop)
        for relprop in relationfields[node]:
            if relprop not in proplist:
                proplist.append(relprop)
        # Lastly build the dataframe
        loadsheets[node] = pd.DataFrame(columns=proplist)
    return loadsheets


def addKeyRelColumns(cds_df, usednodes, keyfields, relationfields):
    cdsfields = list(cds_df.columns)
    for node in usednodes:
        for key in keyfields[node]:
            if key not in cdsfields:
                cds_df[key] = None
        for rel in relationfields[node]:
            if rel not in cdsfields:
                cds_df[rel] = None
    return cds_df

def main(args):
    #
    #  Step 1:  Set up the basics.  Get configs, read the mapping file, and read the submission sheet
    #
    
    # Get configs
    if args.verbose:
        print("Reading configurations")
    configs = crdclib.readYAML(args.configfile)
    
    #Create a dataframe from the liftover mapping file
    if args.verbose:
        print("Creating liftover dataframe")
    mapping_df = pd.read_csv(configs['liftovermap'], sep="\t", header=0)
    
    #Create a dataframe from the CDS submission excel sheet
    if args.verbose:
        print("Creating dataframe from Excel submission sheet")
    cds_df = pd.read_excel(configs['submission_spreadsheet'], sheet_name=configs['submission_worksheet'])
    #Drop all columns that have nothing but NaN
    if args.verbose:
        print("Dropping all empty columns")
    cds_df = cds_df.dropna(axis=1, how='all')
    
    #Read the target model
    if args.verbose:
        print("Reading target data model and creating node and property lists")
    target_mdf = bento_mdf.MDF(*configs['target_model'])
    target_nodes = target_mdf.model.nodes
    target_props = target_mdf.model.props
    
    # Get all fields in the model marked as key
    if args.verbose:
        print("Getting key fields from model")
    keyfields = getKeyProps(target_mdf)
    # Add any manual key fields from the config file
    if args.verbose:
        print("Adding any manuallly provided key fields")
    keyfields = addManualKeys(keyfields, configs['manual_key'])
    # Get the relationship fields from the model edges
    if args.verbose:
        print("Getting relationship fields from model edges")
    relationfields = getRelationFields(target_mdf)
    
    
    
    #
    # Step 2:  Get a list of all the nodes that actually have data
    #
    if args.verbose:
        print("Remove all nodes without any data")
    usednodes = usedNodeLister(cds_df, mapping_df)
    
    #
    # Step 3: Adjust keyfields so that it only includes used nodes
    #
    if args.verbose:
        print("Removing key fields that won't be used")
    keyfields = usedKeyFields(keyfields, usednodes)
    
    #
    # Step 4: create the collection of load sheets
    #
    # loadsheets will be a dictionary of dataframes.  Key is node, value is dataframe for that node
    if args.verbose:
        print("Creating the loadsheet collection")
    loadsheets = buildLoadsheets(usednodes, target_nodes, target_props, keyfields, relationfields)
    
    #
    # Step 5: Add the key and relationship columns to the cds_df
    #
    if args.verbose:
        print("Adding the key and relationship columns to the Excel dataframe")
    cds_df = addKeyRelColumns(cds_df, usednodes, keyfields, relationfields)
                    
    #
    # Step 6: Populate key and relationships in the cds_df
    #
    
    # Start with keys
    if args.verbose:
        print("Populateing Exempt Keys")
    cds_df = populateKey(cds_df, keyfields, configs['keyrules'], 'Exempt')
    if args.verbose:
        print("Popluating No keys")
    cds_df = populateKey(cds_df, keyfields, configs['keyrules'], 'No' )
    if args.verbose:
        print("Populating Yes keys")
    cds_df = populateKey(cds_df,keyfields, configs['keyrules'], 'Yes' )
    
    # And now for the relationships
    if args.verbose:
        print("Populating No relationships")
    cds_df = populateRelations(cds_df, relationfields, configs['keyrules'], "No")
    if args.verbose:
        print("Populating Yes relationships")
    cds_df = populateRelations(cds_df, relationfields, configs['keyrules'], "Yes")
    
    #
    # Step 7: Move the data in the Excel sheet to the loadsheets
    #
    if args.verbose:
        print("Moving data from the Excel dataframe to the loadsheets")
    loadsheets = moveIt(cds_df, mapping_df, loadsheets)

        
    #
    # Step 8: General clean-up
    #

    # Drop all duplicate rows and Insert the mandatory type column
    if args.verbose:
        print("Dropping duplicate lines from loadsheets")
    loadsheets = dropDupes(loadsheets)
    
    #
    # Step 9: And that should do it, just write out the DH style load sheets
    #
    if args.verbose:
        print("Writing the loadsheets to file")
    for node, df in loadsheets.items():
        filename = configs['output_directory']+"CDS_"+node+"_template.tsv"
        df.to_csv(filename, sep="\t", index=False)   
 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument("-v", "--verbose", action='store_true', help="Verbose output")

    args = parser.parse_args()

    main(args)