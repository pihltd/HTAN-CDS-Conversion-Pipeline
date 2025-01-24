# Generates a liftover file between two versions of the same model
import bento_mdf
import argparse
from crdclib import crdclib
import pandas as pd

#TODO:  1) for CDEs with PVs, pull PVs from caDSR  2) Use NCIt codes to map PVs

def getRellist(mdf):
    # For each node, return a list of the "is_key" attributes
    relobj = {}
    for node in mdf.model.nodes:
        rellist = getKeyFields(node, mdf)
        relobj[node] = rellist
    return relobj


def getKeyFields(node, mdf):
    # Look at the edges for a node and return a list of props designated as key
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

def getCDEInfo(term_object):
    # Get the ID and version of caDSR CDEs.  WE officially don't care about other CDEs
    for term in term_object.values():
        term_dict = term.get_attr_dict()
        if 'caDSR' in term_dict['origin_name']:
            return {'cdeid':term_dict['origin_id'], 'cdeversion': term_dict['origin_version']}
        else:
            return{'cdeid':None, 'cdeversion': None}
        
def makeMDFDataFrame(mdf):
    #Create a dataframe from the model file
    columns = ["node", "property", "cdeid", "cdeversion"]
    mdf_df = pd.DataFrame(columns=columns)
    nodes = mdf.model.nodes
    nodelist = list(nodes.keys())
    for node in nodelist:
        propertylist = list(nodes[node].props.keys())
        for property in propertylist:
            #Look to see if there is a CDE, and if so, get the information.
            if mdf.model.props[node, property].concept is not None:
                concept = mdf.model.props[node, property].concept.terms
                if concept is not None:
                    cdeinfo = getCDEInfo(concept)
                    mdf_df.loc[len(mdf_df)] = {"node": node, "property": property, "cdeid": cdeinfo['cdeid'], "cdeversion":cdeinfo["cdeversion"]}
            else:
                #Add info if there is no CDE
                mdf_df.loc[len(mdf_df)] = {"node": node, "property": property, "cdeid": None, "cdeversion": None}
    return mdf_df
            

def cdeCheck2(oldid, oldver, newid, newver):
    if oldid == newid:
        if oldver == newver:
            return "CDE ID and Version match"
        else:
            return "CDE Version mismatch"
    else:
        return "CDE ID mismarch"

def cdeMatch(old_df, new_df, liftover_df, old_version, new_version):
    #Instead of string matching, match by CDE ID.  Ignore CDE version
    for index, row in old_df.iterrows():
        # Meant to handle previous mappings like crdc_id
        if row['property'] not in liftover_df['lift_from_property']:
            if (new_df['cdeid'].eq(row['cdeid'])).any():
                temp_df = new_df.loc[new_df['cdeid'] == row['cdeid']]
                for tempindex, temprow in temp_df.iterrows():
                    cde_relationship = cdeCheck2(row['cdeid'], row['cdeversion'], temprow['cdeid'], temprow['cdeversion'])
                    liftover_df.loc[len(liftover_df)]= {"lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_version": new_version, "lift_to_node": temprow['node'], "lift_to_property": temprow['property'], "lift_from_cde":row['cdeid'], 
                                                         "lift_from_cdeversion": row['cdeversion'], "lift_to_cde": temprow['cdeid'], "lift_to_cdeversion": temprow['cdeversion'], "cde_relationship": cde_relationship}
    return liftover_df


def stringMatch2(old_df, new_df, liftover_df, old_version, new_version):
    for index, row in old_df.iterrows():
        unmapped = True
        if row['property'] in liftover_df['lift_from_property'].unique():
            print(f"CDE Match for {row['property']}")
            unmapped = False
        elif row['property'] == 'crdc_id':
            print(f"crdc_id match for {row['property']}")
            unmapped = False
        # So if not mapped by CDE, or not crdc_id, then try string match
        if unmapped:
            print((f"Query: {row['property']}"))
            print(f"Target: {new_df['property'].unique()}")
            if row['property'] in new_df['property'].unique():
                cde_relationship = "String Match"
                #Make a df of all the rows that match
                temp_df = new_df.loc[new_df['property'] == row['property']]
                #Go through the rows and add to liftover
                for tempindex, temprow in temp_df.iterrows():
                    liftover_df.loc[len(liftover_df)]= {"lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_version": new_version, "lift_to_node": temprow['node'], "lift_to_property": temprow['property'], "lift_from_cde":"N/A", 
                                                         "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A", "lift_to_cdeversion": "N/A", "cde_relationship": cde_relationship}
            else:
                cde_relationship = "Unmapped"
                liftover_df.loc[len(liftover_df)]= {"lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_version": new_version, "lift_to_node": "N/A", "lift_to_property": "N/A", "lift_from_cde":row['cdeid'], 
                                                         "lift_from_cdeversion": row['cdeversion'], "lift_to_cde": "N/A", "lift_to_cdeversion": "N/A", "cde_relationship": cde_relationship}
    return liftover_df
    
def stringMatch(old_df, new_df, liftover_df, old_version, new_version):
    # THIS NEEDS A COMPLETE RETHINK
    for index, row in old_df.iterrows():
        #Check that the property hasn't already been mapped by CDE
        if row['property'] not in liftover_df['lift_from_property']:
            print(f"Prop: {row['property']} is not a CDE Match")
            # If no, then see if there is a string match
            if row['property'] in new_df['property']:
                print(f"Old {row['property']} matches new {new_df['property']}")
                temp_df = new_df.loc[new_df['property'] == row['property']]
                for tempindex, temprow in temp_df.iterrows():
                    #cde_relationship = cdeCheck2(row['cdeid'], row['cdeversion'], temprow['cdeid'], temprow['cdeversion'])
                    cde_relationship = "String Match"
                    liftover_df.loc[len(liftover_df)]= {"lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_version": new_version, "lift_to_node": temprow['node'], "lift_to_property": temprow['property'], "lift_from_cde":row['cdeid'], 
                                                         "lift_from_cdeversion": row['cdeversion'], "lift_to_cde": temprow['cdeid'], "lift_to_cdeversion": temprow['cdeverion'], "cde_relationship": cde_relationship}
            # If no string match
            else:
                cde_relationship = "Unmapped"
                liftover_df.loc[len(liftover_df)]= {"lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_version": new_version, "lift_to_node": "N/A", "lift_to_property": "N/A", "lift_from_cde":row['cdeid'], 
                                                         "lift_from_cdeversion": row['cdeversion'], "lift_to_cde": "N/A", "lift_to_cdeversion": "N/a", "cde_relationship": cde_relationship}
                
    return liftover_df
    

def crdcIDAdd(old_df, liftover_df, new_props, old_version, new_version):
    # A df with all crdc_id rows
    old_crdc_df = old_df.loc[old_df['property'] == 'crdc_id']
    for index, row in old_crdc_df.iterrows():
        # Look to see if the new properties has a crdc_id for that node.  Property key is (node, property).
        if (row['node'], row['property']) in new_props:
            liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_version": new_version, "lift_to_node": row['node'], "lift_to_property": row['property'], "lift_from_cde":"N/A", 
                                                         "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A", "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
        else:
            liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_version": new_version, "lift_to_node": "N/A", "lift_to_property": "N/A", "lift_from_cde":"N/A", 
                                                         "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A", "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
        
    return liftover_df



def main(args):
    configs = crdclib.readYAML(args.configfile)
    
    #Dataframe setup
    columns = ["lift_from_version", "lift_from_node", "lift_from_property", "lift_to_version", "lift_to_node", "lift_to_property", "lift_from_cde", "lift_from_cdeversion",
               "lift_to_cde", "lift_to_cdeversion", "cde_relationship"]
    liftover_df = pd.DataFrame(columns=columns)
    
    # Get MDF objects of each model
    old_mdf = bento_mdf.MDF(*configs["old_version_files"])
    new_mdf = bento_mdf.MDF(*configs["new_version_files"])
    
    # Need the properties from the new model
    new_props = new_mdf.model.props
    #old_props = old_mdf.model.props
    
    #print(f"New Props: {new_props}")
    #print(f"Old Props: {old_props}")
    
    #Grab version numbers
    old_model_version = old_mdf.version
    new_model_version = new_mdf.version
    
    # Create the dataframes
    old_df = makeMDFDataFrame(old_mdf)
    new_df = makeMDFDataFrame(new_mdf)
    
    old_df.to_csv(configs['old_df'], sep="\t", index=False)
    new_df.to_csv(configs['new_df'], sep="\t", index=False)
    
    
    # Add the crdc_id manually since mapping causes cross-mapping artefacts.
    liftover_df = crdcIDAdd(old_df, liftover_df, new_props, old_model_version, new_model_version)
    
    #Match by CDE first
    liftover_df = cdeMatch(old_df, new_df, liftover_df, old_model_version, new_model_version)
    
    #Do exact string match for any fields that didn't match by CDE
    liftover_df = stringMatch2(old_df, new_df, liftover_df, old_model_version, new_model_version)
    
    #
    # 1/21: THis is making the same mistake as the liftover.  There are Key fields, which are defined in the model properties, and
    # relationship fields that are defined in model edges.  Key fields should be mapped, relationship fields should just be added
    #
    #
    # Add the relationship fields.
    old_rels = getRellist(old_mdf)
    new_rels = getRellist(new_mdf)
    for old_node, old_rellist in old_rels.items():
        if old_node in new_rels:
            for old_rel in old_rellist:
                if old_rel in new_rels[old_node]:
                    liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_model_version, "lift_from_node": old_node, "lift_from_property": old_rel, 
                                                         "lift_to_version": new_model_version, "lift_to_node": old_node, "lift_to_property":old_rel, 
                                                         "lift_from_cde": "N/A", "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A",
                                                         "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
                else:
                    liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_model_version, "lift_from_node": old_node, "lift_from_property": old_rel, 
                                                         "lift_to_version": new_model_version, "lift_to_node": "N/A", "lift_to_property":"N/A", 
                                                         "lift_from_cde": "N/A", "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A",
                                                         "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
    
    # Required fields are generally internal IDs, but capture them
    # TODO: Are these ID fields in the model or added later?
    # All "required_columns" are in the model, don't need this
    '''
    required = configs['required_columns']
    for node, rellist in required.items():
        for rel in rellist:
            liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_model_version, "lift_from_node": node, "lift_from_property": rel, 
                                                         "lift_to_version": new_model_version, "lift_to_node": node, "lift_to_property":rel, 
                                                         "lift_from_cde": "N/A", "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A",
                                                         "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
    '''
    
    #Print out the liftover files
    liftover_df.to_csv(configs['mapping_file'], sep="\t", index=False)
    
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)