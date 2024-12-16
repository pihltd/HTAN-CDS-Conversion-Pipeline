# Generates a liftover file between two versions of the same model
import bento_mdf
import argparse
from crdclib import crdclib
import pandas as pd

#TODO:  1) for CDEs with PVs, pull PVs from caDSR  2) Use NCIt codes to map PVs

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
            if mdf.model.props[node, property].concept is not None:
                concept = mdf.model.props[node, property].concept.terms
                if concept is not None:
                    cdeinfo = getCDEInfo(concept)
                    mdf_df.loc[len(mdf_df)] = {"node": node, "property": property, "cdeid": cdeinfo['cdeid'], "cdeversion":cdeinfo["cdeversion"]}
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
            
    
def stringMatch(old_df, new_df, liftover_df, old_version, new_version):
    for index, row in old_df.iterrows():
        #Check that the property hasn't already been mapped by CDE
        if row['property'] not in liftover_df['lift_from_property']:
            # If no, then see if there is a string match
            if row['property'] in new_df['property']:
                temp_df = new_df.loc[new_df['property'] == row['property']]
                for tempindex, temprow in temp_df.iterrows():
                    cde_relationship = cdeCheck2(row['cdeid'], row['cdeversion'], temprow['cdeid'], temprow['cdeversion'])
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
    old_crdc_df = old_df.loc[old_df['property'] == 'crdc_id']
    for index, row in old_crdc_df.iterrows():
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
    
    old_mdf = bento_mdf.MDF(*configs["old_version_files"])
    new_mdf = bento_mdf.MDF(*configs["new_version_files"])
    
    new_props = new_mdf.model.props
    
    old_model_version = old_mdf.version
    new_model_version = new_mdf.version
    
    # Create the dataframes
    old_df = makeMDFDataFrame(old_mdf)
    new_df = makeMDFDataFrame(new_mdf)
    
    
    #Deal with crdc_id
    liftover_df = crdcIDAdd(old_df, liftover_df, new_props, old_model_version, new_model_version)
    
    #Match by CDE first
    print("Map by CDE")
    liftover_df = cdeMatch(old_df, new_df, liftover_df, old_model_version, new_model_version)
    
    #Do exact string match for any fields that didn't match by CDE
    liftover_df = stringMatch(old_df, new_df, liftover_df, old_model_version, new_model_version)
    
            
    # Finally, add the realtionship columns to the mapping file.  These aren't model dependent.  I think.
    relations = configs['relationship_columns']
    for node, rellist in relations.items():
        for rel in rellist:
            liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_model_version, "lift_from_node": node, "lift_from_property": rel, 
                                                         "lift_to_version": new_model_version, "lift_to_node": node, "lift_to_property":rel, 
                                                         "lift_from_cde": "N/A", "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A",
                                                         "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
    
    required = configs['required_columns']
    for node, rellist in required.items():
        for rel in rellist:
            liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_model_version, "lift_from_node": node, "lift_from_property": rel, 
                                                         "lift_to_version": new_model_version, "lift_to_node": node, "lift_to_property":rel, 
                                                         "lift_from_cde": "N/A", "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A",
                                                         "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
    
    #Print out the liftover files
    liftover_df.to_csv(configs['mapping_file'], sep="\t", index=False)
    
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)