# Generates a liftover file between two versions of the same model
import bento_mdf
import argparse
from crdclib import crdclib
import pandas as pd


def getCDEInfo(term_object):
    # Get the ID and version of caDSR CDEs.  WE officially don't care about other CDEs
    for term in term_object.values():
        term_dict = term.get_attr_dict()
        if 'caDSR' in term_dict['origin_name']:
            return {'cdeid':term_dict['origin_id'], 'cdeversion': term_dict['origin_version']}
        else:
            return{'cdeid':None, 'cdeversion': None}
    

def cdeCheck(old_node, old_prop, new_node, new_prop, old_mdf, new_mdf):
    #This tries to provide information from CDEs, not just text matching
    old_concept = None
    new_concept = None
    relation = None
    if old_mdf.model.props[(old_node, old_prop)].concept is not None:
        old_concept = old_mdf.model.props[(old_node, old_prop)].concept.terms
    if new_mdf.model.props[new_node, new_prop].concept is not None:
        new_concept = new_mdf.model.props[(new_node, new_prop)].concept.terms
    if old_concept is not None:
        old_cde = getCDEInfo(old_concept)
    else:
        old_cde = {'cdeid':None, 'cdeversion': None}
    if new_concept is not None:
        new_cde = getCDEInfo(new_concept)
    else:
        new_cde = {'cdeid':None, 'cdeversion': None}
    #Need to check for blanks in cde id
    if(old_cde['cdeid'] == None) or (old_cde['cdeversion'] == None):
        relation = "Missing CDE"
    elif (old_cde['cdeid'] == new_cde['cdeid']) and (old_cde['cdeversion'] == new_cde['cdeversion']):
        relation = "Match CDE and Version"
    elif (old_cde['cdeid'] == new_cde['cdeid']) and (old_cde['cdeversion'] != new_cde['cdeversion']):
        relation = "Version mismatch"
    elif (old_cde['cdeid'] != new_cde['cdeid']):
        relation = "CDE mismatch"
    else:
        relation = "Indeterminant"
    return {'old_cde': old_cde['cdeid'], 'old_version': old_cde['cdeversion'], 'new_cde': new_cde['cdeid'], 'new_version': new_cde['cdeversion'], 'relation':relation}
        
        

def idAdd(old_node, old_property, liftover_df, new_props, old_version, new_version):
    #Handle the crdc_id issue
    if (old_node, old_property) in new_props:
        #Since the check was explicitly if (old_node, old_property) exists in new_props, anything passing means new==old for both and the statements below are NOT a typo
        liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_version, "lift_from_node": old_node, "lift_from_property": old_property, 
                                                         "lift_to_version": new_version, "lift_to_node": old_node, "lift_to_property": old_property, "lift_from_cde":"N/A", 
                                                         "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A", "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
        
    else:
        liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_version, "lift_from_node": old_node, "lift_from_property": old_property, 
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
    
    old_model_version = old_mdf.version
    print(f"Old Model version:\t{old_model_version}")
    new_model_version = new_mdf.version
    print(f"New Model Version:\t{new_model_version}")
    
    old_nodes = old_mdf.model.nodes
    old_nodes_list = list(old_nodes.keys())
    new_props = new_mdf.model.props
    
    for old_node in old_nodes_list:
        #Get the properties associated with the node
        old_node_properties = list(old_nodes[old_node].props.keys())
        for old_property in old_node_properties:
            notfoundprop = True
            if old_property == 'crdc_id':
                liftover_df = idAdd(old_node, old_property, liftover_df, new_props, old_model_version, new_model_version)
                notfoundprop = False
            else:
                for x in new_props:
                    if old_property in x:
                        new_node = x[0]
                        new_property = x[1]
                        cdeinfo = cdeCheck(old_node, old_property, new_node, new_property, old_mdf, new_mdf)
                        liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_model_version, "lift_from_node": old_node, "lift_from_property": old_property, 
                                                         "lift_to_version": new_model_version, "lift_to_node": new_node, "lift_to_property": new_property,
                                                         "lift_from_cde":cdeinfo['old_cde'], "lift_from_cdeversion": cdeinfo['old_version'], "lift_to_cde": cdeinfo['new_cde'],
                                                         "lift_to_cdeversion": cdeinfo['new_version'], "cde_relationship": cdeinfo['relation']}
                        notfoundprop = False
        if notfoundprop:
            liftover_df.loc[len(liftover_df)] = {"lift_from_version": old_model_version, "lift_from_node": old_node, "lift_from_property": old_property, 
                                                         "lift_to_version": new_model_version, "lift_to_node": "Orphan", "lift_to_property":"Orphan", 
                                                         "lift_from_cde": "N/A", "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A",
                                                         "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
            
    # Finally, add the realtionship columns to the mapping file.  These aren't model dependent.  I think.
    relations = configs['relationship_columns']
    for node, rellist in relations.items():
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