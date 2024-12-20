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

def cdeMatch(old_df, new_df, liftover_df, old_version, new_version, old_handle, new_handle):
    #Instead of string matching, match by CDE ID.  Ignore CDE version
    for index, row in old_df.iterrows():
        # Meant to handle previous mappings like crdc_id
        if row['property'] not in liftover_df['lift_from_property']:
            if (new_df['cdeid'].eq(row['cdeid'])).any():
                temp_df = new_df.loc[new_df['cdeid'] == row['cdeid']]
                for tempindex, temprow in temp_df.iterrows():
                    cde_relationship = cdeCheck2(row['cdeid'], row['cdeversion'], temprow['cdeid'], temprow['cdeversion'])
                    liftover_df.loc[len(liftover_df)]= {"lift_from_model": old_handle, "lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_model": new_handle, "lift_to_version": new_version, "lift_to_node": temprow['node'], "lift_to_property": temprow['property'],
                                                         "lift_from_cde":row['cdeid'], "lift_from_cdeversion": row['cdeversion'], "lift_to_cde": temprow['cdeid'], 
                                                         "lift_to_cdeversion": temprow['cdeversion'], "cde_relationship": cde_relationship}
    return liftover_df
            
    
def stringMatch(old_df, new_df, liftover_df, old_version, new_version, old_handle, new_handle):
    for index, row in old_df.iterrows():
        #Check that the property hasn't already been mapped by CDE
        if row['property'] not in liftover_df['lift_from_property']:
            # If no, then see if there is a string match
            if row['property'] in new_df['property']:
                temp_df = new_df.loc[new_df['property'] == row['property']]
                for tempindex, temprow in temp_df.iterrows():
                    cde_relationship = cdeCheck2(row['cdeid'], row['cdeversion'], temprow['cdeid'], temprow['cdeversion'])
                    liftover_df.loc[len(liftover_df)]= {"lift_from_model": old_handle, "lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_model": new_handle, "lift_to_version": new_version, "lift_to_node": temprow['node'], "lift_to_property": temprow['property'],
                                                         "lift_from_cde":row['cdeid'], "lift_from_cdeversion": row['cdeversion'], "lift_to_cde": temprow['cdeid'], 
                                                         "lift_to_cdeversion": temprow['cdeverion'], "cde_relationship": cde_relationship}
            # If no string match
            else:
                cde_relationship = "Unmapped"
                liftover_df.loc[len(liftover_df)]= {"lift_from_model": old_handle, "lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_model": new_handle, "lift_to_version": new_version, "lift_to_node": "N/A", "lift_to_property": "N/A", "lift_from_cde":row['cdeid'], 
                                                         "lift_from_cdeversion": row['cdeversion'], "lift_to_cde": "N/A", "lift_to_cdeversion": "N/a", "cde_relationship": cde_relationship}
                
        return liftover_df
    

def crdcIDAdd(old_df, liftover_df, new_props, old_version, new_version, old_handle, new_handle):
    old_crdc_df = old_df.loc[old_df['property'] == 'crdc_id']
    for index, row in old_crdc_df.iterrows():
        if (row['node'], row['property']) in new_props:
            liftover_df.loc[len(liftover_df)] = {"lift_from_model": old_handle, "lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_model": new_handle, "lift_to_version": new_version, "lift_to_node": row['node'], "lift_to_property": row['property'], "lift_from_cde":"N/A", 
                                                         "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A", "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
        else:
            liftover_df.loc[len(liftover_df)] = {"lift_from_model": old_handle, "lift_from_version": old_version, "lift_from_node": row['node'], "lift_from_property": row['property'], 
                                                         "lift_to_model": new_handle, "lift_to_version": new_version, "lift_to_node": "N/A", "lift_to_property": "N/A", "lift_from_cde":"N/A", 
                                                         "lift_from_cdeversion": "N/A", "lift_to_cde": "N/A", "lift_to_cdeversion": "N/A", "cde_relationship": "N/A"}
        
    return liftover_df



def getcdeJSON(cdeid, cdeversion):
    # So this is a thing.  Sometimes a query can be "success" but has a message "No record found"
    # This definitely happens with retired version numbers.  So re-query without the version and just
    # get the latest version
    
   # print(f"Getting CDE Info for ID {cdeid} version {cdeversion} ")
    cadsrjson = crdclib.getCDERecord(cdeid, cdeversion)
    #print("CDE Info obtained")
    #if cadsrjson['status'] == 'success':
    #    print(f"TRUE Status is {cadsrjson['status']}")
    #if cadsrjson['message'] == 'Record found.':
    #    print(f"TRUE Message is {cadsrjson['message']}")
    if ((cadsrjson['status'] == 'success') and (cadsrjson['message'] == 'Record found.')):
        #print(f" IF STEP: Status: {cadsrjson['status']}\tMessage: {cadsrjson['message']}")
        return {"cadsrjson": cadsrjson, "id":cdeid, "version": cdeversion}
    elif ((cadsrjson['status'] == 'success') and (cadsrjson['message'] == 'No record found.')):
        #print(f"ELIF STEP: Status: {cadsrjson['status']}\tMessage: {cadsrjson['message']}")
        cadsrjson = crdclib.getCDERecord(cdeid)
        cdeversion = cadsrjson['DataElement']['version']
        #logfile.write(cadsrjson)
        return {"cadsrjson": cadsrjson, "id":cdeid, "version": cdeversion}
    else:
        #print(f"ELSE STEP: Status: {cadsrjson['status']}\tMessage: {cadsrjson['message']}")
        #logfile.write(str(cadsrjson))
        return {"cadsrjson": None, "id":cdeid, "version": cdeversion}



def createValueDF(mdf_df, verbose=False):
    columns = ["node", "property", "cdeID", "cdeVersion","permissibleValue", "valueTerm", "ncitCode"]
    value_df = pd.DataFrame(columns=columns)
    #lf = open(r".\examples\troubleshooting.txt", "a", encoding="utf-8")

    for index, row in mdf_df.iterrows():
        #print(row)
        if row['cdeid'] is not None:
            node = row['node']
            prop = row['property']
            cdeid = row['cdeid']
            cdeversion = row['cdeversion']
            #print(f"Starting CDE: {cdeid}\tStarting Version:{cdeversion}")
            #casdrjson = crdclib.getCDERecord(cdeid, cdeversion)
            #lf.write(f"CDE ID: {cdeid}\t CDE Version: {cdeversion}\n")
            #print(f"CDE ID: {cdeid}\t CDE Version: {cdeversion}\n")
            results = getcdeJSON(cdeid, cdeversion)
            #lf.write(str(results)+"\n")
            cadsrjson = results['cadsrjson']
            cdeid = results['id']
            cdeversion = results['version']
            #print(f"CDE ID: {cdeid}\t CDE Version: {cdeversion}\n")
            #if cadsrjson is None:
            #    print("cadsrjson is None")
            #else:
            #    print(cadsrjson)
            if cadsrjson is not None:
                #print("cadsrjson is not None")
                if len(cadsrjson['DataElement']['ValueDomain']['PermissibleValues']) > 0:
                    #print("there are permissible values")
                    for permissiblevalue in cadsrjson['DataElement']['ValueDomain']['PermissibleValues']:
                        pv = permissiblevalue['value']
                        #print(pv)
                        if len(permissiblevalue['ValueMeaning']['Concepts']) > 0:
                            #print("there are concepts")
                            for concept in permissiblevalue['ValueMeaning']['Concepts']:
                                value_df.loc[len(value_df)] = {"node": node, "property": prop, "cdeID": cdeid, "cdeVersion": cdeversion, 
                                                               "permissibleValue": pv, "valueTerm": concept['longName'], "ncitCode": concept['conceptCode']}
                                #print(str({"cdeID": cdeid, "cdeVersion": cdeversion, "valueTerm": concept['longName'], "ncitCode": concept['conceptCode']}))
    # Remove duplicate lines
    value_df.drop_duplicates(keep='first', inplace=True)
    return value_df
    
    
    
def conceptMatch(old_value_df, new_value_df, value_liftover_df, old_model_version, new_model_version, old_model_handle, new_model_handle):
    # TODO: rethink output, row[valueTerm] is wrong.  And probably need a node reference
    # Probably need node:property:pv
    for index, row, in old_value_df.iterrows():
        # First, look for an NCIt code match
        if new_value_df['ncitCode'].eq(row['ncitCode']).any():
            temp_df = new_value_df.loc[new_value_df['ncitCode'] == row['ncitCode']]
            for tempindex, temprow in temp_df.iterrows():
                relationship = "Concept Code Match"
                # ["node", "property", "cdeID", "cdeVersion","permissibleValue", "valueTerm", "ncitCode"]
                # value_columns = ["lift_from_model","lift_from_version", "lift_from_node", "lift_from_property", "lift_from_concept", "lift_from_conceptCode",
                #         "lift_to_model", "lift_to_version", "lift_to_node", "lift_to_property", "lift_to_concept", "lift_to_concepCode", "concept_relationship"]
                value_liftover_df.loc[len(value_liftover_df)] = {"lift_from_model": old_model_handle, "lift_from_version": old_model_version, "lift_from_node": row['node'], 
                                                                 "lift_from_property": row['property'], "lift_from_concept": row['permissibleValue'], "lift_from_conceptCode": row['ncitCode'],
                                                                 "lift_to_model": new_model_handle, "lift_to_version": new_model_version, "lift_to_node": temprow['node'], 
                                                                 "lift_to_property": temprow['property'], "lift_to_concept": temprow['permissibleValue'], 
                                                                 "lift_to_conceptCode": temprow['ncitCode'], "concept_relationship": relationship}
        # If no code match, look for string match on valueTerm, which is probably more standardized than the PV
        elif new_value_df['valueTerm'].eq(row['valueTerm']).any():
            temp_df = new_value_df.loc[new_value_df['valueTerm'] == row['valueTerm']]
            for tempindex, temprow in temp_df.iterrows():
                relationship = "Value Term Match"
                value_liftover_df.loc[len(value_liftover_df)] = {"lift_from_model": old_model_handle, "lift_from_version": old_model_version, "lift_from_node": row['node'], 
                                                                 "lift_from_property": row['property'], "lift_from_concept": row['permissibleValue'], "lift_from_conceptCode": row['ncitCode'],
                                                                 "lift_to_model": new_model_handle, "lift_to_version": new_model_version, "lift_to_node": temprow['node'], 
                                                                 "lift_to_property": temprow['property'], "lift_to_concept": temprow['permissibleValue'], 
                                                                 "lift_to_conceptCode": temprow['ncitCode'], "concept_relationship": relationship}
        # and a complete strike out
        else:
            relationship = "No match"
            value_liftover_df.loc[len(value_liftover_df)] = {"lift_from_model": old_model_handle, "lift_from_version": old_model_version, "lift_from_node": row['node'], 
                                                                 "lift_from_property": row['property'], "lift_from_concept": row['permissibleValue'], "lift_from_conceptCode": row['ncitCode'],
                                                                 "lift_to_model": new_model_handle, "lift_to_version": new_model_version, "lift_to_node": "N/A", 
                                                                 "lift_to_property": "N/A", "lift_to_concept": "N/A", 
                                                                 "lift_to_conceptCode": "N/A", "concept_relationship": relationship}
    return value_liftover_df

def main(args):
    configs = crdclib.readYAML(args.configfile)
    
    #Dataframe setup
    columns = ["lift_from_model", "lift_from_version", "lift_from_node", "lift_from_property", 
               "lift_to_model", "lift_to_version", "lift_to_node", "lift_to_property",
               "lift_from_cde", "lift_from_cdeversion", "lift_to_cde", "lift_to_cdeversion", "cde_relationship"]
    liftover_df = pd.DataFrame(columns=columns)
    
    old_mdf = bento_mdf.MDF(*configs["old_version_files"])
    new_mdf = bento_mdf.MDF(*configs["new_version_files"])
    
    new_props = new_mdf.model.props
    
    old_model_version = old_mdf.version
    new_model_version = new_mdf.version
    old_model_handle = old_mdf.handle
    new_model_handle = new_mdf.handle
    
    # Create the dataframes
    old_df = makeMDFDataFrame(old_mdf)
    new_df = makeMDFDataFrame(new_mdf)

    #Set up value mapping if requested
    if configs["do_value_mapping"]:
        #Need to loop through the model, get the CDEs with terms
        old_value_df = createValueDF(old_df, args.verbose)
        if args.verbose:
            old_value_df.to_csv(r".\examples\old_value_df.csv", sep="\t", index=False)
        new_value_df = createValueDF(new_df, args.verbose)
        if args.verbose:
            new_value_df.to_csv(r".\examples\new_value_df.csv", sep="\t", index=False)
        value_columns = ["lift_from_model","lift_from_version", "lift_from_node", "lift_from_property", "lift_from_concept", "lift_from_conceptCode",
                         "lift_to_model", "lift_to_version", "lift_to_node", "lift_to_property", "lift_to_concept", "lift_to_conceptCode", "concept_relationship"]
        value_liftover_df = pd.DataFrame(columns=value_columns)
        
        #Do the NCIT concept code mapping
        value_liftover_df = conceptMatch(old_value_df, new_value_df, value_liftover_df, old_model_version, new_model_version, old_model_handle, new_model_handle)
        value_liftover_df.drop_duplicates(subset=['lift_from_concept', 'lift_to_concept'], keep='first', inplace=True)
    
    
    #Deal with crdc_id
    liftover_df = crdcIDAdd(old_df, liftover_df, new_props, old_model_version, new_model_version, old_model_handle, new_model_handle)
    
    #Match by CDE first
    liftover_df = cdeMatch(old_df, new_df, liftover_df, old_model_version, new_model_version, old_model_handle, new_model_handle)
    
    #Do exact string match for any fields that didn't match by CDE
    liftover_df = stringMatch(old_df, new_df, liftover_df, old_model_version, new_model_version, old_model_handle, new_model_handle)
    
            
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
    if configs['do_value_mapping']:
        value_liftover_df.to_csv(configs['value_mapping_file'], sep="\t", index=False)
    
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument("-v", "--verbose", action='store_true', help="Verbose Output")

    args = parser.parse_args()

    main(args)