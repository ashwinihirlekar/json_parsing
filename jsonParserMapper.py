#!/usr/bin/env python
import json
import sys
import csv
import os
def to_string(s):
    try:
        return str(s)
    except:
        # Change the encoding type if needed
        return s.encode('utf-8')
def sort_rec(final_info):
    f = str(final_info).split(delimiter)
    rec = f[21] + delimiter +f[22] + delimiter + f[18] + delimiter + f[1][0:11] + delimiter + f[1] + delimiter + f[3] + delimiter + f[2] + delimiter + f[4] + delimiter + f[0] + delimiter + f[15] + delimiter + f[5] + delimiter + f[6] + delimiter + f[7] + delimiter + f[17] + delimiter + f[8] + delimiter + f[16] + delimiter + f[20] + delimiter + f[19] + delimiter + f[9] + delimiter + f[14] + delimiter + f[10] + delimiter + f[11] + delimiter + f[12] + delimiter + f[23] + delimiter + f[24] + delimiter + f[28] + delimiter + f[26] + delimiter + f[27] + delimiter + f[25] + delimiter + f[13] +  delimiter + f[29][0:10]+" "+f[29][11:19] + '\n' 
    sys.stdout.write(rec)
    

def make_row(man_keys,obj_dict):
    delimiter="\x01"
    rec=""
    keys=obj_dict.keys()
    for man_key in man_keys:
        if man_key in keys:
            rec += to_string(obj_dict[man_key])+delimiter
        else:
            rec += "" + delimiter
    return rec

def csv_write(processed_data,output_file):
    f = open(output_file, 'a+')
    for row in processed_data:
        f.write(to_string(row) + '\n')
    f.close()


def read_dict(obj):
    list_str = ""
    delimiter="\x01"
    for sub_obj in obj:
        if type(obj[sub_obj]) is list:
            continue
        else:
            list_str += obj[sub_obj] + delimiter
    return list_str

delimiter="\x01"
for json_input in sys.stdin:
    if json_input == '\n':
	continue
    parsed_input=json.loads(json_input)
    global_info = parsed_input["global_response"]
    card_info = parsed_input["customer_info"]
    all_info = {}
    treatments = parsed_input["treatments"]
    for treat in treatments:
        component_treatments = treat["component_treatments"]
        for comp in component_treatments:
            treat_meta = comp["treatment_metadata"]
            all_info = {}
            for meta in treat_meta:
                for key in meta:
                    printer = "";
                    sub_obj = meta[key]
                    cards = []
                    card_dt = ""
                    if key == "card_objects":
                        card_det_string = ""
                        for card_obj in sub_obj:
                            app = ""
                            app += card_obj["card_number"] + delimiter
                            if len(card_obj["card_number"]) < 13:
                                raise ValueError('card length less than 13 --> card_number:',card_obj["card_number"])
                            app += card_obj["cobrand_prtr_acct"] + delimiter
                            man_fin_keys = ["cmv", "cr_cd", "first_yr_pti", "first_yr_revn", "first_yr_spend",
                                            "line_of_cr", "prod_unit_metric", "scnd_yr_pti",
                                            "scnd_yr_revn"]
                            app += make_row(man_fin_keys, card_obj["financial_info"])
                            cards.append(app)
                        all_info["card"] = cards
                    other = []
                    if key != "card_objects":
                        printer += to_string(meta["treatment_identifier"]) + delimiter
                        printer += to_string(meta["display_rank"]) + delimiter
                        printer += to_string(meta["rsvp_cd"]) + delimiter
                        printer += meta["treatment_duration"]["display_end_date"] + delimiter
                        printer += meta["treatment_content"]["treatment_name"] + delimiter
                        if key == "additional_parameters":
                            man_add_param_keys = ["treatment_score", "cmpgn_in", "offr_ty","offr_sub_ty", "bus_cd", "bus_unit"]
                            add_param_row = make_row(man_add_param_keys, sub_obj)
                            printer += add_param_row
                            sub = sub_obj["talking_points"]
                            if len(sub)==0:
                                row = printer+delimiter*6
                                other.append(row)
                                all_info["other"] = other
                            else:
                                for item in sub:
                                    man_item_keys = ["key_benefit", "trigger", "trigger_score", "transition_statement","true_need", "trigger_id"]
                                    row = printer + make_row(man_item_keys, item)
                                    other.append(row)
                                    all_info["other"] = other
            cards_list = all_info["card"]
            other = all_info["other"]
            final_info = []
            card_xref = card_info["cust_xref_id"]
	    response_time = global_info["response_timestamp"]
            for c in cards_list:
                for o in other:
                    rec = card_xref + delimiter + c + o + response_time
                    sort_rec(rec)


