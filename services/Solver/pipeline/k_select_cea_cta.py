from itertools import permutations

import textdistance

import config
from utils.string_util import get_most_similar
from audit.const import tasks, steps, methods
import math

JACCARD_THRESHOLD = 0.5
TAXON_RANK_QID = 'Q427626'
TAXON_QID = 'Q16521'

def __jaccard_similarity_full(can, src):

    best_match_score = -10
    clean_val_tokens = list(set(src.lower().split()))
    best_cand ={}
    for c in can:
        for label in c['labels']:
            label_tokens = list(set(label.lower().split()))
            score = textdistance.jaccard(label_tokens, clean_val_tokens)
            if score > best_match_score:
                best_match_score = score
                best_cand = c
        #the best candidate is found
        if score == 1:
            break
           
    return  best_cand



def select(pTable, purged=False,proxyService=None , minSupport=0.5):
    """
    for all OBJECT cells select the candidate whose value is most similar to the original one
    ties are broken by popularity, i.e. the entity with the most incoming links
    can be executed either on the remaining candidates or the purged candidates
    """
    print("select_cea_cta")
    cols = pTable.getTargets(cta=True)


    for col in cols:

        # check, if we have to process this column at all
        if col['type'] == "QUANTITY" or col['cand'] == []:
            continue
        cells = pTable.getCells(col_id=col['col_id'])
        
        #select the best candidate, cells of one column
        rankcc = {}
        types_uris_cache={}
        for cell in cells:
            if cell['row_id'] != 0 and cell['clean_val']:
                best_cand=__jaccard_similarity_full(cell['cand'],cell['clean_val'])
                if not best_cand:
                    continue
                cell['sel_cand'] = {'uri':best_cand['uri'][0],
                                    'labels': best_cand['labels']}
                # check if there is a taxon QID, specific for BIODIVTAB DATASET
                if best_cand['types'][0] == TAXON_QID:
                    #check the cache variable
                    if best_cand['uri'][0] in types_uris_cache:
                        res= {best_cand['uri'][0]: types_uris_cache[best_cand['uri'][0]]}
                    else:
                        res = proxyService.get_taxon_rank.send([best_cand['uri']])
                        types_uris_cache.update(res)
                        
                    if len(res[best_cand['uri'][0]]):
                        for itemKey, rankVals in res.items():
                            for rankval in rankVals:
                                cell['sel_cand'].update(
                                    {'rank': rankval['rank'], 'rankLabel': rankval['rankLabel']})
                                if rankval['rank'] in rankcc:
                                    rankcc[rankval['rank']] += 1
                                else:
                                    rankcc.update({rankval['rank']: 1})
                                
                #no rank
                else:
                    if cell['sel_cand']['uri'] in rankcc:
                        rankcc[cell['sel_cand']['uri']] += 1
                    else:
                        rankcc.update({cell['sel_cand']['uri']: 1})

        #after geting the most types or rank in case taxon
        max_count = list(rankcc.values())
        max_key = list(rankcc.keys())
        freq_rank = max_key[max_count.index(max(max_count))]


        #check if the freq type/rank between header candidates
        header = pTable.getCell(col['col_id'], 0)
      
        
        for h_cand in header['cand']:
            if freq_rank in h_cand['uri']:
                header['sel_cand'] = {'uri': h_cand['uri'][0], 'labels': h_cand['labels']}
                col['sel_cand'] =  {'uri': h_cand['uri'][0], 'labels': h_cand['labels']}
                break

    
    # get all the columns with type QUANTITY, specific for BIODIVTAB DATASET
    cols = [col for col in pTable.getTargets(cta=True) if col['type'] == 'QUANTITY' or col['cand'] == [] or col['sel_cand'] is None]
    for col in cols:
        
        temp_sel_cand = {}
        
        header_cell = pTable.getCell(col['col_id'], 0)
        
        if header_cell['cand'] == []:
            continue
        
        #only QUANTITY type columns
        if col['type'] == 'QUANTITY':
            for header_cand in header_cell['cand']:
                # check if chemical_element between the candidates
                try:
                    res_chemical_element = proxyService.get_chemical_element.send([header_cand['uri']])
                    if len(res_chemical_element[header_cand['uri'][0]]):
                        temp_sel_cand = {'uri': header_cand['uri'][
                            0], 'labels': header_cand['labels']}
                        break
                except:
                    print('error chemical_element')

                # check if oxyanion between the candidates
                try:
                    res_oxyanion = proxyService.get_oxyanion.send([header_cand['uri']])
                    if len(res_oxyanion[header_cand['uri'][0]]):
                        temp_sel_cand = {'uri': header_cand['uri'][0], 'labels': header_cand['labels']}
                        break
                except:
                    print('error get oxyanion')

                # check if quantity between the candidates
                try:
                    res_quantity = proxyService.get_quantity.send([header_cand['uri']])
                    if len(res_quantity[header_cand['uri'][0]]):
                        temp_sel_cand = {'uri': header_cand['uri'][0], 'labels': header_cand['labels']}
                        break
                except:
                    print('error get quantity')

                try:

                    res_unit_of_measurement = proxyService.get_unit_of_measurement.send([header_cand['uri']])
                    if len(res_unit_of_measurement[header_cand['uri'][0]]):
                        temp_sel_cand = {'uri': header_cand['uri'][0], 'labels': header_cand['labels']}
                        break
                except:
                    print("error unit of measurement")

            # the last step jaccard similarty and the most number of labels which means that this the most popular

        if temp_sel_cand == {}:
            match = __jaccard_similarity_full(header_cell['cand'], header_cell['clean_val'])
            temp_sel_cand = {'uri': match['uri'][0], 'labels': match['labels']}
        header_cell['sel_cand'] = temp_sel_cand
        col['sel_cand'] = temp_sel_cand


#     get all the unsolved cols
#     cols = [col for  col in pTable.getCols(unsolved=True)]

#     # get all object cells
#     cells = [cell for cell in pTable.getCells(unsolved=True, onlyObj=True)]
    
#     # [Audit] How many cells should be solved a.k.a must have sel_cand
#     target_cells_cnt = len(cells)
    
#     # [Audit] unsolved cells a.k. cells with no sel_cand
#     remaining = []
    
#     # [Audit] How many cells with modified sel_cand by this method
#     solved_cnt = 0
    
#     # get the selected candidate
#     for cell in cells:
    
#         # select the candidates to consider
#         if purged:
#             cands = cell['purged_cand']
#         else:
#             cands = cell['cand']
    
#         # skip cells without candidates
#         if not cands:
#             # [Audit] cells with no candidates are still remaining!
#             remaining.extend([cell])
#             continue
    
#         # if there is only one candidate, we select that one
#         if len(cands) == 1:
#             cell['sel_cand'] = {'uri':cands[0]['uri'][0], 'label':cands[0]['labels'][0]}
#             # [Audit] special case solution
#             solved_cnt = solved_cnt + 1
#             if purged:
#                 cell['cand'] = [cell['sel_cand']]
#             continue
    
#         # for all others check the string similarity
#         # best_match = get_most_similar(cands, cell['value'], proxyService)
#         best_match = get_most_similar(cands, cell['clean_val'], proxyService)
    
#         # add match to candidate
#         cell['sel_cand'] = {'uri':best_match['uri'][0], 'labels':best_match['labels'],'types':best_match['types'][0]}
    
#         # [Audit] if change detected then count as solved otherwise, add to remaining
#         solved_cnt = solved_cnt + 1
    
#         if purged:
#             cell['cand'] = [cell['sel_cand']]

#     [Audit] calculate remaining cnt
#     remaining_cnt = target_cells_cnt - solved_cnt
    
#     # [Audit] get important keys only
#     remaining = [pTable.audit.getSubDict(cell, ['value', 'clean_val', 'row_id', 'col_id'])
#                  for cell in remaining]
    
#     # [Audit] add audit record
#     pTable.audit.addRecord(tasks.CEA, steps.selection,
#                            methods.stringSimilarity, solved_cnt, remaining_cnt, remaining)
