from pyparsing import Each

import config
import copy
from utils import util_log
import config
from audit.const import tasks, steps, methods

from audit.const import tasks, steps
from itertools import permutations
from re import finditer

from os.path import realpath, join
import json

ENTITY_QID ="Q35120"
WIKIMEDIA_DISAMBIGUATION_PAGE = "Q4167410"
def generate(pTable, proxyService):
    
    obj_cells = pTable.getTargets(cea=True)

    obj_cols = pTable.getTargets(cta = True)

    targets_cols=[x['col_id'] for x in obj_cols]


    # only process columns which we dont have solutions so far
    # unsolved_col_ids = [col['col_id'] for col in pTable.getCols(unsolved=True)]

    # get unique list of cell candidates
    uris = [cand['uri'][0] for cell in obj_cells for cand in cell['cand']]
    
    uris = list(set(uris))
    # uris = set(uris)
    """
    
    1- getting the type of each candidate
        
    2- count the types of the entity candidates 
    3 - add a new key to the columns object e.g. header candidate and the rest of the types of the elements
     
    """

    res = proxyService.get_type_subclass.send([uris])  

    for cell in obj_cells:
        for cand in cell['cand']:
            if (cand['uri'][0] in res) and res[cand['uri'][0]]:
                cand['types'] = [item['type'] for item in res[cand['uri'][0]]] # if item['type'] != WIKIMEDIA_DISAMBIGUATION_PAGE
                # cell['types'] = [{'uri': item['type'], 'labels': item['typeLabel]} for item in res[cand['uri'][0]]]
            else:
                # cand['types']= [{'uri':ENTITY_QID, 'labels':'entity'}]
                cand['types'] =[ENTITY_QID]
                # for item in res[cand['uri'][0]]:
                         # cand['types'].append(item['type'])





    # aggregate types on cell level

    for cell in obj_cells:
        types = set()
        for cand in cell['cand']:
            if 'types' in cand:
                for typeCand in cand['types']:
                    # if typeCand == ENTITY_QID:
                    #     types.append({'labels':'entity', 'uri':typeCand})
                    #     continue
                    types.add(typeCand)
        cell['types'] = list(types)




    # aggregate types on col level, only object because the elements in quantity columns does not have candidate
    #for the qunatitly columns we will look at the header to see if it accept quantities
    # cols = [col for col in pTable.getCols(col_id=unsolved_col_ids, onlyObj=True)]
    cols = [col for col in pTable.getCols(col_id=targets_cols)]


    # [Audit] How many cols should be solved
    target_cols_cnt = len(cols)

    # [Audit] init solved cnt of cols
    solved_cnt = 0

    for col in cols:
        # get all cells for this column except the header cell
        # support is only affected by cells that have types, empty types are noise!

        header_cell = pTable.getCells(row_id=0, col_id=col['col_id'])
        cells = [cell for cell in obj_cells if (cell['col_id'] == col['col_id']) and cell['types'] and cell['row_id'] != 0]

        types = list(set([t for cell in cells for t in cell['types']]))
        for cand in header_cell[0]['cand']:
            types.append(cand['uri'])

        # [{t['uri']:t['labels']}for cell in cells for t in cell['types']]
        # [Audit] if some types are retrieved then count as solved
        if types:
            solved_cnt = solved_cnt + 1


        # store candidates
        col['cand'] = [{
            'uri': t
        } for t in types]

    # [Audit] calculate remaining cols
    remaining_cnt = target_cols_cnt - solved_cnt

    # [Audit] filter cols on not having candidates
    remaining = [col for col in cols if not col['cand']]

    # [Audit] get important keys only
    remaining = [pTable.audit.getSubDict(cell, ['col_id']) for cell in remaining]

    # [Audit] add audit record
    pTable.audit.addRecord(tasks.CTA, steps.creation, methods.default, solved_cnt, remaining_cnt, remaining)


    print ('here')

