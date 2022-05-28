import config
import copy
from utils import util_log
import textdistance

from audit.const import tasks, steps
from itertools import permutations
from re import finditer
from Levenshtein import distance ,jaro, median, hamming
JARO_THRESHOLD = 0.8
JACCARD_THRESHOLD = 0.5

""""
STEPS:
    1 extracting entities for cells with single cell
    2 parsing the retrieved result and append them to the list of the candidates 
    3 making list of terms for the values of the cells. The values has at least two terms
        3.1 remove the terms that has digit from the list
        3.2 remove single letters
        3.3 generate a list of the all possible combination of the remaining terms
            3.3.1 remove the combination that has three terms
    4 extracting entities for the term in the label and the title
        
"""

LABEL_LENGTH  = 5

def isLongLabel(label):
    if len(label.split()) < LABEL_LENGTH:
            return False
    return True

def hasANumber(term):
    for letter in term:
        if letter.isdigit():
            return True
    return False

def __replace_character(term):
    matches = finditer('(?<=[\w])[-~,;\/\\_.\s]+(?=[\w])', term)
    if matches:
        term = term.replace('_', ' ')
    return term

def __jaro_similarity(first_term, second_term):
    return jaro(first_term,second_term)

def __combine_dicts(dict1, dict2 ):
    list_dict = []
    index = 0
    for k, v in dict1.items():
        list_dict.append({k: []})
        one = [i for i in dict2[k]]
        for idx in range(len(one)):
            # if (__jaro_similarity(one[idx]['itemLabel'], k)) > 0.8:
            # if (__jaro_similarity(one[idx]['itemLabel'], text)) > 0.8:
            list_dict[index][k].append(one[idx])
        for idx in range(len(v)):
            # if (__jaro_similarity(v[idx]['itemLabel'],k)) > 0.8:
            # if (__jaro_similarity(v[idx]['itemLabel'],text)) > 0.8:
            list_dict[index][k].append(v[idx])
        index = index + 1
    return list_dict

def __jaccard_similarity(can, src):
    last_cands = []
    clean_val_tokens = list(set(src.lower().split()))
    for c in can:
        for label in c['labels']:
            if label != []:
                label_tokens = list(set(label.lower().split()))
                match = textdistance.jaccard(label_tokens, clean_val_tokens)
                if match >= JACCARD_THRESHOLD:
                    last_cands.append(c)
    return  last_cands



def generate(pTable, proxyService):


    obj_cells = pTable.getTargets(cea=True)

    comb_values= {'comb_values': []}
    for cell in obj_cells:
        cell.update(comb_values)
    cells = [cell for cell in obj_cells if not cell['cand']]
    target_cells_cnt = len(cells)

    cells_values = [cell['clean_val'] for cell in cells if not cell['cand'] and len(cell['clean_val'].split(" "))==1]#and len(cell['clean_val'].split(" "))==1
    cells_values = list(set(cells_values))

    #1 extacting entities for cells with single cell
    res_ent = proxyService.get_entity_entitySearch_no_inTitle.send([cells_values])
    # res = proxyService.get_entity_entitySearch_with_inTitle.send([cells_values]) #didn't give me entities
    res_search= proxyService.get_entity_Search_no_inLabel.send([cells_values])

    both = __combine_dicts(res_ent, res_search)

    res = {}
    res_test={}
    for el in both:
        res.update(el)
    # res_test.update(el for el in both)

    qids = []

    #parsing the retrieved result and append them to the list of the candidates
    for key ,values  in res.items():

        elements = [x for x in obj_cells if x['clean_val'] == key]
        
        
        
        can= []
        for value in values:
            # score = jaro(value['itemLabel'], elements[0]['clean_val'])
            # if score > JARO_THRESHOLD:
            if isLongLabel(value['itemLabel']):
                continue
            # if textdistance.jaccard(elements[0]['clean_val'], value['itemLabel']) < JACCARD_THRESHOLD:
            #     continue
            if value['item'] not in qids:
                labels = proxyService.get_labels_for_lst.send([[value['item']], 'en'])

                qids.append(value['item'])
                temp = {
                    'labels': [label['l'] for label in labels[value['item']] ],
                    'uri': [value['item']]
                    # 'score': score
                }
                can.append(temp)

        qids.clear()


        if can:
            #Filtering the entites with Jaccard similairity
            last_cands= __jaccard_similarity(can, elements[0]['clean_val'])

            for element in elements:
                for c in last_cands:
                    element['cand'].append(c)


    #checking the remaining cells without candidates
    #call the remaing cells must have at least two terms
    cells = [cell for cell in cells if not cell['cand']]
    # [Audit] calculate cnts for auditing
    remaining_cnt = len(cells)
    solved_cnt = target_cells_cnt - remaining_cnt

    #adding new values for the reamining cells
    cells_values = [cell['clean_val'] for cell in cells if not cell['cand']]
    cells_values = list(set(cells_values))

    #3 Separateing the cells
    for cell in cells_values:
        # 3.1 remove the terms that has digit from the list
        free_char_cell = __replace_character(cell)
        terms = free_char_cell.split(' ')
        if len(terms)> 4:
            continue
        temp_values = []
        for term in terms:
            #no numbers in the term and the single letter will be ignored
            # 3.2 remove single letters
            if not hasANumber(term) and len(term)>1:
                temp_values.append(term)


        original_value=[cel['clean_val'] for cel in cells if cel['clean_val'] == cell]

        original_value = list(set(original_value))

        final_lst_values = []
        final_lst_values.append(original_value[0])
        # 3.3 generate a list of the all possible combination of the remaining terms
        for i in range (len(temp_values), 0 , -1):
            combinations = permutations(temp_values, i)

            #generating the possible combinations and quering them directly and adding the values and the cand to obj_cell
            for words in list(combinations):
                # 3.3.1 remove the combination that has three terms
                
                if len(words) > 3 :
                    continue
                e = " ".join(word for word in words )
                if not e == cell:
                    final_lst_values.append(e)

        #4 extracting entities for the term in the label and the title

        res_entity = proxyService.get_entity_entitySearch_no_inTitle.send([final_lst_values])
        # res_generate = proxyService.get_entity_Generate_with_inTitle.send([final_lst_values])
        res_search = proxyService. get_entity_Search_no_inLabel.send([final_lst_values])
        results = __combine_dicts(res_entity,res_search)


        item = [x for x in obj_cells if x['clean_val'] == cell]

        # item[0]['comb_values'] = [{key:val} for key , val in results.items() if val]
        # item[0]['cand'] = [{key: val} for key, val in res.items() if val]

        list_dic_values = []
        uris=[]
        # Add the entities to the cell candidates
        for result in results:
            for key, val in result.items():
                # for i in range (len(val)):
                    for entity in val:
                        if isLongLabel(entity['itemLabel']):
                            continue
                        if entity['item'] not in uris:
                            
                            tmp_list= []
                            uris.append(entity['item'])
                            labels = proxyService.get_labels_for_lst.send([[entity['item']], 'en'])
                            for l_key, l_v in labels.items():
                                for ll in l_v:
                                    tmp_list.append(ll['l'])
                            
                            if tmp_list != []:
                                temp = {'labels': tmp_list,
                                        'uri': [entity['item']],
                                        'comb': key}
                                list_dic_values.append(temp)

        for elem in range(len(item)):
            item[elem]['cand'] =  list_dic_values
        

    #check step
    cells = [cell for cell in cells if not cell['cand']]
    # [Audit] calculate cnts for auditing
    remaining_cnt = len(cells)
    solved_cnt = target_cells_cnt - remaining_cnt

    pTable.checkPoint.addCheckPoint('cea_remove_dupes', pTable, cea=True)


    print("ff")
