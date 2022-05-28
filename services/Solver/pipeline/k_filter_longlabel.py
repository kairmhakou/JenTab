import config
import utils.string_dist as sDist

LABEL_LENGTH  = 5

def isLongLabel(labels):
    for label in labels:
        if label and len(label.split()) < LABEL_LENGTH:
            return False
    return True


def doFilter(pTable):
    

    print("---k_filter_longLabel---")

    # get all object cells
    # obj_cells = pTable.getCells(unsolved=True, onlyObj=True)
    # [cell for cell in pTable.getCells(unsolved=True, onlyObj=True)]

    obj_cells = pTable.getTargets(cea=True)

    # purge the candidate lists
    purged = []
    for cell in obj_cells:

        # filter the candidate list
        new_cand = []
        add_purged = []
        for cand in cell['cand']:
            #filter long labels:
            if isLongLabel(cand['labels']):
                cell['cand'].remove(cand)


