
from .generate_cta import generate as generate_cta
#TODO KARIM
from .k_generate_cea_biodiv import generate as k_generate_cea_biodiv
from .k_generate_cta_biodiv import generate as k_generate_cta_biodiv
from .k_select_cea import select as k_select_cea_cta
from.k_filter_longLabel import doFilter as k_filter_longLabel
from.k_select_cta_majority import select as k_select_cta_majority
#TODO UNTIL HERE


from external_services.wikidata_proxy_service import Wikidata_Proxy_Service

from utils import util_log
import utils.table
from utils import res_IO




class Pipeline():
    def __init__(self, table, targets):
        util_log.init('baseline.log')

        # Conditional init of endpoint service.
        self.proxyService = Wikidata_Proxy_Service()

        # memorize the table name
        self.table_name = table['name']

        # parse the raw table structure into something meaningful
        self.table = table
        self.pTable = utils.table.ParsedTable(table, targets)

    def run(self):
        try:
            print("k_pipeline_no_cpa")

            # initialize CEA candidates
            k_generate_cea_biodiv(self.pTable, self.proxyService)
            self.pTable.checkPoint.addCheckPoint('cea_initial', self.pTable, cta=True)
            res_IO.set_res(self.table_name, self.get_results())


            # filter candidates
            k_filter_longLabel(self.pTable)

            # generate CTA candidates
            # generate_cta(self.pTable, self.proxyService)
            k_generate_cta_biodiv(self.pTable,self.proxyService)

            self.pTable.checkPoint.addCheckPoint('cta_initial', self.pTable, cta=True)
            res_IO.set_res(self.table_name, self.get_results()) #no error


            # select CTA candidates
            k_select_cta_majority(self.pTable, self.proxyService)

            # select CEA & CTA candidates
            k_select_cea_cta(self.pTable,  proxyService= self.proxyService) #this will choose the taxon


            # final checkpoints
            self.pTable.checkPoint.addCheckPoint('final_cand', self.pTable, cea=True, cta=True, cpa=True)
            self.pTable.checkPoint.checkpointSelected('final_selected', self.pTable, cea=True, cta=True, cpa=True)
            res_IO.set_res(self.table_name, self.get_results())

            # return pTable object
            return self.pTable

        except Exception as ex:
            raise ex

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Utils ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_CEA(self):
        result = []
        for cell in self.pTable.getTargets(cea=True):
            if ('sel_cand' in cell) and cell['sel_cand']:
                result.append({
                    'col_id': cell['col_id'],
                    'row_id': cell['row_id'],
                    'mapped': cell['sel_cand']['uri']
                })
            elif ('cand' in cell) and cell['cand']:
                # print(cell)
                # print(cell['cand'][0]['uri'])
                result.append({
                    'col_id': cell['col_id'],
                    'row_id': cell['row_id'],
                    'mapped': cell['cand'][0]['uri']
                })
        return result

    def get_CTA(self):
        result = []
        for col in self.pTable.getTargets(cta=True):
            if ('sel_cand' in col) and col['sel_cand']:
                result.append({
                    'col_id': col['col_id'],
                    'mapped': col['sel_cand']['uri']
                })
        return result

    def get_CPA(self):
        result = []
        for pair in self.pTable.getTargets(cpa=True):
            if ('sel_cand' in pair) and pair['sel_cand']:
                result.append({
                    'subj_id': pair['subj_id'],
                    'obj_id': pair['obj_id'],
                    'mapped': pair['sel_cand'],
                })
        return result

    def get_Errors(self):
        return self.pTable.getErrors()

    def get_AuditRecords(self):
        return self.pTable.audit.getRecords()

    def get_CheckPoints(self):
        return self.pTable.checkPoint.getCheckPoints()

    # ~~~~~~~~~~~~~~~~~~~~~ Results ~~~~~~~~~~~~~~~~~

    def get_results(self):
        """
        Wrap up every single part of the res dict
        """
        res = {
               'cea': self.get_CEA(),
               'cta': self.get_CTA(),
               'cpa': self.get_CPA(),
               'errors': self.get_Errors(),
               'audit': self.get_AuditRecords(),
               'checkpoints': self.get_CheckPoints()}
        return res

