from quart import Quart, request
import traceback
from inc.lookup import lookup, lookup_single
import inc.api
import inc.api_properties
import inc.api_classes
import inc.api_types
import inc.api_redirects
import inc.disambiguation
from util import util_log

util_log.init("Wikidata_Proxy.log")

app = Quart(__name__)
# app.debug = False
app.debug = True


@app.route('/test')
async def routeTest():

    # global res dict for unit tests? TODO: should cover all inc.api.xxx we actually use
    res = {}

    res["get_parents_for_lst"] =  await inc.api_classes.get_parents_for_lst(['Q5', 'Q55'])
    res["get_strings_for_lst"]  = await inc.api_properties.get_strings_for_lst(['Q71441798'])  # Note: it doesn't return a format of Wiki Pxxx

    res["lookup_for_lst"] = await lookup(["", "Scarce_swift", "Gil Junger", "Egypt", "Berlin", "London Heathrow Airport"])
    return {'res': res}

    return res


# ~~~~~~~~~~~~~~~~~~~~ Lookup ~~~~~~~~~~~~~~~~~~~~~~
@app.route('/look_for_lst', methods=['POST'])
async def look_for_lst():
    queries = (await request.json)["texts"]
    return await lookup(queries)


@app.route('/look_for', methods=['POST'])
async def look_for():
    term = (await request.json)["text"]
    return await lookup_single(term)


# ~~~~~~~~~~ disambiguation ~~~~~~~~~~~~~~
@app.route('/resolve_disambiguations', methods=['POST'])
async def routeResolve_disambiguation_for_lst():
    entities = (await request.json)["entities"]
    res = await inc.disambiguation.resolve_disambiguations(entities)
    return res


# ~~~~~~~~~~~~~~~~~~~~~~ Cells ~~~~~~~~~~~~~~~~~~~~~~


@app.route('/filter_entities_by_classes', methods=['POST'])
async def routeFilter_values_by_classes():
    entities = (await request.json)["entities"]
    classes = (await request.json)["classes"]
    res = await inc.api.filter_entities_by_classes(entities, classes)
    return res


# ~~~~~~~~~~~~~~~~~~~~~~ Classes ~~~~~~~~~~~~~~~~~~~~~~

@app.route('/get_type_for', methods=['POST'])
async def routeGet_type_for():
    txt = (await request.json)["text"]
    res = await inc.api_types.get_type_lst([txt])
    return res


@app.route('/get_type_for_lst', methods=['POST'])
async def routeGet_type_for_lst():
    queries = (await request.json)["texts"]
    res = await inc.api_types.get_type_lst(queries)
    return res


@app.route('/get_ancestors_for_lst', methods=['POST'])
async def routeGet_ancestors_for_lst():
    entities = (await request.json)["entities"]
    res = await inc.api_types.get_ancestors_for_lst(entities)
    return res


@app.route('/get_hierarchy_for_lst', methods=['POST'])
async def routeGet_hierarchy_for_lst():
    entities = (await request.json)["entities"]
    res = await inc.api_types.get_hierarchy_for_lst(entities)
    return res


@app.route('/get_subclasses_for', methods=['POST'])
async def routeGet_subclasses_for():
    klass = (await request.json)["wikiclass"]
    res = await inc.api.get_subclasses(klass)
    return res


@app.route('/get_parents_for', methods=['POST'])
async def routeGet_parents_for():
    klass = (await request.json)["wikiclass"]
    res = await inc.api_classes.get_parents_for_lst([klass])
    return res


@app.route('/get_parents_for_lst', methods=['POST'])
async def routeGet_parents_for_lst():
    klasses = (await request.json)["texts"]
    res = await inc.api_classes.get_parents_for_lst(klasses)
    return res


# ~~~~~~~~~~~~~~~~~~~~~~ Literals ~~~~~~~~~~~~~~~~~~~~~~


@app.route('/get_literals_for_lst', methods=['POST'])
async def routeGet_literals_for_lst():
    queries = (await request.json)["texts"]
    res = await inc.api_properties.get_literals_full_for_lst(queries)
    return res


@app.route('/get_literals_for', methods=['POST'])
async def routeGet_literals_for():
    txt = (await request.json)["text"]
    res = await inc.api_properties.get_literals_full_for_lst([txt])
    return res


@app.route('/get_literals_full_for_lst', methods=['POST'])
async def routeGet_literals_full_for_lst():
    queries = (await request.json)["texts"]
    res = await inc.api_properties.get_literals_full_for_lst(queries)
    return res


@app.route('/get_literals_full_for', methods=['POST'])
async def routeGet_literals_full_for():
    txt = (await request.json)["text"]
    res = await inc.api_properties.get_literals_full_for_lst([txt])
    return res


@app.route('/get_literals_topranked_for_lst', methods=['POST'])
async def routeGet_literals_topranked_for_lst():
    queries = (await request.json)["texts"]
    res = await inc.api_properties.get_literals_topranked_for_lst(queries)
    return res


@app.route('/get_literals_topranked_for', methods=['POST'])
async def routeGet_literals_topranked_for():
    txt = (await request.json)["text"]
    res = await inc.api_properties.get_literals_topranked_for_lst([txt])
    return res


# ~~~~~~~~~~~~~~~~~~~~~~ Objects ~~~~~~~~~~~~~~~~~~~~~~


@app.route('/get_objects_for_lst', methods=['POST'])
async def routeGet_objects_for_lst():
    queries = (await request.json)["texts"]
    res = await inc.api_properties.get_objects_full_for_lst(queries)
    return res


@app.route('/get_objects_for', methods=['POST'])
async def routeGet_objects_for():
    txt = (await request.json)["text"]
    res = await inc.api_properties.get_objects_full_for_lst([txt])
    return res


@app.route('/get_objects_full_for_lst', methods=['POST'])
async def routeGet_objects_full_for_lst():
    queries = (await request.json)["texts"]
    res = await inc.api_properties.get_objects_full_for_lst(queries)
    return res


@app.route('/get_objects_full_for', methods=['POST'])
async def routeGet_objects_full_for():
    txt = (await request.json)["text"]
    res = await inc.api_properties.get_objects_full_for_lst([txt])
    return res


@app.route('/get_objects_topranked_for_lst', methods=['POST'])
async def routeGet_objects_topranked_for_lst():
    queries = (await request.json)["texts"]
    res = await inc.api_properties.get_objects_topranked_for_lst(queries)
    return res


@app.route('/get_objects_topranked_for', methods=['POST'])
async def routeGet_objects_topranked_for():
    txt = (await request.json)["text"]
    res = await inc.api_properties.get_objects_topranked_for_lst([txt])
    return res


# ~~~~~~~~~~~~~~~~~~~~~~ Reverse Objects ~~~~~~~~~~~~~~~~~~~~~~

@app.route('/get_reverse_objects_for_lst', methods=['POST'])
async def routeGet_reverse_objects_for_lst():
    queries = (await request.json)["entities"]
    res = await inc.api_properties.get_reverse_objects_topranked_for_lst(queries)
    return res


@app.route('/get_reverse_objects_topranked_for_lst', methods=['POST'])
async def routeGet_reverse_objects_topranked_for():
    entities = (await request.json)["entities"]
    res = await inc.api_properties.get_reverse_objects_topranked_for_lst(entities)
    return res


@app.route('/get_reverse_objects_classed_for_lst', methods=['POST'])
async def routeGet_reverse_objects_classed_for_lst():
    params = await request.json
    entities = params["entities"]
    klass = params["class"]
    res = await inc.api_properties.get_reverse_objects_classed_for_lst(entities, klass)
    return res


@app.route('/get_children_for_lst', methods=['POST'])
async def routeGet_children_for_lst():
    params = await request.json
    entities = params["entities"]
    res = await inc.api_types.get_children_for_lst(entities)
    return res

# ~~~~~~~~~~~~~~~~~~~~~~ Strings ~~~~~~~~~~~~~~~~~~~~~~


@app.route('/get_strings_for', methods=['POST'])
async def routeGet_string_for():
    params = await request.json
    if 'lang' in params:
        lang = params['lang']
    else:
        lang = 'en'
    res = await inc.api_properties.get_strings_for_lst(params["text"], lang)
    return res


@app.route('/get_strings_for_lst', methods=['POST'])
async def routeGet_string_for_lst():
    params = await request.json
    if 'lang' in params:
        lang = params['lang']
    else:
        lang = 'en'
    res = await inc.api_properties.get_strings_for_lst(params["texts"], lang)
    return res


@app.route('/get_labels_for_lst', methods=['POST'])
async def routeGet_labels_for_lst():
    params = await request.json
    if 'lang' in params:
        lang = params['lang']
    else:
        lang = 'en'
    res = await inc.api_properties.get_labels_for_lst(params["entities"], lang)
    return res


# ~~~~~~~~~~~~~~~~~~~~~~ Properties ~~~~~~~~~~~~~~~~~~~~~~


@app.route('/get_connection_for_lst', methods=['POST'])
async def routeGet_connection_for_lst():
    pairs = (await request.json)["pairs"]
    res = await inc.api.get_connection_for_lst(pairs)
    return res


@app.route('/get_properties_for', methods=['POST'])
async def routeGet_properties_for():
    params = await request.json
    if 'lang' in params:
        lang = params['lang']
    else:
        lang = 'en'
    res = await inc.api_properties.get_properties_full_for_lst([params["text"]], lang)
    return res


@app.route('/get_properties_for_lst', methods=['POST'])
async def routeGet_properties_for_lst():
    params = await request.json
    if 'lang' in params:
        lang = params['lang']
    else:
        lang = 'en'
    res = await inc.api_properties.get_properties_full_for_lst(params["texts"], lang)
    return res


@app.route('/get_properties_full_for', methods=['POST'])
async def routeGet_properties_full_for():
    params = await request.json
    if 'lang' in params:
        lang = params['lang']
    else:
        lang = 'en'
    res = await inc.api_properties.get_properties_full_for_lst([params["text"]], lang)
    return res


@app.route('/get_properties_full_for_lst', methods=['POST'])
async def routeGet_properties_full_for_lst():
    params = await request.json
    if 'lang' in params:
        lang = params['lang']
    else:
        lang = 'en'
    res = await inc.api_properties.get_properties_full_for_lst(params["texts"], lang)
    return res


@app.route('/get_properties_topranked_for', methods=['POST'])
async def routeGet_properties_topranked_for():
    params = await request.json
    if 'lang' in params:
        lang = params['lang']
    else:
        lang = 'en'
    res = await inc.api_properties.get_properties_topranked_for_lst([params["text"]], lang)
    return res


@app.route('/get_properties_topranked_for_lst', methods=['POST'])
async def routeGet_properties_topranked_for_lst():
    params = await request.json
    if 'lang' in params:
        lang = params['lang']
    else:
        lang = 'en'
    res = await inc.api_properties.get_properties_topranked_for_lst(params["texts"], lang)
    return res


# ~~~~~~~~~~~~~~~~~~~~~~ Other ~~~~~~~~~~~~~~~~~~~~~~


@app.route('/get_popularity_for_lst', methods=['POST'])
async def routeGet_popularity_for_lst():
    entities = (await request.json)["entities"]
    res = await inc.api.get_popularity_for_lst(entities)
    return res


@app.route('/get_direct_types_for_lst', methods=['POST'])
async def routeGet_direct_types_for_lst():
    entities = (await request.json)["entities"]
    res = await inc.api_types.get_direct_types_for_lst(entities)
    return res


# ~~~~~~~~~~~ Redirects ~~~~~~~~~~~

@app.route('/resolve_redirects', methods=['POST'])
async def routeGet_redirects():
    params = await request.json
    res = await inc.api_redirects.resolve_redirects(params["entities"])
    return res


# ~~~~~~~~~~~~~~~~~~~~~~ Default ~~~~~~~~~~~~~~~~~~~~~~


@app.errorhandler(500)
def handle_500(e):
    """output the internal error stack in case of unhandled exception"""
    try:
        raise e
    except:
        return traceback.format_exc(), 500

    
# ~~~~~~~~~~~~~~~~~~~Added by Karim~~~~~~~~~~~~~~~~~~~~
@app.route('/get_entity_entitySearch_no_inTitle', methods=['POST'])
async def routeGet_entity_entitySearch_no_inTitle():
    print("get_entity_entitySearch_no_inTitle in main wikidata_proxy")
    params = await request.json
    res = await inc.k_api.get_entity_entitySearch_no_inTitle(params["texts"])
    return res

@app.route('/get_entity_entitySearch_with_inTitle', methods=['POST'])
async def  routeGet_entity_entitySearch_with_inTitle():
    print("get_entity_entitySearch_with_inTitle in main wikidata_proxy")
    params = await request.json
    res = await inc.k_api.get_entity_entitySearch_with_inTitle(params["texts"])
    return res

@app.route('/get_entity_Search_no_inLabel', methods=['POST'])
async def  routeGet_entity_Search_no_inLabel():
    print("get_entity_Search_no_inLabel in main wikidata_proxy")
    params = await request.json
    res = await inc.k_api.get_entity_Search_no_inLabel(params["texts"])
    return res

@app.route('/get_entity_Generate_with_inTitle', methods=['POST'])
async def routeGet_entity_Generate_with_inTitle():
    print("get_entity_Generate_with_inTitle in main wikidata_proxy")
    params = await request.json
    res = await inc.k_api.get_entity_Generate_with_inTitle(params["texts"])
    return res

@app.route('/get_taxon', methods=['POST'])
async def routeGet_taxon():
    print("get_taxon")
    params = await request.json
    res = await inc.k_api.get_taxon(params["texts"])
    return res

@app.route('/get_type_subclass', methods=['POST'])
async def routeGet_type_subclass():
    print("Get_type_subclass in main wikidata_proxy")
    params = await request.json
    res = await inc.k_api.get_type_subclass(params["texts"])
    return res

# @app.route('/get_taxon_rank', methods=['POST'])
# async def routeGet_taxon_rank():
#     print("get_taxon_rank in main wikidata_proxy")
#     params = await request.json
#     res = await inc.k_api.get_taxon_rank(params["texts"])
#     return res

@app.route('/get_taxon_rank', methods=['POST'])
async def routeGet_taxon_rank():
    print("get taxon rank")
    params = await request.json
    res = await inc.k_api.get_taxon_rank(params['entity'])
    return res

@app.route('/get_unit', methods=['POST'])
async def routeGet_unit():
    print("get_unit")
    params = await request.json
    res = await inc.k_api.get_unit(params['entity'])
    return res

@app.route('/get_reserve_label', methods=['POST'])
async def routeGet_reserve_label():
    print("get_reserve_label")
    params = await request.json
    res = await inc.k_api.get_reserve_label(params['entity'])
    return res

@app.route('/get_found_in_taxon', methods=['POST'])
async def routeGet_found_in_taxon():
    print("get_found_in_taxon")
    params = await request.json
    res = await inc.k_api.get_found_in_taxon(params['entity'])
    return res

@app.route('/get_quantity', methods=['POST'])
async def routeGet_quantity():
    print("get_quantity")
    params = await request.json
    res = await inc.k_api.get_quantity(params['entity'])
    return res

@app.route('/get_oxyanion', methods=['POST'])
async def routeGet_oxyanion():
    print("get_oxyanion")
    params = await request.json
    res = await inc.k_api.get_oxyanion(params['entity'])
    return res

@app.route('/get_chemical_element', methods=['POST'])
async def routeGet_chemical_element():
    print("get_chemical_element")
    params = await request.json
    res = await inc.k_api.get_chemical_element(params['entity'])
    return res

@app.route('/get_unit_of_measurement', methods=['POST'])
async def routeGet_unit_of_measurement():
    print("get_unit_of_measurement")
    params = await request.json
    res = await inc.k_api.get_unit_of_measurement(params['entity'])
    return res
#------------------------------------------------------------

@app.route('/')
def routeRoot():
    return 'proxy.wikidata.svc'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5007)
