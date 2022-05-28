import asyncio
import time
from .aggregate import aggregateByKeys, formatOutput
from Wikidata_Proxy.util.util import getWikiID
from .query import query, runQuerySingleKey
import Wikidata_Proxy.inc.cache


async def get_entity_entitySearch_no_inTitle(entities):
    print("get_entity_entitySearch_no_inTitle in api")
    res = await runQuerySingleKey(cachePopularity, entities, """
            SELECT ?item ?itemLabel  ?num ?base ?article WHERE {
              VALUES ?base { %s } .
              SERVICE wikibase:mwapi {
                    bd:serviceParam wikibase:endpoint "www.wikidata.org";
                                    wikibase:api "EntitySearch";
                                    mwapi:search ?base;
                                    mwapi:language "en";
                                    mwapi:limit "12".
                ?item wikibase:apiOutputItem mwapi:item.
                ?num wikibase:apiOrdinal true.
                }
                OPTIONAL{
                    ?item ^schema:about ?article .
                    ?article schema:isPartOf <https://species.wikimedia.org/>;
                }
                
                 SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                 }
            ORDER BY ASC(?num) 
        """, type='get_entity')
    return res


async def get_entity_entitySearch_with_inTitle(entities):
    print("get_entity_entitySearch_with_inTitle in api")
    res = await runQuerySingleKey(cachePopularity, entities, """
            SELECT ?item ?itemLabel  ?num ?base ?article WHERE {
              VALUES ?base { %s } .
              BIND (CONCAT("intitle:",?base) AS ?intitleBase).
              SERVICE wikibase:mwapi {
                    bd:serviceParam wikibase:endpoint "www.wikidata.org";
                                    wikibase:api "EntitySearch";
                                    mwapi:search ?intitleBase;
                                    mwapi:language "en";
                                    mwapi:limit "12".
                ?item wikibase:apiOutputItem mwapi:item.
                ?num wikibase:apiOrdinal true.
                }
                OPTIONAL{
                    ?item ^schema:about ?article .
                    ?article schema:isPartOf <https://species.wikimedia.org/>;
                }
                 SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                 }
            ORDER BY ASC(?num) 
        """, type='get_entity')
    return res


async def get_entity_Search_no_inLabel(entities):
    res = {}
    print("in get the ", entities)
    reqs = [get_entity_Search_no_inLabel_query([term]) for term in entities]
    resp = await asyncio.gather(*reqs)

    for r in resp:
        res.update({**res, **r})

    # done
    return res


async def get_entity_Search_no_inLabel_query(term):
    print("get_entity_Search_no_inLabel_query in api")
    res = await runQuerySingleKey(cachePopularity, term, """
                SELECT  ?base ?item ?itemLabel
                 WHERE {
                    VALUES ?base { %s } .
                    BIND (CONCAT('inlabel:',?base) as ?inlabelBase)
                    SERVICE wikibase:mwapi {
                        bd:serviceParam wikibase:api "Search";
                        wikibase:endpoint "www.wikidata.org";
                        mwapi:srsearch ?inlabelBase.
                    ?item wikibase:apiOutputItem mwapi:title .
                    }
                    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
                } LIMIT 20
            """, type='get_entity')
    return res


async def get_entity_search_api(entities):
    print("get_entity_search_api in api")
    res = await runQuerySingleKey(cachePopularity, entities, """
            SELECT  ?base ?item ?itemLabel WHERE {
            hint:Query hint:optimizer "None".
            VALUES ?base { %s } .
            BIND (CONCAT('inlabel:',?base) as ?inlabelBase)
            SERVICE wikibase:mwapi {
                    bd:serviceParam wikibase:endpoint "www.wikidata.org";
                                    wikibase:api "EntitySearch";
                                    mwapi:search ?base;
                                    mwapi:language "en";
                                    mwapi:limit "12".
                ?item wikibase:apiOutputItem mwapi:item.
                ?num wikibase:apiOrdinal true.
                }
                OPTIONAL{
                    ?item ^schema:about ?article .
                    ?article schema:isPartOf <https://species.wikimedia.org/>;
                }
                 SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                 }
            ORDER BY ASC(?num) 
        """, type='get_entity')
    return res


async def get_entity_Generate_with_inTitle(entities):
    res = {}
    print("in get the ", entities)
    reqs = [get_entity_Generate_with_inTitle_query([term]) for term in entities]
    resp = await asyncio.gather(*reqs)

    for r in resp:
        res.update({**res, **r})

    # done
    return res


async def get_entity_Generate_with_inTitle_query(term):
    print("get_entity_Generate_with_inTitle in api")
    res = await runQuerySingleKey(cachePopularity, term, """
               SELECT ?base ?item ?itemLabel  
               WHERE {
                    VALUES ?base { %s }
                    BIND (CONCAT('intitle:',?base) as ?intitleBase)
                    SERVICE wikibase:mwapi {
                                bd:serviceParam wikibase:endpoint "en.wikipedia.org";
                                wikibase:api "Generator";
                                mwapi:generator "search";
                                mwapi:gsrsearch ?intitleBase;
                                mwapi:gsrlimit "max".
                    ?item wikibase:apiOutputItem mwapi:item .
                    }
                    hint:Prior hint:runFirst "true".
                    #?type ?typeLabel 
                    #  ?item wdt:P31|wdt:P279 ?type.
                    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                    } LIMIT 10
           """, type='get_entity')
    return res


async def get_entity_Generate_with_inTitle(entities):
    res = {}
    print("in get the ", entities)
    reqs = [get_entity_Generate_with_inTitle_query([term]) for term in entities]
    resp = await asyncio.gather(*reqs)

    for r in resp:
        res.update({**res, **r})

    # done
    return res

async def get_taxon(term):
    print("get_taxon")
    res = await runQuerySingleKey(cachePopularity, term, """
                   SELECT ?item ?itemLabel  ?type ?typeLabel
                    WHERE {

                    ?item wdt:P279 wd:Q16521.
                    OPTIONAL {?item wdt:P31 ?type.}
                    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
               """, type='get_entity')
    return res

async def get_type_subclass(entities):
    res = {}
    print("in get the ", entities)
    reqs = [get_type_subclass_high_level_query([term]) for term in entities]
    resp = await asyncio.gather(*reqs)

    for r in resp:
        res.update({**res, **r})

    # done
    return res
async def get_type_subclass_high_level_query(term):
    print("get_type_subclass_query")
    res = await runQuerySingleKey(cachePopularity, term, """
                       SELECT   ?base ?type ?typeLabel
                        WHERE {
                        VALUES ?base { %s} .
                        { ?base wdt:P31 ?type.}
                        UNION
                        {?base wdt:P279 ?type.}
                        UNION
                        {?base wdt:P279 ?ttype.
                        ?ttype wdt:P279 ?type}
                        
                         FILTER(?base != ?type)
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
                   """)
    return res

#
# async def get_taxon_rank(entities):
#     print("get_taxon_rank")
#     res = await runQuerySingleKey(cachePopularity, entities, """
#     SELECT ?base ?taxonRank ?taxonRankLabel
#     WHERE{
#         VALUES ?base {%s}
#         ?base wdt:P105  ?taxonRank.
#     SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
#     }
#      """)
#     return res

async def get_taxon_rank(entity):
    print("get_taxon_rank")
    res = await runQuerySingleKey(cachePopularity, entity, """
        SELECT ?base ?rank ?rankLabel
            WHERE 
            {
                VALUES ?base { %s }
                ?base wdt:P31 wd:Q16521;
                wdt:P105 ?taxonRank.
  
                ?base p:P105 [ps:P105 ?rank].
                SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
            }
         """)
    return res


async def get_unit(entity):
    print("get_taxon_rank")
    res = await runQuerySingleKey(cachePopularity, entity, """
            SELECT DISTINCT ?base  ?unit ?unitLabel
            WHERE
            {values ?base { %s}
              ?base wdt:P8111 ?unit.
              OPTIONAL{?unit wdt:P31 wd:Q2223662.}
             OPTIONAL {?unit wdt:P31 wd:Q19782718.}
             OPTIONAL {?unit wdt:P31 wd:Q691297847.}
             OPTIONAL {?unit wdt:P31 wd:Q820247053.}
             SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
            }
             """)
    return res

async def get_found_in_taxon(entity):
    print("get_found_in_taxon")
    res = await runQuerySingleKey(cachePopularity, entity, """
                SELECT ?base ?taxon
      WHERE {
        VALUES ?base { %s }
        ?base wdt:P703 ?taxon
      }
                 """)
    return res

async def get_quantity(entity):
    res = await runQuerySingleKey(cachePopularity, entity, """
    SELECT ?base ?quantity ?quantityLabel
    WHERE
    {
        VALUES ?base {%s}
    
        ?base wdt:P31*/wdt:P279* ?quantity.
        ?quantity wdt:P31 wd:Q71758646.
        SERVICE wikibase:label{bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".}
        }
    """)
    return res

async def get_oxyanion(entity):
    res = await runQuerySingleKey(cachePopularity, entity, """
       SELECT ?base ?o
        WHERE
        {
        VALUES ?base{%s}
        
        ?base wdt:P31 ?o.
        
        ?o wdt:P279* wd:Q3269344.
         }
       """)
    return res

async def get_chemical_element(entity):
    res = await runQuerySingleKey(cachePopularity, entity, """
          SELECT  ?base ?sym
           WHERE { 
                
                VALUES?base { %s}
                ?base wdt:P31 wd:Q11344.
               ?base wdt:P246 ?sym
                }
           """)
    return res

async def get_unit_of_measurement(entity):
    res = await runQuerySingleKey(cachePopularity, entity, """
               SELECT ?base ?o
                WHERE
                {VALUES ?base {%s}
                ?base wdt:P31* ?o.
                ?o wdt:P279* wd:Q47574.
                }
               """)
    return res
# async def get_reserve_label (entity , lang='en'):
#     res = await runQuerySingleKey(cachePopularity, entity, """
#                 Select ?base ?o
#                 WHERE
#                 {VALUES ?base {%s}
#                 ?base ^skos:altLabel ?o}
#
#                  """, type='get_entity')
#     return res
