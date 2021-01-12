#!/usr/bin/python3

''' Insert data from collectData.py into a mongodb database and query it.
'''

import glob
import json
import os.path
import pprint

from bson.code import Code
from pymongo import MongoClient

dataDir = 'data/'
fullJsonDir = os.path.join(dataDir, 'fullJson')
jsonDir = os.path.join(dataDir, 'json')
jpgDir = os.path.join(dataDir, 'jpg')
flickrFile = os.path.join(dataDir, 'flickr.pkl')
urlFile = os.path.join(dataDir, 'urls.txt')

def main():
    populateMongo(jsonDir)
    queryMongo()


def populateMongo(jsonDir, clearDb=True):
    'Load the JSON results from google into mongo'

    client = MongoClient()
    db = client.homework3
    collection = db.googleTagged

    if clearDb:
        client.homework3.googleTagged.delete_many({})

    for jsonFile in glob.glob(os.path.join(jsonDir, '*.json')):
        print('Loading', jsonFile, 'into mongo')
        with open(jsonFile) as jf:
            jsonData = json.load(jf)
        key = {'url': jsonData['url']}
        collection.update_one(key, {'$set': jsonData}, upsert=True);

    print('Mongo now contains', collection.count_documents({}), 'documents')


def queryMongo():
    client = MongoClient()
    db = client.homework3
    collection = db.googleTagged

    print('''
    Query 0. Count the total number of JSON documents in the database
    ''')
    res_0 = {
        'documents': collection.count_documents({})
    }
    print(res_0)

    # Example aggregation pipeline
    desc_1 = '''
    Query 1. List all Images tagged with the Label with mid '/m/015kr' (which 
             has the description 'bridge'), ordered by the score of the 
             association between them from highest score to lowest
    '''
    pipeline_1 = [
        {'$unwind': {'path': '$response.labelAnnotations'}},
        {'$project': {'mid': '$response.labelAnnotations.mid',
                      'score': '$response.labelAnnotations.score',
                      'url': 1}},
        {'$match': {'mid': '/m/015kr'}},
        {'$sort': {'score': -1}},
        {'$project': {'_id': 0}}
    ]
    aggregateMongoAndPrintResults(pipeline_1, collection, desc_1)

    # TODO: use the collection.distinct function 
    # (https://api.mongodb.com/python/current/api/pymongo/collection.html
    #  #pymongo.collection.Collection.distinct) 
    # and the built-in python len() function for this
    desc_2 = '''
    Query 2. Count the total number of distinct Labels (by mid), 
             Landmarks (by mid), Locations (by latitude and longitude), 
             Pages (by url), and Web Entities (by entityId) in the database 
             (note that images are counted in a separate query below).
    '''
    print(desc_2)
    res_2 = {
        'labels': len(collection.distinct('response.labelAnnotations.mid')),
        'landmarks': len(collection.distinct(
            'response.landmarkAnnotations.mid')),
        'locations': len(collection.distinct(
            'response.landmarkAnnotations.locations.latLng')),
        'pages': len(collection.distinct(
            'response.webDetection.pagesWithMatchingImages.url')),
        'entities': len(collection.distinct(
            'response.webDetection.webEntities.entityId'))
    }
    print(res_2)
    
    # TODO: develop an aggregation pipeline for this query
    desc_3 = '''
    Query 3. List the 10 Labels that have been applied to the most Images along 
             with the number of Images each has been applied to in descending 
             order.
    '''
    # list of pipeline stages: 
    # https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/
    # list of pipeline operators: 
    # https://docs.mongodb.com/manual/reference/operator/aggregation/
    pipeline_3 = [
        {'$unwind': '$response.labelAnnotations'},
        {'$group': {'_id': '$response.labelAnnotations.description',
                    'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]
    aggregateMongoAndPrintResults(pipeline_3, collection, desc_3)
    
    # TODO: develop an aggregation pipeline for this query
    desc_4 = '''
    Query 4. List the 10 Pages that are linked to the most Images through the 
             webEntities.pagesWithMatchingImages JSON property along with the 
             number of Images linked to each one. Sort them by 
             count (descending), followed by page URL.
    '''
    pipeline_4 = [
        {'$unwind': '$response.webDetection.pagesWithMatchingImages'},
        {'$group': {'_id': '$response.webDetection.pagesWithMatchingImages.url',
                    'count': {'$sum': 1}}},
        {'$sort': {'count': -1, '_id': 1}},
        {'$limit': 10}
    ]
    aggregateMongoAndPrintResults(pipeline_4, collection, desc_4)
    
    # TODO: develop an aggregation pipeline for this query (do not use the 
    #       collection.distinct function, though you can use that to verify 
    #       the correctness of your results)
    desc_5 = '''
    Query 5. List the descriptions of all Landmarks in alphabetical order, with
             each description appearing only once. 
    '''
    pipeline_5 = [
        {'$unwind': '$response.landmarkAnnotations'},
        {'$group': {'_id': '$response.landmarkAnnotations.description'}},
        {'$sort': {'_id': 1}}
    ]
    aggregateMongoAndPrintResults(pipeline_5, collection, desc_5)
    
    # TODO: develop an aggregation pipeline for this query
    desc_6 = '''
    Query 6. List all Images that have at most 5 Labels *and* at most 5 Web 
             Entities associated with them. List them in descending order of 
             the number of Labels, followed by descending order of the number of
             Entities. 
             List only the Image URLs and the numbers of Labels and Entities.
    '''
    pipeline_6 = [
        {'$set': {'entityCount': {'$size': 
                                  '$response.webDetection.webEntities'},
                  'labelCount': {'$size': 
                                 '$response.labelAnnotations'}}},
        {'$match': {'entityCount': {'$lte': 5}, 'labelCount': {'$lte': 5}}},
        {'$sort': {'labelCount': -1, 'entityCount': -1}},
        {'$project': {'_id': 0, 'entityCount': 1, 'labelCount': 1, 'url': 1}}
    ]
    aggregateMongoAndPrintResults(pipeline_6, collection, desc_6)
    
    # TODO: develop an aggregation pipeline for this query
    desc_7 = '''
    Query 7. List all Images that have at least 1 Landmark associated with them. 
             List them in descending order of the number of Landmarks,
             followed by their URL alphabetically. 
             List only the Image URLs and the numbers of Landmarks.
    '''
    pipeline_7 = [
        {'$unwind': '$response.landmarkAnnotations'},
        {'$group': {'_id': '$url', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1, '_id': 1}}
    ]
    aggregateMongoAndPrintResults(pipeline_7, collection, desc_7)
    
    # TODO: develop an aggregation pipeline for this query
    desc_8 = '''
    Query 8. List the 10 Images with the greatest number of Image matches of 
             either type (partial or full). 
             List them in descending order of the number of matches, followed by
             their URL alphabetically. 
             List only the Image URLs and the numbers of matches.
    '''
    # tips: one way to do this is to combine the two relevant fields into a new 
    #       one and then determine the number of elements in the joint array 
    #       (see array expression operators for both steps); 
    #       note that either field might not exist (i.e., return NULL) in a 
    #       given document
    pipeline_8 = [
        {'$set': {'allMatches': {'$concatArrays': [
            {'$ifNull': ['$response.webDetection.partialMatchingImages', []]},
            {'$ifNull': ['$response.webDetection.fullMatchingImages', []]}
            ]}}},
        {'$set': {'matchCount': {'$size': '$allMatches'}}},
        {'$sort': {'matchCount': -1, 'url': 1}},
        {'$limit': 10},
        {'$project': {'_id': 0, 'matchCount': 1, 'url': 1}}
    ]
    aggregateMongoAndPrintResults(pipeline_8, collection, desc_8)
    
    # TODO: develop an aggregation pipeline for this query
    desc_9 = '''
    Query 9. List the 10 most frequent Web Entities that are associated with the 
             same Images as the Label with an id of '/m/015kr' (which has the 
             description 'bridge'). List them in descending order of the number 
             of times they appear, followed by their entityId alphabetically
    '''
    pipeline_9 = [
        {'$unwind': '$response.labelAnnotations'},
        {'$match': {'response.labelAnnotations.mid': '/m/015kr'}},
        {'$unwind': '$response.webDetection.webEntities'},
        {'$project': {'desc': '$response.webDetection.webEntities.description',
                      'eid': '$response.webDetection.webEntities.entityId'}},
        {'$group': {'_id': '$eid', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1, '_id': 1}},
        {'$limit': 10}
    ]
    aggregateMongoAndPrintResults(pipeline_9, collection, desc_9)

    # TODO: develop an aggregation pipeline for this query
    desc_10 = '''
    Query 10. Find all Images associated with any Landmarks that are not 'New
              York' or 'New York City' with an association score of 
              at least 0.6, ordered alphabetically by Landmark description and 
              then by Image URL.
    '''
    pipeline_10 = [
        {'$unwind': '$response.landmarkAnnotations'},
        {'$match': {'response.landmarkAnnotations.description': 
                        {'$nin': ['New York', 'New York City']},
                    'response.landmarkAnnotations.score': {'$gte': 0.6}}},
        {'$project': {'desc': '$response.landmarkAnnotations.description',
                      'url': '$url'}},
        {'$sort': {'desc': 1, 'url': 1}}
    ]
    aggregateMongoAndPrintResults(pipeline_10, collection, desc_10)
    
    # TODO: develop an aggregation pipeline for this query
    desc_11 = ''' 
    Query 11. Count the total number of unique Images in the database.
              This should include both those that have been directly submitted
              to the Google Cloud Vision API as well as those that are referred
              to in the returned analyses.  
    '''
    # tips: image urls appear in three fields in the JSON structure;
    #       identify them and combine them into a single, new field;
    #       then determine all individual urls and count the distinct ones; 
    #       note that not all three fields are present in all JSON documents
    pipeline_11 = [
        {'$set': {'allImages': {'$concatArrays': [
            {'$ifNull': ['$response.webDetection.fullMatchingImages', []]},
            {'$ifNull': ['$response.webDetection.partialMatchingImages', []]},
            [{'url': '$url'}]
        ]}}},
        {'$unwind': '$allImages'},
        {'$group': {'_id': '$allImages'}},
        {'$group': {'_id': None, 'count': {'$sum': 1}}}
    ]
    aggregateMongoAndPrintResults(pipeline_11, collection, desc_11)
    
    # TODO: develop an aggregation pipeline for this query
    desc_12 = ''' 
    Query 12. Some Images have Landmarks and Web Entities with the same 
              description associated with them. That is, some Landmarks are also
              Web Entities or vice versa. List the distinct descriptions of all 
              Landmarks like that, i.e., Landmarks associated with at least one 
              Image which is also associated with a Web Entity of the same 
              description as the Landmark. 
    '''
    # note: to use an expression operator in a query to directly compare two 
    #       fields with each other, use the "$expr" operator for the query and 
    #       then the expression operator you actually want to use
    pipeline_12 = [
        {'$unwind': {'path': '$response.landmarkAnnotations'}},
        {'$unwind': {'path': '$response.webDetection.webEntities'}},
        {'$match': {'$expr': {'$eq': [
            '$response.landmarkAnnotations.description',
            '$response.webDetection.webEntities.description'
        ]}}},
        {'$group': {'_id': '$response.landmarkAnnotations.description'}},
        {'$sort': {'_id': 1}}
    ]
    aggregateMongoAndPrintResults(pipeline_12, collection, desc_12)
    
    # TODO: develop an aggregation pipeline for this query
    desc_13 = '''
    Query 13. List the 10 pairs of Images that appear on the most Pages together 
              through the webEntities.pagesWithMatchingImages JSON property. 
              List them in descending order of the number of pages that they 
              appear on together, then by the URL of the first image. Make sure 
              that each pair is only listed once regardless of which is first 
              and which is second.
    '''
    # tips: one way to do this is to first determine a list of *all* images per 
    #       page (the "$push" operator can do that, after another, initial step)
    #       and then create a second copy of it and unwind both to get all pairs
    pipeline_13 = [
        {'$unwind': '$response.webDetection.pagesWithMatchingImages'},
        {'$group': {'_id': '$response.webDetection.pagesWithMatchingImages.url',
                    'images': {'$push': '$url'}}},
        {'$project': {'_id': 0,
                      'images1': '$images',
                      'images2': '$images',
                      'url': '$_id'}},
        {'$unwind': '$images1'},
        {'$unwind': '$images2'},
        {'$group': {'_id': {'image1': '$images1', 'image2': '$images2'},
                    'shared_page_count': {'$sum': 1}}},
        {'$match': {'$expr': {'$lt': ['$_id.image1', '$_id.image2']}}},
        {'$sort': {'shared_page_count': -1, '_id.image1': 1}},
        {'$limit': 10}
    ]
    aggregateMongoAndPrintResults(pipeline_13, collection, desc_13)





def aggregateMongoAndPrintResults(pipeline, collection, desc='Running query:'):
    print()
    print(desc)
    print('***************** Aggregate pipeline ****************')
    pprint.pprint(pipeline)
    print('********************** Results **********************')
    if len(pipeline) > 0:
        for result in collection.aggregate(pipeline):
            pprint.pprint(result)
    print('*****************************************************')

        
if __name__ == '__main__':
    main()

   
X{'allMatch': {'$concatArrays':[
    {'$ifNull': ['$response.labelAnnotations',[]]},
    {'$ifNull': ['$response.webDetection.pagesWithMatchingImages', []]},
    {'$ifNull': ['$response.webDetection.webEntities', []]},
    {'$ifNull': ['$response.webDetection.fullMatchingImages', []]},
    {'$ifNull': ['$response.webDetection.partialMatchingImages', []]},
    {'$ifNull': ['$response.webDetection.landmarkAnnotations', []]}
    ]}}
}
