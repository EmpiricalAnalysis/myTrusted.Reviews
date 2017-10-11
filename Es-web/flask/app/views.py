from app import app
from elasticsearch import Elasticsearch
import os, json
from flask import g, request, render_template


def connect_db():

    es_usr = os.getenv('ES_USR')
    es_pwd = os.getenv('ES_PWD')

    es_node1_ip = os.getenv('ES_NODE1_IP')
    es_node2_ip = os.getenv('ES_NODE2_IP')
    es_node3_ip = os.getenv('ES_NODE3_IP')

    try:
      es = Elasticsearch(
            [es_node1_ip, es_node2_ip, es_node3_ip],
            http_auth=(es_usr, es_pwd),
            port=9200,
        )
      print "Connected", es.info()
    except Exception as ex:
      print "Error:", ex
    return es


@app.before_request
def before_request():
    g.es = connect_db()


@app.route('/review/<string:itemID>/<string:reviewerIDs_str>/<int:nReviews>/<string:level>/<string:itemName>')
def review(itemID, reviewerIDs_str, nReviews, level, itemName):
    
    reviewerIDs = [x.strip() for x in reviewerIDs_str.split(',')]

    reviews = []

    for elt in reviewerIDs:
      review_search = g.es.search(index="reviews4", \
                                    body={"size": 1, \
                                          "query": { "bool": {  \
                                                        "should": [  \
                                                          {"match": {  \
                                                              "reviewerID": elt  \
                                                          }},  \
                                                          {"match": { \
                                                              "asin": itemID \
                                                          }} \
                                                        ] \
                                                      } \
                                                  } \
                                          } \
                                   )["hits"]["hits"][0]["_source"]
                                   
      reviews.append(dict(level=level, itemName=itemName, reviewerName=review_search.reviewerName, rating=review_search.rating, reviewText=review_search.reviewText, summary=review_search.summary, reviewTime=review_search.reviewTime))

    return render_template('routing/review.html', reviews=reviews)



@app.route('/', methods=['POST', 'GET'])
@app.route('/form', methods=['POST', 'GET'])
def hello_world2():
    if request.method == 'GET':
        return render_template('forms/basic_form.html')
    elif request.method == 'POST':
        item_keyword = request.form['item_keyword']
        userID = request.form['userID']

        search_result = g.es.search(index="uber_meta7", \
                                    body={"size": 10, \
                                          "query": {"multi_match": {"query": item_keyword, \
                                                                    "type":  "cross_fields", \
                                                                    "fields": [ "title", "description", "brand" ], \
                                                                    "operator": "and" \
                                                                    } \
                                                   } \
                                          } \
                                   )["hits"]["hits"]



        first_deg_result = g.es.search(index="first_deg_trust5", body={"size": 1, "query": {"match": {"userID": userID}}})["hits"]["hits"]

        # get trusted first-deg reviewerIDs
        firstDegReviewers = []
        if len(first_deg_result) > 0:
          num1stDegReviewers = len(first_deg_result[0]["_source"]["reviewerIDs"])
          list1DReviewers = sorted([0, 100, num1stDegReviewers])
          firstDegReviewers = first_deg_result[0]["_source"]["reviewerIDs"][0: list1DReviewers[1]]

        sec_deg_result = g.es.search(index="second_deg_trust2", body={"size": 10, "query": {"match": {"userID": userID}}})["hits"]["hits"]#[0]["_source"]

        # get trusted second-deg reviewerIDs
        secDegReviewers = []
        if len(sec_deg_result) > 0:
          num2ndDegReviewers = len(sec_deg_result)
          list2DReviewers = sorted([0, 100, num2ndDegReviewers])
          for i in range(list2DReviewers[1]):
            secDegReviewers.append(sec_deg_result[i]["_source"]["reviewerID_2nd_deg"])

        reviews = []
        relevant1DReviewers = []
        relevant2DReviewers = []

        compareN1D = {}
        compareN2D = {}
        kk = 0

        for entry in search_result:
          if "asin" in entry["_source"]:
            asin=entry["_source"]["asin"]
          else: 
            asin=None

          if "reviewerIDs" in entry["_source"]:
            for elt in entry["_source"]["reviewerIDs"]:
              if elt in firstDegReviewers:
                relevant1DReviewers.append(elt)
              if elt in secDegReviewers:
                relevant2DReviewers.append(elt)

            n1DReviewer = max(0, len(relevant1DReviewers))
            n2DReviewer = max(0, len(relevant2DReviewers))

            compareN1D[kk] = n1DReviewer
            compareN2D[kk] = n2DReviewer

          else: 
            n1DReviewer = 0
            n2DReviewer = 0

          if "price" in entry["_source"]:
            price=entry["_source"]["price"]
          else: 
            price=None
          
          if "imUrl" in entry["_source"]:
            imUrl=entry["_source"]["imUrl"]
          else: 
            imUrl=None
          
          if "title" in entry["_source"]:
            title=entry["_source"]["title"]
          else: 
            title=None
          
          if "description" in entry["_source"]:
            description=entry["_source"]["description"]
          else: 
            description=None
          
          if "category" in entry["_source"]:
            category=entry["_source"]["category"]
          else: 
            category=None
          
          if "brand" in entry["_source"]:
            brand=entry["_source"]["brand"]
          else: 
            brand=None

          relevant1DReviewers_str = ', '.join(relevant1DReviewers)
          relevant2DReviewers_str = ', '.join(relevant2DReviewers)

          reviews.append(dict(relevant1DReviewers=relevant1DReviewers_str, relevant2DReviewers=relevant2DReviewers_str, n1DReviewer=n1DReviewer, n2DReviewer=n2DReviewer, asin=asin, price=price, imUrl=imUrl, description=description, category=category, brand=brand, title=title))
          
          kk += 1

        L = sorted(compareN1D, key=compareN1D.get)
        M = L[::-1]

        orderedReviews = []
        for elt in M:
          orderedReviews.append(reviews[elt])

        return render_template('forms/basic_form_result.html', reviews=orderedReviews, item_keyword=item_keyword)






