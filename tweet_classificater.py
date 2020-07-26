from datetime import datetime
from elasticsearch import Elasticsearch
import tweepy
import sys
from myelasticsearch import MyElasticSearch


#"tweet_indexies"　tweetデータ
#"tweet_query_indexies"　分類したデータ
class Tweet_Clasificater:
    def __init__(self):
        self.test=None
        self.auth = tweepy.OAuthHandler("95vCXKe1AggFQwy8xFT3Q5gyc", "KKJH2iRmC3376XfNfabgBfX7gYg30EcGo80CLeVx8KijchN4pc")
        self.auth.set_access_token("993382630379765761-hPwTQaezOPdDEI7bSogvKkR5if0yuEf", "19CGdnToBLJbt4owrO1NF7zl7tBI7khfcvbgk364ZDWXq")
        self.api = tweepy.API(self.auth)
        self.es=MyElasticSearch("tweet_indexies")
        self.esquery=MyElasticSearch("tweet_query_indexies")

    def make_index(self):
        settings = {
            "settings": {
                "analysis": {
                    "tokenizer": {
                        "kuromoji_search": {
                            "type": "kuromoji_tokenizer",
                            "mode" : "search",
                            "user_dictionary": "userdict_ja.txt"
                        }
                    },
                    "analyzer": {
                        "my_analyzer": {
                            "type": "custom",
                            "char_filter" : ["icu_normalizer", "kuromoji_iteration_mark"],
                            "tokenizer": "kuromoji_search",
                            "filter": ["kuromoji_baseform", "kuromoji_part_of_speech","kuromoji_stemmer","my_synonym_penguin_filter", "my_stop_filter"]
                        }
                    },
                    "filter":{
                        "my_synonym_penguin_filter": {
                            "type": "synonym",
                            "synonyms": ["コウテイペンギン,ペンギン"]
                        },
                        "my_stop_filter": {
                            "type": "stop",
                            "stopwords": ["いい", "もの", "ある", "いう", "それ", "いる"]
                        }
                    }
                }
            }
        }
        self.es.make_index(settings)
        self.esquery.make_index(settings)

    def set_mapping(self):
        mappings = {
            "properties": {
                "tweet":{"type":"text","analyzer":"my_analyzer","fielddata":True},
            }
        }
        query_mmappings={
            "properties":{
                "search":{
                    "properties":{
                        "category":{"type":"text"},
                        "query":{"type":"percolator"}
                    }
                },
                "text":{"type":"text","analyzer":"my_analyzer","fielddata":True},
            }
        }
        self.es.set_mapping(mappings)
        self.esquery.set_mapping(query_mmappings)
        
        
    def delete_index(self):
        self.es.delete_index()
        self.esquery.delete_index()

    def gather(self,query="あ"):
        tweets = self.api.search(q=query,lang = 'ja',result_type="popular",count=10)
        for tweet in tweets:
            self.es.insert_document({"tweet":tweet.text})

    def make_likey_list(self,text):
        query={
            "query":{
                "match":{
                    "tweet":text
                }
            },
            "aggregations": {
                "significantCrimeTypes": {
                    "significant_terms": {
                        "field": "tweet"
                    }
                }
            }
        }
            
        res=self.es.search(query)
        likey_list=""
        for word in res["aggregations"]["significantCrimeTypes"]["buckets"]:
            print(word)
            if len(likey_list)==0 :
                likey_list=word["key"]
            else:
                likey_list=likey_list+" "+word["key"]    
        return likey_list

    #make_likey_listを使ってpercolator作る
    def make_percolator(self,text):
        doc={
            "search":{
                "category":"seccor",
                "query":{
                    "match":{
                        "text":self.make_likey_list(text)
                    }
                }   
            }
        }
        self.esquery.insert_document(doc)


    def percolator_search(self,text):
        query={
            "query" : {
                "percolate" : {
                    "field" : "search.query",
                    "document" : {
                        "text" : text
                    }
                }
            }
        }
        res=self.esquery.search(query)
        print(res["hits"]['hits'])
        
    def analyze_test(self,text):
        self.es.analyze_test("my_analyzer",text)

    def show(self):
        res=self.es.search()
        for tweet in res["hits"]["hits"]:
            print(tweet["_source"])
        res=self.esquery.search()
        for tweet in res["hits"]["hits"]:
            print(tweet["_source"])
                




#後でpercolator_searchを追加する
if __name__ == '__main__':
    clasificater=Tweet_Clasificater()
    argvs=sys.argv
    if len(argvs)==1:
        print ("finish")
    elif argvs[1]=="delete":
        clasificater.delete_index()
    elif argvs[1]=="create":
        clasificater.make_index()
    elif argvs[1]=="gather":
        clasificater.gather(argvs[2])
    elif argvs[1]=="help":
        print("delete,create,gather [],show,analyzer,make_list,help")
    elif argvs[1]=="show":
        clasificater.show()
    elif argvs[1]=="analyzer":
        clasificater.analyze_test(argvs[2])
    elif argvs[1]=="make_list":
        clasificater.make_percolator(argvs[2])
    elif argvs[1]=="mappings":
        clasificater.set_mapping()
    elif argvs[1]=="search":
        clasificater.percolator_search(argvs[2])
    else :
        print("error")
