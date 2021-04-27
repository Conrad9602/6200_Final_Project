#  -*- coding: utf-8 -*-

import pandas as pd
from pandas import DataFrame
import spacy
from collections import defaultdict
import json
from jsoncomment import JsonComment
from tqdm import tqdm
import logging
from elasticsearch import Elasticsearch, helpers
from flask import Flask, render_template, request
import time
from jinja2 import Markup

es = Elasticsearch()

app = Flask(__name__)
app.jinja_env.lstrip_blocks = False
app.jinja_env.trim_blocks = False

@app.route('/')
def upload():
    return render_template('upload.html')

@app.route('/success', methods = ['POST'])
def success():
    if request.method == 'POST':
        query = request.form['text']


    # tokenize the query first
    query_tokenize = tokenizer_query(query)

    dic_title_date_url = load_json('dic_title_date_url.json')

    #json_dic = load_json('data_id_content.json')

    #initialize_elasticsearch(json_dic)

    results = es.search(index="cfc", body=
    {
        "query": {
            "match": {
                "content": query_tokenize
            }
        }
    },  request_timeout=120)

    res = results['hits']['hits']
    res_lst = [x['_id'] for x in res]
    final_res = defaultdict(list)
    for i in res_lst:
        a = " ".join(str((" ".join(["<b><strong>{}</strong></b>".format(word) if word.strip() in query_tokenize.split() else word for word in str(dic_title_date_url[i][3]).split(" ")]))).split()[0:80])
        dic_title_date_url[i].append(a)
        dic_title_date_url[i].remove(dic_title_date_url[i][3])
        final_res[i].append(dic_title_date_url[i])

    return render_template('outputs2.html', res=final_res, q =query, l =len(res_lst))
    #return(3)

def load_data():
    data = pd.read_csv('metadata.csv')
    return data

"""
 cord_uid                                            content publish_time
0  ug7v899j  Clinical features of culture-proven Mycoplasma...   2001-07-04
1  02tnwd4m  Nitric oxide: a pro-inflammatory mediator in l...   2000-08-15
2  ejv2xln0  Surfactant protein-D and pulmonary host defens...   2000-08-25
3  2b73a28n  Role of endothelin-1 in lung diseaseEndothelin...   2001-02-22
4  9785vg6d  Gene expression in epithelial cells in respons...   2001-05-11

"""
def data_dataframe(df):
    # save abstract, title, cord_uid, and publish_time
    df_selected = df[['cord_uid', 'title', 'abstract', 'publish_time', 'url']]
    df_selected['content'] = df_selected['title'].astype('str')  + df_selected['abstract'].astype('str')
    df_used = df_selected[['cord_uid', 'content','publish_time', 'url', 'title', 'abstract']]
    return df_used

"""
   cord_uid                                            content publish_time
0  ug7v899j  [clinical, features, culture-proven, mycoplasm...         2001
1  02tnwd4m  [nitric, oxide, pro-inflammatory, mediator, lu...         2000
2  ejv2xln0  [surfactant, protein-d, pulmonary, host, defen...         2000
3  2b73a28n  [role, endothelin-1, lung, diseaseendothelin-1...         2001
4  9785vg6d  [gene, expression, epithelial, cells, response...         2001

 dic_id_content: {'ug7v899j': ['clinical', 'features', 'culture-proven', 'mycoplasma', 'pneumoniae', 
 dic_id_time: {'ug7v899j': '2001', '02tnwd4m': '2000', 'ejv2xln0': '2000', '2b73a28n': '2001',
"""
def tokenizer(df_selected):
    nlp = spacy.load('en_core_sci_sm', disable=['tagger', 'parser', 'ner'])
    logger = logging.getLogger("spacy")
    logger.setLevel(logging.ERROR)

    for indx in tqdm(df_selected.index):
        sentence = df_selected['content'][indx]
        df_selected['content'][indx] = ' '.join([word.lemma_ for word in nlp(sentence) if not (word.like_num or word.is_stop or word.is_punct or word.is_space or len(word)==1)])
        #df_selected['publish_time'][indx] = df_selected['publish_time'].astype('str')[indx][:4]
    return df_selected

def tokenizer_query(query):
    nlp = spacy.load('en_core_sci_sm', disable=['tagger', 'parser', 'ner'])
    logger = logging.getLogger("spacy")
    logger.setLevel(logging.ERROR)
    return str(' '.join([word.lemma_ for word in nlp(query) if not ( word.like_num or word.is_stop or word.is_punct or word.is_space or len(word) == 1)]))

# save id - content
def save_dics(df_tokenized):
    df1 = df_tokenized[['cord_uid', 'content']]
    dic1 = df1.set_index('cord_uid')['content'].to_dict()
    with open('data_id_content.json', 'w') as fp:
        json.dump(dic1, fp, ensure_ascii=False)

# id - title - time - url
def save_info(df_selected):
    df_useful = df_selected[['cord_uid', 'title', 'publish_time', 'url', 'abstract']]
    dic_title_date_url = df_useful.set_index('cord_uid').T.to_dict('list')
    return dic_title_date_url

def save_info_dic(dic_title_date_url):
    with open('dic_title_date_url.json', 'w') as fp:
        json.dump(dic_title_date_url, fp)

def load_json(file):
    f = open(file)
    json_dic = json.load(f)
    return json_dic

def initialize_elasticsearch(json_dic):
    es.indices.create(index='cfc', ignore=400)
    for key, val in json_dic.items():
        doc = {'content': val}
        es.index(index='cfc', id=key, body= doc)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, threaded=True)