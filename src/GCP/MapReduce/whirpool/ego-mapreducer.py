from threading import Thread, current_thread
from decimal import Decimal

from mrjob.job  import MRJob
from mrjob.protocol import JSONProtocol

import json

class MRProductCategoryAgregator(MRJob):

    OUTPUT_PROTOCOL = JSONProtocol
    
    def mapper(self, _, line):
        data = line.decode('utf-8').split(';')
        
        categoriaProduto = data[21]
        dataStruct = json.loads('{ "loja" : "' + data[4] + '", "precoProduto" : "' + data[24] + '" }')
        
        print 'mapper: %s;%s' % (categoriaProduto,dataStruct)
        yield categoriaProduto, dataStruct

    def combiner (self, key, values):
       
       distinctLojasList = []
       precoList = []

       for x in values:
        if ((x['precoProduto']) and (not x['precoProduto'].isspace())):
            strFloat = x['precoProduto'][:(len(x['precoProduto'])-2)] + '.' + x['precoProduto'][(len(x['precoProduto'])-2):]
            precoList.append(float(strFloat))
        if (x['loja'] not in distinctLojasList):
            distinctLojasList.append(x['loja'])
       
       countGenLojas = len(distinctLojasList) 
       
       dataStruct = json.loads('{ "categoriaProduto" : "' + key + '", "lojas" : ' + str(countGenLojas) + ', "precosProduto" : ' + json.dumps(precoList) + ' }')
       
       print 'combiner: %s;%s' % (key, dataStruct)
       yield key, dataStruct


    def reducer(self, key, values):

       distinctLojasList = []
       precoList = []
       
       for x in values:
           for precoItem in x['precosProduto']:
                precoList.append(precoItem)
           if (x['lojas'] not in distinctLojasList):
                distinctLojasList.append(x['lojas'])

       precoMedio = sum(precoList) / float(len(precoList))

       dataStruct = json.loads('{ "categoriaProduto" : "' + key + '", "totalLojas" : ' + str(sum(distinctLojasList)) + ', "mediaPrecosProdutos" : ' + str(precoMedio) + ' }')

       print 'reducer: %s;%s' % (key, dataStruct)
       yield key, dataStruct


if __name__ == '__main__':
    MRProductCategoryAgregator.run()
