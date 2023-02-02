from __future__ import annotations
import time
import re
import urllib
import requests
from requests.auth import HTTPBasicAuth
import json

class Conn(object):
    """
    Class to represent a HTTP connection
    to SNOW. Includes methods to
    request data.
    """
    def __init__(self
        , domain:str
        , user:str
        , pwd:str
        , buffer:int=10000):
        self.domain = domain
        self.user = user
        self.pwd = pwd
        self.buffer = buffer
        self._req_user_id = self.get_req_user_id()

    def __get_req_user_id_url(self):
        path = 'https://{}.service-now.com/api/now/table/{}?'.format(self.domain, 'sys_user')
        param = dict()
        param['sysparm_query'] = '^'.join(['user_name={}'.format(self.user)])
        param['sysparm_fields'] = ','.join(['sys_id'])
        param['sysparm_exclude_reference_link'] = 'true'
        param['sysparm_limit'] = '1'
        param['sysparm_display_value'] = 'false'
        url = path + urllib.parse.urlencode(param)
        return url

    def http_request(self, url:str):
        """
        Make an HTTP request using
        credentials to access Service Now.
        """
        headers = {"Accept":"application/json"}
        r = requests.get(url, headers=headers, auth=HTTPBasicAuth(self.user, self.pwd))
        # print(r.headers)
        if r.status_code!=200:
            print('Status:', r.status_code, 'Headers:', r.headers, 'Error Response:', r.content)
            exit()
        return r

    def get_req_user_id(self)->list:
        """
        Returns requested data in a dictionary list.
        """
        url = self.__get_req_user_id_url()
        r = self.http_request(url)
        rcont = r.content.decode("utf-8-sig").replace("\n","")
        resp_data = json.loads(rcont)
        if 'error' in resp_data.keys():
            raise Exception(resp_data['error']['message'])
        return resp_data['result'][0]['sys_id']
    
class Table(object):
    """
    Class to represent a table from
    Service Now. Needs to receive
    a HTTP conection as argument.
    """
    def __init__(self
        , conn
        , tablename:str
        , reference:bool=False
        , display_value:list=[]):
        self._conn = conn
        self._tablename = tablename
        self._data = []
        self._reference = reference
        self._display_value = display_value
    
    def create_url(self
        , fields:list=None
        , where:list=[]
        , orderby=None
        , offset:int=0
        , limit:int=0)->str:
        path = 'https://{}.service-now.com/api/now/table/{}?'.format(self._conn.domain, self._tablename)
        param = dict()
        w = where.copy() # alterado me 2020-12-03
        if orderby:
            if type(orderby) is list:
                for f in orderby:
                    w.append("ORDERBY{}".format(f)) #^ORDERBYpriority^ORDERBYDESCnumber
            elif type(orderby) is str:
                w.append("ORDERBY{}".format(orderby))
        if w:
            param['sysparm_query'] = '^'.join(w)
        if fields:
            param['sysparm_fields'] = ','.join(fields)
        if not self._reference:
            param['sysparm_exclude_reference_link'] = 'true'
        if offset>0:
            param['sysparm_offset'] = str(offset)
        if limit>0:
            param['sysparm_limit'] = str(limit)
        param['sysparm_display_value'] = 'all' if self._display_value else 'false'
        return path + urllib.parse.urlencode(param)

    def req_data(self
        , fields:list=None
        , where=[]
        , orderby:list=[]
        , offset=0
        , limit:int=0)->list:
        """
        Faz requisicao de uma tabela no sistema
        Service Now.
        if incremental!='':
            w.append('sys_updated_on>{}'.format(incremental))
        """
        url = self.create_url(fields=fields
            , where=where
            , orderby=orderby
            , offset=offset
            , limit=limit)
        print("\nURL de busca: %s" % (url))
        print("\nBuffer: %s" % (str(limit)))
        r = self._conn.http_request(url)
        rcont = r.content.decode("utf-8", "ignore").replace("\n","").replace(",}\"\"", "")
        resp_data = json.loads(rcont)
        if 'error' in resp_data.keys():
            raise Exception(resp_data['error']['message'])
        if not self._display_value:
            return resp_data['result']
        else:
            data = []
            row = {}
            for r in resp_data['result']:
                for k, v in r.items():
                    row[k] = v['display_value'] if k in self._display_value else v['value']
                data.append(row)
                row = {}
            return data

    def select(self
        , fields:list=None
        , where=[]
        , orderby:list=[]
        , offset=0
        , limit:int=0)->list:
        """
        Faz requisicao de uma tabela no sistema
        Service Now.
        """
        w = where.copy()
        sys_updated_pos = self.__find_sys_update_pos(w)
        if sys_updated_pos is None:
            w.append('sys_updated_on>{}'.format('1800-01-01 00:00:00'))
        data = []
        while True:
            time.sleep(2)
            buffer = limit if (limit>0 and limit<self._conn.buffer) else self._conn.buffer
            try:
                _data = self.req_data(fields, w, orderby, offset, buffer)
            except KeyboardInterrupt:
                data += []
                break
            except Exception as e:
                print(e)
                data += []
                break
            #print("Limite antes: {}".format(limit))
            limit = limit - len(_data)
            #print("Limite depois: {}".format(limit))
            #print(self.__get_sys_update(w), self.__get_max_value('sys_updated_on', _data))
            if not len(_data)\
                or (self._tablename=='sys_user' and _data[-1]['sys_id']==self._conn._req_user_id):
                #print(len(_data), self.__get_sys_update(w)==self.__get_max_value('sys_updated_on', _data))
                #print("AAAAA")
                # or self.__get_sys_update(w)==self.__get_max_value('sys_updated_on', _data)\
                break
            # sa_insert(tablename, data, conn) # print("Inseridos %s registros: " % (data_len))
            data += _data
            #print("Tamanho dos dados {}:".format(len(data)))
            #print("Tamanho da requisicao {}:".format(len(_data)))
            #print("Limite restante {}:".format(limit))
            if len(_data)<buffer: # or limit<0:
                break
            self.__replace_sys_update(where=w, sys_updated_on=self.__get_max_value('sys_updated_on', _data)) # _data[-1]['sys_updated_on'])
        print("\nEncontrados %s registros." % len(data))
        return data

    def __get_max_value(self, key:str, data:list):
        """
        Pega o valor maximo de um determinado
        campo nos resultados.
        """
        max = None
        for row in data:
            if type(row) is dict:
                v = row.get(key, None)
                if max is None or str(v)>str(max):
                    max=v
        return max

    def field_list(self, exclude=[])->list:
        """
        Pega a lista de campos contidos no
        ultimo registro encontrado para
        a tabela no ServiceNow.
        """
        data = self.last()
        if data:
            fields = data[0].keys()
            fields = [x for x in fields if x not in exclude]
            return fields
        return []

    def field_size(self, size_rate:bool=0.0, sample_size=15000):
        data = self.select(orderby=['sys_updated_on'], limit=sample_size)
        colsize = self.__get_columns_size(data)
        if size_rate:
            for key, value in colsize.items():
                colsize[key] = round(value + (value*size_rate))
        return colsize

    def __get_columns_size(self, data:list)->dict:
        """
        Recebe um conjunto de dados
        e retorna um dicionario
        com o nome das colunas e
        o tamanho de cada uma delas.
        """
        sizes = {}
        for row in data:
            if type(row) is dict:
                for k, v in row.items():
                    l = len(v)
                    s = sizes.get(k, 0)
                    sizes[k] = s if l<s else l
            else:
                raise Exception("O metodo para encontrar o tamanho dos campos funciona apenas com uma lista de dicionarios.")
        return sizes

    def last(self)->list:
        data = self.req_data(orderby=['DESCsys_updated_on'], limit=1)
        return data

    def first(self)->list:
        data = self.req_data(orderby=['sys_updated_on'], limit=1)
        return data

    def fetch(self)->list:
        pass

    def __find_sys_update_pos(self, where:list):
        """
        Pega a posicao no filtro em que a data
        sys_update_on estah sendo usado.
        """
        for i, cond in enumerate(where):
            if cond.startswith('sys_updated_on'):
                return i

    def __get_sys_update(self, where:list):
        """
        Pega a data que estah sendo usando no filtro.
        """
        pos = self.__find_sys_update_pos(where)
        if pos is not None:
            m = re.findall(r'sys_updated_on[\>\<\=]+(.*)', where[pos])
            if not m:
                raise Exception("Nao foi possivel localizar a data de sys_updated_on. Vefirique se '%s' esta num formato valido." % where[pos])
            return m[0]

    def __replace_sys_update(self, where:list, sys_updated_on:str):
        """
        Substitui a data de update do filtro.
        """
        pos = self.__find_sys_update_pos(where)
        if pos is not None:
            where[pos] = 'sys_updated_on>{}'.format(sys_updated_on)
