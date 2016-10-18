from elasticsearch import (
    Elasticsearch,
    RequestsHttpConnection
)
from config import (
    CONNECTION_URL
)


__all__ = [
    'CreateAndDeleteIndexSchemaElasticSearch',
    'UpdateSchemaElasticSearch',
    'ElasticSearchQuery',
    'InsertDataElasticSearch',
    'UpdateDataElasticSearch'
    'create_delete_schema_obj',
    'query_obj',
    'insert_index_obj',
    'update_data_obj'
]


class CreateAndDeleteIndexSchemaElasticSearch(object):

    def __init__(self, connection_url):
        self.connection_url = connection_url
        self.elastic_obj = None
        self.set_elastic_search_object()

    def set_elastic_search_object(self):
        self.elastic_obj = Elasticsearch(hosts=self.connection_url,
                                         use_ssl=True, verify_certs=True,
                                         connection_class=
                                         RequestsHttpConnection)
        return self.elastic_obj

    def create_schema(self, index_name, **kwargs):
        response = self.elastic_obj.indices.create(index=index_name,
                                                   ignore=400, body=kwargs)
        return response

    def delete_index(self, index_name):
        response = self.elastic_obj.indices.delete(index=index_name)
        return response


class UpdateSchemaElasticSearch(object):

    def __init__(self, elastic_obj):
        self.elastic_obj = elastic_obj

    def update_schema(self):
        pass

    def delete_schema(self):
        pass


class InsertDataElasticSearch(object):

    def __init__(self, connection_url):
        self.connection_url = connection_url
        self.elastic_obj = None
        self.set_elastic_search_object()

    def set_elastic_search_object(self):
        self.elastic_obj = Elasticsearch(hosts=self.connection_url,
                                         use_ssl=True, verify_certs=True,
                                         connection_class=
                                         RequestsHttpConnection)
        return self.elastic_obj

    def insert_data_elastic_search(self, index_name, doc_type, _id, **kwargs):

        response = self.elastic_obj.index(index=index_name,
                                          doc_type=doc_type, id=_id,
                                          body=kwargs)
        return response


class ElasticSearchQuery(object):

    def __init__(self, connection_url):
        self.connection_url = connection_url
        self.elastic_obj = None
        self.set_elastic_search_object()

    def set_elastic_search_object(self):
        self.elastic_obj = Elasticsearch(hosts=self.connection_url,
                                         use_ssl=True, verify_certs=True,
                                         connection_class=
                                         RequestsHttpConnection)
        return None

    def get_similar_items(self, index_name, doc_type, **kwargs):
        response = self.elastic_obj.search(index=index_name,
                                           doc_type=doc_type, body=kwargs)
        return response


class UpdateDataElasticSearch(object):

    def __init__(self, connection_url):
        self.connection_url = connection_url
        self.elastic_obj = None

    @property
    def set_elastic_search_object(self):
        self.elastic_obj = Elasticsearch(hosts=self.connection_url,
                                         use_ssl=True, verify_certs=True,
                                         connection_class=
                                         RequestsHttpConnection)
        return None

    def get_set_data_es(self, index_name, doc_type, _id, **kwargs):
        response = self.elastic_obj.update(index=index_name,
                                           doc_type=doc_type, id=_id,
                                           body=kwargs)
        return response


create_delete_schema_obj = CreateAndDeleteIndexSchemaElasticSearch(
    CONNECTION_URL)
query_obj = ElasticSearchQuery(CONNECTION_URL)
insert_index_obj = InsertDataElasticSearch(CONNECTION_URL)
update_data_obj = UpdateDataElasticSearch(CONNECTION_URL)
