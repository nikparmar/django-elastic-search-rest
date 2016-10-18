from django.shortcuts import render

# Create your views here.
from rest_framework.views import (
    APIView
)
from rest_framework.response import (
    Response
)
from rest_framework.status import (
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_201_CREATED,
    HTTP_200_OK
)
from es_services import (
    CreateAndDeleteIndexSchemaElasticSearch,
    UpdateSchemaElasticSearch,
    ElasticSearchQuery,
    InsertDataElasticSearch,
    UpdateDataElasticSearch
)
from config import (
   CONNECTION_URL
)

from query import (
    get_query
)



__all__ = [
    'CreateSchema',
    'DeleteIndex',
    'GetSimilarItems',
    'PushDataAndIndex',
    'UpdateDataEs',
    'GetSimilarFoodItems',
    'ExcerciseSearch'
]


class CreateSchema(APIView):
    """
    Create Index Schema in ElasticSearch
    """
    def post(self, request):
        index_name = request.data['index_name']
        schema = request.data['schema']
        obj = CreateAndDeleteIndexSchemaElasticSearch(CONNECTION_URL)
        response = obj.create_schema(index_name, **schema)
        if 'error' in response:
            return Response({'message': response['error']},
                            status=HTTP_500_INTERNAL_SERVER_ERROR)
        return Response({'message': response['acknowledged']},
                        status=HTTP_201_CREATED)


class DeleteIndex(APIView):
    """
    Delete Index in ElasticSearch
    """
    def post(self, request):
        index_name = request.body.get('index_name')
        obj = CreateAndDeleteIndexSchemaElasticSearch(CONNECTION_URL)
        response = obj.delete_index(index_name)
        if response is True:
            return True
        return False


class GetSimilarItems(APIView):
    """
    Get all the Similar items from Elastic Search
    """
    def post(self, request):
        index_name = request.data['index_name']
        search_data = request.data['search_data']
        doc_type = request.data['doc_type']
        # search_params = request.query_params['search_params']
        obj = ElasticSearchQuery(CONNECTION_URL)
        response = obj.get_similar_items(index_name, doc_type, **search_data)
        return Response({'message': response}, status=HTTP_200_OK)


class PushDataAndIndex(APIView):
    """
    Post Data into Elastic Search and Index
    """
    def post(self, request):
        index_name = request.data['index_name']
        payload = request.data['data']
        doc_type = request.data['doc_type']
        _id = request.data['_id']
        obj = InsertDataElasticSearch(CONNECTION_URL)
        response = obj.insert_data_elastic_search(index_name, doc_type, _id,
                                                  **payload)
        if 'error' in response:
            return Response({'message': response['error']},
                            status=HTTP_500_INTERNAL_SERVER_ERROR)
        return Response({'message': response['created']},
                        status=HTTP_201_CREATED)


class UpdateDataEs(APIView):

    def put(self, request):
        index_name = request.data['index_name']
        doc_type = request.data['doc_type']
        update_data =request.data['data']
        _id = request.data['_id']
        obj = UpdateDataElasticSearch(CONNECTION_URL)
        response = obj.get_set_data_es(index_name, doc_type, _id,
                                        **update_data)
        return Response({'message': response}, status=HTTP_200_OK)

