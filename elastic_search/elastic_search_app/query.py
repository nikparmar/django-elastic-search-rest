__all__ = [
    'get_query'
]


def get_query(key, keyword):
    _query = {"query":
                    {"bool":
                         {"must":
                              [
                                  {"match_phrase_prefix":
                                    {
                                        key: keyword
                                     }
                                }
                               ],
                          "must_not":
                              [],
                          "should":[]
                          }
                     },
                "from": 0, "size": 10, "sort": [], "aggs": {}
                }
    return _query