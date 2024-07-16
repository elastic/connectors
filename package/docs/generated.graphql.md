<!-- markdownlint-disable -->

<a href="../../package/generated/graphql.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.graphql`






---

<a href="../../package/generated/graphql.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `GraphQLConnector`
GraphQLConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`http_endpoint`</b> (str):  GraphQL HTTP endpoint 


 - <b>`http_method`</b> (str):  HTTP method for GraphQL requests 


 - <b>`authentication_method`</b> (str):  Authentication Method 


 - <b>`username`</b> (str):  Username 


 - <b>`password`</b> (str):  Password 


 - <b>`token`</b> (str):  Bearer Token 


 - <b>`graphql_query`</b> (str):  GraphQL Body 


 - <b>`graphql_variables`</b> (str):  Graphql Variables 


 - <b>`graphql_object_to_id_map`</b> (str):  GraphQL Objects to ID mapping 
        - Specifies which GraphQL objects should be indexed as individual documents. This allows finer control over indexing, ensuring only relevant data sections from the GraphQL response are stored as separate documents. Use a JSON with key as the GraphQL object name and value as string field within the document, with the requirement that each document must have a distinct value for this field. Use '.' to provide full path of the object from the root of the response. For example {'organization.users.nodes': 'id'} 


 - <b>`headers`</b> (str):  Headers 


 - <b>`pagination_model`</b> (str):  Pagination model 
        - For cursor-based pagination, add 'pageInfo' and an 'after' argument variable in your query at the desired node (Pagination key). Use 'after' query argument with a variable to iterate through pages. Detailed examples and setup instructions are available in the docs. 


 - <b>`pagination_key`</b> (str):  Pagination key 
        - Specifies which GraphQL object is used for pagination. Use '.' to provide full path of the object from the root of the response. For example 'organization.users' 


 - <b>`connection_timeout`</b> (int):  Connection Timeout 

<a href="../../package/generated/graphql.py#L51"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    http_endpoint=None,
    http_method='post',
    authentication_method='none',
    username=None,
    password=None,
    token=None,
    graphql_query=None,
    graphql_variables=None,
    graphql_object_to_id_map=None,
    headers=None,
    pagination_model='no_pagination',
    pagination_key=None,
    connection_timeout=300,
    **kwargs
)
```









