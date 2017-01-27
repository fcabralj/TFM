import aylien_news_api
from aylien_news_api.rest import ApiException

# Configure API key authorization: app_id
aylien_news_api.configuration.api_key['X-AYLIEN-NewsAPI-Application-ID'] = '6efaaf02'
# Configure API key authorization: app_key
aylien_news_api.configuration.api_key['X-AYLIEN-NewsAPI-Application-Key'] = 'b48614535ca0c506b527d0575a500e42'

# create an instance of the API class
api_instance = aylien_news_api.DefaultApi()

opts = {
  'title': 'trump',
  'sort_by': 'social_shares_count.facebook',
  'language': ['en'],
  'published_at_start': 'NOW-7DAYS',
  'published_at_end': 'NOW',
  'entities_body_links_dbpedia': [
    'http://dbpedia.org/resource/Donald_Trump',
    'http://dbpedia.org/resource/Hillary_Rodham_Clinton'
  ]
}

try:
    # List stories
    api_response = api_instance.list_stories(**opts)
    print(api_response)
except ApiException as e:
    print("Exception when calling DefaultApi->list_stories: %s\n" % e)
