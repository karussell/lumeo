Lumeo - Implementing Tinkerpop Blueprints API via Lucene

Implements a real time cache to avoid commiting after adding a node - see RawLucene.realTimeCache

Main To do's:
 * make blueprints tests passing
 * use in-memory codec to make id processing faster 
   (ie. make it a *graph* processing framework - not a graph querying one)
 * store properties into one field (same as the _source field in ElasticSearch)

Code stands under Apache License 2.0

Blueprints
https://github.com/tinkerpop/blueprints/wiki/

Lucene
http://lucene.apache.org/core/