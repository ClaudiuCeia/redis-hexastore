# redis-hexastore

A lightweight
[hexastore](http://redis.io/topics/indexes#representing-and-querying-graphs-using-an-hexastore)
implementation, uzing
[Redis](https://redis.io/docs/manual/data-types/#sorted-sets) sorted sets. It
allows storing graphs as triples (`subject`, `predicate`, `object`) and
querying them in an efficient manner.

## Example

```ts
const hexastore = await Hexastore.get(
  {
    hostname: "127.0.0.1",
    port: 6379,
  },
  {
    name: "example",
  },
);

await hexastore.batchSave([
  {
    subject: "Adrian",
    predicate: "lives",
    object: "Romania",
  },
  {
    subject: "Ana",
    predicate: "lives",
    object: "Germany",
  },
  {
    subject: "Erika",
    predicate: "lives",
    object: "Germany",
  },
  {
    subject: "Adrian",
    predicate: "likes",
    object: "beer",
  },
  {
    subject: "Ana",
    predicate: "likes",
    object: "beer",
  },
]);

const germans = await hexastore.query({
  predicate: "lives",
  object: "Germany",
});

/**
   [{
    subject: "Ana",
    predicate: "lives",
    object: "Germany",
   },
   {
    subject: "Erika",
    predicate: "lives",
    object: "Germany",
   }]
*/

const beerLikers = await hexastore.query({
  predicate: "likes",
  object: "beer",
});

/**
  [
    {
      object: "beer",
      predicate: "likes",
      subject: "Adrian",
    },
    {
      object: "beer",
      predicate: "likes",
      subject: "Ana",
    },
  ]
*/

hexastore.close();
```