import { assertEquals } from "https://deno.land/std@0.120.0/testing/asserts.ts";
import { Hexastore } from "../index.ts";

Deno.test("Simple query", async () => {
  const hexastore = await Hexastore.get(
    {
      hostname: "127.0.0.1",
      port: 6379,
    },
    {
      name: "query-test",
    }
  );

  await hexastore.batchSave([
    {
      subject: "Ana",
      predicate: "has",
      object: "Red Apple",
    },
    {
      subject: "Ana",
      predicate: "has",
      object: "Green Apple",
    },
    {
      subject: "Maria",
      predicate: "owns",
      object: "Common Pear",
    },
    {
      subject: "Ana",
      predicate: "owns",
      object: "Common Pear",
    },
    {
      subject: "Mark",
      predicate: "sells",
      object: "Green Apple",
    },
  ]);

  const whoHas = await hexastore.query({
    predicate: "has",
  });

  assertEquals(whoHas, [
    {
      subject: "Ana",
      predicate: "has",
      object: "Green Apple",
    },
    {
      subject: "Ana",
      predicate: "has",
      object: "Red Apple",
    },
  ]);

  const whoDoesThat = await hexastore.query({
    object: "divides by",
  });

  assertEquals(whoDoesThat, []);

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

  assertEquals(germans, [
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
  ]);

  const beerLikers = await hexastore.query({
    predicate: "likes",
    object: "beer",
  });

  assertEquals(beerLikers, [
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
  ]);

  const who = hexastore.v("who");
  const what = hexastore.v("what");
  const where = hexastore.v("where");

  const query = await hexastore.search([
    { subject: who, predicate: "likes", object: what },
    { subject: who, predicate: "lives", object: where },
  ]);

  assertEquals(query, {
    what: new Set(["beer"]),
    where: new Set(["Germany", "Romania"]),
    who: new Set(["Adrian", "Ana"]),
  });

  const filter = await hexastore.filter({
    filter: ["predicate", ["lia", "liz"]],
  });

  assertEquals(filter, [
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
    {
      object: "Germany",
      predicate: "lives",
      subject: "Ana",
    },
    {
      object: "Germany",
      predicate: "lives",
      subject: "Erika",
    },
    {
      object: "Romania",
      predicate: "lives",
      subject: "Adrian",
    },
  ]);

  hexastore.close();
});
