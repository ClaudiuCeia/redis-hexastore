import { assertEquals } from "https://deno.land/std@0.120.0/testing/asserts.ts";
import { Hexastore } from "../index.ts";

Deno.test("Simple query", async () => {
  const hexastore = await Hexastore.get({
    name: "query-test",
    hostname: "127.0.0.1",
    port: "6379",
  });

  await Promise.all([
    hexastore.save({
      subject: "Ana",
      predicate: "has",
      object: "Red Apple",
    }),
    hexastore.save({
      subject: "Ana",
      predicate: "has",
      object: "Green Apple",
    }),
    hexastore.save({
      subject: "Maria",
      predicate: "owns",
      object: "Common Pear",
    }),
    hexastore.save({
      subject: "Ana",
      predicate: "owns",
      object: "Common Pear",
    }),
    hexastore.save({
      subject: "Mark",
      predicate: "sells",
      object: "Green Apple",
    }),
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
});
