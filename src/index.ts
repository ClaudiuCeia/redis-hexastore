import {
  connect,
  RawOrError,
  Redis,
  RedisPipeline,
} from "https://deno.land/x/redis@v0.26.0/mod.ts";
import { idx } from "./idx.ts";
import { Except } from "./types/Except.ts";
import { RequireExactlyOne } from "./types/RequireExactlyOne.ts";
import { zip } from "./zip.ts";

type HexastoreOpts = {
  name: string;
  prefix?: string;
};

type RedisOpts = {
  hostname: string;
  port: number;
  password?: string,
  username?: string,
};

export type HexastoreTriple = {
  predicate: string;
  subject: string;
  object: string;
};

export type HexastoreFilterStatement<T extends keyof HexastoreTriple> =
  | [T, [string | undefined, string]]
  | [T, [string, string | undefined]];

export type HexastoreFilter<T extends keyof HexastoreTriple> = {
  filter: HexastoreFilterStatement<T>;
} & Partial<Except<HexastoreTriple, T>>;

export type HexastoreHashEncoding =
  | "spo"
  | "sop"
  | "pos"
  | "osp"
  | "pso"
  | "ops";

export type HexastoreHashEncodingChar = "s" | "o" | "p";

export type HexastoreSearchStatement = {
  [Key in keyof HexastoreTriple]: HexastoreTriple[Key] | symbol;
};

export type HexastoreSearchResult = {
  [key: string]: Set<string>;
};

export type HexastorePagination = {
  after?: HexastoreTriple;
  first?: number;
  before?: HexastoreTriple;
  last?: number;
};

export type HexastoreCountParams = RequireExactlyOne<
  {
    cursor: HexastoreTriple;
    query?: Partial<HexastoreTriple>;
    filter?: HexastoreFilter<keyof HexastoreTriple>;
  },
  "query" | "filter"
>;

/**
 * When storing indices, we use compound keys in a sorted set.
 * This separator was chosen to avoid conflicts with either of the values.
 */
const SEPARATOR = "\0\0";
const DEFAULT_PAGE_SIZE = 100;

export class Hexastore {
  private constructor(
    private redis: Redis,
    private redisOpts: RedisOpts,
    private options: HexastoreOpts
  ) {}

  public static async get(
    redisOpts: RedisOpts,
    opts: HexastoreOpts
  ): Promise<Hexastore> {
    const redis = await connect(redisOpts);

    return new Hexastore(redis, redisOpts, {
      ...opts,
      prefix: opts.prefix || "hexastore",
    });
  }

  // Get the Redis key for this hexastore store
  public getKey(): string {
    return `${this.options.prefix}:${this.options.name}`;
  }

  // Get index values for a triple
  private static getHashes(triple: HexastoreTriple): string[] {
    const { subject, object, predicate } = triple;

    const hex = [
      ["spo", subject, predicate, object],
      ["sop", subject, object, predicate],
      ["pos", predicate, object, subject],
      ["osp", object, subject, predicate],
      ["pso", predicate, subject, object],
      ["ops", object, predicate, subject],
    ];

    return hex.map((triple) => triple.join(SEPARATOR));
  }

  /**
   * Given a partial triple (all or some of the triple members),
   * query indices that match it.
   */
  public static getHashRange(
    triple: Partial<HexastoreTriple>
  ): [string, string | undefined] {
    const segments: string[] = [];

    const { subject, object, predicate } = triple;

    // For a full triple, there is just one result
    if (subject && predicate && object) {
      segments.push("spo", subject, predicate, object);
      return [Hexastore.getHashFromSegments(segments), undefined];
    }
    if (subject && predicate) {
      segments.push("spo", subject, predicate);
    } else if (subject && object) {
      segments.push("osp", object, subject);
    } else if (predicate && object) {
      segments.push("pos", predicate, object);
    } else if (subject) {
      segments.push("spo", subject);
    } else if (predicate) {
      segments.push("pos", predicate);
    } else if (object) {
      segments.push("osp", object);
    }
    return [
      `[${Hexastore.getHashFromSegments(segments)}`,
      `[${Hexastore.getHashFromSegments(segments.concat("\xff"))}`,
    ];
  }

  private static getHashFromSegments(segs: string[]): string {
    return segs.join(SEPARATOR);
  }

  /**
   * Hexastore filter statements may omit the min or the max value, which
   * is the equivalent of a "less than" or "greater than" operation.
   */
  private static getRangeCapsFromFilter(
    filter: HexastoreFilter<keyof HexastoreTriple>
  ): [string, string] {
    const [min, max] = filter.filter[1];
    return [min ?? "", max ?? "\xff"];
  }

  private static getSubjectFilterRange(
    filter: HexastoreFilter<"subject">
  ): [string, string] {
    const { predicate, object } = filter;
    const [min, max] = Hexastore.getRangeCapsFromFilter(filter);

    let start: string[];
    let end: string[];

    if (predicate && object) {
      start = ["pos", predicate, object, min];
      end = ["pos", predicate, object, max];
    } else if (predicate) {
      start = ["pso", predicate, min];
      end = ["pso", predicate, max];
    } else if (object) {
      start = ["osp", object, min];
      end = ["osp", object, max];
    } else {
      start = ["spo", min];
      end = ["spo", max];
    }

    return [
      `[${Hexastore.getHashFromSegments(start)}`,
      `[${Hexastore.getHashFromSegments(end)}`,
    ];
  }

  private static getObjectFilterRange(
    filter: HexastoreFilter<"object">
  ): [string, string] {
    const { predicate, subject } = filter;
    const [min, max] = Hexastore.getRangeCapsFromFilter(filter);

    let start: string[];
    let end: string[];

    if (subject && predicate) {
      start = ["spo", subject, predicate, min];
      end = ["osp", subject, predicate, max];
    } else if (subject) {
      start = ["spo", subject, min];
      end = ["spo", subject, max];
    } else if (predicate) {
      start = ["pos", predicate, min];
      end = ["pos", predicate, max];
    } else {
      start = ["osp", min];
      end = ["osp", max];
    }

    return [
      `[${Hexastore.getHashFromSegments(start)}`,
      `[${Hexastore.getHashFromSegments(end)}`,
    ];
  }

  private static getPredicateFilterRange(
    filter: HexastoreFilter<"predicate">
  ): [string, string] {
    const { object, subject } = filter;
    const [min, max] = Hexastore.getRangeCapsFromFilter(filter);

    let start: string[];
    let end: string[];

    if (subject && object) {
      start = ["osp", object, subject, min];
      end = ["osp", object, subject, max];
    } else if (subject) {
      start = ["spo", subject, min];
      end = ["spo", subject, max];
    } else if (object) {
      start = ["ops", object, min];
      end = ["ops", object, max];
    } else {
      start = ["pos", min];
      end = ["pos", max];
    }

    return [
      `[${Hexastore.getHashFromSegments(start)}`,
      `[${Hexastore.getHashFromSegments(end)}`,
    ];
  }

  /**
   * From a filter statement, generate the start and end keys to query for
   */
  private static getHashFilterRange<T extends keyof HexastoreTriple>(
    filter: HexastoreFilter<T>
  ): [string, string] {
    const param = filter.filter[0];
    const [min, max] = filter.filter[1];
    if (min === undefined && max === undefined) {
      throw new Error(`
        Filter ranges can't be open ended on both ends - you need to 
        provide either a max value, a min value, or both.
      `);
    }

    if (param === "subject") {
      return Hexastore.getSubjectFilterRange(
        filter as HexastoreFilter<"subject">
      );
    }

    if (param === "predicate") {
      return Hexastore.getPredicateFilterRange(
        filter as HexastoreFilter<"predicate">
      );
    }

    if (param === "object") {
      return Hexastore.getObjectFilterRange(
        filter as HexastoreFilter<"object">
      );
    }

    throw new Error(`
      Invalid Hexastore filter request:
      ${JSON.stringify(filter)}
    `);
  }

  /**
   * Index a triple as an individual atomic operation
   */
  public async save(triple: HexastoreTriple): Promise<string[]>;

  /**
   * Index a triple using a given Redis pipeline. This is useful if you
   * want to ensure that indices are always written together with the data
   * you're indexing.
   */
  public async save(
    triple: HexastoreTriple,
    pipeline: RedisPipeline
  ): Promise<string[]>;

  public async save(
    triple: HexastoreTriple,
    pipeline?: RedisPipeline
  ): Promise<string[]> {
    const values = Hexastore.getHashes(triple);

    let newClient: Redis | undefined;
    if (!pipeline) {
      newClient = await connect(this.redisOpts);
      pipeline = newClient.pipeline();
    }

    for (const value of values) {
      pipeline.zadd(this.getKey(), 0, value);
    }

    await pipeline.flush();

    if (newClient) {
      newClient.close();
    }

    return values;
  }

  public async batchSave(triples: HexastoreTriple[]): Promise<void>;

  public async batchSave(
    triples: HexastoreTriple[],
    pipeline: RedisPipeline
  ): Promise<void>;

  public async batchSave(
    triples: HexastoreTriple[],
    pipeline?: RedisPipeline
  ): Promise<void> {
    let newClient: Redis | undefined;
    if (!pipeline) {
      newClient = await connect(this.redisOpts);
      pipeline = newClient.pipeline();
    }

    for (const triple of triples) {
      this.stageSave(triple, pipeline);
    }

    await pipeline.flush();

    if (newClient) {
      newClient.close();
    }
  }

  /**
   * Index a triple, but only stage the changes to a Redis pipeline
   * without executing the transaction
   */
  public stageSave(
    triple: HexastoreTriple,
    pipeline: RedisPipeline
  ): RedisPipeline {
    const values = Hexastore.getHashes(triple);
    values.forEach((value) => pipeline.zadd(this.getKey(), 0, value));
    return pipeline;
  }

  /**
   * Delete indices for a triple
   */
  public async delete(triple: Partial<HexastoreTriple>): Promise<number>;

  /**
   * Delete indices for a triple using an existing Redis pipeline, ensuring
   * atomicity.
   */
  public async delete(
    triple: Partial<HexastoreTriple>,
    pipeline: RedisPipeline
  ): Promise<RawOrError[] | number>;

  public async delete(
    triple: Partial<HexastoreTriple>,
    pipeline?: RedisPipeline
  ): Promise<RawOrError[] | number> {
    // For partial triples, remove the range
    const [start, end] = Hexastore.getHashRange(triple);
    if (end !== undefined) {
      if (pipeline) {
        pipeline.zremrangebylex(this.getKey(), start, end);
        return pipeline.flush();
      } else {
        return this.redis.zremrangebylex(this.getKey(), start, end);
      }
    }

    // For fully-defined triples, remove all indices
    const values = Hexastore.getHashes({
      subject: idx(triple.subject),
      object: idx(triple.object),
      predicate: idx(triple.predicate),
    });

    let newClient: Redis | undefined;
    if (!pipeline) {
      newClient = await connect(this.redisOpts);
      pipeline = newClient.pipeline();
    }

    for (const value of values) {
      pipeline.zrem(this.getKey(), value);
    }

    await pipeline.flush();

    if (newClient) {
      newClient.close();
    }

    return values.length;
  }

  /**
   * Stage a triple deletion, but don't execute the pipeline
   */
  public stageDelete(
    triple: Partial<HexastoreTriple>,
    pipeline: RedisPipeline
  ): RedisPipeline {
    const [start, end] = Hexastore.getHashRange(triple);
    if (end !== undefined) {
      pipeline.zremrangebylex(this.getKey(), start, end);
      return pipeline;
    }

    pipeline.zrem(this.getKey(), start);
    return pipeline;
  }

  public async batchDelete(
    triples: Partial<HexastoreTriple>[]
  ): Promise<number>;

  public async batchDelete(
    triples: Partial<HexastoreTriple>[],
    pipeline: RedisPipeline
  ): Promise<RawOrError[] | number>;

  public async batchDelete(
    triples: Partial<HexastoreTriple>[],
    pipeline?: RedisPipeline
  ): Promise<RawOrError[] | number> {
    let newClient: Redis | undefined;
    if (!pipeline) {
      newClient = await connect(this.redisOpts);
      pipeline = newClient.pipeline();
    }

    for (const triple of triples) {
      this.stageDelete(triple, pipeline);
    }

    const res = await pipeline.flush();

    if (newClient) {
      newClient.close();
    }

    return res;
  }

  private static getTripleHash(
    encoding: HexastoreHashEncoding,
    triple: HexastoreTriple
  ): string {
    const chars = encoding.split("") as HexastoreHashEncodingChar[];
    const hash = chars.map((char) => {
      switch (char) {
        case "s":
          return triple.subject;
        case "o":
          return triple.object;
        case "p":
          return triple.predicate;
      }
    });

    return `${encoding}${SEPARATOR}${hash.join(SEPARATOR)}`;
  }

  private static getNameFromEncodingChar(
    char: HexastoreHashEncodingChar
  ): string {
    switch (char) {
      case "s":
        return "subject";
      case "p":
        return "predicate";
      case "o":
        return "object";
      default:
        break;
    }

    throw new Error(`
      Bad SOP character input. Expected "s", "o" or "p", got "${char}"
    `);
  }

  private static getEncodingFromHash(hash: string): HexastoreHashEncoding {
    const [encoding] = hash.split(SEPARATOR);
    // Remove Redis exclusive/inclusive specifier (first char)
    return encoding.slice(1) as HexastoreHashEncoding;
  }

  // Given a start and an end index, query all of the values in that range
  private async queryRange(
    start: string,
    end: string,
    pagination?: HexastorePagination
  ): Promise<HexastoreTriple[]> {
    const [startEncoding, endEncoding] = [
      Hexastore.getEncodingFromHash(start),
      Hexastore.getEncodingFromHash(end),
    ];

    if (startEncoding !== endEncoding) {
      throw new Error(`
        Invalid query range provided. The start and end encoding has to be 
        the same, received ${start} and ${end}.
      `);
    }

    let hashes: string[] = [];
    if (!pagination) {
      pagination = {
        first: DEFAULT_PAGE_SIZE,
      };
    }

    const { first, last, after, before } = pagination;

    if ((first || after) && (last || before)) {
      throw new Error(`
        Invalid pagination parameters. You can either paginate forward 
        using { first, after } or backwards using { last, before }. 
        Received ${JSON.stringify(pagination)}
      `);
    }

    if (first || after) {
      const limit = first ?? DEFAULT_PAGE_SIZE;
      start = after
        ? `(${Hexastore.getTripleHash(startEncoding, after)}`
        : start;

      hashes = await this.redis.zrangebylex(this.getKey(), start, end, {
        limit: {
          offset: 0,
          count: limit,
        },
      });
    } else if (last || before) {
      const limit = last ?? DEFAULT_PAGE_SIZE;
      end = before ? `(${Hexastore.getTripleHash(startEncoding, before)}` : end;

      hashes = await this.redis.zrevrangebylex(this.getKey(), end, start, {
        limit: {
          offset: 0,
          count: limit,
        },
      });
    }

    if (!hashes) {
      throw new Error("No results");
    }

    return hashes.map((hash: string) => {
      const [keys, seg1, seg2, seg3] = hash.split(SEPARATOR);

      const zipped = zip(keys.split(""), [seg1, seg2, seg3]);

      return {
        [Hexastore.getNameFromEncodingChar(
          zipped[0][0] as HexastoreHashEncodingChar
        )]: zipped[0][1],
        [Hexastore.getNameFromEncodingChar(
          zipped[1][0] as HexastoreHashEncodingChar
        )]: zipped[1][1],
        [Hexastore.getNameFromEncodingChar(
          zipped[2][0] as HexastoreHashEncodingChar
        )]: zipped[2][1],
      } as HexastoreTriple;
    });
  }

  // Query indices that match a given partial triple.
  public async query(
    query: Partial<HexastoreTriple>,
    pagination?: HexastorePagination
  ): Promise<HexastoreTriple[]> {
    const [start, end] = Hexastore.getHashRange(query);

    if (!end) {
      const exists = await this.redis.zscore(this.getKey(), start);
      if (exists !== null) {
        return [
          {
            subject: idx(query.subject),
            predicate: idx(query.predicate),
            object: idx(query.object),
          },
        ];
      } else {
        return [];
      }
    }

    return this.queryRange(start, end, pagination);
  }

  public async count(
    direction: "before" | "after",
    params: HexastoreCountParams
  ): Promise<number> {
    const { cursor, query, filter } = params;

    if (query && filter) {
      throw new Error(`
        You can count leading/trailing triples using either
        a query and cursor, or a filter and cursor, but not both.
      `);
    }

    if (!query && !filter) {
      throw new Error(`
        You can't count leading/trailing triples without
        specifying a query or filter.
      `);
    }

    let start = "";
    let end = "";

    if (query) {
      const [maybeStart, maybeEnd] = Hexastore.getHashRange(query);
      if (!maybeEnd) {
        /**
         * A full query was specified so we only have 1 result, and no
         * leading/trailing triples.
         */
        return 0;
      }

      [start, end] = [maybeStart, maybeEnd];
    }

    if (filter) {
      [start, end] = Hexastore.getHashFilterRange(filter);
    }

    const startEncoding = Hexastore.getEncodingFromHash(start);
    switch (direction) {
      case "after": {
        start = cursor
          ? `(${Hexastore.getTripleHash(startEncoding, cursor)}`
          : start;

        return await this.redis.zlexcount(this.getKey(), start, end);
      }
      case "before": {
        end = cursor
          ? `(${Hexastore.getTripleHash(startEncoding, cursor)}`
          : end;

        return await this.redis.zlexcount(this.getKey(), start, end);
      }
    }
  }

  // Query indices that match a given filter
  public async filter<T extends keyof HexastoreTriple>(
    filter: HexastoreFilter<T>,
    pagination?: HexastorePagination
  ): Promise<HexastoreTriple[]> {
    const [start, end] = Hexastore.getHashFilterRange(filter);
    return await this.queryRange(start, end, pagination);
  }

  /**
   * Symbol is scoped to this instance - can't declare this
   * method as static since we won't have access to the
   * symbol we generated anymore
   */
  // eslint-disable-next-line class-methods-use-this
  public v(variableName: string): symbol {
    return Symbol.for(variableName);
  }

  protected static isComplexStatement({
    subject,
    predicate,
    object,
  }: HexastoreSearchStatement): boolean {
    const isSym = (part: unknown): boolean => typeof part === "symbol";
    if (isSym(subject) && isSym(predicate) && isSym(object)) {
      return true;
    }
    return false;
  }

  /**
   * Allow querying the hexastore by providing statements (which are really just
   * partial triples). An example would be getting all of the users
   * related to a company, and all the intermediary objects for the
   * specified path.
   *
   * const hexastore = Hexastore.get("graph");
   * const A = hexastore.v("companies");
   * const B = hexastore.v("users");
   *
   * const usersRelatedToShop = await hexastore.search([
   *   { subject: shopID, predicate: "owned_by", object: A},
   *   { subject: A, predicate: "shareholder", object: B },
   *
   * console.log(usersRelatedToShop);
   * {
   *   companies: Set[...],
   *   entities: Set[...]
   * }
   *
   */
  public async search(
    statements: HexastoreSearchStatement[]
  ): Promise<HexastoreSearchResult> {
    const results: { filteredResults: HexastoreTriple[] }[] = [];
    const materialized: { [key: string]: Set<string> } = {};

    for (const statement of statements) {
      if (Hexastore.isComplexStatement(statement)) {
        const subject = Symbol.keyFor(statement.subject as symbol);
        const predicate = Symbol.keyFor(statement.predicate as symbol);
        const object = Symbol.keyFor(statement.object as symbol);

        throw new Error(`
          Complex queries, with three variables per statement, 
          are not permitted at the moment. Received 
          "{ subject: ${subject}, predicate: ${predicate}, object: ${object} }"
        `);
      }

      const query = statement;
      const targets: { [key: string]: keyof HexastoreTriple } = {};

      for (const [key, part] of Object.entries(statement)) {
        if (typeof part === "symbol") {
          // Get variable symbol key
          const symbolKey = Symbol.keyFor(part);

          // If no key in local scope, the user didn't use hexastore.v()
          if (!symbolKey) {
            throw new Error(`Variable ${part.toString()} unknown`);
          }

          // Add the variables to targets as { var: keyof HexastoreTriple }
          targets[symbolKey] = key as keyof HexastoreTriple;
          // And remove the variable from the query to get the partial
          delete query[key as keyof HexastoreTriple];
        }
      }

      /**
       *  Query partial edge. We actually need to use await here since
       *  the individual queries are dependent on each other and they should
       *  run serially.
       */
      // eslint-disable-next-line no-await-in-loop
      const result = await this.query(query as Partial<HexastoreTriple>);

      const filteredResults: HexastoreTriple[] = [];
      // If we have materialized variables
      if (Object.keys(materialized).length) {
        // Walk the results
        for (const triple of result) {
          // Look at the current variable targets
          for (const [key, target] of Object.entries(targets)) {
            /**
             * If we materialized one of the variables in the current
             * statement, push it to the filtered results
             */
            if (materialized[key] && materialized[key].has(triple[target])) {
              filteredResults.push(triple);
            }
          }
        }
      } else {
        // With no materialized variables, just assign the full result
        filteredResults.push(...result);
      }

      // Set all current targets as materialized, with the result values
      for (const [key, target] of Object.entries(targets)) {
        materialized[key] = new Set(
          filteredResults.map((triple) => triple[target])
        );
      }

      results.push({
        filteredResults,
      });
    }

    return materialized;
  }

  public close(): void {
    return this.redis.close();
  }
}
