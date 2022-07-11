import { connect, Redis } from "https://deno.land/x/redis@v0.26.0/mod.ts";
import { Except } from "./types/Except.ts";

type HexastoreOpts = {
  name: string;
};

type RedisOpts = {
  hostname: string;
  port: string;
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

/**
 * When storing indices, we use compound keys in a sorted set.
 * This separator was chosen to avoid conflicts with either of the values.
 */
const SEPARATOR = "\0\0";

export class Hexastore {
  private static instance: Hexastore;
  private readonly name: string;

  private constructor(private redis: Redis, opts: HexastoreOpts) {
    this.name = opts.name;
  }

  public async get(opts: HexastoreOpts & RedisOpts): Promise<Hexastore> {
    const redis = await connect({
      hostname: opts.hostname,
      port: opts.port,
    });

    return new Hexastore(redis, opts);
  }

  // Get the Redis key for this hexastore store
  public getKey(): string {
    return `index:hex:${this.name}`;
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
      throw new Error(oneLineTrim`
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

    throw new Error(oneLineTrim`
      Invalid Hexastore filter request:
      ${JSON.stringify(filter)}
    `);
  }
}
