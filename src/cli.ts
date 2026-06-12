#!/usr/bin/env node

/**
 * redis-op - Redis data operations for migrating, exporting, and restoring keys.
 *
 * Features:
 *  - Migrate Redis keys from one instance to another
 *  - Export Redis keys to streaming NDJSON
 *  - Restore NDJSON exports to Redis
 *  - Per-type counts and summaries
 *  - Concurrency-limited workers
 *  - SCAN streaming for large DBs by default
 */

import { MultiBar, Presets } from "cli-progress";
import { once } from "node:events";
import * as fs from "node:fs";
import * as os from "node:os";
import * as process from "node:process";
import * as readline from "node:readline";
import redis from "redis";
import signale from "signale";

const { Signale } = signale;

const logger = new Signale({
  scope: "redis-op",
  types: {
    migrate: { color: "cyan", label: "migrating", badge: "🚚" },
    done: { color: "green", label: "done", badge: "✨" },
    info: { color: "blue", label: "info", badge: "ℹ️" },
    warn: { color: "yellow", label: "warn", badge: "⚠️" },
  },
});

type RedisClient = ReturnType<typeof redis.createClient>;

type Command = "migrate" | "export" | "restore";

interface BaseOptions {
  command: Command;
  concurrency: number;
}

interface MigrateOptions extends BaseOptions {
  command: "migrate";
  source: string;
  dest: string;
  keys: string;
  useScan: boolean;
}

interface ExportOptions extends BaseOptions {
  command: "export";
  source: string;
  output: string;
  keys: string;
  useScan: boolean;
}

interface RestoreOptions extends BaseOptions {
  command: "restore";
  dest: string;
  input: string;
}

type CliOptions = MigrateOptions | ExportOptions | RestoreOptions;

type ExportRecord =
  | {
      version: 1;
      key: string;
      type: "string";
      ttl: number;
      value: string | null;
    }
  | { version: 1; key: string; type: "list"; ttl: number; value: string[] }
  | {
      version: 1;
      key: string;
      type: "hash";
      ttl: number;
      value: Record<string, string>;
    }
  | { version: 1; key: string; type: "set"; ttl: number; value: string[] }
  | {
      version: 1;
      key: string;
      type: "zset";
      ttl: number;
      value: Array<{ value: string; score: number }>;
    }
  | { version: 1; key: string; type: "ReJSON-RL"; ttl: number; value: unknown };

interface OperationSummary {
  totalScanned: number;
  totalProcessed: number;
  skipped: number;
  failures: number;
  byType: Map<string, number>;
}

interface RestoreSummary {
  totalRecords: number;
  totalRestored: number;
  failures: number;
  skippedEmpty: number;
  byType: Map<string, number>;
}

class AsyncQueue<T> {
  private buffer: T[] = [];
  private resolvers: ((value: T | null) => void)[] = [];
  private closed = false;

  push(item: T) {
    if (this.closed) throw new Error("Cannot push to closed queue");
    const resolver = this.resolvers.shift();
    if (resolver) {
      resolver(item);
      return;
    }
    this.buffer.push(item);
  }

  close() {
    this.closed = true;
    while (this.resolvers.length > 0) {
      this.resolvers.shift()!(null);
    }
  }

  async shift(): Promise<T | null> {
    const buffered = this.buffer.shift();
    if (buffered !== undefined) return buffered;
    if (this.closed) return null;
    return await new Promise<T | null>((resolve) => {
      this.resolvers.push(resolve);
    });
  }
}

function printHelp(command?: Command): void {
  if (command === "migrate") {
    logger.log(`
Usage:
  redis-op migrate -s <source_url> -d <dest_url> [-k "<pattern>"] [-c <concurrency>] [--no-scan]
  redis-op -s <source_url> -d <dest_url> [-k "<pattern>"] [-c <concurrency>] [--no-scan]

Options:
  -s, --source       Source Redis connection URL (required)
  -d, --dest         Destination Redis connection URL (required)
  -k, --keys         Redis key pattern (default: "*")
  -c, --concurrency  Number of parallel workers (default: 10)
  --no-scan          Use KEYS (single call) instead of SCAN streaming
  -h, --help         Show this help message
`);
    return;
  }

  if (command === "export") {
    logger.log(`
Usage:
  redis-op export -s <source_url> -o <output_file.ndjson> [-k "<pattern>"] [-c <concurrency>] [--no-scan]

Options:
  -s, --source       Source Redis connection URL (required)
  -o, --output       Output NDJSON file path (required)
  -k, --keys         Redis key pattern (default: "*")
  -c, --concurrency  Number of parallel workers (default: 10)
  --no-scan          Use KEYS (single call) instead of SCAN streaming
  -h, --help         Show this help message
`);
    return;
  }

  if (command === "restore") {
    logger.log(`
Usage:
  redis-op restore -d <dest_url> -i <input_file.ndjson> [-c <concurrency>]

Options:
  -d, --dest         Destination Redis connection URL (required)
  -i, --input        Input NDJSON file path (required)
  -c, --concurrency  Number of parallel workers (default: 10)
  -h, --help         Show this help message
`);
    return;
  }

  logger.log(`
Usage:
  redis-op migrate -s <source_url> -d <dest_url> [-k "<pattern>"] [-c <concurrency>] [--no-scan]
  redis-op export -s <source_url> -o <output_file.ndjson> [-k "<pattern>"] [-c <concurrency>] [--no-scan]
  redis-op restore -d <dest_url> -i <input_file.ndjson> [-c <concurrency>]

Legacy alias:
  redis-migrate -s <source_url> -d <dest_url> [-k "<pattern>"] [-c <concurrency>] [--no-scan]

Commands:
  migrate   Copy keys directly from one Redis instance to another
  export    Export Redis keys to streaming NDJSON
  restore   Restore a streaming NDJSON export to Redis

Run "redis-op <command> --help" for command-specific options.
`);
}

function exitWithHelp(command?: Command): never {
  printHelp(command);
  process.exit(1);
}

function parsePositiveNumber(value: string | undefined): number {
  if (!value || Number.isNaN(Number(value)) || Number(value) <= 0) {
    logger.error("❌ Invalid value for -c/--concurrency");
    exitWithHelp();
  }
  return Number(value);
}

function readOptionValue(
  args: string[],
  index: number,
  option: string,
): string {
  const value = args[index + 1];
  if (!value) {
    logger.error(`❌ Missing value for ${option}`);
    exitWithHelp();
  }
  return value;
}

function parseArgs(argv: string[]): CliOptions {
  const args = argv.slice(2);
  const first = args[0];
  const command: Command =
    first === "migrate" || first === "export" || first === "restore"
      ? first
      : "migrate";
  const commandArgs = command === first ? args.slice(1) : args;

  if (commandArgs.includes("-h") || commandArgs.includes("--help")) {
    printHelp(command === first ? command : undefined);
    process.exit(0);
  }

  let source: string | undefined;
  let dest: string | undefined;
  let input: string | undefined;
  let output: string | undefined;
  let keys = "*";
  let concurrency = 10;
  let useScan = true;

  for (let i = 0; i < commandArgs.length; i++) {
    const arg = commandArgs[i];
    if (!arg) continue;

    switch (arg) {
      case "-s":
      case "--source":
        source = readOptionValue(commandArgs, i, arg);
        i++;
        break;
      case "-d":
      case "--dest":
        dest = readOptionValue(commandArgs, i, arg);
        i++;
        break;
      case "-i":
      case "--input":
        input = readOptionValue(commandArgs, i, arg);
        i++;
        break;
      case "-o":
      case "--output":
        output = readOptionValue(commandArgs, i, arg);
        i++;
        break;
      case "-k":
      case "--keys":
        keys = readOptionValue(commandArgs, i, arg);
        i++;
        break;
      case "-c":
      case "--concurrency":
        concurrency = parsePositiveNumber(commandArgs[++i]);
        break;
      case "--no-scan":
        useScan = false;
        break;
      default:
        logger.error(`❌ Unknown argument: ${arg}`);
        exitWithHelp(command);
    }
  }

  if (command === "migrate") {
    if (!source || !dest) {
      logger.error("❌ Both --source and --dest are required for migrate.");
      exitWithHelp(command);
    }
    return { command, source, dest, keys, concurrency, useScan };
  }

  if (command === "export") {
    if (!source || !output) {
      logger.error("❌ Both --source and --output are required for export.");
      exitWithHelp(command);
    }
    return { command, source, output, keys, concurrency, useScan };
  }

  if (!dest || !input) {
    logger.error("❌ Both --dest and --input are required for restore.");
    exitWithHelp(command);
  }

  return { command, dest, input, concurrency };
}

function createSummary(): OperationSummary {
  return {
    totalScanned: 0,
    totalProcessed: 0,
    skipped: 0,
    failures: 0,
    byType: new Map<string, number>(),
  };
}

function incrementType(summary: { byType: Map<string, number> }, type: string) {
  summary.byType.set(type, (summary.byType.get(type) || 0) + 1);
}

async function produceKeys(
  client: RedisClient,
  pattern: string,
  useScan: boolean,
  onKey: (key: string) => void | Promise<void>,
): Promise<number> {
  let scanned = 0;

  if (!useScan) {
    logger.info(
      "Using KEYS command (non-streaming). Be careful with large DBs.",
    );
    const keys = await client.keys(pattern);
    for (const key of keys) {
      scanned++;
      await onKey(key);
    }
    return scanned;
  }

  logger.info(`Starting SCAN with pattern: ${pattern}`);
  let cursor = "0";
  do {
    const result = await client.scan(cursor, {
      MATCH: pattern,
      COUNT: 1000,
    });
    cursor = result.cursor;
    for (const key of result.keys) {
      scanned++;
      await onKey(key);
    }
  } while (cursor !== "0");

  return scanned;
}

async function readRedisKey(
  client: RedisClient,
  key: string,
): Promise<ExportRecord | null> {
  const type = await client.type(key);
  const ttl = await client.ttl(key);

  if (ttl === -2 || type === "none") return null;

  switch (type) {
    case "string":
      return {
        version: 1,
        key,
        type,
        ttl,
        value: await client.get(key),
      };
    case "list":
      return {
        version: 1,
        key,
        type,
        ttl,
        value: await client.lRange(key, 0, -1),
      };
    case "hash":
      return {
        version: 1,
        key,
        type,
        ttl,
        value: await client.hGetAll(key),
      };
    case "set":
      return {
        version: 1,
        key,
        type,
        ttl,
        value: await client.sMembers(key),
      };
    case "zset":
      return {
        version: 1,
        key,
        type,
        ttl,
        value: await client.zRangeWithScores(key, 0, -1),
      };
    case "ReJSON-RL":
      return {
        version: 1,
        key,
        type,
        ttl,
        value: await client.json.get(key),
      };
    default:
      logger.warn(`Unsupported type "${type}" for key "${key}"`);
      return null;
  }
}

function isEmptyCollection(record: ExportRecord): boolean {
  if (record.type === "hash") return Object.keys(record.value).length === 0;
  if (
    record.type === "list" ||
    record.type === "set" ||
    record.type === "zset"
  ) {
    return record.value.length === 0;
  }
  return false;
}

async function restoreRedisKey(
  client: RedisClient,
  record: ExportRecord,
  opts: { overwrite: boolean },
): Promise<"restored" | "empty"> {
  if (opts.overwrite) await client.del(record.key);

  if (isEmptyCollection(record)) return "empty";

  switch (record.type) {
    case "string":
      if (record.value !== null) await client.set(record.key, record.value);
      break;
    case "list":
      await client.rPush(record.key, record.value);
      break;
    case "hash":
      await client.hSet(record.key, record.value);
      break;
    case "set":
      await client.sAdd(record.key, record.value);
      break;
    case "zset":
      await client.zAdd(record.key, record.value);
      break;
    case "ReJSON-RL":
      if (record.value !== null && record.value !== undefined) {
        await client.json.set(
          record.key,
          "$",
          record.value as Parameters<typeof client.json.set>[2],
        );
      }
      break;
  }

  if (record.ttl > 0) await client.expire(record.key, record.ttl);
  return "restored";
}

function parseExportRecord(value: unknown): ExportRecord | null {
  if (!value || typeof value !== "object") return null;
  const record = value as Record<string, unknown>;
  if (record.version !== 1) return null;
  if (typeof record.key !== "string") return null;
  if (typeof record.ttl !== "number") return null;

  switch (record.type) {
    case "string":
      if (typeof record.value !== "string" && record.value !== null)
        return null;
      return record as ExportRecord;
    case "list":
    case "set":
      if (!Array.isArray(record.value)) return null;
      if (!record.value.every((item) => typeof item === "string")) return null;
      return record as ExportRecord;
    case "hash":
      if (!record.value || typeof record.value !== "object") return null;
      if (
        !Object.values(record.value).every((item) => typeof item === "string")
      ) {
        return null;
      }
      return record as ExportRecord;
    case "zset":
      if (!Array.isArray(record.value)) return null;
      if (
        !record.value.every(
          (item) =>
            item &&
            typeof item === "object" &&
            typeof (item as { value?: unknown }).value === "string" &&
            typeof (item as { score?: unknown }).score === "number",
        )
      ) {
        return null;
      }
      return record as ExportRecord;
    case "ReJSON-RL":
      return record as ExportRecord;
    default:
      return null;
  }
}

function createProgressBars(actionName: string) {
  const multi = new MultiBar(
    {
      clearOnComplete: false,
      hideCursor: true,
      format: "{bar} | {name} | {value}/{total} | {msg}",
    },
    Presets.shades_classic,
  );

  const scannedBar = multi.create(1, 0, {
    name: "scanned",
    msg: "scanning...",
  });
  const processedBar = multi.create(1, 0, {
    name: actionName,
    msg: "",
  });

  return { multi, scannedBar, processedBar };
}

async function runKeyWorkers(
  concurrency: number,
  queue: AsyncQueue<string>,
  processKey: (key: string) => Promise<void>,
): Promise<void> {
  const workers: Promise<void>[] = [];
  for (let i = 0; i < concurrency; i++) {
    workers.push(
      (async () => {
        while (true) {
          const key = await queue.shift();
          if (key === null) break;
          await processKey(key);
        }
      })(),
    );
  }
  await Promise.all(workers);
}

function printOperationSummary(title: string, summary: OperationSummary): void {
  logger.log(os.EOL);
  logger.success(`${title} finished.`);
  logger.log("Summary:");
  logger.log(`  Total scanned : ${summary.totalScanned}`);
  logger.log(`  Total processed: ${summary.totalProcessed}`);
  logger.log(`  Skipped       : ${summary.skipped}`);
  logger.log(`  Failures      : ${summary.failures}`);
  logger.log("  By type:");
  for (const [type, count] of summary.byType) {
    logger.log(`    ${type}: ${count}`);
  }
}

async function disconnect(client: RedisClient, name: string): Promise<void> {
  try {
    await client.disconnect();
  } catch (error) {
    logger.warn(`Error disconnecting ${name} client:`, error);
  }
}

async function migrateRedis(opts: MigrateOptions): Promise<void> {
  const sourceClient = redis.createClient({ url: opts.source });
  const destinationClient = redis.createClient({ url: opts.dest });

  sourceClient.on("error", (error) =>
    logger.error("Source Redis error:", error),
  );
  destinationClient.on("error", (error) =>
    logger.error("Destination Redis error:", error),
  );

  logger.info(`Connecting to source: ${opts.source}`);
  logger.info(`Connecting to dest:   ${opts.dest}`);
  await sourceClient.connect();
  await destinationClient.connect();

  const summary = createSummary();
  const queue = new AsyncQueue<string>();
  const { multi, scannedBar, processedBar } = createProgressBars("migrated");

  const producer = (async () => {
    try {
      summary.totalScanned = await produceKeys(
        sourceClient,
        opts.keys,
        opts.useScan,
        (key) => {
          queue.push(key);
          scannedBar.increment();
          processedBar.setTotal(
            Math.max(processedBar.getTotal(), summary.totalScanned + 1),
          );
        },
      );
    } catch (error) {
      summary.failures++;
      logger.error("Producer error during SCAN/KEYS:", error);
    } finally {
      queue.close();
    }
  })();

  await Promise.all([
    producer,
    runKeyWorkers(opts.concurrency, queue, async (key) => {
      try {
        const record = await readRedisKey(sourceClient, key);
        if (!record) {
          summary.skipped++;
          return;
        }
        incrementType(summary, record.type);
        await restoreRedisKey(destinationClient, record, { overwrite: false });
        summary.totalProcessed++;
        processedBar.increment();
      } catch (error) {
        summary.failures++;
        logger.error(`Failed migrating key "${key}":`, error);
      }
    }),
  ]);

  multi.stop();
  await disconnect(sourceClient, "source");
  await disconnect(destinationClient, "destination");
  printOperationSummary("Migration", summary);
}

async function writeNdjsonRecord(
  stream: fs.WriteStream,
  record: ExportRecord,
): Promise<void> {
  if (!stream.write(`${JSON.stringify(record)}\n`)) {
    await once(stream, "drain");
  }
}

async function exportRedis(opts: ExportOptions): Promise<void> {
  const stream = fs.createWriteStream(opts.output, { encoding: "utf8" });
  await new Promise<void>((resolve, reject) => {
    stream.once("open", () => resolve());
    stream.once("error", reject);
  });

  const sourceClient = redis.createClient({ url: opts.source });
  sourceClient.on("error", (error) =>
    logger.error("Source Redis error:", error),
  );

  logger.info(`Connecting to source: ${opts.source}`);
  logger.info(`Writing export:       ${opts.output}`);
  await sourceClient.connect();

  const summary = createSummary();
  const queue = new AsyncQueue<string>();
  const { multi, scannedBar, processedBar } = createProgressBars("exported");

  const producer = (async () => {
    try {
      summary.totalScanned = await produceKeys(
        sourceClient,
        opts.keys,
        opts.useScan,
        (key) => {
          queue.push(key);
          scannedBar.increment();
          processedBar.setTotal(
            Math.max(processedBar.getTotal(), summary.totalScanned + 1),
          );
        },
      );
    } catch (error) {
      summary.failures++;
      logger.error("Producer error during SCAN/KEYS:", error);
    } finally {
      queue.close();
    }
  })();

  let writeFailed = false;
  stream.once("error", (error) => {
    writeFailed = true;
    logger.error("Export write stream failed:", error);
  });

  await Promise.all([
    producer,
    runKeyWorkers(opts.concurrency, queue, async (key) => {
      if (writeFailed) return;
      try {
        const record = await readRedisKey(sourceClient, key);
        if (!record) {
          summary.skipped++;
          return;
        }
        incrementType(summary, record.type);
        await writeNdjsonRecord(stream, record);
        summary.totalProcessed++;
        processedBar.increment();
      } catch (error) {
        summary.failures++;
        logger.error(`Failed exporting key "${key}":`, error);
      }
    }),
  ]);

  await new Promise<void>((resolve, reject) => {
    stream.end((error?: Error | null) => {
      if (error) reject(error);
      else resolve();
    });
  });

  multi.stop();
  await disconnect(sourceClient, "source");
  printOperationSummary("Export", summary);

  if (writeFailed) {
    throw new Error("Export failed because the output stream errored.");
  }
}

async function restoreRedis(opts: RestoreOptions): Promise<void> {
  await fs.promises.access(opts.input, fs.constants.R_OK);

  const destinationClient = redis.createClient({ url: opts.dest });
  destinationClient.on("error", (error) =>
    logger.error("Destination Redis error:", error),
  );

  logger.info(`Connecting to dest: ${opts.dest}`);
  logger.info(`Reading export:     ${opts.input}`);
  await destinationClient.connect();

  const summary: RestoreSummary = {
    totalRecords: 0,
    totalRestored: 0,
    failures: 0,
    skippedEmpty: 0,
    byType: new Map<string, number>(),
  };
  const queue = new AsyncQueue<string>();

  const workers = runKeyWorkers(opts.concurrency, queue, async (line) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    summary.totalRecords++;

    try {
      const record = parseExportRecord(JSON.parse(trimmed));
      if (!record) {
        summary.failures++;
        logger.error(`Invalid export record on line ${summary.totalRecords}`);
        return;
      }

      incrementType(summary, record.type);
      const result = await restoreRedisKey(destinationClient, record, {
        overwrite: true,
      });
      if (result === "empty") summary.skippedEmpty++;
      else summary.totalRestored++;
    } catch (error) {
      summary.failures++;
      logger.error(`Failed restoring line ${summary.totalRecords}:`, error);
    }
  });

  const reader = readline.createInterface({
    input: fs.createReadStream(opts.input, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of reader) {
    queue.push(line);
  }
  queue.close();
  await workers;

  await disconnect(destinationClient, "destination");

  logger.log(os.EOL);
  logger.success("Restore finished.");
  logger.log("Summary:");
  logger.log(`  Total records : ${summary.totalRecords}`);
  logger.log(`  Total restored: ${summary.totalRestored}`);
  logger.log(`  Empty skipped : ${summary.skippedEmpty}`);
  logger.log(`  Failures      : ${summary.failures}`);
  logger.log("  By type:");
  for (const [type, count] of summary.byType) {
    logger.log(`    ${type}: ${count}`);
  }

  if (summary.failures > 0) {
    throw new Error("Restore completed with failures.");
  }
}

function logStart(opts: CliOptions): void {
  if (opts.command === "migrate") {
    logger.info("Starting Redis migration");
    logger.log(`  FROM: ${opts.source}`);
    logger.log(`  TO:   ${opts.dest}`);
    logger.log(`  PATTERN: ${opts.keys}`);
    logger.log(`  CONCURRENCY: ${opts.concurrency}`);
    logger.log(
      `  MODE: ${opts.useScan ? "SCAN (streaming)" : "KEYS (one-shot)"}`,
    );
    logger.log();
    return;
  }

  if (opts.command === "export") {
    logger.info("Starting Redis export");
    logger.log(`  FROM: ${opts.source}`);
    logger.log(`  OUTPUT: ${opts.output}`);
    logger.log(`  PATTERN: ${opts.keys}`);
    logger.log(`  CONCURRENCY: ${opts.concurrency}`);
    logger.log(
      `  MODE: ${opts.useScan ? "SCAN (streaming)" : "KEYS (one-shot)"}`,
    );
    logger.log();
    return;
  }

  logger.info("Starting Redis restore");
  logger.log(`  TO: ${opts.dest}`);
  logger.log(`  INPUT: ${opts.input}`);
  logger.log(`  CONCURRENCY: ${opts.concurrency}`);
  logger.log();
}

(async function main() {
  const opts = parseArgs(process.argv);
  logStart(opts);

  try {
    if (opts.command === "migrate") await migrateRedis(opts);
    else if (opts.command === "export") await exportRedis(opts);
    else await restoreRedis(opts);
    process.exit(0);
  } catch (error) {
    logger.fatal(`${opts.command} failed:`, error);
    process.exit(1);
  }
})();
