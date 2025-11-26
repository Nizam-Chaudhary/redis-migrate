#!/usr/bin/env node

/**
 * redis-migrate - Migrate Redis keys from a source Redis instance to a destination instance.
 *
 * Features:
 *  - CLI with options
 *  - Colorful logging (Signale)
 *  - Progress bars (cli-progress)
 *  - Per-type counts and summary
 *  - Concurrency-limited parallel migration
 *  - SCAN streaming for large DBs (default)
 *
 * Usage:
 *  ts-node redis-migrate.ts -s redis://src:6379 -d redis://dst:6379 -k "user:*" -c 20
 *  OR (after compile) ./redis-migrate -s ... -d ... -k "prefix:*"
 */

import { MultiBar, Presets } from "cli-progress";
import * as os from "node:os";
import * as process from "node:process";
import redis from "redis";
import { Signale } from "signale";

const logger = new Signale({
  scope: "redis-migrate",
  types: {
    migrate: { color: "cyan", label: "migrating", badge: "🚚" },
    done: { color: "green", label: "done", badge: "✨" },
    info: { color: "blue", label: "info", badge: "ℹ️" },
    warn: { color: "yellow", label: "warn", badge: "⚠️" },
  },
});

// ---------- Types ----------
interface CliOptions {
  source: string;
  dest: string;
  keys: string; // glob pattern
  concurrency: number;
  useScan: boolean;
}

interface Summmary {
  totalScanned: number;
  totalMigrated: number;
  failures: number;
  byType: Map<string, number>;
}

// ---------- Helpers ----------
function printHelp(): void {
  logger.log(`
Usage:
  redis-migrate -s <source_url> -d <dest_url> [-k "<pattern>"] [-c <concurrency>] [--no-scan]

Options:
  -s, --source     Source Redis connection URL (required)
  -d, --dest       Destination Redis connection URL (required)
  -k, --keys       Redis key pattern (default: "*")
  -c, --concurrency Number of parallel workers (default: 10)
  --no-scan        Use KEYS (single call) instead of SCAN streaming
  -h, --help       Show this help message

Examples:
  redis-migrate -s redis://localhost:6379 -d redis://localhost:6380
  redis-migrate -s redis://a -d redis://b -k "session:*" -c 20
`);
}

function exitWithHelp(): never {
  printHelp();
  process.exit(1);
}

function parseArgs(argv: string[]): CliOptions {
  const args = argv.slice(2);
  let source: string | undefined;
  let dest: string | undefined;
  let keys = "*";
  let concurrency = 10;
  let useScan = true;

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (!a) continue;
    switch (a) {
      case "-s":
      case "--source": {
        const val = args[++i];
        if (!val) {
          logger.error("❌ Missing value for -s/--source");
          exitWithHelp();
        }
        source = val;
        break;
      }
      case "-d":
      case "--dest": {
        const val = args[++i];
        if (!val) {
          logger.error("❌ Missing value for -d/--dest");
          exitWithHelp();
        }
        dest = val;
        break;
      }
      case "-k":
      case "--keys": {
        const val = args[++i];
        if (!val) {
          logger.error("❌ Missing value for -k/--keys");
          exitWithHelp();
        }
        keys = val;
        break;
      }
      case "-c":
      case "--concurrency": {
        const val = args[++i];
        if (!val || Number.isNaN(Number(val)) || Number(val) <= 0) {
          logger.error("❌ Invalid value for -c/--concurrency");
          exitWithHelp();
        }
        concurrency = Number(val);
        break;
      }
      case "--no-scan":
        useScan = false;
        break;
      case "-h":
      case "--help":
        printHelp();
        process.exit(0);
        break;
      default:
        logger.error(`❌ Unknown argument: ${a}`);
        exitWithHelp();
    }
  }

  if (!source || !dest) {
    logger.error("❌ Both --source and --dest are required.");
    exitWithHelp();
  }

  return { source, dest, keys, concurrency, useScan };
}

// ---------- AsyncQueue for Producer/Consumer ----------
class AsyncQueue<T> {
  private buffer: T[] = [];
  private resolvers: ((value: T | null) => void)[] = [];
  private closed = false;

  push(item: T) {
    if (this.closed) throw new Error("Cannot push to closed queue");
    if (this.resolvers.length > 0) {
      const r = this.resolvers.shift()!;
      r(item);
    } else {
      this.buffer.push(item);
    }
  }

  close() {
    this.closed = true;
    // resolve any waiters with null to signal end
    while (this.resolvers.length > 0) {
      const r = this.resolvers.shift()!;
      r(null);
    }
  }

  async shift(): Promise<T | null> {
    if (this.buffer.length > 0) {
      return this.buffer.shift()!;
    }
    if (this.closed) return null;
    return await new Promise<T | null>((resolve) => {
      this.resolvers.push(resolve);
    });
  }
}

// ---------- Redis Migration Logic ----------
async function migrateRedis(opts: CliOptions): Promise<void> {
  const sourceClient = redis.createClient({ url: opts.source });
  const destinationClient = redis.createClient({ url: opts.dest });

  sourceClient.on("error", (e) => logger.error("Source Redis error:", e));
  destinationClient.on("error", (e) =>
    logger.error("Destination Redis error:", e),
  );

  logger.info(`Connecting to source: ${opts.source}`);
  logger.info(`Connecting to dest:   ${opts.dest}`);

  await sourceClient.connect();
  await destinationClient.connect();

  const summary: Summmary = {
    totalScanned: 0,
    totalMigrated: 0,
    failures: 0,
    byType: new Map<string, number>(),
  };

  // progress bars
  const multi = new MultiBar(
    {
      clearOnComplete: false,
      hideCursor: true,
      format: "{bar} | {name} | {value}/{total} | {msg}",
    },
    Presets.shades_classic,
  );

  // If using KEYS (no scan), we will know total ahead of time.
  let totalKeysKnown = 0;
  let keysList: string[] | undefined;
  if (!opts.useScan) {
    logger.info(
      "Using KEYS command (non-streaming). Be careful with large DBs.",
    );
    keysList = await sourceClient.keys(opts.keys);
    totalKeysKnown = keysList.length;
    logger.info(`Found ${totalKeysKnown} keys matching pattern.`);
  }

  // bars
  const scannedBar = multi.create(100, 0, {
    name: "scanned",
    msg: "scanning...",
  });
  const migratedBar = multi.create(totalKeysKnown || 0, 0, {
    name: "migrated",
    msg: "",
  });

  // If total unknown (SCAN), set total to a large placeholder so the bar still renders;
  // we'll update the "total" via `setTotal` as we learn more if necessary.
  if (!opts.useScan) {
    scannedBar.setTotal(totalKeysKnown);
  } else {
    scannedBar.setTotal(1); // start with 1 so bar is visible (we will increment)
    migratedBar.setTotal(1);
  }

  // queue and workers
  const queue = new AsyncQueue<string>();

  // Producer: either push all keys via KEYS or SCAN
  const producer = (async () => {
    try {
      if (!opts.useScan && keysList) {
        for (const k of keysList) {
          queue.push(k);
          summary.totalScanned++;
          scannedBar.increment();
        }
        queue.close();
        return;
      }

      // Use SCAN streaming
      logger.info(`Starting SCAN with pattern: ${opts.keys}`);
      let cursor = "0";
      do {
        // scan returns [newCursor, keys[]] when using client.scan
        const res = await sourceClient.scan(cursor, {
          MATCH: opts.keys,
          COUNT: 1000,
        });
        const nextCursor = res.cursor as string;
        const foundKeys = res.keys as string[];
        for (const k of foundKeys) {
          queue.push(k);
          summary.totalScanned++;
          // update bars
          scannedBar.increment();
          // if migrated bar total is 1 placeholder, increase it slowly to keep it playing
          if (!opts.useScan) {
            // no-op
          } else {
            // ensure progress bar total is at least scanned count
            try {
              migratedBar.setTotal(
                Math.max(migratedBar.getTotal(), summary.totalScanned),
              );
            } catch {
              // some cli-progress versions throw if setTotal smaller than current value etc.
            }
          }
        }
        cursor = nextCursor;
      } while (cursor !== "0");
      queue.close();
    } catch (err) {
      logger.error("Producer error during SCAN/KEYS:", err);
      queue.close();
    }
  })();

  // worker function
  async function processKey(key: string) {
    try {
      const type = await sourceClient.type(key);
      // increment per-type scanned
      summary.byType.set(type, (summary.byType.get(type) || 0) + 1);

      const ttl = await sourceClient.ttl(key); // seconds, -1 = persist, -2 = not found

      switch (type) {
        case "string": {
          const value = await sourceClient.get(key);
          // Properly call set with/without EX
          if (ttl > 0) {
            await destinationClient.set(key, value as string, { EX: ttl });
          } else {
            await destinationClient.set(key, value as string);
          }
          break;
        }

        case "list": {
          const list = await sourceClient.lRange(key, 0, -1);
          if (list && list.length > 0) {
            await destinationClient.rPush(key, list);
          } else {
            // ensure empty list is created? skip
          }
          if (ttl > 0) await destinationClient.expire(key, ttl);
          break;
        }

        case "hash": {
          const hash = await sourceClient.hGetAll(key);
          // hSet accepts object mapping
          if (hash && Object.keys(hash).length > 0) {
            await destinationClient.hSet(key, hash);
          }
          if (ttl > 0) await destinationClient.expire(key, ttl);
          break;
        }

        case "set": {
          const members = await sourceClient.sMembers(key);
          if (members && members.length > 0) {
            await destinationClient.sAdd(key, members);
          }
          if (ttl > 0) await destinationClient.expire(key, ttl);
          break;
        }

        case "zset": {
          // zRangeWithScores returns array of { value, score }
          const z = await sourceClient.zRangeWithScores(key, 0, -1);
          if (z && z.length > 0) {
            // map to { score, value }
            const zaddArgs = z.map((e: { value: string; score: number }) => ({
              score: e.score,
              value: e.value,
            }));
            // node-redis zAdd expects either { score, value } | Array
            await destinationClient.zAdd(key, zaddArgs);
          }
          if (ttl > 0) await destinationClient.expire(key, ttl);
          break;
        }

        case "ReJSON-RL": {
          // RedisJSON (ReJSON) module value
          const json = await sourceClient.json.get(key);

          if (json !== null && json !== undefined) {
            // Write entire document at root path "$"
            await destinationClient.json.set(key, "$", json);
          }

          if (ttl > 0) await destinationClient.expire(key, ttl);

          break;
        }

        default:
          logger.warn(`Unsupported type "${type}" for key "${key}"`);
      }

      summary.totalMigrated++;
      migratedBar.increment();
    } catch (err) {
      summary.failures++;
      logger.error(`Failed migrating key "${key}":`, err);
    }
  }

  // start worker pool
  const workers: Promise<void>[] = [];
  for (let i = 0; i < opts.concurrency; i++) {
    const w = (async () => {
      while (true) {
        const k = await queue.shift();
        if (k === null) break; // queue closed and drained
        await processKey(k);
      }
    })();
    workers.push(w);
  }

  // wait for producer and workers
  await Promise.all([producer, ...workers]);

  // stop progress bars
  multi.stop();

  // disconnect
  try {
    await sourceClient.disconnect();
  } catch (e) {
    logger.warn("Error disconnecting source client:", e);
  }
  try {
    await destinationClient.disconnect();
  } catch (e) {
    logger.warn("Error disconnecting destination client:", e);
  }

  // Print summary
  logger.log(os.EOL);
  logger.success(`Migration finished.`);
  logger.log("Summary:");
  logger.log(`  Total scanned : ${summary.totalScanned}`);
  logger.log(`  Total migrated: ${summary.totalMigrated}`);
  logger.log(`  Failures      : ${summary.failures}`);
  logger.log("  By type:");
  for (const [t, n] of summary.byType) {
    logger.log(`    ${t}: ${n}`);
  }
}

// ---------- Main ----------
(async function main() {
  const opts = parseArgs(process.argv);

  logger.info(`Starting Redis migration`);
  logger.log(`  FROM: ${opts.source}`);
  logger.log(`  TO:   ${opts.dest}`);
  logger.log(`  PATTERN: ${opts.keys}`);
  logger.log(`  CONCURRENCY: ${opts.concurrency}`);
  logger.log(
    `  MODE: ${opts.useScan ? "SCAN (streaming)" : "KEYS (one-shot)"}`,
  );
  logger.log();

  try {
    await migrateRedis(opts);
    process.exit(0);
  } catch (err) {
    logger.fatal("Migration failed:", err);
    process.exit(1);
  }
})();
