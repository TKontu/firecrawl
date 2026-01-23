import { RateLimiterMode } from "../types";
import { getRedisConnection } from "../services/queue-service";
import { getACUCTeam } from "../controllers/auth";
import { getCrawl, StoredCrawl } from "./crawl-redis";
import { logger } from "./logger";
import { abTestJob } from "../services/ab-test";
import { scrapeQueue, type NuQJob } from "../services/worker/nuq";

const constructKey = (team_id: string) => "concurrency-limiter:" + team_id;
const constructQueueKey = (team_id: string) =>
  "concurrency-limit-queue:" + team_id;

const constructCrawlKey = (crawl_id: string) =>
  "crawl-concurrency-limiter:" + crawl_id;

export async function cleanOldConcurrencyLimitEntries(
  team_id: string,
  now: number = Date.now(),
) {
  await getRedisConnection().zremrangebyscore(
    constructKey(team_id),
    -Infinity,
    now,
  );
}

export async function getConcurrencyLimitActiveJobsCount(
  team_id: string,
): Promise<number> {
  return await getRedisConnection().zcount(
    constructKey(team_id),
    Date.now(),
    Infinity,
  );
}

export async function getConcurrencyLimitActiveJobs(
  team_id: string,
  now: number = Date.now(),
): Promise<string[]> {
  return await getRedisConnection().zrangebyscore(
    constructKey(team_id),
    now,
    Infinity,
  );
}

export async function pushConcurrencyLimitActiveJob(
  team_id: string,
  id: string,
  timeout: number,
  now: number = Date.now(),
) {
  await getRedisConnection().zadd(constructKey(team_id), now + timeout, id);
}

export async function removeConcurrencyLimitActiveJob(
  team_id: string,
  id: string,
) {
  await getRedisConnection().zrem(constructKey(team_id), id);
}

type ConcurrencyLimitedJob = {
  id: string;
  data: any;
  priority: number;
  listenable: boolean;
};

export async function cleanOldConcurrencyLimitedJobs(
  team_id: string,
  now: number = Date.now(),
) {
  await getRedisConnection().zremrangebyscore(
    constructQueueKey(team_id),
    -Infinity,
    now,
  );
}

export async function pushConcurrencyLimitedJob(
  team_id: string,
  job: ConcurrencyLimitedJob,
  timeout: number,
  now: number = Date.now(),
) {
  const queueKey = constructQueueKey(team_id);
  await getRedisConnection().zadd(queueKey, now + timeout, JSON.stringify(job));
  await getRedisConnection().sadd("concurrency-limit-queues", queueKey);
}

export async function getConcurrencyLimitedJobs(team_id: string) {
  return new Set(
    (await getRedisConnection().zrange(constructQueueKey(team_id), 0, -1)).map(
      x => JSON.parse(x).id,
    ),
  );
}

export async function getConcurrencyQueueJobsCount(
  team_id: string,
): Promise<number> {
  return await getRedisConnection().zcount(
    constructQueueKey(team_id),
    Date.now(),
    Infinity,
  );
}

async function cleanOldCrawlConcurrencyLimitEntries(
  crawl_id: string,
  now: number = Date.now(),
) {
  await getRedisConnection().zremrangebyscore(
    constructCrawlKey(crawl_id),
    -Infinity,
    now,
  );
}

export async function getCrawlConcurrencyLimitActiveJobs(
  crawl_id: string,
  now: number = Date.now(),
): Promise<string[]> {
  return await getRedisConnection().zrangebyscore(
    constructCrawlKey(crawl_id),
    now,
    Infinity,
  );
}

export async function pushCrawlConcurrencyLimitActiveJob(
  crawl_id: string,
  id: string,
  timeout: number,
  now: number = Date.now(),
) {
  await getRedisConnection().zadd(
    constructCrawlKey(crawl_id),
    now + timeout,
    id,
  );
}

async function removeCrawlConcurrencyLimitActiveJob(
  crawl_id: string,
  id: string,
) {
  await getRedisConnection().zrem(constructCrawlKey(crawl_id), id);
}

/**
 * Grabs the next job from the team's concurrency limit queue. Handles crawl concurrency limits.
 *
 * This function may only be called once the outer code has verified that the team has not reached its concurrency limit.
 *
 * @param teamId
 * @returns A job that can be run, or null if there are no more jobs to run.
 */
async function getNextConcurrentJob(
  teamId: string,
  i = 0,
): Promise<{
  job: ConcurrencyLimitedJob;
  timeout: number;
} | null> {
  let finalJobs: {
    job: ConcurrencyLimitedJob;
    _member: string;
    timeout: number;
  }[] = [];

  const crawlCache = new Map<string, StoredCrawl>();
  let cursor: string = "0";

  do {
    const scanResult = await getRedisConnection().zscan(
      constructQueueKey(teamId),
      cursor,
      "COUNT",
      20,
    );
    cursor = scanResult[0];
    const results = scanResult[1];

    for (let i = 0; i < results.length; i += 2) {
      const res = {
        job: JSON.parse(results[i]),
        _member: results[i],
        timeout:
          results[i + 1] === "inf" ? Infinity : parseFloat(results[i + 1]),
      };

      // If the job is associated with a crawl ID, we need to check if the crawl has a max concurrency limit
      if (res.job.data.crawl_id) {
        const sc =
          crawlCache.get(res.job.data.crawl_id) ??
          (await getCrawl(res.job.data.crawl_id));
        if (sc !== null) {
          crawlCache.set(res.job.data.crawl_id, sc);
        }

        const maxCrawlConcurrency =
          sc === null
            ? null
            : typeof sc.crawlerOptions?.delay === "number" &&
                sc.crawlerOptions.delay > 0
              ? 1
              : (sc.maxConcurrency ?? null);

        if (maxCrawlConcurrency !== null) {
          // If the crawl has a max concurrency limit, we need to check if the crawl has reached the limit
          const currentActiveConcurrency = (
            await getCrawlConcurrencyLimitActiveJobs(res.job.data.crawl_id)
          ).length;
          if (currentActiveConcurrency < maxCrawlConcurrency) {
            // If we're under the max concurrency limit, we can run the job
            finalJobs.push(res);
          }
        } else {
          // If the crawl has no max concurrency limit, we can run the job
          finalJobs.push(res);
        }
      } else {
        // If the job is not associated with a crawl ID, we can run the job
        finalJobs.push(res);
      }
    }
  } while (finalJobs.length === 0 && cursor !== "0");

  let finalJob: (typeof finalJobs)[number] | null = null;
  if (finalJobs.length > 0) {
    for (const job of finalJobs) {
      const res = await getRedisConnection().zrem(
        constructQueueKey(teamId),
        job._member,
      );
      if (res !== 0) {
        finalJob = job;
        break;
      }
    }

    if (finalJob === null) {
      // It's normal for this to happen, but if it happens too many times, we should log a warning
      if (i > 100) {
        logger.error(
          "Failed to remove job from concurrency limit queue, hard bailing",
          {
            teamId,
            jobIds: finalJobs.map(x => x.job.id),
            zeroDataRetention: finalJobs.some(
              x => x.job.data?.zeroDataRetention,
            ),
            i,
          },
        );
        return null;
      } else if (i > 15) {
        logger.warn("Failed to remove job from concurrency limit queue", {
          teamId,
          jobIds: finalJobs.map(x => x.job.id),
          zeroDataRetention: finalJobs.some(x => x.job.data?.zeroDataRetention),
          i,
        });
      }

      return await new Promise((resolve, reject) =>
        setTimeout(
          () => {
            getNextConcurrentJob(teamId, i + 1)
              .then(resolve)
              .catch(reject);
          },
          Math.floor(Math.random() * 300),
        ),
      ); // Stagger the workers off to break up the clump that causes the race condition
    } else {
      logger.debug("Removed job from concurrency limit queue", {
        teamId,
        jobId: finalJob.job.id,
        zeroDataRetention: finalJob.job.data?.zeroDataRetention,
        i,
      });
    }
  }

  return finalJob;
}

/**
 * Called when a job associated with a concurrency queue is done.
 *
 * @param job The BullMQ job that is done.
 */
export async function concurrentJobDone(job: NuQJob<any>) {
  const entryTime = Date.now();
  const jobLogger = logger.child({
    module: "concurrency-limit",
    method: "concurrentJobDone",
    scrapeId: job.id,
    crawlId: job.data?.crawl_id,
    teamId: job.data?.team_id,
  });

  jobLogger.debug("concurrentJobDone ENTRY", {
    hasJobId: !!job.id,
    hasTeamId: !!job.data?.team_id,
    hasCrawlId: !!job.data?.crawl_id,
  });

  if (job.id && job.data && job.data.team_id) {
    let stepStart = Date.now();
    await removeConcurrencyLimitActiveJob(job.data.team_id, job.id);
    jobLogger.debug("removeConcurrencyLimitActiveJob completed", {
      durationMs: Date.now() - stepStart,
    });

    stepStart = Date.now();
    await cleanOldConcurrencyLimitEntries(job.data.team_id);
    jobLogger.debug("cleanOldConcurrencyLimitEntries completed", {
      durationMs: Date.now() - stepStart,
    });

    if (job.data.crawl_id) {
      stepStart = Date.now();
      await removeCrawlConcurrencyLimitActiveJob(job.data.crawl_id, job.id);
      jobLogger.debug("removeCrawlConcurrencyLimitActiveJob completed", {
        durationMs: Date.now() - stepStart,
      });

      stepStart = Date.now();
      await cleanOldCrawlConcurrencyLimitEntries(job.data.crawl_id);
      jobLogger.debug("cleanOldCrawlConcurrencyLimitEntries completed", {
        durationMs: Date.now() - stepStart,
      });
    }

    let i = 0;
    for (; i < 10; i++) {
      const loopStart = Date.now();
      jobLogger.debug("Promotion loop iteration start", { iteration: i });

      stepStart = Date.now();
      const maxTeamConcurrency =
        (
          await getACUCTeam(
            job.data.team_id,
            false,
            true,
            job.data.is_extract
              ? RateLimiterMode.Extract
              : RateLimiterMode.Crawl,
          )
        )?.concurrency ?? 2;
      jobLogger.debug("getACUCTeam completed", {
        durationMs: Date.now() - stepStart,
        maxTeamConcurrency,
        iteration: i,
      });

      stepStart = Date.now();
      const currentActiveConcurrency = (
        await getConcurrencyLimitActiveJobs(job.data.team_id)
      ).length;
      jobLogger.debug("getConcurrencyLimitActiveJobs completed", {
        durationMs: Date.now() - stepStart,
        currentActiveConcurrency,
        iteration: i,
      });

      if (currentActiveConcurrency < maxTeamConcurrency) {
        stepStart = Date.now();
        const nextJob = await getNextConcurrentJob(job.data.team_id);
        jobLogger.debug("getNextConcurrentJob completed", {
          durationMs: Date.now() - stepStart,
          foundJob: nextJob !== null,
          nextJobId: nextJob?.job?.id,
          iteration: i,
        });

        if (nextJob !== null) {
          stepStart = Date.now();
          await pushConcurrencyLimitActiveJob(
            job.data.team_id,
            nextJob.job.id,
            60 * 1000,
          );
          jobLogger.debug("pushConcurrencyLimitActiveJob completed", {
            durationMs: Date.now() - stepStart,
            iteration: i,
          });

          if (nextJob.job.data.crawl_id) {
            stepStart = Date.now();
            await pushCrawlConcurrencyLimitActiveJob(
              nextJob.job.data.crawl_id,
              nextJob.job.id,
              60 * 1000,
            );
            jobLogger.debug("pushCrawlConcurrencyLimitActiveJob completed", {
              durationMs: Date.now() - stepStart,
              iteration: i,
            });

            stepStart = Date.now();
            const sc = await getCrawl(nextJob.job.data.crawl_id);
            jobLogger.debug("getCrawl for delay check completed", {
              durationMs: Date.now() - stepStart,
              hasDelay: sc !== null && typeof sc.crawlerOptions?.delay === "number",
              delayValue: sc?.crawlerOptions?.delay,
              iteration: i,
            });

            if (sc !== null && typeof sc.crawlerOptions?.delay === "number") {
              const delayMs = sc.crawlerOptions.delay * 1000;
              jobLogger.info("Starting crawler delay sleep", {
                delayMs,
                delaySeconds: sc.crawlerOptions.delay,
                iteration: i,
              });
              await new Promise(resolve =>
                setTimeout(resolve, delayMs),
              );
              jobLogger.info("Crawler delay sleep completed", {
                delayMs,
                iteration: i,
              });
            }
          }

          abTestJob(nextJob.job.data);

          stepStart = Date.now();
          const promotedSuccessfully =
            (await scrapeQueue.promoteJobFromBacklogOrAdd(
              nextJob.job.id,
              nextJob.job.data,
              {
                priority: nextJob.job.priority,
                listenable: nextJob.job.listenable,
                ownerId: nextJob.job.data.team_id ?? undefined,
                groupId: nextJob.job.data.crawl_id ?? undefined,
              },
            )) !== null;
          jobLogger.debug("promoteJobFromBacklogOrAdd completed", {
            durationMs: Date.now() - stepStart,
            promotedSuccessfully,
            iteration: i,
          });

          if (promotedSuccessfully) {
            logger.debug("Successfully promoted concurrent queued job", {
              teamId: job.data.team_id,
              jobId: nextJob.job.id,
              zeroDataRetention: nextJob.job.data?.zeroDataRetention,
            });
            jobLogger.debug("Promotion loop completed - job promoted", {
              loopDurationMs: Date.now() - loopStart,
              totalIterations: i + 1,
            });
            break;
          } else {
            logger.warn(
              "Was unable to promote concurrent queued job as it already exists in the database",
              {
                teamId: job.data.team_id,
                jobId: nextJob.job.id,
                zeroDataRetention: nextJob.job.data?.zeroDataRetention,
              },
            );
          }
        } else {
          jobLogger.debug("Promotion loop completed - no next job", {
            loopDurationMs: Date.now() - loopStart,
            totalIterations: i + 1,
          });
          break;
        }
      } else {
        jobLogger.debug("Promotion loop completed - at concurrency limit", {
          loopDurationMs: Date.now() - loopStart,
          currentActiveConcurrency,
          maxTeamConcurrency,
          totalIterations: i + 1,
        });
        break;
      }
    }

    if (i === 10) {
      logger.warn(
        "Failed to promote a concurrent job after 10 iterations, bailing!",
        {
          teamId: job.data.team_id,
        },
      );
    }
  }

  const totalDuration = Date.now() - entryTime;
  jobLogger.info("concurrentJobDone EXIT", {
    totalDurationMs: totalDuration,
    totalDurationSeconds: Math.round(totalDuration / 1000),
  });
}
