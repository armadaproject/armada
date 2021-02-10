import { DurationStats, JobSet } from "./JobService";

function makeId(length: number): string {
  let result = '';
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
}

function randInt(low: number, high: number): number {
  return low + Math.floor(Math.random() * (high - low + 1));
}

export function makeTestJobSets(nJobSets: number): JobSet[] {
  const idLen = 10

  const result: JobSet[] = []

  for (let i = 0; i < nJobSets; i++) {
    result.push({
      jobSet: makeId(idLen),
      queue: "test",
      jobsQueued: randInt(10, 20),
      jobsPending: randInt(10, 20),
      jobsRunning: randInt(10, 20),
      jobsSucceeded: randInt(10, 20),
      jobsFailed: randInt(10, 20),
      runningStats: makeRandomStats(),
      queuedStats: makeRandomStats(),
    })
  }

  return result
}

function makeRandomStats(): DurationStats {
  return {
    shortest: randInt(1, 5),
    longest: randInt(25, 30),
    average: randInt(10, 20),
    median: randInt(10, 20),
    q1: randInt(6, 9),
    q3: randInt(21, 24),
  }
}
