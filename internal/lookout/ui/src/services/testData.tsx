import { DurationStats, JobSet } from "./JobService";

function generateId(length: number): string {
  var characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  var charactersLength = characters.length;
  return characters.charAt(Math.floor(Math.random() * charactersLength)).repeat(length);
}

// Both inclusive
function randInt(low: number, high: number): number {
  low = Math.ceil(low);
  high = Math.floor(high);
  return Math.floor(Math.random() * (high - low + 1)) + low;
}

function makeDurationStats(): DurationStats {
  return {
    shortest: randInt(1, 5),
    q1: randInt(5, 10),
    median: randInt(10, 15),
    average: randInt(10, 15),
    q3: randInt(15, 20),
    longest: randInt(20, 25),
  }
}

export function makeTestJobSets(nJobSets: number, jobSetLength: number): JobSet[] {
  const jobSets: JobSet[] = []

  for (let i = 0; i < nJobSets; i++) {
    jobSets.push({
      jobSet: generateId(jobSetLength),
      queue: "test",
      jobsQueued: randInt(1, 50),
      jobsPending: randInt(1, 50),
      jobsRunning: randInt(1, 50),
      jobsSucceeded: randInt(1, 50),
      jobsFailed: randInt(1, 50),
      runningStats: makeDurationStats(),
      queuedStats: makeDurationStats(),
    })
  }

  return jobSets
}
