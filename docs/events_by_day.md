# Proposal For Separate Event Streams Per Day

## Motivation
At present, all api events are stored in one Redis Event Stream per jobset, with the stream set to expire after n days of inactivity.  This presents the following issues:
* If a user submits jobs to a jobset such that at least one event is generated more frequently than once every n days, the eventstream will never be cleaned up.  This can lead to memory growth over time.
* Memory usage of these streams can be very large due to both the number of messages and the verbosity of each message.  This can cause issues as Redis requires all data to be held in memory.

To address these issues we suggest splitting each jobset into a logical stream per day.  This should enable old messages to be cleaned up, even if the jobset itself lasts indefinitely. It should also enable old, but still readbale, streams to be compressed which should further save memory.

## Proposed Mechanism for Splitting Streams.

### Write-Side

* When an event message is received from Pulsar, the Pulsar message timestamp will be used to determine the (UTC) day to which the message pertains
* The event message will then be appended to the redis stream under the key `<queue>_<jobset>_<day>` where `<day>` is the UTC day determined in the above step in hh_mm_ss format
* The `<queue>_<jobset>_<day>` key that was appended to in the step above is then added to a Redis set stored at `<queue>_<jobset>_streams`
* The result of the above should be that the messages for a a given jobset should be stored in one key per day. 
The  sets stored under `<queue>_<jobset>_streams` can be used to determine which streams are available.  Because of the way we process pulsar messages, we can guarentee that 
if a subsequent stream exits, any previous streams will never be appended to.

### Read-Side
* When a request for events is received, we first try to retrieve the set of streams at `<queue>_<jobset>_streams`.
* If nothing exists under that key, we continue to poll until something becomes available.
* Once `<queue>_<jobset>_<day>` is available, we first order the keys lexiographically and then call `XRevRangeN` to retrieve the latest MessageId for each stream.
* We now identify the first stream that we need to read from.  In the common case where the user wants to read fromt eh start of the stream, this will simply 
be the first lexiographically.  In the case where the user has provided an offset, we wil need to use the results of `XRevRangeN` determined in the previous step.
* We continue to read the initial stream until we reach the value determined by `XRevRangeN`.  If the set we retrieved in step 1 indicates that there are more streams
available then we can proceed to the next stream.  If this is the last stream **and** the current local time indicates that we are on the same utc day then 
we should continue to listen for new events on this stream.   If the local time indicates that we are on a subsequent utc day, we should refetch `<queue>_<jobset>_streams` 
and continue to poll the existing stream until this indicates that a new stream is available.

### Considerations
  
We may have to replace the autoexpiration of keys with a manual job to clean things up.  That is because when a stream `<queue>_<jobset>_<day>` is deleted,
we also need to remove it from the  `<queue>_<jobset>_streams` set.  If we simply let the stream expire, this will not occur.
  
We have doubled the number of writes when inserting events as now we not only need to insert the event, but also the update the set of streams.  I don't believe this
will be an issue as though the number of writes will be doubled, the extra amount of data will be trivial.  If this does become a problem we can probably batch
the writes to (hopefully) enable many events in the same jobset being written in a single call.

We have marginally increased the number of reads.  Specifically:
* There are n+1 extra reads at the start of a event request where we need to determine the streams and their offses; n is the number of streams. Given that the common case is for 
a subscription to start immediately, most of the time n will be 1 and so this shoudl be inconsequential.
* For jobsets that span multiple days there will be a period between midnight and the first event of the day where every poll will be at least 2 requests (one request to 
see if there any new streams, another to see if there are any new events).  This should be fine, however we might run into problems if there are a large number of jobsets
that span multiple days, have active subscriptions and typically don't see any new events on a given day until long after midnight.  if this is the case we could mitigate
this by only checking for new streams every x minutes.  
  
   
## Extension: Compressing old streams

To further decrease memory usage, we should be able to compress streams which are older than a couple of days.  This is something that can be done safely because 
we know that as long as we are not two days behind in the processing of pulsar messages (in which case we have bigger issues!) these streams will not be appended to.
The compression benefits shoiuld be significant, because while a single event message generally compresses quite poorly, multiple event messages for a given jobset typically 
have a large amount of common information and thus compress very well.
  
The general strategy here is:
  
* Create a job that runs periodically and iterates over the `<queue>_<jobset>_streams` keys
* For each key it will parse out the stream keys and determine if any are over x days old
* For each stream key over x days old it will read the stream and rewrite it to a new stream `<queue>_<jobset>_compressed_<day>` where each message in the new stream 
represents 4MB of compressed messages in the old stream.
* Once the new, compressed stream has been added it will modify the Redis Set stored at  `<queue>_<jobset>_streams` to remove the old uncompressed stream and add the new compressed stream.
* Delete the old, uncompressed, stream.
  
### Write-Side
This wil mainly be as above, but with two differences:
* We will need to manually set the sequenceId of each event in the Redis stream to be the pulsar message id.  This is because we need to maintain exactly the same ids when 
  we create the compressed stream.
* We will need each stream event to be not an `api.Event` but rather a sequence of (`pulsar_message_id`, `api.Event`) tuples.  When initally inserting
we will always just have a single item in the sequence representing the event to be inserted.
  
### Compression Job
* Iterate over all keys
* For each key that matches the regex `*_*_streams` we retrieve the value.
* The value will be a set of strings representing the stream keys in format `<queue>_<jobset>_<day>`.  Identify all streams more than 2 days old.
* For each stream, read a batch of events up to 4MB in size.  Construct a sequence of (`pulsar_message_id`, `api.Event`) tuples and compress. 
* Append the batch of events to `<queue>_<jobset>_compressed_<day>`, setting the Redis sequenceId to be the *highest* pulsar message id of the batch.
* Once the old stream has been converted, modify  `<queue>_<jobset>_streams` to remove the old uncompressed stream and add the new compressed stream.
* Delete the old uncompressed stream.
 
## Read Side
This will mainly be as above, but with two differences
  
* Polling redis will result in a sequence of events, some of which may have sequence numbers *below* which you have asked for.  It will be up to the event api to manually 
filter these out.  
* When polling redis, it is possible to get a KeyNotFound.  Specifically this will happen when the events api is serving up events from a stream that has just been compressed.
In this case the events api will need to refetch `<queue>_<jobset>_streams`, which should show the new, compressed stream and it can pick up where it left off. If no new compressed
stream appears in the results, this can be treated as a genuine KeyNotFound.
  
### Considerations
* The compression job will put non-negligable load on redis.  This is because it is going to have to read *all* events from *all* streams it is going to compress. 
To solve this, we could only compress a subset of the jobs, or we could throttle the job so that it continually runs but only compresses x streams per hour.   
* It's not clear how efficient it wil be to iterate over all keys.  I suspect it wil be fine but would need to check if e.g. we have 1 million keys.
* We should ensure we don't have multiple compression jobs running at the same time.
* I've chose 4MB above becuase it should give a batch big enough to see good compression, but small enought that it won't OOM.  In theory this could lead to 
  us pulling 2GB into memory (we try to pull 500 redis events at a time), but I think that should be unlikely as a) 4MB is the uncompressed batch size and b) it would
  require someone to be fetching old events on a truely gigiantic jobset. If we do think this is a problem we can reduce the batch size when fetching compressed streams. 
  Either way we wil probably have higher memory requirements on the events server.

  
  
  
  
  
  
 
  
  
  
  
  
  
