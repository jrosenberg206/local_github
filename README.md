# LemonHaze

### Note
The three python database load programs are all versions of each other.
The version in all_retail is the best version, and should work for all three purposes.

### shc-3.8.7 
Directory where the current Lemon Haze customers data processing programs reside

Current crontab listing:
01 15 * * * source /data/api/shc-3.8.7/./DailyA.sh >/tmp/DailyA.log 2>&1
03 15 * * * source /data/api/shc-3.8.7/./DailyB.sh >/tmp/DailyB.log 2>&1
05 15 * * * source /data/api/shc-3.8.7/./DailyC.sh >/tmp/DailyC.log 2>&1
07 15 * * * source /data/api/shc-3.8.7/./DailyD.sh >/tmp/DailyD.log 2>&1

### state
Directory where the program used to load the state data loading program.

Running this requires:
- Changing the LoadEntityDb.py program's:
-- StateLoader(...) invocation to point to the next months box.com directory.
-- createSchema('state...')
- Seeing various environment variables for database access.

### all_retail
Directory where latest python database load program exists.

Uses the TetraTrak credentials to load all the BioTrack data for all the retail shops.
The LoadEntityDb.py program is what was used to sucessfully load the database.
There are a few immediate fix ups that need to be made:
- The program is currently grabbing the data via the BioTrack API via streaming http.  This is necessary because some of the table loads were running the program out of memory. This is hard as the entire download is a giant block of JSON, and JSON doesn't stream decode. The current stream decode logic is just ugly. I've written a new class oriented version that is almost pretty. But I need to test it before I commit it.
- The program needs to load additionall data in an incremental fashion.

