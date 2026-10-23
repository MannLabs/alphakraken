# Messenger ids of users replace `INSTRUMENT_USER_SLACK_IDS`

Direct messages about alerts (e.g. queue stops) now go to the messenger id stored with each user
on the admin page. The hard-coded `INSTRUMENT_USER_SLACK_IDS` in `monitoring/alerts/config.py` is
removed. The queue-stop alert recognizes the initials of all active users with a messenger id in
raw file names.

There is no DB migration: the new user field `messenger_id` is optional.

## Steps
1. Note down the entries of `INSTRUMENT_USER_SLACK_IDS` if it was edited on a deployment.
2. Deploy.
3. On the admin page, make sure a user exists for each of these initials, and set the noted
   Slack member ID as its messenger id.
