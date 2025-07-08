"""A very simple Streamlit app that connects to a MongoDB database and displays the data from the RawFile collection."""

import os

import streamlit as st
from service.components import show_sandbox_message
from service.utils import APP_URL, _log, display_info_message, quanting_output_path

from shared.keys import ALLOWED_CHARACTERS_IN_RAW_FILE_NAME, EnvVars

_log(f"loading {__file__} {st.query_params}")

st.set_page_config(page_title="AlphaKraken: home", layout="wide")

"""
# Welcome to AlphaKraken!
"""

if os.environ.get(EnvVars.ENV_NAME) != "production":
    show_sandbox_message()

c1, _ = st.columns([0.5, 0.5])

c1.markdown("""### What is AlphaKraken?

This tool should help you keep track of your acquisitions and monitor their status and quality in (near) real-time.
By default, every single acquisition is processed by AlphaDIA and the results are stored in a database.""")

display_info_message(c1)

c1.markdown("""### How to use it?

The "overview" tab shows all results, allows for filtering and sorting, and provides plots showing quality metrics.

The "status" tab shows the current status of the acquisition pipeline and the status of the last processed files.
It is mostly relevant for AlphaKraken admin users.

The "project" and "settings" tabs allow to manage specific AlphaDIA settings for certain raw files.
Currently they are meant to be used by AlphaKraken admin users only.""")

c1.markdown(f"""### Rules

#### Rule #1
To not interfere with the automated processing, please stick to the following simple rule:

**Do NOT do anything (!) on the acquisition folder (=the folder where the raw files are written to).**

In particular:
**Do not _create_, _move_, _rename_, or _delete_ any files there**! Avoid opening them in any software (wait until the file is moved to the
"Backup" subfolder and open it there).

#### More rules
To ensure a smooth automated processing, please follow these rules when acquiring files:
- Make sure to your raw filename does not contain any special characters. Only allowed: `{ALLOWED_CHARACTERS_IN_RAW_FILE_NAME}`
(basic latin letters, numbers, and a few special characters like `-`, `_`, `+`). Otherwise, they will not be quanted.
- If your file name contains `_dda_`, they will also not be quanted.""")


c1.markdown(f"""### FAQ

Q: Why is there a strange prefix (like "20241029-162042-876912-") in front of my file name?

A: This is because a file with the same name was already processed.
File name uniqueness is a prerequisite for all automated downstream processing to work correctly,
to tell those collision cases apart, a timestamp is added as a prefix.


Q: My file seems to be corrupted on the backup?

A: In very rare cases, the copying operation gets interrupted,
which is reflected by the "overview" table showing the status "error" for the file. Contact the developers in this case.


Q: Why do some files on the acquisition folder carry the extension `.deleteme`?

A: This is because they could not be removed after copying it to the `Backup` subfolder on the instrument
(e.g. due to the file being open). To facilitate manual cleanup, this extension is added.
Those files can be deleted safely.


Q: Some of my files still have status "monitoring_acquisition" although acquisition is already finished.

A: An acquisition is considered "finished" when either a new file is created in the acquisition folder or
if the current file has not changed for 2 hours. As a consequence, the last file in a queue will be processed
with this 2 hour delay (this could be remedied by adding a blank sample to the end of a queue).
Note: if AlphaKraken processing is disrupted for a certain time, it might take up to 2 hours until their
acquisition is considered "finished".


Q: Where to I find the AlphaDIA output files?

A:  The output files associated for a given raw file are stored at
   `{quanting_output_path}/<project id>/out_<raw file name>/`


Q: A lot of jobs are stuck in status "quanting" or "queued_for_quanting".

A: This is the case when the cluster is under heavy load. The jobs will be processed as soon as possible.
Currently, status "quanting" means "quanting job submitted", regardless if the job is still PENDING or already RUNNING.



Q: The data on the overview page seems to be limited, can I see more?

A: For performance reasons, by default only recent data are loaded, and the table is truncated (plots and csv download
are not). If you really need to see more data, use the `?max_age=` and `?max_table_len=` query parameters in the URL
(cf. below how to combine).


Q: I am tired of always filling the filter, can this be saved?

A: Yes. Just add `?filter=value` to the URL to pre-fill the filter and then create a browser bookmark.
For technical reasons, combining multiple conditions is done using "_AND_" or "%26", not "&" like in the UI,
likewise for "=" which needs to be masked as "_IS_".
Combine with other parameters (like `max_age=`) like this: `?filter=value1_AND_value2&max_age=60&max_table_len=9999`.


Q: I am missing a metric or a feature or found a bug or find the AlphaKraken unintuitive to use or want to contribute.

A: Please get in touch with the developers!


Q: I want to know more about the AlphaKraken.

A: Have a look at the [github repo](https://github.com/MannLabs/alphakraken) or the underlying
Airflow implementation [here]({APP_URL}:8080) (read-only, ask for credentials),

""")
